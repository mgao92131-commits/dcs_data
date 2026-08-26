package main

import (
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type batchImporter struct {
	pool              *pgxpool.Pool
	inbox             string
	archive           string
	interval          time.Duration
	importTimeout     time.Duration
	importBatchSize   int
	maxBatchesPerPass int
	timezone          *time.Location
	logger            *log.Logger
}

type importRow struct {
	SampleKey string
	Tag       string
	TimeText  string
	Value     float64
}

type importBatch struct {
	BatchID string
	SHA256  string
	Rows    int
	Data    []importRow
}

func newBatchImporter(config receiverConfig, pool *pgxpool.Pool, logger *log.Logger) (*batchImporter, error) {
	location, err := time.LoadLocation(config.PostgresTimezone)
	if err != nil {
		return nil, fmt.Errorf("invalid PostgreSQL timezone %q: %w", config.PostgresTimezone, err)
	}
	if err := os.MkdirAll(config.Archive, 0750); err != nil {
		return nil, err
	}
	return &batchImporter{
		pool:              pool,
		inbox:             config.Inbox,
		archive:           config.Archive,
		interval:          config.ImportInterval,
		importTimeout:     config.ImportTimeout,
		importBatchSize:   config.ImportBatchSize,
		maxBatchesPerPass: config.MaxBatchesPerPass,
		timezone:          location,
		logger:            logger,
	}, nil
}

func (i *batchImporter) run(ctx context.Context) {
	i.importAndLog(ctx)
	ticker := time.NewTicker(i.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			i.importAndLog(ctx)
		}
	}
}

func (i *batchImporter) importAndLog(ctx context.Context) {
	imported, failed, err := i.importOnce(ctx)
	if err != nil {
		i.logger.Printf("import scan failed: %v", err)
		return
	}
	if imported > 0 || failed > 0 {
		i.logger.Printf("import pass completed imported=%d failed=%d", imported, failed)
	}
}

func (i *batchImporter) importOnce(ctx context.Context) (int, int, error) {
	entries, err := os.ReadDir(i.inbox)
	if err != nil {
		return 0, 0, err
	}
	var directories []string
	for _, entry := range entries {
		if entry.IsDir() {
			directories = append(directories, entry.Name())
		}
	}
	sort.Strings(directories)
	if len(directories) > i.maxBatchesPerPass {
		directories = directories[:i.maxBatchesPerPass]
	}

	imported := 0
	failed := 0
	for _, name := range directories {
		batchDir := filepath.Join(i.inbox, name)
		batch, err := i.loadBatch(batchDir)
		if err == nil {
			batchCtx, cancel := context.WithTimeout(ctx, i.importTimeout)
			err = i.importBatch(batchCtx, batch)
			cancel()
		}
		if err == nil {
			err = i.moveToArchive(batchDir, name)
		}
		if err != nil {
			failed++
			i.logger.Printf("import failed batch=%s error=%v", name, err)
			continue
		}
		imported++
		i.logger.Printf("imported batch=%s rows=%d", batch.BatchID, batch.Rows)
	}
	return imported, failed, nil
}

func (i *batchImporter) loadBatch(directory string) (importBatch, error) {
	metaPath := filepath.Join(directory, "meta.ini")
	dataPath := filepath.Join(directory, "data.csv")
	values, err := readINI(metaPath)
	if err != nil {
		return importBatch{}, err
	}
	batchID := values["Batch.BatchId"]
	expectedHash := strings.ToLower(values["Batch.Sha256"])
	expectedRows, err := strconv.Atoi(values["Batch.Rows"])
	if batchID == "" || !safeID.MatchString(batchID) || batchID != filepath.Base(directory) {
		return importBatch{}, errors.New("invalid BatchId in meta.ini")
	}
	if err != nil || expectedRows < 0 {
		return importBatch{}, errors.New("invalid Rows in meta.ini")
	}
	if decoded, err := hex.DecodeString(expectedHash); err != nil || len(decoded) != sha256.Size {
		return importBatch{}, errors.New("invalid Sha256 in meta.ini")
	}
	actualHash, err := hashFile(dataPath)
	if err != nil {
		return importBatch{}, err
	}
	if !strings.EqualFold(expectedHash, actualHash) {
		return importBatch{}, errors.New("data.csv SHA-256 does not match meta.ini")
	}

	rows, err := i.readImportRows(dataPath)
	if err != nil {
		return importBatch{}, err
	}
	if len(rows) != expectedRows {
		return importBatch{}, fmt.Errorf("row count mismatch: expected %d, got %d", expectedRows, len(rows))
	}
	return importBatch{BatchID: batchID, SHA256: expectedHash, Rows: expectedRows, Data: rows}, nil
}

func (i *batchImporter) readImportRows(path string) ([]importRow, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = 7
	header, err := reader.Read()
	if err != nil {
		return nil, err
	}
	header[0] = strings.TrimPrefix(header[0], "\ufeff")
	expected := []string{"Tag", "Timestamp", "Value", "DataType", "Flags", "SequenceNo", "ArchiveStatus"}
	for index := range expected {
		if header[index] != expected[index] {
			return nil, errors.New("unexpected CSV header")
		}
	}

	var rows []importRow
	line := 1
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		line++
		if err != nil {
			return nil, fmt.Errorf("CSV line %d: %w", line, err)
		}
		tag := strings.TrimSpace(record[0])
		timestamp := strings.TrimSpace(record[1])
		valueText := strings.TrimSpace(record[2])
		if tag == "" {
			return nil, fmt.Errorf("CSV line %d: Tag is empty", line)
		}
		parsedTime, err := time.ParseInLocation("2006-01-02 15:04:05", timestamp, i.timezone)
		if err != nil {
			return nil, fmt.Errorf("CSV line %d: invalid Timestamp %q", line, timestamp)
		}
		value, err := strconv.ParseFloat(valueText, 64)
		if err != nil {
			return nil, fmt.Errorf("CSV line %d: invalid numeric Value %q", line, valueText)
		}
		rows = append(rows, importRow{
			SampleKey: sampleKey(tag, timestamp, valueText),
			Tag:       tag,
			TimeText:  parsedTime.Format("2006-01-02 15:04:05.000000"),
			Value:     value,
		})
	}
	return rows, nil
}

func (i *batchImporter) importBatch(ctx context.Context, batch importBatch) error {
	var existingHash string
	var existingRows int
	err := i.pool.QueryRow(
		ctx,
		"SELECT sha256, row_count FROM imported_batches WHERE batch_id=$1",
		batch.BatchID).Scan(&existingHash, &existingRows)
	if err == nil {
		if strings.EqualFold(strings.TrimSpace(existingHash), batch.SHA256) && existingRows == batch.Rows {
			return nil
		}
		return errors.New("imported batch_id exists with different content")
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	tx, err := i.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	for start := 0; start < len(batch.Data); start += i.importBatchSize {
		end := start + i.importBatchSize
		if end > len(batch.Data) {
			end = len(batch.Data)
		}
		queued := &pgx.Batch{}
		for _, row := range batch.Data[start:end] {
			queued.Queue(
				`INSERT INTO history_raw (sample_key, tag, sample_time, value, batch_id)
				 VALUES ($1, $2, $3::timestamp, $4, $5)
				 ON CONFLICT (sample_key) DO NOTHING`,
				row.SampleKey, row.Tag, row.TimeText, row.Value, batch.BatchID)
		}
		results := tx.SendBatch(ctx, queued)
		for index := start; index < end; index++ {
			if _, err = results.Exec(); err != nil {
				_ = results.Close()
				return err
			}
		}
		if err = results.Close(); err != nil {
			return err
		}
	}

	_, err = tx.Exec(
		ctx,
		`INSERT INTO imported_batches (batch_id, sha256, row_count)
		 VALUES ($1, $2, $3)`,
		batch.BatchID,
		batch.SHA256,
		batch.Rows)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (i *batchImporter) moveToArchive(source, batchID string) error {
	destination := filepath.Join(i.archive, batchID)
	if _, err := os.Stat(destination); err == nil {
		destination += "_duplicate_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Rename(source, destination)
}

func sampleKey(tag, timestamp, value string) string {
	digest := sha256.Sum256([]byte(tag + "\x1f" + timestamp + "\x1f" + value))
	return hex.EncodeToString(digest[:])
}

func hashFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}
