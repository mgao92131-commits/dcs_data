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
	rejected          string
	interval          time.Duration
	importTimeout     time.Duration
	importBatchSize   int
	maxBatchesPerPass int
	timezone          *time.Location
	logger            *log.Logger
}

var errBatchConflict = errors.New("batch_id already exists with different content")
var errInvalidBatch = errors.New("invalid batch")

type importRow struct {
	SampleKey     string
	Tag           string
	TimeText      string
	ValueText     string
	ValueDouble   *float64
	DataType      string
	Flags         string
	SequenceNo    string
	ArchiveStatus string
}

type importBatch struct {
	BatchID     string
	CollectorID string
	SHA256      string
	Rows        int
	Data        []importRow
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
		rejected:          config.Rejected,
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
		if err != nil && errors.Is(err, errInvalidBatch) {
			failed++
			if moveErr := i.moveToRejected(batchDir, name, "invalid"); moveErr != nil {
				i.logger.Printf("invalid batch quarantine failed batch=%s error=%v quarantine_error=%v", name, err, moveErr)
			} else {
				i.logger.Printf("quarantined invalid batch=%s error=%v", name, err)
			}
			continue
		}
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
			if errors.Is(err, errBatchConflict) {
				if moveErr := i.moveToRejected(batchDir, name, "conflict"); moveErr != nil {
					i.logger.Printf("conflicting batch quarantine failed batch=%s error=%v quarantine_error=%v", name, err, moveErr)
				} else {
					i.logger.Printf("quarantined conflicting batch=%s error=%v", name, err)
				}
				continue
			}
			i.logger.Printf("import failed batch=%s error=%v", name, err)
			continue
		}
		imported++
		i.logger.Printf("imported batch=%s rows=%d", batch.BatchID, batch.Rows)
	}
	return imported, failed, nil
}

func (i *batchImporter) importDirectory(ctx context.Context, directory string) (importBatch, error) {
	batch, err := i.loadBatch(directory)
	if err != nil {
		return importBatch{}, err
	}
	batchCtx, cancel := context.WithTimeout(ctx, i.importTimeout)
	err = i.importBatch(batchCtx, batch)
	cancel()
	if err != nil {
		return importBatch{}, err
	}
	return batch, nil
}

func (i *batchImporter) loadBatch(directory string) (importBatch, error) {
	metaPath := filepath.Join(directory, "meta.ini")
	dataPath := filepath.Join(directory, "data.csv")
	values, err := readINI(metaPath)
	if err != nil {
		return importBatch{}, err
	}
	batchID := values["Batch.BatchId"]
	collectorID := values["Batch.CollectorId"]
	expectedHash := strings.ToLower(values["Batch.Sha256"])
	expectedRows, err := strconv.Atoi(values["Batch.Rows"])
	directoryName := filepath.Base(directory)
	validDirectory := batchID == directoryName || strings.HasPrefix(directoryName, batchID+".tmp.")
	if batchID == "" || !safeID.MatchString(batchID) || !validDirectory {
		return importBatch{}, fmt.Errorf("%w: invalid BatchId in meta.ini", errInvalidBatch)
	}
	if collectorID == "" || !safeID.MatchString(collectorID) {
		return importBatch{}, fmt.Errorf("%w: invalid CollectorId in meta.ini", errInvalidBatch)
	}
	if err != nil || expectedRows < 0 {
		return importBatch{}, fmt.Errorf("%w: invalid Rows in meta.ini", errInvalidBatch)
	}
	if decoded, err := hex.DecodeString(expectedHash); err != nil || len(decoded) != sha256.Size {
		return importBatch{}, fmt.Errorf("%w: invalid Sha256 in meta.ini", errInvalidBatch)
	}
	actualHash, err := hashFile(dataPath)
	if err != nil {
		return importBatch{}, err
	}
	if !strings.EqualFold(expectedHash, actualHash) {
		return importBatch{}, fmt.Errorf("%w: data.csv SHA-256 does not match meta.ini", errInvalidBatch)
	}

	rows, err := i.readImportRows(dataPath, collectorID)
	if err != nil {
		return importBatch{}, err
	}
	if len(rows) != expectedRows {
		return importBatch{}, fmt.Errorf("%w: row count mismatch: expected %d, got %d", errInvalidBatch, expectedRows, len(rows))
	}
	return importBatch{BatchID: batchID, CollectorID: collectorID, SHA256: expectedHash, Rows: expectedRows, Data: rows}, nil
}

func (i *batchImporter) readImportRows(path, collectorID string) ([]importRow, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = 7
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("%w: cannot read CSV header: %v", errInvalidBatch, err)
	}
	header[0] = strings.TrimPrefix(header[0], "\ufeff")
	expected := []string{"Tag", "Timestamp", "Value", "DataType", "Flags", "SequenceNo", "ArchiveStatus"}
	for index := range expected {
		if header[index] != expected[index] {
			return nil, fmt.Errorf("%w: unexpected CSV header", errInvalidBatch)
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
			return nil, fmt.Errorf("%w: CSV line %d: %v", errInvalidBatch, line, err)
		}
		tag := strings.TrimSpace(record[0])
		timestamp := strings.TrimSpace(record[1])
		valueText := strings.TrimSpace(record[2])
		if tag == "" {
			return nil, fmt.Errorf("%w: CSV line %d: Tag is empty", errInvalidBatch, line)
		}
		parsedTime, err := time.ParseInLocation("2006-01-02 15:04:05", timestamp, i.timezone)
		if err != nil {
			return nil, fmt.Errorf("%w: CSV line %d: invalid Timestamp %q", errInvalidBatch, line, timestamp)
		}
		var valueDouble *float64
		if value, parseErr := strconv.ParseFloat(valueText, 64); parseErr == nil {
			valueDouble = &value
		}
		rows = append(rows, importRow{
			SampleKey:     sampleKey(collectorID, tag, timestamp, strings.TrimSpace(record[5]), valueText),
			Tag:           tag,
			TimeText:      parsedTime.Format("2006-01-02 15:04:05.000000"),
			ValueText:     valueText,
			ValueDouble:   valueDouble,
			DataType:      strings.TrimSpace(record[3]),
			Flags:         strings.TrimSpace(record[4]),
			SequenceNo:    strings.TrimSpace(record[5]),
			ArchiveStatus: strings.TrimSpace(record[6]),
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
		return errBatchConflict
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
				`INSERT INTO history_samples
				 (sample_key, collector_id, tag, sample_time, value_double, value_text,
				  data_type, flags, sequence_no, archive_status, batch_id)
				 VALUES ($1, $2, $3, $4::timestamp, $5, $6, $7, $8, $9, $10, $11)
				 ON CONFLICT (sample_key) DO UPDATE SET
				  value_double=EXCLUDED.value_double, value_text=EXCLUDED.value_text,
				  data_type=EXCLUDED.data_type, flags=EXCLUDED.flags,
				  archive_status=EXCLUDED.archive_status, batch_id=EXCLUDED.batch_id,
				  received_at=CURRENT_TIMESTAMP`,
				row.SampleKey, batch.CollectorID, row.Tag, row.TimeText,
				row.ValueDouble, row.ValueText, row.DataType, row.Flags,
				row.SequenceNo, row.ArchiveStatus, batch.BatchID)
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

func (i *batchImporter) moveToRejected(source, batchID, reason string) error {
	destination := filepath.Join(
		i.rejected,
		batchID+"_"+reason+"_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	return os.Rename(source, destination)
}

func sampleKey(collectorID, tag, timestamp, sequenceNo, valueText string) string {
	identity := "sequence:" + sequenceNo
	if sequenceNo == "" {
		identity = "value:" + valueText
	}
	digest := sha256.Sum256([]byte(collectorID + "\x1f" + tag + "\x1f" + timestamp + "\x1f" + identity))
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
