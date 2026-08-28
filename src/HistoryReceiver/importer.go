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

type importTimings struct {
	Parse  time.Duration
	Copy   time.Duration
	Upsert time.Duration
	Commit time.Duration
}

type importBatch struct {
	BatchID          string
	CollectorID      string
	SHA256           string
	Rows             int
	Data             []importRow
	Timings          *importTimings
	AlreadyCommitted bool
}

const historySamplesUpsertSQL = `INSERT INTO history_samples
	 (sample_key, collector_id, tag, sample_time, value_double, value_text,
	  data_type, flags, sequence_no, archive_status, batch_id)
	 SELECT sample_key, collector_id, tag, sample_time::timestamp, value_double,
	  value_text, data_type, flags, sequence_no, archive_status, batch_id
	 FROM (
	  SELECT DISTINCT ON (sample_key) *
	  FROM history_samples_stage
	  ORDER BY sample_key, row_order DESC
	 ) AS staged
	 ON CONFLICT (sample_key) DO UPDATE SET
	  value_double=EXCLUDED.value_double, value_text=EXCLUDED.value_text,
	  data_type=EXCLUDED.data_type, flags=EXCLUDED.flags,
	  archive_status=EXCLUDED.archive_status, batch_id=EXCLUDED.batch_id,
	  received_at=CURRENT_TIMESTAMP
	 WHERE history_samples.value_double IS DISTINCT FROM EXCLUDED.value_double
	    OR history_samples.value_text IS DISTINCT FROM EXCLUDED.value_text
	    OR history_samples.data_type IS DISTINCT FROM EXCLUDED.data_type
	    OR history_samples.flags IS DISTINCT FROM EXCLUDED.flags
	    OR history_samples.archive_status IS DISTINCT FROM EXCLUDED.archive_status`

func newBatchImporter(config receiverConfig, pool *pgxpool.Pool, logger *log.Logger) (*batchImporter, error) {
	location := config.PostgresLocation
	if location == nil {
		var err error
		location, err = time.LoadLocation(config.PostgresTimezone)
		if err != nil {
			return nil, fmt.Errorf("invalid PostgreSQL timezone %q: %w", config.PostgresTimezone, err)
		}
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
		started := time.Now()
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
			err = i.importBatch(batchCtx, &batch)
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
		parseMs := int64(0)
		copyMs := int64(0)
		upsertMs := int64(0)
		commitMs := int64(0)
		if batch.Timings != nil {
			parseMs = durationMilliseconds(batch.Timings.Parse)
			copyMs = durationMilliseconds(batch.Timings.Copy)
			upsertMs = durationMilliseconds(batch.Timings.Upsert)
			commitMs = durationMilliseconds(batch.Timings.Commit)
		}
		totalMs := durationMilliseconds(time.Since(started))
		i.logger.Printf(
			"imported batch=%s rows=%d ReceiveMs=0 ValidateMs=0 ParseMs=%d CopyMs=%d UpsertMs=%d CommitMs=%d TotalMs=%d elapsed=%dms",
			batch.BatchID, batch.Rows, parseMs, copyMs, upsertMs, commitMs, totalMs, totalMs)
	}
	return imported, failed, nil
}

func (i *batchImporter) importDirectory(ctx context.Context, directory string) (importBatch, error) {
	batch, err := i.loadBatch(directory)
	if err != nil {
		return batch, err
	}
	return i.importPreparedBatch(ctx, batch)
}

func (i *batchImporter) importPreparedBatch(ctx context.Context, batch importBatch) (importBatch, error) {
	if batch.Timings == nil {
		batch.Timings = &importTimings{}
	}
	batchCtx, cancel := context.WithTimeout(ctx, i.importTimeout)
	err := i.importBatch(batchCtx, &batch)
	cancel()
	if err != nil {
		return batch, err
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
	actualHash, rows, parseElapsed, err := i.readAndParseImportRows(dataPath, collectorID)
	timings := &importTimings{Parse: parseElapsed}
	if actualHash != "" && !strings.EqualFold(expectedHash, actualHash) {
		err = fmt.Errorf("%w: data.csv SHA-256 does not match meta.ini", errInvalidBatch)
	}
	if err != nil {
		return importBatch{Timings: timings}, err
	}
	if len(rows) != expectedRows {
		return importBatch{Timings: timings}, fmt.Errorf("%w: row count mismatch: expected %d, got %d", errInvalidBatch, expectedRows, len(rows))
	}
	return importBatch{
		BatchID: batchID, CollectorID: collectorID, SHA256: expectedHash,
		Rows: expectedRows, Data: rows, Timings: timings,
	}, nil
}

func (i *batchImporter) readImportRows(path, collectorID string) ([]importRow, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return parseImportRows(file, collectorID, i.timezone)
}

func (i *batchImporter) readAndParseImportRows(path, collectorID string) (string, []importRow, time.Duration, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", nil, 0, err
	}
	defer file.Close()

	digest := sha256.New()
	tee := io.TeeReader(file, digest)
	started := time.Now()
	rows, parseErr := parseImportRows(tee, collectorID, i.timezone)
	if parseErr != nil {
		_, drainErr := io.Copy(io.Discard, tee)
		if drainErr != nil {
			parseErr = fmt.Errorf("%w: cannot finish reading data.csv: %v", errInvalidBatch, drainErr)
		}
	}
	return hex.EncodeToString(digest.Sum(nil)), rows, time.Since(started), parseErr
}

func parseImportRows(source io.Reader, collectorID string, timezone *time.Location) ([]importRow, error) {
	if timezone == nil {
		timezone = time.Local
	}
	reader := csv.NewReader(source)
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
		valueText := record[2]
		if tag == "" {
			return nil, fmt.Errorf("%w: CSV line %d: Tag is empty", errInvalidBatch, line)
		}
		parsedTime, err := time.ParseInLocation("2006-01-02 15:04:05", timestamp, timezone)
		if err != nil {
			return nil, fmt.Errorf("%w: CSV line %d: invalid Timestamp %q", errInvalidBatch, line, timestamp)
		}
		var valueDouble *float64
		if value, parseErr := strconv.ParseFloat(strings.TrimSpace(valueText), 64); parseErr == nil {
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

func (i *batchImporter) findImportedBatch(
	ctx context.Context,
	batchID string,
	sha256Text string,
	rows int,
) (bool, error) {
	if i == nil || i.pool == nil {
		return false, errors.New("PostgreSQL importer is unavailable")
	}
	var existingHash string
	var existingRows int
	err := i.pool.QueryRow(
		ctx,
		"SELECT sha256, row_count FROM imported_batches WHERE batch_id=$1",
		batchID).Scan(&existingHash, &existingRows)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !strings.EqualFold(strings.TrimSpace(existingHash), sha256Text) || existingRows != rows {
		return true, errBatchConflict
	}
	return true, nil
}

func (i *batchImporter) importBatch(ctx context.Context, batch *importBatch) error {
	if i == nil || i.pool == nil {
		return errors.New("PostgreSQL importer is unavailable")
	}
	if batch == nil {
		return errors.New("import batch is nil")
	}
	var existingHash string
	var existingRows int
	err := i.pool.QueryRow(
		ctx,
		"SELECT sha256, row_count FROM imported_batches WHERE batch_id=$1",
		batch.BatchID).Scan(&existingHash, &existingRows)
	if err == nil {
		if strings.EqualFold(strings.TrimSpace(existingHash), batch.SHA256) && existingRows == batch.Rows {
			batch.AlreadyCommitted = true
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

	copyStarted := time.Now()
	_, err = tx.Exec(ctx, `CREATE TEMP TABLE history_samples_stage (
		row_order bigint NOT NULL,
		sample_key text NOT NULL,
		collector_id text NOT NULL,
		tag text NOT NULL,
		sample_time text NOT NULL,
		value_double double precision,
		value_text text NOT NULL,
		data_type text NOT NULL,
		flags text NOT NULL,
		sequence_no text NOT NULL,
		archive_status text NOT NULL,
		batch_id text NOT NULL
	) ON COMMIT DROP`)
	if err != nil {
		if batch.Timings != nil {
			batch.Timings.Copy = time.Since(copyStarted)
		}
		return err
	}

	copyRows := make([][]interface{}, len(batch.Data))
	for index, row := range batch.Data {
		copyRows[index] = []interface{}{
			int64(index), row.SampleKey, batch.CollectorID, row.Tag, row.TimeText,
			row.ValueDouble, row.ValueText, row.DataType, row.Flags,
			row.SequenceNo, row.ArchiveStatus, batch.BatchID,
		}
	}
	copied, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"history_samples_stage"},
		[]string{
			"row_order", "sample_key", "collector_id", "tag", "sample_time",
			"value_double", "value_text", "data_type", "flags", "sequence_no",
			"archive_status", "batch_id",
		},
		pgx.CopyFromRows(copyRows))
	if err != nil {
		if batch.Timings != nil {
			batch.Timings.Copy = time.Since(copyStarted)
		}
		return err
	}
	if batch.Timings != nil {
		batch.Timings.Copy = time.Since(copyStarted)
	}
	if copied != int64(len(batch.Data)) {
		return fmt.Errorf("staging COPY row count mismatch: expected %d, got %d", len(batch.Data), copied)
	}

	upsertStarted := time.Now()
	_, err = tx.Exec(ctx, historySamplesUpsertSQL)
	if err != nil {
		if batch.Timings != nil {
			batch.Timings.Upsert = time.Since(upsertStarted)
		}
		return err
	}
	_, err = tx.Exec(
		ctx,
		`INSERT INTO imported_batches (batch_id, sha256, row_count)
		 VALUES ($1, $2, $3)`,
		batch.BatchID,
		batch.SHA256,
		batch.Rows)
	if err != nil {
		if batch.Timings != nil {
			batch.Timings.Upsert = time.Since(upsertStarted)
		}
		return err
	}
	if batch.Timings != nil {
		batch.Timings.Upsert = time.Since(upsertStarted)
	}
	commitStarted := time.Now()
	err = tx.Commit(ctx)
	if batch.Timings != nil {
		batch.Timings.Commit = time.Since(commitStarted)
	}
	return err
}

func (i *batchImporter) moveToArchive(source, batchID string) error {
	destination := filepath.Join(i.archive, batchID)
	if info, err := os.Stat(destination); err == nil {
		if !info.IsDir() {
			return fmt.Errorf("archive destination exists but is not a directory: %s", destination)
		}
		if err := os.RemoveAll(source); err != nil {
			return fmt.Errorf("remove duplicate archive source: %w", err)
		}
		return nil
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
	digestText := hex.EncodeToString(digest[:])
	if !strings.HasPrefix(sequenceNo, "P:") {
		return digestText
	}

	groupDigest := sha256.Sum256([]byte(collectorID + "\x1f" + tag + "\x1f" + sequenceNo))
	groupText := hex.EncodeToString(groupDigest[:])
	sortableTime := strings.NewReplacer("-", "", " ", "", ":", "", ".", "").Replace(timestamp)
	if len(sortableTime) > 21 {
		sortableTime = sortableTime[:21]
	}
	for len(sortableTime) < 21 {
		sortableTime += "0"
	}
	return groupText[:16] + sortableTime + digestText[:27]
}
