//go:build integration
// +build integration

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSynchronousCommitEndToEnd(t *testing.T) {
	databaseURL := os.Getenv("DCS_HISTORY_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DCS_HISTORY_TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	ensureIntegrationSchema(t, ctx, pool)

	root := t.TempDir()
	config := receiverConfig{
		APIKey:            "test-secret",
		MaxBodyBytes:      1024 * 1024,
		Inbox:             filepath.Join(root, "inbox"),
		Archive:           filepath.Join(root, "archive"),
		Staging:           filepath.Join(root, "staging"),
		Rejected:          filepath.Join(root, "rejected"),
		PostgresEnabled:   true,
		SynchronousCommit: true,
		PostgresTimezone:  "Asia/Shanghai",
		ImportTimeout:     10 * time.Second,
		ImportBatchSize:   500,
		MaxBatchesPerPass: 20,
	}
	for _, directory := range []string{config.Inbox, config.Archive, config.Staging, config.Rejected} {
		if err := os.MkdirAll(directory, 0750); err != nil {
			t.Fatal(err)
		}
	}
	importer, err := newBatchImporter(config, pool, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := &receiverServer{
		config:   config,
		logger:   log.New(io.Discard, "", 0),
		dbPool:   pool,
		importer: importer,
	}
	httpServer := httptest.NewServer(server.routes())
	defer httpServer.Close()

	batchID := "integration_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	body := integrationCSV(time.Now().UTC().Truncate(time.Microsecond))
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, "DELETE FROM history_samples WHERE batch_id=$1", batchID)
		_, _ = pool.Exec(cleanupCtx, "DELETE FROM imported_batches WHERE batch_id=$1", batchID)
	}()

	endpoint := httpServer.URL + "/api/history/batch"
	status, responseBody := postIntegrationBatch(t, endpoint, batchID, body)
	if status != http.StatusOK {
		t.Fatalf("first POST status=%d body=%s", status, responseBody)
	}
	var ack ackResponse
	if err := json.Unmarshal(responseBody, &ack); err != nil {
		t.Fatal(err)
	}
	if !ack.OK || !ack.Committed || ack.CommitLevel != "database" ||
		ack.BatchID != batchID || ack.ReceivedRows != 2 {
		t.Fatalf("unexpected synchronous ACK: %+v", ack)
	}

	var importedCount, sampleCount int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM imported_batches WHERE batch_id=$1", batchID).Scan(&importedCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM history_samples WHERE batch_id=$1", batchID).Scan(&sampleCount); err != nil {
		t.Fatal(err)
	}
	if importedCount != 1 || sampleCount != 2 {
		t.Fatalf("database commit not visible after ACK: imported=%d samples=%d", importedCount, sampleCount)
	}

	status, responseBody = postIntegrationBatch(t, endpoint, batchID, body)
	if status != http.StatusOK {
		t.Fatalf("idempotent retry status=%d body=%s", status, responseBody)
	}
	if err := json.Unmarshal(responseBody, &ack); err != nil {
		t.Fatal(err)
	}
	if !ack.OK || !ack.Committed || ack.CommitLevel != "database" {
		t.Fatalf("unexpected retry ACK: %+v", ack)
	}

}

func TestBulkImport49600Rows(t *testing.T) {
	databaseURL := os.Getenv("DCS_HISTORY_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DCS_HISTORY_TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	ensureIntegrationSchema(t, ctx, pool)

	const rowCount = 49600
	testID := strconv.FormatInt(time.Now().UnixNano(), 10)
	collectorID := "DCS-BULK-" + testID
	firstBatchID := "bulk_insert_" + testID
	noopBatchID := "bulk_noop_" + testID
	updateBatchID := "bulk_update_" + testID
	rows := make([]importRow, rowCount)
	startTime := time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC)
	for index := range rows {
		timestamp := startTime.Add(time.Duration(index) * time.Second).Format("2006-01-02 15:04:05.000000")
		value := float64(index) / 10
		rows[index] = importRow{
			SampleKey:     sampleKey(collectorID, "TAG/BULK", timestamp, "P:InterpolatedValue:10", strconv.FormatFloat(value, 'f', 1, 64)),
			Tag:           "TAG/BULK",
			TimeText:      timestamp,
			ValueText:     strconv.FormatFloat(value, 'f', 1, 64),
			ValueDouble:   &value,
			DataType:      "Float",
			SequenceNo:    "P:InterpolatedValue:10",
			ArchiveStatus: "Current",
		}
	}
	importer := &batchImporter{pool: pool}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, "DELETE FROM history_samples WHERE batch_id IN ($1, $2, $3)", firstBatchID, noopBatchID, updateBatchID)
		_, _ = pool.Exec(cleanupCtx, "DELETE FROM imported_batches WHERE batch_id IN ($1, $2, $3)", firstBatchID, noopBatchID, updateBatchID)
	}()

	insertBatch := importBatch{
		BatchID: firstBatchID, CollectorID: collectorID,
		SHA256: strings.Repeat("a", 64), Rows: rowCount, Data: rows,
	}
	started := time.Now()
	if err := importer.importBatch(ctx, insertBatch); err != nil {
		t.Fatal(err)
	}
	t.Logf("bulk insert rows=%d elapsed=%s", rowCount, time.Since(started).Round(time.Millisecond))

	var beforeReceivedAt time.Time
	var beforeBatchID string
	if err := pool.QueryRow(ctx,
		"SELECT received_at, batch_id FROM history_samples WHERE sample_key=$1",
		rows[0].SampleKey).Scan(&beforeReceivedAt, &beforeBatchID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	noopBatch := importBatch{
		BatchID: noopBatchID, CollectorID: collectorID,
		SHA256: strings.Repeat("c", 64), Rows: rowCount, Data: rows,
	}
	if err := importer.importBatch(ctx, noopBatch); err != nil {
		t.Fatal(err)
	}
	var afterReceivedAt time.Time
	var afterBatchID string
	if err := pool.QueryRow(ctx,
		"SELECT received_at, batch_id FROM history_samples WHERE sample_key=$1",
		rows[0].SampleKey).Scan(&afterReceivedAt, &afterBatchID); err != nil {
		t.Fatal(err)
	}
	if !afterReceivedAt.Equal(beforeReceivedAt) || afterBatchID != beforeBatchID {
		t.Fatalf("identical overlap unexpectedly updated row: before=%s/%s after=%s/%s", beforeReceivedAt, beforeBatchID, afterReceivedAt, afterBatchID)
	}

	for index := range rows {
		value := float64(index)/10 + 1
		rows[index].ValueDouble = &value
		rows[index].ValueText = strconv.FormatFloat(value, 'f', 1, 64)
	}
	updateBatch := importBatch{
		BatchID: updateBatchID, CollectorID: collectorID,
		SHA256: strings.Repeat("b", 64), Rows: rowCount, Data: rows,
	}
	started = time.Now()
	if err := importer.importBatch(ctx, updateBatch); err != nil {
		t.Fatal(err)
	}
	t.Logf("bulk update rows=%d elapsed=%s", rowCount, time.Since(started).Round(time.Millisecond))

	var count int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM history_samples WHERE collector_id=$1 AND batch_id=$2",
		collectorID, updateBatchID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != rowCount {
		t.Fatalf("expected %d updated rows, got %d", rowCount, count)
	}
}

func ensureIntegrationSchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	statements := []string{
		`CREATE TABLE IF NOT EXISTS imported_batches (
			batch_id text PRIMARY KEY,
			sha256 char(64) NOT NULL,
			row_count integer NOT NULL,
			imported_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS history_samples (
			sample_key char(64) PRIMARY KEY,
			collector_id text NOT NULL,
			tag text NOT NULL,
			sample_time timestamp(6) NOT NULL,
			value_double double precision,
			value_text text NOT NULL,
			data_type text NOT NULL DEFAULT '',
			flags text NOT NULL DEFAULT '',
			sequence_no text NOT NULL DEFAULT '',
			archive_status text NOT NULL DEFAULT '',
			batch_id text NOT NULL,
			received_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
}

func postIntegrationBatch(t *testing.T, endpoint, batchID string, body []byte) (int, []byte) {
	t.Helper()
	digest := sha256.Sum256(body)
	request, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer test-secret")
	request.Header.Set("X-Collector-Id", "DCS-INTEGRATION")
	request.Header.Set("X-Batch-Id", batchID)
	request.Header.Set("X-Batch-Mode", "sync")
	request.Header.Set("X-Historian-Server", "APP")
	request.Header.Set("X-Range-Start", "2026-08-26 09:00:00.0000000")
	request.Header.Set("X-Range-End", "2026-08-26 09:05:00.0000000")
	request.Header.Set("X-Row-Count", "2")
	request.Header.Set("X-Content-SHA256", hex.EncodeToString(digest[:]))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, responseBody
}

func integrationCSV(timestamp time.Time) []byte {
	text := fmt.Sprintf(
		"\xef\xbb\xbfTag,Timestamp,Value,DataType,Flags,SequenceNo,ArchiveStatus\n"+
			"\"TAG/INTEGRATION/A\",\"%s\",\"1.25\",\"Float\",\"\",\"1\",\"\"\n"+
			"\"TAG/INTEGRATION/B\",\"%s\",\"ready\",\"String\",\"\",\"2\",\"\"\n",
		timestamp.Format("2006-01-02 15:04:05.000000"),
		timestamp.Add(time.Second).Format("2006-01-02 15:04:05.000000"))
	return []byte(text)
}
