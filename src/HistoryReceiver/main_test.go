package main

import (
	"bytes"
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
)

func TestBatchCommitAndIdempotentRetry(t *testing.T) {
	server, root := newTestReceiver(t)
	body := testCSV()
	batchID := "DCS-APP-01_20260826_100000_abc123"

	for attempt := 0; attempt < 2; attempt++ {
		response := sendTestBatch(t, server, batchID, body, hashBytes(body), 2, "test-secret")
		if response.Code != http.StatusOK {
			t.Fatalf("attempt %d: status=%d body=%s", attempt+1, response.Code, response.Body.String())
		}
		var ack ackResponse
		if err := json.Unmarshal(response.Body.Bytes(), &ack); err != nil {
			t.Fatal(err)
		}
		if !ack.OK || !ack.Committed || ack.CommitLevel != "inbox" ||
			ack.BatchID != batchID || ack.ReceivedRows != 2 {
			t.Fatalf("unexpected ACK: %+v", ack)
		}
	}

	entries, err := os.ReadDir(filepath.Join(root, "inbox"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected one inbox batch, got %d", len(entries))
	}
}

func TestRejectsBadHashAndBadAuthentication(t *testing.T) {
	server, root := newTestReceiver(t)
	body := testCSV()

	badHash := sendTestBatch(t, server, "batch_bad_hash", body, hashBytes([]byte("different")), 2, "test-secret")
	if badHash.Code != http.StatusBadRequest {
		t.Fatalf("bad hash status=%d body=%s", badHash.Code, badHash.Body.String())
	}

	badAuth := sendTestBatch(t, server, "batch_bad_auth", body, hashBytes(body), 2, "wrong-secret")
	if badAuth.Code != http.StatusUnauthorized {
		t.Fatalf("bad auth status=%d body=%s", badAuth.Code, badAuth.Body.String())
	}

	entries, err := os.ReadDir(filepath.Join(root, "inbox"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("rejected batches must not enter inbox")
	}
}

func TestBatchIDCannotBeReusedWithDifferentContent(t *testing.T) {
	server, _ := newTestReceiver(t)
	body := testCSV()
	batchID := "same_batch_id"

	first := sendTestBatch(t, server, batchID, body, hashBytes(body), 2, "test-secret")
	if first.Code != http.StatusOK {
		t.Fatalf("first upload failed: %s", first.Body.String())
	}

	other := []byte("Tag,Timestamp,Value,DataType,Flags,SequenceNo,ArchiveStatus\n\"TAG/C\",\"2026-08-26 09:00:02.0000000\",\"3\",\"Float\",\"\",\"\",\"\"\n")
	second := sendTestBatch(t, server, batchID, other, hashBytes(other), 1, "test-secret")
	if second.Code != http.StatusConflict {
		t.Fatalf("expected conflict, got %d body=%s", second.Code, second.Body.String())
	}
}

func TestSynchronousInvalidPayloadReturnsBadRequest(t *testing.T) {
	server, root := newTestReceiver(t)
	server.config.SynchronousCommit = true
	server.importer = &batchImporter{
		inbox:    server.config.Inbox,
		archive:  server.config.Archive,
		rejected: server.config.Rejected,
		timezone: time.FixedZone("Asia/Shanghai", 8*60*60),
	}
	body := []byte("Tag,Timestamp,Value,DataType,Flags,SequenceNo,ArchiveStatus\n" +
		"\"TAG/A\",\"not-a-time\",\"1.25\",\"Float\",\"\",\"\",\"\"\n")
	response := sendTestBatch(t, server, "invalid_sync_payload", body, hashBytes(body), 1, "test-secret")
	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected bad request, got %d body=%s", response.Code, response.Body.String())
	}
	if entries, err := os.ReadDir(filepath.Join(root, "rejected")); err != nil {
		t.Fatal(err)
	} else if len(entries) != 1 {
		t.Fatalf("expected one rejected payload, got %d", len(entries))
	}
}

func TestHealthReturnsServiceUnavailableWhenDatabaseIsDown(t *testing.T) {
	server, _ := newTestReceiver(t)
	server.config.PostgresEnabled = true
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()
	server.routes().ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected health 503, got %d body=%s", response.Code, response.Body.String())
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["ok"] != false || payload["database_ok"] != false {
		t.Fatalf("unexpected health payload: %v", payload)
	}
}

func TestIdempotentRetryValidatesBody(t *testing.T) {
	server, _ := newTestReceiver(t)
	body := testCSV()
	batchID := "retry_body_check"
	if response := sendTestBatch(t, server, batchID, body, hashBytes(body), 2, "test-secret"); response.Code != http.StatusOK {
		t.Fatalf("first upload failed: %s", response.Body.String())
	}

	changed := append([]byte(nil), body...)
	changed[len(changed)-2] = '9'
	response := sendTestBatch(t, server, batchID, changed, hashBytes(body), 2, "test-secret")
	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected bad request, got %d body=%s", response.Code, response.Body.String())
	}
}

func TestArchivedBatchRemainsIdempotent(t *testing.T) {
	server, root := newTestReceiver(t)
	body := testCSV()
	batchID := "archived_batch"
	if response := sendTestBatch(t, server, batchID, body, hashBytes(body), 2, "test-secret"); response.Code != http.StatusOK {
		t.Fatalf("first upload failed: %s", response.Body.String())
	}
	if err := os.Rename(filepath.Join(root, "inbox", batchID), filepath.Join(root, "archive", batchID)); err != nil {
		t.Fatal(err)
	}
	response := sendTestBatch(t, server, batchID, body, hashBytes(body), 2, "test-secret")
	if response.Code != http.StatusOK {
		t.Fatalf("archived retry failed: status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestMoveToArchiveDoesNotCreateDuplicateDirectory(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "archive")
	source := filepath.Join(root, "staging")
	batchID := "already_archived"
	if err := os.MkdirAll(filepath.Join(archive, batchID), 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(source, 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "data.csv"), []byte("payload"), 0640); err != nil {
		t.Fatal(err)
	}

	importer := &batchImporter{archive: archive}
	if err := importer.moveToArchive(source, batchID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(source); !os.IsNotExist(err) {
		t.Fatalf("duplicate source was not removed: %v", err)
	}
	entries, err := os.ReadDir(archive)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != batchID {
		t.Fatalf("expected one archive directory, got %v", entries)
	}
}

func TestReceiverTimeoutDefaultsAreOrdered(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "receiver.ini")
	content := strings.Join([]string{
		"[Server]",
		"Listen=127.0.0.1:8080",
		"ApiKey=test-secret",
		"MaxBodyBytes=1024",
		"WriteTimeoutSeconds=60",
		"",
		"[PostgreSQL]",
		"Enabled=false",
		"SynchronousCommit=false",
		"",
	}, "\n")
	if err := os.WriteFile(configPath, []byte(content), 0640); err != nil {
		t.Fatal(err)
	}
	config, err := loadReceiverConfig(configPath, root)
	if err != nil {
		t.Fatal(err)
	}
	if config.WriteTimeout != 60*time.Second || config.ImportTimeout != 45*time.Second ||
		config.StagingDurability != stagingDurabilityFull {
		t.Fatalf("unexpected defaults: write=%s import=%s staging_durability=%s", config.WriteTimeout, config.ImportTimeout, config.StagingDurability)
	}
}

func TestReceiverRejectsInvalidStagingDurability(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "receiver.ini")
	content := strings.Join([]string{
		"[Server]",
		"Listen=127.0.0.1:8080",
		"ApiKey=test-secret",
		"MaxBodyBytes=1024",
		"",
		"[Files]",
		"StagingDurability=off",
		"",
		"[PostgreSQL]",
		"Enabled=false",
		"SynchronousCommit=false",
		"",
	}, "\n")
	if err := os.WriteFile(configPath, []byte(content), 0640); err != nil {
		t.Fatal(err)
	}
	if _, err := loadReceiverConfig(configPath, root); err == nil ||
		!strings.Contains(err.Error(), "StagingDurability") {
		t.Fatalf("expected staging durability validation error, got %v", err)
	}
}

func TestMaintenanceRetriesArchivePending(t *testing.T) {
	root := t.TempDir()
	config := receiverConfig{
		Archive:              filepath.Join(root, "archive"),
		ArchivePending:       filepath.Join(root, "archive_pending"),
		Logs:                 filepath.Join(root, "logs"),
		Rejected:             filepath.Join(root, "rejected"),
		ArchiveRetentionDays: 30,
		LogRetentionDays:     30,
	}
	for _, directory := range []string{config.Archive, config.ArchivePending, config.Logs, config.Rejected} {
		if err := os.MkdirAll(directory, 0750); err != nil {
			t.Fatal(err)
		}
	}

	batchID := "pending_batch_with_underscores"
	pendingDir := filepath.Join(config.ArchivePending, batchID+"_123456789")
	if err := os.Mkdir(pendingDir, 0750); err != nil {
		t.Fatal(err)
	}
	meta := "[Batch]\nBatchId=" + batchID + "\nCollectorId=DCS-APP-01\nRows=0\nSha256=" + strings.Repeat("a", 64) + "\n"
	if err := os.WriteFile(filepath.Join(pendingDir, "meta.ini"), []byte(meta), 0640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pendingDir, "data.csv"), []byte("payload"), 0640); err != nil {
		t.Fatal(err)
	}

	runMaintenancePass(config, log.New(io.Discard, "", 0), time.Now())
	if _, err := os.Stat(pendingDir); !os.IsNotExist(err) {
		t.Fatalf("recovered archive_pending directory still exists: %v", err)
	}
	if _, err := os.Stat(filepath.Join(config.Archive, batchID)); err != nil {
		t.Fatalf("recovered archive batch is missing: %v", err)
	}

	failedID := "pending_batch_that_stays"
	failedDir := filepath.Join(config.ArchivePending, failedID+"_987654321")
	if err := os.Mkdir(failedDir, 0750); err != nil {
		t.Fatal(err)
	}
	failedMeta := "[Batch]\nBatchId=" + failedID + "\n"
	if err := os.WriteFile(filepath.Join(failedDir, "meta.ini"), []byte(failedMeta), 0640); err != nil {
		t.Fatal(err)
	}
	archiveBlocker := filepath.Join(root, "archive-blocker")
	if err := os.WriteFile(archiveBlocker, []byte("not a directory"), 0640); err != nil {
		t.Fatal(err)
	}
	config.Archive = archiveBlocker
	runMaintenancePass(config, log.New(io.Discard, "", 0), time.Now())
	if _, err := os.Stat(failedDir); err != nil {
		t.Fatalf("failed archive_pending entry must remain for a later retry: %v", err)
	}
}

func TestReceiverRejectsImportTimeoutAtWriteTimeout(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "receiver.ini")
	content := strings.Join([]string{
		"[Server]",
		"Listen=127.0.0.1:8080",
		"ApiKey=test-secret",
		"MaxBodyBytes=1024",
		"WriteTimeoutSeconds=60",
		"",
		"[PostgreSQL]",
		"Enabled=true",
		"SynchronousCommit=true",
		"Host=127.0.0.1",
		"Port=5432",
		"Database=test",
		"User=test",
		"Password=test",
		"ImportTimeoutSeconds=60",
		"",
	}, "\n")
	if err := os.WriteFile(configPath, []byte(content), 0640); err != nil {
		t.Fatal(err)
	}
	if _, err := loadReceiverConfig(configPath, root); err == nil ||
		!strings.Contains(err.Error(), "must be less than") {
		t.Fatalf("expected timeout ordering error, got %v", err)
	}
}

func TestStageAndParseBodyHashesAndParsesInOnePass(t *testing.T) {
	root := t.TempDir()
	body := testCSV()
	path := filepath.Join(root, "data.csv")
	reader := &countingReader{source: bytes.NewReader(body)}
	staged, err := stageAndParseBody(
		path,
		reader,
		int64(len(body)),
		"DCS-APP-01",
		time.FixedZone("Asia/Shanghai", 8*60*60))
	if err != nil {
		t.Fatal(err)
	}
	if staged.BodyBytes != int64(len(body)) || reader.bytes != int64(len(body)) {
		t.Fatalf("body was not fully staged: staged=%d read=%d", staged.BodyBytes, reader.bytes)
	}
	if staged.ActualHash != hashBytes(body) || len(staged.Rows) != 2 {
		t.Fatalf("unexpected staged result: hash=%s rows=%d", staged.ActualHash, len(staged.Rows))
	}
	if staged.Rows[0].Tag != "TAG/A" || !staged.Rows[0].HasValueDouble {
		t.Fatalf("CSV conversion did not produce import rows: %+v", staged.Rows)
	}
	stored, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, body) {
		t.Fatal("staging file differs from the received body")
	}
}

func TestStageAndParseBodyEnforcesMaximum(t *testing.T) {
	root := t.TempDir()
	body := testCSV()
	_, err := stageAndParseBody(
		filepath.Join(root, "data.csv"),
		bytes.NewReader(body),
		int64(len(body)-1),
		"DCS-APP-01",
		time.FixedZone("Asia/Shanghai", 8*60*60))
	if err != errBodyTooLarge {
		t.Fatalf("expected body-too-large error, got %v", err)
	}
}

func TestStageAndParseBodySupportsBufferedDurability(t *testing.T) {
	root := t.TempDir()
	body := testCSV()
	staged, err := stageAndParseBodyWithConfig(
		filepath.Join(root, "data.csv"),
		bytes.NewReader(body),
		int64(len(body)),
		"DCS-APP-01",
		time.FixedZone("Asia/Shanghai", 8*60*60),
		2,
		stagingDurabilityBuffered)
	if err != nil {
		t.Fatal(err)
	}
	if len(staged.Rows) != 2 || cap(staged.Rows) != 2 {
		t.Fatalf("buffered staging did not use expected row capacity: len=%d cap=%d", len(staged.Rows), cap(staged.Rows))
	}
	if staged.ActualHash != hashBytes(body) {
		t.Fatalf("buffered staging produced wrong hash: %s", staged.ActualHash)
	}
}

func TestRetryArchivePendingIsLimitedPerPass(t *testing.T) {
	root := t.TempDir()
	config := receiverConfig{
		Archive:        filepath.Join(root, "archive"),
		ArchivePending: filepath.Join(root, "archive_pending"),
	}
	if err := os.MkdirAll(config.Archive, 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(config.ArchivePending, 0750); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < maxArchivePendingRetries+1; index++ {
		batchID := fmt.Sprintf("pending_%03d", index)
		pendingDir := filepath.Join(config.ArchivePending, batchID+"_retry")
		if err := os.Mkdir(pendingDir, 0750); err != nil {
			t.Fatal(err)
		}
		meta := "[Batch]\nBatchId=" + batchID + "\n"
		if err := os.WriteFile(filepath.Join(pendingDir, "meta.ini"), []byte(meta), 0640); err != nil {
			t.Fatal(err)
		}
	}

	recovered, failed := retryArchivePending(config, log.New(io.Discard, "", 0))
	if recovered != maxArchivePendingRetries || failed != 0 {
		t.Fatalf("unexpected archive_pending retry result: recovered=%d failed=%d", recovered, failed)
	}
	pendingEntries, err := os.ReadDir(config.ArchivePending)
	if err != nil {
		t.Fatal(err)
	}
	archiveEntries, err := os.ReadDir(config.Archive)
	if err != nil {
		t.Fatal(err)
	}
	if len(pendingEntries) != 1 || len(archiveEntries) != maxArchivePendingRetries {
		t.Fatalf("archive_pending retry exceeded per-pass limit: pending=%d archive=%d", len(pendingEntries), len(archiveEntries))
	}
}

func newTestReceiver(t *testing.T) (*receiverServer, string) {
	t.Helper()
	root := t.TempDir()
	config := receiverConfig{
		APIKey:         "test-secret",
		MaxBodyBytes:   1024 * 1024,
		Inbox:          filepath.Join(root, "inbox"),
		Archive:        filepath.Join(root, "archive"),
		ArchivePending: filepath.Join(root, "archive_pending"),
		Staging:        filepath.Join(root, "staging"),
		Rejected:       filepath.Join(root, "rejected"),
		Logs:           filepath.Join(root, "logs"),
	}
	for _, directory := range []string{config.Inbox, config.Archive, config.ArchivePending, config.Staging, config.Rejected, config.Logs} {
		if err := os.MkdirAll(directory, 0750); err != nil {
			t.Fatal(err)
		}
	}
	return &receiverServer{config: config, logger: log.New(io.Discard, "", 0)}, root
}

func sendTestBatch(
	t *testing.T,
	server *receiverServer,
	batchID string,
	body []byte,
	hash string,
	rows int,
	secret string,
) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/api/history/batch", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+secret)
	request.Header.Set("X-Collector-Id", "DCS-APP-01")
	request.Header.Set("X-Batch-Id", batchID)
	request.Header.Set("X-Batch-Mode", "sync")
	request.Header.Set("X-Historian-Server", "APP")
	request.Header.Set("X-Range-Start", "2026-08-26 09:00:00.0000000")
	request.Header.Set("X-Range-End", "2026-08-26 09:05:00.0000000")
	request.Header.Set("X-Row-Count", strconv.Itoa(rows))
	request.Header.Set("X-Content-SHA256", hash)
	response := httptest.NewRecorder()
	server.routes().ServeHTTP(response, request)
	return response
}

func testCSV() []byte {
	return []byte("\xef\xbb\xbfTag,Timestamp,Value,DataType,Flags,SequenceNo,ArchiveStatus\n" +
		"\"TAG/A\",\"2026-08-26 09:00:00.1000000\",\"1.25\",\"Float\",\"\",\"\",\"\"\n" +
		"\"TAG/B\",\"2026-08-26 09:00:01.2000000\",\"2.50\",\"Float\",\"\",\"\",\"\"\n")
}

func hashBytes(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

type countingReader struct {
	source *bytes.Reader
	bytes  int64
}

func (r *countingReader) Read(buffer []byte) (int, error) {
	count, err := r.source.Read(buffer)
	r.bytes += int64(count)
	return count, err
}
