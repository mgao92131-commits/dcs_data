package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
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
		if !ack.OK || !ack.Committed || ack.BatchID != batchID || ack.ReceivedRows != 2 {
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

func newTestReceiver(t *testing.T) (*receiverServer, string) {
	t.Helper()
	root := t.TempDir()
	config := receiverConfig{
		APIKey:       "test-secret",
		MaxBodyBytes: 1024 * 1024,
		Inbox:        filepath.Join(root, "inbox"),
		Archive:      filepath.Join(root, "archive"),
		Staging:      filepath.Join(root, "staging"),
		Rejected:     filepath.Join(root, "rejected"),
		Logs:         filepath.Join(root, "logs"),
	}
	for _, directory := range []string{config.Inbox, config.Archive, config.Staging, config.Rejected, config.Logs} {
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
