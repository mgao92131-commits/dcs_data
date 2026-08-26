package main

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReadImportRowsUsesOnlyCoreFields(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "data.csv")
	content := "\ufeffTag,Timestamp,Value,DataType,Flags,SequenceNo,ArchiveStatus\n" +
		"\"TAG/A\",\"2026-08-26 09:00:00.1234567\",\"1.25\",\"Float\",\"HistoryHole\",\"\",\"\"\n"
	if err := os.WriteFile(path, []byte(content), 0640); err != nil {
		t.Fatal(err)
	}
	importer := &batchImporter{
		timezone: time.FixedZone("Asia/Shanghai", 8*60*60),
		logger:   log.New(io.Discard, "", 0),
	}
	rows, err := importer.readImportRows(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected one row, got %d", len(rows))
	}
	if rows[0].Tag != "TAG/A" || rows[0].Value != 1.25 {
		t.Fatalf("unexpected imported row: %+v", rows[0])
	}
	if rows[0].TimeText != "2026-08-26 09:00:00.123456" {
		t.Fatalf("unexpected timestamp: %s", rows[0].TimeText)
	}
}

func TestSampleKeyIsStable(t *testing.T) {
	first := sampleKey("TAG/A", "2026-08-26 09:00:00.1000000", "1.25")
	second := sampleKey("TAG/A", "2026-08-26 09:00:00.1000000", "1.25")
	different := sampleKey("TAG/A", "2026-08-26 09:00:00.1000000", "1.26")
	if first != second || first == different || len(first) != 64 {
		t.Fatal("sample key is not stable")
	}
}

func TestLoadBatchRejectsChangedCSV(t *testing.T) {
	root := t.TempDir()
	batchID := "test_batch_001"
	directory := filepath.Join(root, batchID)
	if err := os.Mkdir(directory, 0750); err != nil {
		t.Fatal(err)
	}
	data := []byte("Tag,Timestamp,Value,DataType,Flags,SequenceNo,ArchiveStatus\n")
	digest := sha256.Sum256(data)
	hash := hex.EncodeToString(digest[:])
	if err := os.WriteFile(filepath.Join(directory, "data.csv"), append(data, []byte("changed")...), 0640); err != nil {
		t.Fatal(err)
	}
	meta := strings.Join([]string{
		"[Batch]",
		"BatchId=" + batchID,
		"Rows=0",
		"Sha256=" + hash,
		"",
	}, "\n")
	if err := os.WriteFile(filepath.Join(directory, "meta.ini"), []byte(meta), 0640); err != nil {
		t.Fatal(err)
	}
	importer := &batchImporter{timezone: time.FixedZone("Asia/Shanghai", 8*60*60)}
	if _, err := importer.loadBatch(directory); err == nil || !strings.Contains(err.Error(), "SHA-256") {
		t.Fatalf("expected SHA-256 error, got %v", err)
	}
}
