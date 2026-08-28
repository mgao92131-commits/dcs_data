package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
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
	rows, err := importer.readImportRows(path, "DCS-TEST")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected one row, got %d", len(rows))
	}
	if rows[0].Tag != "TAG/A" || rows[0].ValueDouble == nil || *rows[0].ValueDouble != 1.25 || rows[0].ValueText != "1.25" {
		t.Fatalf("unexpected imported row: %+v", rows[0])
	}
	if rows[0].TimeText != "2026-08-26 09:00:00.123456" {
		t.Fatalf("unexpected timestamp: %s", rows[0].TimeText)
	}
}

func TestReadImportRowsAcceptsTextValue(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "data.csv")
	content := "Tag,Timestamp,Value,DataType,Flags,SequenceNo,ArchiveStatus\n" +
		"\"MODE/A\",\"2026-08-26 09:00:00.0000000\",\" RUN \",\"String\",\"\",\"7\",\"Current\"\n"
	if err := os.WriteFile(path, []byte(content), 0640); err != nil {
		t.Fatal(err)
	}
	importer := &batchImporter{timezone: time.FixedZone("Asia/Shanghai", 8*60*60)}
	rows, err := importer.readImportRows(path, "DCS-TEST")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ValueText != " RUN " || rows[0].ValueDouble != nil {
		t.Fatalf("unexpected text row: %+v", rows)
	}
	if rows[0].DataType != "String" || rows[0].SequenceNo != "7" || rows[0].ArchiveStatus != "Current" {
		t.Fatalf("extended fields were not preserved: %+v", rows[0])
	}
}

func TestSampleKeyIsStable(t *testing.T) {
	first := sampleKey("DCS-TEST", "TAG/A", "2026-08-26 09:00:00.1000000", "", "1.25")
	second := sampleKey("DCS-TEST", "TAG/A", "2026-08-26 09:00:00.1000000", "", "1.25")
	different := sampleKey("DCS-TEST", "TAG/A", "2026-08-26 09:00:00.1000000", "", "1.26")
	if first != second || first == different || len(first) != 64 {
		t.Fatal("sample key is not stable")
	}
	withSequence := sampleKey("DCS-TEST", "TAG/A", "2026-08-26 09:00:00.1000000", "7", "1.25")
	changedValue := sampleKey("DCS-TEST", "TAG/A", "2026-08-26 09:00:00.1000000", "7", "1.26")
	if withSequence != changedValue {
		t.Fatal("reliable sequence identity must allow value updates")
	}
}

func TestProcessedSampleKeyIsOrderedByTimestamp(t *testing.T) {
	sequence := "P:InterpolatedValue:10"
	first := sampleKey("DCS-TEST", "TAG/A", "2026-08-28 09:00:00.0000000", sequence, "1.25")
	second := sampleKey("DCS-TEST", "TAG/A", "2026-08-28 09:00:10.0000000", sequence, "1.26")
	changedValue := sampleKey("DCS-TEST", "TAG/A", "2026-08-28 09:00:00.0000000", sequence, "9.99")
	if len(first) != 64 || first >= second {
		t.Fatalf("processed keys must be 64 characters and time ordered: %q %q", first, second)
	}
	if first != changedValue {
		t.Fatal("processed key identity must not depend on value")
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
		"CollectorId=DCS-TEST",
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

func TestInvalidBatchIsClassifiedForPermanentRejection(t *testing.T) {
	root := t.TempDir()
	batchID := "invalid_timestamp_batch"
	directory := filepath.Join(root, batchID)
	if err := os.Mkdir(directory, 0750); err != nil {
		t.Fatal(err)
	}
	data := []byte("Tag,Timestamp,Value,DataType,Flags,SequenceNo,ArchiveStatus\n" +
		"TAG/A,not-a-time,1.25,Float,,,\n")
	digest := sha256.Sum256(data)
	meta := strings.Join([]string{
		"[Batch]",
		"BatchId=" + batchID,
		"CollectorId=DCS-TEST",
		"Rows=1",
		"Sha256=" + hex.EncodeToString(digest[:]),
		"",
	}, "\n")
	if err := os.WriteFile(filepath.Join(directory, "data.csv"), data, 0640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "meta.ini"), []byte(meta), 0640); err != nil {
		t.Fatal(err)
	}
	importer := &batchImporter{timezone: time.FixedZone("Asia/Shanghai", 8*60*60)}
	if _, err := importer.loadBatch(directory); err == nil || !errors.Is(err, errInvalidBatch) {
		t.Fatalf("expected permanent invalid-batch classification, got %v", err)
	}
}

func TestImportOnceQuarantinesInvalidBatch(t *testing.T) {
	root := t.TempDir()
	batchID := "invalid_import_once"
	inbox := filepath.Join(root, "inbox")
	rejected := filepath.Join(root, "rejected")
	directory := filepath.Join(inbox, batchID)
	if err := os.MkdirAll(directory, 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(rejected, 0750); err != nil {
		t.Fatal(err)
	}
	data := []byte("Tag,Timestamp,Value,DataType,Flags,SequenceNo,ArchiveStatus\n" +
		"TAG/A,not-a-time,1.25,Float,,,\n")
	digest := sha256.Sum256(data)
	meta := strings.Join([]string{
		"[Batch]",
		"BatchId=" + batchID,
		"CollectorId=DCS-TEST",
		"Rows=1",
		"Sha256=" + hex.EncodeToString(digest[:]),
		"",
	}, "\n")
	if err := os.WriteFile(filepath.Join(directory, "data.csv"), data, 0640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "meta.ini"), []byte(meta), 0640); err != nil {
		t.Fatal(err)
	}
	importer := &batchImporter{
		inbox:             inbox,
		rejected:          rejected,
		maxBatchesPerPass: 10,
		timezone:          time.FixedZone("Asia/Shanghai", 8*60*60),
		logger:            log.New(io.Discard, "", 0),
	}
	_, failed, err := importer.importOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if failed != 1 {
		t.Fatalf("expected one failed/quarantined batch, got %d", failed)
	}
	if entries, readErr := os.ReadDir(inbox); readErr != nil {
		t.Fatal(readErr)
	} else if len(entries) != 0 {
		t.Fatalf("invalid batch remained in inbox: %v", entries)
	}
	if entries, readErr := os.ReadDir(rejected); readErr != nil {
		t.Fatal(readErr)
	} else if len(entries) != 1 {
		t.Fatalf("expected one rejected batch, got %d", len(entries))
	}
}
