package main

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMaintenanceRemovesOnlyExpiredArchiveAndLogs(t *testing.T) {
	root := t.TempDir()
	config := receiverConfig{
		Archive:              filepath.Join(root, "archive"),
		Logs:                 filepath.Join(root, "logs"),
		Rejected:             filepath.Join(root, "rejected"),
		ArchiveRetentionDays: 30,
		LogRetentionDays:     30,
	}
	for _, directory := range []string{config.Archive, config.Logs, config.Rejected} {
		if err := os.MkdirAll(directory, 0750); err != nil {
			t.Fatal(err)
		}
	}
	oldArchive := filepath.Join(config.Archive, "old")
	newArchive := filepath.Join(config.Archive, "new")
	if err := os.Mkdir(oldArchive, 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(newArchive, 0750); err != nil {
		t.Fatal(err)
	}
	oldLog := filepath.Join(config.Logs, "old.log")
	if err := os.WriteFile(oldLog, []byte("old"), 0640); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	oldTime := now.AddDate(0, 0, -31)
	if err := os.Chtimes(oldArchive, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(oldLog, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}

	runMaintenancePass(config, log.New(io.Discard, "", 0), now)
	if _, err := os.Stat(oldArchive); !os.IsNotExist(err) {
		t.Fatalf("old archive was not removed: %v", err)
	}
	if _, err := os.Stat(oldLog); !os.IsNotExist(err) {
		t.Fatalf("old log was not removed: %v", err)
	}
	if _, err := os.Stat(newArchive); err != nil {
		t.Fatalf("new archive must remain: %v", err)
	}
}
