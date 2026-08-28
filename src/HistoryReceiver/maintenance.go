package main

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"time"
)

const maxArchivePendingRetries = 100

func runReceiverMaintenance(ctx context.Context, config receiverConfig, logger *log.Logger) {
	runMaintenancePass(config, logger, time.Now())
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			runMaintenancePass(config, logger, now)
		}
	}
}

func runMaintenancePass(config receiverConfig, logger *log.Logger, now time.Time) {
	archives := removeOldEntries(config.Archive, now.AddDate(0, 0, -config.ArchiveRetentionDays), true)
	logs := removeOldEntries(config.Logs, now.AddDate(0, 0, -config.LogRetentionDays), false)
	if archives > 0 || logs > 0 {
		logger.Printf("maintenance removed archives=%d logs=%d", archives, logs)
	}
	recovered, retryFailed := retryArchivePending(config, logger)
	if recovered > 0 {
		logger.Printf("maintenance recovered archive_pending=%d", recovered)
	}
	if entries, err := os.ReadDir(config.Rejected); err == nil && len(entries) > 0 {
		logger.Printf("WARNING: rejected batches require attention count=%d", len(entries))
	}
	if entries, err := os.ReadDir(config.ArchivePending); err == nil && len(entries) > 0 {
		logger.Printf(
			"WARNING: archive_pending batches require attention count=%d retry_failed=%d",
			len(entries), retryFailed)
	}
}

func retryArchivePending(config receiverConfig, logger *log.Logger) (int, int) {
	entries, err := os.ReadDir(config.ArchivePending)
	if err != nil {
		if !os.IsNotExist(err) && logger != nil {
			logger.Printf("WARNING: cannot scan archive_pending: %v", err)
		}
		return 0, 0
	}
	if len(entries) == 0 {
		return 0, 0
	}
	if err := os.MkdirAll(config.Archive, 0750); err != nil {
		if logger != nil {
			logger.Printf("WARNING: cannot prepare archive for archive_pending retry: %v", err)
		}
		return 0, len(entries)
	}

	mover := &batchImporter{archive: config.Archive}
	recovered := 0
	failed := 0
	limit := len(entries)
	if limit > maxArchivePendingRetries {
		limit = maxArchivePendingRetries
	}
	for _, entry := range entries[:limit] {
		if !entry.IsDir() {
			failed++
			if logger != nil {
				logger.Printf("WARNING: archive_pending entry is not a directory: %s", entry.Name())
			}
			continue
		}

		source := filepath.Join(config.ArchivePending, entry.Name())
		values, err := readINI(filepath.Join(source, "meta.ini"))
		batchID := ""
		if err == nil {
			batchID = values["Batch.BatchId"]
		}
		if err != nil || !safeID.MatchString(batchID) {
			failed++
			if logger != nil {
				logger.Printf(
					"WARNING: archive_pending metadata invalid entry=%s error=%v",
					entry.Name(), err)
			}
			continue
		}

		if err := mover.moveToArchive(source, batchID); err != nil {
			failed++
			if logger != nil {
				logger.Printf(
					"WARNING: archive_pending retry failed batch=%s entry=%s error=%v",
					batchID, entry.Name(), err)
			}
			continue
		}
		recovered++
	}
	if len(entries) > limit && logger != nil {
		logger.Printf(
			"maintenance deferred archive_pending=%d limit=%d",
			len(entries)-limit,
			maxArchivePendingRetries)
	}
	return recovered, failed
}

func removeOldEntries(root string, cutoff time.Time, directories bool) int {
	entries, err := os.ReadDir(root)
	if err != nil {
		return 0
	}
	removed := 0
	for _, entry := range entries {
		if entry.IsDir() != directories {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.ModTime().Before(cutoff) {
			continue
		}
		path := filepath.Join(root, entry.Name())
		if err := os.RemoveAll(path); err == nil {
			removed++
		}
	}
	return removed
}
