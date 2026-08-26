package main

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"time"
)

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
	if entries, err := os.ReadDir(config.Rejected); err == nil && len(entries) > 0 {
		logger.Printf("WARNING: rejected batches require attention count=%d", len(entries))
	}
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
