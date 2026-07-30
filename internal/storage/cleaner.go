package storage

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ============================================================================
// STRUCT: CleanerConfig
// Description: Parameters for background log retention and compaction.
// ============================================================================
type CleanerConfig struct {
	RetentionMs     time.Duration // Time-based retention threshold. Pass <= 0 to disable.
	RetentionBytes  int64         // Size-based retention limit per partition log in bytes. Pass <= 0 to disable.
	CleanerInterval time.Duration // Time interval between cleaner execution cycles.
}

// DefaultCleanerConfig returns standard production default config (7 days retention).
func DefaultCleanerConfig() CleanerConfig {
	return CleanerConfig{
		RetentionMs:     168 * time.Hour,
		RetentionBytes:  -1,
		CleanerInterval: 60 * time.Second,
	}
}

// PartitionSupplier returns active PartitionLog instances map.
type PartitionSupplier func() map[string]*PartitionLog

// ============================================================================
// STRUCT: CleanerWorker
// Description: Background goroutine executing automated log segment retention
//              deletion and key-based log compaction routines.
// ============================================================================
type CleanerWorker struct {
	supplier PartitionSupplier
	config   CleanerConfig
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// NewCleanerWorker initializes a new CleanerWorker.
func NewCleanerWorker(supplier PartitionSupplier, cfg CleanerConfig) *CleanerWorker {
	if cfg.CleanerInterval <= 0 {
		cfg.CleanerInterval = 60 * time.Second
	}
	return &CleanerWorker{
		supplier: supplier,
		config:   cfg,
		stopChan: make(chan struct{}),
	}
}

// Start launches the background cleaner loop.
func (cw *CleanerWorker) Start() {
	cw.wg.Add(1)
	go cw.loop()
	log.Printf("[CleanerWorker] Retention & Compaction Worker started (interval: %v)\n", cw.config.CleanerInterval)
}

// Stop gracefully terminates the background cleaner loop.
func (cw *CleanerWorker) Stop() {
	close(cw.stopChan)
	cw.wg.Wait()
	log.Println("[CleanerWorker] Retention & Compaction Worker stopped.")
}

func (cw *CleanerWorker) loop() {
	defer cw.wg.Done()
	ticker := time.NewTicker(cw.config.CleanerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cw.stopChan:
			return
		case <-ticker.C:
			cw.RunCleanCycle()
		}
	}
}

// RunCleanCycle executes retention and compaction across all active partition logs.
func (cw *CleanerWorker) RunCleanCycle() {
	if cw.supplier == nil {
		return
	}
	partitions := cw.supplier()
	for key, pl := range partitions {
		cw.cleanTimeRetention(key, pl)
		cw.cleanSizeRetention(key, pl)
	}
}

// cleanTimeRetention purges segments older than RetentionMs.
func (cw *CleanerWorker) cleanTimeRetention(key string, pl *PartitionLog) {
	if cw.config.RetentionMs <= 0 {
		return
	}

	nonActive := pl.GetNonActiveSegments()
	if len(nonActive) == 0 {
		return
	}

	now := time.Now()
	for _, seg := range nonActive {
		logPath := seg.LogFilePath()
		if logPath == "" {
			continue
		}
		info, err := os.Stat(logPath)
		if err != nil {
			continue
		}
		if now.Sub(info.ModTime()) > cw.config.RetentionMs {
			removed, err := pl.RemoveOldestSegment()
			if removed {
				log.Printf("[CleanerWorker] Retention-Time: Purged expired log segment from %s (modTime: %v, err: %v)\n", key, info.ModTime(), err)
			}
		}
	}
}

// cleanSizeRetention purges oldest segments if total partition size exceeds RetentionBytes.
func (cw *CleanerWorker) cleanSizeRetention(key string, pl *PartitionLog) {
	if cw.config.RetentionBytes <= 0 {
		return
	}

	for {
		_, totalSize := pl.Stats()
		if totalSize <= cw.config.RetentionBytes {
			break
		}

		removed, err := pl.RemoveOldestSegment()
		if !removed || err != nil {
			break
		}
		log.Printf("[CleanerWorker] Retention-Size: Purged oldest segment from %s (total size: %d, max limit: %d)\n", key, totalSize, cw.config.RetentionBytes)
	}
}

// CompactLogSegments performs Key-based Log Compaction on closed segment files,
// retaining only the record with the highest offset for each distinct non-empty Key.
func CompactLogSegments(seg *Segment, outDir string) (int, error) {
	records, err := seg.Read(seg.BaseOffset())
	if err != nil || len(records) == 0 {
		return 0, err
	}

	// 1. Build map of Key -> highest offset record
	keyLatestMap := make(map[string]*Record)
	for _, rec := range records {
		if len(rec.Key) > 0 {
			keyLatestMap[string(rec.Key)] = rec
		}
	}

	if len(keyLatestMap) == 0 {
		return len(records), nil
	}

	// 2. Filter records keeping only latest key occurrences
	compactedRecords := make([]*Record, 0, len(records))
	for _, rec := range records {
		if len(rec.Key) == 0 {
			compactedRecords = append(compactedRecords, rec)
			continue
		}
		if latest, ok := keyLatestMap[string(rec.Key)]; ok && latest.Offset == rec.Offset {
			// Skip tombstone (null value) records during final compaction
			if len(rec.Value) > 0 {
				compactedRecords = append(compactedRecords, rec)
			}
		}
	}

	// 3. Write compacted records to temporary segment file
	tmpName := fmt.Sprintf("compact_%020d.tmp", seg.BaseOffset())
	tmpLogPath := filepath.Join(outDir, tmpName)

	tmpFile, err := os.OpenFile(tmpLogPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpLogPath)
	}()

	for _, rec := range compactedRecords {
		data, err := rec.Marshal()
		if err != nil {
			return 0, err
		}
		if _, err := tmpFile.Write(data); err != nil {
			return 0, err
		}
	}

	return len(compactedRecords), nil
}

// ParseRecordStream helper for reading all records from an io.Reader
func ParseRecordStream(r io.Reader) ([]*Record, error) {
	var records []*Record
	for {
		rec, _, err := ReadRecord(r)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		records = append(records, rec)
	}
	return records, nil
}
