package storage

import (
	"io"
	"log"
	"os"
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

	// CompactionEnabled turns on key-based log compaction (see
	// CompactLogSegments) for every closed segment each cycle. Off by
	// default: compaction permanently drops superseded records and
	// tombstones, changing what a consumer re-reading from an old offset
	// sees, so it must be an explicit, informed opt-in rather than a
	// default-on background behavior.
	CompactionEnabled bool
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
		if cw.config.CompactionEnabled {
			cw.compactPartition(key, pl)
		}
	}
}

// compactPartition runs key-based compaction across pl's closed segments
// and logs how many were actually rewritten.
func (cw *CleanerWorker) compactPartition(key string, pl *PartitionLog) {
	rewritten, err := pl.CompactSegments()
	if err != nil {
		log.Printf("[CleanerWorker] Compaction: error compacting %s: %v\n", key, err)
		return
	}
	if rewritten > 0 {
		log.Printf("[CleanerWorker] Compaction: rewrote %d closed segment(s) in %s\n", rewritten, key)
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

// CompactLogSegments performs Key-based Log Compaction on a closed segment,
// retaining only the record whose offset matches keyLatestOffset for its
// key and dropping tombstones (null-value records). Unlike its original
// implementation, the result is actually persisted: when at least one
// record is removed, the segment's own .log/.index files are atomically
// rewritten in place via Segment.replaceContents — previously, the
// compacted output was written to a temp file that a deferred cleanup
// deleted before this function even returned, so compaction had no
// observable effect no matter how it was invoked. Returns the number of
// records kept.
//
// keyLatestOffset must map every key to that key's highest offset across
// the WHOLE partition, not just this segment — see PartitionLog.
// CompactSegments, which builds it by scanning every segment (including the
// active one) before compacting any closed segment. Without a global map, a
// key whose latest occurrence lives in a DIFFERENT segment than the one
// being compacted would be incorrectly kept here too, since nothing within
// a single segment can tell that a newer value exists elsewhere.
//
// outDir MUST be seg's own directory. The caller MUST hold a lock excluding
// concurrent Read/Append against seg for the duration of this call — see
// PartitionLog.CompactSegments, the only production call site.
func CompactLogSegments(seg *Segment, outDir string, keyLatestOffset map[string]uint64) (int, error) {
	records, err := seg.Read(seg.BaseOffset())
	if err != nil || len(records) == 0 {
		return 0, err
	}

	compactedRecords := make([]*Record, 0, len(records))
	for _, rec := range records {
		if len(rec.Key) == 0 {
			compactedRecords = append(compactedRecords, rec)
			continue
		}
		if latestOffset, ok := keyLatestOffset[string(rec.Key)]; ok && latestOffset == rec.Offset {
			// Skip tombstone (null value) records during final compaction
			if len(rec.Value) > 0 {
				compactedRecords = append(compactedRecords, rec)
			}
		}
		// else: a newer occurrence of this key exists elsewhere in the
		// partition, so this one is superseded and gets dropped.
	}

	// Nothing was actually removed — skip the file rewrite entirely.
	if len(compactedRecords) == len(records) {
		return len(compactedRecords), nil
	}

	if err := seg.replaceContents(outDir, compactedRecords); err != nil {
		return 0, err
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
