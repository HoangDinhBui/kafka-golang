package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCleanerWorker_TimeRetention(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "kafka_cleaner_time_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	partDir := filepath.Join(tempDir, "test.topic-0")
	// Create partition log with small 100 byte segment threshold to force multiple segments
	pl, err := NewPartitionLog(partDir, 100, 10)
	if err != nil {
		t.Fatalf("Failed to create partition log: %v", err)
	}

	// Append 10 messages across multiple segments
	for i := 0; i < 10; i++ {
		rec := &Record{
			Timestamp: time.Now().UnixNano(),
			Key:       []byte(fmt.Sprintf("key-%d", i)),
			Value:     []byte(fmt.Sprintf("value-%d-data-payload-long-string", i)),
		}
		if err := pl.Append(rec); err != nil {
			t.Fatalf("Failed to append record %d: %v", i, err)
		}
	}

	segCount, totalSize := pl.Stats()
	if segCount <= 1 {
		t.Fatalf("Expected multiple segments created, got %d", segCount)
	}

	// Supplier returning active partition log
	supplier := func() map[string]*PartitionLog {
		return map[string]*PartitionLog{
			"test.topic-0": pl,
		}
	}

	// Config with 1ms retention (forces all closed segments to be expired)
	cfg := CleanerConfig{
		RetentionMs:     1 * time.Millisecond,
		RetentionBytes:  -1,
		CleanerInterval: 10 * time.Millisecond,
	}

	cw := NewCleanerWorker(supplier, cfg)
	time.Sleep(10 * time.Millisecond) // Let modification time elapse
	cw.RunCleanCycle()

	newSegCount, newTotalSize := pl.Stats()
	if newSegCount >= segCount {
		t.Errorf("Expected segment count to decrease after time retention, before: %d, after: %d", segCount, newSegCount)
	}
	if newTotalSize >= totalSize {
		t.Errorf("Expected total size to decrease, before: %d, after: %d", totalSize, newTotalSize)
	}
}

func TestCleanerWorker_SizeRetention(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "kafka_cleaner_size_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	partDir := filepath.Join(tempDir, "size.topic-0")
	pl, err := NewPartitionLog(partDir, 100, 10)
	if err != nil {
		t.Fatalf("Failed to create partition log: %v", err)
	}

	// Append 15 messages
	for i := 0; i < 15; i++ {
		rec := &Record{
			Timestamp: time.Now().UnixNano(),
			Key:       []byte(fmt.Sprintf("k-%d", i)),
			Value:     []byte(fmt.Sprintf("v-%d-padding-data-for-size", i)),
		}
		_ = pl.Append(rec)
	}

	segCount, initialSize := pl.Stats()

	supplier := func() map[string]*PartitionLog {
		return map[string]*PartitionLog{
			"size.topic-0": pl,
		}
	}

	// Config enforcing size limit smaller than initialSize
	cfg := CleanerConfig{
		RetentionMs:     -1,
		RetentionBytes:  initialSize / 2, // Enforce 50% limit
		CleanerInterval: 10 * time.Millisecond,
	}

	cw := NewCleanerWorker(supplier, cfg)
	cw.RunCleanCycle()

	_, finalSize := pl.Stats()
	if finalSize > initialSize/2 {
		t.Errorf("Expected size retention to reduce storage below %d, got %d (initial: %d, segments: %d)",
			initialSize/2, finalSize, initialSize, segCount)
	}
}

func TestCompactLogSegments(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "kafka_compact_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	seg, err := NewSegment(tempDir, 0, 10000, 100)
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}
	defer seg.Close()

	// Append duplicate key records: key1 -> v1, key2 -> v2, key1 -> v1-updated
	rec1 := &Record{Offset: 0, Timestamp: 100, Key: []byte("user-1"), Value: []byte("address-old")}
	rec2 := &Record{Offset: 1, Timestamp: 101, Key: []byte("user-2"), Value: []byte("address-2")}
	rec3 := &Record{Offset: 2, Timestamp: 102, Key: []byte("user-1"), Value: []byte("address-new")}

	_ = seg.Append(rec1)
	_ = seg.Append(rec2)
	_ = seg.Append(rec3)

	compactedCount, err := CompactLogSegments(seg, tempDir)
	if err != nil {
		t.Fatalf("CompactLogSegments failed: %v", err)
	}

	// Should contain 2 records (user-2, user-1 latest) instead of 3
	if compactedCount != 2 {
		t.Errorf("Expected 2 compacted records, got %d", compactedCount)
	}
}

func TestCleanerWorker_StartStop(t *testing.T) {
	supplier := func() map[string]*PartitionLog {
		return map[string]*PartitionLog{}
	}

	cfg := CleanerConfig{
		CleanerInterval: 50 * time.Millisecond,
	}
	cw := NewCleanerWorker(supplier, cfg)
	cw.Start()
	time.Sleep(120 * time.Millisecond)
	cw.Stop()
}
