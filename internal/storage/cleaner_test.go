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

// TestCompactLogSegments verifies compaction actually persists its result.
// Regression test for a bug where CompactLogSegments wrote its compacted
// output to a temp file, then a deferred cleanup closed and os.Remove'd
// that exact file before the function returned — the compacted count was
// correct, but the segment's real .log/.index files were completely
// untouched, so compaction had no observable effect no matter how or when
// it was invoked.
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

	// Append duplicate key records: key1 -> v1, key2 -> v2, key1 -> v1-updated
	rec1 := &Record{Offset: 0, Timestamp: 100, Key: []byte("user-1"), Value: []byte("address-old")}
	rec2 := &Record{Offset: 1, Timestamp: 101, Key: []byte("user-2"), Value: []byte("address-2")}
	rec3 := &Record{Offset: 2, Timestamp: 102, Key: []byte("user-1"), Value: []byte("address-new")}

	_ = seg.Append(rec1)
	_ = seg.Append(rec2)
	_ = seg.Append(rec3)
	sizeBeforeCompaction := seg.Size()

	// Single segment case: the "global" latest-offset-per-key map is just
	// this segment's own latest occurrences (user-1 -> offset 2, user-2 ->
	// offset 1) — matching what PartitionLog.CompactSegments would compute
	// by scanning the whole partition.
	keyLatestOffset := map[string]uint64{"user-1": 2, "user-2": 1}
	compactedCount, err := CompactLogSegments(seg, tempDir, keyLatestOffset)
	if err != nil {
		t.Fatalf("CompactLogSegments failed: %v", err)
	}

	// Should contain 2 records (user-2, user-1 latest) instead of 3
	if compactedCount != 2 {
		t.Errorf("Expected 2 compacted records, got %d", compactedCount)
	}

	// The segment's own file must have actually shrunk...
	if seg.Size() >= sizeBeforeCompaction {
		t.Errorf("expected segment size to shrink after compaction: before=%d after=%d", sizeBeforeCompaction, seg.Size())
	}
	// ...and reads through the SAME in-memory Segment must reflect it.
	inMemoryRecords, err := seg.Read(0)
	if err != nil {
		t.Fatalf("Read after compaction failed: %v", err)
	}
	if len(inMemoryRecords) != 2 {
		t.Fatalf("expected 2 records via the compacted segment, got %d", len(inMemoryRecords))
	}
	_ = seg.Close()

	// Reopening a completely FRESH Segment against the same directory —
	// simulating a broker restart — must recover the SAME compacted state
	// from disk, proving the swapped files are well-formed on their own,
	// not merely correct via the mutated in-memory struct.
	reopened, err := NewSegment(tempDir, 0, 10000, 100)
	if err != nil {
		t.Fatalf("failed to reopen segment after compaction: %v", err)
	}
	defer reopened.Close()

	records, err := reopened.Read(0)
	if err != nil {
		t.Fatalf("Read on reopened segment failed: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records after reopening, got %d", len(records))
	}

	byKey := make(map[string]*Record)
	for _, rec := range records {
		byKey[string(rec.Key)] = rec
	}
	if got := byKey["user-1"]; got == nil || string(got.Value) != "address-new" || got.Offset != 2 {
		t.Errorf("expected user-1 to keep its latest value at original offset 2, got %+v", got)
	}
	if got := byKey["user-2"]; got == nil || string(got.Value) != "address-2" || got.Offset != 1 {
		t.Errorf("expected user-2 unchanged at original offset 1, got %+v", got)
	}
}

// ============================================================================
// TEST: TestPartitionLog_CompactSegments
// Description: Regression test for compaction being entirely unwired:
//              PartitionLog.CompactSegments (called by CleanerWorker when
//              CompactionEnabled is set) previously did not exist at all —
//              nothing in the running broker ever invoked compaction on a
//              real multi-segment partition. Verifies closed segments are
//              compacted while the active segment is left untouched, and
//              that PartitionLog.Read still returns correct data afterward.
// ============================================================================
func TestPartitionLog_CompactSegments(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "kafka_pl_compact_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Small maxSegmentBytes forces a roll after just a couple of records.
	pl, err := NewPartitionLog(tempDir, 60, 10)
	if err != nil {
		t.Fatalf("Failed to create PartitionLog: %v", err)
	}
	defer pl.Close()

	records := []*Record{
		{Timestamp: 100, Key: []byte("user-1"), Value: []byte("v1")},
		{Timestamp: 101, Key: []byte("user-1"), Value: []byte("v2")},
		{Timestamp: 102, Key: []byte("user-2"), Value: []byte("v1")},
		{Timestamp: 103, Key: []byte("user-1"), Value: []byte("v3-latest")},
	}
	for _, rec := range records {
		if err := pl.Append(rec); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	segCountBefore, _ := pl.Stats()
	if segCountBefore < 2 {
		t.Fatalf("test setup expected multiple segments, got %d", segCountBefore)
	}

	rewritten, err := pl.CompactSegments()
	if err != nil {
		t.Fatalf("CompactSegments failed: %v", err)
	}
	if rewritten == 0 {
		t.Error("expected at least one closed segment to be rewritten")
	}

	all, err := pl.Read(0)
	if err != nil {
		t.Fatalf("Read after compaction failed: %v", err)
	}

	byKey := make(map[string]*Record)
	for _, rec := range all {
		byKey[string(rec.Key)] = rec
	}
	if got := byKey["user-1"]; got == nil || string(got.Value) != "v3-latest" {
		t.Errorf("expected user-1's latest surviving value to be v3-latest, got %+v", got)
	}
	if got := byKey["user-2"]; got == nil || string(got.Value) != "v1" {
		t.Errorf("expected user-2 to survive unchanged, got %+v", got)
	}

	// The superseded user-1/v1 and user-1/v2 records must be gone.
	count := 0
	for _, rec := range all {
		if string(rec.Key) == "user-1" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 surviving user-1 record, got %d", count)
	}
}

// ============================================================================
// TEST: TestCleanerWorker_CompactionIsOptIn
// Description: Verifies compaction only runs when CompactionEnabled is
//              explicitly set, and that RunCleanCycle actually invokes it
//              when enabled — the original finding was that nothing in
//              RunCleanCycle ever called compaction at all, regardless of
//              configuration.
// ============================================================================
func TestCleanerWorker_CompactionIsOptIn(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "kafka_cleaner_compaction_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	newPartitionWithDuplicates := func(dir string) *PartitionLog {
		pl, err := NewPartitionLog(dir, 60, 10)
		if err != nil {
			t.Fatalf("Failed to create PartitionLog: %v", err)
		}
		for _, v := range []string{"v1", "v2", "v3-latest"} {
			if err := pl.Append(&Record{Timestamp: 100, Key: []byte("user-1"), Value: []byte(v)}); err != nil {
				t.Fatalf("Append failed: %v", err)
			}
		}
		return pl
	}

	t.Run("disabled by default", func(t *testing.T) {
		dir := filepath.Join(tempDir, "disabled")
		pl := newPartitionWithDuplicates(dir)
		defer pl.Close()

		cw := NewCleanerWorker(func() map[string]*PartitionLog {
			return map[string]*PartitionLog{"disabled-0": pl}
		}, CleanerConfig{CleanerInterval: time.Hour})
		cw.RunCleanCycle()

		countBefore, _ := pl.Stats()
		if countBefore < 2 {
			t.Fatalf("test setup expected multiple segments, got %d", countBefore)
		}
		// With compaction disabled, the closed segment must be untouched.
		all, _ := pl.Read(0)
		userOneCount := 0
		for _, rec := range all {
			if string(rec.Key) == "user-1" {
				userOneCount++
			}
		}
		if userOneCount != 3 {
			t.Errorf("expected all 3 duplicate-key records to remain with compaction disabled, got %d", userOneCount)
		}
	})

	t.Run("enabled explicitly", func(t *testing.T) {
		dir := filepath.Join(tempDir, "enabled")
		pl := newPartitionWithDuplicates(dir)
		defer pl.Close()

		cw := NewCleanerWorker(func() map[string]*PartitionLog {
			return map[string]*PartitionLog{"enabled-0": pl}
		}, CleanerConfig{CleanerInterval: time.Hour, CompactionEnabled: true})
		cw.RunCleanCycle()

		all, _ := pl.Read(0)
		userOneCount := 0
		for _, rec := range all {
			if string(rec.Key) == "user-1" {
				userOneCount++
			}
		}
		if userOneCount != 1 {
			t.Errorf("expected compaction to reduce user-1 to its single latest record, got %d", userOneCount)
		}
	})
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
