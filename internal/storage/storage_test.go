package storage

import (
	"bytes"
	"os"
	"testing"
	"time"
)

// ============================================================================
// TEST: TestRecordMarshalUnmarshal
// Description: Verifies binary serialization and deserialization of a Record.
// ============================================================================
func TestRecordMarshalUnmarshal(t *testing.T) {
	orig := &Record{
		Offset:    105,
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("test-key"),
		Value:     []byte("hello-kafka-golang-storage"),
	}

	data, err := orig.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal record: %v", err)
	}

	reader := bytes.NewReader(data)
	decoded, bytesRead, err := ReadRecord(reader)
	if err != nil {
		t.Fatalf("Failed to read record: %v", err)
	}

	if int64(len(data)) != bytesRead {
		t.Errorf("Expected bytesRead %d, got %d", len(data), bytesRead)
	}

	if decoded.Offset != orig.Offset {
		t.Errorf("Expected Offset %d, got %d", orig.Offset, decoded.Offset)
	}

	if decoded.Timestamp != orig.Timestamp {
		t.Errorf("Expected Timestamp %d, got %d", orig.Timestamp, decoded.Timestamp)
	}

	if !bytes.Equal(decoded.Key, orig.Key) {
		t.Errorf("Expected Key %s, got %s", orig.Key, decoded.Key)
	}

	if !bytes.Equal(decoded.Value, orig.Value) {
		t.Errorf("Expected Value %s, got %s", orig.Value, decoded.Value)
	}
}

// ============================================================================
// TEST: TestIndexLookup
// Description: Verifies disk-based binary search lookup in an Index file.
// ============================================================================
func TestIndexLookup(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kafka_index_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	idxPath := tmpDir + "/00000000000000000000.index"
	idx, err := NewIndex(idxPath)
	if err != nil {
		t.Fatalf("Failed to create Index: %v", err)
	}
	defer idx.Close()

	// Write index entries: (offset -> position)
	entries := []struct {
		offset uint64
		pos    int64
	}{
		{offset: 0, pos: 0},
		{offset: 10, pos: 150},
		{offset: 20, pos: 320},
		{offset: 30, pos: 500},
	}

	for _, e := range entries {
		if err := idx.WriteEntry(e.offset, e.pos); err != nil {
			t.Fatalf("Failed to write entry: %v", err)
		}
	}

	// Test lookups
	tests := []struct {
		target   uint64
		expected int64
	}{
		{target: 0, expected: 0},
		{target: 5, expected: 0},     // <= 5 is offset 0 (pos 0)
		{target: 10, expected: 150},  // exact match
		{target: 15, expected: 150},  // <= 15 is offset 10 (pos 150)
		{target: 25, expected: 320},  // <= 25 is offset 20 (pos 320)
		{target: 100, expected: 500}, // > 30 is offset 30 (pos 500)
	}

	for _, tt := range tests {
		got, err := idx.Lookup(tt.target)
		if err != nil {
			t.Errorf("Lookup(%d) error: %v", tt.target, err)
		}
		if got != tt.expected {
			t.Errorf("Lookup(%d): expected position %d, got %d", tt.target, tt.expected, got)
		}
	}
}

// ============================================================================
// TEST: TestPartitionLogRollAndRecovery
// Description: Verifies rolling multiple segments and recovering them on restart.
// ============================================================================
func TestPartitionLogRollAndRecovery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kafka_partition_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Small segment size (150 bytes) to force rolling new segments frequently
	maxSegBytes := int64(150)
	indexInterval := int64(40)

	pl, err := NewPartitionLog(tmpDir, maxSegBytes, indexInterval)
	if err != nil {
		t.Fatalf("Failed to create PartitionLog: %v", err)
	}

	// Append 10 records
	totalRecords := 10
	for i := 0; i < totalRecords; i++ {
		rec := &Record{
			Timestamp: time.Now().UnixNano(),
			Key:       []byte("k"),
			Value:     []byte("payload-data-for-testing-segment-rolling"),
		}
		if err := pl.Append(rec); err != nil {
			t.Fatalf("Failed to append record %d: %v", i, err)
		}
	}

	if err := pl.Close(); err != nil {
		t.Fatalf("Failed to close PartitionLog: %v", err)
	}

	// Re-open PartitionLog to simulate broker restart recovery
	plRestored, err := NewPartitionLog(tmpDir, maxSegBytes, indexInterval)
	if err != nil {
		t.Fatalf("Failed to re-open PartitionLog: %v", err)
	}
	defer plRestored.Close()

	// Read all records starting from offset 0
	records, err := plRestored.Read(0)
	if err != nil {
		t.Fatalf("Failed to read records after recovery: %v", err)
	}

	if len(records) != totalRecords {
		t.Errorf("Expected %d records, got %d", totalRecords, len(records))
	}

	for i, r := range records {
		if r.Offset != uint64(i) {
			t.Errorf("Record %d has incorrect offset %d", i, r.Offset)
		}
	}
}
