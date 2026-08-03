package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSendFileToSocket(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "zero_copy_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	filePath := filepath.Join(tempDir, "test.log")
	testData := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")

	if err := os.WriteFile(filePath, testData, 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	var buf bytes.Buffer
	// Transfer 10 bytes starting from offset 10 ("ABCDEFGHIJ")
	n, err := SendFileToSocket(&buf, file, 10, 10)
	if err != nil {
		t.Fatalf("SendFileToSocket failed: %v", err)
	}

	if n != 10 {
		t.Errorf("Expected 10 bytes transferred, got %d", n)
	}

	expected := "ABCDEFGHIJ"
	if buf.String() != expected {
		t.Errorf("Expected content '%s', got '%s'", expected, buf.String())
	}
}

func TestPartitionLog_ReadZeroCopy(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "partition_log_zerocopy_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	partDir := filepath.Join(tempDir, "zc.topic-0")
	pl, err := NewPartitionLog(partDir, 1024, 64)
	if err != nil {
		t.Fatalf("Failed to create partition log: %v", err)
	}
	defer pl.Close()

	// Append 5 records
	for i := 0; i < 5; i++ {
		rec := &Record{
			Timestamp: time.Now().UnixNano(),
			Key:       []byte(fmt.Sprintf("k-%d", i)),
			Value:     []byte(fmt.Sprintf("v-%d-payload", i)),
		}
		if err := pl.Append(rec); err != nil {
			t.Fatalf("Failed to append record %d: %v", i, err)
		}
	}

	var socketBuf bytes.Buffer
	transferred, err := pl.ReadZeroCopy(0, 10000, &socketBuf)
	if err != nil {
		t.Fatalf("ReadZeroCopy failed: %v", err)
	}

	if transferred <= 0 {
		t.Errorf("Expected positive transferred bytes, got %d", transferred)
	}

	if socketBuf.Len() != int(transferred) {
		t.Errorf("Expected buffer length %d, got %d", transferred, socketBuf.Len())
	}
}

func BenchmarkZeroCopyTransfer(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "bench_zerocopy_*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	filePath := filepath.Join(tempDir, "bench.log")
	data := bytes.Repeat([]byte("A"), 64*1024) // 64KB
	_ = os.WriteFile(filePath, data, 0644)

	file, _ := os.Open(filePath)
	defer file.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		_, _ = SendFileToSocket(&buf, file, 0, 64*1024)
	}
}
