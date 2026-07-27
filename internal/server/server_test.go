package server

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/HoangDinhBui/kafka-golang/internal/protocol"
	"github.com/HoangDinhBui/kafka-golang/internal/storage"
)

// ============================================================================
// TEST: TestTCPServerE2E
// Description: End-to-end integration test of TCPServer over real TCP socket for ApiVersions, Produce, and Fetch.
// ============================================================================
func TestTCPServerE2E(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kafka_server_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Initialize handler and TCP server on dynamic port (127.0.0.1:0)
	handler := NewHandler(tmpDir, 1, "127.0.0.1", 0)
	server := NewTCPServer("127.0.0.1:0", handler)

	// Start server loop in background
	serverErrChan := make(chan error, 1)
	go func() {
		serverErrChan <- server.Start()
	}()

	// Wait for listener to bind
	for i := 0; i < 50; i++ {
		if server.Addr() != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if server.Addr() == nil {
		t.Fatalf("Server failed to bind listener")
	}
	addr := server.Addr().String()

	// Dial TCP server
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to dial server at %s: %v", addr, err)
	}
	defer conn.Close()

	// ------------------------------------------------------------------------
	// 1. Test ApiVersions (ApiKey 18) over TCP
	// ------------------------------------------------------------------------
	reqBodyBuf := new(bytes.Buffer)
	_ = protocol.WriteInt16(reqBodyBuf, protocol.ApiKeyApiVersions)
	_ = protocol.WriteInt16(reqBodyBuf, 0)
	_ = protocol.WriteInt32(reqBodyBuf, 777)
	_ = protocol.WriteString(reqBodyBuf, "e2e-client")

	// Send 4-byte size + request payload
	var sizeBuf [4]byte
	binary.BigEndian.PutUint32(sizeBuf[:], uint32(reqBodyBuf.Len()))
	_, _ = conn.Write(sizeBuf[:])
	_, _ = conn.Write(reqBodyBuf.Bytes())

	// Read response size header
	_, err = io.ReadFull(conn, sizeBuf[:])
	if err != nil {
		t.Fatalf("Failed to read response size header: %v", err)
	}
	respSize := binary.BigEndian.Uint32(sizeBuf[:])

	respBuf := make([]byte, respSize)
	_, err = io.ReadFull(conn, respBuf)
	if err != nil {
		t.Fatalf("Failed to read response payload: %v", err)
	}

	respReader := bytes.NewReader(respBuf)
	corrId, err := protocol.ReadInt32(respReader)
	if err != nil || corrId != 777 {
		t.Fatalf("Expected CorrelationId 777, got %d, err: %v", corrId, err)
	}

	errCode, err := protocol.ReadInt16(respReader)
	if err != nil || errCode != 0 {
		t.Fatalf("Expected ErrorCode 0, got %d, err: %v", errCode, err)
	}

	// ------------------------------------------------------------------------
	// 2. Test Produce (ApiKey 0) over TCP
	// ------------------------------------------------------------------------
	rec := &storage.Record{
		Timestamp: time.Now().UnixNano(),
		Key:       []byte("order-key"),
		Value:     []byte("order-value-payload"),
	}
	recBytes, _ := rec.Marshal()

	produceBodyBuf := new(bytes.Buffer)
	_ = protocol.WriteInt16(produceBodyBuf, protocol.ApiKeyProduce)
	_ = protocol.WriteInt16(produceBodyBuf, 0)
	_ = protocol.WriteInt32(produceBodyBuf, 888)
	_ = protocol.WriteString(produceBodyBuf, "e2e-producer")

	// Produce payload: Acks=1, Timeout=1000, 1 topic ("e2e-orders"), 1 partition (0)
	_ = protocol.WriteInt16(produceBodyBuf, 1)
	_ = protocol.WriteInt32(produceBodyBuf, 1000)
	_ = protocol.WriteInt32(produceBodyBuf, 1)
	_ = protocol.WriteString(produceBodyBuf, "e2e-orders")
	_ = protocol.WriteInt32(produceBodyBuf, 1)
	_ = protocol.WriteInt32(produceBodyBuf, 0)
	_ = protocol.WriteInt32(produceBodyBuf, int32(len(recBytes)))
	produceBodyBuf.Write(recBytes)

	binary.BigEndian.PutUint32(sizeBuf[:], uint32(produceBodyBuf.Len()))
	_, _ = conn.Write(sizeBuf[:])
	_, _ = conn.Write(produceBodyBuf.Bytes())

	// Read Produce Response
	_, _ = io.ReadFull(conn, sizeBuf[:])
	produceRespSize := binary.BigEndian.Uint32(sizeBuf[:])
	produceRespBuf := make([]byte, produceRespSize)
	_, _ = io.ReadFull(conn, produceRespBuf)

	produceRespReader := bytes.NewReader(produceRespBuf)
	produceCorrId, _ := protocol.ReadInt32(produceRespReader)
	if produceCorrId != 888 {
		t.Errorf("Expected Produce CorrelationId 888, got %d", produceCorrId)
	}

	// ------------------------------------------------------------------------
	// 3. Test Fetch (ApiKey 1) over TCP
	// ------------------------------------------------------------------------
	fetchBodyBuf := new(bytes.Buffer)
	_ = protocol.WriteInt16(fetchBodyBuf, protocol.ApiKeyFetch)
	_ = protocol.WriteInt16(fetchBodyBuf, 0)
	_ = protocol.WriteInt32(fetchBodyBuf, 999)
	_ = protocol.WriteString(fetchBodyBuf, "e2e-consumer")

	// Fetch payload: ReplicaId=-1, MaxWait=500, MinBytes=1, 1 topic ("e2e-orders"), 1 partition (0), FetchOffset=0, MaxBytes=1024
	_ = protocol.WriteInt32(fetchBodyBuf, -1)
	_ = protocol.WriteInt32(fetchBodyBuf, 500)
	_ = protocol.WriteInt32(fetchBodyBuf, 1)
	_ = protocol.WriteInt32(fetchBodyBuf, 1)
	_ = protocol.WriteString(fetchBodyBuf, "e2e-orders")
	_ = protocol.WriteInt32(fetchBodyBuf, 1)
	_ = protocol.WriteInt32(fetchBodyBuf, 0)
	_ = protocol.WriteInt64(fetchBodyBuf, 0)
	_ = protocol.WriteInt32(fetchBodyBuf, 1024)

	binary.BigEndian.PutUint32(sizeBuf[:], uint32(fetchBodyBuf.Len()))
	_, _ = conn.Write(sizeBuf[:])
	_, _ = conn.Write(fetchBodyBuf.Bytes())

	// Read Fetch Response
	_, _ = io.ReadFull(conn, sizeBuf[:])
	fetchRespSize := binary.BigEndian.Uint32(sizeBuf[:])
	fetchRespBuf := make([]byte, fetchRespSize)
	_, _ = io.ReadFull(conn, fetchRespBuf)

	fetchRespReader := bytes.NewReader(fetchRespBuf)
	fetchCorrId, _ := protocol.ReadInt32(fetchRespReader)
	if fetchCorrId != 999 {
		t.Errorf("Expected Fetch CorrelationId 999, got %d", fetchCorrId)
	}

	// Close client connection to unblock reading goroutine
	conn.Close()

	// Stop server gracefully
	if err := server.Stop(); err != nil {
		t.Errorf("Failed to stop TCPServer gracefully: %v", err)
	}
}
