package server

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/HoangDinhBui/kafka-golang/internal/protocol"
	"github.com/HoangDinhBui/kafka-golang/internal/storage"
)

// sendFramedRequest writes a length-prefixed request body to conn and
// returns the length-prefixed response body read back.
func sendFramedRequest(t *testing.T, conn net.Conn, body []byte) []byte {
	t.Helper()

	var sizeBuf [4]byte
	binary.BigEndian.PutUint32(sizeBuf[:], uint32(len(body)))
	if _, err := conn.Write(sizeBuf[:]); err != nil {
		t.Fatalf("failed to write request size: %v", err)
	}
	if _, err := conn.Write(body); err != nil {
		t.Fatalf("failed to write request body: %v", err)
	}

	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		t.Fatalf("failed to read response size: %v", err)
	}
	respSize := binary.BigEndian.Uint32(sizeBuf[:])
	resp := make([]byte, respSize)
	if _, err := io.ReadFull(conn, resp); err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	return resp
}

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

// ============================================================================
// TEST: TestTCPServerE2E_SaslScram
// Description: End-to-end integration test driving a full SASL/SCRAM-SHA-256
//              handshake (SaslHandshake -> 2x SaslAuthenticate) over a real
//              TCP connection, acting as the client. Regression test for the
//              gap where SCRAM-SHA-256 was advertised but the server only
//              ever validated SASL/PLAIN payloads.
// ============================================================================
func TestTCPServerE2E_SaslScram(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kafka_server_sasl_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	handler := NewHandler(tmpDir, 1, "127.0.0.1", 0)
	if err := handler.AddSASLUser("carol", "s3cr3t-pass"); err != nil {
		t.Fatalf("AddSASLUser failed: %v", err)
	}

	server := NewTCPServer("127.0.0.1:0", handler)
	serverErrChan := make(chan error, 1)
	go func() {
		serverErrChan <- server.Start()
	}()

	for i := 0; i < 50; i++ {
		if server.Addr() != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if server.Addr() == nil {
		t.Fatalf("Server failed to bind listener")
	}

	conn, err := net.Dial("tcp", server.Addr().String())
	if err != nil {
		t.Fatalf("Failed to dial server: %v", err)
	}
	defer conn.Close()

	// 1. SaslHandshake negotiating SCRAM-SHA-256
	handshakeBody := new(bytes.Buffer)
	_ = protocol.WriteInt16(handshakeBody, protocol.ApiKeySaslHandshake)
	_ = protocol.WriteInt16(handshakeBody, 0)
	_ = protocol.WriteInt32(handshakeBody, 1)
	_ = protocol.WriteString(handshakeBody, "sasl-e2e-client")
	_ = protocol.WriteString(handshakeBody, "SCRAM-SHA-256")

	handshakeResp := sendFramedRequest(t, conn, handshakeBody.Bytes())
	handshakeReader := bytes.NewReader(handshakeResp)
	if _, err := protocol.ReadInt32(handshakeReader); err != nil { // CorrelationId
		t.Fatalf("failed to read handshake correlation id: %v", err)
	}
	handshakeErrCode, err := protocol.ReadInt16(handshakeReader)
	if err != nil || handshakeErrCode != 0 {
		t.Fatalf("expected SaslHandshake ErrorCode 0, got %d, err: %v", handshakeErrCode, err)
	}

	// 2. SaslAuthenticate: client-first-message
	clientNonce := "e2e-nonce-0123456789"
	clientFirstBare := "n=carol,r=" + clientNonce
	clientFirst := []byte("n,," + clientFirstBare)

	authFirstBody := new(bytes.Buffer)
	_ = protocol.WriteInt16(authFirstBody, protocol.ApiKeySaslAuthenticate)
	_ = protocol.WriteInt16(authFirstBody, 0)
	_ = protocol.WriteInt32(authFirstBody, 2)
	_ = protocol.WriteString(authFirstBody, "sasl-e2e-client")
	_ = protocol.WriteBytes(authFirstBody, clientFirst)

	authFirstResp := sendFramedRequest(t, conn, authFirstBody.Bytes())
	authFirstReader := bytes.NewReader(authFirstResp)
	_, _ = protocol.ReadInt32(authFirstReader) // CorrelationId
	authFirstErrCode, err := protocol.ReadInt16(authFirstReader)
	if err != nil || authFirstErrCode != 0 {
		t.Fatalf("expected client-first ErrorCode 0, got %d, err: %v", authFirstErrCode, err)
	}
	if _, err := protocol.ReadNullableString(authFirstReader); err != nil {
		t.Fatalf("failed to read error message: %v", err)
	}
	serverFirstBytes, err := protocol.ReadBytes(authFirstReader)
	if err != nil {
		t.Fatalf("failed to read server-first-message: %v", err)
	}

	serverFirst := string(serverFirstBytes)
	attrs := make(map[string]string)
	for _, part := range strings.Split(serverFirst, ",") {
		kv := strings.SplitN(part, "=", 2)
		attrs[kv[0]] = kv[1]
	}
	fullNonce := attrs["r"]
	salt, err := base64.StdEncoding.DecodeString(attrs["s"])
	if err != nil {
		t.Fatalf("failed to decode salt: %v", err)
	}
	var iterations int
	if _, err := fmt.Sscanf(attrs["i"], "%d", &iterations); err != nil {
		t.Fatalf("failed to parse iterations: %v", err)
	}

	// 3. Compute the client proof exactly as a real SCRAM client would.
	saltedPassword := testPBKDF2HMACSHA256([]byte("s3cr3t-pass"), salt, iterations, sha256.Size)
	clientKey := testHMACSHA256(saltedPassword, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)

	clientFinalWithoutProof := "c=biws,r=" + fullNonce
	authMessage := clientFirstBare + "," + serverFirst + "," + clientFinalWithoutProof
	clientSignature := testHMACSHA256(storedKey[:], []byte(authMessage))

	clientProof := make([]byte, len(clientKey))
	for i := range clientProof {
		clientProof[i] = clientKey[i] ^ clientSignature[i]
	}
	clientFinal := []byte(clientFinalWithoutProof + ",p=" + base64.StdEncoding.EncodeToString(clientProof))

	// 4. SaslAuthenticate: client-final-message
	authFinalBody := new(bytes.Buffer)
	_ = protocol.WriteInt16(authFinalBody, protocol.ApiKeySaslAuthenticate)
	_ = protocol.WriteInt16(authFinalBody, 0)
	_ = protocol.WriteInt32(authFinalBody, 3)
	_ = protocol.WriteString(authFinalBody, "sasl-e2e-client")
	_ = protocol.WriteBytes(authFinalBody, clientFinal)

	authFinalResp := sendFramedRequest(t, conn, authFinalBody.Bytes())
	authFinalReader := bytes.NewReader(authFinalResp)
	_, _ = protocol.ReadInt32(authFinalReader) // CorrelationId
	authFinalErrCode, err := protocol.ReadInt16(authFinalReader)
	if err != nil || authFinalErrCode != 0 {
		t.Fatalf("expected client-final ErrorCode 0 (authentication success), got %d, err: %v", authFinalErrCode, err)
	}
	if _, err := protocol.ReadNullableString(authFinalReader); err != nil {
		t.Fatalf("failed to read error message: %v", err)
	}
	serverFinalBytes, err := protocol.ReadBytes(authFinalReader)
	if err != nil {
		t.Fatalf("failed to read server-final-message: %v", err)
	}
	if !strings.HasPrefix(string(serverFinalBytes), "v=") {
		t.Errorf("expected server-final-message to start with 'v=', got %q", serverFinalBytes)
	}

	conn.Close()
	if err := server.Stop(); err != nil {
		t.Errorf("Failed to stop TCPServer gracefully: %v", err)
	}
}

// testPBKDF2HMACSHA256 and testHMACSHA256 mirror internal/security's SCRAM
// key derivation so this black-box test can act as a real client without
// reaching into that package's unexported internals.
func testPBKDF2HMACSHA256(password, salt []byte, iterations, keyLen int) []byte {
	mac := hmac.New(sha256.New, password)
	blockSalt := make([]byte, len(salt)+4)
	copy(blockSalt, salt)
	binary.BigEndian.PutUint32(blockSalt[len(salt):], 1)

	mac.Write(blockSalt)
	u := mac.Sum(nil)
	t := make([]byte, len(u))
	copy(t, u)

	for i := 1; i < iterations; i++ {
		mac.Reset()
		mac.Write(u)
		u = mac.Sum(nil)
		for j := range t {
			t[j] ^= u[j]
		}
	}
	return t[:keyLen]
}

func testHMACSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}
