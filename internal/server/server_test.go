package server

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/HoangDinhBui/kafka-golang/internal/protocol"
	"github.com/HoangDinhBui/kafka-golang/internal/security"
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

// buildSaslHandshakeRequest / buildSaslAuthenticateRequest / buildProduceRequest
// assemble minimal framed request bodies for the enforcement tests below.
func buildSaslHandshakeRequest(correlationId int32, mechanism string) []byte {
	body := new(bytes.Buffer)
	_ = protocol.WriteInt16(body, protocol.ApiKeySaslHandshake)
	_ = protocol.WriteInt16(body, 0)
	_ = protocol.WriteInt32(body, correlationId)
	_ = protocol.WriteString(body, "enforcement-test-client")
	_ = protocol.WriteString(body, mechanism)
	return body.Bytes()
}

func buildSaslAuthenticateRequest(correlationId int32, authData []byte) []byte {
	body := new(bytes.Buffer)
	_ = protocol.WriteInt16(body, protocol.ApiKeySaslAuthenticate)
	_ = protocol.WriteInt16(body, 0)
	_ = protocol.WriteInt32(body, correlationId)
	_ = protocol.WriteString(body, "enforcement-test-client")
	_ = protocol.WriteBytes(body, authData)
	return body.Bytes()
}

func buildProduceRequest(correlationId int32, topic string, partitionId int32, value []byte) []byte {
	rec := &storage.Record{Timestamp: time.Now().UnixNano(), Value: value}
	recBytes, _ := rec.Marshal()

	body := new(bytes.Buffer)
	_ = protocol.WriteInt16(body, protocol.ApiKeyProduce)
	_ = protocol.WriteInt16(body, 0)
	_ = protocol.WriteInt32(body, correlationId)
	_ = protocol.WriteString(body, "enforcement-test-client")
	_ = protocol.EncodeProduceRequest(body, &protocol.ProduceRequest{
		Acks:    1,
		Timeout: 1000,
		Topics: []protocol.TopicProduceData{{
			TopicName: topic,
			Partitions: []protocol.PartitionProduceData{{
				PartitionId: partitionId,
				RecordsData: recBytes,
			}},
		}},
	})
	return body.Bytes()
}

// decodeProduceResponse mirrors EncodeProduceResponse's layout (no
// DecodeProduceResponse exists since only the broker itself encodes it).
func decodeProduceResponse(t *testing.T, respBody []byte) *protocol.ProduceResponse {
	t.Helper()
	r := bytes.NewReader(respBody)
	if _, err := protocol.ReadInt32(r); err != nil { // CorrelationId
		t.Fatalf("failed to read correlation id: %v", err)
	}

	topicCount, err := protocol.ReadInt32(r)
	if err != nil {
		t.Fatalf("failed to read topic count: %v", err)
	}
	resp := &protocol.ProduceResponse{}
	for i := int32(0); i < topicCount; i++ {
		topicName, err := protocol.ReadString(r)
		if err != nil {
			t.Fatalf("failed to read topic name: %v", err)
		}
		partCount, err := protocol.ReadInt32(r)
		if err != nil {
			t.Fatalf("failed to read partition count: %v", err)
		}
		topicResp := protocol.TopicProduceResponse{TopicName: topicName}
		for j := int32(0); j < partCount; j++ {
			partId, _ := protocol.ReadInt32(r)
			errCode, _ := protocol.ReadInt16(r)
			baseOffset, _ := protocol.ReadInt64(r)
			logAppendTime, _ := protocol.ReadInt64(r)
			topicResp.Partitions = append(topicResp.Partitions, protocol.PartitionProduceResponse{
				PartitionId:   partId,
				ErrorCode:     errCode,
				BaseOffset:    baseOffset,
				LogAppendTime: logAppendTime,
			})
		}
		resp.Topics = append(resp.Topics, topicResp)
	}
	return resp
}

// authenticatePlainOverConn drives a SaslHandshake + SaslAuthenticate(PLAIN)
// exchange over conn, failing the test if authentication does not succeed.
func authenticatePlainOverConn(t *testing.T, conn net.Conn, username, password string) {
	t.Helper()

	handshakeResp := sendFramedRequest(t, conn, buildSaslHandshakeRequest(100, "PLAIN"))
	hr := bytes.NewReader(handshakeResp)
	_, _ = protocol.ReadInt32(hr)
	if code, err := protocol.ReadInt16(hr); err != nil || code != 0 {
		t.Fatalf("SaslHandshake failed: code=%d err=%v", code, err)
	}

	authPayload := []byte("\x00" + username + "\x00" + password)
	authResp := sendFramedRequest(t, conn, buildSaslAuthenticateRequest(101, authPayload))
	ar := bytes.NewReader(authResp)
	_, _ = protocol.ReadInt32(ar)
	if code, err := protocol.ReadInt16(ar); err != nil || code != 0 {
		t.Fatalf("SaslAuthenticate failed: code=%d err=%v", code, err)
	}
}

// ============================================================================
// TEST: TestHandler_SASLRequired
// Description: Regression test for the gap where -sasl-enabled was printed
//              in the startup banner but never actually gated any request —
//              Produce/Fetch/etc. worked whether or not a client
//              authenticated. Verifies unauthenticated requests are now
//              rejected (closing the connection, matching real Kafka
//              behavior) and authenticated requests still succeed.
// ============================================================================
func TestHandler_SASLRequired(t *testing.T) {
	// PartitionLog does not close its segment file handles, so on Windows
	// t.TempDir()'s cleanup fails with "used by another process". Match the
	// os.MkdirTemp + best-effort os.RemoveAll pattern used elsewhere in this
	// file, which tolerates that instead of failing the test on cleanup.
	tmpDir, err := os.MkdirTemp("", "kafka_sasl_required_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	handler := NewHandler(tmpDir, 1, "127.0.0.1", 0)
	if err := handler.AddSASLUser("eve", "eve-pass"); err != nil {
		t.Fatalf("AddSASLUser failed: %v", err)
	}
	handler.SetSASLRequired(true)

	server := NewTCPServer("127.0.0.1:0", handler)
	go func() { _ = server.Start() }()
	for i := 0; i < 50 && server.Addr() == nil; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if server.Addr() == nil {
		t.Fatalf("Server failed to bind listener")
	}
	defer server.Stop()

	t.Run("unauthenticated request is rejected", func(t *testing.T) {
		conn, err := net.Dial("tcp", server.Addr().String())
		if err != nil {
			t.Fatalf("Failed to dial server: %v", err)
		}
		defer conn.Close()

		produceReq := buildProduceRequest(1, "orders", 0, []byte("should-not-be-accepted"))
		var sizeBuf [4]byte
		binary.BigEndian.PutUint32(sizeBuf[:], uint32(len(produceReq)))
		if _, err := conn.Write(sizeBuf[:]); err != nil {
			t.Fatalf("failed to write request size: %v", err)
		}
		if _, err := conn.Write(produceReq); err != nil {
			t.Fatalf("failed to write request body: %v", err)
		}

		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, err := io.ReadFull(conn, sizeBuf[:]); err == nil {
			t.Error("expected the connection to be closed for an unauthenticated Produce request, but got a response")
		}
	})

	t.Run("authenticated request succeeds", func(t *testing.T) {
		conn, err := net.Dial("tcp", server.Addr().String())
		if err != nil {
			t.Fatalf("Failed to dial server: %v", err)
		}
		defer conn.Close()

		authenticatePlainOverConn(t, conn, "eve", "eve-pass")

		produceResp := sendFramedRequest(t, conn, buildProduceRequest(2, "orders", 0, []byte("accepted-after-auth")))
		resp := decodeProduceResponse(t, produceResp)
		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
			t.Fatalf("unexpected produce response shape: %+v", resp)
		}
		if code := resp.Topics[0].Partitions[0].ErrorCode; code != 0 {
			t.Errorf("expected ErrorCode 0 after authentication, got %d", code)
		}
	})
}

// ============================================================================
// TEST: TestHandler_ACLAuthorization_Produce
// Description: Regression test for the gap where ACLManager was constructed
//              and reachable via AddACLRule but never consulted by any
//              request handler — every Produce/Fetch succeeded regardless of
//              ACL rules. Verifies a Deny rule now blocks Produce to the
//              matching topic while an unrelated topic remains unaffected.
// ============================================================================
func TestHandler_ACLAuthorization_Produce(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kafka_acl_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	handler := NewHandler(tmpDir, 1, "127.0.0.1", 0)
	if err := handler.AddSASLUser("frank", "frank-pass"); err != nil {
		t.Fatalf("AddSASLUser failed: %v", err)
	}
	handler.AddACLRule(security.ACLRule{
		Principal:      "frank",
		ResourceType:   security.ResourceTypeTopic,
		ResourceName:   "secret-topic",
		Operation:      security.OpWrite,
		PermissionType: security.PermDeny,
	})

	session := security.NewSASLSession()
	session.SetMechanism("PLAIN")
	if _, done, _, err := handler.saslAuth.Authenticate(session, []byte("\x00frank\x00frank-pass")); err != nil || !done {
		t.Fatalf("failed to authenticate test session: done=%v err=%v", done, err)
	}

	sendProduce := func(topic string) *protocol.ProduceResponse {
		header := &protocol.RequestHeader{ApiKey: protocol.ApiKeyProduce, CorrelationId: 1, ClientId: "acl-test"}
		body := new(bytes.Buffer)
		_ = protocol.EncodeProduceRequest(body, &protocol.ProduceRequest{
			Acks:    1,
			Timeout: 1000,
			Topics: []protocol.TopicProduceData{{
				TopicName:  topic,
				Partitions: []protocol.PartitionProduceData{{PartitionId: 0, RecordsData: mustMarshalRecord(t, "payload")}},
			}},
		})
		respBuf := new(bytes.Buffer)
		if err := handler.HandleRequest(header, body, respBuf, session); err != nil {
			t.Fatalf("HandleRequest failed: %v", err)
		}
		respWithCorrId := new(bytes.Buffer)
		_ = protocol.WriteInt32(respWithCorrId, 1)
		respWithCorrId.Write(respBuf.Bytes())
		return decodeProduceResponse(t, respWithCorrId.Bytes())
	}

	if resp := sendProduce("secret-topic"); resp.Topics[0].Partitions[0].ErrorCode != errCodeTopicAuthorizationFailed {
		t.Errorf("expected TOPIC_AUTHORIZATION_FAILED (%d) for denied topic, got %d", errCodeTopicAuthorizationFailed, resp.Topics[0].Partitions[0].ErrorCode)
	}

	if resp := sendProduce("public-topic"); resp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Errorf("expected ErrorCode 0 for unrestricted topic, got %d", resp.Topics[0].Partitions[0].ErrorCode)
	}
}

func mustMarshalRecord(t *testing.T, value string) []byte {
	t.Helper()
	rec := &storage.Record{Timestamp: time.Now().UnixNano(), Value: []byte(value)}
	b, err := rec.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal record: %v", err)
	}
	return b
}

// ============================================================================
// TEST: TestTCPServer_TLS
// Description: Regression test for the gap where -tls printed "ENABLED" in
//              the startup banner but TCPServer.Start() always bound a
//              plaintext net.Listen — TLS was never actually applied to the
//              socket. Verifies SetTLSConfig makes the listener negotiate a
//              real TLS handshake.
// ============================================================================
func TestTCPServer_TLS(t *testing.T) {
	tmpDir := t.TempDir()
	handler := NewHandler(tmpDir, 1, "127.0.0.1", 0)

	tlsConfig, err := security.GenerateSelfSignedTLSConfig()
	if err != nil {
		t.Fatalf("GenerateSelfSignedTLSConfig failed: %v", err)
	}

	server := NewTCPServer("127.0.0.1:0", handler)
	server.SetTLSConfig(tlsConfig)

	go func() { _ = server.Start() }()
	for i := 0; i < 50 && server.Addr() == nil; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if server.Addr() == nil {
		t.Fatalf("Server failed to bind listener")
	}
	defer server.Stop()

	conn, err := tls.Dial("tcp", server.Addr().String(), &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("expected a TLS handshake to succeed against the TLS-enabled listener: %v", err)
	}
	defer conn.Close()

	body := new(bytes.Buffer)
	_ = protocol.WriteInt16(body, protocol.ApiKeyApiVersions)
	_ = protocol.WriteInt16(body, 0)
	_ = protocol.WriteInt32(body, 42)
	_ = protocol.WriteString(body, "tls-test-client")

	resp := sendFramedRequest(t, conn, body.Bytes())
	r := bytes.NewReader(resp)
	corrId, err := protocol.ReadInt32(r)
	if err != nil || corrId != 42 {
		t.Fatalf("expected CorrelationId 42 over TLS, got %d, err: %v", corrId, err)
	}
}

// ============================================================================
// TEST: TestConnectionHandler_RejectsOversizedFrame
// Description: Regression test for a single-packet memory-exhaustion bug:
//              ConnectionHandler.Handle() read the 4-byte request-size
//              prefix and immediately called make([]byte, requestSize) with
//              no upper bound, so a forged size near the uint32 max (~4 GiB)
//              would force a multi-gigabyte allocation attempt before a
//              single payload byte arrived. Verifies a claimed size beyond
//              maxRequestFrameBytes now closes the connection immediately
//              instead of attempting the allocation.
// ============================================================================
func TestConnectionHandler_RejectsOversizedFrame(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kafka_oversized_frame_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	handler := NewHandler(tmpDir, 1, "127.0.0.1", 0)
	server := NewTCPServer("127.0.0.1:0", handler)
	go func() { _ = server.Start() }()
	for i := 0; i < 50 && server.Addr() == nil; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if server.Addr() == nil {
		t.Fatalf("Server failed to bind listener")
	}
	defer server.Stop()

	conn, err := net.Dial("tcp", server.Addr().String())
	if err != nil {
		t.Fatalf("Failed to dial server: %v", err)
	}
	defer conn.Close()

	// Claim a frame far larger than maxRequestFrameBytes without sending any
	// payload bytes at all — if the server allocated based on this claim
	// before validating it, this alone would attempt a multi-gigabyte alloc.
	var sizeBuf [4]byte
	binary.BigEndian.PutUint32(sizeBuf[:], 0xFFFFFFF0) // ~4 GiB claimed size
	if _, err := conn.Write(sizeBuf[:]); err != nil {
		t.Fatalf("failed to write oversized size header: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(conn, sizeBuf[:]); err == nil {
		t.Error("expected the connection to be closed for an oversized frame, but got a response")
	}
}

// ============================================================================
// TEST: TestHandler_Produce_RejectsPathTraversalTopicName
// Description: Regression test for a path-traversal bug: getOrCreatePartitionLog
//              joined the raw, client-supplied topic name directly under
//              dataDir and called os.MkdirAll on the result, so a topic
//              name like "../../evil" would create files outside dataDir
//              entirely. Verifies such a topic is now rejected (per-partition
//              error, no directory created outside dataDir) while an
//              ordinary topic name still works.
// ============================================================================
func TestHandler_Produce_RejectsPathTraversalTopicName(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kafka_path_traversal_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dataDir := filepath.Join(tmpDir, "data")
	handler := NewHandler(dataDir, 1, "127.0.0.1", 0)

	sendProduce := func(topic string) int16 {
		header := &protocol.RequestHeader{ApiKey: protocol.ApiKeyProduce, CorrelationId: 1, ClientId: "traversal-test"}
		body := new(bytes.Buffer)
		_ = protocol.EncodeProduceRequest(body, &protocol.ProduceRequest{
			Acks:    1,
			Timeout: 1000,
			Topics: []protocol.TopicProduceData{{
				TopicName:  topic,
				Partitions: []protocol.PartitionProduceData{{PartitionId: 0, RecordsData: mustMarshalRecord(t, "payload")}},
			}},
		})
		respBuf := new(bytes.Buffer)
		if err := handler.HandleRequest(header, body, respBuf, security.NewSASLSession()); err != nil {
			t.Fatalf("HandleRequest failed: %v", err)
		}
		respWithCorrId := new(bytes.Buffer)
		_ = protocol.WriteInt32(respWithCorrId, 1)
		respWithCorrId.Write(respBuf.Bytes())
		resp := decodeProduceResponse(t, respWithCorrId.Bytes())
		return resp.Topics[0].Partitions[0].ErrorCode
	}

	if code := sendProduce("../../evil-traversal"); code == 0 {
		t.Error("expected a path-traversal topic name to be rejected, got ErrorCode 0")
	}
	if _, err := os.Stat(filepath.Join(tmpDir, "evil-traversal-0")); err == nil {
		t.Error("expected no directory to be created outside dataDir for a path-traversal topic name")
	}

	if code := sendProduce("legit-topic"); code != 0 {
		t.Errorf("expected an ordinary topic name to be accepted, got ErrorCode %d", code)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "legit-topic-0")); err != nil {
		t.Errorf("expected a directory to be created under dataDir for a valid topic name: %v", err)
	}
}

func TestIsValidTopicName(t *testing.T) {
	valid := []string{"orders", "my.topic.name", "topic_with_underscores", "topic-with-dashes", "__consumer_offsets"}
	for _, name := range valid {
		if !isValidTopicName(name) {
			t.Errorf("expected %q to be a valid topic name", name)
		}
	}

	invalid := []string{"", ".", "..", "../etc/evil", "a/b", "a\\b", strings.Repeat("a", 250)}
	for _, name := range invalid {
		if isValidTopicName(name) {
			t.Errorf("expected %q to be rejected as an invalid topic name", name)
		}
	}
}
