package protocol

import (
	"bytes"
	"testing"
)

// ============================================================================
// TEST: TestRequestResponseHeader
// Description: Verifies encoding and decoding of Kafka Request and Response headers.
// ============================================================================
func TestRequestResponseHeader(t *testing.T) {
	buf := new(bytes.Buffer)

	// Encode RequestHeader fields
	_ = WriteInt16(buf, ApiKeyProduce)
	_ = WriteInt16(buf, 2)
	_ = WriteInt32(buf, 1001)
	_ = WriteString(buf, "test-client-id")

	header, err := DecodeRequestHeader(buf)
	if err != nil {
		t.Fatalf("DecodeRequestHeader failed: %v", err)
	}

	if header.ApiKey != ApiKeyProduce {
		t.Errorf("Expected ApiKey %d, got %d", ApiKeyProduce, header.ApiKey)
	}
	if header.ApiVersion != 2 {
		t.Errorf("Expected ApiVersion 2, got %d", header.ApiVersion)
	}
	if header.CorrelationId != 1001 {
		t.Errorf("Expected CorrelationId 1001, got %d", header.CorrelationId)
	}
	if header.ClientId != "test-client-id" {
		t.Errorf("Expected ClientId test-client-id, got %s", header.ClientId)
	}

	// Test ResponseHeader
	resBuf := new(bytes.Buffer)
	respHeader := &ResponseHeader{CorrelationId: 1001}
	if err := EncodeResponseHeader(resBuf, respHeader); err != nil {
		t.Fatalf("EncodeResponseHeader failed: %v", err)
	}

	readCorrId, err := ReadInt32(resBuf)
	if err != nil {
		t.Fatalf("ReadInt32 for ResponseHeader failed: %v", err)
	}
	if readCorrId != 1001 {
		t.Errorf("Expected Response CorrelationId 1001, got %d", readCorrId)
	}
}

// ============================================================================
// TEST: TestApiVersionsResponse
// Description: Verifies encoding of default ApiVersions response.
// ============================================================================
func TestApiVersionsResponse(t *testing.T) {
	resp := DefaultApiVersionResponse()
	buf := new(bytes.Buffer)

	if err := EncodeApiVersionResponse(buf, resp); err != nil {
		t.Fatalf("EncodeApiVersionResponse failed: %v", err)
	}

	errCode, err := ReadInt16(buf)
	if err != nil {
		t.Fatalf("Failed to read ErrorCode: %v", err)
	}
	if errCode != 0 {
		t.Errorf("Expected ErrorCode 0, got %d", errCode)
	}

	count, err := ReadInt32(buf)
	if err != nil {
		t.Fatalf("Failed to read ApiKeys count: %v", err)
	}
	if count != int32(len(resp.ApiKeys)) {
		t.Errorf("Expected ApiKeys count %d, got %d", len(resp.ApiKeys), count)
	}
}

// ============================================================================
// TEST: TestMetadataRequestResponse
// Description: Verifies encoding and decoding of Metadata requests and responses.
// ============================================================================
func TestMetadataRequestResponse(t *testing.T) {
	// Encode MetadataRequest
	buf := new(bytes.Buffer)
	_ = WriteInt32(buf, 2)
	_ = WriteString(buf, "topic-a")
	_ = WriteString(buf, "topic-b")

	req, err := DecodeMetadataRequest(buf)
	if err != nil {
		t.Fatalf("DecodeMetadataRequest failed: %v", err)
	}

	if len(req.Topics) != 2 {
		t.Fatalf("Expected 2 topics, got %d", len(req.Topics))
	}
	if req.Topics[0] != "topic-a" || req.Topics[1] != "topic-b" {
		t.Errorf("Unexpected topics decoded: %v", req.Topics)
	}

	// Encode MetadataResponse
	resp := &MetadataResponse{
		Brokers: []BrokerMetadata{
			{NodeId: 1, Host: "127.0.0.1", Port: 9092},
		},
		Topics: []TopicMetadata{
			{
				ErrorCode: 0,
				TopicName: "topic-a",
				Partitions: []PartitionMetadata{
					{
						ErrorCode:      0,
						PartitionId:    0,
						LeaderId:       1,
						Replicas:       []int32{1},
						InSyncReplicas: []int32{1},
					},
				},
			},
		},
	}

	resBuf := new(bytes.Buffer)
	if err := EncodeMetadataResponse(resBuf, resp); err != nil {
		t.Fatalf("EncodeMetadataResponse failed: %v", err)
	}

	brokerCount, err := ReadInt32(resBuf)
	if err != nil || brokerCount != 1 {
		t.Fatalf("Failed to read broker count: %v", err)
	}

	nodeId, _ := ReadInt32(resBuf)
	host, _ := ReadString(resBuf)
	port, _ := ReadInt32(resBuf)

	if nodeId != 1 || host != "127.0.0.1" || port != 9092 {
		t.Errorf("Decoded broker info mismatch: %d, %s, %d", nodeId, host, port)
	}
}

// ============================================================================
// TEST: TestProduceRequestResponse
// Description: Verifies encoding and decoding of Produce requests and responses.
// ============================================================================
func TestProduceRequestResponse(t *testing.T) {
	buf := new(bytes.Buffer)
	_ = WriteInt16(buf, 1)    // Acks = 1
	_ = WriteInt32(buf, 1000) // Timeout = 1000ms
	_ = WriteInt32(buf, 1)    // Topic count = 1
	_ = WriteString(buf, "test-topic")
	_ = WriteInt32(buf, 1) // Partition count = 1
	_ = WriteInt32(buf, 0) // PartitionId = 0

	payload := []byte("raw-message-data")
	_ = WriteInt32(buf, int32(len(payload)))
	buf.Write(payload)

	req, err := DecodeProduceRequest(buf)
	if err != nil {
		t.Fatalf("DecodeProduceRequest failed: %v", err)
	}

	if req.Acks != 1 || req.Timeout != 1000 {
		t.Errorf("ProduceRequest metadata mismatch: Acks %d, Timeout %d", req.Acks, req.Timeout)
	}
	if len(req.Topics) != 1 || req.Topics[0].TopicName != "test-topic" {
		t.Fatalf("ProduceRequest topic mismatch")
	}

	// Test ProduceResponse Encoding
	respBuf := new(bytes.Buffer)
	resp := &ProduceResponse{
		Topics: []TopicProduceResponse{
			{
				TopicName: "test-topic",
				Partitions: []PartitionProduceResponse{
					{PartitionId: 0, ErrorCode: 0, BaseOffset: 42, LogAppendTime: -1},
				},
			},
		},
	}

	if err := EncodeProduceResponse(respBuf, resp); err != nil {
		t.Fatalf("EncodeProduceResponse failed: %v", err)
	}
}

// ============================================================================
// TEST: TestFetchRequestResponse
// Description: Verifies encoding and decoding of Fetch requests and responses.
// ============================================================================
func TestFetchRequestResponse(t *testing.T) {
	buf := new(bytes.Buffer)
	_ = WriteInt32(buf, -1)   // ReplicaId = -1
	_ = WriteInt32(buf, 500)  // MaxWaitTime = 500ms
	_ = WriteInt32(buf, 1)    // MinBytes = 1
	_ = WriteInt32(buf, 1)    // Topic count = 1
	_ = WriteString(buf, "test-topic")
	_ = WriteInt32(buf, 1)   // Partition count = 1
	_ = WriteInt32(buf, 0)   // PartitionId = 0
	_ = WriteInt64(buf, 100) // FetchOffset = 100
	_ = WriteInt32(buf, 1024)// MaxBytes = 1024

	req, err := DecodeFetchRequest(buf)
	if err != nil {
		t.Fatalf("DecodeFetchRequest failed: %v", err)
	}

	if req.ReplicaId != -1 || req.MaxWaitTime != 500 {
		t.Errorf("FetchRequest metadata mismatch")
	}

	// Test FetchResponse Encoding
	respBuf := new(bytes.Buffer)
	resp := &FetchResponse{
		Topics: []TopicFetchResponse{
			{
				TopicName: "test-topic",
				Partitions: []PartitionFetchResponse{
					{PartitionId: 0, ErrorCode: 0, HighWatermark: 105, RecordsData: []byte("fetched-data")},
				},
			},
		},
	}

	if err := EncodeFetchResponse(respBuf, resp); err != nil {
		t.Fatalf("EncodeFetchResponse failed: %v", err)
	}
}

// ============================================================================
// TEST: TestReadArrayCount_RejectsImplausiblyLargeCounts
// Description: Regression test for a forged-length amplification bug: a
//              client-supplied array/list count was passed straight into
//              make([]T, count) by every decoder (Produce, Fetch, JoinGroup,
//              SyncGroup, OffsetCommit, OffsetFetch, AddPartitionsToTxn,
//              Metadata) with no upper bound, so a single 4-byte forged
//              count near math.MaxInt32 could force a multi-gigabyte
//              allocation attempt. ReadArrayCount now rejects it instead.
// ============================================================================
func TestReadArrayCount_RejectsImplausiblyLargeCounts(t *testing.T) {
	buf := new(bytes.Buffer)
	_ = WriteInt32(buf, 2000000000) // ~2 billion, would size a multi-GB slice

	if _, err := ReadArrayCount(buf); err == nil {
		t.Error("expected ReadArrayCount to reject an implausibly large count, got nil error")
	}
}

func TestReadArrayCount_NegativeNormalizesToZero(t *testing.T) {
	buf := new(bytes.Buffer)
	_ = WriteInt32(buf, -1)

	count, err := ReadArrayCount(buf)
	if err != nil {
		t.Fatalf("expected no error for a negative (null-array) count, got %v", err)
	}
	if count != 0 {
		t.Errorf("expected negative count to normalize to 0, got %d", count)
	}
}

func TestReadArrayCount_AllowsReasonableCounts(t *testing.T) {
	buf := new(bytes.Buffer)
	_ = WriteInt32(buf, 100)

	count, err := ReadArrayCount(buf)
	if err != nil {
		t.Fatalf("expected a reasonable count to be accepted, got %v", err)
	}
	if count != 100 {
		t.Errorf("expected count 100, got %d", count)
	}
}

// ============================================================================
// TEST: TestReadBytes_RejectsImplausiblyLargeLength
// Description: Regression test for the same amplification bug applied to
//              raw byte fields (RecordsData, SASL AuthData, group assignment
//              metadata): a forged int32 length near math.MaxInt32 could
//              force a multi-gigabyte make([]byte, length) from 4 bytes of
//              input, regardless of how much data actually followed.
// ============================================================================
func TestReadBytes_RejectsImplausiblyLargeLength(t *testing.T) {
	buf := new(bytes.Buffer)
	_ = WriteInt32(buf, 2000000000) // ~2 billion bytes claimed, none actually sent

	if _, err := ReadBytes(buf); err == nil {
		t.Error("expected ReadBytes to reject an implausibly large length, got nil error")
	}
}

func TestReadBytes_AllowsReasonableLength(t *testing.T) {
	buf := new(bytes.Buffer)
	payload := []byte("hello-world")
	_ = WriteBytes(buf, payload)

	got, err := ReadBytes(buf)
	if err != nil {
		t.Fatalf("expected a reasonable byte field to be accepted, got %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("expected %q, got %q", payload, got)
	}
}

// ============================================================================
// TEST: TestDecoders_TruncatedInputErrorsWithoutPanic
// Description: Feeds each request decoder a claimed-but-truncated payload
//              (a plausible array count with no actual elements behind it)
//              to verify malformed/adversarial input produces a decode
//              error rather than a panic (e.g. index-out-of-range or a huge
//              allocation attempt).
// ============================================================================
func TestDecoders_TruncatedInputErrorsWithoutPanic(t *testing.T) {
	buildTruncated := func(count int32) *bytes.Buffer {
		buf := new(bytes.Buffer)
		_ = WriteInt32(buf, count) // claim `count` elements, then provide none
		return buf
	}

	cases := []struct {
		name   string
		decode func(*bytes.Buffer) error
	}{
		{"Metadata", func(b *bytes.Buffer) error { _, err := DecodeMetadataRequest(b); return err }},
		{"JoinGroup", func(b *bytes.Buffer) error {
			full := new(bytes.Buffer)
			_ = WriteString(full, "group")
			_ = WriteInt32(full, 1000)
			_ = WriteInt32(full, 1000)
			_ = WriteString(full, "member")
			_ = WriteString(full, "consumer")
			full.Write(b.Bytes())
			_, err := DecodeJoinGroupRequest(full)
			return err
		}},
		{"SyncGroup", func(b *bytes.Buffer) error {
			full := new(bytes.Buffer)
			_ = WriteString(full, "group")
			_ = WriteInt32(full, 1)
			_ = WriteString(full, "member")
			full.Write(b.Bytes())
			_, err := DecodeSyncGroupRequest(full)
			return err
		}},
		{"OffsetCommit", func(b *bytes.Buffer) error {
			full := new(bytes.Buffer)
			_ = WriteString(full, "group")
			_ = WriteInt32(full, 1)
			_ = WriteString(full, "member")
			_ = WriteInt64(full, -1)
			full.Write(b.Bytes())
			_, err := DecodeOffsetCommitRequest(full)
			return err
		}},
		{"OffsetFetch", func(b *bytes.Buffer) error {
			full := new(bytes.Buffer)
			_ = WriteString(full, "group")
			full.Write(b.Bytes())
			_, err := DecodeOffsetFetchRequest(full)
			return err
		}},
		{"AddPartitionsToTxn", func(b *bytes.Buffer) error {
			full := new(bytes.Buffer)
			_ = WriteString(full, "txn-id")
			_ = WriteInt64(full, 1)
			_ = WriteInt16(full, 0)
			full.Write(b.Bytes())
			_, err := DecodeAddPartitionsToTxnRequest(full)
			return err
		}},
		{"Produce", func(b *bytes.Buffer) error {
			full := new(bytes.Buffer)
			_ = WriteInt16(full, 1)
			_ = WriteInt32(full, 1000)
			full.Write(b.Bytes())
			_, err := DecodeProduceRequest(full)
			return err
		}},
		{"Fetch", func(b *bytes.Buffer) error {
			full := new(bytes.Buffer)
			_ = WriteInt32(full, -1)
			_ = WriteInt32(full, 500)
			_ = WriteInt32(full, 1)
			full.Write(b.Bytes())
			_, err := DecodeFetchRequest(full)
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("decoder panicked on truncated input: %v", r)
				}
			}()
			// Claim 5 elements but supply none: must error, never panic.
			if err := tc.decode(buildTruncated(5)); err == nil {
				t.Error("expected an error decoding a truncated payload, got nil")
			}
		})
	}
}
