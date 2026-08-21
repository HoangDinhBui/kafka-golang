package protocol

import (
	"bytes"
	"reflect"
	"testing"
)

func TestOffsetCommitProtocol(t *testing.T) {
	req := &OffsetCommitRequest{
		GroupId:      "test-group",
		GenerationId: 1,
		MemberId:     "member-1",
		RetentionTime: 86400000,
		Topics: []OffsetCommitTopic{
			{
				TopicName: "orders",
				Partitions: []OffsetCommitPartition{
					{PartitionIndex: 0, CommittedOffset: 150, Metadata: "meta-p0"},
					{PartitionIndex: 1, CommittedOffset: 300, Metadata: "meta-p1"},
				},
			},
		},
	}

	var buf bytes.Buffer
	_ = WriteString(&buf, req.GroupId)
	_ = WriteInt32(&buf, req.GenerationId)
	_ = WriteString(&buf, req.MemberId)
	_ = WriteInt64(&buf, req.RetentionTime)
	_ = WriteInt32(&buf, int32(len(req.Topics)))

	for _, topic := range req.Topics {
		_ = WriteString(&buf, topic.TopicName)
		_ = WriteInt32(&buf, int32(len(topic.Partitions)))
		for _, p := range topic.Partitions {
			_ = WriteInt32(&buf, p.PartitionIndex)
			_ = WriteInt64(&buf, p.CommittedOffset)
			_ = WriteString(&buf, p.Metadata)
		}
	}

	decoded, err := DecodeOffsetCommitRequest(&buf)
	if err != nil {
		t.Fatalf("unexpected error decoding OffsetCommitRequest: %v", err)
	}

	if !reflect.DeepEqual(req, decoded) {
		t.Errorf("decoded OffsetCommitRequest mismatch.\nExpected: %+v\nGot: %+v", req, decoded)
	}

	// Test Response Encoding
	res := &OffsetCommitResponse{
		Topics: []OffsetCommitResponseTopic{
			{
				TopicName: "orders",
				Partitions: []OffsetCommitResponsePartition{
					{PartitionIndex: 0, ErrorCode: 0},
					{PartitionIndex: 1, ErrorCode: 0},
				},
			},
		},
	}

	var resBuf bytes.Buffer
	err = EncodeOffsetCommitResponse(&resBuf, res)
	if err != nil {
		t.Fatalf("unexpected error encoding OffsetCommitResponse: %v", err)
	}

	if resBuf.Len() == 0 {
		t.Error("expected non-empty response buffer")
	}
}

func TestOffsetFetchProtocol(t *testing.T) {
	req := &OffsetFetchRequest{
		GroupId: "fetch-group",
		Topics: []OffsetFetchTopic{
			{
				TopicName:        "payments",
				PartitionIndexes: []int32{0, 1, 2},
			},
		},
	}

	var buf bytes.Buffer
	_ = WriteString(&buf, req.GroupId)
	_ = WriteInt32(&buf, int32(len(req.Topics)))
	for _, topic := range req.Topics {
		_ = WriteString(&buf, topic.TopicName)
		_ = WriteInt32(&buf, int32(len(topic.PartitionIndexes)))
		for _, pIdx := range topic.PartitionIndexes {
			_ = WriteInt32(&buf, pIdx)
		}
	}

	decoded, err := DecodeOffsetFetchRequest(&buf)
	if err != nil {
		t.Fatalf("unexpected error decoding OffsetFetchRequest: %v", err)
	}

	if !reflect.DeepEqual(req, decoded) {
		t.Errorf("decoded OffsetFetchRequest mismatch.\nExpected: %+v\nGot: %+v", req, decoded)
	}
}

func TestJoinGroupProtocol(t *testing.T) {
	req := &JoinGroupRequest{
		GroupId:            "join-group",
		SessionTimeoutMs:   10000,
		RebalanceTimeoutMs: 30000,
		MemberId:           "member-xyz",
		ProtocolType:       "consumer",
		Protocols: []GroupProtocol{
			{Name: "range", Metadata: []byte{1, 2, 3}},
		},
	}

	var buf bytes.Buffer
	_ = WriteString(&buf, req.GroupId)
	_ = WriteInt32(&buf, req.SessionTimeoutMs)
	_ = WriteInt32(&buf, req.RebalanceTimeoutMs)
	_ = WriteString(&buf, req.MemberId)
	_ = WriteString(&buf, req.ProtocolType)
	_ = WriteInt32(&buf, int32(len(req.Protocols)))
	for _, p := range req.Protocols {
		_ = WriteString(&buf, p.Name)
		_ = WriteBytes(&buf, p.Metadata)
	}

	decoded, err := DecodeJoinGroupRequest(&buf)
	if err != nil {
		t.Fatalf("unexpected error decoding JoinGroupRequest: %v", err)
	}

	if !reflect.DeepEqual(req, decoded) {
		t.Errorf("decoded JoinGroupRequest mismatch.\nExpected: %+v\nGot: %+v", req, decoded)
	}

	res := &JoinGroupResponse{
		ErrorCode:    0,
		GenerationId: 2,
		ProtocolName: "range",
		LeaderId:     "leader-1",
		MemberId:     "member-xyz",
		Members: []JoinGroupResponseMember{
			{MemberId: "leader-1", Metadata: []byte{10, 20}},
			{MemberId: "member-xyz", Metadata: []byte{30, 40}},
		},
	}

	var resBuf bytes.Buffer
	err = EncodeJoinGroupResponse(&resBuf, res)
	if err != nil {
		t.Fatalf("unexpected error encoding JoinGroupResponse: %v", err)
	}
}

func TestSyncGroupAndHeartbeatProtocol(t *testing.T) {
	// SyncGroup Test
	syncReq := &SyncGroupRequest{
		GroupId:      "sync-group",
		GenerationId: 1,
		MemberId:     "leader-1",
		GroupAssignments: []GroupAssignment{
			{MemberId: "member-1", Assignment: []byte{0, 1}},
			{MemberId: "member-2", Assignment: []byte{2, 3}},
		},
	}

	var syncBuf bytes.Buffer
	_ = WriteString(&syncBuf, syncReq.GroupId)
	_ = WriteInt32(&syncBuf, syncReq.GenerationId)
	_ = WriteString(&syncBuf, syncReq.MemberId)
	_ = WriteInt32(&syncBuf, int32(len(syncReq.GroupAssignments)))
	for _, a := range syncReq.GroupAssignments {
		_ = WriteString(&syncBuf, a.MemberId)
		_ = WriteBytes(&syncBuf, a.Assignment)
	}

	decodedSync, err := DecodeSyncGroupRequest(&syncBuf)
	if err != nil {
		t.Fatalf("unexpected error decoding SyncGroupRequest: %v", err)
	}
	if !reflect.DeepEqual(syncReq, decodedSync) {
		t.Errorf("decoded SyncGroupRequest mismatch.\nExpected: %+v\nGot: %+v", syncReq, decodedSync)
	}

	// Heartbeat Test
	hbReq := &HeartbeatRequest{
		GroupId:      "hb-group",
		GenerationId: 1,
		MemberId:     "m-1",
	}

	var hbBuf bytes.Buffer
	_ = WriteString(&hbBuf, hbReq.GroupId)
	_ = WriteInt32(&hbBuf, hbReq.GenerationId)
	_ = WriteString(&hbBuf, hbReq.MemberId)

	decodedHb, err := DecodeHeartbeatRequest(&hbBuf)
	if err != nil {
		t.Fatalf("unexpected error decoding HeartbeatRequest: %v", err)
	}
	if !reflect.DeepEqual(hbReq, decodedHb) {
		t.Errorf("decoded HeartbeatRequest mismatch.\nExpected: %+v\nGot: %+v", hbReq, decodedHb)
	}
}

// ============================================================================
// TEST: TestFindCoordinatorProtocol
// Description: Regression test for the missing FindCoordinator (ApiKey 10)
//              API: real client libraries send this before JoinGroup to
//              discover the coordinator broker, so without request/response
//              encoding for it a client cannot even begin the
//              consumer-group flow.
// ============================================================================
func TestFindCoordinatorProtocol(t *testing.T) {
	req := &FindCoordinatorRequest{Key: "my-consumer-group"}

	var reqBuf bytes.Buffer
	_ = WriteString(&reqBuf, req.Key)

	decoded, err := DecodeFindCoordinatorRequest(&reqBuf)
	if err != nil {
		t.Fatalf("unexpected error decoding FindCoordinatorRequest: %v", err)
	}
	if !reflect.DeepEqual(req, decoded) {
		t.Errorf("decoded FindCoordinatorRequest mismatch.\nExpected: %+v\nGot: %+v", req, decoded)
	}

	resp := &FindCoordinatorResponse{ErrorCode: 0, NodeId: 1, Host: "127.0.0.1", Port: 9092}
	var respBuf bytes.Buffer
	if err := EncodeFindCoordinatorResponse(&respBuf, resp); err != nil {
		t.Fatalf("EncodeFindCoordinatorResponse failed: %v", err)
	}

	errCode, err := ReadInt16(&respBuf)
	if err != nil || errCode != 0 {
		t.Fatalf("expected ErrorCode 0, got %d, err: %v", errCode, err)
	}
	nodeId, err := ReadInt32(&respBuf)
	if err != nil || nodeId != 1 {
		t.Fatalf("expected NodeId 1, got %d, err: %v", nodeId, err)
	}
	host, err := ReadString(&respBuf)
	if err != nil || host != "127.0.0.1" {
		t.Fatalf("expected Host 127.0.0.1, got %q, err: %v", host, err)
	}
	port, err := ReadInt32(&respBuf)
	if err != nil || port != 9092 {
		t.Fatalf("expected Port 9092, got %d, err: %v", port, err)
	}
}

// ============================================================================
// TEST: TestLeaveGroupProtocol
// Description: Regression test for the missing LeaveGroup (ApiKey 13) API:
//              the ApiKey constant existed but no request/response encoding
//              or handler existed anywhere, so a client sending it got
//              "unsupported ApiKey" instead of leaving the group cleanly.
// ============================================================================
func TestLeaveGroupProtocol(t *testing.T) {
	req := &LeaveGroupRequest{GroupId: "leave-group", MemberId: "member-1"}

	var reqBuf bytes.Buffer
	_ = WriteString(&reqBuf, req.GroupId)
	_ = WriteString(&reqBuf, req.MemberId)

	decoded, err := DecodeLeaveGroupRequest(&reqBuf)
	if err != nil {
		t.Fatalf("unexpected error decoding LeaveGroupRequest: %v", err)
	}
	if !reflect.DeepEqual(req, decoded) {
		t.Errorf("decoded LeaveGroupRequest mismatch.\nExpected: %+v\nGot: %+v", req, decoded)
	}

	var respBuf bytes.Buffer
	if err := EncodeLeaveGroupResponse(&respBuf, &LeaveGroupResponse{ErrorCode: 0}); err != nil {
		t.Fatalf("EncodeLeaveGroupResponse failed: %v", err)
	}
	errCode, err := ReadInt16(&respBuf)
	if err != nil || errCode != 0 {
		t.Fatalf("expected ErrorCode 0, got %d, err: %v", errCode, err)
	}
}
