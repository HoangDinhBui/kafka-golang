package protocol

import (
	"bytes"
	"testing"
)

func TestInitProducerIdProtocol(t *testing.T) {
	txId := "tx-app-01"
	req := &InitProducerIdRequest{
		TransactionalId:      &txId,
		TransactionTimeoutMs: 60000,
	}

	buf := new(bytes.Buffer)
	if err := WriteNullableString(buf, req.TransactionalId); err != nil {
		t.Fatalf("Failed to write txId: %v", err)
	}
	if err := WriteInt32(buf, req.TransactionTimeoutMs); err != nil {
		t.Fatalf("Failed to write timeout: %v", err)
	}

	decoded, err := DecodeInitProducerIdRequest(buf)
	if err != nil {
		t.Fatalf("DecodeInitProducerIdRequest failed: %v", err)
	}

	if decoded.TransactionalId == nil || *decoded.TransactionalId != txId {
		t.Errorf("Expected txId '%s', got %v", txId, decoded.TransactionalId)
	}
	if decoded.TransactionTimeoutMs != 60000 {
		t.Errorf("Expected timeout 60000, got %d", decoded.TransactionTimeoutMs)
	}

	resp := &InitProducerIdResponse{
		ErrorCode:     0,
		ProducerId:    1001,
		ProducerEpoch: 1,
	}
	respBuf := new(bytes.Buffer)
	if err := EncodeInitProducerIdResponse(respBuf, resp); err != nil {
		t.Fatalf("EncodeInitProducerIdResponse failed: %v", err)
	}

	if respBuf.Len() != 12 { // int16 + int64 + int16 = 2 + 8 + 2 = 12 bytes
		t.Errorf("Expected response length 12, got %d", respBuf.Len())
	}
}

func TestEndTxnProtocol(t *testing.T) {
	req := &EndTxnRequest{
		TransactionalId: "tx-app-01",
		ProducerId:      1001,
		ProducerEpoch:   1,
		Committed:       true,
	}

	buf := new(bytes.Buffer)
	_ = WriteString(buf, req.TransactionalId)
	_ = WriteInt64(buf, req.ProducerId)
	_ = WriteInt16(buf, req.ProducerEpoch)
	_ = WriteInt8(buf, 1)

	decoded, err := DecodeEndTxnRequest(buf)
	if err != nil {
		t.Fatalf("DecodeEndTxnRequest failed: %v", err)
	}

	if decoded.TransactionalId != "tx-app-01" || decoded.ProducerId != 1001 || !decoded.Committed {
		t.Errorf("Unexpected decoded EndTxnRequest: %+v", decoded)
	}
}
