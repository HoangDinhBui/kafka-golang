package protocol

import (
	"io"
)

// ============================================================================
// STRUCT: InitProducerIdRequest (ApiKey 22)
// Description: Sent by transactional or idempotent producers to obtain a unique
//              ProducerId and Epoch from the broker.
// ============================================================================
type InitProducerIdRequest struct {
	TransactionalId      *string // Optional transactional ID (nil for non-transactional idempotent producers)
	TransactionTimeoutMs int32   // Transaction timeout in milliseconds
}

type InitProducerIdResponse struct {
	ErrorCode     int16
	ProducerId    int64
	ProducerEpoch int16
}

// DecodeInitProducerIdRequest decodes InitProducerIdRequest from stream
func DecodeInitProducerIdRequest(r io.Reader) (*InitProducerIdRequest, error) {
	txId, err := ReadNullableString(r)
	if err != nil {
		return nil, err
	}

	timeout, err := ReadInt32(r)
	if err != nil {
		return nil, err
	}

	return &InitProducerIdRequest{
		TransactionalId:      txId,
		TransactionTimeoutMs: timeout,
	}, nil
}

// EncodeInitProducerIdResponse encodes InitProducerIdResponse to stream
func EncodeInitProducerIdResponse(w io.Writer, resp *InitProducerIdResponse) error {
	if err := WriteInt16(w, resp.ErrorCode); err != nil {
		return err
	}
	if err := WriteInt64(w, resp.ProducerId); err != nil {
		return err
	}
	return WriteInt16(w, resp.ProducerEpoch)
}

// ============================================================================
// STRUCT: AddPartitionsToTxnRequest (ApiKey 24)
// Description: Registers topic partitions involved in an ongoing transaction.
// ============================================================================
type AddPartitionsToTxnTopic struct {
	TopicName  string
	Partitions []int32
}

type AddPartitionsToTxnRequest struct {
	TransactionalId string
	ProducerId      int64
	ProducerEpoch   int16
	Topics          []AddPartitionsToTxnTopic
}

type AddPartitionsToTxnResult struct {
	PartitionId int32
	ErrorCode   int16
}

type AddPartitionsToTxnTopicResult struct {
	TopicName  string
	Partitions []AddPartitionsToTxnResult
}

type AddPartitionsToTxnResponse struct {
	Results []AddPartitionsToTxnTopicResult
}

// DecodeAddPartitionsToTxnRequest decodes AddPartitionsToTxnRequest from stream
func DecodeAddPartitionsToTxnRequest(r io.Reader) (*AddPartitionsToTxnRequest, error) {
	txId, err := ReadString(r)
	if err != nil {
		return nil, err
	}

	pid, err := ReadInt64(r)
	if err != nil {
		return nil, err
	}

	epoch, err := ReadInt16(r)
	if err != nil {
		return nil, err
	}

	topicLen, err := ReadArrayCount(r)
	if err != nil {
		return nil, err
	}

	topics := make([]AddPartitionsToTxnTopic, topicLen)
	for i := int32(0); i < topicLen; i++ {
		tName, err := ReadString(r)
		if err != nil {
			return nil, err
		}

		pLen, err := ReadArrayCount(r)
		if err != nil {
			return nil, err
		}

		parts := make([]int32, pLen)
		for j := int32(0); j < pLen; j++ {
			pId, err := ReadInt32(r)
			if err != nil {
				return nil, err
			}
			parts[j] = pId
		}

		topics[i] = AddPartitionsToTxnTopic{
			TopicName:  tName,
			Partitions: parts,
		}
	}

	return &AddPartitionsToTxnRequest{
		TransactionalId: txId,
		ProducerId:      pid,
		ProducerEpoch:   epoch,
		Topics:          topics,
	}, nil
}

// EncodeAddPartitionsToTxnResponse encodes AddPartitionsToTxnResponse to stream
func EncodeAddPartitionsToTxnResponse(w io.Writer, resp *AddPartitionsToTxnResponse) error {
	if err := WriteInt32(w, int32(len(resp.Results))); err != nil {
		return err
	}

	for _, tRes := range resp.Results {
		if err := WriteString(w, tRes.TopicName); err != nil {
			return err
		}
		if err := WriteInt32(w, int32(len(tRes.Partitions))); err != nil {
			return err
		}

		for _, pRes := range tRes.Partitions {
			if err := WriteInt32(w, pRes.PartitionId); err != nil {
				return err
			}
			if err := WriteInt16(w, pRes.ErrorCode); err != nil {
				return err
			}
		}
	}
	return nil
}

// ============================================================================
// STRUCT: EndTxnRequest (ApiKey 26)
// Description: Sent by producer to commit or abort an active transaction.
// ============================================================================
type EndTxnRequest struct {
	TransactionalId string
	ProducerId      int64
	ProducerEpoch   int16
	Committed       bool // true = COMMIT, false = ABORT
}

type EndTxnResponse struct {
	ErrorCode int16
}

// DecodeEndTxnRequest decodes EndTxnRequest from stream
func DecodeEndTxnRequest(r io.Reader) (*EndTxnRequest, error) {
	txId, err := ReadString(r)
	if err != nil {
		return nil, err
	}

	pid, err := ReadInt64(r)
	if err != nil {
		return nil, err
	}

	epoch, err := ReadInt16(r)
	if err != nil {
		return nil, err
	}

	committedByte, err := ReadInt8(r)
	if err != nil {
		return nil, err
	}

	return &EndTxnRequest{
		TransactionalId: txId,
		ProducerId:      pid,
		ProducerEpoch:   epoch,
		Committed:       committedByte != 0,
	}, nil
}

// EncodeEndTxnResponse encodes EndTxnResponse to stream
func EncodeEndTxnResponse(w io.Writer, resp *EndTxnResponse) error {
	return WriteInt16(w, resp.ErrorCode)
}
