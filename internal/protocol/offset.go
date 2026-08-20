package protocol

import (
	"io"
)

// ============================================================================
// STRUCT: OffsetCommitPartition
// Description: Partition details for an OffsetCommitRequest.
// ============================================================================
type OffsetCommitPartition struct {
	PartitionIndex int32  // Partition index (0-indexed)
	CommittedOffset int64  // Offset position to commit
	Metadata        string // Optional client metadata
}

// ============================================================================
// STRUCT: OffsetCommitTopic
// Description: Topic details for an OffsetCommitRequest.
// ============================================================================
type OffsetCommitTopic struct {
	TopicName  string                  // Name of the target topic
	Partitions []OffsetCommitPartition // Partition offset commits
}

// ============================================================================
// STRUCT: OffsetCommitRequest
// Description: Request payload for ApiKey 8 (OffsetCommit).
// ============================================================================
type OffsetCommitRequest struct {
	GroupId      string              // Consumer group identifier
	GenerationId int32               // Group generation ID
	MemberId     string              // Consumer member identifier
	RetentionTime int64              // Offset retention time in milliseconds
	Topics       []OffsetCommitTopic // Topics to commit offsets for
}

// ============================================================================
// STRUCT: OffsetCommitResponsePartition
// Description: Partition commit result for an OffsetCommitResponse.
// ============================================================================
type OffsetCommitResponsePartition struct {
	PartitionIndex int32 // Partition index
	ErrorCode      int16 // Error code (0 = NONE)
}

// ============================================================================
// STRUCT: OffsetCommitResponseTopic
// Description: Topic commit result for an OffsetCommitResponse.
// ============================================================================
type OffsetCommitResponseTopic struct {
	TopicName  string                          // Topic name
	Partitions []OffsetCommitResponsePartition // Partition results
}

// ============================================================================
// STRUCT: OffsetCommitResponse
// Description: Response payload for ApiKey 8 (OffsetCommit).
// ============================================================================
type OffsetCommitResponse struct {
	Topics []OffsetCommitResponseTopic // Results per topic
}

// ============================================================================
// FUNCTION: DecodeOffsetCommitRequest
// Description: Reads and parses an OffsetCommitRequest from an io.Reader stream.
// ============================================================================
func DecodeOffsetCommitRequest(r io.Reader) (*OffsetCommitRequest, error) {
	groupId, err := ReadString(r)
	if err != nil {
		return nil, err
	}

	generationId, err := ReadInt32(r)
	if err != nil {
		return nil, err
	}

	memberId, err := ReadString(r)
	if err != nil {
		return nil, err
	}

	retentionTime, err := ReadInt64(r)
	if err != nil {
		return nil, err
	}

	numTopics, err := ReadArrayCount(r)
	if err != nil {
		return nil, err
	}

	topics := make([]OffsetCommitTopic, numTopics)
	for i := int32(0); i < numTopics; i++ {
		topicName, err := ReadString(r)
		if err != nil {
			return nil, err
		}

		numPartitions, err := ReadArrayCount(r)
		if err != nil {
			return nil, err
		}

		partitions := make([]OffsetCommitPartition, numPartitions)
		for j := int32(0); j < numPartitions; j++ {
			pIdx, err := ReadInt32(r)
			if err != nil {
				return nil, err
			}
			offset, err := ReadInt64(r)
			if err != nil {
				return nil, err
			}
			meta, err := ReadString(r)
			if err != nil {
				return nil, err
			}

			partitions[j] = OffsetCommitPartition{
				PartitionIndex:  pIdx,
				CommittedOffset: offset,
				Metadata:        meta,
			}
		}

		topics[i] = OffsetCommitTopic{
			TopicName:  topicName,
			Partitions: partitions,
		}
	}

	return &OffsetCommitRequest{
		GroupId:       groupId,
		GenerationId:  generationId,
		MemberId:      memberId,
		RetentionTime: retentionTime,
		Topics:        topics,
	}, nil
}

// ============================================================================
// FUNCTION: EncodeOffsetCommitResponse
// Description: Writes an OffsetCommitResponse to an io.Writer stream.
// ============================================================================
func EncodeOffsetCommitResponse(w io.Writer, res *OffsetCommitResponse) error {
	if err := WriteInt32(w, int32(len(res.Topics))); err != nil {
		return err
	}

	for _, topic := range res.Topics {
		if err := WriteString(w, topic.TopicName); err != nil {
			return err
		}

		if err := WriteInt32(w, int32(len(topic.Partitions))); err != nil {
			return err
		}

		for _, p := range topic.Partitions {
			if err := WriteInt32(w, p.PartitionIndex); err != nil {
				return err
			}
			if err := WriteInt16(w, p.ErrorCode); err != nil {
				return err
			}
		}
	}

	return nil
}

// ============================================================================
// STRUCT: OffsetFetchTopic
// Description: Topic details for an OffsetFetchRequest.
// ============================================================================
type OffsetFetchTopic struct {
	TopicName        string  // Target topic name
	PartitionIndexes []int32 // List of partition indexes to fetch offsets for
}

// ============================================================================
// STRUCT: OffsetFetchRequest
// Description: Request payload for ApiKey 9 (OffsetFetch).
// ============================================================================
type OffsetFetchRequest struct {
	GroupId string             // Target consumer group identifier
	Topics  []OffsetFetchTopic // Topic partition filters
}

// ============================================================================
// STRUCT: OffsetFetchResponsePartition
// Description: Partition offset details for an OffsetFetchResponse.
// ============================================================================
type OffsetFetchResponsePartition struct {
	PartitionIndex  int32  // Partition index
	CommittedOffset int64  // Committed offset (-1 if not found)
	Metadata        string // Metadata string
	ErrorCode       int16  // Error code (0 = NONE)
}

// ============================================================================
// STRUCT: OffsetFetchResponseTopic
// Description: Topic result for an OffsetFetchResponse.
// ============================================================================
type OffsetFetchResponseTopic struct {
	TopicName  string                         // Topic name
	Partitions []OffsetFetchResponsePartition // Partition results
}

// ============================================================================
// STRUCT: OffsetFetchResponse
// Description: Response payload for ApiKey 9 (OffsetFetch).
// ============================================================================
type OffsetFetchResponse struct {
	ErrorCode int16                      // Top-level error code
	Topics    []OffsetFetchResponseTopic // Results per topic
}

// ============================================================================
// FUNCTION: DecodeOffsetFetchRequest
// Description: Reads and parses an OffsetFetchRequest from an io.Reader stream.
// ============================================================================
func DecodeOffsetFetchRequest(r io.Reader) (*OffsetFetchRequest, error) {
	groupId, err := ReadString(r)
	if err != nil {
		return nil, err
	}

	numTopics, err := ReadArrayCount(r)
	if err != nil {
		return nil, err
	}

	topics := make([]OffsetFetchTopic, numTopics)
	for i := int32(0); i < numTopics; i++ {
		topicName, err := ReadString(r)
		if err != nil {
			return nil, err
		}

		numPartitions, err := ReadArrayCount(r)
		if err != nil {
			return nil, err
		}

		partitions := make([]int32, numPartitions)
		for j := int32(0); j < numPartitions; j++ {
			pIdx, err := ReadInt32(r)
			if err != nil {
				return nil, err
			}
			partitions[j] = pIdx
		}

		topics[i] = OffsetFetchTopic{
			TopicName:        topicName,
			PartitionIndexes: partitions,
		}
	}

	return &OffsetFetchRequest{
		GroupId: groupId,
		Topics:  topics,
	}, nil
}

// ============================================================================
// FUNCTION: EncodeOffsetFetchResponse
// Description: Writes an OffsetFetchResponse to an io.Writer stream.
// ============================================================================
func EncodeOffsetFetchResponse(w io.Writer, res *OffsetFetchResponse) error {
	if err := WriteInt32(w, int32(len(res.Topics))); err != nil {
		return err
	}

	for _, topic := range res.Topics {
		if err := WriteString(w, topic.TopicName); err != nil {
			return err
		}

		if err := WriteInt32(w, int32(len(topic.Partitions))); err != nil {
			return err
		}

		for _, p := range topic.Partitions {
			if err := WriteInt32(w, p.PartitionIndex); err != nil {
				return err
			}
			if err := WriteInt64(w, p.CommittedOffset); err != nil {
				return err
			}
			if err := WriteString(w, p.Metadata); err != nil {
				return err
			}
			if err := WriteInt16(w, p.ErrorCode); err != nil {
				return err
			}
		}
	}

	return nil
}
