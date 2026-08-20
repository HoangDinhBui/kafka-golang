package protocol

import "io"

// ============================================================================
// STRUCT: PartitionFetchData
// Description: Parameters for fetching records from a specific partition.
// ============================================================================
type PartitionFetchData struct {
	PartitionId int32 // Partition index (0-indexed)
	FetchOffset int64 // Starting offset to fetch records from
	MaxBytes    int32 // Maximum bytes to return for this partition
}

// ============================================================================
// STRUCT: TopicFetchData
// Description: Represents fetch parameters for a specific topic.
// ============================================================================
type TopicFetchData struct {
	TopicName  string               // Name of the target topic
	Partitions []PartitionFetchData // List of partition fetch parameters
}

// ============================================================================
// STRUCT: FetchRequest
// Description: Request payload for ApiKey 1 (Fetch).
// ============================================================================
type FetchRequest struct {
	ReplicaId   int32            // -1 for regular consumers, NodeId for follower brokers
	MaxWaitTime int32            // Maximum wait time in milliseconds for long polling
	MinBytes    int32            // Minimum byte threshold before returning data
	Topics      []TopicFetchData // List of topic fetch requests
}

// ============================================================================
// STRUCT: PartitionFetchResponse
// Description: Represents the fetch result for a single partition.
// ============================================================================
type PartitionFetchResponse struct {
	PartitionId   int32  // Partition index (0-indexed)
	ErrorCode     int16  // Error code (0 = NO_ERROR)
	HighWatermark int64  // High watermark offset of the partition
	RecordsData   []byte // Raw binary bytes containing returned records
}

// ============================================================================
// STRUCT: TopicFetchResponse
// Description: Represents fetch results for a topic.
// ============================================================================
type TopicFetchResponse struct {
	TopicName  string                   // Name of the topic
	Partitions []PartitionFetchResponse // List of fetch responses per partition
}

// ============================================================================
// STRUCT: FetchResponse
// Description: Response payload for ApiKey 1 (Fetch).
// ============================================================================
type FetchResponse struct {
	Topics []TopicFetchResponse // Fetch responses grouped by topic
}

// ============================================================================
// FUNCTION: DecodeFetchRequest
// Description: Decodes a FetchRequest from an io.Reader stream.
// ============================================================================
func DecodeFetchRequest(r io.Reader) (*FetchRequest, error) {
	replicaId, err := ReadInt32(r)
	if err != nil {
		return nil, err
	}
	maxWaitTime, err := ReadInt32(r)
	if err != nil {
		return nil, err
	}
	minBytes, err := ReadInt32(r)
	if err != nil {
		return nil, err
	}
	topicCount, err := ReadArrayCount(r)
	if err != nil {
		return nil, err
	}
	var topics []TopicFetchData
	if topicCount > 0 {
		topics = make([]TopicFetchData, topicCount)
		for i := 0; i < int(topicCount); i++ {
			topicName, err := ReadString(r)
			if err != nil {
				return nil, err
			}
			partitionCount, err := ReadArrayCount(r)
			if err != nil {
				return nil, err
			}
			var partitions []PartitionFetchData
			if partitionCount > 0 {
				partitions = make([]PartitionFetchData, partitionCount)
				for j := 0; j < int(partitionCount); j++ {
					partId, err := ReadInt32(r)
					if err != nil {
						return nil, err
					}
					fetchOffset, err := ReadInt64(r)
					if err != nil {
						return nil, err
					}
					maxBytes, err := ReadInt32(r)
					if err != nil {
						return nil, err
					}
					partitions[j] = PartitionFetchData{
						PartitionId: partId,
						FetchOffset: fetchOffset,
						MaxBytes:    maxBytes,
					}
				}
			}
			topics[i] = TopicFetchData{
				TopicName:  topicName,
				Partitions: partitions,
			}
		}
	}
	return &FetchRequest{
		ReplicaId:   replicaId,
		MaxWaitTime: maxWaitTime,
		MinBytes:    minBytes,
		Topics:      topics,
	}, nil
}

// ============================================================================
// FUNCTION: EncodeFetchResponse
// Description: Encodes a FetchResponse into binary format to an io.Writer.
// ============================================================================
func EncodeFetchResponse(w io.Writer, resp *FetchResponse) error {
	// Write Topics array count
	if err := WriteInt32(w, int32(len(resp.Topics))); err != nil {
		return err
	}
	for _, t := range resp.Topics {
		if err := WriteString(w, t.TopicName); err != nil {
			return err
		}
		// Write Partitions array count inside Topic
		if err := WriteInt32(w, int32(len(t.Partitions))); err != nil {
			return err
		}
		for _, p := range t.Partitions {
			if err := WriteInt32(w, p.PartitionId); err != nil {
				return err
			}
			if err := WriteInt16(w, p.ErrorCode); err != nil {
				return err
			}
			if err := WriteInt64(w, p.HighWatermark); err != nil {
				return err
			}
			// Write RecordsData length & payload
			recordsLen := int32(len(p.RecordsData))
			if err := WriteInt32(w, recordsLen); err != nil {
				return err
			}
			if recordsLen > 0 {
				if _, err := w.Write(p.RecordsData); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
