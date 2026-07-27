package protocol

import "io"

// ============================================================================
// STRUCT: PartitionProduceData
// Description: Represents raw record batch data to be written to a partition.
// ============================================================================
type PartitionProduceData struct {
	PartitionId int32  // Partition index (0-indexed)
	RecordsData []byte // Raw binary bytes containing record set
}

// ============================================================================
// STRUCT: TopicProduceData
// Description: Represents produce data targeting a specific topic.
// ============================================================================
type TopicProduceData struct {
	TopicName  string                 // Name of the target topic
	Partitions []PartitionProduceData // List of partition payloads for this topic
}

// ============================================================================
// STRUCT: ProduceRequest
// Description: Request payload for ApiKey 0 (Produce).
// ============================================================================
type ProduceRequest struct {
	Acks    int16              // Acknowledgment setting (0=none, 1=leader, -1=all replicas)
	Timeout int32              // Maximum timeout in milliseconds
	Topics  []TopicProduceData // List of topic data payloads
}

// ============================================================================
// STRUCT: PartitionProduceResponse
// Description: Represents the produce result for a single partition.
// ============================================================================
type PartitionProduceResponse struct {
	PartitionId   int32 // Partition index (0-indexed)
	ErrorCode     int16 // Error code (0 = NO_ERROR)
	BaseOffset    int64 // Base offset assigned to the first message appended
	LogAppendTime int64 // Log append timestamp (-1 if not used)
}

// ============================================================================
// STRUCT: TopicProduceResponse
// Description: Represents the produce results for a topic.
// ============================================================================
type TopicProduceResponse struct {
	TopicName  string                     // Name of the topic
	Partitions []PartitionProduceResponse // List of produce responses per partition
}

// ============================================================================
// STRUCT: ProduceResponse
// Description: Response payload for ApiKey 0 (Produce).
// ============================================================================
type ProduceResponse struct {
	Topics []TopicProduceResponse // Produce responses grouped by topic
}

// ============================================================================
// FUNCTION: DecodeProduceRequest
// Description: Decodes a ProduceRequest from an io.Reader stream.
// ============================================================================
func DecodeProduceRequest(r io.Reader) (*ProduceRequest, error) {
	acks, err := ReadInt16(r)
	if err != nil {
		return nil, err
	}
	timeout, err := ReadInt32(r)
	if err != nil {
		return nil, err
	}
	topicCount, err := ReadInt32(r)
	if err != nil {
		return nil, err
	}
	var topics []TopicProduceData
	if topicCount > 0 {
		topics = make([]TopicProduceData, topicCount)
		for i := 0; i < int(topicCount); i++ {
			topicName, err := ReadString(r)
			if err != nil {
				return nil, err
			}
			partitionCount, err := ReadInt32(r)
			if err != nil {
				return nil, err
			}
			var partitions []PartitionProduceData
			if partitionCount > 0 {
				partitions = make([]PartitionProduceData, partitionCount)
				for j := 0; j < int(partitionCount); j++ {
					partId, err := ReadInt32(r)
					if err != nil {
						return nil, err
					}
					recordsSize, err := ReadInt32(r)
					if err != nil {
						return nil, err
					}
					var recordsData []byte
					if recordsSize > 0 {
						recordsData = make([]byte, recordsSize)
						if _, err := io.ReadFull(r, recordsData); err != nil {
							return nil, err
						}
					}
					partitions[j] = PartitionProduceData{
						PartitionId: partId,
						RecordsData: recordsData,
					}
				}
			}
			topics[i] = TopicProduceData{
				TopicName:  topicName,
				Partitions: partitions,
			}
		}
	}
	return &ProduceRequest{
		Acks:    acks,
		Timeout: timeout,
		Topics:  topics,
	}, nil
}

// ============================================================================
// FUNCTION: EncodeProduceResponse
// Description: Encodes a ProduceResponse into binary format to an io.Writer.
// ============================================================================
func EncodeProduceResponse(w io.Writer, resp *ProduceResponse) error {
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
			if err := WriteInt64(w, p.BaseOffset); err != nil {
				return err
			}
			if err := WriteInt64(w, p.LogAppendTime); err != nil {
				return err
			}
		}
	}
	return nil
}
