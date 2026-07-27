package protocol

import "io"

// ============================================================================
// STRUCT: MetadataRequest
// Description: Request payload for ApiKey 3 (Metadata).
// ============================================================================
type MetadataRequest struct {
	Topics []string // List of topic names requested (empty array requests all topics)
}

// ============================================================================
// STRUCT: BrokerMetadata
// Description: Represents information about a broker node in the cluster.
// ============================================================================
type BrokerMetadata struct {
	NodeId int32  // Unique integer identifier for the broker node (e.g., 1)
	Host   string // Hostname or IP address of the broker (e.g., "127.0.0.1")
	Port   int32  // TCP port number of the broker (e.g., 9092)
}

// ============================================================================
// STRUCT: PartitionMetadata
// Description: Represents metadata for a specific partition.
// ============================================================================
type PartitionMetadata struct {
	ErrorCode      int16   // Error code (0 = NO_ERROR)
	PartitionId    int32   // Zero-based index of the partition
	LeaderId       int32   // NodeId of the leader broker managing this partition
	Replicas       []int32 // List of broker NodeIds holding replicas for this partition
	InSyncReplicas []int32 // List of broker NodeIds currently in sync with the leader (ISR)
}

// ============================================================================
// STRUCT: TopicMetadata
// Description: Represents metadata for a specific topic.
// ============================================================================
type TopicMetadata struct {
	ErrorCode  int16               // Error code (0 = NO_ERROR)
	TopicName  string              // Name of the topic
	Partitions []PartitionMetadata // Metadata list of all partitions in this topic
}

// ============================================================================
// STRUCT: MetadataResponse
// Description: Response payload for ApiKey 3 (Metadata).
// ============================================================================
type MetadataResponse struct {
	Brokers []BrokerMetadata // List of active broker nodes in the cluster
	Topics  []TopicMetadata  // Metadata for the requested topics
}

// ============================================================================
// FUNCTION: DecodeMetadataRequest
// Description: Decodes a MetadataRequest from an io.Reader stream.
// ============================================================================
func DecodeMetadataRequest(r io.Reader) (*MetadataRequest, error) {
	topicCount, err := ReadInt32(r)
	if err != nil {
		return nil, err
	}
	var topics []string
	if topicCount > 0 {
		topics = make([]string, topicCount)
		for i := 0; i < int(topicCount); i++ {
			topicName, err := ReadString(r)
			if err != nil {
				return nil, err
			}
			topics[i] = topicName
		}
	}
	return &MetadataRequest{
		Topics: topics,
	}, nil
}

// ============================================================================
// FUNCTION: EncodeMetadataResponse
// Description: Encodes a MetadataResponse into binary format to an io.Writer stream.
// ============================================================================
func EncodeMetadataResponse(w io.Writer, resp *MetadataResponse) error {
	// 1. Write Brokers array
	if err := WriteInt32(w, int32(len(resp.Brokers))); err != nil {
		return err
	}
	for _, b := range resp.Brokers {
		if err := WriteInt32(w, b.NodeId); err != nil {
			return err
		}
		if err := WriteString(w, b.Host); err != nil {
			return err
		}
		if err := WriteInt32(w, b.Port); err != nil {
			return err
		}
	}
	// 2. Write Topics array
	if err := WriteInt32(w, int32(len(resp.Topics))); err != nil {
		return err
	}
	for _, t := range resp.Topics {
		if err := WriteInt16(w, t.ErrorCode); err != nil {
			return err
		}
		if err := WriteString(w, t.TopicName); err != nil {
			return err
		}
		// Write Partitions array inside Topic
		if err := WriteInt32(w, int32(len(t.Partitions))); err != nil {
			return err
		}
		for _, p := range t.Partitions {
			if err := WriteInt16(w, p.ErrorCode); err != nil {
				return err
			}
			if err := WriteInt32(w, p.PartitionId); err != nil {
				return err
			}
			if err := WriteInt32(w, p.LeaderId); err != nil {
				return err
			}
			// Write Replicas array
			if err := WriteInt32(w, int32(len(p.Replicas))); err != nil {
				return err
			}
			for _, rep := range p.Replicas {
				if err := WriteInt32(w, rep); err != nil {
					return err
				}
			}
			// Write InSyncReplicas array
			if err := WriteInt32(w, int32(len(p.InSyncReplicas))); err != nil {
				return err
			}
			for _, isr := range p.InSyncReplicas {
				if err := WriteInt32(w, isr); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
