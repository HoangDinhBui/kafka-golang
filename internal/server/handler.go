package server

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/HoangDinhBui/kafka-golang/internal/protocol"
	"github.com/HoangDinhBui/kafka-golang/internal/storage"
)

// ============================================================================
// STRUCT: Handler
// Description: Routes Kafka protocol requests and coordinates with storage.
// ============================================================================
type Handler struct {
	dataDir    string                           // Base data directory for logs
	nodeId     int32                            // Broker Node ID (e.g., 1)
	host       string                           // Broker host/IP
	port       int32                            // Broker TCP port
	mu         sync.RWMutex                     // Mutex protecting partition logs map
	partitions map[string]*storage.PartitionLog // Active partition logs map (key: topic-partitionId)
}

// ============================================================================
// FUNCTION: NewHandler
// Description: Initializes a new Request Handler.
// ============================================================================
func NewHandler(dataDir string, nodeId int32, host string, port int32) *Handler {
	return &Handler{
		dataDir:    dataDir,
		nodeId:     nodeId,
		host:       host,
		port:       port,
		partitions: make(map[string]*storage.PartitionLog),
	}
}

// ============================================================================
// FUNCTION: HandleRequest
// Description: Dispatches an incoming request to the appropriate API handler.
// ============================================================================
func (h *Handler) HandleRequest(header *protocol.RequestHeader, bodyReader io.Reader, respWriter io.Writer) error {
	switch header.ApiKey {
	case protocol.ApiKeyApiVersions:
		return h.handleApiVersions(respWriter)
	case protocol.ApiKeyMetadata:
		return h.handleMetadata(bodyReader, respWriter)
	case protocol.ApiKeyProduce:
		return h.handleProduce(bodyReader, respWriter)
	case protocol.ApiKeyFetch:
		return h.handleFetch(bodyReader, respWriter)
	default:
		return fmt.Errorf("unsupported ApiKey: %d", header.ApiKey)
	}
}

// ============================================================================
// PRIVATE METHOD: handleApiVersions
// Description: Handles ApiVersions (ApiKey 18) requests.
// ============================================================================
func (h *Handler) handleApiVersions(respWriter io.Writer) error {
	resp := protocol.DefaultApiVersionResponse()
	return protocol.EncodeApiVersionResponse(respWriter, resp)
}

// ============================================================================
// PRIVATE METHOD: handleMetadata
// Description: Handles Metadata (ApiKey 3) requests.
// ============================================================================
func (h *Handler) handleMetadata(bodyReader io.Reader, respWriter io.Writer) error {
	req, err := protocol.DecodeMetadataRequest(bodyReader)
	if err != nil {
		return err
	}

	brokers := []protocol.BrokerMetadata{
		{NodeId: h.nodeId, Host: h.host, Port: h.port},
	}

	var topics []protocol.TopicMetadata
	for _, topicName := range req.Topics {
		topics = append(topics, protocol.TopicMetadata{
			ErrorCode: 0,
			TopicName: topicName,
			Partitions: []protocol.PartitionMetadata{
				{
					ErrorCode:      0,
					PartitionId:    0,
					LeaderId:       h.nodeId,
					Replicas:       []int32{h.nodeId},
					InSyncReplicas: []int32{h.nodeId},
				},
			},
		})
	}

	resp := &protocol.MetadataResponse{
		Brokers: brokers,
		Topics:  topics,
	}

	return protocol.EncodeMetadataResponse(respWriter, resp)
}

// ============================================================================
// PRIVATE METHOD: handleProduce
// Description: Handles Produce (ApiKey 0) requests and appends to PartitionLog.
// ============================================================================
func (h *Handler) handleProduce(bodyReader io.Reader, respWriter io.Writer) error {
	req, err := protocol.DecodeProduceRequest(bodyReader)
	if err != nil {
		return err
	}

	var topicResponses []protocol.TopicProduceResponse

	for _, topicData := range req.Topics {
		var partResponses []protocol.PartitionProduceResponse

		for _, partData := range topicData.Partitions {
			pl, err := h.getOrCreatePartitionLog(topicData.TopicName, partData.PartitionId)
			if err != nil {
				partResponses = append(partResponses, protocol.PartitionProduceResponse{
					PartitionId:   partData.PartitionId,
					ErrorCode:     1,
					BaseOffset:    -1,
					LogAppendTime: -1,
				})
				continue
			}

			// Read records from RecordsData payload
			reader := bytes.NewReader(partData.RecordsData)
			var firstOffset int64 = -1

			for reader.Len() > 0 {
				rec, _, err := storage.ReadRecord(reader)
				if err != nil {
					// Fallback: If payload is raw text value, wrap it into a new Record
					if reader.Len() > 0 {
						rawVal := make([]byte, reader.Len())
						_, _ = reader.Read(rawVal)
						rec = &storage.Record{
							Timestamp: time.Now().UnixNano(),
							Value:     rawVal,
						}
					} else {
						break
					}
				}

				if err := pl.Append(rec); err != nil {
					break
				}

				if firstOffset == -1 {
					firstOffset = int64(rec.Offset)
				}
			}

			if firstOffset == -1 {
				firstOffset = 0
			}

			partResponses = append(partResponses, protocol.PartitionProduceResponse{
				PartitionId:   partData.PartitionId,
				ErrorCode:     0,
				BaseOffset:    firstOffset,
				LogAppendTime: time.Now().UnixMilli(),
			})
		}

		topicResponses = append(topicResponses, protocol.TopicProduceResponse{
			TopicName:  topicData.TopicName,
			Partitions: partResponses,
		})
	}

	resp := &protocol.ProduceResponse{
		Topics: topicResponses,
	}

	return protocol.EncodeProduceResponse(respWriter, resp)
}

// ============================================================================
// PRIVATE METHOD: handleFetch
// Description: Handles Fetch (ApiKey 1) requests and reads from PartitionLog.
// ============================================================================
func (h *Handler) handleFetch(bodyReader io.Reader, respWriter io.Writer) error {
	req, err := protocol.DecodeFetchRequest(bodyReader)
	if err != nil {
		return err
	}

	var topicResponses []protocol.TopicFetchResponse

	for _, topicData := range req.Topics {
		var partResponses []protocol.PartitionFetchResponse

		for _, partData := range topicData.Partitions {
			pl, err := h.getOrCreatePartitionLog(topicData.TopicName, partData.PartitionId)
			if err != nil {
				partResponses = append(partResponses, protocol.PartitionFetchResponse{
					PartitionId:   partData.PartitionId,
					ErrorCode:     1,
					HighWatermark: 0,
					RecordsData:   nil,
				})
				continue
			}

			records, err := pl.Read(uint64(partData.FetchOffset))
			if err != nil {
				partResponses = append(partResponses, protocol.PartitionFetchResponse{
					PartitionId:   partData.PartitionId,
					ErrorCode:     1,
					HighWatermark: 0,
					RecordsData:   nil,
				})
				continue
			}

			// Marshal all returned records into a single byte payload
			buf := new(bytes.Buffer)
			var highestOffset int64 = 0
			for _, rec := range records {
				data, err := rec.Marshal()
				if err == nil {
					buf.Write(data)
					highestOffset = int64(rec.Offset)
				}
			}

			partResponses = append(partResponses, protocol.PartitionFetchResponse{
				PartitionId:   partData.PartitionId,
				ErrorCode:     0,
				HighWatermark: highestOffset,
				RecordsData:   buf.Bytes(),
			})
		}

		topicResponses = append(topicResponses, protocol.TopicFetchResponse{
			TopicName:  topicData.TopicName,
			Partitions: partResponses,
		})
	}

	resp := &protocol.FetchResponse{
		Topics: topicResponses,
	}

	return protocol.EncodeFetchResponse(respWriter, resp)
}

// ============================================================================
// PRIVATE METHOD: getOrCreatePartitionLog
// Description: Thread-safely retrieves or initializes a PartitionLog instance.
// ============================================================================
func (h *Handler) getOrCreatePartitionLog(topic string, partitionId int32) (*storage.PartitionLog, error) {
	key := fmt.Sprintf("%s-%d", topic, partitionId)

	h.mu.RLock()
	pl, exists := h.partitions[key]
	h.mu.RUnlock()

	if exists {
		return pl, nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	// Double check after acquiring write lock
	if pl, exists := h.partitions[key]; exists {
		return pl, nil
	}

	dir := filepath.Join(h.dataDir, key)
	// Default 10MB per segment, 4KB index interval
	newPl, err := storage.NewPartitionLog(dir, 10*1024*1024, 4096)
	if err != nil {
		return nil, err
	}

	h.partitions[key] = newPl
	return newPl, nil
}
