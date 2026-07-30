package server

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/HoangDinhBui/kafka-golang/internal/coordinator"
	"github.com/HoangDinhBui/kafka-golang/internal/protocol"
	"github.com/HoangDinhBui/kafka-golang/internal/storage"
)

// ============================================================================
// STRUCT: Handler
// Description: Routes Kafka protocol requests and coordinates with storage,
//              offset management, and consumer group coordinator.
// ============================================================================
type Handler struct {
	dataDir          string                           // Base data directory for logs
	nodeId           int32                            // Broker Node ID (e.g., 1)
	host             string                           // Broker host/IP
	port             int32                            // Broker TCP port
	mu               sync.RWMutex                     // Mutex protecting partition logs map
	partitions       map[string]*storage.PartitionLog // Active partition logs map (key: topic-partitionId)
	offsetManager    *coordinator.OffsetManager       // Offset persistence manager
	groupCoordinator *coordinator.GroupCoordinator    // Consumer group coordinator
}

// ============================================================================
// FUNCTION: NewHandler
// Description: Initializes a new Request Handler.
// ============================================================================
func NewHandler(dataDir string, nodeId int32, host string, port int32) *Handler {
	offsetMgr := coordinator.NewOffsetManager()
	groupCoord := coordinator.NewGroupCoordinator(offsetMgr)

	return &Handler{
		dataDir:          dataDir,
		nodeId:           nodeId,
		host:             host,
		port:             port,
		partitions:       make(map[string]*storage.PartitionLog),
		offsetManager:    offsetMgr,
		groupCoordinator: groupCoord,
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
	case protocol.ApiKeyOffsetCommit:
		return h.handleOffsetCommit(bodyReader, respWriter)
	case protocol.ApiKeyOffsetFetch:
		return h.handleOffsetFetch(bodyReader, respWriter)
	case protocol.ApiKeyJoinGroup:
		return h.handleJoinGroup(bodyReader, respWriter)
	case protocol.ApiKeySyncGroup:
		return h.handleSyncGroup(bodyReader, respWriter)
	case protocol.ApiKeyHeartbeat:
		return h.handleHeartbeat(bodyReader, respWriter)
	default:
		return fmt.Errorf("unsupported ApiKey: %d", header.ApiKey)
	}
}

// ============================================================================
// PUBLIC METHOD: GetOffsetManager
// Description: Returns the OffsetManager instance.
// ============================================================================
func (h *Handler) GetOffsetManager() *coordinator.OffsetManager {
	return h.offsetManager
}

// ============================================================================
// PUBLIC METHOD: GetGroupCoordinator
// Description: Returns the GroupCoordinator instance.
// ============================================================================
func (h *Handler) GetGroupCoordinator() *coordinator.GroupCoordinator {
	return h.groupCoordinator
}

// ============================================================================
// PUBLIC METHOD: GetPartitions
// Description: Returns a thread-safe shallow copy map of active PartitionLog pointers.
// ============================================================================
func (h *Handler) GetPartitions() map[string]*storage.PartitionLog {
	h.mu.RLock()
	defer h.mu.RUnlock()

	result := make(map[string]*storage.PartitionLog, len(h.partitions))
	for k, v := range h.partitions {
		result[k] = v
	}
	return result
}

// ============================================================================
// PUBLIC METHOD: GetNodeID
// Description: Returns broker Node ID.
// ============================================================================
func (h *Handler) GetNodeID() int32 {
	return h.nodeId
}

// ============================================================================
// PUBLIC METHOD: GetHost
// Description: Returns broker host address.
// ============================================================================
func (h *Handler) GetHost() string {
	return h.host
}

// ============================================================================
// PUBLIC METHOD: GetPort
// Description: Returns broker TCP port.
// ============================================================================
func (h *Handler) GetPort() int32 {
	return h.port
}

// ============================================================================
// PUBLIC METHOD: GetDataDir
// Description: Returns base log storage directory.
// ============================================================================
func (h *Handler) GetDataDir() string {
	return h.dataDir
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
// PRIVATE METHOD: handleOffsetCommit
// Description: Handles OffsetCommit (ApiKey 8) requests.
// ============================================================================
func (h *Handler) handleOffsetCommit(bodyReader io.Reader, respWriter io.Writer) error {
	req, err := protocol.DecodeOffsetCommitRequest(bodyReader)
	if err != nil {
		return err
	}

	var topicResponses []protocol.OffsetCommitResponseTopic

	for _, topic := range req.Topics {
		var partResponses []protocol.OffsetCommitResponsePartition
		for _, p := range topic.Partitions {
			err := h.offsetManager.CommitOffset(req.GroupId, topic.TopicName, p.PartitionIndex, p.CommittedOffset, p.Metadata)
			var errCode int16 = 0
			if err != nil {
				errCode = 1
			}
			partResponses = append(partResponses, protocol.OffsetCommitResponsePartition{
				PartitionIndex: p.PartitionIndex,
				ErrorCode:      errCode,
			})
		}
		topicResponses = append(topicResponses, protocol.OffsetCommitResponseTopic{
			TopicName:  topic.TopicName,
			Partitions: partResponses,
		})
	}

	resp := &protocol.OffsetCommitResponse{Topics: topicResponses}
	return protocol.EncodeOffsetCommitResponse(respWriter, resp)
}

// ============================================================================
// PRIVATE METHOD: handleOffsetFetch
// Description: Handles OffsetFetch (ApiKey 9) requests.
// ============================================================================
func (h *Handler) handleOffsetFetch(bodyReader io.Reader, respWriter io.Writer) error {
	req, err := protocol.DecodeOffsetFetchRequest(bodyReader)
	if err != nil {
		return err
	}

	var topicResponses []protocol.OffsetFetchResponseTopic

	for _, topic := range req.Topics {
		var partResponses []protocol.OffsetFetchResponsePartition
		for _, pIdx := range topic.PartitionIndexes {
			offset, metadata, err := h.offsetManager.FetchOffset(req.GroupId, topic.TopicName, pIdx)
			var errCode int16 = 0
			if err != nil {
				errCode = 0 // Offset not found returns offset -1 with ErrorCode NONE
			}
			partResponses = append(partResponses, protocol.OffsetFetchResponsePartition{
				PartitionIndex:  pIdx,
				CommittedOffset: offset,
				Metadata:        metadata,
				ErrorCode:       errCode,
			})
		}
		topicResponses = append(topicResponses, protocol.OffsetFetchResponseTopic{
			TopicName:  topic.TopicName,
			Partitions: partResponses,
		})
	}

	resp := &protocol.OffsetFetchResponse{
		ErrorCode: 0,
		Topics:    topicResponses,
	}
	return protocol.EncodeOffsetFetchResponse(respWriter, resp)
}

// ============================================================================
// PRIVATE METHOD: handleJoinGroup
// Description: Handles JoinGroup (ApiKey 11) requests.
// ============================================================================
func (h *Handler) handleJoinGroup(bodyReader io.Reader, respWriter io.Writer) error {
	req, err := protocol.DecodeJoinGroupRequest(bodyReader)
	if err != nil {
		return err
	}

	protoMap := make(map[string][]byte)
	for _, p := range req.Protocols {
		protoMap[p.Name] = p.Metadata
	}

	group, member, err := h.groupCoordinator.AddMember(
		req.GroupId,
		req.MemberId,
		"127.0.0.1",
		req.SessionTimeoutMs,
		req.RebalanceTimeoutMs,
		req.ProtocolType,
		protoMap,
	)

	if err != nil {
		resp := &protocol.JoinGroupResponse{ErrorCode: 1}
		return protocol.EncodeJoinGroupResponse(respWriter, resp)
	}

	var memberList []protocol.JoinGroupResponseMember
	// Include members array ONLY if this member is elected Leader
	if group.LeaderID == member.MemberID {
		for mID, m := range group.Members {
			var meta []byte
			for _, bytesData := range m.SupportedProtocols {
				meta = bytesData
				break
			}
			memberList = append(memberList, protocol.JoinGroupResponseMember{
				MemberId: mID,
				Metadata: meta,
			})
		}
	}

	selectedProtocol := "range"
	if len(req.Protocols) > 0 {
		selectedProtocol = req.Protocols[0].Name
	}

	resp := &protocol.JoinGroupResponse{
		ErrorCode:    0,
		GenerationId: group.GenerationID,
		ProtocolName: selectedProtocol,
		LeaderId:     group.LeaderID,
		MemberId:     member.MemberID,
		Members:      memberList,
	}

	return protocol.EncodeJoinGroupResponse(respWriter, resp)
}

// ============================================================================
// PRIVATE METHOD: handleSyncGroup
// Description: Handles SyncGroup (ApiKey 14) requests.
// ============================================================================
func (h *Handler) handleSyncGroup(bodyReader io.Reader, respWriter io.Writer) error {
	req, err := protocol.DecodeSyncGroupRequest(bodyReader)
	if err != nil {
		return err
	}

	var assignedBytes []byte
	for _, a := range req.GroupAssignments {
		if a.MemberId == req.MemberId {
			assignedBytes = a.Assignment
			break
		}
	}

	resp := &protocol.SyncGroupResponse{
		ErrorCode:  0,
		Assignment: assignedBytes,
	}

	return protocol.EncodeSyncGroupResponse(respWriter, resp)
}

// ============================================================================
// PRIVATE METHOD: handleHeartbeat
// Description: Handles Heartbeat (ApiKey 12) requests.
// ============================================================================
func (h *Handler) handleHeartbeat(bodyReader io.Reader, respWriter io.Writer) error {
	req, err := protocol.DecodeHeartbeatRequest(bodyReader)
	if err != nil {
		return err
	}

	err = h.groupCoordinator.Heartbeat(req.GroupId, req.MemberId)
	var errCode int16 = 0
	if err != nil {
		errCode = 25
	}

	resp := &protocol.HeartbeatResponse{
		ErrorCode: errCode,
	}

	return protocol.EncodeHeartbeatResponse(respWriter, resp)
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
