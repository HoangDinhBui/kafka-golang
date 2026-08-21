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
	"github.com/HoangDinhBui/kafka-golang/internal/security"
	"github.com/HoangDinhBui/kafka-golang/internal/storage"
)

type TelemetryListener interface {
	RecordMsgIn(count uint64, bytes uint64)
	RecordBytesOut(bytes uint64)
}

// ============================================================================
// STRUCT: Handler
// Description: Routes Kafka protocol requests and coordinates with storage,
//              offset management, and consumer group coordinator.
// ============================================================================
type Handler struct {
	dataDir          string                              // Base data directory for logs
	nodeId           int32                               // Broker Node ID (e.g., 1)
	host             string                              // Broker host/IP
	port             int32                               // Broker TCP port
	mu               sync.RWMutex                        // Mutex protecting partition logs map, telemetry & saslRequired
	partitions       map[string]*storage.PartitionLog    // Active partition logs map (key: topic-partitionId)
	offsetManager    *coordinator.OffsetManager          // Offset persistence manager
	groupCoordinator *coordinator.GroupCoordinator       // Consumer group coordinator
	txnCoordinator   *coordinator.TransactionCoordinator // Transaction coordinator
	saslAuth         *security.SASLAuthenticator         // SASL Authenticator
	aclManager       *security.ACLManager                // ACL Manager
	saslRequired     bool                                // When true, all requests other than ApiVersions/SaslHandshake/SaslAuthenticate require a successful SASL exchange first
	telemetry        TelemetryListener                   // Telemetry metric listener
}

// errCodeTopicAuthorizationFailed mirrors the Kafka protocol's
// TOPIC_AUTHORIZATION_FAILED error code, returned per-partition when the
// ACLManager denies a Produce/Fetch request for a topic.
const errCodeTopicAuthorizationFailed int16 = 29

// errCodeGroupAuthorizationFailed mirrors the Kafka protocol's
// GROUP_AUTHORIZATION_FAILED error code, returned when the ACLManager
// denies a JoinGroup/SyncGroup/Heartbeat/OffsetCommit/OffsetFetch request
// for a consumer group.
const errCodeGroupAuthorizationFailed int16 = 30

// ============================================================================
// FUNCTION: NewHandler
// Description: Initializes a new Request Handler.
// ============================================================================
func NewHandler(dataDir string, nodeId int32, host string, port int32) *Handler {
	offsetMgr := coordinator.NewOffsetManager()
	groupCoord := coordinator.NewGroupCoordinator(offsetMgr)
	txnCoord := coordinator.NewTransactionCoordinator()
	saslAuth := security.NewSASLAuthenticator()
	aclMgr := security.NewACLManager()

	return &Handler{
		dataDir:          dataDir,
		nodeId:           nodeId,
		host:             host,
		port:             port,
		partitions:       make(map[string]*storage.PartitionLog),
		offsetManager:    offsetMgr,
		groupCoordinator: groupCoord,
		txnCoordinator:   txnCoord,
		saslAuth:         saslAuth,
		aclManager:       aclMgr,
	}
}

// SetTelemetryListener registers a TelemetryListener implementation.
func (h *Handler) SetTelemetryListener(l TelemetryListener) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.telemetry = l
}

// SetSASLRequired toggles whether clients must complete a successful SASL
// exchange before any API request other than ApiVersions/SaslHandshake/
// SaslAuthenticate will be served. See cmd/broker's -sasl-enabled flag.
func (h *Handler) SetSASLRequired(required bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.saslRequired = required
}

func (h *Handler) isSASLRequired() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.saslRequired
}

// AddSASLUser registers a user credential for SASL/PLAIN and
// SASL/SCRAM-SHA-256 authentication (see cmd/broker's -sasl-users flag).
// Only the salted, derived credential is retained — the plaintext password
// is not stored.
func (h *Handler) AddSASLUser(username, password string) error {
	return h.saslAuth.AddUser(username, password)
}

// AddACLRule registers an access-control rule evaluated on Produce (Write)
// and Fetch (Read) requests (see cmd/broker's -acl-rules flag). With no
// rules registered, ACLManager defaults to allowing all access.
func (h *Handler) AddACLRule(rule security.ACLRule) {
	h.aclManager.AddRule(rule)
}

// authorizeTopic reports whether the session's authenticated principal
// (empty string if unauthenticated) may perform op on topic.
func (h *Handler) authorizeTopic(session *security.SASLSession, topic string, op string) bool {
	principal := ""
	if session != nil {
		principal = session.Username
	}
	return h.aclManager.Authorize(principal, security.ResourceTypeTopic, topic, op)
}

// authorizeGroup reports whether the session's authenticated principal
// (empty string if unauthenticated) may perform op on the given consumer
// group. Mirrors authorizeTopic — security.ResourceTypeGroup previously had
// no call site anywhere in the handler, so no consumer-group ACL rule could
// ever actually be enforced regardless of configuration.
func (h *Handler) authorizeGroup(session *security.SASLSession, groupId string, op string) bool {
	principal := ""
	if session != nil {
		principal = session.Username
	}
	return h.aclManager.Authorize(principal, security.ResourceTypeGroup, groupId, op)
}


// ============================================================================
// FUNCTION: HandleRequest
// Description: Dispatches an incoming request to the appropriate API handler.
// ============================================================================
func (h *Handler) HandleRequest(header *protocol.RequestHeader, bodyReader io.Reader, respWriter io.Writer, saslSession *security.SASLSession) error {
	if h.isSASLRequired() && (saslSession == nil || !saslSession.Authenticated()) {
		switch header.ApiKey {
		case protocol.ApiKeyApiVersions, protocol.ApiKeySaslHandshake, protocol.ApiKeySaslAuthenticate:
			// Allowed before authentication completes.
		default:
			return fmt.Errorf("SASL authentication required before serving ApiKey %d", header.ApiKey)
		}
	}

	switch header.ApiKey {
	case protocol.ApiKeyApiVersions:
		return h.handleApiVersions(respWriter)
	case protocol.ApiKeyMetadata:
		return h.handleMetadata(bodyReader, respWriter)
	case protocol.ApiKeyProduce:
		return h.handleProduce(bodyReader, respWriter, saslSession)
	case protocol.ApiKeyFetch:
		return h.handleFetch(bodyReader, respWriter, saslSession)
	case protocol.ApiKeyOffsetCommit:
		return h.handleOffsetCommit(bodyReader, respWriter, saslSession)
	case protocol.ApiKeyOffsetFetch:
		return h.handleOffsetFetch(bodyReader, respWriter, saslSession)
	case protocol.ApiKeyGroupCoordinator:
		return h.handleFindCoordinator(bodyReader, respWriter)
	case protocol.ApiKeyJoinGroup:
		return h.handleJoinGroup(bodyReader, respWriter, saslSession)
	case protocol.ApiKeySyncGroup:
		return h.handleSyncGroup(bodyReader, respWriter, saslSession)
	case protocol.ApiKeyHeartbeat:
		return h.handleHeartbeat(bodyReader, respWriter, saslSession)
	case protocol.ApiKeyLeaveGroup:
		return h.handleLeaveGroup(bodyReader, respWriter, saslSession)
	case protocol.ApiKeyInitProducerId:
		return h.handleInitProducerId(bodyReader, respWriter)
	case protocol.ApiKeyAddPartitionsToTxn:
		return h.handleAddPartitionsToTxn(bodyReader, respWriter, saslSession)
	case protocol.ApiKeyEndTxn:
		return h.handleEndTxn(bodyReader, respWriter)
	case protocol.ApiKeySaslHandshake:
		return h.handleSaslHandshake(bodyReader, respWriter, saslSession)
	case protocol.ApiKeySaslAuthenticate:
		return h.handleSaslAuthenticate(bodyReader, respWriter, saslSession)
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
func (h *Handler) handleProduce(bodyReader io.Reader, respWriter io.Writer, session *security.SASLSession) error {
	req, err := protocol.DecodeProduceRequest(bodyReader)
	if err != nil {
		return err
	}

	var topicResponses []protocol.TopicProduceResponse

	for _, topicData := range req.Topics {
		if !h.authorizeTopic(session, topicData.TopicName, security.OpWrite) {
			var deniedResponses []protocol.PartitionProduceResponse
			for _, partData := range topicData.Partitions {
				deniedResponses = append(deniedResponses, protocol.PartitionProduceResponse{
					PartitionId:   partData.PartitionId,
					ErrorCode:     errCodeTopicAuthorizationFailed,
					BaseOffset:    -1,
					LogAppendTime: -1,
				})
			}
			topicResponses = append(topicResponses, protocol.TopicProduceResponse{
				TopicName:  topicData.TopicName,
				Partitions: deniedResponses,
			})
			continue
		}

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

				h.mu.RLock()
				tel := h.telemetry
				h.mu.RUnlock()
				if tel != nil {
					tel.RecordMsgIn(1, uint64(len(rec.Value)))
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
func (h *Handler) handleFetch(bodyReader io.Reader, respWriter io.Writer, session *security.SASLSession) error {
	req, err := protocol.DecodeFetchRequest(bodyReader)
	if err != nil {
		return err
	}

	var topicResponses []protocol.TopicFetchResponse

	for _, topicData := range req.Topics {
		if !h.authorizeTopic(session, topicData.TopicName, security.OpRead) {
			var deniedResponses []protocol.PartitionFetchResponse
			for _, partData := range topicData.Partitions {
				deniedResponses = append(deniedResponses, protocol.PartitionFetchResponse{
					PartitionId:   partData.PartitionId,
					ErrorCode:     errCodeTopicAuthorizationFailed,
					HighWatermark: 0,
					RecordsData:   nil,
				})
			}
			topicResponses = append(topicResponses, protocol.TopicFetchResponse{
				TopicName:  topicData.TopicName,
				Partitions: deniedResponses,
			})
			continue
		}

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

			var recordsBuf bytes.Buffer
			maxBytes := int64(partData.MaxBytes)
			if maxBytes <= 0 {
				maxBytes = 1024 * 1024 // Default 1MB max fetch limit
			}

			bytesWritten, err := pl.ReadZeroCopy(uint64(partData.FetchOffset), maxBytes, &recordsBuf)
			if err != nil {
				partResponses = append(partResponses, protocol.PartitionFetchResponse{
					PartitionId:   partData.PartitionId,
					ErrorCode:     1,
					HighWatermark: 0,
					RecordsData:   nil,
				})
				continue
			}

			h.mu.RLock()
			tel := h.telemetry
			h.mu.RUnlock()
			if tel != nil && bytesWritten > 0 {
				tel.RecordBytesOut(uint64(bytesWritten))
			}

			leo := int64(pl.LEO())
			partResponses = append(partResponses, protocol.PartitionFetchResponse{
				PartitionId:   partData.PartitionId,
				ErrorCode:     0,
				HighWatermark: leo,
				RecordsData:   recordsBuf.Bytes(),
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
func (h *Handler) handleOffsetCommit(bodyReader io.Reader, respWriter io.Writer, session *security.SASLSession) error {
	req, err := protocol.DecodeOffsetCommitRequest(bodyReader)
	if err != nil {
		return err
	}

	var topicResponses []protocol.OffsetCommitResponseTopic

	groupAuthorized := h.authorizeGroup(session, req.GroupId, security.OpRead)

	for _, topic := range req.Topics {
		var partResponses []protocol.OffsetCommitResponsePartition

		if !groupAuthorized || !h.authorizeTopic(session, topic.TopicName, security.OpRead) {
			for _, p := range topic.Partitions {
				partResponses = append(partResponses, protocol.OffsetCommitResponsePartition{
					PartitionIndex: p.PartitionIndex,
					ErrorCode:      errCodeGroupAuthorizationFailed,
				})
			}
			topicResponses = append(topicResponses, protocol.OffsetCommitResponseTopic{
				TopicName:  topic.TopicName,
				Partitions: partResponses,
			})
			continue
		}

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
func (h *Handler) handleOffsetFetch(bodyReader io.Reader, respWriter io.Writer, session *security.SASLSession) error {
	req, err := protocol.DecodeOffsetFetchRequest(bodyReader)
	if err != nil {
		return err
	}

	if !h.authorizeGroup(session, req.GroupId, security.OpDescribe) {
		resp := &protocol.OffsetFetchResponse{ErrorCode: errCodeGroupAuthorizationFailed}
		return protocol.EncodeOffsetFetchResponse(respWriter, resp)
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
func (h *Handler) handleJoinGroup(bodyReader io.Reader, respWriter io.Writer, session *security.SASLSession) error {
	req, err := protocol.DecodeJoinGroupRequest(bodyReader)
	if err != nil {
		return err
	}

	if !h.authorizeGroup(session, req.GroupId, security.OpRead) {
		resp := &protocol.JoinGroupResponse{ErrorCode: errCodeGroupAuthorizationFailed}
		return protocol.EncodeJoinGroupResponse(respWriter, resp)
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
func (h *Handler) handleSyncGroup(bodyReader io.Reader, respWriter io.Writer, session *security.SASLSession) error {
	req, err := protocol.DecodeSyncGroupRequest(bodyReader)
	if err != nil {
		return err
	}

	if !h.authorizeGroup(session, req.GroupId, security.OpRead) {
		resp := &protocol.SyncGroupResponse{ErrorCode: errCodeGroupAuthorizationFailed}
		return protocol.EncodeSyncGroupResponse(respWriter, resp)
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
func (h *Handler) handleHeartbeat(bodyReader io.Reader, respWriter io.Writer, session *security.SASLSession) error {
	req, err := protocol.DecodeHeartbeatRequest(bodyReader)
	if err != nil {
		return err
	}

	if !h.authorizeGroup(session, req.GroupId, security.OpRead) {
		resp := &protocol.HeartbeatResponse{ErrorCode: errCodeGroupAuthorizationFailed}
		return protocol.EncodeHeartbeatResponse(respWriter, resp)
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
// PRIVATE METHOD: handleFindCoordinator (ApiKey 10)
// Description: Real client libraries send FindCoordinator to discover which
//              broker is the coordinator for a group/transactional id
//              before starting JoinGroup — without a handler for it, they
//              cannot begin the consumer-group flow at all, regardless of
//              whether JoinGroup itself works. This broker never runs as
//              part of a real multi-broker cluster (see internal/replication
//              and internal/consensus, which are not wired into cmd/broker),
//              so it always answers by identifying itself.
// ============================================================================
func (h *Handler) handleFindCoordinator(bodyReader io.Reader, respWriter io.Writer) error {
	if _, err := protocol.DecodeFindCoordinatorRequest(bodyReader); err != nil {
		return err
	}

	resp := &protocol.FindCoordinatorResponse{
		ErrorCode: 0,
		NodeId:    h.nodeId,
		Host:      h.host,
		Port:      h.port,
	}
	return protocol.EncodeFindCoordinatorResponse(respWriter, resp)
}

// ============================================================================
// PRIVATE METHOD: handleLeaveGroup (ApiKey 13)
// Description: Lets a consumer voluntarily leave a group (e.g. on clean
//              shutdown) instead of waiting out a session timeout.
// ============================================================================
func (h *Handler) handleLeaveGroup(bodyReader io.Reader, respWriter io.Writer, session *security.SASLSession) error {
	req, err := protocol.DecodeLeaveGroupRequest(bodyReader)
	if err != nil {
		return err
	}

	if !h.authorizeGroup(session, req.GroupId, security.OpRead) {
		resp := &protocol.LeaveGroupResponse{ErrorCode: errCodeGroupAuthorizationFailed}
		return protocol.EncodeLeaveGroupResponse(respWriter, resp)
	}

	errCode := int16(0)
	if err := h.groupCoordinator.RemoveMember(req.GroupId, req.MemberId); err != nil {
		errCode = 25 // matches the same "unknown member/group" code Heartbeat already uses
	}

	resp := &protocol.LeaveGroupResponse{ErrorCode: errCode}
	return protocol.EncodeLeaveGroupResponse(respWriter, resp)
}

// ============================================================================
// PRIVATE METHOD: getOrCreatePartitionLog
// Description: Thread-safely retrieves or initializes a PartitionLog instance.
// ============================================================================
func (h *Handler) getOrCreatePartitionLog(topic string, partitionId int32) (*storage.PartitionLog, error) {
	if !isValidTopicName(topic) {
		return nil, fmt.Errorf("invalid topic name: %q", topic)
	}

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

// isValidTopicName reports whether topic is safe to use when building a
// filesystem path (getOrCreatePartitionLog joins it directly under
// h.dataDir). Without this check, a topic name such as "../../etc/evil"
// sent in a Produce/Fetch/EndTxn request would let filepath.Join walk the
// resulting directory outside dataDir entirely, letting a client make the
// broker create files at an arbitrary filesystem location it has write
// access to. Mirrors Kafka's own legal topic name charset.
func isValidTopicName(topic string) bool {
	if topic == "" || len(topic) > 249 || topic == "." || topic == ".." {
		return false
	}
	for _, r := range topic {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '.' || r == '_' || r == '-':
		default:
			return false
		}
	}
	return true
}

// ============================================================================
// PRIVATE METHOD: handleInitProducerId (ApiKey 22)
// ============================================================================
func (h *Handler) handleInitProducerId(bodyReader io.Reader, respWriter io.Writer) error {
	req, err := protocol.DecodeInitProducerIdRequest(bodyReader)
	if err != nil {
		return err
	}

	pid, epoch, err := h.txnCoordinator.InitProducerId(req.TransactionalId, req.TransactionTimeoutMs)
	errorCode := int16(0)
	if err != nil {
		errorCode = 1
	}

	resp := &protocol.InitProducerIdResponse{
		ErrorCode:     errorCode,
		ProducerId:    pid,
		ProducerEpoch: epoch,
	}
	return protocol.EncodeInitProducerIdResponse(respWriter, resp)
}

// ============================================================================
// PRIVATE METHOD: handleAddPartitionsToTxn (ApiKey 24)
// ============================================================================
func (h *Handler) handleAddPartitionsToTxn(bodyReader io.Reader, respWriter io.Writer, session *security.SASLSession) error {
	req, err := protocol.DecodeAddPartitionsToTxnRequest(bodyReader)
	if err != nil {
		return err
	}

	var results []protocol.AddPartitionsToTxnTopicResult
	for _, tReq := range req.Topics {
		var pResults []protocol.AddPartitionsToTxnResult

		// Denied topics are never registered with txnCoordinator below, so
		// EndTxn — which only ever writes control records to partitions
		// previously registered here — can never touch a topic this
		// principal lacks Write access to.
		if !h.authorizeTopic(session, tReq.TopicName, security.OpWrite) {
			for _, partId := range tReq.Partitions {
				pResults = append(pResults, protocol.AddPartitionsToTxnResult{
					PartitionId: partId,
					ErrorCode:   errCodeTopicAuthorizationFailed,
				})
			}
			results = append(results, protocol.AddPartitionsToTxnTopicResult{
				TopicName:  tReq.TopicName,
				Partitions: pResults,
			})
			continue
		}

		for _, partId := range tReq.Partitions {
			err := h.txnCoordinator.AddPartitionsToTxn(req.TransactionalId, req.ProducerId, req.ProducerEpoch, tReq.TopicName, []int32{partId})
			errCode := int16(0)
			if err != nil {
				errCode = 1
			}
			pResults = append(pResults, protocol.AddPartitionsToTxnResult{
				PartitionId: partId,
				ErrorCode:   errCode,
			})
		}
		results = append(results, protocol.AddPartitionsToTxnTopicResult{
			TopicName:  tReq.TopicName,
			Partitions: pResults,
		})
	}

	resp := &protocol.AddPartitionsToTxnResponse{Results: results}
	return protocol.EncodeAddPartitionsToTxnResponse(respWriter, resp)
}

// ============================================================================
// PRIVATE METHOD: handleEndTxn (ApiKey 26)
// ============================================================================
func (h *Handler) handleEndTxn(bodyReader io.Reader, respWriter io.Writer) error {
	req, err := protocol.DecodeEndTxnRequest(bodyReader)
	if err != nil {
		return err
	}

	targets, err := h.txnCoordinator.EndTxn(req.TransactionalId, req.ProducerId, req.ProducerEpoch, req.Committed)
	errorCode := int16(0)
	if err != nil {
		errorCode = 1
	} else {
		// Write control record marker to each partition
		markerType := storage.ControlMarkerCommit
		if !req.Committed {
			markerType = storage.ControlMarkerAbort
		}
		controlRec := storage.NewControlRecord(markerType)

		for _, t := range targets {
			pl, err := h.getOrCreatePartitionLog(t.Topic, t.Partition)
			if err == nil {
				_ = pl.Append(controlRec)
			}
		}
	}

	resp := &protocol.EndTxnResponse{ErrorCode: errorCode}
	return protocol.EncodeEndTxnResponse(respWriter, resp)
}

// ============================================================================
// PRIVATE METHOD: handleSaslHandshake (ApiKey 17)
// ============================================================================
func (h *Handler) handleSaslHandshake(bodyReader io.Reader, respWriter io.Writer, session *security.SASLSession) error {
	req, err := protocol.DecodeSaslHandshakeRequest(bodyReader)
	if err != nil {
		return err
	}

	errorCode := int16(0)
	if !h.saslAuth.IsMechanismSupported(req.Mechanism) {
		errorCode = 33 // UNSUPPORTED_SASL_MECHANISM
	} else {
		session.SetMechanism(req.Mechanism)
	}

	resp := &protocol.SaslHandshakeResponse{
		ErrorCode:          errorCode,
		EnabledMechanisms: h.saslAuth.GetEnabledMechanisms(),
	}
	return protocol.EncodeSaslHandshakeResponse(respWriter, resp)
}

// ============================================================================
// PRIVATE METHOD: handleSaslAuthenticate (ApiKey 36)
// Description: Feeds one SaslAuthenticate payload into the connection's
//              SASL session. For SCRAM-SHA-256 this may take two round
//              trips (client-first / client-final) before authentication
//              concludes; the session tracks progress between calls.
// ============================================================================
func (h *Handler) handleSaslAuthenticate(bodyReader io.Reader, respWriter io.Writer, session *security.SASLSession) error {
	req, err := protocol.DecodeSaslAuthenticateRequest(bodyReader)
	if err != nil {
		return err
	}

	authData, _, _, err := h.saslAuth.Authenticate(session, req.AuthData)
	errorCode := int16(0)
	var errMsg *string
	if err != nil {
		errorCode = 58 // SASL_AUTHENTICATION_FAILED
		msg := err.Error()
		errMsg = &msg
	}

	resp := &protocol.SaslAuthenticateResponse{
		ErrorCode:    errorCode,
		ErrorMessage: errMsg,
		AuthData:     authData,
	}
	return protocol.EncodeSaslAuthenticateResponse(respWriter, resp)
}
