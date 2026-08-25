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

// IsSASLRequired reports whether -sasl-enabled is in effect, i.e. whether
// the TCP protocol port requires authentication. The Web UI server uses
// this to decide whether it must also require credentials on its own HTTP
// port — without it, an operator who locks down the Kafka port gets a false
// sense of security while the UI still serves every topic's raw messages
// to anyone who can reach it.
func (h *Handler) IsSASLRequired() bool {
	return h.isSASLRequired()
}

// AuthenticateBasic verifies a username/password pair against the same
// credential store used for SASL/PLAIN, so the Web UI can reuse -sasl-users
// accounts for HTTP Basic Auth instead of maintaining a separate identity
// system.
func (h *Handler) AuthenticateBasic(username, password string) bool {
	payload := []byte("\x00" + username + "\x00" + password)
	_, err := h.saslAuth.AuthenticatePlain(payload)
	return err == nil
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
		return h.handleOffsetCommit(bodyReader, respWriter)
	case protocol.ApiKeyOffsetFetch:
		return h.handleOffsetFetch(bodyReader, respWriter)
	case protocol.ApiKeyJoinGroup:
		return h.handleJoinGroup(bodyReader, respWriter)
	case protocol.ApiKeySyncGroup:
		return h.handleSyncGroup(bodyReader, respWriter)
	case protocol.ApiKeyHeartbeat:
		return h.handleHeartbeat(bodyReader, respWriter)
	case protocol.ApiKeyInitProducerId:
		return h.handleInitProducerId(bodyReader, respWriter)
	case protocol.ApiKeyAddPartitionsToTxn:
		return h.handleAddPartitionsToTxn(bodyReader, respWriter)
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
func (h *Handler) handleAddPartitionsToTxn(bodyReader io.Reader, respWriter io.Writer) error {
	req, err := protocol.DecodeAddPartitionsToTxnRequest(bodyReader)
	if err != nil {
		return err
	}

	var results []protocol.AddPartitionsToTxnTopicResult
	for _, tReq := range req.Topics {
		var pResults []protocol.AddPartitionsToTxnResult
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
