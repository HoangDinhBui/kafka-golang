package protocol

import (
	"io"
)

// ============================================================================
// STRUCT: GroupProtocol
// Description: Protocol metadata supported by a joining consumer.
// ============================================================================
type GroupProtocol struct {
	Name     string // Protocol name (e.g. "range", "roundrobin")
	Metadata []byte // Raw assignment metadata bytes
}

// ============================================================================
// STRUCT: JoinGroupRequest
// Description: Request payload for ApiKey 11 (JoinGroup).
// ============================================================================
type JoinGroupRequest struct {
	GroupId          string          // Consumer group identifier
	SessionTimeoutMs int32           // Client session timeout in milliseconds
	RebalanceTimeoutMs int32         // Rebalance timeout in milliseconds
	MemberId         string          // Consumer member identifier (empty for initial join)
	ProtocolType     string          // Protocol type (e.g. "consumer")
	Protocols        []GroupProtocol // List of assignment protocols supported by client
}

// ============================================================================
// STRUCT: JoinGroupResponseMember
// Description: Member metadata returned to the group leader in JoinGroupResponse.
// ============================================================================
type JoinGroupResponseMember struct {
	MemberId string // Unique member identifier
	Metadata []byte // Raw member metadata bytes
}

// ============================================================================
// STRUCT: JoinGroupResponse
// Description: Response payload for ApiKey 11 (JoinGroup).
// ============================================================================
type JoinGroupResponse struct {
	ErrorCode    int16                     // Error code (0 = NONE)
	GenerationId int32                     // Group generation ID
	ProtocolName string                    // Selected group assignment protocol
	LeaderId     string                    // Group leader member ID
	MemberId     string                    // Assigned unique member ID
	Members      []JoinGroupResponseMember // List of group members (non-empty ONLY for leader)
}

// ============================================================================
// FUNCTION: DecodeJoinGroupRequest
// Description: Reads and parses a JoinGroupRequest from an io.Reader stream.
// ============================================================================
func DecodeJoinGroupRequest(r io.Reader) (*JoinGroupRequest, error) {
	groupId, err := ReadString(r)
	if err != nil {
		return nil, err
	}

	sessionTimeoutMs, err := ReadInt32(r)
	if err != nil {
		return nil, err
	}

	rebalanceTimeoutMs, err := ReadInt32(r)
	if err != nil {
		return nil, err
	}

	memberId, err := ReadString(r)
	if err != nil {
		return nil, err
	}

	protocolType, err := ReadString(r)
	if err != nil {
		return nil, err
	}

	numProtocols, err := ReadArrayCount(r)
	if err != nil {
		return nil, err
	}

	protocols := make([]GroupProtocol, numProtocols)
	for i := int32(0); i < numProtocols; i++ {
		name, err := ReadString(r)
		if err != nil {
			return nil, err
		}
		meta, err := ReadBytes(r)
		if err != nil {
			return nil, err
		}
		protocols[i] = GroupProtocol{
			Name:     name,
			Metadata: meta,
		}
	}

	return &JoinGroupRequest{
		GroupId:            groupId,
		SessionTimeoutMs:   sessionTimeoutMs,
		RebalanceTimeoutMs: rebalanceTimeoutMs,
		MemberId:           memberId,
		ProtocolType:       protocolType,
		Protocols:          protocols,
	}, nil
}

// ============================================================================
// FUNCTION: EncodeJoinGroupResponse
// Description: Writes a JoinGroupResponse to an io.Writer stream.
// ============================================================================
func EncodeJoinGroupResponse(w io.Writer, res *JoinGroupResponse) error {
	if err := WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}
	if err := WriteInt32(w, res.GenerationId); err != nil {
		return err
	}
	if err := WriteString(w, res.ProtocolName); err != nil {
		return err
	}
	if err := WriteString(w, res.LeaderId); err != nil {
		return err
	}
	if err := WriteString(w, res.MemberId); err != nil {
		return err
	}

	if err := WriteInt32(w, int32(len(res.Members))); err != nil {
		return err
	}

	for _, member := range res.Members {
		if err := WriteString(w, member.MemberId); err != nil {
			return err
		}
		if err := WriteBytes(w, member.Metadata); err != nil {
			return err
		}
	}

	return nil
}

// ============================================================================
// STRUCT: GroupAssignment
// Description: Member assignment provided by the leader in SyncGroupRequest.
// ============================================================================
type GroupAssignment struct {
	MemberId   string // Target member identifier
	Assignment []byte // Raw binary partition assignment byte payload
}

// ============================================================================
// STRUCT: SyncGroupRequest
// Description: Request payload for ApiKey 14 (SyncGroup).
// ============================================================================
type SyncGroupRequest struct {
	GroupId          string            // Consumer group identifier
	GenerationId     int32             // Group generation ID
	MemberId         string            // Consumer member identifier
	GroupAssignments []GroupAssignment // Member partition assignments (sent by Leader)
}

// ============================================================================
// STRUCT: SyncGroupResponse
// Description: Response payload for ApiKey 14 (SyncGroup).
// ============================================================================
type SyncGroupResponse struct {
	ErrorCode  int16  // Error code (0 = NONE)
	Assignment []byte // Assigned partition byte payload for this member
}

// ============================================================================
// FUNCTION: DecodeSyncGroupRequest
// Description: Reads and parses a SyncGroupRequest from an io.Reader stream.
// ============================================================================
func DecodeSyncGroupRequest(r io.Reader) (*SyncGroupRequest, error) {
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

	numAssignments, err := ReadArrayCount(r)
	if err != nil {
		return nil, err
	}

	assignments := make([]GroupAssignment, numAssignments)
	for i := int32(0); i < numAssignments; i++ {
		mId, err := ReadString(r)
		if err != nil {
			return nil, err
		}
		assignBytes, err := ReadBytes(r)
		if err != nil {
			return nil, err
		}
		assignments[i] = GroupAssignment{
			MemberId:   mId,
			Assignment: assignBytes,
		}
	}

	return &SyncGroupRequest{
		GroupId:          groupId,
		GenerationId:     generationId,
		MemberId:         memberId,
		GroupAssignments: assignments,
	}, nil
}

// ============================================================================
// FUNCTION: EncodeSyncGroupResponse
// Description: Writes a SyncGroupResponse to an io.Writer stream.
// ============================================================================
func EncodeSyncGroupResponse(w io.Writer, res *SyncGroupResponse) error {
	if err := WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}
	return WriteBytes(w, res.Assignment)
}

// ============================================================================
// STRUCT: HeartbeatRequest
// Description: Request payload for ApiKey 12 (Heartbeat).
// ============================================================================
type HeartbeatRequest struct {
	GroupId      string // Consumer group identifier
	GenerationId int32  // Group generation ID
	MemberId     string // Consumer member identifier
}

// ============================================================================
// STRUCT: HeartbeatResponse
// Description: Response payload for ApiKey 12 (Heartbeat).
// ============================================================================
type HeartbeatResponse struct {
	ErrorCode int16 // Error code (0 = NONE)
}

// ============================================================================
// FUNCTION: DecodeHeartbeatRequest
// Description: Reads and parses a HeartbeatRequest from an io.Reader stream.
// ============================================================================
func DecodeHeartbeatRequest(r io.Reader) (*HeartbeatRequest, error) {
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

	return &HeartbeatRequest{
		GroupId:      groupId,
		GenerationId: generationId,
		MemberId:     memberId,
	}, nil
}

// ============================================================================
// FUNCTION: EncodeHeartbeatResponse
// Description: Writes a HeartbeatResponse to an io.Writer stream.
// ============================================================================
func EncodeHeartbeatResponse(w io.Writer, res *HeartbeatResponse) error {
	return WriteInt16(w, res.ErrorCode)
}

// ============================================================================
// STRUCT: FindCoordinatorRequest (ApiKey 10)
// Description: Sent by a client to discover which broker acts as the
//              coordinator for a given consumer group (or transactional
//              id), before it can start the JoinGroup flow. Real client
//              libraries send this unconditionally as part of bootstrapping
//              group membership — without a handler for it, they cannot
//              proceed to JoinGroup at all, regardless of whether JoinGroup
//              itself works.
// ============================================================================
type FindCoordinatorRequest struct {
	Key string // Group ID or Transactional ID being looked up
}

// ============================================================================
// STRUCT: FindCoordinatorResponse
// Description: Response payload for ApiKey 10 (FindCoordinator).
// ============================================================================
type FindCoordinatorResponse struct {
	ErrorCode int16  // Error code (0 = NONE)
	NodeId    int32  // Coordinator broker's Node ID
	Host      string // Coordinator broker's advertised host
	Port      int32  // Coordinator broker's advertised port
}

// DecodeFindCoordinatorRequest decodes a FindCoordinatorRequest from an io.Reader stream.
func DecodeFindCoordinatorRequest(r io.Reader) (*FindCoordinatorRequest, error) {
	key, err := ReadString(r)
	if err != nil {
		return nil, err
	}
	return &FindCoordinatorRequest{Key: key}, nil
}

// EncodeFindCoordinatorResponse writes a FindCoordinatorResponse to an io.Writer stream.
func EncodeFindCoordinatorResponse(w io.Writer, res *FindCoordinatorResponse) error {
	if err := WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}
	if err := WriteInt32(w, res.NodeId); err != nil {
		return err
	}
	if err := WriteString(w, res.Host); err != nil {
		return err
	}
	return WriteInt32(w, res.Port)
}

// ============================================================================
// STRUCT: LeaveGroupRequest (ApiKey 13)
// Description: Sent by a consumer to voluntarily leave a group (e.g. on
//              clean shutdown) instead of waiting for a session timeout.
// ============================================================================
type LeaveGroupRequest struct {
	GroupId  string
	MemberId string
}

// ============================================================================
// STRUCT: LeaveGroupResponse
// Description: Response payload for ApiKey 13 (LeaveGroup).
// ============================================================================
type LeaveGroupResponse struct {
	ErrorCode int16
}

// DecodeLeaveGroupRequest decodes a LeaveGroupRequest from an io.Reader stream.
func DecodeLeaveGroupRequest(r io.Reader) (*LeaveGroupRequest, error) {
	groupId, err := ReadString(r)
	if err != nil {
		return nil, err
	}
	memberId, err := ReadString(r)
	if err != nil {
		return nil, err
	}
	return &LeaveGroupRequest{GroupId: groupId, MemberId: memberId}, nil
}

// EncodeLeaveGroupResponse writes a LeaveGroupResponse to an io.Writer stream.
func EncodeLeaveGroupResponse(w io.Writer, res *LeaveGroupResponse) error {
	return WriteInt16(w, res.ErrorCode)
}
