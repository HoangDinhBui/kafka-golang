package coordinator

import (
	"errors"
	"fmt"

	"sync"
	"time"
)

// ============================================================================
// CONSTANTS & ENUMS: GroupState
// Description: Represents the lifecycle state machine of a Consumer Group.
// ============================================================================
type GroupState string

const (
	GroupStateEmpty               GroupState = "Empty"
	GroupStatePreparingRebalance GroupState = "PreparingRebalance"
	GroupStateCompletingRebalance GroupState = "CompletingRebalance"
	GroupStateStable              GroupState = "Stable"
	GroupStateDead                GroupState = "Dead"
)

// ============================================================================
// ERRORS
// Description: Common errors returned by Group Coordinator operations.
// ============================================================================
var (
	ErrGroupNotFound        = errors.New("consumer group not found")
	ErrMemberNotFound       = errors.New("group member not found")
	ErrUnknownMemberID      = errors.New("unknown member id")
	ErrIllegalGeneration    = errors.New("illegal generation id")
	ErrRebalanceInProgress  = errors.New("rebalance in progress")
	ErrInvalidSessionTimeout = errors.New("invalid session timeout")
)

// ============================================================================
// STRUCT: GroupMember
// Description: Metadata for a single active member inside a Consumer Group.
// ============================================================================
type GroupMember struct {
	MemberID           string            `json:"member_id"`
	ClientID           string            `json:"client_id"`
	ClientHost         string            `json:"client_host"`
	SessionTimeout     time.Duration     `json:"session_timeout"`
	RebalanceTimeout   time.Duration     `json:"rebalance_timeout"`
	ProtocolType       string            `json:"protocol_type"`
	SupportedProtocols map[string][]byte `json:"supported_protocols"`
	Assignment         []byte            `json:"assignment"`
	LastHeartbeat      time.Time         `json:"last_heartbeat"`
}

// ============================================================================
// STRUCT: ConsumerGroup
// Description: State machine and member registry for a Consumer Group.
// ============================================================================
type ConsumerGroup struct {
	GroupID      string                  `json:"group_id"`
	State        GroupState              `json:"state"`
	ProtocolType string                  `json:"protocol_type"`
	Protocol     string                  `json:"protocol"`
	LeaderID     string                  `json:"leader_id"`
	GenerationID int32                   `json:"generation_id"`
	Members      map[string]*GroupMember `json:"members"`
	mu           sync.RWMutex
}

// ============================================================================
// STRUCT: GroupCoordinator
// Description: Manages consumer group lifecycles, rebalancing, heartbeats,
//              and offset management across all active consumer groups.
// ============================================================================
type GroupCoordinator struct {
	mu            sync.RWMutex
	groups        map[string]*ConsumerGroup
	offsetManager *OffsetManager
}

// ============================================================================
// FUNCTION: NewGroupCoordinator
// Description: Creates and initializes a new GroupCoordinator instance.
// ============================================================================
func NewGroupCoordinator(offsetManager *OffsetManager) *GroupCoordinator {
	return &GroupCoordinator{
		groups:        make(map[string]*ConsumerGroup),
		offsetManager: offsetManager,
	}
}

// ============================================================================
// FUNCTION: GetOrCreateGroup
// Description: Retrieves an existing ConsumerGroup or creates a new Empty group.
// ============================================================================
func (gc *GroupCoordinator) GetOrCreateGroup(groupID string, protocolType string) *ConsumerGroup {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	group, exists := gc.groups[groupID]
	if !exists {
		group = &ConsumerGroup{
			GroupID:      groupID,
			State:        GroupStateEmpty,
			ProtocolType: protocolType,
			Members:      make(map[string]*GroupMember),
		}
		gc.groups[groupID] = group
	}

	return group
}

// ============================================================================
// FUNCTION: AddMember
// Description: Registers a new member into a consumer group, triggering a rebalance.
// ============================================================================
func (gc *GroupCoordinator) AddMember(
	groupID string,
	clientID string,
	clientHost string,
	sessionTimeoutMs int32,
	rebalanceTimeoutMs int32,
	protocolType string,
	protocols map[string][]byte,
) (*ConsumerGroup, *GroupMember, error) {
	if groupID == "" {
		return nil, nil, errors.New("group_id cannot be empty")
	}

	group := gc.GetOrCreateGroup(groupID, protocolType)

	group.mu.Lock()
	defer group.mu.Unlock()

	// Generate a unique member ID if not specified
	memberID := fmt.Sprintf("%s-%s", clientID, generateUUIDShort())

	member := &GroupMember{
		MemberID:           memberID,
		ClientID:           clientID,
		ClientHost:         clientHost,
		SessionTimeout:     time.Duration(sessionTimeoutMs) * time.Millisecond,
		RebalanceTimeout:   time.Duration(rebalanceTimeoutMs) * time.Millisecond,
		ProtocolType:       protocolType,
		SupportedProtocols: protocols,
		LastHeartbeat:      time.Now().UTC(),
	}

	group.Members[memberID] = member

	// Elect Leader if group was empty
	if group.LeaderID == "" || len(group.Members) == 1 {
		group.LeaderID = memberID
	}

	// Trigger rebalance state transition
	group.State = GroupStatePreparingRebalance
	group.GenerationID++

	return group, member, nil
}

// ============================================================================
// FUNCTION: RemoveMember
// Description: Evicts a member from a consumer group and triggers rebalance.
// ============================================================================
func (gc *GroupCoordinator) RemoveMember(groupID string, memberID string) error {
	gc.mu.RLock()
	group, exists := gc.groups[groupID]
	gc.mu.RUnlock()

	if !exists {
		return ErrGroupNotFound
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	if _, ok := group.Members[memberID]; !ok {
		return ErrMemberNotFound
	}

	delete(group.Members, memberID)

	if len(group.Members) == 0 {
		group.State = GroupStateEmpty
		group.LeaderID = ""
	} else {
		// If Leader left, elect a new Leader from remaining members
		if group.LeaderID == memberID {
			for newLeaderID := range group.Members {
				group.LeaderID = newLeaderID
				break
			}
		}
		group.State = GroupStatePreparingRebalance
		group.GenerationID++
	}

	return nil
}

// ============================================================================
// FUNCTION: Heartbeat
// Description: Updates the last heartbeat timestamp for a group member.
// ============================================================================
func (gc *GroupCoordinator) Heartbeat(groupID string, memberID string) error {
	gc.mu.RLock()
	group, exists := gc.groups[groupID]
	gc.mu.RUnlock()

	if !exists {
		return ErrGroupNotFound
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	member, ok := group.Members[memberID]
	if !ok {
		return ErrUnknownMemberID
	}

	if group.State == GroupStatePreparingRebalance {
		return ErrRebalanceInProgress
	}

	member.LastHeartbeat = time.Now().UTC()
	return nil
}

// ============================================================================
// FUNCTION: CheckHeartbeatTimeouts
// Description: Scans all consumer groups and evicts members whose session
//              timeout has expired without receiving a heartbeat.
// ============================================================================
func (gc *GroupCoordinator) CheckHeartbeatTimeouts() []string {
	gc.mu.RLock()
	defer gc.mu.RUnlock()

	var evictedMembers []string
	now := time.Now().UTC()

	for _, group := range gc.groups {
		group.mu.Lock()
		for memberID, member := range group.Members {
			if now.Sub(member.LastHeartbeat) > member.SessionTimeout {
				delete(group.Members, memberID)
				evictedMembers = append(evictedMembers, fmt.Sprintf("%s/%s", group.GroupID, memberID))

				// Elect new Leader if needed
				if group.LeaderID == memberID {
					group.LeaderID = ""
					for newLeaderID := range group.Members {
						group.LeaderID = newLeaderID
						break
					}
				}

				if len(group.Members) == 0 {
					group.State = GroupStateEmpty
				} else {
					group.State = GroupStatePreparingRebalance
					group.GenerationID++
				}
			}
		}
		group.mu.Unlock()
	}

	return evictedMembers
}

// ============================================================================
// PRIVATE HELPER: generateUUIDShort
// Description: Generates a short pseudo-random string for member IDs.
// ============================================================================
func generateUUIDShort() string {
	return fmt.Sprintf("%x", time.Now().UnixNano()%1000000)
}
