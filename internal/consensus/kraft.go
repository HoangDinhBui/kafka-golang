package consensus

import (
	"sync"
)

// ============================================================================
// ENUM: RaftRole
// Description: Represents the current Raft consensus role of a node.
// ============================================================================
type RaftRole int

const (
	RoleFollower  RaftRole = 0
	RoleCandidate RaftRole = 1
	RoleLeader    RaftRole = 2
)

// ============================================================================
// STRUCT: KRaftNode
// Description: Manages Raft metadata consensus state and leader election for KRaft mode.
// ============================================================================
type KRaftNode struct {
	nodeId      int32        // Broker node ID
	clusterSize int          // Total number of nodes in the cluster
	currentTerm int64        // Latest term node has seen
	votedFor    int32        // CandidateId that received vote in current term (-1 if none)
	role        RaftRole     // Current role (Follower, Candidate, Leader)
	leaderId    int32        // Node ID of current cluster Leader Controller (-1 if unknown)
	commitIndex int64        // Index of highest log entry known to be committed
	mu          sync.RWMutex // Mutex protecting Raft consensus state
}

// ============================================================================
// FUNCTION: NewKRaftNode
// Description: Instantiates a new KRaft consensus node.
// ============================================================================
func NewKRaftNode(nodeId int32, clusterSize int) *KRaftNode {
	return &KRaftNode{
		nodeId:      nodeId,
		clusterSize: clusterSize,
		currentTerm: 0,
		votedFor:    -1,
		role:        RoleFollower,
		leaderId:    -1,
		commitIndex: 0,
	}
}

// ============================================================================
// FUNCTION: RequestVote
// Description: Processes an incoming vote request from a candidate node.
// Output: Granted (bool), CurrentTerm (int64)
// ============================================================================
func (kn *KRaftNode) RequestVote(candidateId int32, candidateTerm int64) (bool, int64) {
	kn.mu.Lock()
	defer kn.mu.Unlock()

	// 1. Reject vote if candidate's term is smaller than current term
	if candidateTerm < kn.currentTerm {
		return false, kn.currentTerm
	}

	// 2. Update term and step down to follower if candidate's term is larger
	if candidateTerm > kn.currentTerm {
		kn.currentTerm = candidateTerm
		kn.role = RoleFollower
		kn.votedFor = -1
		kn.leaderId = -1
	}

	// 3. Grant vote if haven't voted or already voted for this candidate
	if kn.votedFor == -1 || kn.votedFor == candidateId {
		kn.votedFor = candidateId
		return true, kn.currentTerm
	}

	return false, kn.currentTerm
}

// ============================================================================
// FUNCTION: BecomeLeader
// Description: Transitions node state to RoleLeader upon winning an election.
// ============================================================================
func (kn *KRaftNode) BecomeLeader(term int64) {
	kn.mu.Lock()
	defer kn.mu.Unlock()

	kn.role = RoleLeader
	kn.currentTerm = term
	kn.leaderId = kn.nodeId
	kn.votedFor = kn.nodeId
}

// ============================================================================
// FUNCTION: BecomeFollower
// Description: Transitions node state to RoleFollower upon discovering a higher term/leader.
// ============================================================================
func (kn *KRaftNode) BecomeFollower(term int64, leaderId int32) {
	kn.mu.Lock()
	defer kn.mu.Unlock()

	kn.role = RoleFollower
	kn.currentTerm = term
	kn.leaderId = leaderId
	kn.votedFor = -1
}

// ============================================================================
// FUNCTION: GetState
// Description: Returns current node role, term, and leader ID thread-safely.
// ============================================================================
func (kn *KRaftNode) GetState() (RaftRole, int64, int32) {
	kn.mu.RLock()
	defer kn.mu.RUnlock()
	return kn.role, kn.currentTerm, kn.leaderId
}
