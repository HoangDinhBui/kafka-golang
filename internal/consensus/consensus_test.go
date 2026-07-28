package consensus

import (
	"testing"
)

// ============================================================================
// TEST: TestKRaftNode_StateTransitionsAndVoting
// Description: Verifies Raft candidate voting and role state transitions.
// ============================================================================
func TestKRaftNode_StateTransitionsAndVoting(t *testing.T) {
	node := NewKRaftNode(1, 3)

	role, term, leaderId := node.GetState()
	if role != RoleFollower || term != 0 || leaderId != -1 {
		t.Errorf("Unexpected initial state: role %v, term %d, leader %d", role, term, leaderId)
	}

	// 1. Candidate with higher term (term 1) requesting vote -> Granted
	granted, currentTerm := node.RequestVote(2, 1)
	if !granted || currentTerm != 1 {
		t.Errorf("Expected vote granted for candidate with term 1, got granted=%v, term=%d", granted, currentTerm)
	}

	// 2. Candidate with smaller term (term 0 < currentTerm 1) requesting vote -> Rejected
	granted, _ = node.RequestVote(4, 0)
	if granted {
		t.Errorf("Expected vote rejection for candidate with term < currentTerm")
	}

	// 3. Second candidate with same term (term 1) requesting vote -> Rejected (already voted for node 2)
	granted, _ = node.RequestVote(3, 1)
	if granted {
		t.Errorf("Expected vote rejection because node already voted in term 1")
	}

	// 4. Node wins election in term 2 -> BecomeLeader
	node.BecomeLeader(2)
	role, term, leaderId = node.GetState()
	if role != RoleLeader || term != 2 || leaderId != 1 {
		t.Errorf("Expected RoleLeader with term 2 and leaderId 1, got role %v, term %d, leader %d", role, term, leaderId)
	}

	// 5. Node discovers higher term 3 from Leader 2 -> BecomeFollower
	node.BecomeFollower(3, 2)
	role, term, leaderId = node.GetState()
	if role != RoleFollower || term != 3 || leaderId != 2 {
		t.Errorf("Expected RoleFollower with term 3 and leaderId 2, got role %v, term %d, leader %d", role, term, leaderId)
	}
}
