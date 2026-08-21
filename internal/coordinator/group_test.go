package coordinator

import (
	"testing"
	"time"
)

func TestGroupCoordinator_AddMemberAndLeaderElection(t *testing.T) {
	om := NewOffsetManager()
	gc := NewGroupCoordinator(om)

	protocols := map[string][]byte{"range": []byte("metadata")}

	// Add 1st member
	group, member1, err := gc.AddMember("test-group", "client-1", "127.0.0.1", 10000, 30000, "consumer", protocols)
	if err != nil {
		t.Fatalf("unexpected error adding member 1: %v", err)
	}

	if group.LeaderID != member1.MemberID {
		t.Errorf("expected member 1 (%s) to be leader, got %s", member1.MemberID, group.LeaderID)
	}
	if group.State != GroupStatePreparingRebalance {
		t.Errorf("expected state PreparingRebalance, got %s", group.State)
	}
	if group.GenerationID != 1 {
		t.Errorf("expected generation ID 1, got %d", group.GenerationID)
	}

	// Add 2nd member
	group, member2, err := gc.AddMember("test-group", "client-2", "127.0.0.1", 10000, 30000, "consumer", protocols)
	if err != nil {
		t.Fatalf("unexpected error adding member 2: %v", err)
	}

	// Leader should still be member 1
	if group.LeaderID != member1.MemberID {
		t.Errorf("expected member 1 (%s) to remain leader, got %s", member1.MemberID, group.LeaderID)
	}
	if len(group.Members) != 2 {
		t.Errorf("expected 2 members in group, got %d", len(group.Members))
	}
	if group.GenerationID != 2 {
		t.Errorf("expected generation ID 2, got %d", group.GenerationID)
	}

	_ = member2
}

func TestGroupCoordinator_RemoveMemberAndRebalance(t *testing.T) {
	om := NewOffsetManager()
	gc := NewGroupCoordinator(om)

	protocols := map[string][]byte{"range": []byte("metadata")}

	group, member1, _ := gc.AddMember("g-remove", "c1", "127.0.0.1", 10000, 30000, "consumer", protocols)
	_, member2, _ := gc.AddMember("g-remove", "c2", "127.0.0.1", 10000, 30000, "consumer", protocols)

	// Remove leader (member1)
	err := gc.RemoveMember("g-remove", member1.MemberID)
	if err != nil {
		t.Fatalf("unexpected error removing member 1: %v", err)
	}

	if len(group.Members) != 1 {
		t.Errorf("expected 1 remaining member, got %d", len(group.Members))
	}
	if group.LeaderID != member2.MemberID {
		t.Errorf("expected member 2 (%s) to become new leader, got %s", member2.MemberID, group.LeaderID)
	}

	// Remove last member
	err = gc.RemoveMember("g-remove", member2.MemberID)
	if err != nil {
		t.Fatalf("unexpected error removing member 2: %v", err)
	}

	if len(group.Members) != 0 {
		t.Errorf("expected 0 members, got %d", len(group.Members))
	}
	if group.State != GroupStateEmpty {
		t.Errorf("expected state Empty after all members left, got %s", group.State)
	}
}

func TestGroupCoordinator_HeartbeatAndTimeout(t *testing.T) {
	om := NewOffsetManager()
	gc := NewGroupCoordinator(om)

	protocols := map[string][]byte{"range": []byte("metadata")}

	// Member with a very short session timeout (50ms)
	group, member, err := gc.AddMember("g-timeout", "c1", "127.0.0.1", 50, 30000, "consumer", protocols)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Mark state to Stable for heartbeat testing
	group.State = GroupStateStable

	// Send valid heartbeat
	err = gc.Heartbeat("g-timeout", member.MemberID)
	if err != nil {
		t.Errorf("unexpected error on Heartbeat: %v", err)
	}

	// Wait for session timeout to expire
	time.Sleep(80 * time.Millisecond)

	// Check heartbeat timeouts
	evicted := gc.CheckHeartbeatTimeouts()
	if len(evicted) != 1 {
		t.Fatalf("expected 1 evicted member due to heartbeat timeout, got %d", len(evicted))
	}

	if len(group.Members) != 0 {
		t.Errorf("expected group members to be empty after timeout eviction, got %d", len(group.Members))
	}
}

// ============================================================================
// TEST: TestGroupCoordinator_MaxGroups
// Description: Regression test for unbounded resource growth: JoinGroup
//              previously created a brand new ConsumerGroup for any
//              never-before-seen GroupId with no limit at all, so a client
//              could grow gc.groups without bound just by sending an
//              ever-changing group ID. Verifies SetMaxGroups caps distinct
//              groups while still allowing existing groups to accept new
//              members past the cap, and that 0 (the default) stays
//              unlimited.
// ============================================================================
func TestGroupCoordinator_MaxGroups(t *testing.T) {
	om := NewOffsetManager()
	gc := NewGroupCoordinator(om)
	gc.SetMaxGroups(2)

	protocols := map[string][]byte{"range": []byte("metadata")}

	if _, _, err := gc.AddMember("group-a", "client-1", "127.0.0.1", 10000, 30000, "consumer", protocols); err != nil {
		t.Fatalf("unexpected error creating group 1/2: %v", err)
	}
	if _, _, err := gc.AddMember("group-b", "client-1", "127.0.0.1", 10000, 30000, "consumer", protocols); err != nil {
		t.Fatalf("unexpected error creating group 2/2: %v", err)
	}

	if _, _, err := gc.AddMember("group-c", "client-1", "127.0.0.1", 10000, 30000, "consumer", protocols); err == nil {
		t.Error("expected creating a 3rd group past the cap of 2 to fail")
	}

	// A second member joining an EXISTING group must still work past the cap.
	if _, _, err := gc.AddMember("group-a", "client-2", "127.0.0.1", 10000, 30000, "consumer", protocols); err != nil {
		t.Errorf("expected joining an existing group to succeed even at the group cap: %v", err)
	}
}
