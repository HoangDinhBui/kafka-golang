package coordinator

import (
	"testing"
	"time"
)

func TestEndToEndConsumerGroupLifecycle(t *testing.T) {
	offsetMgr := NewOffsetManager()
	groupCoord := NewGroupCoordinator(offsetMgr)

	groupID := "e2e-group"
	protocols := map[string][]byte{"range": []byte{0x01}}

	// 1. Consumer 1 joins group
	group, member1, err := groupCoord.AddMember(groupID, "worker-1", "127.0.0.1", 10000, 30000, "consumer", protocols)
	if err != nil {
		t.Fatalf("failed to add worker-1: %v", err)
	}

	if group.LeaderID != member1.MemberID {
		t.Fatalf("expected worker-1 to be leader, got %s", group.LeaderID)
	}

	// 2. Consumer 2 joins group -> Triggers rebalance
	group, member2, err := groupCoord.AddMember(groupID, "worker-2", "127.0.0.1", 10000, 30000, "consumer", protocols)
	if err != nil {
		t.Fatalf("failed to add worker-2: %v", err)
	}

	if group.GenerationID != 2 {
		t.Errorf("expected generation ID 2 after worker-2 joined, got %d", group.GenerationID)
	}

	// 3. Leader completes SyncGroup assignment
	group.State = GroupStateStable

	// 4. Send Heartbeats
	if err := groupCoord.Heartbeat(groupID, member1.MemberID); err != nil {
		t.Errorf("unexpected error on worker-1 heartbeat: %v", err)
	}
	if err := groupCoord.Heartbeat(groupID, member2.MemberID); err != nil {
		t.Errorf("unexpected error on worker-2 heartbeat: %v", err)
	}

	// 5. Commit offsets for worker-1 and worker-2
	err1 := offsetMgr.CommitOffset(groupID, "orders", 0, 500, "p0 committed")
	err2 := offsetMgr.CommitOffset(groupID, "orders", 1, 750, "p1 committed")
	if err1 != nil || err2 != nil {
		t.Fatalf("failed to commit offsets: %v %v", err1, err2)
	}

	// 6. Fetch committed offsets
	offP0, metaP0, err := offsetMgr.FetchOffset(groupID, "orders", 0)
	if err != nil || offP0 != 500 || metaP0 != "p0 committed" {
		t.Errorf("expected p0 offset 500 'p0 committed', got %d '%s' (err: %v)", offP0, metaP0, err)
	}

	offP1, metaP1, err := offsetMgr.FetchOffset(groupID, "orders", 1)
	if err != nil || offP1 != 750 || metaP1 != "p1 committed" {
		t.Errorf("expected p1 offset 750 'p1 committed', got %d '%s' (err: %v)", offP1, metaP1, err)
	}

	// 7. Worker-1 leaves group
	if err := groupCoord.RemoveMember(groupID, member1.MemberID); err != nil {
		t.Fatalf("failed to remove worker-1: %v", err)
	}

	// Verify worker-2 is now leader
	if group.LeaderID != member2.MemberID {
		t.Errorf("expected worker-2 to become leader after worker-1 left, got %s", group.LeaderID)
	}
}

func TestHeartbeatTimeoutEvictionIntegration(t *testing.T) {
	offsetMgr := NewOffsetManager()
	groupCoord := NewGroupCoordinator(offsetMgr)

	groupID := "timeout-group"
	protocols := map[string][]byte{"range": []byte{0x01}}

	// Add member with 40ms session timeout
	group, member, err := groupCoord.AddMember(groupID, "short-worker", "127.0.0.1", 40, 30000, "consumer", protocols)
	if err != nil {
		t.Fatalf("unexpected error adding member: %v", err)
	}

	group.State = GroupStateStable

	time.Sleep(60 * time.Millisecond)

	evicted := groupCoord.CheckHeartbeatTimeouts()
	if len(evicted) != 1 {
		t.Fatalf("expected 1 member evicted due to timeout, got %d", len(evicted))
	}

	_ = member
}
