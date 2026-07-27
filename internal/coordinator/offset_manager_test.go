package coordinator

import (
	"sync"
	"testing"
)

func TestOffsetManager_CommitAndFetch(t *testing.T) {
	om := NewOffsetManager()

	// Commit offset for group "g1", topic "t1", partition 0
	err := om.CommitOffset("g1", "t1", 0, 105, "user metadata")
	if err != nil {
		t.Fatalf("unexpected error on CommitOffset: %v", err)
	}

	// Fetch offset
	offset, metadata, err := om.FetchOffset("g1", "t1", 0)
	if err != nil {
		t.Fatalf("unexpected error on FetchOffset: %v", err)
	}

	if offset != 105 {
		t.Errorf("expected offset 105, got %d", offset)
	}
	if metadata != "user metadata" {
		t.Errorf("expected metadata 'user metadata', got '%s'", metadata)
	}
}

func TestOffsetManager_FetchNotFound(t *testing.T) {
	om := NewOffsetManager()

	offset, _, err := om.FetchOffset("non-existent-group", "t1", 0)
	if err != ErrOffsetNotFound {
		t.Errorf("expected ErrOffsetNotFound, got %v", err)
	}
	if offset != -1 {
		t.Errorf("expected offset -1 for non-existent offset, got %d", offset)
	}
}

func TestOffsetManager_FetchGroupOffsets(t *testing.T) {
	om := NewOffsetManager()

	_ = om.CommitOffset("g1", "t1", 0, 100, "")
	_ = om.CommitOffset("g1", "t1", 1, 200, "")
	_ = om.CommitOffset("g2", "t2", 0, 300, "") // different group

	group1Offsets := om.FetchGroupOffsets("g1")
	if len(group1Offsets) != 2 {
		t.Fatalf("expected 2 partitions committed for group1, got %d", len(group1Offsets))
	}

	p0Meta, ok0 := group1Offsets[TopicPartition{Topic: "t1", Partition: 0}]
	if !ok0 || p0Meta.Offset != 100 {
		t.Errorf("expected partition 0 offset 100, got %v", p0Meta)
	}

	p1Meta, ok1 := group1Offsets[TopicPartition{Topic: "t1", Partition: 1}]
	if !ok1 || p1Meta.Offset != 200 {
		t.Errorf("expected partition 1 offset 200, got %v", p1Meta)
	}
}

func TestOffsetManager_ConcurrentCommit(t *testing.T) {
	om := NewOffsetManager()
	var wg sync.WaitGroup

	numRoutines := 50
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func(partition int32) {
			defer wg.Done()
			_ = om.CommitOffset("concurrent-group", "topic-test", partition, int64(partition*10), "")
		}(int32(i))
	}

	wg.Wait()

	groupOffsets := om.FetchGroupOffsets("concurrent-group")
	if len(groupOffsets) != numRoutines {
		t.Errorf("expected %d committed offsets, got %d", numRoutines, len(groupOffsets))
	}
}

func TestOffsetManager_DeleteGroupOffsets(t *testing.T) {
	om := NewOffsetManager()

	_ = om.CommitOffset("g-del", "t1", 0, 50, "")
	_ = om.CommitOffset("g-del", "t1", 1, 60, "")
	_ = om.CommitOffset("g-keep", "t1", 0, 70, "")

	deleted := om.DeleteGroupOffsets("g-del")
	if deleted != 2 {
		t.Errorf("expected 2 deleted offsets, got %d", deleted)
	}

	// Verify group-del is gone
	_, _, err := om.FetchOffset("g-del", "t1", 0)
	if err != ErrOffsetNotFound {
		t.Errorf("expected ErrOffsetNotFound after deletion, got %v", err)
	}

	// Verify group-keep is still intact
	offset, _, err := om.FetchOffset("g-keep", "t1", 0)
	if err != nil || offset != 70 {
		t.Errorf("expected offset 70 for kept group, got offset %d (err: %v)", offset, err)
	}
}
