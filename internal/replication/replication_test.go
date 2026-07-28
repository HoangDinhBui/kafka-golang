package replication

import (
	"sync/atomic"
	"testing"
	"time"
)

// ============================================================================
// TEST: TestReplicaManager_HighWatermarkCalculation
// Description: Verifies calculation of partition High Watermark based on follower LEOs.
// ============================================================================
func TestReplicaManager_HighWatermarkCalculation(t *testing.T) {
	rm := NewReplicaManager(1)
	ps := rm.CreatePartition("orders", 0, 1, []int32{1, 2, 3})

	if ps.HighWatermark != 0 {
		t.Errorf("Expected initial HighWatermark 0, got %d", ps.HighWatermark)
	}

	// Leader LEO is 20, Follower 2 reaches 10, Follower 3 is 0
	_, _ = rm.UpdateFollowerLEO("orders", 0, 1, 20)
	hw, err := rm.UpdateFollowerLEO("orders", 0, 2, 10)
	if err != nil {
		t.Fatalf("UpdateFollowerLEO failed: %v", err)
	}

	// HW should remain 0 because Follower 3's LEO is still 0
	if hw != 0 {
		t.Errorf("Expected HighWatermark 0 (min of ISR), got %d", hw)
	}

	// Update Follower 3's LEO to 8 -> HW should advance to 8 (minimum LEO in ISR)
	hw, _ = rm.UpdateFollowerLEO("orders", 0, 3, 8)
	if hw != 8 {
		t.Errorf("Expected HighWatermark 8, got %d", hw)
	}

	// Update Follower 3's LEO to 15 -> HW should advance to 10 (limited by Follower 2 at 10)
	hw, _ = rm.UpdateFollowerLEO("orders", 0, 3, 15)
	if hw != 10 {
		t.Errorf("Expected HighWatermark 10, got %d", hw)
	}

	if gotHW := rm.GetHighWatermark("orders", 0); gotHW != 10 {
		t.Errorf("GetHighWatermark returned %d, expected 10", gotHW)
	}
}

// ============================================================================
// TEST: TestReplicationFetcher_TaskLifecycleAndBatch
// Description: Verifies fetch task registration, execution, and graceful shutdown.
// ============================================================================
func TestReplicationFetcher_TaskLifecycleAndBatch(t *testing.T) {
	fetcher := NewReplicationFetcher(2, "127.0.0.1:9092", 10)

	fetcher.AddFetchTask("events", 0, 0)

	var callbackCalls int32
	mockCallback := func(topic string, partitionId int32, offset int64) (int64, error) {
		atomic.AddInt32(&callbackCalls, 1)
		return offset + 5, nil // Simulate fetching 5 messages
	}

	fetcher.Start(mockCallback)
	time.Sleep(50 * time.Millisecond)
	fetcher.Stop()

	if atomic.LoadInt32(&callbackCalls) == 0 {
		t.Errorf("Expected fetch callback to be executed at least once")
	}

	fetcher.mu.RLock()
	task, exists := fetcher.tasks["events-0"]
	fetcher.mu.RUnlock()

	if !exists {
		t.Fatalf("Task events-0 not found")
	}
	if task.FetchOffset == 0 {
		t.Errorf("Expected FetchOffset to advance above 0, got %d", task.FetchOffset)
	}
}
