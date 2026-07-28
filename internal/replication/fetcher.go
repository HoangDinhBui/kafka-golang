package replication

import (
	"fmt"
	"sync"
	"time"
)

// ============================================================================
// STRUCT: FetchTopicPartition
// Description: Represents a partition fetch task assigned to the follower fetcher.
// ============================================================================
type FetchTopicPartition struct {
	Topic       string // Name of the topic to fetch
	PartitionId int32  // Partition index
	FetchOffset int64  // Current fetch offset for this partition
}

// ============================================================================
// STRUCT: ReplicationFetcher
// Description: Background worker that continuously fetches logs from Leader brokers.
// ============================================================================
type ReplicationFetcher struct {
	localBrokerId int32                           // Local follower broker ID
	leaderAddr    string                          // TCP address of the Leader broker
	intervalMs    int                             // Fetch polling interval in milliseconds
	tasks         map[string]*FetchTopicPartition // Active fetch tasks map (key: topic-partitionId)
	quit          chan struct{}                   // Signal channel for stopping the fetcher
	wg            sync.WaitGroup                  // WaitGroup for background fetch routines
	mu            sync.RWMutex                    // Mutex protecting fetch tasks map
}

// ============================================================================
// FUNCTION: NewReplicationFetcher
// Description: Instantiates a new ReplicationFetcher.
// ============================================================================
func NewReplicationFetcher(localBrokerId int32, leaderAddr string, intervalMs int) *ReplicationFetcher {
	return &ReplicationFetcher{
		localBrokerId: localBrokerId,
		leaderAddr:    leaderAddr,
		intervalMs:    intervalMs,
		tasks:         make(map[string]*FetchTopicPartition),
		quit:          make(chan struct{}),
	}
}

// ============================================================================
// FUNCTION: AddFetchTask
// Description: Registers a partition for the follower to start fetching from the leader.
// ============================================================================
func (rf *ReplicationFetcher) AddFetchTask(topic string, partitionId int32, initialOffset int64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topic, partitionId)
	rf.tasks[key] = &FetchTopicPartition{
		Topic:       topic,
		PartitionId: partitionId,
		FetchOffset: initialOffset,
	}
}

// ============================================================================
// FUNCTION: RemoveFetchTask
// Description: Removes a partition fetch task when the broker is no longer a follower.
// ============================================================================
func (rf *ReplicationFetcher) RemoveFetchTask(topic string, partitionId int32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topic, partitionId)
	delete(rf.tasks, key)
}

// ============================================================================
// FUNCTION: Start
// Description: Starts the background replication loop using the provided fetcher callback.
// ============================================================================
func (rf *ReplicationFetcher) Start(fetchCallback func(topic string, partitionId int32, offset int64) (int64, error)) {
	rf.wg.Add(1)
	go func() {
		defer rf.wg.Done()
		ticker := time.NewTicker(time.Duration(rf.intervalMs) * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-rf.quit:
				return
			case <-ticker.C:
				rf.fetchBatch(fetchCallback)
			}
		}
	}()
}

// ============================================================================
// FUNCTION: Stop
// Description: Stops the background replication fetcher routine gracefully.
// ============================================================================
func (rf *ReplicationFetcher) Stop() {
	close(rf.quit)
	rf.wg.Wait()
}

// ============================================================================
// PRIVATE METHOD: fetchBatch
// Description: Iterates over assigned tasks and invokes the fetch callback to replicate logs.
// ============================================================================
func (rf *ReplicationFetcher) fetchBatch(fetchCallback func(topic string, partitionId int32, offset int64) (int64, error)) {
	rf.mu.RLock()
	tasksCopy := make([]*FetchTopicPartition, 0, len(rf.tasks))
	for _, task := range rf.tasks {
		tasksCopy = append(tasksCopy, task)
	}
	rf.mu.RUnlock()

	for _, task := range tasksCopy {
		nextOffset, err := fetchCallback(task.Topic, task.PartitionId, task.FetchOffset)
		if err == nil && nextOffset > task.FetchOffset {
			rf.mu.Lock()
			task.FetchOffset = nextOffset
			rf.mu.Unlock()
		}
	}
}
