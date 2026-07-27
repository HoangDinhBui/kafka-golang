package coordinator

import (
	"errors"
	"sync"
	"time"
)

// ============================================================================
// ERRORS
// ============================================================================
var (
	// ErrOffsetNotFound is returned when fetching an offset that has not been committed yet.
	ErrOffsetNotFound = errors.New("offset not found for group topic partition")
)

// ============================================================================
// STRUCT: TopicPartition
// Description: Identifies a specific partition within a topic.
// ============================================================================
type TopicPartition struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
}

// ============================================================================
// STRUCT: GroupTopicPartition
// Description: Uniquely identifies a committed partition for a consumer group.
// ============================================================================
type GroupTopicPartition struct {
	GroupID   string `json:"group_id"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
}

// ============================================================================
// STRUCT: OffsetCommitMetadata
// Description: Stores offset details, user metadata, and commit timestamp.
// ============================================================================
type OffsetCommitMetadata struct {
	Offset     int64     `json:"offset"`
	Metadata   string    `json:"metadata"`
	CommitTime time.Time `json:"commit_time"`
}

// ============================================================================
// STRUCT: OffsetManager
// Description: Thread-safe in-memory manager for storing and retrieving
//              committed partition offsets for consumer groups.
// ============================================================================
type OffsetManager struct {
	mu      sync.RWMutex
	offsets map[GroupTopicPartition]OffsetCommitMetadata
}

// ============================================================================
// FUNCTION: NewOffsetManager
// Description: Creates and initializes a new thread-safe OffsetManager.
// ============================================================================
func NewOffsetManager() *OffsetManager {
	return &OffsetManager{
		offsets: make(map[GroupTopicPartition]OffsetCommitMetadata),
	}
}

// ============================================================================
// FUNCTION: CommitOffset
// Description: Stores or updates the committed offset and metadata for a
//              given group, topic, and partition.
// ============================================================================
func (om *OffsetManager) CommitOffset(groupID, topic string, partition int32, offset int64, metadata string) error {
	if groupID == "" || topic == "" {
		return errors.New("group_id and topic cannot be empty")
	}

	om.mu.Lock()
	defer om.mu.Unlock()

	key := GroupTopicPartition{
		GroupID:   groupID,
		Topic:     topic,
		Partition: partition,
	}

	om.offsets[key] = OffsetCommitMetadata{
		Offset:     offset,
		Metadata:   metadata,
		CommitTime: time.Now().UTC(),
	}

	return nil
}

// ============================================================================
// FUNCTION: FetchOffset
// Description: Retrieves the committed offset and metadata for a specific
//              group, topic, and partition. Returns ErrOffsetNotFound if uncommitted.
// ============================================================================
func (om *OffsetManager) FetchOffset(groupID, topic string, partition int32) (int64, string, error) {
	om.mu.RLock()
	defer om.mu.RUnlock()

	key := GroupTopicPartition{
		GroupID:   groupID,
		Topic:     topic,
		Partition: partition,
	}

	meta, exists := om.offsets[key]
	if !exists {
		return -1, "", ErrOffsetNotFound
	}

	return meta.Offset, meta.Metadata, nil
}

// ============================================================================
// FUNCTION: FetchGroupOffsets
// Description: Retrieves all committed topic-partition offsets for a given
//              consumer group.
// ============================================================================
func (om *OffsetManager) FetchGroupOffsets(groupID string) map[TopicPartition]OffsetCommitMetadata {
	om.mu.RLock()
	defer om.mu.RUnlock()

	result := make(map[TopicPartition]OffsetCommitMetadata)
	for key, meta := range om.offsets {
		if key.GroupID == groupID {
			result[TopicPartition{Topic: key.Topic, Partition: key.Partition}] = meta
		}
	}

	return result
}

// ============================================================================
// FUNCTION: DeleteGroupOffsets
// Description: Removes all committed partition offsets for a specific
//              consumer group. Returns the number of deleted partitions.
// ============================================================================
func (om *OffsetManager) DeleteGroupOffsets(groupID string) int {
	om.mu.Lock()
	defer om.mu.Unlock()

	deletedCount := 0
	for key := range om.offsets {
		if key.GroupID == groupID {
			delete(om.offsets, key)
			deletedCount++
		}
	}

	return deletedCount
}
