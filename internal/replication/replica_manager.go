package replication

import (
	"fmt"
	"sync"
)

// ============================================================================
// STRUCT: Replica
// Description: Represents a replica node holding partition data and its Log End Offset.
// ============================================================================
type Replica struct {
	BrokerId int32 // Node ID of the broker hosting this replica
	LEO      int64 // Log End Offset (highest offset written to this replica's log)
}

// ============================================================================
// STRUCT: PartitionState
// Description: Tracks leader, follower replicas, ISR set, and High Watermark for a partition.
// ============================================================================
type PartitionState struct {
	Topic         string             // Name of the topic
	PartitionId   int32              // Partition index (0-indexed)
	LeaderId      int32              // Node ID of the current leader broker
	Replicas      map[int32]*Replica // Map of broker NodeID to Replica tracker
	ISR           []int32            // In-Sync Replicas list (Broker IDs)
	HighWatermark int64              // High Watermark offset committed across ISR
	mu            sync.RWMutex       // Mutex protecting partition replication state
}

// ============================================================================
// STRUCT: ReplicaManager
// Description: Manages partition replication states and High Watermarks across the broker.
// ============================================================================
type ReplicaManager struct {
	localBrokerId int32                      // Local broker node ID
	partitions    map[string]*PartitionState // Map of "topic-partitionId" to PartitionState
	mu            sync.RWMutex               // Mutex protecting partitions map
}

// ============================================================================
// FUNCTION: NewReplicaManager
// Description: Instantiates a new ReplicaManager for the local broker.
// ============================================================================
func NewReplicaManager(localBrokerId int32) *ReplicaManager {
	return &ReplicaManager{
		localBrokerId: localBrokerId,
		partitions:    make(map[string]*PartitionState),
	}
}

// ============================================================================
// FUNCTION: CreatePartition
// Description: Initializes replication state for a new topic partition.
// ============================================================================
func (rm *ReplicaManager) CreatePartition(topic string, partitionId int32, leaderId int32, replicaIds []int32) *PartitionState {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topic, partitionId)
	if ps, exists := rm.partitions[key]; exists {
		return ps
	}

	replicasMap := make(map[int32]*Replica)
	for _, id := range replicaIds {
		replicasMap[id] = &Replica{
			BrokerId: id,
			LEO:      0,
		}
	}

	// Default ISR set includes all initial replicas
	isr := make([]int32, len(replicaIds))
	copy(isr, replicaIds)

	ps := &PartitionState{
		Topic:         topic,
		PartitionId:   partitionId,
		LeaderId:      leaderId,
		Replicas:      replicasMap,
		ISR:           isr,
		HighWatermark: 0,
	}

	rm.partitions[key] = ps
	return ps
}

// ============================================================================
// FUNCTION: UpdateFollowerLEO
// Description: Updates a follower replica's LEO and recalculates the High Watermark.
// ============================================================================
func (rm *ReplicaManager) UpdateFollowerLEO(topic string, partitionId int32, followerId int32, leo int64) (int64, error) {
	rm.mu.RLock()
	key := fmt.Sprintf("%s-%d", topic, partitionId)
	ps, exists := rm.partitions[key]
	rm.mu.RUnlock()

	if !exists {
		return 0, fmt.Errorf("partition state not found for %s", key)
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	replica, exists := ps.Replicas[followerId]
	if !exists {
		return ps.HighWatermark, fmt.Errorf("replica %d not found in partition %s", followerId, key)
	}

	replica.LEO = leo

	// Recalculate High Watermark (minimum LEO among all ISR members)
	var minISRLEO int64 = -1
	for _, isrId := range ps.ISR {
		if r, ok := ps.Replicas[isrId]; ok {
			if minISRLEO == -1 || r.LEO < minISRLEO {
				minISRLEO = r.LEO
			}
		}
	}

	if minISRLEO > ps.HighWatermark {
		ps.HighWatermark = minISRLEO
	}

	return ps.HighWatermark, nil
}

// ============================================================================
// FUNCTION: GetHighWatermark
// Description: Returns the current High Watermark for a partition.
// ============================================================================
func (rm *ReplicaManager) GetHighWatermark(topic string, partitionId int32) int64 {
	rm.mu.RLock()
	key := fmt.Sprintf("%s-%d", topic, partitionId)
	ps, exists := rm.partitions[key]
	rm.mu.RUnlock()

	if !exists {
		return 0
	}

	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.HighWatermark
}
