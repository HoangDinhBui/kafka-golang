package coordinator

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// ============================================================================
// STRUCT: TransactionState
// Description: Internal tracking state for an active transaction.
// ============================================================================
type TxnPartitionTarget struct {
	Topic     string
	Partition int32
}

type TxnMetaData struct {
	TransactionalId string
	ProducerId      int64
	ProducerEpoch   int16
	State           string // "Empty", "Ongoing", "PrepareCommit", "PrepareAbort"
	Partitions      map[string][]int32
}

// ============================================================================
// STRUCT: TransactionCoordinator
// Description: Manages Producer ID allocation, transaction lifecycle, and
//              sequence deduplication for Idempotent/Transactional producers.
// ============================================================================
type TransactionCoordinator struct {
	mu                sync.RWMutex
	producerIdCounter int64
	txns              map[string]*TxnMetaData       // txId -> TxnMetaData
	producerTxns      map[int64]string              // producerId -> txId
	sequences         map[string]int32              // "producerId:topic:partition" -> lastSeq
}

func NewTransactionCoordinator() *TransactionCoordinator {
	return &TransactionCoordinator{
		producerIdCounter: 1000,
		txns:              make(map[string]*TxnMetaData),
		producerTxns:      make(map[int64]string),
		sequences:         make(map[string]int32),
	}
}

// ============================================================================
// METHOD: InitProducerId
// Description: Assigns a unique ProducerId or increments ProducerEpoch.
// ============================================================================
func (tc *TransactionCoordinator) InitProducerId(transactionalId *string, timeoutMs int32) (int64, int16, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if transactionalId == nil || *transactionalId == "" {
		// Non-transactional Idempotent Producer
		pid := atomic.AddInt64(&tc.producerIdCounter, 1)
		return pid, 0, nil
	}

	txId := *transactionalId
	txn, exists := tc.txns[txId]
	if !exists {
		pid := atomic.AddInt64(&tc.producerIdCounter, 1)
		txn = &TxnMetaData{
			TransactionalId: txId,
			ProducerId:      pid,
			ProducerEpoch:   0,
			State:           "Empty",
			Partitions:      make(map[string][]int32),
		}
		tc.txns[txId] = txn
		tc.producerTxns[pid] = txId
		return pid, 0, nil
	}

	// Increment epoch for existing transaction ID (Producer fence)
	txn.ProducerEpoch++
	txn.State = "Empty"
	txn.Partitions = make(map[string][]int32)
	return txn.ProducerId, txn.ProducerEpoch, nil
}

// ============================================================================
// METHOD: AddPartitionsToTxn
// Description: Registers topic partitions involved in an ongoing transaction.
// ============================================================================
func (tc *TransactionCoordinator) AddPartitionsToTxn(txId string, pid int64, epoch int16, topic string, partitions []int32) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	txn, exists := tc.txns[txId]
	if !exists || txn.ProducerId != pid || txn.ProducerEpoch != epoch {
		return fmt.Errorf("invalid producer epoch or transactional ID %s", txId)
	}

	txn.State = "Ongoing"
	txn.Partitions[topic] = append(txn.Partitions[topic], partitions...)
	return nil
}

// ============================================================================
// METHOD: EndTxn
// Description: Finalizes transaction state (COMMIT or ABORT) and returns target partitions.
// ============================================================================
func (tc *TransactionCoordinator) EndTxn(txId string, pid int64, epoch int16, committed bool) ([]TxnPartitionTarget, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	txn, exists := tc.txns[txId]
	if !exists || txn.ProducerId != pid || txn.ProducerEpoch != epoch {
		return nil, fmt.Errorf("invalid producer epoch or transactional ID %s", txId)
	}

	var targets []TxnPartitionTarget
	for topic, parts := range txn.Partitions {
		for _, part := range parts {
			targets = append(targets, TxnPartitionTarget{
				Topic:     topic,
				Partition: part,
			})
		}
	}

	if committed {
		txn.State = "CompleteCommit"
	} else {
		txn.State = "CompleteAbort"
	}

	// Reset active transaction partitions
	txn.Partitions = make(map[string][]int32)
	return targets, nil
}

// ============================================================================
// METHOD: CheckAndSetSequence
// Description: Deduplicates idempotent retries based on (ProducerId, Topic, Partition, Sequence).
// Output:
//   - isDuplicate bool: true if message sequence was already processed
// ============================================================================
func (tc *TransactionCoordinator) CheckAndSetSequence(pid int64, topic string, partition int32, seq int32) (bool, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	key := fmt.Sprintf("%d:%s:%d", pid, topic, partition)
	lastSeq, exists := tc.sequences[key]
	if !exists {
		tc.sequences[key] = seq
		return false, nil
	}

	if seq <= lastSeq {
		// Duplicate record sequence received via network retry
		return true, nil
	}

	tc.sequences[key] = seq
	return false, nil
}
