package coordinator

import (
	"testing"
)

func TestTransactionCoordinator_InitAndEpoch(t *testing.T) {
	tc := NewTransactionCoordinator()

	txId := "app-tx-1"
	pid1, epoch1, err := tc.InitProducerId(&txId, 60000)
	if err != nil {
		t.Fatalf("InitProducerId failed: %v", err)
	}

	if pid1 <= 0 || epoch1 != 0 {
		t.Errorf("Expected pid > 0 and epoch 0, got pid=%d epoch=%d", pid1, epoch1)
	}

	// Re-init with same txId -> increment epoch
	pid2, epoch2, err := tc.InitProducerId(&txId, 60000)
	if err != nil {
		t.Fatalf("Re-InitProducerId failed: %v", err)
	}

	if pid2 != pid1 {
		t.Errorf("Expected same pid %d, got %d", pid1, pid2)
	}
	if epoch2 != 1 {
		t.Errorf("Expected epoch 1, got %d", epoch2)
	}
}

func TestTransactionCoordinator_AddPartitionsAndEndTxn(t *testing.T) {
	tc := NewTransactionCoordinator()

	txId := "tx-checkout"
	pid, epoch, _ := tc.InitProducerId(&txId, 60000)

	err := tc.AddPartitionsToTxn(txId, pid, epoch, "orders", []int32{0, 1})
	if err != nil {
		t.Fatalf("AddPartitionsToTxn failed: %v", err)
	}

	targets, err := tc.EndTxn(txId, pid, epoch, true)
	if err != nil {
		t.Fatalf("EndTxn failed: %v", err)
	}

	if len(targets) != 2 {
		t.Errorf("Expected 2 targets, got %d", len(targets))
	}
}

func TestTransactionCoordinator_SequenceDeduplication(t *testing.T) {
	tc := NewTransactionCoordinator()

	pid := int64(1005)
	topic := "payments"
	part := int32(0)

	// First send sequence 0
	isDup, err := tc.CheckAndSetSequence(pid, topic, part, 0)
	if err != nil || isDup {
		t.Errorf("Expected first seq 0 to not be duplicate, got dup=%v, err=%v", isDup, err)
	}

	// Retry send sequence 0
	isDup2, err := tc.CheckAndSetSequence(pid, topic, part, 0)
	if err != nil || !isDup2 {
		t.Errorf("Expected retry seq 0 to be duplicate, got dup=%v, err=%v", isDup2, err)
	}

	// Send sequence 1
	isDup3, err := tc.CheckAndSetSequence(pid, topic, part, 1)
	if err != nil || isDup3 {
		t.Errorf("Expected seq 1 to not be duplicate, got dup=%v, err=%v", isDup3, err)
	}
}
