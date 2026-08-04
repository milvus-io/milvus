package rootcoord

import (
	"errors"
	"testing"
	"time"
)

func TestTransferGateAllowsActiveCollection(t *testing.T) {
	gate := newTransferGate()

	done, err := gate.BeginUserOperation(100, 0)
	if err != nil {
		t.Fatalf("collection without transfer state should be active: %v", err)
	}
	done()
}

func TestTransferGateRejectsUserOperationAfterFreeze(t *testing.T) {
	gate := newTransferGate()
	if err := gate.Freeze(100, "transfer-1", 7); err != nil {
		t.Fatalf("freeze: %v", err)
	}

	if _, err := gate.BeginUserOperation(100, 7); !errors.Is(err, errCollectionTransferring) {
		t.Fatalf("user operation after freeze should reject, got %v", err)
	}
}

func TestTransferGateRejectsStaleEpoch(t *testing.T) {
	gate := newTransferGate()
	if err := gate.Freeze(100, "transfer-1", 7); err != nil {
		t.Fatalf("freeze: %v", err)
	}

	if err := gate.AllowTransferOperation(100, "transfer-1", 6, transferStateTransferringOut); !errors.Is(err, errStaleTransferEpoch) {
		t.Fatalf("stale transfer epoch should reject, got %v", err)
	}
}

func TestTransferGateAllowsMatchingTransferOperation(t *testing.T) {
	gate := newTransferGate()
	if err := gate.Freeze(100, "transfer-1", 7); err != nil {
		t.Fatalf("freeze: %v", err)
	}

	if err := gate.AllowTransferOperation(100, "transfer-1", 7, transferStateTransferringOut); err != nil {
		t.Fatalf("matching transfer should pass: %v", err)
	}
}

func TestTransferGateRejectsWrongTransferID(t *testing.T) {
	gate := newTransferGate()
	if err := gate.Freeze(100, "transfer-1", 7); err != nil {
		t.Fatalf("freeze: %v", err)
	}

	if err := gate.AllowTransferOperation(100, "transfer-2", 7, transferStateTransferringOut); !errors.Is(err, errTransferMismatch) {
		t.Fatalf("wrong transfer id should reject, got %v", err)
	}
}

func TestTransferGateRejectsInvalidFreezeInput(t *testing.T) {
	gate := newTransferGate()

	if err := gate.Freeze(100, "", 7); !errors.Is(err, errTransferIDRequired) {
		t.Fatalf("empty transfer id should reject, got %v", err)
	}
	if err := gate.Freeze(100, "transfer-1", 0); !errors.Is(err, errTransferEpochRequired) {
		t.Fatalf("zero transfer epoch should reject, got %v", err)
	}
}

func TestTransferGateDeactivateIsIdempotentForMatchingTransfer(t *testing.T) {
	gate := newTransferGate()
	if err := gate.Freeze(100, "transfer-1", 7); err != nil {
		t.Fatalf("freeze: %v", err)
	}

	if err := gate.Deactivate(100, "transfer-1", 7); err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if err := gate.Deactivate(100, "transfer-1", 7); err != nil {
		t.Fatalf("duplicate deactivate should be idempotent: %v", err)
	}
	if _, err := gate.BeginUserOperation(100, 7); !errors.Is(err, errCollectionTransferredOut) {
		t.Fatalf("transferred-out collection should reject user operations, got %v", err)
	}
	if err := gate.AllowTransferOperation(100, "transfer-1", 7, transferStateTransferringOut); !errors.Is(err, errTransferStateMismatch) {
		t.Fatalf("transfer operation in wrong state should reject, got %v", err)
	}
}

func TestTransferGateDeactivateIsIdempotentAfterRestoreLost(t *testing.T) {
	gate := newTransferGate()
	if err := gate.Deactivate(100, "transfer-1", 7); err != nil {
		t.Fatalf("deactivate without in-memory gate should be idempotent after restart: %v", err)
	}
}

func TestTransferGateFreezeRejectsWhileUserOperationInFlight(t *testing.T) {
	gate := newTransferGate()
	done, err := gate.BeginUserOperation(100, 0)
	if err != nil {
		t.Fatalf("begin user operation: %v", err)
	}
	defer done()

	if err := gate.Freeze(100, "transfer-1", 7); !errors.Is(err, errCollectionHasInFlightOperations) {
		t.Fatalf("freeze with in-flight operation should reject, got %v", err)
	}
}

func TestTransferGateFreezeWaitsForInFlightDrain(t *testing.T) {
	gate := newTransferGate()
	done, err := gate.BeginUserOperation(100, 0)
	if err != nil {
		t.Fatalf("begin user operation: %v", err)
	}

	result := make(chan error, 1)
	go func() {
		result <- gate.FreezeWithDrain(100, "transfer-1", 7, time.Second)
	}()

	time.Sleep(10 * time.Millisecond)
	done()

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("freeze should succeed after drain: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("freeze did not finish after in-flight operation drained")
	}
}

func TestTransferGateFreezeWithDrainRollsBackFreezeOnTimeout(t *testing.T) {
	gate := newTransferGate()
	done, err := gate.BeginUserOperation(100, 0)
	if err != nil {
		t.Fatalf("begin user operation: %v", err)
	}
	defer done()

	if err := gate.FreezeWithDrain(100, "transfer-1", 7, time.Millisecond); !errors.Is(err, errCollectionHasInFlightOperations) {
		t.Fatalf("freeze timeout should report in-flight operation, got %v", err)
	}

	nextDone, err := gate.BeginUserOperation(100, 0)
	if err != nil {
		t.Fatalf("source should not remain frozen after timeout: %v", err)
	}
	nextDone()
}

func TestTransferGateAbortReleasesSourceFence(t *testing.T) {
	gate := newTransferGate()
	if err := gate.Freeze(100, "transfer-1", 7); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	if err := gate.Abort(100, "transfer-1", 7); err != nil {
		t.Fatalf("abort: %v", err)
	}
	done, err := gate.BeginUserOperation(100, 0)
	if err != nil {
		t.Fatalf("source should be writable after abort: %v", err)
	}
	done()
}

func TestTransferGateRestoreDurableState(t *testing.T) {
	gate := newTransferGate()
	gate.Restore(100, transferGateEntry{
		transferID: "transfer-1",
		epoch:      7,
		state:      transferStateTransferringOut,
	})

	if _, err := gate.BeginUserOperation(100, 7); !errors.Is(err, errCollectionTransferring) {
		t.Fatalf("restored frozen collection should reject user operation, got %v", err)
	}
}
