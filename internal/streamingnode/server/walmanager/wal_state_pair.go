package walmanager

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
)

// newWALStatePair create a new walStatePair
func newWALStatePair() *walStatePair {
	return &walStatePair{
		currentState:  newWALStateWithCond(initialCurrentWALState),  // current state of wal, should always be same or greater (e.g. open wal failure) than expected state finally.
		expectedState: newWALStateWithCond(initialExpectedWALState), // finial state expected of wal.
	}
}

// walStatePair is a wal with its state pair.
// a state pair is consist of current state and expected state.
type walStatePair struct {
	currentState  walStateWithCond[currentWALState]
	expectedState walStateWithCond[expectedWALState]
}

// WaitCurrentStateReachExpected waits until the current state is reach the expected state.
func (w *walStatePair) WaitCurrentStateReachExpected(ctx context.Context, expected expectedWALState) error {
	current := w.currentState.GetState()
	for isStateBefore(current, expected) {
		if err := w.currentState.WatchChanged(ctx, current); err != nil {
			// context canceled.
			return err
		}
		current = w.currentState.GetState()
	}
	// Request term is a expired term, return term error.
	if current.Term() > expected.Term() {
		return status.NewUnmatchedChannelTerm("request term is expired, expected: %d, actual: %d", expected.Term(), current.Term())
	}
	// Check if the wal is as expected.
	return current.GetLastError()
}

// GetExpectedState returns the expected state of the wal.
func (w *walStatePair) GetExpectedState() expectedWALState {
	return w.expectedState.GetState()
}

// GetCurrentState returns the current state of the wal.
func (w *walStatePair) GetCurrentState() currentWALState {
	return w.currentState.GetState()
}

// WaitExpectedStateChanged waits until the expected state is changed.
func (w *walStatePair) WaitExpectedStateChanged(ctx context.Context, oldExpected walState) error {
	return w.expectedState.WatchChanged(ctx, oldExpected)
}

// SetExpectedState sets the expected state of the wal.
func (w *walStatePair) SetExpectedState(s expectedWALState) bool {
	return w.expectedState.SetStateAndNotify(s)
}

// SetCurrentState sets the current state of the wal.
func (w *walStatePair) SetCurrentState(s currentWALState) bool {
	return w.currentState.SetStateAndNotify(s)
}

// GetWAL returns the current wal.
func (w *walStatePair) GetWAL() wal.WAL {
	return w.currentState.GetState().GetWAL()
}
