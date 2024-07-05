package cgo

import "go.uber.org/atomic"

const (
	stateUnready int32 = iota
	stateReady
	stateConsumed
)

// newFutureState creates a new futureState.
func newFutureState() futureState {
	return futureState{
		inner: atomic.NewInt32(stateUnready),
	}
}

// futureState is a state machine for future.
// unready --BlockUntilReady--> ready --BlockAndLeakyGet--> consumed
type futureState struct {
	inner *atomic.Int32
}

// intoReady sets the state to ready.
func (s *futureState) intoReady() {
	s.inner.CompareAndSwap(stateUnready, stateReady)
}

// intoConsumed sets the state to consumed.
// if the state is not ready, it does nothing and returns false.
func (s *futureState) intoConsumed() bool {
	return s.inner.CompareAndSwap(stateReady, stateConsumed)
}

// checkUnready checks if the state is unready.
func (s *futureState) checkUnready() bool {
	return s.inner.Load() == stateUnready
}
