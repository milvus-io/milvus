package cgo

import (
	"sync"
)

const (
	stateUnready state = iota
	stateReady
	stateConsumed
	stateDestoryed
)

// newFutureState creates a new futureState.
func newFutureState() futureState {
	return futureState{
		mu:    sync.Mutex{},
		inner: stateUnready,
	}
}

type state int32

// futureState is a state machine for future.
// unready --BlockUntilReady--> ready --BlockAndLeakyGet--> consumed
type futureState struct {
	mu    sync.Mutex
	inner state
}

// LockForCancel locks the state for cancel.
func (s *futureState) LockForCancel() *lockGuard {
	s.mu.Lock()
	// only unready future can be canceled.
	// cancel on a ready future make no sense.
	if s.inner != stateUnready {
		s.mu.Unlock()
		return nil
	}
	return &lockGuard{
		locker: s,
		target: stateUnready,
	}
}

// LockForConsume locks the state for consume.
func (s *futureState) LockForConsume() *lockGuard {
	s.mu.Lock()
	if s.inner != stateReady {
		s.mu.Unlock()
		return nil
	}
	return &lockGuard{
		locker: s,
		target: stateConsumed,
	}
}

// LockForRelease locks the state for release.
func (s *futureState) LockForRelease() *lockGuard {
	s.mu.Lock()
	if s.inner != stateReady && s.inner != stateConsumed {
		s.mu.Unlock()
		return nil
	}
	return &lockGuard{
		locker: s,
		target: stateDestoryed,
	}
}

// checkUnready checks if the state is unready.
func (s *futureState) CheckUnready() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.inner == stateUnready
}

// IntoReady changes the state to ready.
func (s *futureState) IntoReady() {
	s.mu.Lock()
	if s.inner == stateUnready {
		s.inner = stateReady
	}
	s.mu.Unlock()
}

// lockGuard is a guard for futureState.
type lockGuard struct {
	locker *futureState
	target state
}

// Unlock unlocks the state.
func (lg *lockGuard) Unlock() {
	lg.locker.inner = lg.target
	lg.locker.mu.Unlock()
}
