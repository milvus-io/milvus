package state

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
)

type loadStateEnum int

// LoadState represent the state transition of segment.
// LoadStateOnlyMeta: segment is created with meta, but not loaded.
// LoadStateDataLoading: segment is loading data.
// LoadStateDataLoaded: segment is full loaded, ready to be searched or queried.
// LoadStateReleasing: segment is releasing resources.
// LoadStateReleased: segment is released.
// LoadStateOnlyMeta -> LoadStateDataLoading -> LoadStateDataLoaded -> LoadStateReleasing -> (LoadStateReleased or LoadStateOnlyMeta)
const (
	LoadStateOnlyMeta    loadStateEnum = iota
	LoadStateDataLoading               // There will be only one goroutine access segment when loading.
	LoadStateDataLoaded
	LoadStateReleasing // There will be only one goroutine access segment when releasing.
	LoadStateReleased
)

// LoadState is the state of segment loading.
func (ls loadStateEnum) String() string {
	switch ls {
	case LoadStateOnlyMeta:
		return "meta"
	case LoadStateDataLoading:
		return "loading"
	case LoadStateDataLoaded:
		return "loaded"
	case LoadStateReleasing:
		return "releasing"
	case LoadStateReleased:
		return "released"
	default:
		return "unknown"
	}
}

// NewLoadStateLock creates a LoadState.
func NewLoadStateLock(state loadStateEnum) *LoadStateLock {
	if state != LoadStateOnlyMeta && state != LoadStateDataLoaded {
		panic(fmt.Sprintf("invalid state for construction of LoadStateLock, %s", state.String()))
	}

	l := &sync.RWMutex{}
	return &LoadStateLock{
		RWMutex: l,
		cv:      sync.Cond{L: l},
		state:   state,
	}
}

// LoadStateLock is the state of segment loading.
type LoadStateLock struct {
	*sync.RWMutex
	cv    sync.Cond
	state loadStateEnum
}

// RLockIfNotReleased locks the segment if the state is not released.
func (ls *LoadStateLock) RLockIfNotReleased() bool {
	ls.RLock()
	if ls.state == LoadStateReleased {
		ls.RUnlock()
		return false
	}
	return true
}

// StartLoadData starts load segment data
// Fast fail if segment is not in LoadStateOnlyMeta.
func (ls *LoadStateLock) StartLoadData() (*LoadStateLockGuard, error) {
	// only meta can be loaded.
	ls.cv.L.Lock()
	defer ls.cv.L.Unlock()

	if ls.state == LoadStateDataLoaded {
		return nil, nil
	}
	if ls.state != LoadStateOnlyMeta {
		return nil, errors.New("segment is not in LoadStateOnlyMeta, cannot start to loading data")
	}
	ls.cv.Broadcast()
	ls.state = LoadStateDataLoading

	return newLoadStateLockGuard(ls, LoadStateOnlyMeta, LoadStateDataLoaded), nil
}

// StartReleaseData wait until the segment is releasable and starts releasing segment data.
func (ls *LoadStateLock) StartReleaseData() (g *LoadStateLockGuard) {
	ls.cv.L.Lock()
	defer ls.cv.L.Unlock()

	ls.waitUntilReleasable()

	switch ls.state {
	case LoadStateDataLoaded:
		ls.state = LoadStateReleasing
		ls.cv.Broadcast()
		return newLoadStateLockGuard(ls, LoadStateDataLoaded, LoadStateOnlyMeta)
	case LoadStateOnlyMeta:
		// already transit to target state, do nothing.
		return nil
	case LoadStateReleased:
		// do nothing for empty segment.
		return nil
	default:
		panic(fmt.Sprintf("unreachable code: invalid state when releasing data, %s", ls.state.String()))
	}
}

// StartReleaseAll wait until the segment is releasable and starts releasing all segment.
func (ls *LoadStateLock) StartReleaseAll() (g *LoadStateLockGuard) {
	ls.cv.L.Lock()
	defer ls.cv.L.Unlock()

	ls.waitUntilReleasable()

	switch ls.state {
	case LoadStateDataLoaded:
		ls.state = LoadStateReleasing
		ls.cv.Broadcast()
		return newLoadStateLockGuard(ls, LoadStateDataLoaded, LoadStateReleased)
	case LoadStateOnlyMeta:
		ls.state = LoadStateReleasing
		ls.cv.Broadcast()
		return newLoadStateLockGuard(ls, LoadStateOnlyMeta, LoadStateReleased)
	case LoadStateReleased:
		// already transit to target state, do nothing.
		return nil
	default:
		panic(fmt.Sprintf("unreachable code: invalid state when releasing data, %s", ls.state.String()))
	}
}

// waitUntilReleasable waits until segment is releasable.
func (ls *LoadStateLock) waitUntilReleasable() {
	state := ls.state
	for state != LoadStateDataLoaded && state != LoadStateOnlyMeta && state != LoadStateReleased {
		ls.cv.Wait()
		state = ls.state
	}
}
