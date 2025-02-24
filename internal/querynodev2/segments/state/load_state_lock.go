package state

import (
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type loadStateEnum int

func noop() {}

// LoadState represent the state transition of segment.
// LoadStateOnlyMeta: segment is created with meta, but not loaded.
// LoadStateDataLoading: segment is loading data.
// LoadStateDataLoaded: segment is full loaded, ready to be searched or queried.
// LoadStateDataReleasing: segment is releasing data.
// LoadStateReleased: segment is released.
// LoadStateOnlyMeta -> LoadStateDataLoading -> LoadStateDataLoaded -> LoadStateDataReleasing -> (LoadStateReleased or LoadStateOnlyMeta)
const (
	LoadStateOnlyMeta    loadStateEnum = iota
	LoadStateDataLoading               // There will be only one goroutine access segment when loading.
	LoadStateDataLoaded
	LoadStateDataReleasing // There will be only one goroutine access segment when releasing.
	LoadStateReleased
)

// LoadState is the state of segment loading.
func (ls loadStateEnum) String() string {
	switch ls {
	case LoadStateOnlyMeta:
		return "meta"
	case LoadStateDataLoading:
		return "loading-data"
	case LoadStateDataLoaded:
		return "loaded"
	case LoadStateDataReleasing:
		return "releasing-data"
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

	mu := &sync.RWMutex{}
	return &LoadStateLock{
		mu:     mu,
		cv:     sync.Cond{L: mu},
		state:  state,
		refCnt: atomic.NewInt32(0),
	}
}

// LoadStateLock is the state of segment loading.
type LoadStateLock struct {
	mu     *sync.RWMutex
	cv     sync.Cond
	state  loadStateEnum
	refCnt *atomic.Int32
	// ReleaseAll can be called only when refCnt is 0.
	// We need it to be modified when lock is
}

// RLockIfNotReleased locks the segment if the state is not released.
func (ls *LoadStateLock) PinIf(pred StatePredicate) bool {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	if !pred(ls.state) {
		return false
	}
	ls.refCnt.Inc()
	return true
}

// Unpin unlocks the segment.
func (ls *LoadStateLock) Unpin() {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	newCnt := ls.refCnt.Dec()
	if newCnt < 0 {
		panic("unpin more than pin")
	}
	if newCnt == 0 {
		// notify ReleaseAll to release segment if refcnt is zero.
		ls.cv.Broadcast()
	}
}

// PinIfNotReleased pin the segment if the state is not released.
// grammar suger for PinIf(IsNotReleased).
func (ls *LoadStateLock) PinIfNotReleased() bool {
	return ls.PinIf(IsNotReleased)
}

// StartLoadData starts load segment data
// Fast fail if segment is not in LoadStateOnlyMeta.
func (ls *LoadStateLock) StartLoadData() (LoadStateLockGuard, error) {
	// only meta can be loaded.
	ls.cv.L.Lock()
	defer ls.cv.L.Unlock()

	if ls.state == LoadStateDataLoaded {
		return nil, nil
	}
	if ls.state != LoadStateOnlyMeta {
		return nil, errors.New("segment is not in LoadStateOnlyMeta, cannot start to loading data")
	}
	ls.state = LoadStateDataLoading
	ls.cv.Broadcast()

	return newLoadStateLockGuard(ls, LoadStateOnlyMeta, LoadStateDataLoaded), nil
}

// StartReleaseData wait until the segment is releasable and starts releasing segment data.
func (ls *LoadStateLock) StartReleaseData() (g LoadStateLockGuard) {
	ls.waitOrPanic(ls.canReleaseData, func() {
		switch ls.state {
		case LoadStateDataLoaded:
			ls.state = LoadStateDataReleasing
			ls.cv.Broadcast()
			g = newLoadStateLockGuard(ls, LoadStateDataLoaded, LoadStateOnlyMeta)
		case LoadStateOnlyMeta:
			// already transit to target state, do nothing.
			g = nil
		case LoadStateReleased:
			// do nothing for empty segment.
			g = nil
		default:
			panic(fmt.Sprintf("unreachable code: invalid state when releasing data, %s", ls.state.String()))
		}
	})
	return g
}

// StartReleaseAll wait until the segment is releasable and starts releasing all segment.
func (ls *LoadStateLock) StartReleaseAll() (g LoadStateLockGuard) {
	ls.waitOrPanic(ls.canReleaseAll, func() {
		switch ls.state {
		case LoadStateDataLoaded:
			ls.state = LoadStateReleased
			ls.cv.Broadcast()
			g = newNopLoadStateLockGuard()
		case LoadStateOnlyMeta:
			ls.state = LoadStateReleased
			ls.cv.Broadcast()
			g = newNopLoadStateLockGuard()
		case LoadStateReleased:
			// already transit to target state, do nothing.
			g = nil
		default:
			panic(fmt.Sprintf("unreachable code: invalid state when releasing data, %s", ls.state.String()))
		}
	})

	return g
}

// blockUntilDataLoadedOrReleased blocks until the segment is loaded or released.
func (ls *LoadStateLock) BlockUntilDataLoadedOrReleased() bool {
	var ok bool
	ls.waitOrPanic(func(state loadStateEnum) bool {
		return state == LoadStateDataLoaded || state == LoadStateReleased
	}, func() { ok = true })
	return ok
}

// waitUntilCanReleaseData waits until segment is release data able.
func (ls *LoadStateLock) canReleaseData(state loadStateEnum) bool {
	return state == LoadStateDataLoaded || state == LoadStateOnlyMeta || state == LoadStateReleased
}

// waitUntilCanReleaseAll waits until segment is releasable.
func (ls *LoadStateLock) canReleaseAll(state loadStateEnum) bool {
	return (state == LoadStateDataLoaded || state == LoadStateOnlyMeta || state == LoadStateReleased) && ls.refCnt.Load() == 0
}

func (ls *LoadStateLock) waitOrPanic(ready func(state loadStateEnum) bool, then func()) {
	ch := make(chan struct{})
	maxWaitTime := paramtable.Get().CommonCfg.MaxWLockConditionalWaitTime.GetAsDuration(time.Second)
	go func() {
		ls.cv.L.Lock()
		defer ls.cv.L.Unlock()
		defer close(ch)
		for !ready(ls.state) {
			ls.cv.Wait()
		}
		then()
	}()

	select {
	case <-time.After(maxWaitTime):
		log.Error("load state lock wait timeout", zap.Duration("maxWaitTime", maxWaitTime))
	case <-ch:
	}
}

type StatePredicate func(state loadStateEnum) bool

// IsNotReleased checks if the segment is not released.
func IsNotReleased(state loadStateEnum) bool {
	return state != LoadStateReleased
}

// IsDataLoaded checks if the segment is loaded.
func IsDataLoaded(state loadStateEnum) bool {
	return state == LoadStateDataLoaded
}
