package walmanager

import (
	"context"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

var (
	_ currentWALState  = (*availableCurrentWALState)(nil)
	_ currentWALState  = (*unavailableCurrentWALState)(nil)
	_ expectedWALState = (*availableExpectedWALState)(nil)
	_ expectedWALState = (*unavailableExpectedWALState)(nil)

	initialExpectedWALState expectedWALState = &unavailableExpectedWALState{
		term: types.InitialTerm,
	}
	initialCurrentWALState currentWALState = &unavailableCurrentWALState{
		term: types.InitialTerm,
		err:  nil,
	}
)

// newAvailableCurrentState creates a new available current state.
func newAvailableCurrentState(l wal.WAL) currentWALState {
	return availableCurrentWALState{
		l: l,
	}
}

// newUnavailableCurrentState creates a new unavailable current state.
func newUnavailableCurrentState(term int64, err error) currentWALState {
	return unavailableCurrentWALState{
		term: term,
		err:  err,
	}
}

// newAvailableExpectedState creates a new available expected state.
func newAvailableExpectedState(ctx context.Context, channel types.PChannelInfo) expectedWALState {
	return availableExpectedWALState{
		ctx:     ctx,
		channel: channel,
	}
}

// newUnavailableExpectedState creates a new unavailable expected state.
func newUnavailableExpectedState(term int64) expectedWALState {
	return unavailableExpectedWALState{
		term: term,
	}
}

// walState describe the state of a wal.
type walState interface {
	// Term returns the term of the wal.
	Term() int64

	// Available returns whether the wal is available.
	Available() bool
}

// currentWALState is the current (exactly status) state of a wal.
type currentWALState interface {
	walState

	// GetWAL returns the current wal.
	// Return empty if the wal is not available now.
	GetWAL() wal.WAL

	// GetLastError returns the last error of wal management.
	GetLastError() error
}

// expectedWALState is the expected state (which is sent from log coord) of a wal.
type expectedWALState interface {
	walState

	// GetPChannelInfo returns the expected pchannel info of the wal.
	// Return nil if the expected wal state is unavailable.
	GetPChannelInfo() types.PChannelInfo

	// Context returns the context of the expected wal state.
	Context() context.Context
}

// availableCurrentWALState is a available wal state of current wal.
type availableCurrentWALState struct {
	l wal.WAL
}

func (s availableCurrentWALState) Term() int64 {
	return s.l.Channel().Term
}

func (s availableCurrentWALState) Available() bool {
	return true
}

func (s availableCurrentWALState) GetWAL() wal.WAL {
	return s.l
}

func (s availableCurrentWALState) GetLastError() error {
	return nil
}

// unavailableCurrentWALState is a unavailable state of current wal.
type unavailableCurrentWALState struct {
	term int64
	err  error
}

func (s unavailableCurrentWALState) Term() int64 {
	return s.term
}

func (s unavailableCurrentWALState) Available() bool {
	return false
}

func (s unavailableCurrentWALState) GetWAL() wal.WAL {
	return nil
}

func (s unavailableCurrentWALState) GetLastError() error {
	return s.err
}

type availableExpectedWALState struct {
	ctx     context.Context
	channel types.PChannelInfo
}

func (s availableExpectedWALState) Term() int64 {
	return s.channel.Term
}

func (s availableExpectedWALState) Available() bool {
	return true
}

func (s availableExpectedWALState) Context() context.Context {
	return s.ctx
}

func (s availableExpectedWALState) GetPChannelInfo() types.PChannelInfo {
	return s.channel
}

type unavailableExpectedWALState struct {
	term int64
}

func (s unavailableExpectedWALState) Term() int64 {
	return s.term
}

func (s unavailableExpectedWALState) Available() bool {
	return false
}

func (s unavailableExpectedWALState) GetPChannelInfo() types.PChannelInfo {
	return types.PChannelInfo{}
}

func (s unavailableExpectedWALState) Context() context.Context {
	return context.Background()
}

// newWALStateWithCond creates new walStateWithCond.
func newWALStateWithCond[T walState](state T) walStateWithCond[T] {
	return walStateWithCond[T]{
		state: state,
		cond:  syncutil.NewContextCond(&sync.Mutex{}),
	}
}

// walStateWithCond is the walState with cv.
type walStateWithCond[T walState] struct {
	state T
	cond  *syncutil.ContextCond
}

// GetState returns the state of the wal.
func (w *walStateWithCond[T]) GetState() T {
	w.cond.L.Lock()
	defer w.cond.L.Unlock()

	// Copy the state, all state should be value type but not pointer type.
	return w.state
}

// SetStateAndNotify sets the state of the wal.
// Return false if the state is not changed.
func (w *walStateWithCond[T]) SetStateAndNotify(s T) bool {
	w.cond.LockAndBroadcast()
	defer w.cond.L.Unlock()
	if isStateBefore(w.state, s) {
		// Only update state when current state is before new state.
		w.state = s
		return true
	}
	return false
}

// WatchChanged waits until the state is changed.
func (w *walStateWithCond[T]) WatchChanged(ctx context.Context, s walState) error {
	w.cond.L.Lock()
	for w.state.Term() == s.Term() && w.state.Available() == s.Available() {
		if err := w.cond.Wait(ctx); err != nil {
			return err
		}
	}
	w.cond.L.Unlock()
	return nil
}

// isStateBefore returns whether s1 is before s2.
func isStateBefore(s1, s2 walState) bool {
	// w1 is before w2 if term of w1 is less than w2.
	// or w1 is available and w2 is not available in same term.
	// because wal should always be available before unavailable in same term.
	// (1, true) -> (1, false) is allowed.
	// (1, true) -> (2, false) is allowed.
	// (1, false) -> (2, true) is allowed.
	// (1, false) -> (1, true) is not allowed.
	return s1.Term() < s2.Term() || (s1.Term() == s2.Term() && s1.Available() && !s2.Available())
}

// toStateString returns the string representation of wal state.
func toStateString(s walState) string {
	return fmt.Sprintf("(%d,%t)", s.Term(), s.Available())
}
