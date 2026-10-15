package inspector

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// NewTimeTickSyncInspector creates a new time tick sync inspector.
func NewTimeTickSyncInspector() TimeTickSyncInspector {
	inspector := &timeTickSyncInspectorImpl{
		taskNotifier:      syncutil.NewAsyncTaskNotifier[struct{}](),
		syncNotifier:      newSyncNotifier(),
		operators:         typeutil.NewConcurrentMap[string, TimeTickSyncOperator](),
		lastPersistedSync: typeutil.NewConcurrentMap[string, time.Time](),
	}
	go inspector.background()
	return inspector
}

type timeTickSyncInspectorImpl struct {
	taskNotifier *syncutil.AsyncTaskNotifier[struct{}]
	syncNotifier *syncNotifier
	operators    *typeutil.ConcurrentMap[string, TimeTickSyncOperator]
	wg           sync.WaitGroup
	working      typeutil.ConcurrentSet[string]
	barrierMu    sync.Mutex
	barriers     map[types.PChannelInfo]*barrierState
	// lastPersistedSync records the last time a persisted time tick sync was
	// triggered for each pchannel.
	lastPersistedSync *typeutil.ConcurrentMap[string, time.Time]
}

func (s *timeTickSyncInspectorImpl) TriggerSync(pChannelInfo types.PChannelInfo, persisted bool) {
	s.syncNotifier.AddAndNotify(pChannelInfo, persisted)
}

func (s *timeTickSyncInspectorImpl) GetOperator(pChannelInfo types.PChannelInfo) (TimeTickSyncOperator, bool) {
	return s.operators.Get(pChannelInfo.Name)
}

// MustGetOperator gets the operator by pchannel info.
func (s *timeTickSyncInspectorImpl) MustGetOperator(pChannelInfo types.PChannelInfo) TimeTickSyncOperator {
	operator, ok := s.GetOperator(pChannelInfo)
	if !ok {
		panic("sync operator not found, critical bug in code")
	}
	return operator
}

// RegisterSyncOperator registers a sync operator.
func (s *timeTickSyncInspectorImpl) RegisterSyncOperator(operator TimeTickSyncOperator) {
	s.barrierMu.Lock()
	defer s.barrierMu.Unlock()
	mlog.Info(context.TODO(), "RegisterSyncOperator", mlog.String("channel", operator.Channel().Name))
	_, loaded := s.operators.GetOrInsert(operator.Channel().Name, operator)
	if loaded {
		panic("sync operator already exists, critical bug in code")
	}
	if s.barriers == nil {
		s.barriers = make(map[types.PChannelInfo]*barrierState)
	}
	ctx, cancel := context.WithCancel(s.taskNotifier.Context()) //nolint:gosec // Ownership transfers to barrierState; Unregister cancels it, Close cancels the parent.
	s.barriers[operator.Channel()] = &barrierState{ctx: ctx, cancel: cancel, sent: make(map[string]uint64), waiters: make(map[*barrierWaiter]struct{})}
}

// UnregisterSyncOperator unregisters a sync operator.
func (s *timeTickSyncInspectorImpl) UnregisterSyncOperator(operator TimeTickSyncOperator) {
	s.barrierMu.Lock()
	defer s.barrierMu.Unlock()
	mlog.Info(context.TODO(), "UnregisterSyncOperator", mlog.String("channel", operator.Channel().Name))
	_, loaded := s.operators.GetAndRemove(operator.Channel().Name)
	if !loaded {
		panic("sync operator not found, critical bug in code")
	}
	if state := s.barriers[operator.Channel()]; state != nil {
		state.cancel()
		delete(s.barriers, operator.Channel())
	}
}

// background executes the time tick sync inspector.
func (s *timeTickSyncInspectorImpl) background() {
	defer s.taskNotifier.Finish(struct{}{})

	interval := paramtable.Get().ProxyCfg.TimeTickInterval.GetAsDuration(time.Millisecond)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.taskNotifier.Context().Done():
			return
		case <-ticker.C:
			// Sync a non-persisted heartbeat for every pchannel on each tick;
			// on top of that, emit a persisted time tick at least every
			// dataNode.segment.syncPeriod so an idle pchannel still refreshes
			// the persisted recovery checkpoint on a bounded cadence (the
			// non-persisted heartbeats never advance durable recovery state).
			// The persisted sync must be requested before the heartbeat: the
			// working set allows only one in-flight sync per pchannel, and the
			// heartbeat goroutine would otherwise win the slot on every tick.
			// A requested persisted sync is itself a full sync carrying the
			// freshest ack info, so the heartbeat of the same tick is skipped.
			now := time.Now()
			s.operators.Range(func(name string, _ TimeTickSyncOperator) bool {
				if !s.maybeForcePersistedSync(name, now) {
					s.asyncSync(name, false)
				}
				return true
			})
		case <-s.syncNotifier.WaitChan():
			signals := s.syncNotifier.Get()
			for pchannel, persisted := range signals {
				s.asyncSync(pchannel.Name, persisted)
			}
		}
	}
}

// maybeForcePersistedSync emits a persisted time tick sync for a pchannel if
// it has not had one within dataNode.segment.syncPeriod, and reports whether a
// sync was actually launched. The recorded time is the trigger time; a sync
// skipped by the working set is retried on the next tick after the interval
// elapses again. The interval is read from the (refreshable) config on every
// call so a config change takes effect without a restart.
func (s *timeTickSyncInspectorImpl) maybeForcePersistedSync(name string, now time.Time) bool {
	interval := paramtable.Get().DataNodeCfg.SyncPeriod.GetAsDuration(time.Second)
	if last, ok := s.lastPersistedSync.Get(name); ok && now.Sub(last) < interval {
		return false
	}
	s.lastPersistedSync.Insert(name, now)
	return s.asyncSync(name, true)
}

// asyncSync syncs the pchannel in a goroutine. It reports whether the sync was
// actually launched: the working set allows only one in-flight sync per
// pchannel, and a second request within the same window is skipped.
func (s *timeTickSyncInspectorImpl) asyncSync(pchannelName string, persisted bool) bool {
	if !s.working.Insert(pchannelName) {
		// Check if the sync operation of pchannel is working, if so, skip it.
		return false
	}
	s.barrierMu.Lock()
	operator, ok := s.operators.Get(pchannelName)
	if !ok {
		s.barrierMu.Unlock()
		s.working.Remove(pchannelName)
		return false
	}
	state := s.barriers[operator.Channel()]
	targets, revision := state.targets()
	s.barrierMu.Unlock()

	s.wg.Add(1)
	go func() {
		defer func() {
			s.working.Remove(pchannelName)
			// A request arriving during Sync must survive the busy-slot skip.
			s.barrierMu.Lock()
			if state != nil && state.ctx.Err() == nil && state.revision != revision && len(state.waiters) != 0 {
				s.syncNotifier.AddAndNotify(operator.Channel(), false)
			}
			s.barrierMu.Unlock()
			s.wg.Done()
		}()
		if len(targets) == 0 {
			operator.Sync(s.taskNotifier.Context(), persisted)
			return
		}
		tick, err := operator.SyncWithQueryBarriers(state.ctx, persisted, targets)
		s.completeBarriers(state, tick, err)
	}()
	return true
}

func (s *timeTickSyncInspectorImpl) Close() {
	s.taskNotifier.Cancel()
	s.taskNotifier.BlockUntilFinish()
	s.wg.Wait()
}
