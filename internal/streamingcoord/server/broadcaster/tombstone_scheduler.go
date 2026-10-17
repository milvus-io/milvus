package broadcaster

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// tombstoneItem is a tombstone item with expired time.
type tombstoneItem struct {
	broadcastID uint64
	// createTime is when the tombstone was created. Recovery resets it to the current
	// time, so a restart delays this tombstone's GC by up to another maxLifetime. That
	// makes the idempotency window the tombstone backs a lower bound rather than an
	// exact one, so any retention coupled to maxLifetime must leave margin rather than
	// match it exactly.
	createTime time.Time
}

// tombstoneScheduler is a scheduler for the tombstone.
type tombstoneScheduler struct {
	mlog.Binder

	notifier   *syncutil.AsyncTaskNotifier[struct{}]
	wakeup     chan struct{}
	pendingMu  sync.Mutex
	pending    []tombstoneItem // protected by pendingMu; never hold it during catalog I/O
	bm         *broadcastTaskManager
	tombstones []tombstoneItem
}

// newTombstoneScheduler creates a new tombstone scheduler.
func newTombstoneScheduler(logger *mlog.Logger) *tombstoneScheduler {
	ts := &tombstoneScheduler{
		notifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		wakeup:   make(chan struct{}, 1),
	}
	ts.SetLogger(logger)
	return ts
}

// Initialize initializes the tombstone scheduler.
func (s *tombstoneScheduler) Initialize(bm *broadcastTaskManager, tombstoneBroadcastIDs []uint64) {
	sort.Slice(tombstoneBroadcastIDs, func(i, j int) bool {
		return tombstoneBroadcastIDs[i] < tombstoneBroadcastIDs[j]
	})
	s.bm = bm
	s.tombstones = make([]tombstoneItem, 0, len(tombstoneBroadcastIDs))
	for _, broadcastID := range tombstoneBroadcastIDs {
		s.tombstones = append(s.tombstones, tombstoneItem{
			broadcastID: broadcastID,
			createTime:  time.Now(),
		})
	}
	go s.background()
}

// AddPending records a durable tombstone without waiting for catalog I/O.
func (s *tombstoneScheduler) AddPending(broadcastID uint64) {
	s.pendingMu.Lock()
	if s.notifier.Context().Err() != nil {
		s.pendingMu.Unlock()
		// MarkAckCallbackDone already persisted TOMBSTONE. Recovery will enqueue
		// it again if shutdown races with this handoff.
		s.Logger().Info(s.notifier.Context(), "tombstone scheduler is closing, skip adding pending tombstone", mlog.Uint64("broadcastID", broadcastID))
		return
	}
	s.pending = append(s.pending, tombstoneItem{broadcastID: broadcastID, createTime: time.Now()})
	s.pendingMu.Unlock()

	// Only notifications are coalesced; every ID remains in pending until drained.
	select {
	case s.wakeup <- struct{}{}:
	default:
	}
}

// Close closes the tombstone scheduler.
func (s *tombstoneScheduler) Close() {
	s.notifier.Cancel()
	s.notifier.BlockUntilFinish()
}

// background is the background goroutine of the tombstone scheduler.
func (s *tombstoneScheduler) background() {
	defer func() {
		s.notifier.Finish(struct{}{})
		s.Logger().Info(context.TODO(), "tombstone scheduler background exit")
	}()
	s.Logger().Info(context.TODO(), "tombstone scheduler background start")

	tombstoneGCInterval := paramtable.Get().StreamingCfg.WALBroadcasterTombstoneCheckInternal.GetAsDurationByParse()
	ticker := time.NewTicker(tombstoneGCInterval)
	defer ticker.Stop()

	for s.notifier.Context().Err() == nil {
		s.pendingMu.Lock()
		pending := s.pending
		s.pending = nil
		s.pendingMu.Unlock()
		s.tombstones = append(s.tombstones, pending...)
		s.triggerGCTombstone()
		select {
		case <-s.notifier.Context().Done():
			return
		case <-s.wakeup:
		case <-ticker.C:
		}
	}
}

// triggerGCTombstone triggers the garbage collection of the tombstone.
func (s *tombstoneScheduler) triggerGCTombstone() {
	ctx := s.notifier.Context()
	maxTombstoneLifetime := paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxLifetime.GetAsDurationByParse()
	maxTombstoneCount := paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxCount.GetAsInt()
	batchSize := max(1, paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt())

	expiredTime := time.Now().Add(-maxTombstoneLifetime)
	expiredOffset := 0
	if len(s.tombstones) > maxTombstoneCount {
		expiredOffset = len(s.tombstones) - maxTombstoneCount
	}
	s.Logger().Info(ctx,
		"triggerGCTombstone",
		mlog.Int("tombstone count", len(s.tombstones)),
		mlog.Int("expired offset", expiredOffset),
		mlog.Time("expired time", expiredTime))
	ids := make([]uint64, 0, min(batchSize, len(s.tombstones)))
	for len(s.tombstones) > 0 && ctx.Err() == nil {
		ids = ids[:0]
		for idx, tombstone := range s.tombstones[:min(batchSize, len(s.tombstones))] {
			if idx >= expiredOffset && tombstone.createTime.After(expiredTime) {
				break
			}
			ids = append(ids, tombstone.broadcastID)
		}
		if len(ids) == 0 {
			return
		}
		if err := s.bm.DropTombstones(ctx, ids); err != nil {
			s.Logger().Warn(ctx, "failed to drop tombstone batch", mlog.Int("batchSize", len(ids)), mlog.Err(err))
			return
		}
		// Advance only after the whole batch succeeds. A failed batch remains
		// queued for idempotent retry, while earlier successful batches stay gone.
		clear(s.tombstones[:len(ids)])
		s.tombstones = s.tombstones[len(ids):]
		expiredOffset -= len(ids)
	}
	if len(s.tombstones) == 0 {
		s.tombstones = nil
	}
}
