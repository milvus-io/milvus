package broadcaster

import (
	"context"
	"sort"
	"time"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	pending    chan uint64
	bm         *broadcastTaskManager
	tombstones []tombstoneItem
}

// newTombstoneScheduler creates a new tombstone scheduler.
func newTombstoneScheduler(logger *mlog.Logger) *tombstoneScheduler {
	ts := &tombstoneScheduler{
		notifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		pending:  make(chan uint64),
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

// AddPending adds a pending tombstone to the scheduler.
func (s *tombstoneScheduler) AddPending(broadcastID uint64) {
	select {
	case <-s.notifier.Context().Done():
		// The scheduler is closing while an in-flight ack callback still tries to
		// enqueue a tombstone. This is reachable under concurrent shutdown, so it
		// must not panic. Dropping the in-memory enqueue is safe: the task state is
		// already persisted as TOMBSTONE (MarkAckCallbackDone) before reaching here,
		// and will be recovered into the GC list on the next startup.
		s.Logger().Info(context.TODO(), "tombstone scheduler is closing, skip adding pending tombstone", mlog.FieldBroadcastID(broadcastID))
		return
	case s.pending <- broadcastID:
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

	for {
		select {
		case <-s.notifier.Context().Done():
			return
		case broadcastID := <-s.pending:
			s.tombstones = append(s.tombstones, tombstoneItem{
				broadcastID: broadcastID,
				createTime:  time.Now(),
			})
		case <-ticker.C:
			s.triggerGCTombstone()
		}
	}
}

// triggerGCTombstone triggers the garbage collection of the tombstone.
func (s *tombstoneScheduler) triggerGCTombstone() {
	maxTombstoneLifetime := paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxLifetime.GetAsDurationByParse()
	maxTombstoneCount := paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxCount.GetAsInt()
	ctx := s.notifier.Context()
	expiredTime := time.Now().Add(-maxTombstoneLifetime)
	if len(s.tombstones) > maxTombstoneCount {
		s.Logger().RatedWarn(ctx, 1.0, "tombstone count exceeds soft limit; TTL and WAL checkpoints still gate GC",
			mlog.Int("tombstoneCount", len(s.tombstones)), mlog.Int("maxCount", maxTombstoneCount))
	}
	// Items are ordered by enqueue time. Avoid catalog I/O until one expires.
	if len(s.tombstones) == 0 || s.tombstones[0].createTime.After(expiredTime) {
		return
	}
	checkpoints, err := s.loadCheckpoints(ctx)
	if err != nil {
		s.Logger().Warn(ctx, "failed to load WAL checkpoints for tombstone GC", mlog.Err(err))
		return
	}
	retained := s.tombstones[:0]
	for idx, tombstone := range s.tombstones {
		if tombstone.createTime.After(expiredTime) {
			retained = append(retained, s.tombstones[idx:]...)
			break
		}
		dropped, err := s.bm.DropTombstone(ctx, tombstone.broadcastID, checkpoints)
		if err != nil {
			s.Logger().Warn(ctx, "failed to drop tombstone", mlog.Err(err))
			retained = append(retained, s.tombstones[idx:]...)
			break
		}
		if !dropped {
			// A lagging PChannel must not block unrelated eligible tombstones.
			retained = append(retained, tombstone)
		}
	}
	s.tombstones = retained
}

// loadCheckpoints reads the durable recovery boundary once per PChannel per GC
// pass. Collect the entire map before deleting tasks; never reuse a prior pass.
func (s *tombstoneScheduler) loadCheckpoints(ctx context.Context) (map[string]*streamingpb.WALCheckpoint, error) {
	channels, err := resource.Resource().StreamingCatalog().ListPChannel(ctx)
	if err != nil {
		return nil, merr.Wrap(err, "list PChannels for tombstone GC")
	}
	checkpoints := make(map[string]*streamingpb.WALCheckpoint, len(channels))
	for _, channel := range channels {
		name := channel.GetChannel().GetName()
		checkpoint, err := resource.Resource().StreamingNodeCatalog().GetConsumeCheckpoint(ctx, name)
		if err != nil {
			return nil, merr.Wrapf(err, "read WAL checkpoint for tombstone GC on %s", name)
		}
		checkpoints[name] = checkpoint
	}
	return checkpoints, nil
}
