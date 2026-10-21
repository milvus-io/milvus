package broadcaster

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// tombstoneItem is a tombstone item with expired time.
type tombstoneItem struct {
	broadcastID uint64
	// createTime is when the scheduler received the tombstone. Recovery resets
	// this clock. Count-based eviction can remove checkpoint-eligible tombstones
	// before maxLifetime expires.
	createTime time.Time
}

// tombstoneScheduler is a scheduler for the tombstone.
type tombstoneScheduler struct {
	mlog.Binder

	notifier   *syncutil.AsyncTaskNotifier[struct{}]
	bm         *broadcastTaskManager
	tombstones []tombstoneItem

	// Only enqueue and queue exchange hold pendingMu. GC owns the detached queue
	// and tombstones, so catalog I/O never blocks ACK callbacks from enqueueing.
	pendingMu sync.Mutex
	pending   *typeutil.MultipartQueue[tombstoneItem]
}

// newTombstoneScheduler creates a new tombstone scheduler.
func newTombstoneScheduler(logger *mlog.Logger) *tombstoneScheduler {
	ts := &tombstoneScheduler{
		notifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		pending:  typeutil.NewMultipartQueue[tombstoneItem](),
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
	s.pendingMu.Lock()
	if s.notifier.Context().Err() != nil {
		s.pendingMu.Unlock()
		// The scheduler is closing while an in-flight ack callback still tries to
		// enqueue a tombstone. This is reachable under concurrent shutdown, so it
		// must not panic. Dropping the in-memory enqueue is safe: the task state is
		// already persisted as TOMBSTONE (MarkAckCallbackDone) before reaching here,
		// and will be recovered into the GC list on the next startup.
		s.Logger().Info(context.TODO(), "tombstone scheduler is closing, skip adding pending tombstone", mlog.FieldBroadcastID(broadcastID))
		return
	}
	// Stamp under the enqueue lock to preserve age order across concurrent ACKs.
	s.pending.AddOne(tombstoneItem{broadcastID: broadcastID, createTime: time.Now()})
	s.pendingMu.Unlock()
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
		case <-ticker.C:
			s.triggerGCTombstone()
		}
	}
}

// triggerGCTombstone triggers the garbage collection of the tombstone.
func (s *tombstoneScheduler) triggerGCTombstone() {
	s.pendingMu.Lock()
	pending := s.pending
	s.pending = typeutil.NewMultipartQueue[tombstoneItem]()
	s.pendingMu.Unlock()
	for pending.Len() > 0 {
		s.tombstones = append(s.tombstones, pending.Next())
		pending.UnsafeAdvance()
	}

	maxTombstoneLifetime := paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxLifetime.GetAsDurationByParse()
	maxTombstoneCount := paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxCount.GetAsInt()
	ctx := s.notifier.Context()
	expiredTime := time.Now().Add(-maxTombstoneLifetime)
	// Items are ordered by enqueue time. Avoid catalog I/O when neither age nor
	// count can trigger GC, even if every tombstone satisfies the checkpoint gate.
	if len(s.tombstones) == 0 || (len(s.tombstones) <= maxTombstoneCount && s.tombstones[0].createTime.After(expiredTime)) {
		return
	}
	checkpoints, err := s.loadCheckpoints(ctx)
	if err != nil {
		s.Logger().Warn(ctx, "failed to load WAL checkpoints for tombstone GC", mlog.Err(err))
		return
	}
	toDrop := make(map[uint64]struct{})
	var eligible []uint64
	for _, tombstone := range s.tombstones {
		task, ok := s.bm.getBroadcastTaskByID(tombstone.broadcastID)
		if !ok {
			toDrop[tombstone.broadcastID] = struct{}{}
			continue
		}
		if !task.canDropTombstone(checkpoints) {
			continue
		}
		if !tombstone.createTime.After(expiredTime) {
			toDrop[tombstone.broadcastID] = struct{}{}
		} else {
			eligible = append(eligible, tombstone.broadcastID)
		}
	}
	// Expired tombstones already leave the quota. Among the remaining eligible
	// tombstones, keep the largest BroadcastIDs, regardless of enqueue order.
	sort.Slice(eligible, func(i, j int) bool { return eligible[i] < eligible[j] })
	for i := 0; i < len(eligible)-max(0, maxTombstoneCount); i++ {
		toDrop[eligible[i]] = struct{}{}
	}

	retained := s.tombstones[:0]
	for idx, tombstone := range s.tombstones {
		if _, ok := toDrop[tombstone.broadcastID]; !ok {
			retained = append(retained, tombstone)
			continue
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
