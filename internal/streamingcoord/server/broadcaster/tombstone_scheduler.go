package broadcaster

import (
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// tombstoneItem is a tombstone item with expired time.
type tombstoneItem struct {
	broadcastID uint64
	createTime  time.Time // the time when the tombstone is created, when recovery, the createTime will be reset to the current time, but it's ok.
}

// tombstoneScheduler is a scheduler for the tombstone.
type tombstoneScheduler struct {
	log.Binder

	notifier   *syncutil.AsyncTaskNotifier[struct{}]
	pending    chan uint64
	bm         *broadcastTaskManager
	tombstones []tombstoneItem
}

// newTombstoneScheduler creates a new tombstone scheduler.
func newTombstoneScheduler(logger *log.MLogger) *tombstoneScheduler {
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
		panic("unreachable: tombstone scheduler is closing when adding pending tombstone")
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
		s.Logger().Info("tombstone scheduler background exit")
	}()
	s.Logger().Info("tombstone scheduler background start")

	tombstoneGCInterval := paramtable.Get().StreamingCfg.WALBroadcasterTombstoneCheckInternal.GetAsDurationByParse()
	ticker := time.NewTicker(tombstoneGCInterval)
	defer ticker.Stop()

	for {
		s.triggerGCTombstone()
		select {
		case <-s.notifier.Context().Done():
			return
		case broadcastID := <-s.pending:
			s.tombstones = append(s.tombstones, tombstoneItem{
				broadcastID: broadcastID,
				createTime:  time.Now(),
			})
		case <-ticker.C:
		}
	}
}

// triggerGCTombstone triggers the garbage collection of the tombstone.
func (s *tombstoneScheduler) triggerGCTombstone() {
	maxTombstoneLifetime := paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxLifetime.GetAsDurationByParse()
	maxTombstoneCount := paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxCount.GetAsInt()

	expiredTime := time.Now().Add(-maxTombstoneLifetime)
	expiredOffset := 0
	if len(s.tombstones) > maxTombstoneCount {
		expiredOffset = len(s.tombstones) - maxTombstoneCount
	}
	s.Logger().Info("triggerGCTombstone",
		zap.Int("tombstone count", len(s.tombstones)),
		zap.Int("expired offset", expiredOffset),
		zap.Time("expired time", expiredTime))
	for idx, tombstone := range s.tombstones {
		// drop tombstone until the expired time or until the expired offset.
		if idx >= expiredOffset && tombstone.createTime.After(expiredTime) {
			s.tombstones = s.tombstones[idx:]
			return
		}
		if err := s.bm.DropTombstone(s.notifier.Context(), tombstone.broadcastID); err != nil {
			s.Logger().Error("failed to drop tombstone", zap.Error(err))
			s.tombstones = s.tombstones[idx:]
			return
		}
	}
}
