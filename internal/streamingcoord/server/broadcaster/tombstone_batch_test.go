package broadcaster

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func newBatchGCTestScheduler(t *testing.T, count int) *tombstoneScheduler {
	t.Helper()
	configureTombstoneGCTest(t)
	limit := &paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum
	previous := limit.SwapTempValue("4")
	t.Cleanup(func() { limit.SwapTempValue(previous) })
	s := newTombstoneScheduler(mlog.With())
	t.Cleanup(s.notifier.Cancel)
	s.bm = newTombstoneGCTestManager()
	for id := uint64(1); id <= uint64(count); id++ {
		msg := createNewBroadcastMsg([]string{"v1"}).WithBroadcastID(id)
		task := newBroadcastTaskFromProto(createNewWaitAckBroadcastTaskFromMessage(
			msg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, []byte{1}), newBroadcasterMetrics(), nil)
		task.SetLogger(mlog.With())
		s.bm.tasks[id] = task
		s.tombstones = append(s.tombstones, tombstoneItem{broadcastID: id, createTime: time.Now()})
	}
	return s
}

func TestTombstoneGCBatchRetention(t *testing.T) {
	for _, test := range []struct {
		name     string
		maxCount string
		aged     int
		batches  []int
	}{
		{"count", "2", 0, []int{4, 3}},
		{"lifetime", "20", 5, []int{4, 1}},
		{"both", "4", 7, []int{4, 3}},
		{"all", "0", 0, []int{4, 4, 1}},
		{"exact_batch", "5", 0, []int{4}},
		{"none", "9", 0, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newBatchGCTestScheduler(t, 9)
			paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxCount.SwapTempValue(test.maxCount)
			for i := range test.aged {
				s.tombstones[i].createTime = time.Now().Add(-2 * time.Hour)
			}
			var batches []int
			var removed []uint64
			meta := mock_metastore.NewMockStreamingCoordCataLog(t)
			meta.EXPECT().RemoveBroadcastTasks(mock.Anything, mock.Anything).
				RunAndReturn(func(ctx context.Context, ids []uint64) error {
					batches = append(batches, len(ids))
					removed = append(removed, ids...)
					return nil
				}).Maybe()
			resource.InitForTest(resource.OptStreamingCatalog(meta))
			s.triggerGCTombstone()
			require.Equal(t, test.batches, batches)
			slices.Sort(removed)
			for i, id := range removed {
				require.Equal(t, uint64(i+1), id)
				require.NotContains(t, s.bm.tasks, id)
			}
			require.Len(t, s.tombstones, 9-len(removed))
			require.Len(t, s.bm.tasks, len(s.tombstones))
			for i, item := range s.tombstones {
				require.Equal(t, uint64(len(removed)+i+1), item.broadcastID)
				require.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, s.bm.tasks[item.broadcastID].State())
			}
		})
	}
}

func TestTombstoneGCBatchFailurePreservesProgress(t *testing.T) {
	s := newBatchGCTestScheduler(t, 10)
	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	var batches [][]uint64
	meta.EXPECT().RemoveBroadcastTasks(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, ids []uint64) error {
			batch := slices.Clone(ids)
			slices.Sort(batch)
			batches = append(batches, batch)
			if len(batches) == 2 {
				return context.DeadlineExceeded
			}
			return nil
		})
	resource.InitForTest(resource.OptStreamingCatalog(meta))
	s.triggerGCTombstone()
	require.Equal(t, [][]uint64{{1, 2, 3, 4}, {5, 6, 7, 8}}, batches)
	require.Len(t, s.tombstones, 6)
	require.Len(t, s.bm.tasks, 6)
	for id := uint64(5); id <= 10; id++ {
		require.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, s.bm.tasks[id].State())
	}
	s.triggerGCTombstone()
	require.Equal(t, [][]uint64{{1, 2, 3, 4}, {5, 6, 7, 8}, {5, 6, 7, 8}, {9, 10}}, batches)
	require.Empty(t, s.tombstones)
	require.Empty(t, s.bm.tasks)
}

func TestTombstoneGCBatchShutdownRecovery(t *testing.T) {
	for _, committed := range []bool{false, true} {
		name := "canceled_before_commit"
		if committed {
			name = "commit_response_lost"
		}
		t.Run(name, func(t *testing.T) {
			s := newBatchGCTestScheduler(t, 10)
			registry.ResetRegistration()
			var mu sync.Mutex
			persisted := make(map[uint64]*streamingpb.BroadcastTask)
			for id, task := range s.bm.tasks {
				persisted[id] = proto.Clone(task.task).(*streamingpb.BroadcastTask)
			}
			meta := mock_metastore.NewMockStreamingCoordCataLog(t)
			meta.EXPECT().ListBroadcastTask(mock.Anything).
				RunAndReturn(func(ctx context.Context) ([]*streamingpb.BroadcastTask, error) {
					mu.Lock()
					defer mu.Unlock()
					var tasks []*streamingpb.BroadcastTask
					for _, task := range persisted {
						tasks = append(tasks, proto.Clone(task).(*streamingpb.BroadcastTask))
					}
					return tasks, nil
				})
			blocked := make(chan struct{})
			attempts := 0
			meta.EXPECT().RemoveBroadcastTasks(mock.Anything, mock.Anything).
				RunAndReturn(func(ctx context.Context, ids []uint64) error {
					attempts++
					if attempts != 2 || committed {
						mu.Lock()
						for _, id := range ids {
							delete(persisted, id)
						}
						mu.Unlock()
					}
					if attempts == 2 {
						close(blocked)
						<-ctx.Done()
						return ctx.Err()
					}
					return nil
				})
			resource.InitForTest(resource.OptStreamingCatalog(meta))
			bc, err := RecoverBroadcaster(context.Background())
			require.NoError(t, err)
			closed := sync.OnceFunc(bc.Close)
			defer func() {
				// Bound cleanup even if the shutdown-cancellation assertion fails.
				bc.(*broadcastTaskManager).ackScheduler.tombstoneScheduler.notifier.Cancel()
				closed()
			}()
			select {
			case <-blocked:
			case <-time.After(5 * time.Second):
				t.Fatal("second batch did not start")
			}
			closeDone := make(chan struct{})
			go func() { closed(); close(closeDone) }()
			select {
			case <-closeDone:
			case <-time.After(5 * time.Second):
				t.Fatal("manager shutdown did not cancel in-flight GC")
			}
			old := bc.(*broadcastTaskManager)
			require.Len(t, old.tasks, 6, "the canceled batch must remain in memory")
			for _, task := range old.tasks {
				require.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, task.State())
			}
			wantRemaining := 6
			if committed {
				wantRemaining = 2
			}
			require.Len(t, persisted, wantRemaining)
			// No business callbacks are registered. Remaining TOMBSTONE records
			// must recover directly into GC, including after an ambiguous commit.
			recovered, err := RecoverBroadcaster(context.Background())
			require.NoError(t, err)
			defer recovered.Close()
			bm := recovered.(*broadcastTaskManager)
			require.Eventually(t, func() bool {
				bm.mu.Lock()
				defer bm.mu.Unlock()
				return len(bm.tasks) == 0
			}, 5*time.Second, time.Millisecond)
			mu.Lock()
			remaining := len(persisted)
			mu.Unlock()
			require.Zero(t, remaining)
		})
	}
}

func TestTombstoneGCBatchDoesNotBlockResultsOrReviveLateACKs(t *testing.T) {
	s := newBatchGCTestScheduler(t, 2)
	registry.ResetRegistration()
	registerDropCollectionNoopCallbacks()
	// Legacy tombstones may lack checkpoints. A late ACK must not fill them
	// and save the record again while its deletion is in progress.
	legacy := s.bm.tasks[2]
	legacy.task.AckedCheckpoints[0] = nil
	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	started := make(chan struct{})
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	meta.EXPECT().RemoveBroadcastTasks(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, ids []uint64) error {
			close(started)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}).Once()
	resource.InitForTest(resource.OptStreamingCatalog(meta))
	gcDone := make(chan struct{})
	go func() { s.triggerGCTombstone(); close(gcDone) }()
	defer func() { unblock(); <-gcDone }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("GC did not start")
	}
	resultDone := make(chan error, 1)
	go func() {
		task, _ := s.bm.getBroadcastTaskByID(1)
		_, err := task.BlockUntilDone(context.Background())
		resultDone <- err
	}()
	select {
	case err := <-resultDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("RPC result waited for GC I/O")
	}
	_, result := s.bm.tasks[1].BroadcastResult()
	lateACK := legacy.GetImmutableMessageFromVChannel("v1")
	ackDone := make(chan error, 1)
	go func() {
		if err := legacy.FastAck(context.Background(), map[string]*types.AppendResult{"v1": result["v1"]}); err != nil {
			ackDone <- err
			return
		}
		ackDone <- legacy.Ack(context.Background(), lateACK)
	}()
	select {
	case err := <-ackDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("late ACK waited for GC I/O")
	}
	unblock()
	<-gcDone
	require.NoError(t, legacy.Ack(context.Background(), lateACK))
	require.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE, legacy.State())
	require.Nil(t, legacy.task.AckedCheckpoints[0])
	require.Empty(t, s.bm.tasks)
}

func TestDropTombstonesValidatesAndDeduplicatesIDs(t *testing.T) {
	s := newBatchGCTestScheduler(t, 2)
	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(meta))
	s.bm.tasks[2].task.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING
	require.Error(t, s.bm.DropTombstones(context.Background(), []uint64{1, 2}))
	require.Len(t, s.bm.tasks, 2)
	meta.EXPECT().RemoveBroadcastTasks(mock.Anything, []uint64{1}).Return(nil).Once()
	require.NoError(t, s.bm.DropTombstones(context.Background(), []uint64{1, 1, 99}))
	require.Len(t, s.bm.tasks, 1)
	require.NoError(t, s.bm.DropTombstones(context.Background(), nil))
	s.bm.lifetime.SetState(typeutil.LifetimeStateStopped)
	require.Error(t, s.bm.DropTombstones(context.Background(), []uint64{2}))
}

func TestTombstoneGCRetiresIdempotencyOnlyAfterCatalogSuccess(t *testing.T) {
	s := newBatchGCTestScheduler(t, 1)
	msg := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{}).
		WithBody(&msgpb.DropCollectionRequest{}).
		WithIdempotencyKey(message.NewCollectionScopedIdempotencyKey(1, "gc-retry")).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast().
		WithBroadcastID(1)
	task := newBroadcastTaskFromProto(createNewWaitAckBroadcastTaskFromMessage(
		msg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, []byte{1}), newBroadcasterMetrics(), nil)
	task.SetLogger(mlog.With())
	s.bm.tasks[1] = task
	s.bm.idempotencyIndex = newIdempotencyIndex()
	scope := task.IdempotencyScope()
	require.NotEmpty(t, scope)
	s.bm.idempotencyIndex.Add(scope, 1)

	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(meta))
	attempts := 0
	meta.EXPECT().RemoveBroadcastTasks(mock.Anything, []uint64{1}).
		RunAndReturn(func(context.Context, []uint64) error {
			attempts++
			owner, ok := s.bm.idempotencyIndex.Get(scope)
			require.True(t, ok, "catalog I/O must retain the idempotency window")
			require.Equal(t, uint64(1), owner)
			if attempts == 1 {
				return context.DeadlineExceeded
			}
			return nil
		}).Twice()

	s.triggerGCTombstone()
	_, ok := s.bm.idempotencyIndex.Get(scope)
	require.True(t, ok, "a failed delete must remain retryable and deduplicated")
	require.Same(t, task, s.bm.tasks[1])
	require.Len(t, s.tombstones, 1)
	s.triggerGCTombstone()
	_, ok = s.bm.idempotencyIndex.Get(scope)
	require.False(t, ok)
	require.Empty(t, s.bm.tasks)
	require.Empty(t, s.tombstones)
	require.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE, task.State())
}
