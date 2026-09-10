package broadcaster

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestAckCallbacksCompleteWhileTombstoneGCBlocked(t *testing.T) {
	configureTombstoneGCTest(t)
	registry.ResetRegistration()
	registerDropCollectionNoopCallbacks()

	gcStarted := make(chan struct{})
	gcContext, releaseGC := context.WithCancel(context.Background())
	defer releaseGC()
	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, id uint64, task *streamingpb.BroadcastTask) error {
			if id == 0 {
				close(gcStarted)
				select {
				case <-gcContext.Done():
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	resource.InitForTest(resource.OptStreamingCatalog(meta))

	s := newAckCallbackScheduler(mlog.With())
	bm := newTombstoneGCTestManager()
	const callbacks = 32
	for id := uint64(0); id <= callbacks; id++ {
		rk := message.NewExclusiveCollectionNameResourceKey("db", "collection-"+strconv.FormatUint(id, 10))
		msg := createNewBroadcastMsg([]string{"v1"}, rk).WithBroadcastID(id)
		state := streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING
		if id == 0 {
			state = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE
		}
		task := newBroadcastTaskFromProto(createNewWaitAckBroadcastTaskFromMessage(msg, state, []byte{1}), newBroadcasterMetrics(), s)
		task.SetLogger(mlog.With())
		bm.tasks[id] = task
	}
	s.tombstoneScheduler.Initialize(bm, []uint64{0})
	var workers sync.WaitGroup
	defer func() {
		s.notifier.Cancel()
		s.tombstoneScheduler.Close()
		workers.Wait()
	}()
	select {
	case <-gcStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("GC did not start")
	}

	completed := make(chan error, callbacks)
	for id := uint64(1); id <= callbacks; id++ {
		task := bm.tasks[id]
		guards := s.rkLocker.Lock(task.Header().ResourceKeys.Collect()...)
		workers.Add(1)
		go func() {
			defer workers.Done()
			completed <- s.doAckCallback(task, guards)
		}()
	}
	for range callbacks {
		select {
		case err := <-completed:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("completed ACK callback is waiting for tombstone GC")
		}
	}
	for id := uint64(1); id <= callbacks; id++ {
		guards, ok := s.rkLocker.TryLock(bm.tasks[id].Header().ResourceKeys.Collect()...)
		require.True(t, ok, "the next callback for the same collection must acquire its locks")
		guards.Unlock()
	}

	releaseGC()
	require.Eventually(t, func() bool {
		bm.mu.Lock()
		defer bm.mu.Unlock()
		return len(bm.tasks) == 0
	}, 5*time.Second, time.Millisecond, "all concurrent GC enqueues must be consumed")
}

func TestTombstoneRecoveryAfterShutdownBeforeHandoff(t *testing.T) {
	configureTombstoneGCTest(t)
	registry.ResetRegistration()
	registerDropCollectionNoopCallbacks()
	s := newAckCallbackScheduler(mlog.With())
	defer s.notifier.Cancel()
	s.tombstoneScheduler.notifier.Cancel()

	var persisted *streamingpb.BroadcastTask
	deleted := make(chan struct{})
	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	meta.EXPECT().SaveBroadcastTask(mock.Anything, uint64(1), mock.Anything).
		RunAndReturn(func(ctx context.Context, id uint64, task *streamingpb.BroadcastTask) error {
			if task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE {
				persisted = proto.Clone(task).(*streamingpb.BroadcastTask)
			} else {
				close(deleted)
			}
			return nil
		}).Times(2)
	resource.InitForTest(resource.OptStreamingCatalog(meta))
	task := newBroadcastTaskFromProto(createNewWaitAckBroadcastTaskFromMessage(
		createNewBroadcastMsg([]string{"v1"}).WithBroadcastID(1),
		streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, []byte{1}), newBroadcasterMetrics(), s)
	task.SetLogger(mlog.With())
	require.NoError(t, s.doAckCallback(task, s.rkLocker.Lock()))
	require.NotNil(t, persisted)
	require.Empty(t, s.tombstoneScheduler.pending)

	// No callbacks are registered after restart: a recovered tombstone must go
	// directly to GC instead of replaying its completed business callback.
	registry.ResetRegistration()
	meta.EXPECT().ListBroadcastTask(mock.Anything).Return([]*streamingpb.BroadcastTask{persisted}, nil).Once()
	recovered, err := RecoverBroadcaster(context.Background())
	require.NoError(t, err)
	defer recovered.Close()
	select {
	case <-deleted:
	case <-time.After(5 * time.Second):
		t.Fatal("persisted tombstone was not recovered into GC")
	}
}

func configureTombstoneGCTest(t *testing.T) {
	t.Helper()
	paramtable.Init()
	cfg := &paramtable.Get().StreamingCfg
	for _, setting := range []struct {
		param *paramtable.ParamItem
		value string
	}{
		{&cfg.WALBroadcasterTombstoneMaxCount, "0"},
		{&cfg.WALBroadcasterTombstoneMaxLifetime, "1h"},
		{&cfg.WALBroadcasterTombstoneCheckInternal, "1h"},
	} {
		previous := setting.param.SwapTempValue(setting.value)
		t.Cleanup(func() { setting.param.SwapTempValue(previous) })
	}
}

func newTombstoneGCTestManager() *broadcastTaskManager {
	bm := &broadcastTaskManager{
		lifetime: typeutil.NewLifetime(),
		mu:       &sync.Mutex{},
		tasks:    make(map[uint64]*broadcastTask),
	}
	bm.SetLogger(mlog.With())
	return bm
}
