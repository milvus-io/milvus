package broadcaster

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// Exercises the real ACK scheduler and load/release message types with controlled
// business callbacks. It deliberately overlaps a retry, blocked persistence,
// queued same-key successors, and blocked GC catalog deletion.
func TestAckCallbacksSameCollectionRemainOrderedWithBatchGC(t *testing.T) {
	configureTombstoneGCTest(t)
	registry.ResetRegistration()
	defer registry.ResetRegistration()
	s := newAckCallbackScheduler(mlog.With())
	bm := newTombstoneGCTestManager()
	gcStarted := make(chan struct{})
	gcGate := make(chan struct{})
	persistStarted := make(chan struct{})
	persistGate := make(chan struct{})
	releaseGC := sync.OnceFunc(func() { close(gcGate) })
	releasePersist := sync.OnceFunc(func() { close(persistGate) })
	defer releaseGC()
	defer releasePersist()

	var eventsMu sync.Mutex
	var appliedIDs []uint64
	var appliedStates []bool
	var active atomic.Int32
	var firstAttempts atomic.Int32
	apply := func(id uint64, loaded bool) error {
		if active.Add(1) != 1 {
			t.Error("same-collection business callbacks overlapped")
		}
		defer active.Add(-1)
		if id == 1 && firstAttempts.Add(1) == 1 {
			return context.DeadlineExceeded
		}
		eventsMu.Lock()
		appliedIDs = append(appliedIDs, id)
		appliedStates = append(appliedStates, loaded)
		eventsMu.Unlock()
		return nil
	}
	registry.RegisterAlterLoadConfigV2AckCallback(func(_ context.Context, result message.BroadcastResultAlterLoadConfigMessageV2) error {
		return apply(result.Message.BroadcastHeader().BroadcastID, true)
	})
	registry.RegisterDropLoadConfigV2AckCallback(func(_ context.Context, result message.BroadcastResultDropLoadConfigMessageV2) error {
		return apply(result.Message.BroadcastHeader().BroadcastID, false)
	})

	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, id uint64, task *streamingpb.BroadcastTask) error {
			if id == 1 && task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE {
				close(persistStarted)
				select {
				case <-persistGate:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	meta.EXPECT().RemoveBroadcastTasks(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, ids []uint64) error {
			if len(ids) == 1 && ids[0] == 0 {
				close(gcStarted)
				select {
				case <-gcGate:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	resource.InitForTest(resource.OptStreamingCatalog(meta))

	rk := message.NewExclusiveCollectionNameResourceKey("db", "same_collection")
	tasks := make([]*broadcastTask, 0, 3)
	for id := uint64(0); id <= 3; id++ {
		var msg message.BroadcastMutableMessage
		if id == 2 {
			msg = message.NewDropLoadConfigMessageBuilderV2().
				WithHeader(&message.DropLoadConfigMessageHeader{CollectionId: 42}).
				WithBody(&message.DropLoadConfigMessageBody{}).
				WithBroadcast([]string{"by-dev-0_vcchan"}).MustBuildBroadcast()
		} else {
			msg = message.NewAlterLoadConfigMessageBuilderV2().
				WithHeader(&message.AlterLoadConfigMessageHeader{CollectionId: 42}).
				WithBody(&message.AlterLoadConfigMessageBody{}).
				WithBroadcast([]string{"by-dev-0_vcchan"}).MustBuildBroadcast()
		}
		msg = msg.OverwriteBroadcastHeader(id, rk)
		state := streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING
		if id == 0 {
			state = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE
		}
		p := createNewWaitAckBroadcastTaskFromMessage(msg, state, []byte{1})
		p.AckedCheckpoints[0].TimeTick = id + 100
		task := newBroadcastTaskFromProto(p, newBroadcasterMetrics(), s)
		task.SetLogger(mlog.With())
		bm.tasks[id] = task
		if id != 0 {
			tasks = append(tasks, task)
		}
	}
	s.Initialize([]*broadcastTask{tasks[2], tasks[0], tasks[1]}, []uint64{0}, bm)
	defer func() {
		releaseGC()
		releasePersist()
		s.Close()
	}()
	for _, gate := range []<-chan struct{}{gcStarted, persistStarted} {
		select {
		case <-gate:
		case <-time.After(5 * time.Second):
			t.Fatal("expected blocked phase was not reached")
		}
	}

	guards, ok := s.rkLocker.TryLock(rk)
	if ok {
		guards.Unlock()
		t.Fatal("ACK lock released before durable completion")
	}
	eventsMu.Lock()
	ids := slices.Clone(appliedIDs)
	eventsMu.Unlock()
	require.Equal(t, []uint64{1}, ids)
	releasePersist()

	// GC is still blocked, but all three same-collection operations must finish
	// business execution in WAL order and enqueue all of their GC work.
	require.Eventually(t, func() bool {
		s.tombstoneScheduler.pendingMu.Lock()
		defer s.tombstoneScheduler.pendingMu.Unlock()
		return len(s.tombstoneScheduler.pending) == 3
	}, 5*time.Second, time.Millisecond)
	eventsMu.Lock()
	ids = slices.Clone(appliedIDs)
	states := slices.Clone(appliedStates)
	eventsMu.Unlock()
	require.Equal(t, []uint64{1, 2, 3}, ids)
	require.Equal(t, []bool{true, false, true}, states)
	require.EqualValues(t, 2, firstAttempts.Load())
	require.Zero(t, active.Load())

	releaseGC()
	require.Eventually(t, func() bool {
		bm.mu.Lock()
		defer bm.mu.Unlock()
		return len(bm.tasks) == 0
	}, 5*time.Second, time.Millisecond)
}
