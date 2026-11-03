package broadcaster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestAckQueueUnboundedEnqueuePreservesConflictingCallbackOrder(t *testing.T) {
	setupTombstoneGC(t)
	const count = 64 // Exceeds the previous channel capacity.
	order := make(chan uint64, count)
	patch := mockey.Mock(registry.CallMessageAckCallback).To(func(_ context.Context, msg message.BroadcastMutableMessage, _ map[string]*message.AppendResult) error {
		order <- msg.BroadcastHeader().BroadcastID
		return nil
	}).Build()
	defer patch.UnPatch()

	s := newAckCallbackScheduler(mlog.With())
	// Stall the scheduler without holding the enqueue lock.
	s.rkLockerMu.Lock()
	var unlockOnce sync.Once
	unlock := func() { unlockOnce.Do(s.rkLockerMu.Unlock) }
	s.Initialize(nil, nil, nil)
	defer s.Close()
	defer unlock()

	key := message.NewExclusiveCollectionNameResourceKey("db", "ack-queue")
	metrics := newBroadcasterMetrics()
	tasks := make([]*broadcastTask, 0, count)
	for i := 1; i <= count; i++ {
		id := uint64(i)
		proto := gcTestTombstone(id, []string{"p0_vcchan"}, true)
		proto.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED
		proto.Message = createNewBroadcastMsg([]string{"p0_vcchan"}, key).WithBroadcastID(id).IntoMessageProto()
		task := newBroadcastTaskFromProto(proto, metrics, s)
		task.SetLogger(mlog.With())
		tasks = append(tasks, task)
	}
	enqueued := make(chan struct{})
	go func() {
		for _, task := range tasks {
			s.AddTask(task)
		}
		close(enqueued)
	}()
	select {
	case <-enqueued:
	case <-time.After(5 * time.Second):
		unlock()
		<-enqueued
		t.Fatal("AddTask blocked behind the stalled scheduler or a full notification channel")
	}
	require.Len(t, s.triggerChan, 1, "enqueue notifications coalesce")
	unlock()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for _, task := range tasks {
		_, err := task.BlockUntilDone(ctx)
		require.NoError(t, err)
	}
	for i := 1; i <= count; i++ {
		require.Equal(t, uint64(i), <-order, "conflicting callbacks retain FIFO order")
	}
	require.Empty(t, order)
	// A later enqueue must also be picked up after the first batch.
	nextProto := gcTestTombstone(count+1, []string{"p0_vcchan"}, true)
	nextProto.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED
	next := newBroadcastTaskFromProto(nextProto, metrics, s)
	next.SetLogger(mlog.With())
	s.AddTask(next)
	select {
	case id := <-order:
		require.Equal(t, uint64(count+1), id)
	case <-ctx.Done():
		t.Fatal("enqueue wakeup was lost")
	}
	_, err := next.BlockUntilDone(ctx)
	require.NoError(t, err)
}

func TestAckQueueRejectsEnqueueAfterClose(t *testing.T) {
	setupTombstoneGC(t)
	s := newAckCallbackScheduler(mlog.With())
	s.Initialize(nil, nil, nil)
	s.Close()
	require.Panics(t, func() { s.AddTask(nil) }, "preserve the existing closed-scheduler contract")
}

func TestAckQueueBlockedDependenciesCannotBeOvertaken(t *testing.T) {
	mockey.PatchConvey("a blocked multi-key callback retains its place on every conflicting key", t, func() {
		s := newAckCallbackScheduler(mlog.With())
		order := make(chan uint64, 4)
		mockey.Mock((*ackCallbackScheduler).doAckCallback).To(func(s *ackCallbackScheduler, task *broadcastTask, guards *lockGuards) error {
			id := task.Header().BroadcastID
			s.rkLockerMu.Lock()
			guards.Unlock()
			s.rkLockerMu.Unlock()
			order <- id
			return nil
		}).Build()
		key := func(name string) message.ResourceKey {
			return message.NewExclusiveCollectionNameResourceKey("db", name)
		}
		first := s.rkLocker.Lock(key("collection"))
		var release sync.Once
		defer release.Do(first.Unlock)
		// Load holds collection. Snapshot waits for collection + snapshot;
		// Restore waits for snapshot + target. Its successor also waits on target.
		s.pendingAckedTasks = []*broadcastTask{
			{msg: createNewBroadcastMsg([]string{"p0_vcchan"}, key("collection"), key("snapshot")).WithBroadcastID(1)},
			{msg: createNewBroadcastMsg([]string{"p0_vcchan"}, key("snapshot"), key("target")).WithBroadcastID(2)},
			{msg: createNewBroadcastMsg([]string{"p0_vcchan"}, key("target")).WithBroadcastID(3)},
			{msg: createNewBroadcastMsg([]string{"p0_vcchan"}, key("independent")).WithBroadcastID(4)},
		}
		run := func(want uint64, remaining int) {
			t.Helper()
			count := len(s.pendingAckedTasks)
			s.triggerAckCallback()
			got := make([]uint64, 0, count-len(s.pendingAckedTasks))
			for range count - len(s.pendingAckedTasks) {
				select {
				case id := <-order:
					got = append(got, id)
				case <-time.After(5 * time.Second):
					t.Fatal("scheduled callback did not finish")
				}
			}
			require.Equal(t, []uint64{want}, got)
			require.Len(t, s.pendingAckedTasks, remaining)
		}
		run(4, 3)
		release.Do(first.Unlock)
		run(1, 2)
		run(2, 1)
		run(3, 0)
	})
}

func TestAckQueuePendingReadersKeepCompatibleConcurrency(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		firstShared, nextShared bool
		concurrent              bool
	}{
		{name: "reader reader", firstShared: true, nextShared: true, concurrent: true},
		{name: "reader writer", firstShared: true},
		{name: "writer reader", nextShared: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockey.PatchConvey("only conflicting accesses inherit the earlier wait", t, func() {
				s := newAckCallbackScheduler(mlog.With())
				order := make(chan uint64, 2)
				mockey.Mock((*ackCallbackScheduler).doAckCallback).To(func(s *ackCallbackScheduler, task *broadcastTask, guards *lockGuards) error {
					id := task.Header().BroadcastID
					s.rkLockerMu.Lock()
					guards.Unlock()
					s.rkLockerMu.Unlock()
					order <- id
					return nil
				}).Build()
				blocked := message.NewExclusiveCollectionNameResourceKey("db", "collection")
				firstKey := message.NewExclusiveSnapshotNameResourceKey(100, "snapshot")
				firstKey.Shared = tc.firstShared
				nextKey := firstKey
				nextKey.Shared = tc.nextShared
				guard := s.rkLocker.Lock(blocked)
				var release sync.Once
				defer release.Do(guard.Unlock)
				s.pendingAckedTasks = []*broadcastTask{
					{msg: createNewBroadcastMsg([]string{"p0_vcchan"}, blocked, firstKey).WithBroadcastID(1)},
					{msg: createNewBroadcastMsg([]string{"p0_vcchan"}, nextKey).WithBroadcastID(2)},
				}
				pass := func() []uint64 {
					count := len(s.pendingAckedTasks)
					s.triggerAckCallback()
					ids := make([]uint64, 0, count-len(s.pendingAckedTasks))
					for range count - len(s.pendingAckedTasks) {
						select {
						case id := <-order:
							ids = append(ids, id)
						case <-time.After(5 * time.Second):
							t.Fatal("scheduled callback did not finish")
						}
					}
					return ids
				}
				if tc.concurrent {
					require.Equal(t, []uint64{2}, pass())
				} else {
					require.Empty(t, pass())
				}
				release.Do(guard.Unlock)
				require.Equal(t, []uint64{1}, pass())
				if !tc.concurrent {
					require.Equal(t, []uint64{2}, pass())
				}
				require.Empty(t, s.pendingAckedTasks)
			})
		})
	}
}
