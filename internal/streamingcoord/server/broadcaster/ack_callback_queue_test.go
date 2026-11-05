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
