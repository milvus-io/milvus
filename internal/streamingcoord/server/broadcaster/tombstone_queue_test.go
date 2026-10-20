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
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestTombstoneQueueConcurrentEnqueueAndGC(t *testing.T) {
	setupTombstoneGC(t)
	const producers, perProducer = 8, 512
	paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxCount.SwapTempValue("10000")
	s := newTombstoneScheduler(mlog.With())
	defer s.notifier.Cancel()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	started := time.Now()
	var producersDone sync.WaitGroup
	for producer := range producers {
		producersDone.Add(1)
		go func() {
			defer producersDone.Done()
			for i := range perProducer {
				s.AddPending(uint64(producer*perProducer + i))
			}
		}()
	}
	finished := make(chan struct{})
	go func() { producersDone.Wait(); close(finished) }()
	for {
		s.triggerGCTombstone()
		select {
		case <-finished:
			s.triggerGCTombstone()
			require.Len(t, s.tombstones, producers*perProducer)
			seen := make(map[uint64]struct{})
			next := make([]int, producers)
			previous := started
			for _, item := range s.tombstones {
				require.False(t, item.createTime.Before(previous), "enqueue time must stay ordered across queue swaps")
				previous = item.createTime
				seen[item.broadcastID] = struct{}{}
				producer, offset := int(item.broadcastID)/perProducer, int(item.broadcastID)%perProducer
				require.Equal(t, next[producer], offset, "preserve each producer's FIFO order")
				next[producer]++
			}
			require.Len(t, seen, producers*perProducer, "no lost or duplicated entries")
			return
		case <-time.After(5 * time.Millisecond):
		case <-deadline.C:
			t.Fatal("enqueue or GC queue exchange stalled")
		}
	}
}

func TestTombstoneQueueDoesNotHoldCallbackLockDuringGC(t *testing.T) {
	coord, node := setupTombstoneGC(t)
	require.NoError(t, coord.SaveBroadcastTask(t.Context(), 1, gcTestTombstone(1, []string{"p0_vcchan"}, true)))
	bc, err := RecoverBroadcaster(t.Context())
	require.NoError(t, err)
	defer bc.Close()
	bm := bc.(*broadcastTaskManager)
	s := gcTestScheduler(bm, 1)

	// Hold a GC pass in catalog I/O while an unrelated callback completes.
	entered, release, gcDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	var enteredOnce sync.Once
	checkpointPatch := mockey.Mock(mockey.GetMethod(node, "GetConsumeCheckpoint")).To(func(context.Context, string) (*streamingpb.WALCheckpoint, error) {
		enteredOnce.Do(func() { close(entered) })
		<-release
		return &streamingpb.WALCheckpoint{TimeTick: 1000}, nil
	}).Build()
	defer checkpointPatch.UnPatch()
	go func() { s.triggerGCTombstone(); close(gcDone) }()
	defer func() { unblock(); <-gcDone }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("GC did not enter catalog read")
	}

	callbackPatch := mockey.Mock(registry.CallMessageAckCallback).Return(nil).Build()
	defer callbackPatch.UnPatch()
	ackScheduler := newAckCallbackScheduler(mlog.With())
	ackScheduler.tombstoneScheduler = s
	ackScheduler.triggerChan <- struct{}{} // Completion must not wait for an outstanding wakeup.
	proto := gcTestTombstone(2, []string{"p0_vcchan"}, true)
	proto.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED
	task := newBroadcastTaskFromProto(proto, bm.metrics, ackScheduler)
	task.SetLogger(mlog.With())
	bm.mu.Lock()
	bm.tasks[2] = task
	bm.mu.Unlock()
	key := message.NewExclusiveClusterResourceKey()
	guard := ackScheduler.rkLocker.Lock(key)
	callbackDone := make(chan error, 1)
	enqueuedAfter := time.Now()
	go func() { callbackDone <- ackScheduler.doAckCallback(task, guard) }()
	select {
	case err := <-callbackDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		unblock()
		<-callbackDone
		t.Fatal("callback waited for GC to accept its tombstone")
	}
	enqueuedBefore := time.Now()
	nextGuard, err := ackScheduler.rkLocker.FastLock(key)
	require.NoError(t, err, "the next conflicting callback must acquire its resource lock while GC is stalled")
	nextGuard.Unlock()
	select {
	case <-gcDone:
		t.Fatal("GC unexpectedly finished before release")
	default:
	}

	unblock()
	<-gcDone
	require.Empty(t, s.tombstones, "the active GC snapshot must not include later enqueues")
	s.triggerGCTombstone()
	require.Len(t, s.tombstones, 1, "next pass must merge the tombstone enqueued during GC")
	item := s.tombstones[0]
	require.Equal(t, uint64(2), item.broadcastID)
	require.False(t, item.createTime.Before(enqueuedAfter))
	require.False(t, item.createTime.After(enqueuedBefore), "TTL starts at enqueue, not at the next GC pass")
	persisted, err := coord.ListBroadcastTask(t.Context())
	require.NoError(t, err)
	require.Len(t, persisted, 1)
	require.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, persisted[0].State)
}

func TestTombstoneQueueCloseWithPendingEntries(t *testing.T) {
	setupTombstoneGC(t)
	s := newTombstoneScheduler(mlog.With())
	s.Initialize(nil, nil)
	s.AddPending(1)
	closed := make(chan struct{})
	go func() { s.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close waited for pending tombstones to be consumed")
	}
	s.AddPending(2)
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	require.Equal(t, 1, s.pending.Len(), "enqueue after Close must be ignored")
	require.Equal(t, uint64(1), s.pending.Next().broadcastID)
}
