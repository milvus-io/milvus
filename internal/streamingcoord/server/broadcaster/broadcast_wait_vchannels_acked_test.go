package broadcaster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// newWaitTestManager builds an empty broadcast task manager backed by an
// accept-everything catalog, the shape a secondary cluster's coord has before
// any replica of a given broadcast has reached it.
func newWaitTestManager(t *testing.T) *broadcastTaskManager {
	paramtable.Init()
	registry.ResetRegistration()

	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	meta.EXPECT().ListBroadcastTask(mock.Anything).Return(nil, nil).Maybe()
	meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	rc := idalloc.NewMockRootCoordClient(t)
	f := syncutil.NewFuture[internaltypes.MixCoordClient]()
	f.Set(rc)
	resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

	return newBroadcastTaskManager(nil)
}

// replicatedReplicaOf extracts one vchannel's replica of a broadcast and dresses
// it as a replicated, already-appended message -- exactly what a secondary
// cluster's proxy hands to Ack, and the only shape that makes the manager
// create a task it never broadcast itself.
func replicatedReplicaOf(msg message.BroadcastMutableMessage, vchannel string, timetick uint64) message.ImmutableMessage {
	id := walimplstest.NewTestMessageID(int64(timetick))
	for _, replica := range msg.SplitIntoMutableMessage() {
		if replica.VChannel() != vchannel {
			continue
		}
		return replica.WithReplicateHeader(&message.ReplicateHeader{
			ClusterID:              "primary",
			MessageID:              id,
			LastConfirmedMessageID: id,
			TimeTick:               timetick,
			VChannel:               vchannel,
		}).WithTimeTick(timetick).
			WithLastConfirmed(id).
			IntoImmutableMessage(id)
	}
	panic("vchannel is not one of the broadcast's own")
}

// waitersOf reports how many creation waiters are registered for a broadcast.
func waitersOf(bm *broadcastTaskManager, broadcastID uint64) int {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	return len(bm.taskCreationWaiters[broadcastID])
}

// TestWaitVChannelsAckedReturnsWhenTheNamedReplicasLand drives the wait a
// secondary cluster's append gate makes: two append-first vchannels, a waiter
// that registers BEFORE the broadcast exists here at all, and a release that
// must come only once BOTH named replicas have landed -- not after the first.
func TestWaitVChannelsAckedReturnsWhenTheNamedReplicasLand(t *testing.T) {
	bm := newWaitTestManager(t)
	defer bm.Close()

	const broadcastID = uint64(900)
	msg := createNewSplitShardBroadcastMsg(
		[]string{"p0_1v0", "p1_1v1", "p2_1v2"}, "p0_1v0", "p1_1v1").WithBroadcastID(broadcastID)

	ctx := context.Background()
	done := make(chan error, 1)
	go func() {
		done <- bm.WaitVChannelsAcked(ctx, broadcastID, []string{"p0_1v0", "p1_1v1"})
	}()

	// The waiter is registered against a broadcast the manager has never heard
	// of; nothing about it exists yet but the id.
	require.Eventually(t, func() bool { return waitersOf(bm, broadcastID) == 1 },
		time.Second, time.Millisecond, "the waiter must register before the task exists")
	_, ok := bm.getBroadcastTaskByID(broadcastID)
	require.False(t, ok)

	// The first replica creates the task and lands one of the two named
	// vchannels. That must NOT release the waiter.
	require.NoError(t, bm.Ack(ctx, replicatedReplicaOf(msg, "p0_1v0", 100)))
	require.Equal(t, 0, waitersOf(bm, broadcastID), "the creation waiter is dropped once the task exists")
	select {
	case err := <-done:
		t.Fatalf("released after only one of the named replicas landed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	// The second one releases it.
	require.NoError(t, bm.Ack(ctx, replicatedReplicaOf(msg, "p1_1v1", 101)))
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the waiter was not released after every named replica landed")
	}

	// A waiter arriving after the fact answers from the recorded checkpoints
	// alone, with no channel to wait on.
	assert.NoError(t, bm.WaitVChannelsAcked(ctx, broadcastID, []string{"p0_1v0", "p1_1v1"}))
	// A vchannel of the broadcast that has NOT been acked still blocks, so the
	// answer above is the ack state and not merely the task's existence.
	shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, bm.WaitVChannelsAcked(shortCtx, broadcastID, []string{"p2_1v2"}), context.DeadlineExceeded)
}

// TestWaitVChannelsAckedReturnsTheContextError asserts that both halves of the
// wait -- for the task, and for a vchannel's ack -- answer with the caller's own
// context error, and that an abandoned creation waiter is not left behind.
func TestWaitVChannelsAckedReturnsTheContextError(t *testing.T) {
	bm := newWaitTestManager(t)
	defer bm.Close()

	const broadcastID = uint64(901)

	// (a) Cancelled while waiting for a broadcast that never arrives.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- bm.WaitVChannelsAcked(ctx, broadcastID, []string{"p0_1v0"}) }()
	require.Eventually(t, func() bool { return waitersOf(bm, broadcastID) == 1 }, time.Second, time.Millisecond)
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
	assert.Eventually(t, func() bool { return waitersOf(bm, broadcastID) == 0 },
		time.Second, time.Millisecond, "an abandoned waiter must not be left registered")

	// (b) Cancelled while waiting for a vchannel of an existing broadcast.
	msg := createNewSplitShardBroadcastMsg(
		[]string{"p0_1v0", "p1_1v1", "p2_1v2"}, "p0_1v0", "p1_1v1").WithBroadcastID(broadcastID)
	require.NoError(t, bm.Ack(context.Background(), replicatedReplicaOf(msg, "p0_1v0", 110)))
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() { done2 <- bm.WaitVChannelsAcked(ctx2, broadcastID, []string{"p0_1v0", "p1_1v1"}) }()
	time.Sleep(50 * time.Millisecond)
	cancel2()
	assert.ErrorIs(t, <-done2, context.Canceled)
}

// TestWaitVChannelsAckedRejectsAVChannelOutsideTheBroadcast asserts that a name
// that is not part of the broadcast is reported rather than waited on forever:
// the waiter and the task would otherwise disagree about the broadcast's own
// topology and never converge.
func TestWaitVChannelsAckedRejectsAVChannelOutsideTheBroadcast(t *testing.T) {
	bm := newWaitTestManager(t)
	defer bm.Close()

	const broadcastID = uint64(902)
	msg := createNewSplitShardBroadcastMsg(
		[]string{"p0_1v0", "p1_1v1"}, "p0_1v0").WithBroadcastID(broadcastID)
	require.NoError(t, bm.Ack(context.Background(), replicatedReplicaOf(msg, "p0_1v0", 120)))

	err := bm.WaitVChannelsAcked(context.Background(), broadcastID, []string{"p9_1v9"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "p9_1v9")
}

// TestWaitVChannelsAckedOnStoppedBroadcaster asserts the wait refuses to start
// on a broadcaster that is shutting down, rather than blocking a shutdown that
// can no longer produce the acks it would wait for.
func TestWaitVChannelsAckedOnStoppedBroadcaster(t *testing.T) {
	bm := newWaitTestManager(t)
	bm.Close()

	assert.Error(t, bm.WaitVChannelsAcked(context.Background(), 903, []string{"p0_1v0"}))
}

// TestBlockUntilVChannelAckedOnARecoveredTask asserts that a task recovered from
// the catalog with a checkpoint already persisted answers immediately: the
// gate's fact is the checkpoint, not an in-memory channel that a restart loses.
func TestBlockUntilVChannelAckedOnARecoveredTask(t *testing.T) {
	paramtable.Init()
	registry.ResetRegistration()

	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(nil)

	msg := createNewSplitShardBroadcastMsg([]string{"p0_1v0", "p1_1v1"}, "p0_1v0").WithBroadcastID(904)
	proto := createNewWaitAckBroadcastTaskFromMessage(
		msg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED, []byte{0x01, 0x00})
	task := newBroadcastTaskFromProto(proto, metrics, ackScheduler)

	assert.NoError(t, task.BlockUntilVChannelAcked(context.Background(), "p0_1v0"))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, task.BlockUntilVChannelAcked(ctx, "p1_1v1"), context.DeadlineExceeded)
}
