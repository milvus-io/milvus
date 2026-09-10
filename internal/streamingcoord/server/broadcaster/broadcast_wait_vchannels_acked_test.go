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

	// (a) Canceled while waiting for a broadcast that never arrives.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- bm.WaitVChannelsAcked(ctx, broadcastID, []string{"p0_1v0"}) }()
	require.Eventually(t, func() bool { return waitersOf(bm, broadcastID) == 1 }, time.Second, time.Millisecond)
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
	assert.Eventually(t, func() bool { return waitersOf(bm, broadcastID) == 0 },
		time.Second, time.Millisecond, "an abandoned waiter must not be left registered")

	// (b) Canceled while waiting for a vchannel of an existing broadcast.
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
		msg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED, bitmapAcking(msg, "p0_1v0"))
	task := newBroadcastTaskFromProto(proto, metrics, ackScheduler)

	assert.NoError(t, task.BlockUntilVChannelAcked(context.Background(), "p0_1v0", nil))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, task.BlockUntilVChannelAcked(ctx, "p1_1v1", nil), context.DeadlineExceeded)
}

// TestWaitVChannelsAckedIsReleasedByClose is the shutdown case. A gated wait
// holds the broadcaster's lifetime, and Close() waits on that lifetime -- so if
// Close did not release the waiters first, the two would deadlock and mixcoord's
// shutdown would hang until SIGKILL. The gate's caller cannot break the tie: it
// is a different process's replicate stream, whose context has no deadline.
//
// Both halves of the wait are covered: one waiter parked on a broadcast that
// never arrives, one parked on a vchannel of a broadcast that did.
func TestWaitVChannelsAckedIsReleasedByClose(t *testing.T) {
	bm := newWaitTestManager(t)

	const arrived = uint64(905)
	const neverArrives = uint64(906)

	msg := createNewSplitShardBroadcastMsg(
		[]string{"p0_1v0", "p1_1v1", "p2_1v2"}, "p0_1v0", "p1_1v1").WithBroadcastID(arrived)
	require.NoError(t, bm.Ack(context.Background(), replicatedReplicaOf(msg, "p0_1v0", 130)))

	// (a) parked on an unacked vchannel of an existing broadcast.
	onVChannel := make(chan error, 1)
	go func() {
		onVChannel <- bm.WaitVChannelsAcked(context.Background(), arrived, []string{"p0_1v0", "p1_1v1"})
	}()
	// (b) parked on a broadcast that has never been heard of.
	onTask := make(chan error, 1)
	go func() {
		onTask <- bm.WaitVChannelsAcked(context.Background(), neverArrives, []string{"p0_1v0"})
	}()
	require.Eventually(t, func() bool { return waitersOf(bm, neverArrives) == 1 }, time.Second, time.Millisecond)
	// Give (a) a moment to reach its select before Close races it.
	time.Sleep(50 * time.Millisecond)

	closed := make(chan struct{})
	go func() {
		bm.Close()
		close(closed)
	}()

	select {
	case err := <-onVChannel:
		assert.Error(t, err, "a wait parked on a vchannel must end when the broadcaster closes")
	case <-time.After(10 * time.Second):
		t.Fatal("Close() did not release the vchannel waiter")
	}
	select {
	case err := <-onTask:
		assert.Error(t, err, "a wait parked on a missing broadcast must end when the broadcaster closes")
	case <-time.After(10 * time.Second):
		t.Fatal("Close() did not release the task-creation waiter")
	}
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close() did not return; it is deadlocked against the waiters it must release")
	}

	// A wait started after the close is refused outright rather than parked.
	assert.Error(t, bm.WaitVChannelsAcked(context.Background(), arrived, []string{"p1_1v1"}))
}

// TestUnackedVChannelsReportsWhatTheGateIsStillWaitingFor covers the naming the
// slow-wait Warn depends on: an operator seeing a frozen replicate stream needs
// to be told WHICH vchannel has not landed, not merely that something has not.
func TestUnackedVChannelsReportsWhatTheGateIsStillWaitingFor(t *testing.T) {
	bm := newWaitTestManager(t)
	defer bm.Close()

	const broadcastID = uint64(907)
	msg := createNewSplitShardBroadcastMsg(
		[]string{"p0_1v0", "p1_1v1", "p2_1v2"}, "p0_1v0", "p1_1v1").WithBroadcastID(broadcastID)
	require.NoError(t, bm.Ack(context.Background(), replicatedReplicaOf(msg, "p0_1v0", 140)))

	task, ok := bm.getBroadcastTaskByID(broadcastID)
	require.True(t, ok)
	assert.Equal(t, []string{"p1_1v1"}, task.UnackedVChannels([]string{"p0_1v0", "p1_1v1"}))
	// A name outside the broadcast counts as unacked rather than raising here:
	// this feeds a log line, not a decision.
	assert.Equal(t, []string{"p9_1v9"}, task.UnackedVChannels([]string{"p9_1v9"}))

	require.NoError(t, bm.Ack(context.Background(), replicatedReplicaOf(msg, "p1_1v1", 141)))
	assert.Empty(t, task.UnackedVChannels([]string{"p0_1v0", "p1_1v1"}))
}

// TestIsVChannelAckedAcceptsALegacyBitmapOnlyTask pins the one predicate every
// reader of the ack state shares.
//
// A task persisted before 2.6.1 carries only the acked bitmap, with no
// checkpoint beside it. PendingBroadcastMessages has always treated that as
// acked; if the gate's own check disagreed, such a vchannel would read as
// unacked forever and the gate would never open.
func TestIsVChannelAckedAcceptsALegacyBitmapOnlyTask(t *testing.T) {
	paramtable.Init()
	registry.ResetRegistration()

	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(nil)

	msg := createNewSplitShardBroadcastMsg([]string{"p0_1v0", "p1_1v1"}, "p0_1v0").WithBroadcastID(908)
	proto := createNewWaitAckBroadcastTaskFromMessage(
		msg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED, bitmapAcking(msg, "p0_1v0"))
	// Strip the checkpoints, leaving only the bitmap: the legacy shape.
	proto.AckedCheckpoints = nil
	task := newBroadcastTaskFromProto(proto, metrics, ackScheduler)

	assert.Empty(t, task.UnackedVChannels([]string{"p0_1v0"}))
	assert.NoError(t, task.BlockUntilVChannelAcked(context.Background(), "p0_1v0", nil))

	// Every shape the predicate has to answer for, stated directly. The task
	// above is the legacy one; the rest are the shapes a live or recovered task
	// takes, including the truncated arrays a partially written record leaves.
	assert.True(t, isVChannelAcked(&streamingpb.BroadcastTask{
		AckedVchannelBitmap: []byte{0x01},
	}, 0), "bitmap set, no checkpoint array at all")
	assert.False(t, isVChannelAcked(&streamingpb.BroadcastTask{
		AckedVchannelBitmap: []byte{0x00},
	}, 0), "nothing set, and no checkpoint to fall back on")
	assert.False(t, isVChannelAcked(&streamingpb.BroadcastTask{}, 3),
		"an index past both arrays is not acked")
	assert.True(t, isVChannelAcked(&streamingpb.BroadcastTask{
		AckedCheckpoints: []*streamingpb.AckedCheckpoint{{TimeTick: 7}},
	}, 0), "a checkpoint with a real tick")
	assert.False(t, isVChannelAcked(&streamingpb.BroadcastTask{
		AckedCheckpoints: []*streamingpb.AckedCheckpoint{{TimeTick: 0}},
	}, 0), "TimeTick 0 is the not-yet-acked sentinel, not an ack")
	assert.False(t, isVChannelAcked(&streamingpb.BroadcastTask{
		AckedCheckpoints: []*streamingpb.AckedCheckpoint{nil},
	}, 0), "a nil checkpoint is not an ack")

	// The unacked one still blocks, so the bitmap is being read rather than
	// everything being called acked.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, task.BlockUntilVChannelAcked(ctx, "p1_1v1", nil), context.DeadlineExceeded)
}

// TestWaitVChannelsAckedReportsASlowWait drives the observability the reviewer
// asked for: a wait that outlives the threshold must report itself once, naming
// the broadcast and the vchannels still unacked, and must report its release.
// Head-of-line blocking makes this the only signal an operator gets -- a gated
// replica freezes its whole pchannel's replicate stream, which from outside
// looks like replication being broken rather than being ordered.
func TestWaitVChannelsAckedReportsASlowWait(t *testing.T) {
	bm := newWaitTestManager(t)
	defer bm.Close()

	restore := waitVChannelsAckedSlowThreshold
	waitVChannelsAckedSlowThreshold = 10 * time.Millisecond
	defer func() { waitVChannelsAckedSlowThreshold = restore }()

	const broadcastID = uint64(909)
	msg := createNewSplitShardBroadcastMsg(
		[]string{"p0_1v0", "p1_1v1", "p2_1v2"}, "p0_1v0", "p1_1v1").WithBroadcastID(broadcastID)

	// (a) Slow while the broadcast has not arrived at all: the observer reports
	// every named vchannel, since it has no task to ask.
	early := make(chan error, 1)
	earlyCtx, cancelEarly := context.WithCancel(context.Background())
	go func() { early <- bm.WaitVChannelsAcked(earlyCtx, broadcastID, []string{"p0_1v0", "p1_1v1"}) }()
	time.Sleep(80 * time.Millisecond)
	cancelEarly()
	assert.ErrorIs(t, <-early, context.Canceled)

	// (b) Slow with the task present and one vchannel still missing: released
	// once it lands, which is the Info half.
	require.NoError(t, bm.Ack(context.Background(), replicatedReplicaOf(msg, "p0_1v0", 150)))
	late := make(chan error, 1)
	go func() {
		late <- bm.WaitVChannelsAcked(context.Background(), broadcastID, []string{"p0_1v0", "p1_1v1"})
	}()
	time.Sleep(80 * time.Millisecond)
	require.NoError(t, bm.Ack(context.Background(), replicatedReplicaOf(msg, "p1_1v1", 151)))
	select {
	case err := <-late:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the reported wait was never released")
	}
}

// bitmapAcking builds the positional ack bitmap for the vchannels named, in the
// order the broadcast header actually carries them. WithBroadcast deduplicates
// the vchannels through a set, so the header's order is not the order the test
// listed them in; a bitmap written by list position marks a random vchannel and
// a wait on the intended one never returns.
func bitmapAcking(msg message.BroadcastMutableMessage, acked ...string) []byte {
	header := msg.BroadcastHeader().VChannels
	bitmap := make([]byte, len(header))
	for _, vchannel := range acked {
		idx := findIdxOfVChannel(vchannel, header)
		if idx < 0 {
			panic("vchannel " + vchannel + " is not in the broadcast header")
		}
		bitmap[idx] = 0x01
	}
	return bitmap
}
