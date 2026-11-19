package recovery

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestBroadcastAckLegacyImportRetriesBeforeAck(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	t.Cleanup(module.Close)
	var events []string
	attempt := 0
	patch := mockey.Mock(commitLegacyImportVChannel).To(func(_ context.Context, req *datapb.HandleCommitVchannelRequest) error {
		require.EqualValues(t, 42, req.GetJobId())
		require.Equal(t, "v1", req.GetVchannel())
		require.EqualValues(t, 100, req.GetCommitTimestamp())
		events = append(events, "rpc")
		attempt++
		if attempt == 1 {
			return context.DeadlineExceeded
		}
		return nil
	}).Build()
	defer patch.UnPatch()
	module.ack = func(context.Context, message.ImmutableMessage) error {
		events = append(events, "ack")
		return nil
	}
	msg := newBroadcastAckMessageWith(t, message.NewCommitImportMessageBuilderV2().
		WithHeader(&message.CommitImportMessageHeader{JobId: 42}).
		WithBody(&message.CommitImportMessageBody{}).WithBroadcast([]string{"v1"}), 1, 100)
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	owner := tracker.Track(msg)
	module.Accept(owner)
	task := scheduler.waitTask(t)
	require.ErrorIs(t, task.Execute(context.Background()), nodescheduler.ErrDelay)
	require.Equal(t, []string{"rpc"}, events)
	require.Zero(t, tracker.CompletedPoint().TimeTick)
	require.Same(t, msg, owner.Message())
	require.NoError(t, task.Execute(context.Background()))
	require.Equal(t, []string{"rpc", "rpc", "ack"}, events)
	require.EqualValues(t, 100, tracker.CompletedPoint().TimeTick)
}

func TestBroadcastAckSkipsLegacyRPCForNewImportAndControl(t *testing.T) {
	patch := mockey.Mock(commitLegacyImportVChannel).Return(context.DeadlineExceeded).Build()
	defer patch.UnPatch()
	for _, test := range []struct {
		channel     string
		coordinator bool
	}{{"v1", true}, {funcutil.GetControlChannel("test"), false}} {
		msg := newBroadcastAckMessage(t, message.NewCommitImportMessageBuilderV2().
			WithHeader(&message.CommitImportMessageHeader{JobId: 42, CommitByCoordinator: test.coordinator}).
			WithBody(&message.CommitImportMessageBody{}).WithBroadcast([]string{test.channel}))
		require.NoError(t, ackLegacyCommitImport(context.Background(), msg))
	}
	require.Zero(t, patch.Times())
}

func TestBroadcastAckHoldsOwnerUntilExclusiveAndAckSucceeds(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	t.Cleanup(module.Close)
	module.ack = func(context.Context, message.ImmutableMessage) error { return nil }
	msg := newBroadcastAckMessage(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1, PartitionIds: []int64{10}}).
		WithBody(&msgpb.CreateCollectionRequest{}))
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	owner := tracker.Track(msg)
	other := owner.Clone()

	module.Accept(owner)

	require.Empty(t, scheduler.snapshot())
	assert.Same(t, msg, owner.Message())
	assert.Zero(t, tracker.CompletedPoint().TimeTick)

	other.Release()
	task := scheduler.waitTask(t)
	require.NoError(t, task.Execute(context.Background()))
	assert.Panics(t, func() { _ = owner.Message() })
	assert.Equal(t, msg.TimeTick(), tracker.CompletedPoint().TimeTick)
}

func TestBroadcastAckPoisonKeepsConflictingTasksBlocked(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	t.Cleanup(module.Close)
	var acked []uint64
	module.ack = func(_ context.Context, msg message.ImmutableMessage) error {
		acked = append(acked, msg.TimeTick())
		return nil
	}
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	makeOwner := func(id, tt uint64, collection string) message.OwnedImmutableMessage {
		return tracker.Track(newBroadcastAckMessageWith(t, message.NewCreateCollectionMessageBuilderV1().
			WithBroadcast([]string{"v1"}).
			WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1}).
			WithBody(&msgpb.CreateCollectionRequest{}), id, tt,
			message.NewExclusiveCollectionNameResourceKey("db", collection)))
	}
	first := makeOwner(1, 10, "c1")
	failed := first.Clone()
	module.Accept(first)
	module.Accept(makeOwner(2, 20, "c1"))
	module.Accept(makeOwner(3, 30, "c2"))
	failed.PoisonedRelease()
	module.dispatchReadyTasks()
	require.Panics(t, func() { first.Message() }, "poison releases payload memory")
	task := scheduler.waitTask(t)
	require.NoError(t, task.Execute(context.Background()))
	module.dispatchReadyTasks()
	require.Len(t, scheduler.snapshot(), 1)
	require.Equal(t, []uint64{30}, acked, "only the independent broadcast may succeed")
	require.Zero(t, tracker.CompletedPoint().TimeTick)
}

func TestBroadcastAckSubmitsExclusiveOwnerImmediately(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	t.Cleanup(module.Close)
	module.ack = func(context.Context, message.ImmutableMessage) error { return nil }
	msg := newBroadcastAckMessage(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.CreateCollectionRequest{}))
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	owner := tracker.Track(msg)

	module.Accept(owner)

	assert.Same(t, msg, owner.Message())
	task := scheduler.waitTask(t)
	require.NoError(t, task.Execute(context.Background()))
	assert.Panics(t, func() { _ = owner.Message() })
}

func TestBroadcastAckReleasesNonBroadcastOwnerImmediately(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	t.Cleanup(module.Close)
	raw := message.CreateTestTimeTickSyncMessage(t, 1, 20, walimplstest.NewTestMessageID(10)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(11))
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	owner := tracker.Track(raw)

	module.Accept(owner)

	assert.Equal(t, raw.TimeTick(), tracker.CompletedPoint().TimeTick)
	assert.Empty(t, scheduler.snapshot())
	assert.Panics(t, func() { _ = owner.Message() })
}

func TestBroadcastAckRetriesSameTaskAfterFailure(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	t.Cleanup(module.Close)
	attempts := 0
	module.ack = func(context.Context, message.ImmutableMessage) error {
		attempts++
		if attempts == 1 {
			return errors.New("coordinator unavailable")
		}
		return nil
	}
	msg := newBroadcastAckMessage(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1, PartitionIds: []int64{10}}).
		WithBody(&msgpb.CreateCollectionRequest{}))
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	owner := tracker.Track(msg)
	module.Accept(owner)

	first := scheduler.waitTask(t)
	require.ErrorIs(t, first.Execute(context.Background()), nodescheduler.ErrDelay)
	assert.Same(t, msg, owner.Message())

	module.dispatchReadyTasks()
	require.Len(t, scheduler.snapshot(), 1, "retry stays owned by the same scheduler submission")
	require.NoError(t, first.Execute(context.Background()))
	assert.Equal(t, 2, attempts)
	assert.Panics(t, func() { _ = owner.Message() })
	assert.Equal(t, msg.TimeTick(), tracker.CompletedPoint().TimeTick)
}

func TestBroadcastAckAllowsReadyNonConflictingTaskToPass(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	t.Cleanup(module.Close)
	module.ack = func(context.Context, message.ImmutableMessage) error { return nil }
	firstMsg := newBroadcastAckMessageWith(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1, PartitionIds: []int64{10}}).
		WithBody(&msgpb.CreateCollectionRequest{}), 1, 10,
		message.NewExclusiveCollectionNameResourceKey("db", "c1"))
	secondMsg := newBroadcastAckMessageWith(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v2"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 2, PartitionIds: []int64{20}}).
		WithBody(&msgpb.CreateCollectionRequest{}), 2, 20,
		message.NewExclusiveCollectionNameResourceKey("db", "c2"))
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	firstOwner := tracker.Track(firstMsg)
	firstConsumer := firstOwner.Clone()
	secondOwner := tracker.Track(secondMsg)

	module.Accept(firstOwner)
	module.Accept(secondOwner)
	secondTask := scheduler.waitTask(t)
	require.NoError(t, secondTask.Execute(context.Background()))
	assert.Same(t, firstMsg, firstOwner.Message())
	assert.Panics(t, func() { _ = secondOwner.Message() })

	firstConsumer.Release()
	firstTask := scheduler.waitTaskAfter(t, 1)
	require.NoError(t, firstTask.Execute(context.Background()))
	assert.Panics(t, func() { _ = firstOwner.Message() })
}

func TestBroadcastAckKeepsConflictingTasksOrderedAcrossRetry(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	t.Cleanup(module.Close)
	var acked []uint64
	failFirst := true
	module.ack = func(_ context.Context, msg message.ImmutableMessage) error {
		if msg.TimeTick() == 10 && failFirst {
			failFirst = false
			return errors.New("coordinator unavailable")
		}
		acked = append(acked, msg.TimeTick())
		return nil
	}
	key := message.NewExclusiveCollectionNameResourceKey("db", "collection")
	firstMsg := newBroadcastAckMessageWith(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1, PartitionIds: []int64{10}}).
		WithBody(&msgpb.CreateCollectionRequest{}), 1, 10, key)
	secondMsg := newBroadcastAckMessageWith(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v2"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 2, PartitionIds: []int64{20}}).
		WithBody(&msgpb.CreateCollectionRequest{}), 2, 20, key)
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	firstOwner := tracker.Track(firstMsg)
	secondOwner := tracker.Track(secondMsg)

	module.Accept(firstOwner)
	module.Accept(secondOwner)
	firstTask := scheduler.waitTask(t)

	require.ErrorIs(t, firstTask.Execute(context.Background()), nodescheduler.ErrDelay)
	assert.Empty(t, acked)
	assert.Same(t, firstMsg, firstOwner.Message())
	assert.Same(t, secondMsg, secondOwner.Message())
	assert.Len(t, scheduler.snapshot(), 1)

	module.dispatchReadyTasks()
	require.Len(t, scheduler.snapshot(), 1)
	require.NoError(t, firstTask.Execute(context.Background()))
	assert.Panics(t, func() { _ = firstOwner.Message() })
	assert.Same(t, secondMsg, secondOwner.Message())
	secondTask := scheduler.waitTaskAfter(t, 1)
	require.NoError(t, secondTask.Execute(context.Background()))
	assert.Panics(t, func() { _ = secondOwner.Message() })
	assert.Equal(t, []uint64{10, 20}, acked)
}

func TestBroadcastAckSharedTasksDoNotConflict(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	t.Cleanup(module.Close)
	module.ack = func(context.Context, message.ImmutableMessage) error { return nil }
	key := message.NewSharedCollectionNameResourceKey("db", "collection")
	firstMsg := newBroadcastAckMessageWith(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.CreateCollectionRequest{}), 1, 10, key)
	secondMsg := newBroadcastAckMessageWith(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v2"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 2}).
		WithBody(&msgpb.CreateCollectionRequest{}), 2, 20, key)
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	firstOwner := tracker.Track(firstMsg)
	firstConsumer := firstOwner.Clone()
	secondOwner := tracker.Track(secondMsg)

	module.Accept(firstOwner)
	module.Accept(secondOwner)
	secondTask := scheduler.waitTask(t)
	require.NoError(t, secondTask.Execute(context.Background()))

	firstConsumer.Release()
	firstTask := scheduler.waitTaskAfter(t, 1)
	require.NoError(t, firstTask.Execute(context.Background()))
}

func TestBroadcastAckExclusiveClusterPreservesBarrierOrder(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	t.Cleanup(module.Close)
	module.ack = func(context.Context, message.ImmutableMessage) error { return nil }
	firstMsg := newBroadcastAckMessageWith(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.CreateCollectionRequest{}), 1, 10,
		message.NewSharedClusterResourceKey())
	barrierMsg := newBroadcastAckMessageWith(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v2"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 2}).
		WithBody(&msgpb.CreateCollectionRequest{}), 2, 20,
		message.NewExclusiveClusterResourceKey())
	lastMsg := newBroadcastAckMessageWith(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v3"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 3}).
		WithBody(&msgpb.CreateCollectionRequest{}), 3, 30,
		message.NewSharedClusterResourceKey())
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	firstOwner := tracker.Track(firstMsg)
	firstConsumer := firstOwner.Clone()
	barrierOwner := tracker.Track(barrierMsg)
	lastOwner := tracker.Track(lastMsg)

	module.Accept(firstOwner)
	module.Accept(barrierOwner)
	module.Accept(lastOwner)
	require.Never(t, func() bool { return len(scheduler.snapshot()) != 0 }, 50*time.Millisecond, time.Millisecond)

	firstConsumer.Release()
	firstTask := scheduler.waitTask(t)
	require.NoError(t, firstTask.Execute(context.Background()))
	barrierTask := scheduler.waitTaskAfter(t, 1)
	require.Never(t, func() bool { return len(scheduler.snapshot()) > 2 }, 50*time.Millisecond, time.Millisecond)

	require.NoError(t, barrierTask.Execute(context.Background()))
	lastTask := scheduler.waitTaskAfter(t, 2)
	require.NoError(t, lastTask.Execute(context.Background()))
}

func TestNormalizeBroadcastAckResourceKeys(t *testing.T) {
	collectionKey := message.NewExclusiveCollectionNameResourceKey("db", "collection")

	keys := normalizeBroadcastAckResourceKeys(nil)
	require.Equal(t, []message.ResourceKey{message.NewExclusiveClusterResourceKey()}, keys)

	keys = normalizeBroadcastAckResourceKeys([]message.ResourceKey{collectionKey})
	assert.ElementsMatch(t, []message.ResourceKey{
		collectionKey,
		message.NewSharedClusterResourceKey(),
	}, keys)

	keys = normalizeBroadcastAckResourceKeys([]message.ResourceKey{
		collectionKey,
		message.NewExclusiveClusterResourceKey(),
	})
	assert.ElementsMatch(t, []message.ResourceKey{
		collectionKey,
		message.NewExclusiveClusterResourceKey(),
	}, keys)
}

func TestBroadcastAckCloseCancelsPendingConsumerWait(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	t.Cleanup(module.Close)
	msg := newBroadcastAckMessage(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.CreateCollectionRequest{}))
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	owner := tracker.Track(msg)
	consumer := owner.Clone()

	module.Accept(owner)
	module.Close()
	consumer.Release()

	assert.Empty(t, scheduler.snapshot())
	assert.Zero(t, tracker.CompletedPoint().TimeTick)
	assert.Same(t, msg, owner.Message())
}

func TestBroadcastAckCloseCancelsPendingRetry(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	t.Cleanup(module.Close)
	module.ack = func(context.Context, message.ImmutableMessage) error {
		return errors.New("coordinator unavailable")
	}
	msg := newBroadcastAckMessage(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.CreateCollectionRequest{}))
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	owner := tracker.Track(msg)
	module.Accept(owner)

	first := scheduler.waitTask(t)
	require.ErrorIs(t, first.Execute(context.Background()), nodescheduler.ErrDelay)
	module.Close()

	assert.Len(t, scheduler.snapshot(), 1)
	assert.Zero(t, tracker.CompletedPoint().TimeTick)
	assert.Same(t, msg, owner.Message())
}

func TestBroadcastAckTimeoutYieldsWorkerAndRetries(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	scoped := newScopedTaskScheduler(scheduler)
	defer scoped.Close()
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scoped})
	defer module.Close()
	firstAttempt, acked, otherRan := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var attempts atomic.Int32
	module.ack = func(ctx context.Context, _ message.ImmutableMessage) error {
		if attempts.Add(1) == 1 {
			close(firstAttempt)
			<-ctx.Done()
			return ctx.Err()
		}
		close(acked)
		return nil
	}
	msg := newBroadcastAckMessage(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.CreateCollectionRequest{}))
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	owner := tracker.Track(msg)
	module.Accept(owner)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	select {
	case <-firstAttempt:
	case <-ctx.Done():
		t.Fatal("ack did not start")
	}
	scheduler.Submit(nodeschedulerTaskFunc(func(context.Context) error { close(otherRan); return nil }))
	select {
	case <-otherRan:
	case <-ctx.Done():
		t.Fatal("timed-out ACK did not release the worker")
	}
	select {
	case <-acked:
	case <-ctx.Done():
		t.Fatal("scheduler did not retry ACK")
	}
	require.NoError(t, scoped.WaitIdle(ctx))
	require.EqualValues(t, 2, attempts.Load())
	require.Equal(t, msg.TimeTick(), tracker.CompletedPoint().TimeTick)
}

func TestBroadcastAckModuleCloseCancelsAttempt(t *testing.T) {
	module := newBroadcastAckModule(moduleapi.Runtime{})
	defer module.Close()
	started := make(chan struct{})
	module.ack = func(ctx context.Context, _ message.ImmutableMessage) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	msg := newBroadcastAckMessage(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.CreateCollectionRequest{}))
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	owner := tracker.Track(msg)
	task := &broadcastAckTask{module: module, owner: owner}
	done := make(chan error, 1)
	go func() { done <- task.Execute(t.Context()) }()
	<-started
	module.Close()
	select {
	case err := <-done:
		require.ErrorIs(t, err, nodescheduler.ErrDelay)
	case <-time.After(time.Second):
		t.Fatal("module close did not cancel attempt")
	}
	require.ErrorIs(t, task.Execute(t.Context()), context.Canceled)
	require.Same(t, msg, owner.Message())
	require.Zero(t, tracker.CompletedPoint().TimeTick)
}

func TestBroadcastAckUsesReservedWorker(t *testing.T) {
	scheduler := nodescheduler.New(2)
	defer scheduler.Close()
	scoped := newScopedTaskScheduler(scheduler)
	defer scoped.Close()
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scoped})
	defer module.Close()
	busy := make(chan struct{})
	scheduler.Submit(nodeschedulerTaskFunc(func(ctx context.Context) error {
		close(busy)
		<-ctx.Done()
		return nil
	}))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	select {
	case <-busy:
	case <-ctx.Done():
		t.Fatal("data worker did not start")
	}
	acked := make(chan struct{})
	module.ack = func(context.Context, message.ImmutableMessage) error {
		close(acked)
		return nil
	}
	msg := newBroadcastAckMessage(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.CreateCollectionRequest{}))
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	module.Accept(tracker.Track(msg))
	select {
	case <-acked:
	case <-ctx.Done():
		t.Fatal("ACK did not use reserved capacity while data worker remained busy")
	}
	require.NoError(t, scoped.WaitIdle(ctx))
	require.Equal(t, msg.TimeTick(), tracker.CompletedPoint().TimeTick)
}

func TestBroadcastAckLegacyImportOutlivesAckTimeout(t *testing.T) {
	module := newBroadcastAckModule(moduleapi.Runtime{})
	defer module.Close()
	patch := mockey.Mock(commitLegacyImportVChannel).To(func(ctx context.Context, _ *datapb.HandleCommitVchannelRequest) error {
		_, hasDeadline := ctx.Deadline()
		require.False(t, hasDeadline, "legacy completion must not inherit the ACK budget")
		timer := time.NewTimer(2 * broadcastAckAttemptTimeout)
		defer timer.Stop()
		select {
		case <-timer.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}).Build()
	defer patch.UnPatch()
	acked := false
	module.ack = func(ctx context.Context, _ message.ImmutableMessage) error {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.LessOrEqual(t, time.Until(deadline), broadcastAckAttemptTimeout)
		require.NoError(t, ctx.Err(), "ACK must receive a fresh budget after legacy completion")
		acked = true
		return nil
	}
	msg := newBroadcastAckMessageWith(t, message.NewCommitImportMessageBuilderV2().
		WithHeader(&message.CommitImportMessageHeader{JobId: 42}).
		WithBody(&message.CommitImportMessageBody{}).WithBroadcast([]string{"v1"}), 1, 100)
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	owner := tracker.Track(msg)
	task := &broadcastAckTask{module: module, owner: owner}
	require.NoError(t, task.Execute(context.Background()))
	require.True(t, acked)
	require.Equal(t, 1, patch.Times())
	require.EqualValues(t, 100, tracker.CompletedPoint().TimeTick)
}

func TestBroadcastAckLegacyImportRemainsCancelable(t *testing.T) {
	for _, mode := range []string{"task-cancel", "module-close"} {
		t.Run(mode, func(t *testing.T) {
			module := newBroadcastAckModule(moduleapi.Runtime{})
			defer module.Close()
			entered := make(chan context.Context, 1)
			patch := mockey.Mock(commitLegacyImportVChannel).To(func(ctx context.Context, _ *datapb.HandleCommitVchannelRequest) error {
				entered <- ctx
				<-ctx.Done()
				return ctx.Err()
			}).Build()
			defer patch.UnPatch()
			var acked atomic.Bool
			module.ack = func(context.Context, message.ImmutableMessage) error {
				acked.Store(true)
				return nil
			}
			msg := newBroadcastAckMessageWith(t, message.NewCommitImportMessageBuilderV2().
				WithHeader(&message.CommitImportMessageHeader{JobId: 42}).
				WithBody(&message.CommitImportMessageBody{}).WithBroadcast([]string{"v1"}), 1, 100)
			tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
			owner := tracker.Track(msg)
			task := &broadcastAckTask{module: module, owner: owner}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- task.Execute(ctx) }()
			select {
			case legacyCtx := <-entered:
				_, hasDeadline := legacyCtx.Deadline()
				require.False(t, hasDeadline)
			case <-time.After(5 * time.Second):
				t.Fatal("legacy completion did not start")
			}
			require.Same(t, msg, owner.Message())
			require.Zero(t, tracker.CompletedPoint().TimeTick)
			if mode == "task-cancel" {
				cancel()
			} else {
				module.Close()
			}
			select {
			case err := <-done:
				require.ErrorIs(t, err, nodescheduler.ErrDelay)
			case <-time.After(5 * time.Second):
				t.Fatal("legacy completion ignored cancellation")
			}
			require.False(t, acked.Load())
			require.Same(t, msg, owner.Message())
			require.Zero(t, tracker.CompletedPoint().TimeTick)
		})
	}
}
