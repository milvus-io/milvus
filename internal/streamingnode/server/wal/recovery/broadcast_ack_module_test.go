package recovery

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestBroadcastAckHoldsOwnerUntilExclusiveAndAckSucceeds(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	t.Cleanup(module.Close)
	module.retryDelay = time.Millisecond
	module.ack = func(context.Context, message.ImmutableMessage) error { return nil }
	msg := newBroadcastAckMessage(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1, PartitionIds: []int64{10}}).
		WithBody(&msgpb.CreateCollectionRequest{}))
	tracker := messageack.NewTracker(utility.WALConsumeCheckpoint{}, nil)
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

func TestBroadcastAckSubmitsExclusiveOwnerImmediately(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	t.Cleanup(module.Close)
	module.retryDelay = time.Millisecond
	module.ack = func(context.Context, message.ImmutableMessage) error { return nil }
	msg := newBroadcastAckMessage(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.CreateCollectionRequest{}))
	tracker := messageack.NewTracker(utility.WALConsumeCheckpoint{}, nil)
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
	tracker := messageack.NewTracker(utility.WALConsumeCheckpoint{}, nil)
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
	tracker := messageack.NewTracker(utility.WALConsumeCheckpoint{}, nil)
	owner := tracker.Track(msg)
	module.Accept(owner)

	first := scheduler.waitTask(t)
	require.NoError(t, first.Execute(context.Background()))
	assert.Same(t, msg, owner.Message())

	retry := scheduler.waitTaskAfter(t, 1)
	require.NoError(t, retry.Execute(context.Background()))
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
	tracker := messageack.NewTracker(utility.WALConsumeCheckpoint{}, nil)
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
	tracker := messageack.NewTracker(utility.WALConsumeCheckpoint{}, nil)
	firstOwner := tracker.Track(firstMsg)
	secondOwner := tracker.Track(secondMsg)

	module.Accept(firstOwner)
	module.Accept(secondOwner)
	firstTask := scheduler.waitTask(t)

	require.NoError(t, firstTask.Execute(context.Background()))
	assert.Empty(t, acked)
	assert.Same(t, firstMsg, firstOwner.Message())
	assert.Same(t, secondMsg, secondOwner.Message())
	assert.Len(t, scheduler.snapshot(), 1)

	retryTask := scheduler.waitTaskAfter(t, 1)
	require.NoError(t, retryTask.Execute(context.Background()))
	assert.Panics(t, func() { _ = firstOwner.Message() })
	assert.Same(t, secondMsg, secondOwner.Message())
	secondTask := scheduler.waitTaskAfter(t, 2)
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
	tracker := messageack.NewTracker(utility.WALConsumeCheckpoint{}, nil)
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
	tracker := messageack.NewTracker(utility.WALConsumeCheckpoint{}, nil)
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
	tracker := messageack.NewTracker(utility.WALConsumeCheckpoint{}, nil)
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
	module.retryDelay = time.Hour
	module.ack = func(context.Context, message.ImmutableMessage) error {
		return errors.New("coordinator unavailable")
	}
	msg := newBroadcastAckMessage(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.CreateCollectionRequest{}))
	tracker := messageack.NewTracker(utility.WALConsumeCheckpoint{}, nil)
	owner := tracker.Track(msg)
	module.Accept(owner)

	first := scheduler.waitTask(t)
	require.NoError(t, first.Execute(context.Background()))
	module.Close()

	assert.Len(t, scheduler.snapshot(), 1)
	assert.Zero(t, tracker.CompletedPoint().TimeTick)
	assert.Same(t, msg, owner.Message())
}
