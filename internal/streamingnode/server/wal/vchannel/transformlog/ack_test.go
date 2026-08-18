package transformlog

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestTransformLogFailedChunkWriteRetainsMessageRef(t *testing.T) {
	scheduler := &recordingScheduler{}
	store := &failingTransformLogStore{
		memoryStore: newMemoryStore(),
		err:         errors.New("object storage unavailable"),
	}
	transformLog := New(Config{
		VChannel: "v1",
		MaxRows:  1,
		Store:    store,
		Runtime:  moduleapi.Runtime{Scheduler: scheduler},
	})
	transformLog.SwitchIntoMetaAndData()
	owner := newRefCountedTransformMessage(newTransformLogTestDeleteMessage(t, 10))
	probe := owner.Clone()

	observeRetainedTransformMessage(transformLog, owner)
	owner.Release()
	require.Len(t, scheduler.tasks, 1)

	err := scheduler.tasks[0].Execute(context.Background())
	assert.True(t, errors.Is(err, nodescheduler.ErrDelay))
	assert.NotPanics(t, func() { _ = probe.Message().TimeTick() })

	store.err = nil
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	probe.Release()
	assert.Panics(t, func() { _ = owner.Message() })
	assert.Equal(t, uint64(10), transformLog.SnapshotMeta().GetCheckpointTimeTick())
	assert.True(t, transformLog.HasDirty())
}

func TestTransformLogDoesNotCloneUnrelatedMessage(t *testing.T) {
	transformLog := New(Config{VChannel: "v1"})
	transformLog.SwitchIntoMetaAndData()
	raw := message.CreateTestTimeTickSyncMessage(t, 1, 10, walimplstest.NewTestMessageID(10)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(11))
	owner := newRefCountedTransformMessage(raw)

	observeRetainedTransformMessage(transformLog, owner)
	exclusive := false
	owner.RegisterExclusiveCallback(func() { exclusive = true })
	assert.True(t, exclusive, "unrelated message should not retain an asynchronous handle")
	owner.Release()

	assert.Panics(t, func() { _ = owner.Message() })
}

func TestTransformLogRequestPersistThrough(t *testing.T) {
	scheduler := &recordingScheduler{}
	transformLog := New(Config{
		VChannel: "v1",
		MaxRows:  100,
		Store:    newMemoryStore(),
		Runtime:  moduleapi.Runtime{Scheduler: scheduler},
	})
	transformLog.SwitchIntoMetaAndData()
	first := newRefCountedTransformMessage(newTransformLogTestDeleteMessage(t, 10))
	second := newRefCountedTransformMessage(newTransformLogTestDeleteMessage(t, 11))
	observeRetainedTransformMessage(transformLog, first)
	observeRetainedTransformMessage(transformLog, second)
	first.Release()
	second.Release()
	require.Empty(t, scheduler.tasks)

	assert.False(t, transformLog.RequestPersistThrough(9))
	require.Empty(t, scheduler.tasks)
	require.True(t, transformLog.RequestPersistThrough(10))
	require.Len(t, scheduler.tasks, 1)
	task, ok := scheduler.tasks[0].(*transformFlushTask)
	require.True(t, ok)
	assert.Equal(t, uint64(11), task.timetick)

	assert.False(t, transformLog.RequestPersistThrough(11))
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, task.Execute(context.Background()))
	assert.Panics(t, func() { _ = first.Message() })
	assert.Panics(t, func() { _ = second.Message() })
}

func TestTransformLogBarrierRefCompletesWithoutMaterialization(t *testing.T) {
	scheduler := &recordingScheduler{}
	materializer := &recordingMaterializer{}
	transformLog := New(Config{
		VChannel:     "v1",
		MaxRows:      100,
		Store:        newMemoryStore(),
		Materializer: materializer,
		Runtime:      moduleapi.Runtime{Scheduler: scheduler},
	})
	transformLog.SwitchIntoMetaAndData()
	deleteMessage := newRefCountedTransformMessage(newTransformLogTestDeleteMessage(t, 10))
	barrierMessage := newRefCountedTransformMessage(newTransformLogTestManualFlushMessage(t, 20))

	observeRetainedTransformMessage(transformLog, deleteMessage)
	deleteMessage.Release()
	observeRetainedTransformMessage(transformLog, barrierMessage)
	barrierMessage.Release()
	require.Len(t, scheduler.tasks, 2)
	assert.IsType(t, &transformFlushTask{}, scheduler.tasks[0])
	assert.IsType(t, &transformMaterializeTask{}, scheduler.tasks[1])

	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.Panics(t, func() { _ = deleteMessage.Message() })
	assert.Panics(t, func() { _ = barrierMessage.Message() })
	assert.Empty(t, materializer.requests)
}

func TestTransformLogMultiChunkFlushReleasesRefsByDurablePrefix(t *testing.T) {
	scheduler := &recordingScheduler{}
	transformLog := New(Config{
		VChannel: "v1",
		MaxRows:  1,
		Store:    newMemoryStore(),
		Runtime:  moduleapi.Runtime{Scheduler: scheduler},
	})
	transformLog.SwitchIntoMetaAndData()
	first := newRefCountedTransformMessage(newTransformLogTestDeleteMessage(t, 10))
	second := newRefCountedTransformMessage(newTransformLogTestDeleteMessage(t, 11))
	barrier := newRefCountedTransformMessage(newTransformLogTestManualFlushMessage(t, 20))
	firstProbe := first.Clone()
	secondProbe := second.Clone()
	barrierProbe := barrier.Clone()

	observeRetainedTransformMessage(transformLog, first)
	first.Release()
	observeRetainedTransformMessage(transformLog, second)
	second.Release()
	observeRetainedTransformMessage(transformLog, barrier)
	barrier.Release()
	require.GreaterOrEqual(t, len(scheduler.tasks), 3)

	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.NotPanics(t, func() { _ = firstProbe.Message() })
	assert.NotPanics(t, func() { _ = secondProbe.Message().TimeTick() })
	assert.NotPanics(t, func() { _ = barrierProbe.Message().TimeTick() })
	assert.Equal(t, uint64(10), transformLog.SnapshotMeta().GetCheckpointTimeTick())

	require.NoError(t, scheduler.tasks[1].Execute(context.Background()))
	assert.NotPanics(t, func() { _ = secondProbe.Message() })
	assert.NotPanics(t, func() { _ = barrierProbe.Message() })
	firstProbe.Release()
	secondProbe.Release()
	barrierProbe.Release()
	assert.Panics(t, func() { _ = second.Message() })
	assert.Panics(t, func() { _ = barrier.Message() })
	assert.Equal(t, uint64(11), transformLog.SnapshotMeta().GetCheckpointTimeTick())
}

func TestTransformLogRegistersBarrierRefBeforeConcurrentFlushCommit(t *testing.T) {
	store := &blockingTransformLogWriteStore{
		memoryStore:  newMemoryStore(),
		writeStarted: make(chan struct{}),
		releaseWrite: make(chan struct{}),
	}
	transformLog := New(Config{
		VChannel: "v1",
		MaxRows:  100,
		Store:    store,
	})
	transformLog.SwitchIntoMetaAndData()
	deleteMessage := newRefCountedTransformMessage(newTransformLogTestDeleteMessage(t, 10))
	observeRetainedTransformMessage(transformLog, deleteMessage)
	deleteMessage.Release()

	type flushOutcome struct {
		result flushResult
		err    error
	}
	flushDone := make(chan flushOutcome, 1)
	go func() {
		result, err := transformLog.flush(context.Background(), flushOption{TargetTimeTick: 10})
		flushDone <- flushOutcome{result: result, err: err}
	}()
	<-store.writeStarted

	barrierController := newRefCountedTransformMessage(newTransformLogTestManualFlushMessage(t, 20))
	observeRetainedTransformMessage(transformLog, barrierController)

	close(store.releaseWrite)
	var flush flushResult
	select {
	case outcome := <-flushDone:
		require.NoError(t, outcome.err)
		flush = outcome.result
		releaseMessages(flush.CompletedMessages)
	case <-time.After(100 * time.Millisecond):
	}
	if !flush.Started {
		outcome := <-flushDone
		require.NoError(t, outcome.err)
		flush = outcome.result
		releaseMessages(flush.CompletedMessages)
	}
	barrierController.Release()

	assert.Panics(t, func() { _ = deleteMessage.Message() })
	assert.Panics(t, func() { _ = barrierController.Message() })
}

type failingTransformLogStore struct {
	*memoryStore
	err error
}

func newRefCountedTransformMessage(raw message.ImmutableMessage) message.OwnedImmutableMessage {
	return message.NewOwnedImmutableMessage(raw, nil)
}

func observeRetainedTransformMessage(transformLog *TransformLog, owner message.OwnedImmutableMessage) {
	dispatch := owner.Clone()
	transformLog.ObserveMessage(context.Background(), dispatch)
	dispatch.Release()
}

type blockingTransformLogWriteStore struct {
	*memoryStore
	writeStarted chan struct{}
	releaseWrite chan struct{}
}

func (s *blockingTransformLogWriteStore) WriteTransformLogChunk(
	ctx context.Context,
	vchannel string,
	chunk *streamingpb.TransformLogChunk,
) error {
	close(s.writeStarted)
	select {
	case <-s.releaseWrite:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.memoryStore.WriteTransformLogChunk(ctx, vchannel, chunk)
}

func (s *failingTransformLogStore) WriteTransformLogChunk(
	ctx context.Context,
	vchannel string,
	chunk *streamingpb.TransformLogChunk,
) error {
	if s.err != nil {
		return s.err
	}
	return s.memoryStore.WriteTransformLogChunk(ctx, vchannel, chunk)
}
