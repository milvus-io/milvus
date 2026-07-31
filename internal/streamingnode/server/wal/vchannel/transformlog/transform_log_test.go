package transformlog

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestReadSyncsInitialFrontierBeforeFutureUpdates(t *testing.T) {
	transformLog := New(Config{VChannel: "v1"})
	manager := NewStreamManager("p1")
	manager.Register("v1", transformLog)
	for timeTick := uint64(1); timeTick <= 20; timeTick++ {
		require.True(t, transformLog.syncUp(timeTick).Appended)
	}

	stream, err := manager.AcquireStream(context.Background(), "p1")
	require.NoError(t, err)
	defer stream.Close()
	handler := newRecordingStreamHandler()
	_, err = stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{
		VChannel:           "v1",
		StartAfterTimeTick: 0,
		Handler:            handler,
	})
	require.NoError(t, err)

	syncUpEvent := recvStreamEvent(t, handler.events)
	require.NotNil(t, syncUpEvent.SyncUp)
	assert.Equal(t, uint64(20), syncUpEvent.SyncUp.TimeTick)
	require.True(t, transformLog.syncUp(21).Appended)

	liveEvent := recvStreamEvent(t, handler.events)
	require.NotNil(t, liveEvent.SyncUp)
	assert.Equal(t, uint64(21), liveEvent.SyncUp.TimeTick)
}

func TestObserveMessageOwnsAppendFlushAndMaterializeScheduling(t *testing.T) {
	scheduler := &recordingScheduler{}
	transformLog := New(Config{
		VChannel:     "v1",
		Store:        newMemoryStore(),
		Materializer: &recordingMaterializer{},
		Runtime:      moduleapi.Runtime{Scheduler: scheduler},
	})
	transformLog.SwitchIntoMetaAndData()

	deleteResult := transformLog.ObserveMessage(context.Background(), newTransformLogTestDeleteMessage(t, 10))
	require.NotNil(t, deleteResult.Data)
	assert.Equal(t, uint64(0), deleteResult.Data.TimeTick())
	assert.Empty(t, scheduler.tasks)

	flushResult := transformLog.ObserveMessage(context.Background(), newTransformLogTestManualFlushMessage(t, 20))
	require.NotNil(t, flushResult.Data)
	require.Len(t, scheduler.tasks, 2)
	assert.IsType(t, &transformFlushTask{}, scheduler.tasks[0])
	assert.IsType(t, &transformMaterializeTask{}, scheduler.tasks[1])
}

func TestEmptyBarrierAdvancesInMemoryFrontierWithoutDirtySnapshot(t *testing.T) {
	transformLog := New(Config{VChannel: "v1"})
	transformLog.SwitchIntoMetaAndData()

	result := transformLog.ObserveMessage(context.Background(), newTransformLogTestManualFlushMessage(t, 20))
	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(20), result.Data.TimeTick())
	assert.Zero(t, transformLog.SnapshotMeta().GetCheckpointTimeTick())
	assert.Nil(t, transformLog.ConsumeDirtyAndGetSnapshot())
}

func TestSyncUpWithoutStreamNotifierDoesNotAllocate(t *testing.T) {
	transformLog := New(Config{VChannel: "v1"})
	timeTick := uint64(1)
	allocs := testing.AllocsPerRun(100, func() {
		if !transformLog.syncUp(timeTick).Appended {
			panic("sync up did not advance")
		}
		timeTick++
	})
	assert.Zero(t, allocs)
}

func TestStreamNotifierReceivesVChannelWithoutPerLogCallback(t *testing.T) {
	notifier := &recordingTransformLogNotifier{}
	transformLog := New(Config{VChannel: "v1"})
	transformLog.setStreamNotifier(notifier)

	require.True(t, transformLog.syncUp(1).Appended)
	assert.Equal(t, []string{"v1"}, notifier.vchannels)
}

type recordingTransformLogNotifier struct {
	vchannels []string
}

func (n *recordingTransformLogNotifier) notify(vchannel string) {
	n.vchannels = append(n.vchannels, vchannel)
}

func TestReadStopsAtEndTimeTick(t *testing.T) {
	transformLog := New(Config{VChannel: "v1"})
	manager := NewStreamManager("p1")
	manager.Register("v1", transformLog)
	require.True(t, transformLog.append(newTransformLogTestDeleteMessage(t, 10), appendOption{}).Appended)
	require.True(t, transformLog.append(newTransformLogTestDeleteMessage(t, 20), appendOption{}).Appended)
	require.True(t, transformLog.append(newTransformLogTestDeleteMessage(t, 30), appendOption{}).Appended)
	require.True(t, transformLog.syncUp(40).Appended)

	stream, err := manager.AcquireStream(context.Background(), "p1")
	require.NoError(t, err)
	defer stream.Close()
	handler := newRecordingStreamHandler()
	_, err = stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{
		VChannel:           "v1",
		StartAfterTimeTick: 1,
		EndTimeTick:        20,
		Handler:            handler,
	})
	require.NoError(t, err)

	first := recvStreamEvent(t, handler.events)
	require.NotNil(t, first.Entry)
	assert.Equal(t, uint64(10), first.Entry.GetTimeTick())
	second := recvStreamEvent(t, handler.events)
	require.NotNil(t, second.Entry)
	assert.Equal(t, uint64(20), second.Entry.GetTimeTick())
	syncUp := recvStreamEvent(t, handler.events)
	require.NotNil(t, syncUp.SyncUp)
	assert.Equal(t, uint64(20), syncUp.SyncUp.TimeTick)

	require.Eventually(t, func() bool {
		select {
		case <-handler.closed:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	requireNoStreamEvent(t, handler.events)
}

func TestNewKeepsRecoveredChunksColdUntilRead(t *testing.T) {
	store := newMemoryStore()
	require.NoError(t, store.WriteTransformLogChunk(context.Background(), "v1", &streamingpb.TransformLogChunk{
		ChunkId: 0,
		Entries: []*streamingpb.TransformLogEntry{
			testTransformLogDeleteEntry(10, 1),
		},
	}))
	store.resetReadCount()
	transformLog := New(Config{
		VChannel: "v1",
		Store:    store,
		Meta: &streamingpb.VChannelTransformLogMeta{
			CheckpointTimeTick: 10,
			NextChunkId:        1,
		},
	})
	manager := NewStreamManager("p1")
	manager.Register("v1", transformLog)

	assert.Equal(t, 0, store.readCount("v1", 0))

	stream, err := manager.AcquireStream(context.Background(), "p1")
	require.NoError(t, err)
	defer stream.Close()
	handler := newRecordingStreamHandler()
	_, err = stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{
		VChannel:           "v1",
		StartAfterTimeTick: 0,
		Handler:            handler,
	})
	require.NoError(t, err)

	event := recvStreamEvent(t, handler.events)
	require.NotNil(t, event.Entry)
	assert.Equal(t, uint64(10), event.Entry.GetTimeTick())
	assert.Equal(t, 1, store.readCount("v1", 0))
}

func TestMaterializeLoadsRecoveredColdChunk(t *testing.T) {
	store := newMemoryStore()
	require.NoError(t, store.WriteTransformLogChunk(context.Background(), "v1", &streamingpb.TransformLogChunk{
		ChunkId: 0,
		Entries: []*streamingpb.TransformLogEntry{
			testTransformLogDeleteEntry(10, 1, 2),
		},
	}))
	store.resetReadCount()
	materializer := &recordingMaterializer{}
	transformLog := New(Config{
		VChannel:     "v1",
		Store:        store,
		Materializer: materializer,
		Meta: &streamingpb.VChannelTransformLogMeta{
			CheckpointTimeTick: 10,
			NextChunkId:        1,
		},
	})

	result, err := transformLog.materialize(context.Background(), materializeOption{TargetTimeTick: 10})
	require.NoError(t, err)
	assert.True(t, result.HasMaterializedSegments)
	assert.Equal(t, uint64(2), result.MaterializedRows)
	assert.Equal(t, 1, store.readCount("v1", 0))
	require.Len(t, materializer.requests, 1)
	require.Len(t, materializer.requests[0].Entries, 1)
}

func TestTruncateLoadsRecoveredColdChunkToAdvanceFirstChunk(t *testing.T) {
	store := newMemoryStore()
	require.NoError(t, store.WriteTransformLogChunk(context.Background(), "v1", &streamingpb.TransformLogChunk{
		ChunkId: 0,
		Entries: []*streamingpb.TransformLogEntry{
			testTransformLogDeleteEntry(10, 1),
		},
	}))
	store.resetReadCount()
	transformLog := New(Config{
		VChannel: "v1",
		Store:    store,
		Meta: &streamingpb.VChannelTransformLogMeta{
			CheckpointTimeTick: 10,
			NextChunkId:        1,
		},
	})

	result := transformLog.truncate(truncateOption{TimeTick: 10})
	assert.True(t, result.Changed)
	assert.Equal(t, uint64(1), transformLog.SnapshotMeta().GetFirstChunkId())
	assert.Equal(t, 1, store.readCount("v1", 0))
}

func TestFlushWhileScannerDrainsDoesNotDuplicateEntries(t *testing.T) {
	transformLog := New(Config{VChannel: "v1", Store: newMemoryStore()})
	manager := NewStreamManager("p1")
	manager.Register("v1", transformLog)
	for timeTick := uint64(1); timeTick <= 20; timeTick++ {
		require.True(t, transformLog.append(newTransformLogTestDeleteMessage(t, timeTick), appendOption{}).Appended)
	}

	stream, err := manager.AcquireStream(context.Background(), "p1")
	require.NoError(t, err)
	defer stream.Close()
	handler := newRecordingStreamHandler()
	_, err = stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{
		VChannel:           "v1",
		StartAfterTimeTick: 0,
		Handler:            handler,
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return len(handler.events) == 16
	}, time.Second, 10*time.Millisecond)

	_, err = transformLog.flush(context.Background(), flushOption{TargetTimeTick: 20})
	require.NoError(t, err)

	for expected := uint64(1); expected <= 20; expected++ {
		event := <-handler.events
		require.NotNil(t, event.Entry)
		assert.Equal(t, expected, event.Entry.GetTimeTick())
	}
	syncUp := <-handler.events
	require.NotNil(t, syncUp.SyncUp)
}

func TestConsumeDirtySnapshotKeepsStableInFlightView(t *testing.T) {
	transformLog := New(Config{
		VChannel: "v1",
		Meta: &streamingpb.VChannelTransformLogMeta{
			CheckpointTimeTick: 10,
		},
	})

	transformLog.mu.Lock()
	transformLog.meta.CheckpointTimeTick = 20
	transformLog.dirty = true
	transformLog.mu.Unlock()

	first := transformLog.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, first)
	assert.Equal(t, uint64(20), first.GetCheckpointTimeTick())

	transformLog.mu.Lock()
	transformLog.meta.CheckpointTimeTick = 30
	transformLog.dirty = true
	transformLog.mu.Unlock()

	inFlight := transformLog.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, inFlight)
	assert.Equal(t, uint64(20), inFlight.GetCheckpointTimeTick())

	transformLog.MarkSnapshotPersisted(first)

	next := transformLog.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, next)
	assert.Equal(t, uint64(30), next.GetCheckpointTimeTick())

	transformLog.MarkSnapshotPersisted(next)
	assert.Nil(t, transformLog.ConsumeDirtyAndGetSnapshot())
}

func TestMaterializeAdvancesMaterializedBarrierAfterSnapshotPersisted(t *testing.T) {
	materializer := &recordingMaterializer{}
	transformLog := New(Config{
		VChannel:     "by-dev-rootcoord-dml_1v0",
		Store:        newMemoryStore(),
		Materializer: materializer,
	})
	require.True(t, transformLog.append(newTransformLogTestDeleteMessage(t, 10), appendOption{}).Appended)
	_, err := transformLog.flush(context.Background(), flushOption{TargetTimeTick: 20})
	require.NoError(t, err)
	require.True(t, transformLog.syncUp(20).Appended)

	result, err := transformLog.materialize(context.Background(), materializeOption{TargetTimeTick: 20})
	require.NoError(t, err)
	assert.True(t, result.Started)
	assert.True(t, result.HasMaterializedSegments)
	assert.Equal(t, uint64(20), result.MaterializedTimeTick)
	require.Len(t, materializer.requests, 1)
	require.Len(t, materializer.requests[0].Entries, 1)
	assert.Equal(t, uint64(0), transformLog.MaterializedBarrierTimeTick())

	snapshot := transformLog.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, uint64(20), snapshot.GetMaterializedTimeTick())

	transformLog.MarkSnapshotPersisted(snapshot)
	assert.Equal(t, uint64(20), transformLog.MaterializedBarrierTimeTick())
}

func TestMaterializeWithoutEntriesOnlyAdvancesCursor(t *testing.T) {
	materializer := &recordingMaterializer{}
	transformLog := New(Config{
		VChannel:     "by-dev-rootcoord-dml_1v0",
		Materializer: materializer,
		Meta: &streamingpb.VChannelTransformLogMeta{
			CheckpointTimeTick: 20,
		},
	})

	result, err := transformLog.materialize(context.Background(), materializeOption{TargetTimeTick: 20})
	require.NoError(t, err)
	assert.True(t, result.Started)
	assert.False(t, result.HasMaterializedSegments)
	assert.Empty(t, materializer.requests)

	snapshot := transformLog.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, uint64(20), snapshot.GetMaterializedTimeTick())
}

func TestMaterializeSkipsBarrierEntries(t *testing.T) {
	materializer := &recordingMaterializer{}
	transformLog := New(Config{
		VChannel:     "by-dev-rootcoord-dml_1v0",
		Store:        newMemoryStore(),
		Materializer: materializer,
	})
	require.True(t, transformLog.syncUp(5).Appended)
	require.True(t, transformLog.append(newTransformLogTestDeleteMessage(t, 10), appendOption{}).Appended)
	require.True(t, transformLog.syncUp(20).Appended)
	_, err := transformLog.flush(context.Background(), flushOption{TargetTimeTick: 20})
	require.NoError(t, err)

	result, err := transformLog.materialize(context.Background(), materializeOption{TargetTimeTick: 20})
	require.NoError(t, err)
	assert.True(t, result.Started)
	assert.True(t, result.HasMaterializedSegments)
	assert.Equal(t, uint64(20), result.MaterializedTimeTick)
	assert.Equal(t, uint64(1), result.MaterializedRows)
	require.Len(t, materializer.requests, 1)
	require.Len(t, materializer.requests[0].Entries, 1)
	assert.Equal(t, uint64(10), materializer.requests[0].Entries[0].GetTimeTick())
	require.NotNil(t, materializer.requests[0].Entries[0].GetDelete())
}

func TestFlushPersistsLastEntryAndExposesBarrierAfterSnapshotPersisted(t *testing.T) {
	transformLog := New(Config{
		VChannel: "v1",
		Store:    newMemoryStore(),
	})

	appendResult := transformLog.append(newTransformLogTestDeleteMessage(t, 10), appendOption{})
	require.True(t, appendResult.Appended)
	require.True(t, transformLog.syncUp(20).Appended)

	result, err := transformLog.flush(context.Background(), flushOption{TargetTimeTick: 20})
	require.NoError(t, err)
	assert.True(t, result.Started)
	assert.Equal(t, uint64(10), result.DurableTimeTick)
	assert.Equal(t, uint64(10), transformLog.SnapshotMeta().GetCheckpointTimeTick())
	assert.Zero(t, transformLog.DataBarrierTimeTick())

	snapshot := transformLog.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, snapshot)
	transformLog.MarkSnapshotPersisted(snapshot)
	assert.Equal(t, uint64(20), transformLog.DataBarrierTimeTick())
	require.Len(t, transformLog.chunks, 1)
	require.Len(t, transformLog.chunks[0].entries, 1)
	assert.Equal(t, uint64(10), transformLog.chunks[0].entries[0].GetTimeTick())
}

func TestFlushKeepsCheckpointAtLastDurableEntryWhenTargetStillHasPendingEntries(t *testing.T) {
	transformLog := New(Config{
		VChannel: "v1",
		Store:    newMemoryStore(),
		MaxRows:  1,
	})

	require.True(t, transformLog.append(newTransformLogTestDeleteMessage(t, 10), appendOption{}).Appended)
	require.True(t, transformLog.append(newTransformLogTestDeleteMessage(t, 11), appendOption{}).Appended)
	require.True(t, transformLog.syncUp(20).Appended)

	result, err := transformLog.flush(context.Background(), flushOption{TargetTimeTick: 20})
	require.NoError(t, err)
	assert.True(t, result.Started)
	assert.Equal(t, uint64(10), result.DurableTimeTick)
	assert.Equal(t, uint64(20), result.NextTargetTimeTick)
	assert.Equal(t, uint64(10), transformLog.SnapshotMeta().GetCheckpointTimeTick())
	firstSnapshot := transformLog.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, firstSnapshot)
	transformLog.MarkSnapshotPersisted(firstSnapshot)
	assert.Equal(t, uint64(10), transformLog.DataBarrierTimeTick())

	result, err = transformLog.flush(context.Background(), flushOption{TargetTimeTick: result.NextTargetTimeTick})
	require.NoError(t, err)
	assert.Equal(t, uint64(11), result.DurableTimeTick)
	assert.Zero(t, result.NextTargetTimeTick)
	assert.Equal(t, uint64(11), transformLog.SnapshotMeta().GetCheckpointTimeTick())
	secondSnapshot := transformLog.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, secondSnapshot)
	transformLog.MarkSnapshotPersisted(secondSnapshot)
	assert.Equal(t, uint64(20), transformLog.DataBarrierTimeTick())
}

func TestShouldMaterializeUsesUnmaterializedRowsAndBytes(t *testing.T) {
	transformLog := New(Config{
		VChannel:            "by-dev-rootcoord-dml_1v0",
		MaterializeMaxRows:  2,
		MaterializeMaxBytes: 1 << 30,
		Meta: &streamingpb.VChannelTransformLogMeta{
			CheckpointTimeTick: 20,
		},
	})
	transformLog.chunks = []*chunkDescriptor{newLoadedChunkDescriptor(&streamingpb.TransformLogChunk{
		ChunkId: 1,
		Entries: []*streamingpb.TransformLogEntry{testTransformLogDeleteEntry(10, 1, 2)},
	})}

	assert.True(t, transformLog.shouldMaterialize())
	transformLog.meta.MaterializedTimeTick = 10
	assert.False(t, transformLog.shouldMaterialize())
}

func TestSplitMaterializeGroupsSplitsSingleBlockByRowLimit(t *testing.T) {
	groups := splitMaterializeGroups(MaterializeRequest{
		VChannel:       "by-dev-rootcoord-dml_1v0",
		TargetTimeTick: 10,
		MaxRows:        2,
		MaxBytes:       1 << 30,
		Entries:        []*streamingpb.TransformLogEntry{testTransformLogDeleteEntry(10, 1, 2, 3, 4, 5)},
	})

	require.Len(t, groups, 3)
	assert.Len(t, groups[0].pks, 2)
	assert.Len(t, groups[1].pks, 2)
	assert.Len(t, groups[2].pks, 1)
	for _, group := range groups {
		assert.Equal(t, int64(10), group.partitionID)
		assert.Equal(t, schemapb.DataType_Int64, group.pkType)
		assert.Len(t, group.pks, len(group.timestamps))
	}
}

func TestTransformLogDeleteRowsUsesPrimaryKeyCount(t *testing.T) {
	request := &msgpb.DeleteRequest{
		PrimaryKeys: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
			},
		},
		Timestamps: []uint64{10},
	}

	assert.Equal(t, uint64(3), deleteEntryRows(request))
}

func testTransformLogEntry(timeTick uint64) *streamingpb.TransformLogEntry {
	return &streamingpb.TransformLogEntry{TimeTick: timeTick}
}

func testTransformLogDeleteEntry(timeTick uint64, pks ...int64) *streamingpb.TransformLogEntry {
	return &streamingpb.TransformLogEntry{
		TimeTick: timeTick,
		Entry: &streamingpb.TransformLogEntry_Delete{
			Delete: &streamingpb.TransformDeleteEntry{
				Blocks: []*streamingpb.TransformDeleteBlock{
					{
						PartitionId: 10,
						PrimaryKeys: &schemapb.IDs{
							IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}},
						},
					},
				},
			},
		},
	}
}

type recordingMaterializer struct {
	requests []MaterializeRequest
}

func (m *recordingMaterializer) Materialize(_ context.Context, req MaterializeRequest) error {
	cloned := MaterializeRequest{
		VChannel:       req.VChannel,
		TargetTimeTick: req.TargetTimeTick,
		MaxRows:        req.MaxRows,
		MaxBytes:       req.MaxBytes,
		Entries:        make([]*streamingpb.TransformLogEntry, 0, len(req.Entries)),
	}
	for _, entry := range req.Entries {
		cloned.Entries = append(cloned.Entries, proto.Clone(entry).(*streamingpb.TransformLogEntry))
	}
	m.requests = append(m.requests, cloned)
	return nil
}

type recordingScheduler struct {
	tasks []nodescheduler.Task
}

func (s *recordingScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
	s.tasks = append(s.tasks, task)
	return recordingTaskHandle{}
}

type recordingTaskHandle struct{}

func (recordingTaskHandle) Cancel() {}

func (recordingTaskHandle) Wait(context.Context) error { return nil }
