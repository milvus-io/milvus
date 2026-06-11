package transformlog

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	transformlogapi "github.com/milvus-io/milvus/internal/streamingnode/transformlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestReadIgnoresDelayedPublishForRetainedEntries(t *testing.T) {
	transformLog := New(Config{VChannel: "v1"}).(*transformLog)
	entry := testTransformLogEntry(50)
	transformLog.retainedChunks = []*streamingpb.TransformLogChunk{
		{ChunkId: 0, Entries: []*streamingpb.TransformLogEntry{entry}},
	}

	scanner := transformLog.Read(context.Background(), transformlogapi.ReadOption{
		Name:               "test-scanner",
		VChannel:           "v1",
		StartAfterTimeTick: 10,
	})
	defer scanner.Close()

	transformLog.publish([]*streamingpb.TransformLogEntry{entry})

	entryEvent := <-scanner.Chan()
	require.NotNil(t, entryEvent.Entry)
	assert.Equal(t, uint64(50), entryEvent.Entry.GetTimeTick())
	caughtUpEvent := <-scanner.Chan()
	require.NotNil(t, caughtUpEvent.CaughtUp)

	select {
	case event := <-scanner.Chan():
		t.Fatalf("unexpected duplicate event: %+v", event)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestReadBuffersLiveEntriesUntilCaughtUp(t *testing.T) {
	transformLog := New(Config{VChannel: "v1"}).(*transformLog)
	entries := make([]*streamingpb.TransformLogEntry, 0, 20)
	for timeTick := uint64(1); timeTick <= 20; timeTick++ {
		entries = append(entries, testTransformLogEntry(timeTick))
	}
	transformLog.retainedChunks = []*streamingpb.TransformLogChunk{
		{ChunkId: 0, Entries: entries},
	}

	scanner := transformLog.Read(context.Background(), transformlogapi.ReadOption{
		Name:               "test-scanner",
		VChannel:           "v1",
		StartAfterTimeTick: 0,
	})
	defer scanner.Close()

	require.Eventually(t, func() bool {
		return len(scanner.Chan()) == 16
	}, time.Second, 10*time.Millisecond)
	transformLog.publish([]*streamingpb.TransformLogEntry{testTransformLogEntry(21)})

	for expected := uint64(1); expected <= 20; expected++ {
		event := <-scanner.Chan()
		require.NotNil(t, event.Entry)
		assert.Equal(t, expected, event.Entry.GetTimeTick())
	}
	caughtUpEvent := <-scanner.Chan()
	require.NotNil(t, caughtUpEvent.CaughtUp)
	liveEvent := <-scanner.Chan()
	require.NotNil(t, liveEvent.Entry)
	assert.Equal(t, uint64(21), liveEvent.Entry.GetTimeTick())
}

func TestSnapshotChunksCopiesSliceOnly(t *testing.T) {
	first := &streamingpb.TransformLogChunk{ChunkId: 1}
	second := &streamingpb.TransformLogChunk{ChunkId: 2}
	chunks := []*streamingpb.TransformLogChunk{first}

	snapshot := snapshotChunks(chunks)
	require.Len(t, snapshot, 1)
	assert.Same(t, first, snapshot[0])

	chunks[0] = second
	assert.Same(t, first, snapshot[0])
}

func TestConsumeDirtySnapshotKeepsStableInFlightView(t *testing.T) {
	transformLog := New(Config{
		VChannel: "v1",
		Meta: &streamingpb.VChannelTransformLogMeta{
			CheckpointTimeTick: 10,
		},
	}).(*transformLog)

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
		Materializer: materializer,
		Meta: &streamingpb.VChannelTransformLogMeta{
			CheckpointTimeTick: 20,
		},
	}).(*transformLog)
	transformLog.retainedChunks = []*streamingpb.TransformLogChunk{
		{ChunkId: 1, Entries: []*streamingpb.TransformLogEntry{testTransformLogDeleteEntry(10, 1, 2)}},
	}

	result, err := transformLog.Materialize(context.Background(), MaterializeOption{TargetTimeTick: 20})
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
	}).(*transformLog)

	result, err := transformLog.Materialize(context.Background(), MaterializeOption{TargetTimeTick: 20})
	require.NoError(t, err)
	assert.True(t, result.Started)
	assert.False(t, result.HasMaterializedSegments)
	assert.Empty(t, materializer.requests)

	snapshot := transformLog.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, uint64(20), snapshot.GetMaterializedTimeTick())
}

func TestFlushAdvancesCheckpointToFenceTimeTick(t *testing.T) {
	transformLog := New(Config{
		VChannel: "v1",
		Store:    newMemoryStore(),
	}).(*transformLog)

	appendResult := transformLog.Append(newModuleTestDeleteMessage(t, 10), AppendOption{})
	require.True(t, appendResult.Appended)

	result, err := transformLog.Flush(context.Background(), FlushOption{TargetTimeTick: 20})
	require.NoError(t, err)
	assert.True(t, result.Started)
	assert.Equal(t, uint64(20), result.DurableTimeTick)

	snapshot := transformLog.SnapshotMeta()
	assert.Equal(t, uint64(20), snapshot.GetCheckpointTimeTick())
	require.Len(t, transformLog.retainedChunks, 1)
	require.Len(t, transformLog.retainedChunks[0].GetEntries(), 1)
	assert.Equal(t, uint64(10), transformLog.retainedChunks[0].GetEntries()[0].GetTimeTick())
}

func TestFlushKeepsCheckpointAtLastDurableEntryWhenTargetStillHasPendingEntries(t *testing.T) {
	transformLog := New(Config{
		VChannel: "v1",
		Store:    newMemoryStore(),
		MaxRows:  1,
	}).(*transformLog)

	require.True(t, transformLog.Append(newModuleTestDeleteMessage(t, 10), AppendOption{}).Appended)
	require.True(t, transformLog.Append(newModuleTestDeleteMessage(t, 11), AppendOption{}).Appended)

	result, err := transformLog.Flush(context.Background(), FlushOption{TargetTimeTick: 20})
	require.NoError(t, err)
	assert.True(t, result.Started)
	assert.Equal(t, uint64(10), result.DurableTimeTick)
	assert.Equal(t, uint64(20), result.NextTargetTimeTick)
	assert.Equal(t, uint64(10), transformLog.SnapshotMeta().GetCheckpointTimeTick())
}

func TestShouldMaterializeUsesUnmaterializedRowsAndBytes(t *testing.T) {
	transformLog := New(Config{
		VChannel:            "by-dev-rootcoord-dml_1v0",
		MaterializeMaxRows:  2,
		MaterializeMaxBytes: 1 << 30,
		Meta: &streamingpb.VChannelTransformLogMeta{
			CheckpointTimeTick: 20,
		},
	}).(*transformLog)
	transformLog.retainedChunks = []*streamingpb.TransformLogChunk{
		{ChunkId: 1, Entries: []*streamingpb.TransformLogEntry{testTransformLogDeleteEntry(10, 1, 2)}},
	}

	assert.True(t, transformLog.ShouldMaterialize())
	transformLog.meta.MaterializedTimeTick = 10
	assert.False(t, transformLog.ShouldMaterialize())
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
