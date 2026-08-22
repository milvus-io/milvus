package transformlog

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestTransformLogRetriesRequestedMaterializationAsUpperBoundAdvances(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	require.NoError(t, store.WriteTransformLogChunk(ctx, "v1", &streamingpb.TransformLogChunk{
		ChunkId: 0,
		Entries: []*streamingpb.TransformLogEntry{
			testTransformLogDeleteEntry(10, 1),
			testTransformLogDeleteEntry(20, 2),
			testTransformLogDeleteEntry(30, 3),
		},
	}))
	scheduler := &recordingScheduler{}
	materializer := &recordingMaterializer{}
	transformLog := New(Config{
		VChannel:     "v1",
		Store:        store,
		Materializer: materializer,
		Runtime:      moduleapi.Runtime{Scheduler: scheduler},
		Meta: &streamingpb.VChannelTransformLogMeta{
			CheckpointTimeTick: 30,
			NextChunkId:        1,
		},
	})

	require.False(t, transformLog.SetMaterializeUpperBound(10))
	require.True(t, transformLog.RequestMaterializeThrough(30))
	require.False(t, transformLog.SetMaterializeUpperBound(10))
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, scheduler.tasks[0].Execute(ctx))
	assert.Equal(t, uint64(10), transformLog.SnapshotMeta().GetMaterializedTimeTick())
	require.Len(t, materializer.requests, 1)
	assert.Equal(t, uint64(10), materializer.requests[0].TargetTimeTick)
	require.Len(t, materializer.requests[0].Entries, 1)
	assert.Equal(t, uint64(10), materializer.requests[0].Entries[0].GetTimeTick())

	require.True(t, transformLog.SetMaterializeUpperBound(20))
	require.Len(t, scheduler.tasks, 2)
	require.NoError(t, scheduler.tasks[1].Execute(ctx))
	assert.Equal(t, uint64(20), transformLog.SnapshotMeta().GetMaterializedTimeTick())
	require.Len(t, materializer.requests, 2)
	assert.Equal(t, uint64(20), materializer.requests[1].TargetTimeTick)
	require.Len(t, materializer.requests[1].Entries, 1)
	assert.Equal(t, uint64(20), materializer.requests[1].Entries[0].GetTimeTick())

	require.True(t, transformLog.SetMaterializeUpperBound(math.MaxUint64))
	require.Len(t, scheduler.tasks, 3)
	require.NoError(t, scheduler.tasks[2].Execute(ctx))
	assert.Equal(t, uint64(30), transformLog.SnapshotMeta().GetMaterializedTimeTick())
	require.Len(t, materializer.requests, 3)
	assert.Equal(t, uint64(30), materializer.requests[2].TargetTimeTick)
	require.Len(t, materializer.requests[2].Entries, 1)
	assert.Equal(t, uint64(30), materializer.requests[2].Entries[0].GetTimeTick())
}

func TestTransformLogMaterializationRequestWithoutScheduler(t *testing.T) {
	var nilTransformLog *TransformLog
	assert.False(t, nilTransformLog.RequestMaterializeThrough(10))
	assert.False(t, nilTransformLog.SetMaterializeUpperBound(10))

	transformLog := New(Config{VChannel: "v1"})
	assert.False(t, transformLog.RequestMaterializeThrough(10))
	assert.False(t, transformLog.SetMaterializeUpperBound(10))
}

func TestTransformLogShouldMaterializeDoesNotScanBlockedBacklog(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	require.NoError(t, store.WriteTransformLogChunk(ctx, "v1", &streamingpb.TransformLogChunk{
		ChunkId: 0,
		Entries: []*streamingpb.TransformLogEntry{
			testTransformLogDeleteEntry(10, 1),
			testTransformLogDeleteEntry(20, 2),
			testTransformLogDeleteEntry(30, 3),
		},
	}))
	scheduler := &recordingScheduler{}
	materializer := &recordingMaterializer{}
	transformLog := New(Config{
		VChannel:           "v1",
		Store:              store,
		Materializer:       materializer,
		MaterializeMaxRows: 1,
		Runtime:            moduleapi.Runtime{Scheduler: scheduler},
		Meta: &streamingpb.VChannelTransformLogMeta{
			CheckpointTimeTick: 30,
			NextChunkId:        1,
		},
	})
	require.False(t, transformLog.SetMaterializeUpperBound(10), "no materialization requested yet")
	require.True(t, transformLog.shouldMaterialize(ctx), "data inside the bound should still trigger")

	// Materialize through the bound.
	require.True(t, transformLog.RequestMaterializeThrough(30))
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, scheduler.tasks[0].Execute(ctx))
	require.Equal(t, uint64(10), transformLog.SnapshotMeta().GetMaterializedTimeTick())

	// The backlog beyond the bound is blocked: shouldMaterialize must report
	// false instead of re-scanning it on every flush task.
	require.False(t, transformLog.shouldMaterialize(ctx))
}

func TestTransformLogMaterializeClampedWhenBoundRetractsAfterSchedule(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	require.NoError(t, store.WriteTransformLogChunk(ctx, "v1", &streamingpb.TransformLogChunk{
		ChunkId: 0,
		Entries: []*streamingpb.TransformLogEntry{
			testTransformLogDeleteEntry(10, 1),
			testTransformLogDeleteEntry(20, 2),
			testTransformLogDeleteEntry(30, 3),
		},
	}))
	scheduler := &recordingScheduler{}
	materializer := &recordingMaterializer{}
	transformLog := New(Config{
		VChannel:     "v1",
		Store:        store,
		Materializer: materializer,
		Runtime:      moduleapi.Runtime{Scheduler: scheduler},
		Meta: &streamingpb.VChannelTransformLogMeta{
			CheckpointTimeTick: 30,
			NextChunkId:        1,
		},
	})
	// Schedule with no bound: the task targets the full checkpoint frontier.
	require.True(t, transformLog.RequestMaterializeThrough(30))
	require.Len(t, scheduler.tasks, 1)
	// The bound retracts before the task executes. No new task is scheduled
	// (the pending target 30 already covers it), so the execution-time clamp
	// must cap the emitted request by itself.
	require.False(t, transformLog.SetMaterializeUpperBound(10))
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, scheduler.tasks[0].Execute(ctx))
	require.Equal(t, uint64(10), transformLog.SnapshotMeta().GetMaterializedTimeTick())
	require.Len(t, materializer.requests, 1)
	assert.Equal(t, uint64(10), materializer.requests[0].TargetTimeTick)
	assert.Len(t, materializer.requests[0].Entries, 1)
	assert.Equal(t, uint64(10), materializer.requests[0].Entries[0].GetTimeTick())
}

func TestTransformLogMaterializeBatchCappedByMaxRows(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	require.NoError(t, store.WriteTransformLogChunk(ctx, "v1", &streamingpb.TransformLogChunk{
		ChunkId: 0,
		Entries: []*streamingpb.TransformLogEntry{
			testTransformLogDeleteEntry(10, 1, 2),
			testTransformLogDeleteEntry(20, 3, 4),
			testTransformLogDeleteEntry(30, 5, 6),
		},
	}))
	scheduler := &recordingScheduler{}
	materializer := &recordingMaterializer{}
	transformLog := New(Config{
		VChannel:           "v1",
		Store:              store,
		Materializer:       materializer,
		MaterializeMaxRows: 2,
		Runtime:            moduleapi.Runtime{Scheduler: scheduler},
		Meta: &streamingpb.VChannelTransformLogMeta{
			CheckpointTimeTick: 30,
			NextChunkId:        1,
		},
	})
	require.True(t, transformLog.RequestMaterializeThrough(30))
	require.Len(t, scheduler.tasks, 1)

	// First batch is capped after the 2-row entry at tick 10.
	require.NoError(t, scheduler.tasks[0].Execute(ctx))
	require.Equal(t, uint64(10), transformLog.SnapshotMeta().GetMaterializedTimeTick())
	require.Len(t, materializer.requests, 1)
	assert.Equal(t, uint64(10), materializer.requests[0].TargetTimeTick)
	require.Len(t, materializer.requests[0].Entries, 1)
	assert.Equal(t, uint64(10), materializer.requests[0].Entries[0].GetTimeTick())
	// The retained request schedules a continuation for the capped remainder.
	require.Len(t, scheduler.tasks, 2)

	require.NoError(t, scheduler.tasks[1].Execute(ctx))
	require.Equal(t, uint64(20), transformLog.SnapshotMeta().GetMaterializedTimeTick())
	require.Len(t, materializer.requests, 2)
	assert.Equal(t, uint64(20), materializer.requests[1].TargetTimeTick)
	require.Len(t, materializer.requests[1].Entries, 1)
	assert.Equal(t, uint64(20), materializer.requests[1].Entries[0].GetTimeTick())
	require.Len(t, scheduler.tasks, 3)

	// The final batch drains the rest; no further task is scheduled.
	require.NoError(t, scheduler.tasks[2].Execute(ctx))
	require.Equal(t, uint64(30), transformLog.SnapshotMeta().GetMaterializedTimeTick())
	require.Len(t, materializer.requests, 3)
	assert.Equal(t, uint64(30), materializer.requests[2].TargetTimeTick)
	require.Len(t, materializer.requests[2].Entries, 1)
	assert.Equal(t, uint64(30), materializer.requests[2].Entries[0].GetTimeTick())
	require.Len(t, scheduler.tasks, 3)
}
