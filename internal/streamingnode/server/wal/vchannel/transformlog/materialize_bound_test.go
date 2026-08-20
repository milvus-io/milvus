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
