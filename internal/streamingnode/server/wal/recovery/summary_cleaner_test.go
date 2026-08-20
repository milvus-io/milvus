package recovery

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func queueTestPendingGC(manager *summaryManager, refs ...*streamingpb.PChannelSummaryChunkRef) {
	manager.manifest = &streamingpb.PChannelSummaryManifest{PendingGc: refs}
}

func TestRunPendingGCDeletesQueuedChunks(t *testing.T) {
	ctx := context.Background()
	chunkManager, _ := newTestSummaryStore(t)

	records := map[string][]*SummaryRecord{"v1": {newTestSummaryRecord("key-a", 10, 1)}}
	writeTestPChannelSummaryChunk(ctx, t, chunkManager, "p1", 0, 1, records)
	writeTestPChannelSummaryChunk(ctx, t, chunkManager, "p1", 1, 2, records)
	writeTestPChannelSummaryChunk(ctx, t, chunkManager, "p1", 2, 2, records)

	manager := newTestSummaryManager(t, "p1", 3, newTestSummaryConfig())
	// Only the first two are queued; the third is still part of the set recovery
	// reads and must survive.
	queueTestPendingGC(manager,
		&streamingpb.PChannelSummaryChunkRef{Generation: 0, Term: 1},
		&streamingpb.PChannelSummaryChunkRef{Generation: 1, Term: 2},
	)

	require.NoError(t, manager.runPendingGC(ctx, manager.Logger()))

	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 0, 1, false)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 1, 2, false)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 2, 2, true)

	// Completion is tracked in memory and becomes durable at the next manifest
	// write; until then the queue still names the work.
	require.Len(t, manager.completedGC, 2)
	require.Len(t, manager.manifest.GetPendingGc(), 2)
}

func TestRunPendingGCIsIdempotent(t *testing.T) {
	ctx := context.Background()
	chunkManager, _ := newTestSummaryStore(t)
	writeTestPChannelSummaryChunk(ctx, t, chunkManager, "p1", 0, 1, map[string][]*SummaryRecord{
		"v1": {newTestSummaryRecord("key-a", 10, 1)},
	})

	manager := newTestSummaryManager(t, "p1", 2, newTestSummaryConfig())
	queueTestPendingGC(manager, &streamingpb.PChannelSummaryChunkRef{Generation: 0, Term: 1})

	require.NoError(t, manager.runPendingGC(ctx, manager.Logger()))
	// A crash before the completion reached the manifest replays the batch. The
	// second pass must be a no-op rather than an error: deleting an absent object
	// succeeds, which is what makes the work queue safe to replay.
	manager.completedGC = make(map[pchannelSummaryGCRef]struct{})
	require.NoError(t, manager.runPendingGC(ctx, manager.Logger()))
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 0, 1, false)
}

func TestRunPendingGCLeavesEntryQueuedOnFailure(t *testing.T) {
	ctx := context.Background()
	enableRecoveryIdempotency(t)
	catalog, _ := newTestPChannelSummaryCatalog(t)
	chunkManager := mocks.NewChunkManager(t)
	chunkManager.EXPECT().RootPath().Return("root").Maybe()
	chunkManager.EXPECT().Remove(mock.Anything, mock.Anything).Return(errors.New("object storage unavailable"))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	manager := newTestSummaryManager(t, "p1", 2, newTestSummaryConfig())
	queueTestPendingGC(manager,
		&streamingpb.PChannelSummaryChunkRef{Generation: 0, Term: 1},
		&streamingpb.PChannelSummaryChunkRef{Generation: 1, Term: 1},
	)

	require.Error(t, manager.runPendingGC(ctx, manager.Logger()))
	// Nothing is marked done, so the next cycle retries it. Stopping costs
	// nothing: the objects are already unreferenced by every reader.
	require.Empty(t, manager.completedGC)
	require.Len(t, manager.manifest.GetPendingGc(), 2)
}

func TestRunPendingGCSkipsAlreadyCompleted(t *testing.T) {
	ctx := context.Background()
	enableRecoveryIdempotency(t)
	catalog, _ := newTestPChannelSummaryCatalog(t)
	chunkManager := mocks.NewChunkManager(t)
	chunkManager.EXPECT().RootPath().Return("root").Maybe()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	manager := newTestSummaryManager(t, "p1", 2, newTestSummaryConfig())
	queueTestPendingGC(manager, &streamingpb.PChannelSummaryChunkRef{Generation: 0, Term: 1})
	manager.completedGC[pchannelSummaryGCRef{term: 1, generation: 0}] = struct{}{}

	// No Remove is expected: the in-memory completion record is what keeps a
	// replayed queue from re-issuing deletes it already made.
	require.NoError(t, manager.runPendingGC(ctx, manager.Logger()))
}

func TestRunPendingGCOnEmptyQueueDoesNothing(t *testing.T) {
	ctx := context.Background()
	enableRecoveryIdempotency(t)
	catalog, _ := newTestPChannelSummaryCatalog(t)
	chunkManager := mocks.NewChunkManager(t)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	manager := newTestSummaryManager(t, "p1", 2, newTestSummaryConfig())
	manager.manifest = &streamingpb.PChannelSummaryManifest{}
	require.NoError(t, manager.runPendingGC(ctx, manager.Logger()))
}

func TestRetentionSlideQueuesChunksForGC(t *testing.T) {
	ctx := context.Background()
	newTestSummaryStore(t)

	cfg := newTestSummaryConfig()
	// A cap of one retains only the newest chunk, so every persist past the first
	// slides the boundary and queues the chunk it displaced.
	cfg.idempotencyMaxRetainedChunks = 1

	manager := newTestSummaryManager(t, "p1", 1, cfg)
	rs := newTestRecoveryStorageForSummary(t, 5, "v1")
	_, err := manager.recoverFromSummaryStore(ctx, "p1", rs.checkpoint, rs.vchannels)
	require.NoError(t, err)
	manager.setNormalMode()

	for i := 0; i < 3; i++ {
		_, err := manager.persistPChannelSummary(ctx, manager.Logger(), map[string][]*SummaryRecord{
			"v1": {newTestSummaryRecord("key", uint64(10+i), int64(i))},
		})
		require.NoError(t, err)
	}

	require.Len(t, manager.manifest.GetChunks(), 1)
	require.Equal(t, uint64(2), manager.manifest.GetChunks()[0].GetGeneration())
	require.Len(t, manager.manifest.GetPendingGc(), 2)

	// GC's whole input is that queue; it never re-decides what is releasable.
	require.NoError(t, manager.runPendingGC(ctx, manager.Logger()))
	requirePChannelSummaryChunkExists(t, ctx, resource.Resource().ChunkManager(), "p1", 0, 1, false)
	requirePChannelSummaryChunkExists(t, ctx, resource.Resource().ChunkManager(), "p1", 1, 1, false)
	requirePChannelSummaryChunkExists(t, ctx, resource.Resource().ChunkManager(), "p1", 2, 1, true)
}
