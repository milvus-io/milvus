//go:build test
// +build test

package recovery

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestPChannelSummaryCleanerAdvancesMinAvailableAndDeletesOldChunks(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := newTestPChannelSummaryCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))
	writeTestPChannelSummaryChunks(t, ctx, "p1", chunkManager, 0, 3)
	catalogState.storeMeta = testPChannelSummaryStoreMeta(t, ctx, "p1", chunkManager, 3, 0, 0)

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(300),
		TimeTick:  300,
	})
	addTestSummaryPinnedAtGeneration(rs.summaryManager, "v1", 2)
	rs.summaryManager.markActiveViewsInitialized()
	rs.SetLogger(resource.Resource().Logger())

	require.NoError(t, rs.summaryManager.cleanPChannelSummary(ctx, resource.Resource().Logger()))
	require.Equal(t, uint64(2), catalogState.storeMeta.GetMinAvailableGeneration())
	require.Equal(t, uint64(2), catalogState.storeMeta.GetMinInUseGeneration())
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 0, false)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 1, false)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 2, true)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 3, true)
}

func TestPChannelSummaryCleanerDoesNotDeleteLatestGeneration(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := newTestPChannelSummaryCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))
	writeTestPChannelSummaryChunks(t, ctx, "p1", chunkManager, 0, 3)
	catalogState.storeMeta = testPChannelSummaryStoreMeta(t, ctx, "p1", chunkManager, 3, 0, 0)

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(300),
		TimeTick:  300,
	})
	addTestSummaryPinnedAtGeneration(rs.summaryManager, "v1", 4)
	rs.summaryManager.markActiveViewsInitialized()
	rs.SetLogger(resource.Resource().Logger())

	require.NoError(t, rs.summaryManager.cleanPChannelSummary(ctx, resource.Resource().Logger()))
	require.Equal(t, uint64(3), catalogState.storeMeta.GetMinAvailableGeneration())
	require.Equal(t, uint64(4), catalogState.storeMeta.GetMinInUseGeneration())
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 0, false)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 1, false)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 2, false)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 3, true)
}

func TestPChannelSummaryCleanerWaitsForActiveViewInitialization(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := newTestPChannelSummaryCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))
	writeTestPChannelSummaryChunks(t, ctx, "p1", chunkManager, 0, 2)
	catalogState.storeMeta = testPChannelSummaryStoreMeta(t, ctx, "p1", chunkManager, 2, 0, 0)

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(200),
		TimeTick:  200,
	})
	addTestSummaryPinnedAtGeneration(rs.summaryManager, "v1", 2)
	rs.SetLogger(resource.Resource().Logger())

	require.NoError(t, rs.summaryManager.cleanPChannelSummary(ctx, resource.Resource().Logger()))
	require.Equal(t, uint64(0), catalogState.storeMeta.GetMinAvailableGeneration())
	require.Equal(t, uint64(0), catalogState.storeMeta.GetMinInUseGeneration())
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 0, true)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 1, true)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 2, true)
}

func TestPChannelSummaryCleanerNoActiveViewsKeepsOnlyLatestGeneration(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := newTestPChannelSummaryCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))
	writeTestPChannelSummaryChunks(t, ctx, "p1", chunkManager, 0, 3)
	catalogState.storeMeta = testPChannelSummaryStoreMeta(t, ctx, "p1", chunkManager, 3, 0, 2)

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(300),
		TimeTick:  300,
	})
	rs.summaryManager.markActiveViewsInitialized()
	rs.SetLogger(resource.Resource().Logger())

	require.NoError(t, rs.summaryManager.cleanPChannelSummary(ctx, resource.Resource().Logger()))
	require.Equal(t, uint64(3), catalogState.storeMeta.GetMinAvailableGeneration())
	require.Equal(t, uint64(3), catalogState.storeMeta.GetMinInUseGeneration())
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 0, false)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 1, false)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 2, false)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 3, true)
}

func TestPChannelSummaryCleanerEmptyActiveSummaryKeepsOnlyLatestGeneration(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := newTestPChannelSummaryCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))
	writeTestPChannelSummaryChunks(t, ctx, "p1", chunkManager, 0, 3)
	catalogState.storeMeta = testPChannelSummaryStoreMeta(t, ctx, "p1", chunkManager, 3, 0, 0)

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(300),
		TimeTick:  300,
	})
	rs.summaryManager.setSummaries(map[string]*vchannelSummary{
		"v1": newEmptyVChannelSummary("p1", "v1", &utility.WALCheckpoint{
			MessageID: rmq.NewRmqID(300),
			TimeTick:  300,
		}),
	})
	rs.summaryManager.markActiveViewsInitialized()
	rs.SetLogger(resource.Resource().Logger())

	require.NoError(t, rs.summaryManager.cleanPChannelSummary(ctx, resource.Resource().Logger()))
	require.Equal(t, uint64(3), catalogState.storeMeta.GetMinAvailableGeneration())
	require.Equal(t, uint64(3), catalogState.storeMeta.GetMinInUseGeneration())
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 0, false)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 1, false)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 2, false)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 3, true)
}

func TestPChannelSummaryCleanerReclaimsFromMinAvailableLeavingLowerChunks(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := newTestPChannelSummaryCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))
	writeTestPChannelSummaryChunks(t, ctx, "p1", chunkManager, 0, 4)
	catalogState.storeMeta = testPChannelSummaryStoreMeta(t, ctx, "p1", chunkManager, 4, 2, 2)

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(300),
		TimeTick:  300,
	})
	addTestSummaryPinnedAtGeneration(rs.summaryManager, "v1", 3)
	rs.summaryManager.markActiveViewsInitialized()
	rs.SetLogger(resource.Resource().Logger())

	require.NoError(t, rs.summaryManager.cleanPChannelSummary(ctx, resource.Resource().Logger()))
	require.Equal(t, uint64(3), catalogState.storeMeta.GetMinAvailableGeneration())
	require.Equal(t, uint64(3), catalogState.storeMeta.GetMinInUseGeneration())
	// Chunks below the old MinAvailableGeneration (2) are assumed already deleted
	// and are never re-scanned, so any residual chunks there are left untouched.
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 0, true)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 1, true)
	// [MinAvailableGeneration, MinInUseGeneration) = [2, 3) is reclaimed.
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 2, false)
	// [MinInUseGeneration, LatestGeneration] = [3, 4] is still in use.
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 3, true)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 4, true)
}

func TestPChannelSummaryCleanerReDeletesIdempotentlyAfterCrashBeforeSave(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := newTestPChannelSummaryCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))
	writeTestPChannelSummaryChunks(t, ctx, "p1", chunkManager, 0, 3)
	// A prior cleaner cycle persisted the advanced MinInUseGeneration (2), then
	// deleted chunk 0 from [MinAvailableGeneration, MinInUseGeneration) but
	// crashed before completing the deletions and advancing
	// MinAvailableGeneration, so the meta still says MinAvailable=0. The next
	// cycle must re-delete the range idempotently and finish the advance.
	catalogState.storeMeta = testPChannelSummaryStoreMeta(t, ctx, "p1", chunkManager, 3, 0, 2)
	require.NoError(t, chunkManager.Remove(ctx, buildPChannelSummaryChunkKey("p1", 0, 0)))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(300),
		TimeTick:  300,
	})
	addTestSummaryPinnedAtGeneration(rs.summaryManager, "v1", 2)
	rs.summaryManager.markActiveViewsInitialized()
	rs.SetLogger(resource.Resource().Logger())

	require.NoError(t, rs.summaryManager.cleanPChannelSummary(ctx, resource.Resource().Logger()))
	require.Equal(t, uint64(2), catalogState.storeMeta.GetMinAvailableGeneration())
	require.Equal(t, uint64(2), catalogState.storeMeta.GetMinInUseGeneration())
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 0, false) // already gone, skipped
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 1, false) // reclaimed this cycle
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 2, true)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 3, true)
}

// removeFailingChunkManager fails every Remove, freezing chunk deletion to
// simulate a cleaner interrupted between the in-use advance and the deletions.
type removeFailingChunkManager struct {
	storage.ChunkManager
}

func (f removeFailingChunkManager) Remove(ctx context.Context, key string) error {
	return errors.New("injected remove failure")
}

// The crash-consistency contract of the cleaner: the advanced
// MinInUseGeneration must be durable BEFORE any chunk deletion, because
// recovery replays chunks from the persisted MinInUseGeneration upward and
// hard-fails on a missing chunk. An interruption after the deletions started
// must therefore never leave the persisted meta pointing into the deleted
// range — WAL open would fail permanently with no self-heal.
func TestPChannelSummaryCleanerPersistsMinInUseBeforeDeleting(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := newTestPChannelSummaryCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(removeFailingChunkManager{chunkManager}))
	writeTestPChannelSummaryChunks(t, ctx, "p1", chunkManager, 0, 3)
	catalogState.storeMeta = testPChannelSummaryStoreMeta(t, ctx, "p1", chunkManager, 3, 0, 0)

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(300),
		TimeTick:  300,
	})
	addTestSummaryPinnedAtGeneration(rs.summaryManager, "v1", 2)
	rs.summaryManager.markActiveViewsInitialized()
	rs.SetLogger(resource.Resource().Logger())

	// The cycle aborts on the injected deletion failure...
	require.Error(t, rs.summaryManager.cleanPChannelSummary(ctx, resource.Resource().Logger()))
	// ...but the in-use advance is already durable, the low-water is not, and
	// no chunk is missing below the persisted MinInUseGeneration.
	require.Equal(t, uint64(2), catalogState.storeMeta.GetMinInUseGeneration())
	require.Equal(t, uint64(0), catalogState.storeMeta.GetMinAvailableGeneration())
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 2, true)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 3, true)

	// A restart in this state must recover: the replay range persisted in the
	// meta only references live chunks.
	recovered := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(300),
		TimeTick:  300,
	})
	recovered.SetLogger(resource.Resource().Logger())
	_, err := recovered.summaryManager.recoverSummariesFromStore(ctx, "p1", pchannelSummaryStoreMetaFromCatalog(catalogState.storeMeta))
	require.NoError(t, err)
}

// addTestSummaryPinnedAtGeneration injects an idempotency summary whose
// min-required generation is pinned at the given generation, giving cleaner tests
// a precise GC boundary (idempotency is the only summary view that participates in
// the boundary).
func addTestSummaryPinnedAtGeneration(m *summaryManager, vchannel string, generation uint64) {
	summary := newEmptyVChannelSummary(m.pchannel, vchannel, nil)
	key := "pin-" + vchannel
	summary.entries[key] = &summaryEntry{
		entry:         &streamingpb.SummaryEntry{Idempotency: &streamingpb.IdempotencyContent{Key: key}},
		generation:    generation,
		generationSet: true,
	}
	summary.latestAppliedGeneration = generation
	summary.refreshMinRequiredGeneration()
	m.setSummary(vchannel, summary)
}

func newTestPChannelSummaryCleanerChunkManager() storage.ChunkManager {
	return storage.NewLocalChunkManager(objectstorage.RootPath(paramtable.Get().MinioCfg.RootPath.GetValue()))
}

func writeTestPChannelSummaryChunks(t *testing.T, ctx context.Context, pchannel string, chunkManager storage.ChunkManager, startGeneration uint64, endGeneration uint64) {
	t.Helper()
	for generation := startGeneration; generation <= endGeneration; generation++ {
		writeTestPChannelSummaryChunk(ctx, t, pchannel, generation, chunkManager, &utility.WALCheckpoint{
			MessageID: rmq.NewRmqID(int64(generation + 1)),
			TimeTick:  generation + 1,
		}, nil)
	}
}

func testPChannelSummaryStoreMeta(
	t *testing.T,
	ctx context.Context,
	pchannel string,
	chunkManager storage.ChunkManager,
	latestGeneration uint64,
	minAvailableGeneration uint64,
	minInUseGeneration uint64,
) *streamingpb.PChannelSummaryMeta {
	t.Helper()
	footer, _, _ := writeTestPChannelSummaryChunk(ctx, t, pchannel, latestGeneration, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(int64(latestGeneration + 1)),
		TimeTick:  latestGeneration + 1,
	}, nil)
	return newPChannelSummaryStoreMetaFromChunk(pchannel, footer, minAvailableGeneration, minInUseGeneration).intoCatalogMeta()
}

func requirePChannelSummaryChunkExists(t *testing.T, ctx context.Context, chunkManager storage.ChunkManager, pchannel string, generation uint64, expected bool) {
	t.Helper()
	exists, err := chunkManager.Exist(ctx, buildPChannelSummaryChunkKey(pchannel, generation, 0))
	require.NoError(t, err)
	require.Equal(t, expected, exists)
}
