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

func TestPChannelWindowCleanerAdvancesMinAvailableAndDeletesOldChunks(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := newTestPChannelWindowCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))
	writeTestPChannelWindowChunks(t, ctx, "p1", chunkManager, 0, 3)
	catalogState.storeMeta = testPChannelWindowStoreMeta(t, ctx, "p1", chunkManager, 3, 0, 0)

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(300),
		TimeTick:  300,
	})
	addTestIdempotencyWindowPinnedAtGeneration(rs.windowManager, "v1", 2)
	rs.windowManager.markActiveViewsInitialized()
	rs.SetLogger(resource.Resource().Logger())

	require.NoError(t, rs.windowManager.cleanPChannelWindow(ctx, resource.Resource().Logger()))
	require.Equal(t, uint64(2), catalogState.storeMeta.GetMinAvailableGeneration())
	require.Equal(t, uint64(2), catalogState.storeMeta.GetMinInUseGeneration())
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 0, false)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 1, false)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 2, true)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 3, true)
}

func TestPChannelWindowCleanerDoesNotDeleteLatestGeneration(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := newTestPChannelWindowCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))
	writeTestPChannelWindowChunks(t, ctx, "p1", chunkManager, 0, 3)
	catalogState.storeMeta = testPChannelWindowStoreMeta(t, ctx, "p1", chunkManager, 3, 0, 0)

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(300),
		TimeTick:  300,
	})
	addTestIdempotencyWindowPinnedAtGeneration(rs.windowManager, "v1", 4)
	rs.windowManager.markActiveViewsInitialized()
	rs.SetLogger(resource.Resource().Logger())

	require.NoError(t, rs.windowManager.cleanPChannelWindow(ctx, resource.Resource().Logger()))
	require.Equal(t, uint64(3), catalogState.storeMeta.GetMinAvailableGeneration())
	require.Equal(t, uint64(4), catalogState.storeMeta.GetMinInUseGeneration())
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 0, false)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 1, false)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 2, false)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 3, true)
}

func TestPChannelWindowCleanerWaitsForActiveViewInitialization(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := newTestPChannelWindowCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))
	writeTestPChannelWindowChunks(t, ctx, "p1", chunkManager, 0, 2)
	catalogState.storeMeta = testPChannelWindowStoreMeta(t, ctx, "p1", chunkManager, 2, 0, 0)

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(200),
		TimeTick:  200,
	})
	addTestIdempotencyWindowPinnedAtGeneration(rs.windowManager, "v1", 2)
	rs.SetLogger(resource.Resource().Logger())

	require.NoError(t, rs.windowManager.cleanPChannelWindow(ctx, resource.Resource().Logger()))
	require.Equal(t, uint64(0), catalogState.storeMeta.GetMinAvailableGeneration())
	require.Equal(t, uint64(0), catalogState.storeMeta.GetMinInUseGeneration())
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 0, true)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 1, true)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 2, true)
}

func TestPChannelWindowCleanerNoActiveViewsKeepsOnlyLatestGeneration(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := newTestPChannelWindowCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))
	writeTestPChannelWindowChunks(t, ctx, "p1", chunkManager, 0, 3)
	catalogState.storeMeta = testPChannelWindowStoreMeta(t, ctx, "p1", chunkManager, 3, 0, 2)

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(300),
		TimeTick:  300,
	})
	rs.windowManager.markActiveViewsInitialized()
	rs.SetLogger(resource.Resource().Logger())

	require.NoError(t, rs.windowManager.cleanPChannelWindow(ctx, resource.Resource().Logger()))
	require.Equal(t, uint64(3), catalogState.storeMeta.GetMinAvailableGeneration())
	require.Equal(t, uint64(3), catalogState.storeMeta.GetMinInUseGeneration())
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 0, false)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 1, false)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 2, false)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 3, true)
}

func TestPChannelWindowCleanerEmptyActiveWindowKeepsOnlyLatestGeneration(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := newTestPChannelWindowCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))
	writeTestPChannelWindowChunks(t, ctx, "p1", chunkManager, 0, 3)
	catalogState.storeMeta = testPChannelWindowStoreMeta(t, ctx, "p1", chunkManager, 3, 0, 0)

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(300),
		TimeTick:  300,
	})
	rs.windowManager.setIdempotencyWindows(map[string]*vchannelWindow{
		"v1": newEmptyVChannelWindow("p1", "v1", &utility.WALCheckpoint{
			MessageID: rmq.NewRmqID(300),
			TimeTick:  300,
		}),
	})
	rs.windowManager.markActiveViewsInitialized()
	rs.SetLogger(resource.Resource().Logger())

	require.NoError(t, rs.windowManager.cleanPChannelWindow(ctx, resource.Resource().Logger()))
	require.Equal(t, uint64(3), catalogState.storeMeta.GetMinAvailableGeneration())
	require.Equal(t, uint64(3), catalogState.storeMeta.GetMinInUseGeneration())
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 0, false)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 1, false)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 2, false)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 3, true)
}

func TestPChannelWindowCleanerReclaimsFromMinAvailableLeavingLowerChunks(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := newTestPChannelWindowCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))
	writeTestPChannelWindowChunks(t, ctx, "p1", chunkManager, 0, 4)
	catalogState.storeMeta = testPChannelWindowStoreMeta(t, ctx, "p1", chunkManager, 4, 2, 2)

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(300),
		TimeTick:  300,
	})
	addTestIdempotencyWindowPinnedAtGeneration(rs.windowManager, "v1", 3)
	rs.windowManager.markActiveViewsInitialized()
	rs.SetLogger(resource.Resource().Logger())

	require.NoError(t, rs.windowManager.cleanPChannelWindow(ctx, resource.Resource().Logger()))
	require.Equal(t, uint64(3), catalogState.storeMeta.GetMinAvailableGeneration())
	require.Equal(t, uint64(3), catalogState.storeMeta.GetMinInUseGeneration())
	// Chunks below the old MinAvailableGeneration (2) are assumed already deleted
	// and are never re-scanned, so any residual chunks there are left untouched.
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 0, true)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 1, true)
	// [MinAvailableGeneration, MinInUseGeneration) = [2, 3) is reclaimed.
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 2, false)
	// [MinInUseGeneration, LatestGeneration] = [3, 4] is still in use.
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 3, true)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 4, true)
}

func TestPChannelWindowCleanerReDeletesIdempotentlyAfterCrashBeforeSave(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := newTestPChannelWindowCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))
	writeTestPChannelWindowChunks(t, ctx, "p1", chunkManager, 0, 3)
	// A prior cleaner cycle persisted the advanced MinInUseGeneration (2), then
	// deleted chunk 0 from [MinAvailableGeneration, MinInUseGeneration) but
	// crashed before completing the deletions and advancing
	// MinAvailableGeneration, so the meta still says MinAvailable=0. The next
	// cycle must re-delete the range idempotently and finish the advance.
	catalogState.storeMeta = testPChannelWindowStoreMeta(t, ctx, "p1", chunkManager, 3, 0, 2)
	require.NoError(t, chunkManager.Remove(ctx, buildPChannelWindowChunkKey("p1", 0, 0)))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(300),
		TimeTick:  300,
	})
	addTestIdempotencyWindowPinnedAtGeneration(rs.windowManager, "v1", 2)
	rs.windowManager.markActiveViewsInitialized()
	rs.SetLogger(resource.Resource().Logger())

	require.NoError(t, rs.windowManager.cleanPChannelWindow(ctx, resource.Resource().Logger()))
	require.Equal(t, uint64(2), catalogState.storeMeta.GetMinAvailableGeneration())
	require.Equal(t, uint64(2), catalogState.storeMeta.GetMinInUseGeneration())
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 0, false) // already gone, skipped
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 1, false) // reclaimed this cycle
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 2, true)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 3, true)
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
func TestPChannelWindowCleanerPersistsMinInUseBeforeDeleting(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := newTestPChannelWindowCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(removeFailingChunkManager{chunkManager}))
	writeTestPChannelWindowChunks(t, ctx, "p1", chunkManager, 0, 3)
	catalogState.storeMeta = testPChannelWindowStoreMeta(t, ctx, "p1", chunkManager, 3, 0, 0)

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(300),
		TimeTick:  300,
	})
	addTestIdempotencyWindowPinnedAtGeneration(rs.windowManager, "v1", 2)
	rs.windowManager.markActiveViewsInitialized()
	rs.SetLogger(resource.Resource().Logger())

	// The cycle aborts on the injected deletion failure...
	require.Error(t, rs.windowManager.cleanPChannelWindow(ctx, resource.Resource().Logger()))
	// ...but the in-use advance is already durable, the low-water is not, and
	// no chunk is missing below the persisted MinInUseGeneration.
	require.Equal(t, uint64(2), catalogState.storeMeta.GetMinInUseGeneration())
	require.Equal(t, uint64(0), catalogState.storeMeta.GetMinAvailableGeneration())
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 2, true)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 3, true)

	// A restart in this state must recover: the replay range persisted in the
	// meta only references live chunks.
	recovered := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(300),
		TimeTick:  300,
	})
	recovered.SetLogger(resource.Resource().Logger())
	_, err := recovered.windowManager.recoverIdempotencyWindowsFromPChannelWindowStore(ctx, "p1", pchannelWindowStoreMetaFromCatalog(catalogState.storeMeta))
	require.NoError(t, err)
}

// addTestIdempotencyWindowPinnedAtGeneration injects an idempotency window whose
// min-required generation is pinned at the given generation, giving cleaner tests
// a precise GC boundary (idempotency is the only window view that participates in
// the boundary).
func addTestIdempotencyWindowPinnedAtGeneration(m *windowManager, vchannel string, generation uint64) {
	window := newEmptyVChannelWindow(m.pchannel, vchannel, nil)
	key := "pin-" + vchannel
	window.entries[key] = &windowEntry{
		entry:         &streamingpb.WindowEntry{Key: key},
		generation:    generation,
		generationSet: true,
	}
	window.latestAppliedGeneration = generation
	window.refreshMinRequiredGeneration()
	m.setIdempotencyWindow(vchannel, window)
}

func newTestPChannelWindowCleanerChunkManager() storage.ChunkManager {
	return storage.NewLocalChunkManager(objectstorage.RootPath(paramtable.Get().MinioCfg.RootPath.GetValue()))
}

func writeTestPChannelWindowChunks(t *testing.T, ctx context.Context, pchannel string, chunkManager storage.ChunkManager, startGeneration uint64, endGeneration uint64) {
	t.Helper()
	for generation := startGeneration; generation <= endGeneration; generation++ {
		writeTestPChannelWindowChunk(ctx, t, pchannel, generation, chunkManager, &utility.WALCheckpoint{
			MessageID: rmq.NewRmqID(int64(generation + 1)),
			TimeTick:  generation + 1,
		}, nil)
	}
}

func testPChannelWindowStoreMeta(
	t *testing.T,
	ctx context.Context,
	pchannel string,
	chunkManager storage.ChunkManager,
	latestGeneration uint64,
	minAvailableGeneration uint64,
	minInUseGeneration uint64,
) *streamingpb.PChannelWindowMeta {
	t.Helper()
	footer, _, _ := writeTestPChannelWindowChunk(ctx, t, pchannel, latestGeneration, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(int64(latestGeneration + 1)),
		TimeTick:  latestGeneration + 1,
	}, nil)
	return newPChannelWindowStoreMetaFromChunk(pchannel, footer, minAvailableGeneration, minInUseGeneration).intoCatalogMeta()
}

func requirePChannelWindowChunkExists(t *testing.T, ctx context.Context, chunkManager storage.ChunkManager, pchannel string, generation uint64, expected bool) {
	t.Helper()
	exists, err := chunkManager.Exist(ctx, buildPChannelWindowChunkKey(pchannel, generation, 0))
	require.NoError(t, err)
	require.Equal(t, expected, exists)
}
