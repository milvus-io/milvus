package recovery

import (
	"context"
	"testing"

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

// When idempotency is disabled, window-store recovery is skipped: the window
// cache is never consulted, so the recovery path must not bootstrap any state.
// It only probes the catalog to drop a store left behind by an earlier enabled
// run; with nothing persisted, no writes happen at all.
func TestRecoverWindowsSkipsWhenIdempotencyDisabled(t *testing.T) {
	ctx := context.Background()
	params := paramtable.Get()
	params.Save(params.StreamingCfg.IdempotencyEnabled.Key, "false")
	t.Cleanup(func() { params.Reset(params.StreamingCfg.IdempotencyEnabled.Key) })

	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	rs.SetLogger(resource.Resource().Logger())

	_, err := rs.windowManager.recoverWindows(ctx, "p1", rs.checkpoint, rs.vchannels)
	require.NoError(t, err)

	// No window state is set up, no active views are marked initialized, and no
	// bootstrap chunk or catalog meta is written.
	require.Empty(t, rs.windowManager.idempotencyWindows())
	require.False(t, rs.windowManager.activeViewsInitialized)
	require.Nil(t, catalogState.storeMeta)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 0, false)
}

// Disabling idempotency drops any window store left behind by an earlier
// enabled run: while disabled nothing is recorded into the store, checkpoints
// advance freely and the WAL truncates past the stored SourceCheckpoint, so a
// kept store would rewind recovery to a truncated position on re-enable.
func TestRecoverWindowsDropsStaleStoreWhenIdempotencyDisabled(t *testing.T) {
	ctx := context.Background()
	params := paramtable.Get()
	params.Save(params.StreamingCfg.IdempotencyEnabled.Key, "false")
	t.Cleanup(func() { params.Reset(params.StreamingCfg.IdempotencyEnabled.Key) })

	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	// A store persisted by an earlier enabled run: one chunk, the pchannel meta,
	// and one vchannel window meta.
	footer, _, _ := writeTestPChannelWindowChunk(t, ctx, "p1", 0, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(10),
		TimeTick:  10,
	}, nil)
	catalogState.storeMeta = newPChannelWindowStoreMetaFromChunk("p1", footer, 0, 0).intoCatalogMeta()
	catalogState.windowMetas["v1"] = &streamingpb.VChannelWindowMeta{Pchannel: "p1", Vchannel: "v1"}

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(100),
		TimeTick:  100,
	})
	rs.SetLogger(resource.Resource().Logger())

	checkpoint, err := rs.windowManager.recoverWindows(ctx, "p1", rs.checkpoint, rs.vchannels)
	require.NoError(t, err)
	// The checkpoint is returned unchanged: no window replay rewind while disabled.
	require.Equal(t, uint64(100), checkpoint.TimeTick)
	// The stale store is fully dropped: both metas and the chunk are gone, so a
	// later re-enable bootstraps from the then-current checkpoint.
	require.Nil(t, catalogState.storeMeta)
	require.Empty(t, catalogState.windowMetas)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 0, false)
}

// Window lifecycle (creation) belongs on vchannel events, not the per-message
// observe loop: the per-message checkpoint advance must not materialize windows
// for active vchannels, while a vchannel becoming active does create its window.
func TestIdempotencyWindowLifecycleMovedToVChannelEvents(t *testing.T) {
	catalog, _ := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	rs.SetLogger(resource.Resource().Logger())

	// The per-message checkpoint advance must not create windows for active vchannels.
	rs.windowManager.advanceIdempotencyWindowCheckpoints(rs.checkpoint)
	require.Empty(t, rs.windowManager.idempotencyWindows())

	// Window creation happens on the vchannel lifecycle event.
	rs.windowManager.ensureIdempotencyWindow("v1", rs.checkpoint)
	require.Len(t, rs.windowManager.idempotencyWindows(), 1)
	require.Contains(t, rs.windowManager.idempotencyWindows(), "v1")

	// Re-ensuring is idempotent.
	rs.windowManager.ensureIdempotencyWindow("v1", rs.checkpoint)
	require.Len(t, rs.windowManager.idempotencyWindows(), 1)
}

// The durable window store is a rebuildable cache, not a source of truth: a
// corrupted chunk must not abort WAL recovery. Recovery drops the corrupted
// cache and continues with an empty, initialized window set.
func TestRecoverWindowsTreatsCorruptionAsNonFatal(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	records := map[string][]committedWriteRecord{
		"v1": {
			*committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
				Key:            "key-1",
				CommitTimetick: 99,
				MessageId:      rmq.NewRmqID(99).IntoProto(),
			}),
		},
	}
	footer, key, _ := writeTestPChannelWindowChunk(t, ctx, "p1", 0, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	}, records)
	catalogState.storeMeta = newPChannelWindowStoreMetaFromChunk("p1", footer, 0, 0).intoCatalogMeta()

	// Corrupt the persisted chunk header magic so recovery hits a decode failure.
	payload, err := chunkManager.Read(ctx, key)
	require.NoError(t, err)
	payload[0] ^= 0x01
	require.NoError(t, chunkManager.Write(ctx, key, payload))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	rs.SetLogger(resource.Resource().Logger())

	// Corruption is swallowed: recovery succeeds with a clean, empty window cache.
	_, err = rs.windowManager.recoverWindows(ctx, "p1", rs.checkpoint, rs.vchannels)
	require.NoError(t, err)
	require.True(t, rs.windowManager.activeViewsInitialized)
	for vchannel, window := range rs.windowManager.idempotencyWindows() {
		require.Emptyf(t, window.snapshot().GetEntries(), "window %s should be reset to empty after corruption", vchannel)
	}
}
