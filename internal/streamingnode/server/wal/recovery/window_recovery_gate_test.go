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
	footer, _, _ := writeTestPChannelWindowChunk(ctx, t, "p1", 0, chunkManager, &utility.WALCheckpoint{
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

// A persist writes the chunk before the pchannel meta, so a crash in between
// leaves an orphan chunk above LatestGeneration. Dropping the store must reap
// that orphan too: a chunk kept behind outlives the metas that referenced the
// store, and permanently fails the write-if-absent of the same generation once
// idempotency is re-enabled.
func TestRecoverWindowsDropsOrphanChunksWhenIdempotencyDisabled(t *testing.T) {
	ctx := context.Background()
	params := paramtable.Get()
	params.Save(params.StreamingCfg.IdempotencyEnabled.Key, "false")
	t.Cleanup(func() { params.Reset(params.StreamingCfg.IdempotencyEnabled.Key) })

	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	footer, _, _ := writeTestPChannelWindowChunk(ctx, t, "p1", 0, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(10),
		TimeTick:  10,
	}, nil)
	// The meta only ever referenced generation 0; generation 1 is the orphan of a
	// persist that crashed after writing the chunk.
	writeTestPChannelWindowChunk(ctx, t, "p1", 1, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(20),
		TimeTick:  20,
	}, nil)
	catalogState.storeMeta = newPChannelWindowStoreMetaFromChunk("p1", footer, 0, 0).intoCatalogMeta()

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(100),
		TimeTick:  100,
	})
	rs.SetLogger(resource.Resource().Logger())

	_, err := rs.windowManager.recoverWindows(ctx, "p1", rs.checkpoint, rs.vchannels)
	require.NoError(t, err)
	require.Nil(t, catalogState.storeMeta)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 0, false)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 1, false)
}

// A stale opener can observe no pchannel meta, pause, and resume after another
// owner has bootstrapped. Bootstrap must therefore not prefix-delete chunks
// based on the earlier no-meta read; it publishes only the current term's
// generation-0 chunk through the pchannel meta CAS and leaves unrelated
// term-suffixed leftovers unreferenced.
func TestBootstrapDoesNotReapChunksLeftByIncompleteDrop(t *testing.T) {
	ctx := context.Background()
	params := paramtable.Get()
	params.Save(params.StreamingCfg.IdempotencyEnabled.Key, "true")
	t.Cleanup(func() { params.Reset(params.StreamingCfg.IdempotencyEnabled.Key) })

	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	// Leftovers of a store whose metas are already gone. Their term suffix keeps
	// them from colliding with the new bootstrap chunk, and bootstrap must not
	// delete them based only on catalog absence.
	_, gen0Term1Key, _ := writeTestPChannelWindowChunkWithTerm(ctx, t, "p1", 0, 1, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(10),
		TimeTick:  10,
	}, nil)
	_, gen3Term1Key, _ := writeTestPChannelWindowChunkWithTerm(ctx, t, "p1", 3, 1, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(40),
		TimeTick:  40,
	}, nil)

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1", Term: 2}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(100),
		TimeTick:  100,
	})
	rs.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	rs.SetLogger(resource.Resource().Logger())

	checkpoint, err := rs.windowManager.recoverWindows(ctx, "p1", rs.checkpoint, rs.vchannels)
	require.NoError(t, err)
	// The store is bootstrapped from the current checkpoint, not from the stale
	// leftovers, and the stale leftovers are not referenced by the new manifest.
	require.Equal(t, uint64(100), checkpoint.TimeTick)
	require.NotNil(t, catalogState.storeMeta)
	require.Equal(t, uint64(0), catalogState.storeMeta.GetLatestGeneration())
	require.Equal(t, uint64(100), catalogState.storeMeta.GetSourceCheckpointTimetick())
	require.Equal(t, int64(2), catalogState.storeMeta.GetChunkManifest().GetRanges()[0].GetTerm())
	exists, err := chunkManager.Exist(ctx, gen0Term1Key)
	require.NoError(t, err)
	require.True(t, exists)
	exists, err = chunkManager.Exist(ctx, gen3Term1Key)
	require.NoError(t, err)
	require.True(t, exists)
}

// Window lifecycle (creation) belongs on vchannel events, not the per-message
// observe loop: the per-message checkpoint advance must not materialize windows
// for active vchannels, while a vchannel becoming active does create its window.
func TestIdempotencyWindowLifecycleMovedToVChannelEvents(t *testing.T) {
	enableRecoveryIdempotency(t)
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

// A corrupted chunk the meta references fails the WAL open: the window
// snapshot checkpoint gated WAL truncation, so a referenced chunk is the only
// durable copy of the idempotency keys below the consume checkpoint. Silently
// resetting to an empty window (the old behavior) would accept in-TTL client
// retries as fresh writes — duplicate data with no error anywhere. Here the
// consume checkpoint (120) has advanced together with the chunk, modeling the
// truncated-WAL reality where key-1@99 is not replayable.
func TestRecoverWindowsFailsOnReferencedChunkCorruption(t *testing.T) {
	enableRecoveryIdempotency(t)
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
	footer, key, _ := writeTestPChannelWindowChunk(ctx, t, "p1", 0, chunkManager, &utility.WALCheckpoint{
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
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	})
	rs.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	rs.SetLogger(resource.Resource().Logger())

	_, err = rs.windowManager.recoverWindows(ctx, "p1", rs.checkpoint, rs.vchannels)
	require.ErrorIs(t, err, ErrPChannelWindowStoreCorrupted)
	require.ErrorContains(t, err, "streaming.idempotency.enabled=false")
	require.False(t, rs.windowManager.activeViewsInitialized)
}

// A corrupt chunk ABOVE the durable LatestGeneration is an orphan: the meta
// never referenced it, its source data is still in the WAL, and the recovery
// probe drops it inline — recovery must still succeed with the referenced
// window intact.
func TestRecoverWindowsSelfHealsCorruptOrphanChunk(t *testing.T) {
	enableRecoveryIdempotency(t)
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
	footer, _, _ := writeTestPChannelWindowChunk(ctx, t, "p1", 0, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	}, records)
	catalogState.storeMeta = newPChannelWindowStoreMetaFromChunk("p1", footer, 0, 0).intoCatalogMeta()

	// An orphan at generation 1 from a persist that crashed before the meta
	// advanced; corrupt it.
	_, orphanKey, _ := writeTestPChannelWindowChunk(ctx, t, "p1", 1, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(130),
		TimeTick:  130,
	}, nil)
	payload, err := chunkManager.Read(ctx, orphanKey)
	require.NoError(t, err)
	payload[0] ^= 0x01
	require.NoError(t, chunkManager.Write(ctx, orphanKey, payload))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	})
	rs.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	rs.SetLogger(resource.Resource().Logger())

	_, err = rs.windowManager.recoverWindows(ctx, "p1", rs.checkpoint, rs.vchannels)
	require.NoError(t, err)
	require.True(t, rs.windowManager.activeViewsInitialized)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 1, false)
	window, ok := rs.windowManager.idempotencyWindows()["v1"]
	require.True(t, ok)
	require.Len(t, window.entries, 1)
	require.Contains(t, window.entries, "key-1")
}
