package recovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func disableRecoveryIdempotency(t *testing.T) {
	t.Helper()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.StreamingCfg.IdempotencyEnabled.Key, "false"))
	t.Cleanup(func() { _ = params.Reset(params.StreamingCfg.IdempotencyEnabled.Key) })
}

func newTestRecoveryStorageForSummary(t *testing.T, timetick uint64, vchannels ...string) *recoveryStorageImpl {
	t.Helper()
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, testRecoveryCheckpoint(int64(timetick), timetick))
	if len(vchannels) > 0 {
		metas := make([]*streamingpb.VChannelMeta, 0, len(vchannels))
		for _, vchannel := range vchannels {
			metas = append(metas, &streamingpb.VChannelMeta{
				Vchannel: vchannel,
				State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			})
		}
		rs.vchannels = newVChannelRecoveryInfoFromVChannelMeta(metas)
	}
	rs.SetLogger(resource.Resource().Logger())
	return rs
}

// When idempotency is disabled, summary-store recovery is skipped: the summary
// cache is never consulted, so the recovery path must not bootstrap any state.
// It only probes the catalog to drop a store left behind by an earlier enabled
// run; with nothing persisted, no writes happen at all.
func TestRecoverSummariesSkipsWhenIdempotencyDisabled(t *testing.T) {
	ctx := context.Background()
	disableRecoveryIdempotency(t)

	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := newTestSummaryStoreChunkManager(t)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	rs := newTestRecoveryStorageForSummary(t, 1, "v1")
	_, err := rs.summaryManager.recoverSummaries(ctx, "p1", rs.checkpoint, rs.vchannels)
	require.NoError(t, err)

	require.Empty(t, rs.summaryManager.summaries())
	require.False(t, rs.summaryManager.activeViewsInitialized)
	require.Nil(t, catalogState.storeMeta)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 0, 0, false)
}

// Disabling idempotency drops any summary store left behind by an earlier
// enabled run: while disabled nothing is recorded, checkpoints advance freely
// and the WAL truncates past what the store covers, so a kept store would be
// stale by definition on re-enable.
func TestRecoverSummariesDropsStaleStoreWhenIdempotencyDisabled(t *testing.T) {
	ctx := context.Background()
	disableRecoveryIdempotency(t)

	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := newTestSummaryStoreChunkManager(t)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	writeTestPChannelSummaryChunk(ctx, t, chunkManager, "p1", 0, 0, map[string][]*SummaryRecord{
		"v1": {newTestSummaryRecord("key-1", 10, 1)},
	})
	catalogState.storeMeta = (&pchannelSummaryStoreMeta{PChannel: "p1"}).intoCatalogMeta()

	rs := newTestRecoveryStorageForSummary(t, 100)
	checkpoint, err := rs.summaryManager.recoverSummaries(ctx, "p1", rs.checkpoint, rs.vchannels)
	require.NoError(t, err)
	// The checkpoint is returned unchanged; nothing about the store moves it.
	require.Equal(t, uint64(100), checkpoint.TimeTick)
	require.Nil(t, catalogState.storeMeta)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 0, 0, false)
}

// A chunk is written before the manifest that names it, so a crash in between
// leaves an object no manifest references. Dropping the store must reap those
// too -- the enabled path reaps them when retention passes, but the drop path
// has no later pass.
func TestRecoverSummariesDropsUnreferencedChunksWhenIdempotencyDisabled(t *testing.T) {
	ctx := context.Background()
	disableRecoveryIdempotency(t)

	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := newTestSummaryStoreChunkManager(t)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	records := map[string][]*SummaryRecord{"v1": {newTestSummaryRecord("key-1", 10, 1)}}
	writeTestPChannelSummaryChunk(ctx, t, chunkManager, "p1", 0, 0, records)
	writeTestPChannelSummaryChunk(ctx, t, chunkManager, "p1", 1, 0, records)
	catalogState.storeMeta = (&pchannelSummaryStoreMeta{PChannel: "p1"}).intoCatalogMeta()

	rs := newTestRecoveryStorageForSummary(t, 100)
	_, err := rs.summaryManager.recoverSummaries(ctx, "p1", rs.checkpoint, rs.vchannels)
	require.NoError(t, err)
	require.Nil(t, catalogState.storeMeta)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 0, 0, false)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 1, 0, false)
}

// Summary lifecycle (creation) belongs on vchannel events, not the per-message
// observe loop: observing a message must not materialize summaries for active
// vchannels, while a vchannel becoming active does create its summary.
func TestIdempotencySummaryLifecycleMovedToVChannelEvents(t *testing.T) {
	enableRecoveryIdempotency(t)
	catalog, _ := newTestPChannelSummaryCatalog(t)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(newTestSummaryStoreChunkManager(t)))

	rs := newTestRecoveryStorageForSummary(t, 1, "v1")

	rs.summaryManager.observeMessage(buildTimeTickMessage(t, 2))
	require.Empty(t, rs.summaryManager.summaries())

	rs.summaryManager.ensureSummary("v1", rs.checkpoint)
	require.Len(t, rs.summaryManager.summaries(), 1)
	require.Contains(t, rs.summaryManager.summaries(), "v1")

	// Re-ensuring is idempotent.
	rs.summaryManager.ensureSummary("v1", rs.checkpoint)
	require.Len(t, rs.summaryManager.summaries(), 1)
}

// A corrupt chunk the manifest RETAINS fails the WAL open. The WAL is truncated
// on the consume checkpoint, which advanced only after that chunk was durable,
// so the chunk is the only remaining copy of those keys. Silently starting with
// an empty window would accept in-retention client retries as fresh writes --
// duplicate data with no error anywhere.
func TestRecoverSummariesFailsOnRetainedChunkCorruption(t *testing.T) {
	enableRecoveryIdempotency(t)
	ctx := context.Background()
	catalog, _ := newTestPChannelSummaryCatalog(t)
	chunkManager := newTestSummaryStoreChunkManager(t)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	entry := writeTestPChannelSummaryChunk(ctx, t, chunkManager, "p1", 0, 0, map[string][]*SummaryRecord{
		"v1": {newTestSummaryRecord("key-1", 99, 1)},
	})
	writeTestPChannelSummaryManifest(ctx, t, "p1", 0, entry)

	key := buildPChannelSummaryChunkKey(chunkManager, "p1", 0, 0)
	payload, err := chunkManager.Read(ctx, key)
	require.NoError(t, err)
	payload[0] ^= 0x01
	require.NoError(t, chunkManager.Write(ctx, key, payload))

	rs := newTestRecoveryStorageForSummary(t, 120, "v1")
	_, err = rs.summaryManager.recoverSummaries(ctx, "p1", rs.checkpoint, rs.vchannels)
	require.ErrorIs(t, err, ErrPChannelSummaryStoreCorrupted)
	require.ErrorContains(t, err, "streaming.idempotency.enabled=false")
	require.False(t, rs.summaryManager.activeViewsInitialized)
}

// A corrupt chunk ABOVE what the manifest records is the opposite case. The
// persist that wrote it had to write the manifest next, and failing that fails
// the whole checkpoint persist -- so its writes are still in the WAL. Recovery
// drops it and succeeds, with the retained summary intact.
func TestRecoverSummariesSelfHealsCorruptChunkAboveManifest(t *testing.T) {
	enableRecoveryIdempotency(t)
	ctx := context.Background()
	catalog, _ := newTestPChannelSummaryCatalog(t)
	chunkManager := newTestSummaryStoreChunkManager(t)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	entry := writeTestPChannelSummaryChunk(ctx, t, chunkManager, "p1", 0, 0, map[string][]*SummaryRecord{
		"v1": {newTestSummaryRecord("key-1", 99, 1)},
	})
	writeTestPChannelSummaryManifest(ctx, t, "p1", 0, entry)

	writeTestPChannelSummaryChunk(ctx, t, chunkManager, "p1", 1, 0, map[string][]*SummaryRecord{
		"v1": {newTestSummaryRecord("key-2", 130, 2)},
	})
	orphanKey := buildPChannelSummaryChunkKey(chunkManager, "p1", 1, 0)
	payload, err := chunkManager.Read(ctx, orphanKey)
	require.NoError(t, err)
	payload[0] ^= 0x01
	require.NoError(t, chunkManager.Write(ctx, orphanKey, payload))

	rs := newTestRecoveryStorageForSummary(t, 120, "v1")
	_, err = rs.summaryManager.recoverSummaries(ctx, "p1", rs.checkpoint, rs.vchannels)
	require.NoError(t, err)
	require.True(t, rs.summaryManager.activeViewsInitialized)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 1, 0, false)

	summary, ok := rs.summaryManager.summaries()["v1"]
	require.True(t, ok)
	require.Len(t, summary.records, 1)
	require.Contains(t, summary.records, "key-1")
}

// Dropping the store must remove the MANIFESTS too. They live beside the chunks
// directory rather than inside it, so a sweep of the chunks prefix alone leaves
// the only index into the store behind -- and a later re-enable finds it and
// fails the WAL open on objects that are no longer there.
func TestRecoverSummariesDropsManifestsWhenIdempotencyDisabled(t *testing.T) {
	ctx := context.Background()

	// Build a real store under an enabled run first.
	chunkManager, catalogState := newTestSummaryStore(t)
	writer := recoverTestSummaryManager(t, ctx, 1, 5, "v1")
	writer.setNormalMode()
	_, err := writer.persistPChannelSummary(ctx, writer.Logger(), map[string][]*SummaryRecord{
		"v1": {newTestSummaryRecord("key-a", 20, 100)},
	})
	require.NoError(t, err)
	require.NotNil(t, catalogState.storeMeta)

	manifestKey := buildPChannelSummaryManifestKey(chunkManager, "p1", 1)
	exists, err := chunkManager.Exist(ctx, manifestKey)
	require.NoError(t, err)
	require.True(t, exists)

	// Now open with the feature off.
	disableRecoveryIdempotency(t)
	rs := newTestRecoveryStorageForSummary(t, 100)
	_, err = rs.summaryManager.recoverSummaries(ctx, "p1", rs.checkpoint, rs.vchannels)
	require.NoError(t, err)

	exists, err = chunkManager.Exist(ctx, manifestKey)
	require.NoError(t, err)
	require.False(t, exists, "the manifest must be swept, not only the chunks")
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 0, 1, false)
	require.Nil(t, catalogState.storeMeta)
}
