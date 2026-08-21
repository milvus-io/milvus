package recovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// newTestSummaryStore initialises the process resources a summary manager reads
// through and returns the chunk manager backing them.
func newTestSummaryStore(t *testing.T) (storage.ChunkManager, *testPChannelSummaryCatalogState) {
	t.Helper()
	enableRecoveryIdempotency(t)
	catalog, catalogState := newTestPChannelSummaryCASCatalog(t)
	chunkManager := newTestSummaryStoreChunkManager(t)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))
	return chunkManager, catalogState
}

// recoverTestSummaryManager brings up one owner at a term and runs the whole
// recovery sequence, exactly as WAL open does.
func recoverTestSummaryManager(t *testing.T, ctx context.Context, term int64, timetick uint64, vchannels ...string) *summaryManager {
	t.Helper()
	manager := newTestSummaryManager(t, "p1", term, newTestSummaryConfig())
	rs := newTestRecoveryStorageForSummary(t, timetick, vchannels...)
	_, err := manager.recoverFromSummaryStore(ctx, "p1", rs.checkpoint, rs.vchannels)
	require.NoError(t, err)
	return manager
}

func TestSummaryStoreRoundTripsAcrossTerms(t *testing.T) {
	ctx := context.Background()
	newTestSummaryStore(t)

	writer := recoverTestSummaryManager(t, ctx, 1, 10, "v1", "v2")
	writer.setNormalMode()
	generation, err := writer.persistPChannelSummary(ctx, writer.Logger(), map[string][]*SummaryRecord{
		"v1": {newTestSummaryRecord("key-a", 20, 100), newTestSummaryRecord("key-b", 21, 101)},
		"v2": {newTestSummaryRecord("key-c", 22, 200)},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(0), generation)

	// A new owner reads the previous term's manifest and replays what it retains.
	reader := recoverTestSummaryManager(t, ctx, 2, 10, "v1", "v2")
	require.Equal(t, uint64(1), reader.nextGeneration)

	v1 := reader.summaries()["v1"]
	require.Len(t, v1.records, 2)
	require.Contains(t, v1.records, "key-a")
	// The duplicate response survives the section split intact.
	require.Equal(t, []int64{100}, v1.records["key-a"].InsertResult.GetIds().GetIntId().GetData())
	require.Equal(t, []uint32{0}, v1.records["key-a"].InsertResult.GetRowOffsets())
	require.Len(t, reader.summaries()["v2"].records, 1)
}

func TestSummaryStoreRecoveryProbesChunkWrittenAfterManifest(t *testing.T) {
	ctx := context.Background()
	chunkManager, _ := newTestSummaryStore(t)

	writer := recoverTestSummaryManager(t, ctx, 1, 10, "v1")
	writer.setNormalMode()
	_, err := writer.persistPChannelSummary(ctx, writer.Logger(), map[string][]*SummaryRecord{
		"v1": {newTestSummaryRecord("key-a", 20, 100)},
	})
	require.NoError(t, err)

	// A chunk made durable while the manifest write never landed. The manifest is
	// written after its chunk, so this is exactly what a crash in between leaves.
	writeTestPChannelSummaryChunk(ctx, t, chunkManager, "p1", 1, 1, map[string][]*SummaryRecord{
		"v1": {newTestSummaryRecord("key-b", 30, 101)},
	})

	reader := recoverTestSummaryManager(t, ctx, 2, 10, "v1")
	require.Contains(t, reader.summaries()["v1"].records, "key-b")
	require.Equal(t, uint64(2), reader.nextGeneration)

	// The probed tail must be sealed into THIS term's manifest. A later recovery
	// probes only within its own term, so a tail left unrecorded here would be
	// unreachable forever.
	manifest, found, err := readPChannelSummaryManifest(ctx, "p1", 2)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, manifest.GetChunks(), 2)
	require.Equal(t, uint64(1), manifest.GetChunks()[1].GetGeneration())
	require.Equal(t, int64(1), manifest.GetChunks()[1].GetTerm())
}

func TestSummaryStoreRecoveryProbesFromGenerationZero(t *testing.T) {
	ctx := context.Background()
	chunkManager, _ := newTestSummaryStore(t)

	// A term that published its manifest, wrote its first chunk, and died. The
	// manifest names nothing, so a probe that started above "the newest recorded
	// chunk" would never look at generation 0 and would drop it permanently.
	writeTestPChannelSummaryManifest(ctx, t, "p1", 1)
	writeTestPChannelSummaryChunk(ctx, t, chunkManager, "p1", 0, 1, map[string][]*SummaryRecord{
		"v1": {newTestSummaryRecord("key-a", 20, 100)},
	})

	reader := recoverTestSummaryManager(t, ctx, 2, 10, "v1")
	require.Contains(t, reader.summaries()["v1"].records, "key-a")
	require.Equal(t, uint64(1), reader.nextGeneration)
}

func TestSummaryStoreRecoveryPublishesManifestBeforeAnyChunk(t *testing.T) {
	ctx := context.Background()
	newTestSummaryStore(t)

	recoverTestSummaryManager(t, ctx, 4, 10, "v1")

	// The manifest of this term must exist before it may write a chunk: a chunk
	// written without one is a chunk recovery can never find, because probing
	// starts from a manifest.
	manifest, found, err := readPChannelSummaryManifest(ctx, "p1", 4)
	require.NoError(t, err)
	require.True(t, found)
	require.Empty(t, manifest.GetChunks())
}

func TestSummaryStoreRecoveryStopsWhenMetaOwnedByNewerTerm(t *testing.T) {
	ctx := context.Background()
	_, catalogState := newTestSummaryStore(t)
	catalogState.storeMeta = (&pchannelSummaryStoreMeta{PChannel: "p1", Term: 9}).intoCatalogMeta()

	manager := newTestSummaryManager(t, "p1", 3, newTestSummaryConfig())
	rs := newTestRecoveryStorageForSummary(t, 10, "v1")
	_, err := manager.recoverFromSummaryStore(ctx, "p1", rs.checkpoint, rs.vchannels)
	require.ErrorIs(t, err, ErrPChannelSummaryStoreFenced)
}

func TestSummaryStoreRecoveryStopsOnChunkFromNewerTerm(t *testing.T) {
	ctx := context.Background()
	chunkManager, _ := newTestSummaryStore(t)

	// The manifest is this owner's own, but it names a chunk stamped with a term
	// above ours: another owner has taken the store over.
	entry := writeTestPChannelSummaryChunk(ctx, t, chunkManager, "p1", 0, 7, map[string][]*SummaryRecord{
		"v1": {newTestSummaryRecord("key-a", 20, 100)},
	})
	writeTestPChannelSummaryManifest(ctx, t, "p1", 2, entry)

	manager := newTestSummaryManager(t, "p1", 3, newTestSummaryConfig())
	rs := newTestRecoveryStorageForSummary(t, 10, "v1")
	_, err := manager.recoverFromSummaryStore(ctx, "p1", rs.checkpoint, rs.vchannels)
	require.ErrorIs(t, err, ErrPChannelSummaryStoreFenced)
}

func TestSummaryStoreRecoveryReplaysOnlyRetainedChunks(t *testing.T) {
	ctx := context.Background()
	chunkManager, _ := newTestSummaryStore(t)

	released := writeTestPChannelSummaryChunk(ctx, t, chunkManager, "p1", 0, 1, map[string][]*SummaryRecord{
		"v1": {newTestSummaryRecord("key-old", 10, 1)},
	})
	retained := writeTestPChannelSummaryChunk(ctx, t, chunkManager, "p1", 1, 1, map[string][]*SummaryRecord{
		"v1": {newTestSummaryRecord("key-new", 20, 2)},
	})

	// Generation 0 has been released by retention: it is queued for deletion and
	// no longer part of the set recovery reads, even though the object still
	// exists because gc has not run.
	manifest := &streamingpb.PChannelSummaryManifest{
		Chunks:    []*streamingpb.PChannelSummaryChunkIndexEntry{retained},
		PendingGc: []*streamingpb.PChannelSummaryChunkRef{{Generation: released.GetGeneration(), Term: released.GetTerm()}},
	}
	require.NoError(t, writePChannelSummaryManifest(ctx, "p1", 1, manifest))

	reader := recoverTestSummaryManager(t, ctx, 2, 5, "v1")
	require.Contains(t, reader.summaries()["v1"].records, "key-new")
	require.NotContains(t, reader.summaries()["v1"].records, "key-old")

	// The queued deletion carries forward: a new owner inherits the manifest
	// rather than rebuilding it, so unfinished work is never dropped.
	require.Len(t, reader.manifest.GetPendingGc(), 1)
}

func TestSummaryStoreRecoverySkipsRecordsAlreadyCoveredByCheckpoint(t *testing.T) {
	ctx := context.Background()
	newTestSummaryStore(t)

	writer := recoverTestSummaryManager(t, ctx, 1, 10, "v1")
	writer.setNormalMode()
	_, err := writer.persistPChannelSummary(ctx, writer.Logger(), map[string][]*SummaryRecord{
		"v1": {newTestSummaryRecord("key-a", 20, 100)},
	})
	require.NoError(t, err)

	// Recovery seeds each vchannel's applied position from the WAL checkpoint, so
	// a message replayed at or below it is not folded in a second time.
	reader := recoverTestSummaryManager(t, ctx, 2, 50, "v1")
	summary := reader.summaries()["v1"]
	require.Equal(t, uint64(50), summary.appliedTimetick)

	summary.observeMessage(newTestIdempotentCommittedInsertMessage(t, "v1", "key-replayed", 50))
	require.NotContains(t, summary.records, "key-replayed")
}

// Recovery must claim the store for its own term, and only after the manifest of
// that term exists. The meta's term is the floor a later opener probes down to,
// so a meta naming a term with no manifest would stop the probe above every
// manifest that does exist and hide the whole store.
func TestSummaryStoreRecoveryClaimsTheStoreAfterPublishingItsManifest(t *testing.T) {
	ctx := context.Background()
	_, catalogState := newTestSummaryStore(t)
	require.Nil(t, catalogState.storeMeta)

	recoverTestSummaryManager(t, ctx, 4, 10, "v1")

	require.NotNil(t, catalogState.storeMeta)
	require.Equal(t, int64(4), catalogState.storeMeta.GetTerm())
	// The claimed term always has a manifest, which is what makes it a usable floor.
	_, found, err := readPChannelSummaryManifest(ctx, "p1", catalogState.storeMeta.GetTerm())
	require.NoError(t, err)
	require.True(t, found)
}

// The claim is what fences a stale owner. Without a writer this branch was
// unreachable, so a stale opener would happily write into a store a newer term
// already owned.
func TestSummaryStoreClaimFencesStaleOwner(t *testing.T) {
	ctx := context.Background()
	_, catalogState := newTestSummaryStore(t)

	recoverTestSummaryManager(t, ctx, 9, 10, "v1")
	require.Equal(t, int64(9), catalogState.storeMeta.GetTerm())

	stale := newTestSummaryManager(t, "p1", 3, newTestSummaryConfig())
	rs := newTestRecoveryStorageForSummary(t, 10, "v1")
	_, err := stale.recoverFromSummaryStore(ctx, "p1", rs.checkpoint, rs.vchannels)
	require.ErrorIs(t, err, ErrPChannelSummaryStoreFenced)
	// The stale open must not have taken the store over.
	require.Equal(t, int64(9), catalogState.storeMeta.GetTerm())
}

// Re-opening at the same term must not spend a CAS round: a WAL open retried
// after a transient failure is the normal case, not a takeover.
func TestSummaryStoreClaimIsIdempotentAtTheSameTerm(t *testing.T) {
	ctx := context.Background()
	_, catalogState := newTestSummaryStore(t)

	recoverTestSummaryManager(t, ctx, 5, 10, "v1")
	writes := len(catalogState.operations)
	require.Positive(t, writes)

	recoverTestSummaryManager(t, ctx, 5, 10, "v1")
	require.Len(t, catalogState.operations, writes)
}
