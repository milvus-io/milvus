package recovery

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// newTestChunkIndexEntry builds a manifest entry of a given object size, which
// is the unit retention accounts in. The vchannel list is filled for realism
// only -- retention never reads it.
func newTestChunkIndexEntry(generation uint64, term int64, endTimetick uint64, objectSize int, vchannels ...string) *streamingpb.PChannelSummaryChunkIndexEntry {
	entry := &streamingpb.PChannelSummaryChunkIndexEntry{
		Generation:    generation,
		Term:          term,
		ObjectSize:    uint64(objectSize),
		StartTimetick: endTimetick,
		EndTimetick:   endTimetick,
	}
	for _, vchannel := range vchannels {
		entry.Vchannels = append(entry.Vchannels, &streamingpb.VChannelSummaryChunkIndex{
			Vchannel:    vchannel,
			Idempotency: &streamingpb.VChannelSummarySectionRef{Length: 1, RecordCount: 1},
			Inserts:     &streamingpb.VChannelSummarySectionRef{Length: 1, RecordCount: 1},
		})
	}
	return entry
}

func testManifestOf(entries ...*streamingpb.PChannelSummaryChunkIndexEntry) *streamingpb.PChannelSummaryManifest {
	return &streamingpb.PChannelSummaryManifest{Chunks: entries}
}

func TestRetentionFloorKeepsRecentBytesRegardlessOfAge(t *testing.T) {
	// Every chunk is far older than the horizon, but the floor is not yet filled,
	// so nothing may be released. This is the outage case the floor exists for: a
	// time-only rule would empty the store exactly when a resuming client needs
	// it.
	manifest := testManifestOf(
		newTestChunkIndexEntry(0, 1, 10, 100, "v1"),
		newTestChunkIndexEntry(1, 1, 20, 100, "v1"),
		newTestChunkIndexEntry(2, 1, 30, 100, "v1"),
	)
	require.Equal(t, 0, retentionBoundary(manifest, 1000, 0, 1_000_000))
}

func TestRetentionReleasesOnlyWhereFloorAndTTLAgree(t *testing.T) {
	manifest := testManifestOf(
		newTestChunkIndexEntry(0, 1, 10, 100, "v1"),
		newTestChunkIndexEntry(1, 1, 20, 100, "v1"),
		newTestChunkIndexEntry(2, 1, 30, 100, "v1"),
		newTestChunkIndexEntry(3, 1, 40, 100, "v1"),
	)

	// Floor of 200 bytes is met by the two newest chunks, so chunks 0 and 1 are
	// outside it. With no TTL horizon nothing expires by age, so nothing goes.
	require.Equal(t, 0, retentionBoundary(manifest, 200, 0, 0))

	// A horizon above chunk 0 only releases chunk 0, even though the floor would
	// allow releasing chunk 1 too.
	require.Equal(t, 1, retentionBoundary(manifest, 200, 0, 20))

	// A horizon above everything still cannot cross the floor.
	require.Equal(t, 2, retentionBoundary(manifest, 200, 0, 1_000_000))
}

func TestRetentionAccountsWholeObjects(t *testing.T) {
	// Retention is decided per object: a chunk is retained or released whole, so
	// what fills the floor is the object's size, not any one vchannel's slice of
	// it. Two chunks of 100 bytes cover a 200-byte floor whatever mix of
	// vchannels wrote them.
	manifest := testManifestOf(
		newTestChunkIndexEntry(0, 1, 10, 100, "v1", "v2"),
		newTestChunkIndexEntry(1, 1, 20, 100, "v1"),
		newTestChunkIndexEntry(2, 1, 30, 100, "v1", "v2", "v3"),
	)
	require.Equal(t, 1, retentionBoundary(manifest, 200, 0, 1_000_000))

	// The consequence, stated as a test: an idle vchannel's history is displaced
	// by the PCHANNEL's write rate. v2 wrote nothing after chunk 0, yet a single
	// newer object large enough to cover the floor releases everything below it,
	// chunk 0 included.
	manifest.Chunks = append(manifest.Chunks, newTestChunkIndexEntry(3, 1, 40, 200, "v1"))
	require.Equal(t, 3, retentionBoundary(manifest, 200, 0, 1_000_000))
}

func TestRetentionMaxChunksOverridesFloor(t *testing.T) {
	// Small objects: the floor is never filled, so floor and TTL alone would
	// retain everything. The cap is what bounds recovery, which pays per chunk.
	entries := make([]*streamingpb.PChannelSummaryChunkIndexEntry, 0, 10)
	for i := 0; i < 10; i++ {
		entries = append(entries, newTestChunkIndexEntry(uint64(i), 1, uint64(10+i), 2, "v1"))
	}
	manifest := testManifestOf(entries...)

	require.Equal(t, 0, retentionBoundary(manifest, 1<<20, 0, 0))
	// Capping at 3 releases everything below the newest three.
	require.Equal(t, 7, retentionBoundary(manifest, 1<<20, 3, 0))
	// A cap above the retained count changes nothing.
	require.Equal(t, 0, retentionBoundary(manifest, 1<<20, 100, 0))
}

func TestRetentionKeepsUndatedChunks(t *testing.T) {
	// A chunk with no span was never dated, so age cannot expire it.
	manifest := testManifestOf(
		newTestChunkIndexEntry(0, 1, 0, 10, "v1"),
		newTestChunkIndexEntry(1, 1, 20, 10, "v1"),
	)
	require.Equal(t, 0, retentionBoundary(manifest, 0, 0, 1_000_000))
}

func TestReleaseBelowRetentionBoundaryMovesChunksToPendingGC(t *testing.T) {
	manifest := testManifestOf(
		newTestChunkIndexEntry(0, 1, 10, 10, "v1"),
		newTestChunkIndexEntry(1, 2, 20, 10, "v1"),
		newTestChunkIndexEntry(2, 2, 30, 10, "v1"),
	)

	require.Equal(t, 2, releaseBelowRetentionBoundary(manifest, 2))

	// Both halves of the move land in one manifest value: recovery stops
	// depending on the objects at the exact moment gc gains the term it needs to
	// name them, so no crash can leave a chunk in neither place.
	require.Len(t, manifest.GetChunks(), 1)
	require.Equal(t, uint64(2), manifest.GetChunks()[0].GetGeneration())
	require.Len(t, manifest.GetPendingGc(), 2)
	require.Equal(t, uint64(0), manifest.GetPendingGc()[0].GetGeneration())
	require.Equal(t, int64(1), manifest.GetPendingGc()[0].GetTerm())
	require.Equal(t, int64(2), manifest.GetPendingGc()[1].GetTerm())
}

func TestReleaseBelowRetentionBoundaryIsNoOpAtZero(t *testing.T) {
	manifest := testManifestOf(newTestChunkIndexEntry(0, 1, 10, 10, "v1"))
	require.Equal(t, 0, releaseBelowRetentionBoundary(manifest, 0))
	require.Len(t, manifest.GetChunks(), 1)
	require.Empty(t, manifest.GetPendingGc())
}

func TestDropCompletedPendingGCKeepsUnfinishedWork(t *testing.T) {
	manifest := &streamingpb.PChannelSummaryManifest{
		PendingGc: []*streamingpb.PChannelSummaryChunkRef{
			{Generation: 0, Term: 1},
			{Generation: 1, Term: 1},
			{Generation: 2, Term: 2},
		},
	}
	dropCompletedPendingGC(manifest, map[pchannelSummaryGCRef]struct{}{
		{term: 1, generation: 1}: {},
	})

	require.Len(t, manifest.GetPendingGc(), 2)
	require.Equal(t, uint64(0), manifest.GetPendingGc()[0].GetGeneration())
	require.Equal(t, uint64(2), manifest.GetPendingGc()[1].GetGeneration())
}

func TestInheritPChannelSummaryManifestFoldsInDiscoveredChunks(t *testing.T) {
	previous := testManifestOf(newTestChunkIndexEntry(0, 1, 10, 10, "v1"))
	previous.PendingGc = []*streamingpb.PChannelSummaryChunkRef{{Generation: 9, Term: 0}}

	// A tail found by probing must be folded in here. A later recovery probes
	// only within its own term, so anything left unrecorded now is unreachable
	// forever.
	inherited := inheritPChannelSummaryManifest(previous, []*streamingpb.PChannelSummaryChunkIndexEntry{
		newTestChunkIndexEntry(1, 1, 20, 10, "v1"),
	})

	require.Len(t, inherited.GetChunks(), 2)
	require.Equal(t, uint64(1), inherited.GetChunks()[1].GetGeneration())
	// Unfinished deletions carry forward too, because the new manifest is derived
	// from the old one rather than assembled from scratch.
	require.Len(t, inherited.GetPendingGc(), 1)
	// The previous manifest is not mutated: it may still be read by the caller.
	require.Len(t, previous.GetChunks(), 1)
}

func TestRecordPChannelSummaryChunkIsIdempotentAndOrdered(t *testing.T) {
	manifest := &streamingpb.PChannelSummaryManifest{}
	recordPChannelSummaryChunk(manifest, newTestChunkIndexEntry(2, 1, 30, 10, "v1"))
	recordPChannelSummaryChunk(manifest, newTestChunkIndexEntry(0, 1, 10, 10, "v1"))
	recordPChannelSummaryChunk(manifest, newTestChunkIndexEntry(1, 1, 20, 10, "v1"))
	// A re-recorded generation must not duplicate: the persist path may retry.
	recordPChannelSummaryChunk(manifest, newTestChunkIndexEntry(1, 1, 20, 10, "v1"))

	require.Len(t, manifest.GetChunks(), 3)
	for i, entry := range manifest.GetChunks() {
		require.Equal(t, uint64(i), entry.GetGeneration())
	}
}

func TestRetentionTTLHorizonDerivesFromCoveredTimetick(t *testing.T) {
	// The horizon comes from the newest timetick a chunk covered, not the wall
	// clock, so retention measures time the way the write path does and simply
	// stops advancing while the channel is idle.
	latest := tsoutil.ComposeTS(1_000_000, 0)
	horizon := retentionTTLHorizon(latest, 10*time.Minute)
	physical, _ := tsoutil.ParseHybridTs(horizon)
	require.Equal(t, int64(1_000_000-600_000), physical)

	require.Equal(t, uint64(0), retentionTTLHorizon(latest, 0))
	require.Equal(t, uint64(0), retentionTTLHorizon(0, 10*time.Minute))
	// A TTL longer than everything the store has covered expires nothing.
	require.Equal(t, uint64(0), retentionTTLHorizon(tsoutil.ComposeTS(1000, 0), 10*time.Minute))
}

func TestPChannelSummaryManifestFrameRoundTrip(t *testing.T) {
	manifest := testManifestOf(newTestChunkIndexEntry(0, 1, 10, 10, "v1"))
	manifest.PendingGc = []*streamingpb.PChannelSummaryChunkRef{{Generation: 5, Term: 0}}

	payload, err := marshalPChannelSummaryManifest(manifest)
	require.NoError(t, err)
	decoded, err := unmarshalPChannelSummaryManifest(payload)
	require.NoError(t, err)
	require.Len(t, decoded.GetChunks(), 1)
	require.Len(t, decoded.GetPendingGc(), 1)

	t.Run("checksum", func(t *testing.T) {
		// The manifest is the only index into the chunk set, so a damaged one must
		// fail loudly rather than present a shorter chunk list as the truth.
		damaged := append([]byte{}, payload...)
		damaged[len(damaged)-1] ^= 0xff
		_, err := unmarshalPChannelSummaryManifest(damaged)
		require.ErrorIs(t, err, ErrPChannelSummaryStoreCorrupted)
	})

	t.Run("magic", func(t *testing.T) {
		damaged := append([]byte{}, payload...)
		damaged[0] ^= 0xff
		_, err := unmarshalPChannelSummaryManifest(damaged)
		require.ErrorIs(t, err, ErrPChannelSummaryStoreCorrupted)
	})

	t.Run("truncated", func(t *testing.T) {
		_, err := unmarshalPChannelSummaryManifest(payload[:8])
		require.ErrorIs(t, err, ErrPChannelSummaryStoreCorrupted)
	})
}
