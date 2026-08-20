//go:build test
// +build test

package recovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// The chunk store's Exist->Write is not atomic, so two split-brain owners can
// both pass the absence check for the same generation. The footer term must
// arbitrate: the stale owner is fenced, the newer owner overwrites, and only a
// same-term payload mismatch remains corruption.
func TestWritePChannelSummaryChunkIfAbsentArbitratesByTerm(t *testing.T) {
	ctx := context.Background()
	chunkManager, _ := newTestSummaryStore(t)

	staleRecords := map[string][]*SummaryRecord{"v1": {newTestSummaryRecord("key-a", 10, 1)}}
	currentRecords := map[string][]*SummaryRecord{"v1": {newTestSummaryRecord("key-b", 20, 2)}}

	stalePayload, staleFooter, err := marshalPChannelSummaryChunk("p1", 7, 3, staleRecords)
	require.NoError(t, err)
	currentPayload, currentFooter, err := marshalPChannelSummaryChunk("p1", 7, 5, currentRecords)
	require.NoError(t, err)
	chunkKey := buildPChannelSummaryChunkKey(chunkManager, "p1", 7)

	// A stale owner (term 3) must not overwrite the newer owner's chunk (term 5).
	require.NoError(t, chunkManager.Write(ctx, chunkKey, currentPayload))
	err = writePChannelSummaryChunkIfAbsent(ctx, chunkKey, stalePayload, staleFooter, staleRecords, 3)
	require.ErrorIs(t, err, ErrPChannelSummaryStoreFenced)
	stored, err := chunkManager.Read(ctx, chunkKey)
	require.NoError(t, err)
	require.Equal(t, currentPayload, stored)

	// The newer owner overwrites a stale owner's leftover chunk.
	require.NoError(t, chunkManager.Write(ctx, chunkKey, stalePayload))
	require.NoError(t, writePChannelSummaryChunkIfAbsent(ctx, chunkKey, currentPayload, currentFooter, currentRecords, 5))
	stored, err = chunkManager.Read(ctx, chunkKey)
	require.NoError(t, err)
	require.Equal(t, currentPayload, stored)

	// Same term, different content: undecidable — corruption.
	conflictRecords := map[string][]*SummaryRecord{"v1": {newTestSummaryRecord("key-c", 30, 3)}}
	conflictPayload, conflictFooter, err := marshalPChannelSummaryChunk("p1", 7, 5, conflictRecords)
	require.NoError(t, err)
	err = writePChannelSummaryChunkIfAbsent(ctx, chunkKey, conflictPayload, conflictFooter, conflictRecords, 5)
	require.ErrorIs(t, err, ErrPChannelSummaryStoreCorrupted)
}

// A same-term retry of an identical chunk must stay idempotent even when the
// stored bytes differ. The payload encoding is protobuf, which is not guaranteed
// byte-stable across library versions, so a retry that spans a binary upgrade
// re-encodes the same records differently; arbitration must compare the decoded
// records, not the raw bytes, or a healthy retry would be reported as corruption.
func TestWritePChannelSummaryChunkIfAbsentAcceptsByteDifferentSameContentRetry(t *testing.T) {
	ctx := context.Background()
	chunkManager, _ := newTestSummaryStore(t)

	records := map[string][]*SummaryRecord{"v1": {newTestSummaryRecord("key-1", 101, 7)}}
	payload, footer, err := marshalPChannelSummaryChunk("p1", 7, 5, records)
	require.NoError(t, err)
	chunkKey := buildPChannelSummaryChunkKey(chunkManager, "p1", 7)

	// Simulate a byte-different but semantically identical stored chunk by
	// padding the footer with a proto unknown field, which a decoder preserves
	// and ignores. The sections and the footer's identity are untouched, so the
	// content comparison must accept it.
	stored, _ := repackChunkWithPaddedFooter(t, payload)
	require.NotEqual(t, payload, stored)
	require.NoError(t, chunkManager.Write(ctx, chunkKey, stored))

	require.NoError(t, writePChannelSummaryChunkIfAbsent(ctx, chunkKey, payload, footer, records, 5))
	// The stored chunk is left as-is: it already holds the same records.
	after, err := chunkManager.Read(ctx, chunkKey)
	require.NoError(t, err)
	require.Equal(t, stored, after)
}

func TestSummaryRecordsByVChannelEqualComparesContent(t *testing.T) {
	base := map[string][]*SummaryRecord{
		"v1": {newTestSummaryRecord("key-a", 10, 1), newTestSummaryRecord("key-b", 11, 2)},
	}
	// Order is normalised before comparison, because chunk order is derived, not
	// given by the caller.
	reordered := map[string][]*SummaryRecord{
		"v1": {newTestSummaryRecord("key-b", 11, 2), newTestSummaryRecord("key-a", 10, 1)},
	}
	require.True(t, summaryRecordsByVChannelEqual(base, reordered))

	require.False(t, summaryRecordsByVChannelEqual(base, map[string][]*SummaryRecord{
		"v1": {newTestSummaryRecord("key-a", 10, 1)},
	}))
	require.False(t, summaryRecordsByVChannelEqual(base, map[string][]*SummaryRecord{
		"v1": {newTestSummaryRecord("key-a", 10, 1), newTestSummaryRecord("key-b", 11, 99)},
	}))
	require.False(t, summaryRecordsByVChannelEqual(base, map[string][]*SummaryRecord{
		"v2": {newTestSummaryRecord("key-a", 10, 1), newTestSummaryRecord("key-b", 11, 2)},
	}))
}
