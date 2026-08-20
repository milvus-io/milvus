//go:build test
// +build test

package recovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

// The chunk store's Exist->Write is not atomic, so two split-brain owners can
// both pass the absence check for the same generation. The footer term must
// arbitrate: the stale owner is fenced, the newer owner overwrites, and only a
// same-term payload mismatch remains corruption.
func TestWritePChannelSummaryChunkIfAbsentArbitratesByTerm(t *testing.T) {
	ctx := context.Background()
	catalog, _ := newTestPChannelSummaryCatalog(t)
	chunkManager := newTestPChannelSummaryCleanerChunkManager(t)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	checkpoint := &utility.WALCheckpoint{MessageID: rmq.NewRmqID(100), TimeTick: 100}
	stalePayload, staleFooter, _, err := marshalPChannelSummaryChunk("p1", 7, 3, checkpoint, nil)
	require.NoError(t, err)
	currentPayload, currentFooter, _, err := marshalPChannelSummaryChunk("p1", 7, 5, checkpoint, nil)
	require.NoError(t, err)
	chunkKey := buildPChannelSummaryChunkKey(chunkManager, "p1", 7)

	// A stale owner (term 3) must not overwrite the newer owner's chunk (term 5).
	require.NoError(t, chunkManager.Write(ctx, chunkKey, currentPayload))
	err = writePChannelSummaryChunkIfAbsent(ctx, chunkKey, stalePayload, staleFooter, 3)
	require.ErrorIs(t, err, ErrPChannelSummaryStoreFenced)
	stored, err := chunkManager.Read(ctx, chunkKey)
	require.NoError(t, err)
	require.Equal(t, currentPayload, stored)

	// The newer owner overwrites a stale owner's leftover chunk.
	require.NoError(t, chunkManager.Write(ctx, chunkKey, stalePayload))
	require.NoError(t, writePChannelSummaryChunkIfAbsent(ctx, chunkKey, currentPayload, currentFooter, 5))
	stored, err = chunkManager.Read(ctx, chunkKey)
	require.NoError(t, err)
	require.Equal(t, currentPayload, stored)

	// Same term, different content: undecidable — corruption.
	conflictPayload, conflictFooter, _, err := marshalPChannelSummaryChunk("p1", 7, 5, &utility.WALCheckpoint{MessageID: rmq.NewRmqID(200), TimeTick: 200}, nil)
	require.NoError(t, err)
	err = writePChannelSummaryChunkIfAbsent(ctx, chunkKey, conflictPayload, conflictFooter, 5)
	require.ErrorIs(t, err, ErrPChannelSummaryStoreCorrupted)
}

// A same-term retry of an identical chunk must stay idempotent even when the
// stored bytes differ. The payload encoding is protobuf, which is not guaranteed
// byte-stable across library versions, so a retry that spans a binary upgrade
// re-encodes the same records differently; arbitration must compare the decoded
// content, not the raw bytes, or a healthy retry would be reported as corruption.
func TestWritePChannelSummaryChunkIfAbsentAcceptsByteDifferentSameContentRetry(t *testing.T) {
	ctx := context.Background()
	catalog, _ := newTestPChannelSummaryCatalog(t)
	chunkManager := newTestPChannelSummaryCleanerChunkManager(t)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	checkpoint := &utility.WALCheckpoint{MessageID: rmq.NewRmqID(100), TimeTick: 100}
	records := map[string][]*streamingpb.SummaryEntry{
		"v1": {{
			SourceMessageId: rmq.NewRmqID(101).IntoProto(),
			SourceTimetick:  101,
			Idempotency: &streamingpb.IdempotencyContent{
				Key: "key-1",
				InsertResult: message.NewIdempotentInsertResult(
					[]uint32{0},
					&schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7}}}},
				),
			},
		}},
	}
	payload, footer, _, err := marshalPChannelSummaryChunk("p1", 7, 5, checkpoint, records)
	require.NoError(t, err)
	chunkKey := buildPChannelSummaryChunkKey(chunkManager, "p1", 7)

	// Simulate a byte-different but semantically identical stored chunk by
	// padding the footer with a proto unknown field, which a decoder preserves
	// and ignores. The per-vchannel payloads and the footer's identity fields are
	// untouched, so the content comparison must accept it.
	stored, _ := repackChunkWithPaddedFooter(t, payload)
	require.NotEqual(t, payload, stored)
	require.NoError(t, chunkManager.Write(ctx, chunkKey, stored))

	require.NoError(t, writePChannelSummaryChunkIfAbsent(ctx, chunkKey, payload, footer, 5))
	// The stored chunk is left as-is: it already holds the same content.
	after, err := chunkManager.Read(ctx, chunkKey)
	require.NoError(t, err)
	require.Equal(t, stored, after)
}
