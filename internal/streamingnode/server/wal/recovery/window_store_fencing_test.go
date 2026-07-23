//go:build test
// +build test

package recovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

// The chunk store's Exist->Write is not atomic, so two split-brain owners can
// both pass the absence check for the same generation. The footer term must
// arbitrate: the stale owner is fenced, the newer owner overwrites, and only a
// same-term payload mismatch remains corruption.
func TestWritePChannelWindowChunkIfAbsentArbitratesByTerm(t *testing.T) {
	ctx := context.Background()
	catalog, _ := newTestPChannelWindowCatalog(t)
	chunkManager := newTestPChannelWindowCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	checkpoint := &utility.WALCheckpoint{MessageID: rmq.NewRmqID(100), TimeTick: 100}
	stalePayload, _, _, err := marshalPChannelWindowChunk("p1", 7, 3, checkpoint, nil)
	require.NoError(t, err)
	currentPayload, _, _, err := marshalPChannelWindowChunk("p1", 7, 5, checkpoint, nil)
	require.NoError(t, err)
	chunkKey := buildPChannelWindowChunkKey("p1", 7)

	// A stale owner (term 3) must not overwrite the newer owner's chunk (term 5).
	require.NoError(t, chunkManager.Write(ctx, chunkKey, currentPayload))
	err = writePChannelWindowChunkIfAbsent(ctx, chunkKey, stalePayload, 3)
	require.ErrorIs(t, err, ErrPChannelWindowStoreFenced)
	stored, err := chunkManager.Read(ctx, chunkKey)
	require.NoError(t, err)
	require.Equal(t, currentPayload, stored)

	// The newer owner overwrites a stale owner's leftover chunk.
	require.NoError(t, chunkManager.Write(ctx, chunkKey, stalePayload))
	require.NoError(t, writePChannelWindowChunkIfAbsent(ctx, chunkKey, currentPayload, 5))
	stored, err = chunkManager.Read(ctx, chunkKey)
	require.NoError(t, err)
	require.Equal(t, currentPayload, stored)

	// Same term, different payload: undecidable — corruption, as before.
	conflictPayload, _, _, err := marshalPChannelWindowChunk("p1", 7, 5, &utility.WALCheckpoint{MessageID: rmq.NewRmqID(200), TimeTick: 200}, nil)
	require.NoError(t, err)
	err = writePChannelWindowChunkIfAbsent(ctx, chunkKey, conflictPayload, 5)
	require.ErrorIs(t, err, ErrPChannelWindowStoreCorrupted)
}

// A cleaner whose assignment term is older than the term stamped in the durable
// meta must abort without deleting chunks or rewriting the meta.
func TestPChannelWindowCleanerFencedByNewerTerm(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := newTestPChannelWindowCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))
	writeTestPChannelWindowChunks(t, ctx, "p1", chunkManager, 0, 3)
	catalogState.storeMeta = testPChannelWindowStoreMeta(t, ctx, "p1", chunkManager, 3, 0, 0)
	catalogState.storeMeta.Term = 5

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1", Term: 3}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(300),
		TimeTick:  300,
	})
	addTestIdempotencyWindowPinnedAtGeneration(rs.windowManager, "v1", 2)
	rs.windowManager.markActiveViewsInitialized()
	rs.SetLogger(resource.Resource().Logger())

	err := rs.windowManager.cleanPChannelWindow(ctx, resource.Resource().Logger())
	require.ErrorIs(t, err, ErrPChannelWindowStoreFenced)
	require.Equal(t, int64(5), catalogState.storeMeta.GetTerm())
	require.Equal(t, uint64(0), catalogState.storeMeta.GetMinInUseGeneration())
	require.Equal(t, uint64(0), catalogState.storeMeta.GetMinAvailableGeneration())
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 0, true)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 1, true)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 2, true)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 3, true)
}

// An open whose assignment term is older than the durable meta's term is a
// stale split-brain open and must fail instead of recovering from (and later
// persisting over) the current owner's state.
func TestRecoverWindowsFencedByNewerTermMeta(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1", Term: 3}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(300),
		TimeTick:  300,
	})
	_, err := rs.windowManager.recoverIdempotencyWindowsFromPChannelWindowStore(context.Background(), "p1", &pchannelWindowStoreMeta{PChannel: "p1", Term: 5})
	require.ErrorIs(t, err, ErrPChannelWindowStoreFenced)
}
