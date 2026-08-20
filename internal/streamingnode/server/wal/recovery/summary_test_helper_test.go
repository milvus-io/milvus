package recovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// enableRecoveryIdempotency turns the summary store on for a test and restores
// the parameter afterwards.
func enableRecoveryIdempotency(t *testing.T) {
	t.Helper()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.StreamingCfg.IdempotencyEnabled.Key, "true"))
	t.Cleanup(func() { _ = params.Reset(params.StreamingCfg.IdempotencyEnabled.Key) })
}

// newTestPChannelSummaryCleanerChunkManager roots the store at a per-test temp
// directory. The root MUST come straight from t.TempDir(): chunk keys are built
// from the chunk manager's own root and LocalChunkManager writes a key verbatim,
// so a relative root would drop the chunk files into the package directory.
func newTestPChannelSummaryCleanerChunkManager(t *testing.T) storage.ChunkManager {
	t.Helper()
	return storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
}

func requirePChannelSummaryChunkExists(t *testing.T, ctx context.Context, chunkManager storage.ChunkManager, pchannel string, generation uint64, expected bool) {
	t.Helper()
	exists, err := chunkManager.Exist(ctx, buildPChannelSummaryChunkKey(chunkManager, pchannel, generation, 0))
	require.NoError(t, err)
	require.Equal(t, expected, exists)
}

func testRecoveryCheckpoint(messageID int64, timetick uint64) *WALCheckpoint {
	return &WALCheckpoint{
		MessageID: rmq.NewRmqID(messageID),
		TimeTick:  timetick,
	}
}
