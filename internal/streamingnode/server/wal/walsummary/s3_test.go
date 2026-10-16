package walsummary

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Opt in against the workspace's existing S3-compatible dependency. Objects
// are isolated under a unique root and removed when the test finishes.
func newS3SummaryStore(t *testing.T) *Store {
	t.Helper()
	if os.Getenv("MILVUS_WALSUMMARY_S3_TEST") != "1" {
		t.Skip("set MILVUS_WALSUMMARY_S3_TEST=1 to test object-store pagination")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cfg := &paramtable.Get().MinioCfg
	root := fmt.Sprintf("walsummary-tests/%d", time.Now().UnixNano())
	factory := storage.NewChunkManagerFactory("remote",
		objectstorage.RootPath(root), objectstorage.Address(cfg.Address.GetValue()),
		objectstorage.AccessKeyID(cfg.AccessKeyID.GetValue()), objectstorage.SecretAccessKeyID(cfg.SecretAccessKey.GetValue()),
		objectstorage.UseSSL(cfg.UseSSL.GetAsBool()), objectstorage.BucketName(cfg.BucketName.GetValue()),
		objectstorage.CloudProvider("minio"), objectstorage.CreateBucket(true))
	cm, err := factory.NewPersistentStorageChunkManager(ctx)
	require.NoError(t, err)
	store := NewStore(cm, "p1", 1)
	t.Cleanup(func() { require.NoError(t, store.RemoveAllObjects(context.Background())) })
	return store
}

func TestS3SummaryPaginationAndRecovery(t *testing.T) {
	store := newS3SummaryStore(t)
	cm := store.chunkManager
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// More than one S3 page in a single 100-generation prefix. Other terms
	// must neither satisfy a gap nor cause the walker to stop after page one.
	for term := int64(2); term <= 1002; term++ {
		require.NoError(t, cm.Write(ctx, buildChunkKey(cm, "p1", 98, term), []byte("other term")))
	}
	for _, gen := range []uint64{98, 99, 100} {
		_, _, err := store.WriteChunk(ctx, gen, writeSections(map[string][]uint64{"v1": {gen + 1}}), TimeTickRange{Start: gen + 1, End: gen + 1})
		require.NoError(t, err)
	}
	entries, err := store.ProbeChunkForward(ctx, 98)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	require.Equal(t, uint64(100), entries[2].Generation)
	m := newTestManager(t, NewStore(cm, "p2", 1), 1<<30)
	require.NoError(t, m.Restore(ctx))
	require.NoError(t, drainSummary(ctx, m))
	require.NoError(t, stageChunk(t, m, 100).Execute(ctx))
	// The last PUT only wrote the chunk: recovery must discover it from S3.
	successor := newTestManager(t, nextTermStore(m.cfg.Store), 1<<30)
	require.NoError(t, successor.Restore(ctx))
	require.Equal(t, uint64(100), successor.LastAcked())
	require.NoError(t, drainSummary(ctx, successor))
	successor.cfg.RetentionMaxBytes = 1
	require.NoError(t, gcSummary(ctx, successor))
	require.Empty(t, successor.Manifest().Chunks)
	require.NoError(t, successor.cfg.Store.RemoveAllObjects(ctx))
}

func TestS3SummaryChunkCache(t *testing.T) {
	store := newS3SummaryStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	m := newTestManager(t, store, 1<<30)
	m.chunkIndex = newChunkIndex(1 << 20)
	for tt := uint64(1); tt <= 10; tt++ {
		m.ObserveMessage(ctx, newTestDeleteMessage(t, "a", tt, 1, int64(tt)))
	}
	m.ObserveMessage(ctx, newTestDeleteMessage(t, "b", 11, 1, 11))
	require.NoError(t, persistSummary(ctx, m))
	var calls atomic.Int64
	var original func(*storage.RemoteChunkManager, context.Context, string) ([]byte, error)
	patch := mockey.Mock((*storage.RemoteChunkManager).Read).Origin(&original).To(func(cm *storage.RemoteChunkManager, ctx context.Context, key string) ([]byte, error) {
		calls.Add(1)
		return original(cm, ctx, key)
	}).Build()
	defer patch.UnPatch()
	for _, cold := range []bool{false, true} {
		if cold {
			m = newTestManager(t, nextTermStore(store), 1<<30)
			m.chunkIndex = newChunkIndex(1 << 20)
			require.NoError(t, m.Restore(ctx))
			calls.Store(0) // exclude manifest discovery from payload read accounting
		}
		for _, vc := range []string{"a", "b", "a"} {
			var cursor uint64
			for cursor < 11 {
				batch, err := m.ReadTransform(ctx, vc, cursor, 11, ReadLimits{MaxRows: 2})
				require.NoError(t, err)
				require.Greater(t, batch.CoveredThrough, cursor)
				cursor = batch.CoveredThrough
			}
		}
		expected := int64(0)
		if cold {
			expected = 1
		}
		require.Equal(t, expected, calls.Load())
	}
}
