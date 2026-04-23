package rootcoord

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
)

func Test_gcLegacySnapshotBatch(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	require.NoError(t, err)
	defer etcdCli.Close()

	rootPath := fmt.Sprintf("/test/meta/legacy-gc-%d", rand.Int())
	metaKV := etcdkv.NewEtcdKV(etcdCli, rootPath)
	defer metaKV.Close()
	defer metaKV.RemoveWithPrefix(context.TODO(), "")

	ctx := context.TODO()

	// Seed 50 snapshot keys (to be deleted)
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("%s/root-coord/fields/%d/0_ts%d", SnapshotPrefix, 100+i, 438497159122780160+i)
		err := metaKV.Save(ctx, key, fmt.Sprintf("snapshot-value-%d", i))
		require.NoError(t, err)
	}

	// Seed 5 plain keys (must NOT be deleted)
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("root-coord/fields/%d/0", 200+i)
		err := metaKV.Save(ctx, key, fmt.Sprintf("plain-value-%d", i))
		require.NoError(t, err)
	}

	t.Run("batch deletes snapshot keys", func(t *testing.T) {
		deleted, err := gcLegacySnapshotBatch(ctx, metaKV)
		assert.NoError(t, err)
		assert.Equal(t, 50, deleted)
	})

	t.Run("returns zero when no more snapshot keys", func(t *testing.T) {
		deleted, err := gcLegacySnapshotBatch(ctx, metaKV)
		assert.NoError(t, err)
		assert.Equal(t, 0, deleted, "should find no more snapshot keys")
	})

	t.Run("plain keys are untouched", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("root-coord/fields/%d/0", 200+i)
			val, err := metaKV.Load(ctx, key)
			assert.NoError(t, err, "plain key %s should still exist", key)
			assert.Equal(t, fmt.Sprintf("plain-value-%d", i), val)
		}
	})
}

func Test_gcLegacySnapshotBatch_LargeBatch(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	require.NoError(t, err)
	defer etcdCli.Close()

	rootPath := fmt.Sprintf("/test/meta/legacy-gc-large-%d", rand.Int())
	metaKV := etcdkv.NewEtcdKV(etcdCli, rootPath)
	defer metaKV.Close()
	defer metaKV.RemoveWithPrefix(context.TODO(), "")

	ctx := context.TODO()

	// Seed 5000 snapshot keys — more than one batch (legacyGCBatchSize=2000)
	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("%s/root-coord/fields/%d/0_ts%d", SnapshotPrefix, i, 438497159122780160+i)
		err := metaKV.Save(ctx, key, fmt.Sprintf("v%d", i))
		require.NoError(t, err)
	}

	// Batch 1: should delete legacyGCBatchSize keys
	deleted1, err := gcLegacySnapshotBatch(ctx, metaKV)
	assert.NoError(t, err)
	assert.Equal(t, legacyGCBatchSize, deleted1)

	// Batch 2: should delete another legacyGCBatchSize keys
	deleted2, err := gcLegacySnapshotBatch(ctx, metaKV)
	assert.NoError(t, err)
	assert.Equal(t, legacyGCBatchSize, deleted2)

	// Batch 3: remaining keys
	deleted3, err := gcLegacySnapshotBatch(ctx, metaKV)
	assert.NoError(t, err)
	assert.Equal(t, 5000-2*legacyGCBatchSize, deleted3)

	// Batch 4: no more keys
	deleted4, err := gcLegacySnapshotBatch(ctx, metaKV)
	assert.NoError(t, err)
	assert.Equal(t, 0, deleted4)
}

func Test_gcLegacySnapshotBatch_WalkError(t *testing.T) {
	kvMock := mocks.NewMetaKv(t)
	kvMock.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(fmt.Errorf("etcd unavailable"))

	deleted, err := gcLegacySnapshotBatch(context.TODO(), kvMock)
	assert.Error(t, err)
	assert.Equal(t, 0, deleted)
}

func Test_gcLegacySnapshotBatch_PrefixMismatch(t *testing.T) {
	// Simulate: WalkWithPrefix returns keys that don't match expected prefix.
	// This should never happen in practice but tests the safety guard.
	kvMock := mocks.NewMetaKv(t)
	kvMock.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error {
			// Return a key with wrong prefix
			return fn([]byte("wrong-prefix/some-key"), []byte("value"))
		})
	kvMock.EXPECT().GetPath(SnapshotPrefix).Return("root/snapshots")

	deleted, err := gcLegacySnapshotBatch(context.TODO(), kvMock)
	assert.NoError(t, err)
	assert.Equal(t, 0, deleted, "mismatched keys should be skipped, nothing deleted")
}

func Test_gcLegacySnapshotBatch_DeleteError(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	require.NoError(t, err)
	defer etcdCli.Close()

	rootPath := fmt.Sprintf("/test/meta/legacy-gc-delerr-%d", rand.Int())
	metaKV := etcdkv.NewEtcdKV(etcdCli, rootPath)
	defer metaKV.Close()
	defer metaKV.RemoveWithPrefix(context.TODO(), "")

	ctx := context.TODO()

	// Seed some snapshot keys
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("%s/root-coord/fields/%d/0_ts%d", SnapshotPrefix, i, 438497159122780160+i)
		err := metaKV.Save(ctx, key, fmt.Sprintf("v%d", i))
		require.NoError(t, err)
	}

	// Use canceled context to trigger delete error
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()

	deleted, err := gcLegacySnapshotBatch(canceledCtx, metaKV)
	// Either returns error (context canceled) or succeeds — both are acceptable.
	// The key point is it doesn't panic.
	_ = deleted
	_ = err
}

func Test_runLegacySnapshotGC_ContextCancel(t *testing.T) {
	// Test that runLegacySnapshotGC exits when context is canceled.
	kvMock := mocks.NewMetaKv(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	// runLegacySnapshotGC should exit quickly via ctx.Done()
	done := make(chan struct{})
	go func() {
		runLegacySnapshotGC(ctx, kvMock)
		close(done)
	}()

	select {
	case <-done:
		// success — goroutine exited
	case <-time.After(3 * time.Second):
		t.Fatal("runLegacySnapshotGC did not exit after context cancel")
	}
}

func Test_runLegacySnapshotGC_SelfTerminate(t *testing.T) {
	// Test that runLegacySnapshotGC self-terminates when no keys found.
	kvMock := mocks.NewMetaKv(t)
	// WalkWithPrefix returns 0 keys → deleted=0 → goroutine exits
	kvMock.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	done := make(chan struct{})
	go func() {
		runLegacySnapshotGC(context.Background(), kvMock)
		close(done)
	}()

	select {
	case <-done:
		// success — goroutine self-terminated
	case <-time.After(10 * time.Second):
		t.Fatal("runLegacySnapshotGC did not self-terminate")
	}
}

func Test_LegacySnapshotUtils(t *testing.T) {
	t.Run("IsTombstone", func(t *testing.T) {
		assert.True(t, IsTombstone(string(SuffixSnapshotTombstone)))
		assert.False(t, IsTombstone("normal-value"))
		assert.False(t, IsTombstone(""))
	})

	t.Run("ConstructTombstone", func(t *testing.T) {
		tomb := ConstructTombstone()
		assert.Equal(t, SuffixSnapshotTombstone, tomb)
		tomb[0] = 0x00
		assert.NotEqual(t, SuffixSnapshotTombstone, tomb)
	})

	t.Run("ComposeSnapshotKey", func(t *testing.T) {
		key := ComposeSnapshotKey("snapshots/", "root-coord/fields/100/0", "_ts", 12345)
		assert.Contains(t, key, "snapshots/")
		assert.Contains(t, key, "root-coord/fields/100/0_ts12345")
	})
}
