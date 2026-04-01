package rootcoord

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
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
