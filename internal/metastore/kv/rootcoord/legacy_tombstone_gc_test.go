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
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
)

func newTombstoneTestMetaKV(t *testing.T) (kv.MetaKv, func()) {
	t.Helper()
	cli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	require.NoError(t, err)

	rootPath := fmt.Sprintf("/test/meta/tombstone-gc-%d", rand.Int())
	metaKV := etcdkv.NewEtcdKV(cli, rootPath)
	cleanup := func() {
		_ = metaKV.RemoveWithPrefix(context.TODO(), "")
		metaKV.Close()
		cli.Close()
	}
	return metaKV, cleanup
}

func Test_gcLegacyTombstoneBatch(t *testing.T) {
	metaKV, cleanup := newTombstoneTestMetaKV(t)
	defer cleanup()

	ctx := context.TODO()
	tomb := string(SuffixSnapshotTombstone)

	// Seed: tombstone-valued key under each tracked prefix.
	tombKeys := []string{
		CollectionMetaPrefix + "/1001",
		CollectionInfoMetaPrefix + "/1/1001",
		PartitionMetaPrefix + "/1001/1",
		FieldMetaPrefix + "/1001/100",
		StructArrayFieldMetaPrefix + "/1001/200",
		FunctionMetaPrefix + "/1001/300",
		AliasMetaPrefix + "/1/alias-x",
		CollectionAliasMetaPrefix210 + "/alias-y",
		DBInfoMetaPrefix + "/1001",
	}
	for _, k := range tombKeys {
		require.NoError(t, metaKV.Save(ctx, k, tomb))
	}

	// Seed: normal-valued keys (must survive) + 3-byte non-tombstone values
	// (must also survive — they are not exactly {0xE2,0x9B,0xBC}).
	normalKeys := []string{
		CollectionInfoMetaPrefix + "/1/2002",
		PartitionMetaPrefix + "/2002/1",
		FieldMetaPrefix + "/2002/100",
	}
	for i, k := range normalKeys {
		require.NoError(t, metaKV.Save(ctx, k, fmt.Sprintf("normal-%d", i)))
	}

	threeByteNonTomb := CollectionMetaPrefix + "/3003"
	require.NoError(t, metaKV.Save(ctx, threeByteNonTomb, string([]byte{0x01, 0x02, 0x03})))

	// Run one pass: should delete exactly len(tombKeys) entries.
	deleted, err := gcLegacyTombstonePass(ctx, metaKV)
	assert.NoError(t, err)
	assert.Equal(t, len(tombKeys), deleted)

	// All tombstone keys are gone.
	for _, k := range tombKeys {
		_, err := metaKV.Load(ctx, k)
		assert.Error(t, err, "tombstone key %s should have been deleted", k)
	}

	// Normal + 3-byte non-tombstone keys are preserved.
	for i, k := range normalKeys {
		v, err := metaKV.Load(ctx, k)
		assert.NoError(t, err, "normal key %s should still exist", k)
		assert.Equal(t, fmt.Sprintf("normal-%d", i), v)
	}
	v, err := metaKV.Load(ctx, threeByteNonTomb)
	assert.NoError(t, err)
	assert.Equal(t, string([]byte{0x01, 0x02, 0x03}), v)

	// Second pass: zero deletions, signals self-terminate.
	deleted, err = gcLegacyTombstonePass(ctx, metaKV)
	assert.NoError(t, err)
	assert.Equal(t, 0, deleted)
}

func Test_gcLegacyTombstoneBatch_PrefixIsolation(t *testing.T) {
	// Tombstone value lives at a prefix we do NOT scan (e.g. snapshots/).
	// Must not be touched by the tombstone GC.
	metaKV, cleanup := newTombstoneTestMetaKV(t)
	defer cleanup()

	ctx := context.TODO()
	tomb := string(SuffixSnapshotTombstone)

	outOfScopeKey := SnapshotPrefix + "/root-coord/collection/5_ts1"
	require.NoError(t, metaKV.Save(ctx, outOfScopeKey, tomb))

	deleted, err := gcLegacyTombstonePass(ctx, metaKV)
	assert.NoError(t, err)
	assert.Equal(t, 0, deleted)

	v, err := metaKV.Load(ctx, outOfScopeKey)
	assert.NoError(t, err)
	assert.Equal(t, tomb, v)
}

func Test_gcLegacyTombstoneBatch_WalkError(t *testing.T) {
	kvMock := mocks.NewMetaKv(t)
	kvMock.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(fmt.Errorf("etcd unavailable"))

	deleted, err := gcLegacyTombstoneBatch(context.TODO(), kvMock, CollectionMetaPrefix+"/")
	assert.Error(t, err)
	assert.Equal(t, 0, deleted)
}

func Test_gcLegacyTombstoneBatch_PrefixMismatch(t *testing.T) {
	// If WalkWithPrefix yields a key whose full path doesn't include the
	// expected full-path prefix, we skip it rather than MultiRemove a bogus key.
	kvMock := mocks.NewMetaKv(t)
	kvMock.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error {
			return fn([]byte("wrong-prefix/some-key"), SuffixSnapshotTombstone)
		})
	kvMock.EXPECT().GetPath(mock.Anything).Return("root/root-coord/collection/")

	deleted, err := gcLegacyTombstoneBatch(context.TODO(), kvMock, CollectionMetaPrefix+"/")
	assert.NoError(t, err)
	assert.Equal(t, 0, deleted)
}

func Test_runLegacyTombstoneGC_ContextCancel(t *testing.T) {
	kvMock := mocks.NewMetaKv(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		runLegacyTombstoneGC(ctx, kvMock)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("runLegacyTombstoneGC did not exit after context cancel")
	}
}

func Test_runLegacyTombstoneGC_SelfTerminate(t *testing.T) {
	kvMock := mocks.NewMetaKv(t)
	// All prefixes scanned return zero tombstones → deleted=0 → goroutine exits.
	kvMock.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	done := make(chan struct{})
	go func() {
		runLegacyTombstoneGC(context.Background(), kvMock)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("runLegacyTombstoneGC did not self-terminate")
	}
}

func Test_stripRootPath(t *testing.T) {
	kvMock := mocks.NewMetaKv(t)
	kvMock.EXPECT().GetPath("root-coord/collection/").
		Return("by-dev/meta/root-coord/collection/").Maybe()

	rel, ok := stripRootPath(kvMock, "by-dev/meta/root-coord/collection/42", "root-coord/collection/")
	assert.True(t, ok)
	assert.Equal(t, "root-coord/collection/42", rel)

	rel, ok = stripRootPath(kvMock, "totally/unrelated/key", "root-coord/collection/")
	assert.False(t, ok)
	assert.Equal(t, "", rel)
}
