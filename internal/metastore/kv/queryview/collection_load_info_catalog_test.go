package queryview

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	legacycatalog "github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func newLoadInfoTestEtcd(t *testing.T) (*clientv3.Client, kv.MetaKv) {
	t.Helper()
	paramtable.Init()
	cfg := &paramtable.Get().EtcdCfg
	client, err := etcd.GetEtcdClient(cfg.UseEmbedEtcd.GetAsBool(), cfg.EtcdUseSSL.GetAsBool(), cfg.Endpoints.GetAsStrings(),
		cfg.EtcdTLSCert.GetValue(), cfg.EtcdTLSKey.GetValue(), cfg.EtcdTLSCACert.GetValue(), cfg.EtcdTLSMinVersion.GetValue())
	require.NoError(t, err)
	storage := etcdkv.NewEtcdKV(client, "qv-load-info-"+uuid.NewString())
	t.Cleanup(func() {
		require.NoError(t, storage.RemoveWithPrefix(context.Background(), ""))
		storage.Close()
	})
	return client, storage
}

func TestCollectionLoadInfoSingleRecord(t *testing.T) {
	_, storage := newLoadInfoTestEtcd(t)
	ctx := t.Context()
	legacy := legacycatalog.NewCatalog(storage)
	require.NoError(t, legacy.SaveCollection(ctx, &querypb.CollectionLoadInfo{CollectionID: 1, ReplicaNumber: 7},
		&querypb.PartitionLoadInfo{CollectionID: 1, PartitionID: 99}))
	require.NoError(t, legacy.SaveReplica(ctx, &querypb.Replica{CollectionID: 1, ID: 88, ResourceGroup: "legacy"}))
	keysBefore, valuesBefore, err := storage.LoadWithPrefix(ctx, "")
	require.NoError(t, err)
	catalog := NewCollectionLoadInfoCatalog(storage)
	info := &viewpb.CollectionLoadConfig{
		CollectionID: 1, Version: 10, PartitionIds: []int64{11, 12},
		RequiredFields: []*viewpb.LoadFieldConfig{{FieldId: 100, IndexIds: []int64{1000, 1001}}},
		Replicas:       []*viewpb.LoadReplicaConfig{{ReplicaId: 20, ResourceGroupName: "a"}, {ReplicaId: 21, ResourceGroupName: "b"}},
	}
	require.NoError(t, catalog.Save(ctx, info))
	keys, _, err := storage.LoadWithPrefix(ctx, "")
	require.NoError(t, err)
	require.Len(t, keys, len(keysBefore)+1, "all partitions and replicas must be in one new key")
	value, err := storage.Load(ctx, collectionLoadInfoVersionKey(1, 10))
	require.NoError(t, err)
	decoded := new(viewpb.CollectionLoadConfig)
	require.NoError(t, proto.Unmarshal([]byte(value), decoded))
	require.True(t, proto.Equal(info, decoded))
	info.Version = 11
	info.Replicas = info.Replicas[:1]
	require.NoError(t, catalog.Save(ctx, info))
	records, err := catalog.List(ctx)
	require.NoError(t, err)
	require.Len(t, records, 2)
	require.NoError(t, catalog.RemoveVersions(ctx, 1, []uint64{10}))
	neighbor := &viewpb.CollectionLoadConfig{CollectionID: 10, Version: 12}
	require.NoError(t, catalog.Save(ctx, neighbor))
	require.NoError(t, catalog.RemoveCollection(ctx, 1))
	require.NoError(t, catalog.RemoveCollection(ctx, 1))
	records, err = catalog.List(ctx)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.True(t, proto.Equal(neighbor, records[0]))
	require.NoError(t, catalog.RemoveCollection(ctx, 10))
	keysAfter, valuesAfter, err := storage.LoadWithPrefix(ctx, "")
	require.NoError(t, err)
	require.Equal(t, keysBefore, keysAfter)
	require.Equal(t, valuesBefore, valuesAfter, "QV operations must leave all legacy metadata untouched")
	records, err = catalog.List(ctx)
	require.NoError(t, err)
	require.Empty(t, records, "legacy records must not recreate QV load intent")
}

func TestCollectionLoadInfoRemoveCollectionIsAtomic(t *testing.T) {
	client, storage := newLoadInfoTestEtcd(t)
	limit := &paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum
	previous := limit.SwapTempValue("2")
	t.Cleanup(func() { limit.SwapTempValue(previous) })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	catalog := NewCollectionLoadInfoCatalog(storage)
	const versions = 3
	require.Greater(t, versions, storage.MaxTxnOps())
	for version := uint64(1); version <= versions; version++ {
		require.NoError(t, catalog.Save(ctx, &viewpb.CollectionLoadConfig{CollectionID: 1, Version: version}))
	}
	prefix := storage.GetPath(VersionedLoadInfoPrefix + "/1/")
	before, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, before.Kvs, versions)

	watch := client.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(before.Header.Revision+1))
	require.NoError(t, catalog.RemoveCollection(ctx, 1))
	deletion, ok := <-watch
	require.True(t, ok, "watch closed before observing the deletion")
	require.NoError(t, deletion.Err())
	require.Len(t, deletion.Events, versions, "all versions must be deleted in one revision")
	revision := deletion.Events[0].Kv.ModRevision
	for _, event := range deletion.Events {
		require.Equal(t, clientv3.EventTypeDelete, event.Type)
		require.Equal(t, revision, event.Kv.ModRevision)
	}
	records, err := catalog.List(ctx)
	require.NoError(t, err)
	require.Empty(t, records)
}
