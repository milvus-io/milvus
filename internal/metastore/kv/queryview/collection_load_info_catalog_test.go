package queryview

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	legacycatalog "github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestCollectionLoadInfoSingleRecord(t *testing.T) {
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
