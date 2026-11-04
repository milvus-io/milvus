//go:build test || dynamic

// Package loadmgrtest supplies isolated real-catalog fixtures for component tests.
package loadmgrtest

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Dependencies use a unique metadata root and keep one allocator across recovery.
// Tests that need particular index/schema metadata replace the relevant provider.
func Dependencies(t testing.TB) loadmgr.LoadConfigStoreDependencies {
	t.Helper()
	paramtable.Init()
	cfg := &paramtable.Get().EtcdCfg
	client, err := etcd.GetEtcdClient(cfg.UseEmbedEtcd.GetAsBool(), cfg.EtcdUseSSL.GetAsBool(), cfg.Endpoints.GetAsStrings(),
		cfg.EtcdTLSCert.GetValue(), cfg.EtcdTLSKey.GetValue(), cfg.EtcdTLSCACert.GetValue(), cfg.EtcdTLSMinVersion.GetValue())
	require.NoError(t, err)
	storage := etcdkv.NewEtcdKV(client, fmt.Sprintf("test-qv-load-config-%d", time.Now().UnixNano()))
	t.Cleanup(func() {
		require.NoError(t, storage.RemoveWithPrefix(context.Background(), ""))
		storage.Close()
	})
	var ids atomic.Uint64
	return loadmgr.LoadConfigStoreDependencies{
		Catalog:         catalogkv.NewCollectionLoadInfoCatalog(storage),
		AllocateVersion: func(context.Context) (uint64, error) { return ids.Add(1), nil },
		Schema: func(context.Context, int64) (*schemapb.CollectionSchema, error) {
			return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100}, {FieldID: 200}}}, nil
		},
	}
}

// FromLegacyCatalog migrates existing component-test fixtures into the new
// catalog. Only fixture reads use the legacy interface; operations under test
// use the production versioned store and its real persistence path.
func FromLegacyCatalog(t testing.TB, catalog metastore.QueryCoordCatalog) loadmgr.LoadConfigStoreDependencies {
	t.Helper()
	ctx := context.Background()
	deps := Dependencies(t)
	infos, err := catalog.GetCollections(ctx)
	require.NoError(t, err)
	ids := make([]int64, 0, len(infos))
	for _, info := range infos {
		ids = append(ids, info.CollectionID)
	}
	partitions, err := catalog.GetPartitions(ctx, ids)
	require.NoError(t, err)
	replicas, err := catalog.GetReplicas(ctx)
	require.NoError(t, err)
	for _, source := range infos {
		info := &viewpb.CollectionLoadConfig{
			CollectionID: source.CollectionID, DbID: source.DbID,
			UserSpecifiedReplicaMode: source.UserSpecifiedReplicaMode,
			RequiredFields:           loadmgr.FieldsFromMessage(source.RequiredFields),
		}
		for _, fieldID := range source.GetLoadFields() {
			field := &viewpb.LoadFieldConfig{FieldId: fieldID}
			if id := source.GetFieldIndexID()[fieldID]; id != 0 {
				field.IndexIds = []int64{id}
			}
			info.PreloadFields = append(info.PreloadFields, field)
		}
		info.Version, err = deps.AllocateVersion(ctx)
		require.NoError(t, err)
		for _, partition := range partitions[info.CollectionID] {
			info.PartitionIds = append(info.PartitionIds, partition.PartitionID)
		}
		for _, replica := range replicas {
			if replica.CollectionID == info.CollectionID {
				info.Replicas = append(info.Replicas, &viewpb.LoadReplicaConfig{ReplicaId: replica.ID, ResourceGroupName: replica.ResourceGroup})
			}
		}
		if len(info.RequiredFields) == 0 {
			schema, err := deps.Schema(ctx, info.CollectionID)
			require.NoError(t, err)
			info.RequiredFields = loadmgr.RequiredFieldsForSchema(schema)
		}
		require.NoError(t, deps.Catalog.Save(ctx, info))
	}
	return deps
}
