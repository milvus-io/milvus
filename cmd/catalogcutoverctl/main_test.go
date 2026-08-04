package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/rootcoord/catalogmigration"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestDefaultCutoverTimestampUsesRootCoordTSO(t *testing.T) {
	require.Zero(t, defaultCutoverTimestamp())
}

func TestValidateCutoverOptionsRequiresTimestampForOfflineMigration(t *testing.T) {
	err := validateCutoverOptions(cutoverOptions{
		SourceRootPath:  "by-dev/milvus/meta",
		TargetNamespace: "milvus2",
	})
	require.ErrorContains(t, err, "--ts is required")
}

func TestValidateCutoverOptionsAllowsZeroTimestampForOnlineCutover(t *testing.T) {
	err := validateCutoverOptions(cutoverOptions{
		RootCoordAddress: "127.0.0.1:53100",
		TargetNamespace:  "milvus2",
	})
	require.NoError(t, err)
}

func TestMigrateRootCoordCatalogSnapshotCopiesRootCoordMetadata(t *testing.T) {
	ctx := context.Background()
	source := kvrootcoord.NewCatalog(memkv.NewMemoryKV())
	target := kvrootcoord.NewCatalog(memkv.NewMemoryKV())

	require.NoError(t, source.CreateDatabase(ctx, &model.Database{ID: 10, Name: "db"}, 100))
	require.NoError(t, source.CreateCollection(ctx, &model.Collection{
		CollectionID: 100,
		DBID:         10,
		DBName:       "db",
		Name:         "coll",
		Fields: []*model.Field{{
			FieldID:  101,
			Name:     "id",
			DataType: schemapb.DataType_Int64,
		}},
		Partitions: []*model.Partition{{
			PartitionID:   200,
			PartitionName: "_default",
			CollectionID:  100,
			State:         etcdpb.PartitionState_PartitionCreated,
		}},
		State: etcdpb.CollectionState_CollectionCreated,
	}, 101))
	require.NoError(t, source.CreateAlias(ctx, &model.Alias{
		Name:         "alias",
		CollectionID: 100,
		DbID:         10,
		State:        etcdpb.AliasState_AliasCreated,
	}, 102))
	require.NoError(t, source.SaveFileResource(ctx, &internalpb.FileResourceInfo{Id: 300, Name: "resource"}, 1))

	result, err := catalogmigration.Snapshot(ctx, source, target, 200)
	require.NoError(t, err)
	require.Equal(t, catalogmigration.Result{Databases: 1, Collections: 1, Aliases: 1, FileResources: 1}, result)

	got, err := target.GetCollectionByName(ctx, 10, "db", "coll", typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Equal(t, int64(100), got.CollectionID)

	aliases, err := target.ListAliases(ctx, 10, typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Len(t, aliases, 1)
	require.Equal(t, "alias", aliases[0].Name)

	resources, version, err := target.ListFileResource(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)
	require.Len(t, resources, 1)
}
