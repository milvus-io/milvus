package rootcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestMigrateRootCoordCatalogCopiesSemanticSnapshot(t *testing.T) {
	ctx := context.Background()
	source := kvrootcoord.NewCatalog(memkv.NewMemoryKV())
	target := kvrootcoord.NewCatalog(memkv.NewMemoryKV())

	db := &model.Database{ID: 10, Name: "db1"}
	require.NoError(t, source.CreateDatabase(ctx, db, 100))
	coll := &model.Collection{
		CollectionID: 1000,
		DBID:         10,
		DBName:       "db1",
		Name:         "coll1",
		Fields: []*model.Field{{
			FieldID:  100,
			Name:     "pk",
			DataType: schemapb.DataType_Int64,
		}},
		Partitions: []*model.Partition{{
			PartitionID:   2000,
			PartitionName: "_default",
			CollectionID:  1000,
			State:         etcdpb.PartitionState_PartitionCreated,
		}},
		State: etcdpb.CollectionState_CollectionCreated,
	}
	require.NoError(t, source.CreateCollection(ctx, coll, 101))
	require.NoError(t, source.CreateAlias(ctx, &model.Alias{
		Name:         "alias1",
		CollectionID: 1000,
		DbID:         10,
		State:        etcdpb.AliasState_AliasCreated,
	}, 102))
	require.NoError(t, source.SaveFileResource(ctx, &internalpb.FileResourceInfo{Id: 99, Name: "resource1"}, 1))

	result, err := migrateRootCoordCatalogSnapshot(ctx, source, target, 200)
	require.NoError(t, err)
	require.Equal(t, 1, result.Databases)
	require.Equal(t, 1, result.Collections)
	require.Equal(t, 1, result.Aliases)
	require.Equal(t, 1, result.FileResources)

	got, err := target.GetCollectionByName(ctx, 10, "db1", "coll1", typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Equal(t, int64(1000), got.CollectionID)
	require.Len(t, got.Partitions, 1)

	aliases, err := target.ListAliases(ctx, 10, typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Len(t, aliases, 1)
	require.Equal(t, "alias1", aliases[0].Name)

	resources, version, err := target.ListFileResource(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)
	require.Len(t, resources, 1)
}

func TestMetaTableCutoverCatalogMigratesAndReloadsWithoutReplacingMetaTable(t *testing.T) {
	ctx := context.Background()
	source := kvrootcoord.NewCatalog(memkv.NewMemoryKV())
	target := kvrootcoord.NewCatalog(memkv.NewMemoryKV())

	require.NoError(t, source.CreateDatabase(ctx, model.NewDefaultDatabase(nil), 99))
	db := &model.Database{ID: 10, Name: "db1"}
	require.NoError(t, source.CreateDatabase(ctx, db, 100))
	coll := &model.Collection{
		CollectionID: 1000,
		DBID:         10,
		DBName:       "db1",
		Name:         "coll1",
		Fields: []*model.Field{{
			FieldID:  100,
			Name:     "pk",
			DataType: schemapb.DataType_Int64,
		}},
		Partitions: []*model.Partition{{
			PartitionID:   2000,
			PartitionName: "_default",
			CollectionID:  1000,
			State:         etcdpb.PartitionState_PartitionCreated,
		}},
		State: etcdpb.CollectionState_CollectionCreated,
	}
	require.NoError(t, source.CreateCollection(ctx, coll, 101))

	meta, err := NewMetaTable(ctx, source, nil)
	require.NoError(t, err)
	metaPtr := meta

	result, err := meta.CutoverCatalog(ctx, target, 200)
	require.NoError(t, err)
	require.Equal(t, rootCoordCatalogMigrationResult{
		Databases:   2,
		Collections: 1,
	}, result)
	require.Same(t, metaPtr, meta)

	got, err := meta.GetCollectionByName(ctx, "db1", "coll1", typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	require.Equal(t, int64(1000), got.CollectionID)

	require.NoError(t, meta.CreateDatabase(ctx, &model.Database{ID: 11, Name: "db2"}, 300))
	_, err = target.GetCollectionByName(ctx, 10, "db1", "coll1", typeutil.MaxTimestamp)
	require.NoError(t, err)
	dbs, err := target.ListDatabases(ctx, typeutil.MaxTimestamp)
	require.NoError(t, err)
	dbNames := make([]string, 0, len(dbs))
	for _, db := range dbs {
		dbNames = append(dbNames, db.Name)
	}
	require.Contains(t, dbNames, "db2")
}
