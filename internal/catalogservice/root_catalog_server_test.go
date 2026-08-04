package catalogservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/metastore"
	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestRootCatalogServerDatabaseCollectionAliasRoundTrip(t *testing.T) {
	ctx := context.Background()
	catalog := kvrootcoord.NewCatalog(memkv.NewMemoryKV())
	server := NewRootCatalogServer(StaticRootCoordCatalogResolver(map[string]metastore.RootCoordCatalog{
		"ns1": catalog,
	}))
	header := &catalogpb.CatalogRequestHeader{Namespace: "ns1", RequestId: "req-1"}

	createDB, err := server.CreateDatabase(ctx, &catalogpb.CreateDatabaseRequest{
		Header: header,
		Database: &etcdpb.DatabaseInfo{
			Id:   10,
			Name: "db1",
		},
		Ts: 100,
	})
	require.NoError(t, err)
	require.True(t, merr.Ok(createDB.GetHeader().GetStatus()), createDB.GetHeader().GetStatus().String())

	listDB, err := server.ListDatabases(ctx, &catalogpb.ListDatabasesRequest{
		Header: header,
		Ts:     typeutil.MaxTimestamp,
	})
	require.NoError(t, err)
	require.True(t, merr.Ok(listDB.GetHeader().GetStatus()), listDB.GetHeader().GetStatus().String())
	require.Len(t, listDB.GetDatabases(), 1)
	require.Equal(t, "db1", listDB.GetDatabases()[0].GetName())

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
	createColl, err := server.CreateCollection(ctx, &catalogpb.CreateCollectionRequest{
		Header:     header,
		Collection: rootCatalogCollectionFromModel(coll),
		Ts:         101,
	})
	require.NoError(t, err)
	require.True(t, merr.Ok(createColl.GetHeader().GetStatus()), createColl.GetHeader().GetStatus().String())

	getColl, err := server.GetCollectionByName(ctx, &catalogpb.GetCollectionByNameRequest{
		Header:         header,
		DbId:           10,
		DbName:         "db1",
		CollectionName: "coll1",
		Ts:             typeutil.MaxTimestamp,
	})
	require.NoError(t, err)
	require.True(t, merr.Ok(getColl.GetHeader().GetStatus()), getColl.GetHeader().GetStatus().String())
	require.Equal(t, int64(1000), getColl.GetCollection().GetCollection().GetID())
	require.Len(t, getColl.GetCollection().GetCollection().GetSchema().GetFields(), 1)
	require.Equal(t, "pk", getColl.GetCollection().GetCollection().GetSchema().GetFields()[0].GetName())
	require.Len(t, getColl.GetCollection().GetPartitions(), 1)

	createAlias, err := server.CreateAlias(ctx, &catalogpb.CreateAliasRequest{
		Header: header,
		Alias: &etcdpb.AliasInfo{
			AliasName:    "alias1",
			CollectionId: 1000,
			DbId:         10,
			State:        etcdpb.AliasState_AliasCreated,
		},
		Ts: 102,
	})
	require.NoError(t, err)
	require.True(t, merr.Ok(createAlias.GetHeader().GetStatus()), createAlias.GetHeader().GetStatus().String())

	listAlias, err := server.ListAliases(ctx, &catalogpb.ListAliasesRequest{
		Header: header,
		DbId:   10,
		Ts:     typeutil.MaxTimestamp,
	})
	require.NoError(t, err)
	require.True(t, merr.Ok(listAlias.GetHeader().GetStatus()), listAlias.GetHeader().GetStatus().String())
	require.Len(t, listAlias.GetAliases(), 1)
	require.Equal(t, "alias1", listAlias.GetAliases()[0].GetAliasName())
}
