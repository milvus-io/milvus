package rootcoord

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestDDLCallbacksCollectionDDL(t *testing.T) {
	core := initStreamingSystemAndCore(t)

	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)
	partitionName := "testPartition" + funcutil.RandomString(10)
	testSchema := &schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				Name:     "field1",
				DataType: schemapb.DataType_Int64,
			},
		},
	}
	schemaBytes, err := proto.Marshal(testSchema)
	require.NoError(t, err)

	// drop a collection that db not exist should be ignored.
	status, err := core.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		DbName:         "notExistDB",
		CollectionName: collectionName,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	// drop a collection that collection not exist should be ignored.
	status, err = core.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		DbName:         dbName,
		CollectionName: "notExistCollection",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	// create a collection that database not exist should return error.
	status, err = core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         "notExistDB",
		CollectionName: collectionName,
		Schema:         schemaBytes,
	})
	require.Error(t, merr.CheckRPCCall(status, err))

	// Test CreateCollection
	// create a database and a collection.
	status, err = core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{
		DbName: dbName,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	status, err = core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         schemaBytes,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Equal(t, coll.Name, collectionName)
	// create a collection with same schema should be idempotent.
	status, err = core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         schemaBytes,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	// Test CreatePartition
	status, err = core.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		PartitionName:  partitionName,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	coll, err = core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Len(t, coll.Partitions, 2)
	require.Contains(t, lo.Map(coll.Partitions, func(p *model.Partition, _ int) string { return p.PartitionName }), partitionName)
	// create a partition with same name should be idempotent.
	status, err = core.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		PartitionName:  partitionName,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	coll, err = core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Len(t, coll.Partitions, 2)

	status, err = core.DropPartition(ctx, &milvuspb.DropPartitionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		PartitionName:  partitionName,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	// drop a partition that partition not exist should be idempotent.
	status, err = core.DropPartition(ctx, &milvuspb.DropPartitionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		PartitionName:  partitionName,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	// Test DropCollection
	// drop the collection should be ok.
	status, err = core.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	coll, err = core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	require.Error(t, err)
	// drop a dropped collection should be idempotent.
	status, err = core.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
}
