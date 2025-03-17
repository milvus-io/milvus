package testcases

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v2/entity"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

func TestMilvusDefault(t *testing.T) {
	// Create context and client
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// Create collection
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// Insert data
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(100))

	// Flush data
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// Create index
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))

	// Load collection
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// Prepare search vectors
	vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)

	// Execute search
	searchResult, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).
		WithConsistencyLevel(entity.ClStrong))
	require.NoError(t, err)
	require.Equal(t, common.DefaultNq, len(searchResult))

	// Get primary key field name
	var pkFieldName string
	for _, field := range schema.Fields {
		if field.IsPrimaryKey {
			pkFieldName = field.Name
			break
		}
	}
	require.NotEmpty(t, pkFieldName, "Primary key field not found")

	// Execute query
	queryResult, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).
		WithFilter(pkFieldName+" in [1, 2, 3, 4]").
		WithOutputFields("*"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, queryResult.GetColumn(pkFieldName).Len(), 4)

	// Release collection
	err = mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(schema.CollectionName))
	require.NoError(t, err)

	// Re-insert data
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(100))

	// Reload collection
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// Execute batch search
	vectors = hp.GenSearchVectors(5, common.DefaultDim, entity.FieldTypeFloatVector)
	searchResult, err = mc.Search(ctx, client.NewSearchOption(schema.CollectionName, 5, vectors).
		WithConsistencyLevel(entity.ClStrong))
	require.NoError(t, err)
	require.Equal(t, 5, len(searchResult))
	require.LessOrEqual(t, len(searchResult[0].IDs.(*column.ColumnInt64).Data()), 5)
} 