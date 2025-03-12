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
	// 创建上下文和客户端
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// 创建集合
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// 插入数据
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(100))

	// 刷新数据
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// 创建索引
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))

	// 加载集合
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// 准备搜索向量
	vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)

	// 执行搜索
	searchResult, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors).
		WithConsistencyLevel(entity.ClStrong))
	require.NoError(t, err)
	require.Equal(t, common.DefaultNq, len(searchResult))

	// 获取主键字段名
	var pkFieldName string
	for _, field := range schema.Fields {
		if field.IsPrimaryKey {
			pkFieldName = field.Name
			break
		}
	}
	require.NotEmpty(t, pkFieldName, "Primary key field not found")

	// 执行查询
	queryResult, err := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).
		WithFilter(pkFieldName+" in [1, 2, 3, 4]").
		WithOutputFields("*"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, queryResult.GetColumn(pkFieldName).Len(), 4)

	// 释放集合
	err = mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(schema.CollectionName))
	require.NoError(t, err)

	// 重新插入数据
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(100))

	// 重新加载集合
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// 执行批量搜索
	vectors = hp.GenSearchVectors(5, common.DefaultDim, entity.FieldTypeFloatVector)
	searchResult, err = mc.Search(ctx, client.NewSearchOption(schema.CollectionName, 5, vectors).
		WithConsistencyLevel(entity.ClStrong))
	require.NoError(t, err)
	require.Equal(t, 5, len(searchResult))
	require.LessOrEqual(t, len(searchResult[0].IDs.(*column.ColumnInt64).Data()), 5)
} 