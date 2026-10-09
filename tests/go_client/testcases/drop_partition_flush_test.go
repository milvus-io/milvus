package testcases

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/entity"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// Dropping one partition flushes earlier segments across the VChannel. Inserts
// into a surviving partition must allocate a new segment and survive reload.
func TestDropPartitionPreservesSurvivingPartitionInserts(t *testing.T) {
	ctx := hp.CreateContext(t, 3*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc,
		hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption(), hp.TWithShardNum(1))
	name := schema.CollectionName
	removed := "removed"
	require.NoError(t, mc.CreatePartition(ctx, client.NewCreatePartitionOption(name, removed)))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(100))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema).TWithPartitionName(removed),
		hp.TNewDataOption().TWithNb(100).TWithStart(1000))
	require.NoError(t, mc.DropPartition(ctx, client.NewDropPartitionOption(name, removed)))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(100).TWithStart(100))
	flush, err := mc.Flush(ctx, client.NewFlushOption(name))
	require.NoError(t, err)
	require.NoError(t, flush.Await(ctx))
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(name))
	require.NoError(t, mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(name)))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(name))
	result, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("int64 >= 0").
		WithOutputFields(common.DefaultInt64FieldName).WithConsistencyLevel(entity.ClStrong))
	require.NoError(t, err)
	require.Equal(t, 200, result.ResultCount)
	ids := make([]int64, 0, result.ResultCount)
	for i := range result.ResultCount {
		id, err := result.GetColumn(common.DefaultInt64FieldName).GetAsInt64(i)
		require.NoError(t, err)
		ids = append(ids, id)
	}
	expected := make([]int64, 200)
	for i := range expected {
		expected[i] = int64(i)
	}
	require.ElementsMatch(t, expected, ids)
}
