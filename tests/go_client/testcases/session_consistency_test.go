package testcases

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// TestSessionConsistencyReadYourWrites verifies that Session consistency honors
// read-your-writes: a row inserted just before the search/query is immediately
// visible, because the SDK caches the insert timestamp and sends it as the
// request guarantee_timestamp.
//
// Method:
//  1. create a collection, build index and load it
//  2. insert one row (no flush, stays in the growing segment)
//  3. search with ClSession and verify the just-inserted row is returned
//  4. query with ClSession and verify the row is returned
//
// Expected: the just-inserted row is visible under Session consistency.
func TestSessionConsistencyReadYourWrites(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString("session_consistency", 6)

	// schema: int64 primary key + float vector
	schema := entity.NewSchema().
		WithName(collName).
		WithField(entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim))

	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)
	defer mc.DropCollection(ctx, client.NewDropCollectionOption(collName))

	// build index and load
	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, common.DefaultFloatVecFieldName, index.NewAutoIndex(entity.COSINE)))
	common.CheckErr(t, err, true)
	err = idxTask.Await(ctx)
	common.CheckErr(t, err, true)

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// insert one row without flush, it stays in the growing segment
	pk := int64(1)
	vec := common.GenFloatVector(common.DefaultDim)
	insertResult, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).
		WithColumns(
			column.NewColumnInt64(common.DefaultInt64FieldName, []int64{pk}),
			column.NewColumnFloatVector(common.DefaultFloatVecFieldName, common.DefaultDim, [][]float32{vec}),
		))
	common.CheckErr(t, err, true)
	require.Equal(t, int64(1), insertResult.InsertCount)

	// search immediately with Session consistency, the just-inserted row must be visible
	resSearch, err := mc.Search(ctx, client.NewSearchOption(collName, 1, []entity.Vector{entity.FloatVector(vec)}).
		WithANNSField(common.DefaultFloatVecFieldName).
		WithConsistencyLevel(entity.ClSession))
	common.CheckErr(t, err, true)
	require.Equal(t, 1, len(resSearch))
	require.Equal(t, 1, resSearch[0].ResultCount, "session consistency search should return the just-inserted row")
	gotID, err := resSearch[0].IDs.GetAsInt64(0)
	require.NoError(t, err)
	require.Equal(t, pk, gotID, "top-1 result should be the just-inserted row")

	// query with Session consistency, the just-inserted row must be visible too
	queryResult, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter(fmt.Sprintf("%s == %d", common.DefaultInt64FieldName, pk)).
		WithOutputFields(common.DefaultInt64FieldName).
		WithConsistencyLevel(entity.ClSession))
	common.CheckErr(t, err, true)
	require.Equal(t, 1, queryResult.ResultCount, "session consistency query should return the just-inserted row")
}

// TestSessionConsistencyDefaultCollection verifies the default-consistency path:
// when the collection is created with Session as its default consistency level
// and no explicit consistency level is given, the SDK still sends the cached
// write timestamp, so the just-inserted row is immediately visible.
//
// Method:
//  1. create a collection whose default consistency level is Session, build index and load
//  2. insert one row (no flush)
//  3. search/query without specifying a consistency level
//
// Expected: the just-inserted row is visible under the Session default.
func TestSessionConsistencyDefaultCollection(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString("session_default_consistency", 6)

	// schema: int64 primary key + float vector
	schema := entity.NewSchema().
		WithName(collName).
		WithField(entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim))

	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClSession))
	common.CheckErr(t, err, true)
	defer mc.DropCollection(ctx, client.NewDropCollectionOption(collName))

	// build index and load
	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, common.DefaultFloatVecFieldName, index.NewAutoIndex(entity.COSINE)))
	common.CheckErr(t, err, true)
	err = idxTask.Await(ctx)
	common.CheckErr(t, err, true)

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// insert one row without flush
	pk := int64(1)
	vec := common.GenFloatVector(common.DefaultDim)
	insertResult, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).
		WithColumns(
			column.NewColumnInt64(common.DefaultInt64FieldName, []int64{pk}),
			column.NewColumnFloatVector(common.DefaultFloatVecFieldName, common.DefaultDim, [][]float32{vec}),
		))
	common.CheckErr(t, err, true)
	require.Equal(t, int64(1), insertResult.InsertCount)

	// search with the collection default consistency (Session), no explicit level given
	resSearch, err := mc.Search(ctx, client.NewSearchOption(collName, 1, []entity.Vector{entity.FloatVector(vec)}).
		WithANNSField(common.DefaultFloatVecFieldName))
	common.CheckErr(t, err, true)
	require.Equal(t, 1, len(resSearch))
	require.Equal(t, 1, resSearch[0].ResultCount, "default session consistency search should return the just-inserted row")
	gotID, err := resSearch[0].IDs.GetAsInt64(0)
	require.NoError(t, err)
	require.Equal(t, pk, gotID, "top-1 result should be the just-inserted row")
}
