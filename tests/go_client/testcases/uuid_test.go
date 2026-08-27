package testcases

import (
	"context"
	"fmt"
	"io"
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

func genUUID(i int) string {
	return fmt.Sprintf("%08x-0000-4000-8000-%012x", i, i)
}

func uuidSchema(collName string) *entity.Schema {
	return entity.NewSchema().WithName(collName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeUUID).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim))
}

func uuidColumnData(nb int) ([]string, [][]float32) {
	ids := make([]string, nb)
	vectors := make([][]float32, nb)
	for i := 0; i < nb; i++ {
		ids[i] = genUUID(i)
		vectors[i] = common.GenFloatVector(common.DefaultDim)
	}
	return ids, vectors
}

// TestUUIDCreateInsertQuery tests create, insert, query with == and in, uuid normalization and invalid uuid rejection
func TestUUIDCreateInsertQuery(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(prefix, 6)
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, uuidSchema(collName)))
	common.CheckErr(t, err, true)
	t.Cleanup(func() {
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	nb := 100
	upperUUID := "550E8400-E29B-41D4-A716-446655440000"
	lowerUUID := "550e8400-e29b-41d4-a716-446655440000"
	ids, vectors := uuidColumnData(nb)
	ids = append(ids, upperUUID)
	vectors = append(vectors, common.GenFloatVector(common.DefaultDim))
	insertRes, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, column.NewColumnUUID("id", ids),
		column.NewColumnFloatVector(common.DefaultFloatVecFieldName, common.DefaultDim, vectors)))
	common.CheckErr(t, err, true)
	require.Equal(t, int64(nb+1), insertRes.InsertCount)

	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, column.NewColumnUUID("id", []string{"not-a-valid-uuid"}),
		column.NewColumnFloatVector(common.DefaultFloatVecFieldName, common.DefaultDim, [][]float32{common.GenFloatVector(common.DefaultDim)})))
	common.CheckErr(t, err, false, "invalid uuid", "invalid UUID")

	task, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	err = task.Await(ctx)
	common.CheckErr(t, err, true)

	// query == returns the row with expected uuid
	queryRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter(fmt.Sprintf("id == \"%s\"", genUUID(0))).
		WithOutputFields("id").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, 1, queryRes.ResultCount)
	uuidStr, err := queryRes.Fields[0].GetAsString(0)
	common.CheckErr(t, err, true)
	require.Equal(t, genUUID(0), uuidStr)

	// query in returns both rows
	queryRes, err = mc.Query(ctx, client.NewQueryOption(collName).WithFilter(fmt.Sprintf("id in [\"%s\", \"%s\"]", genUUID(1), genUUID(2))).
		WithOutputFields("id").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, 2, queryRes.ResultCount)

	// query with lowercase canonical form finds the uppercase inserted row
	queryRes, err = mc.Query(ctx, client.NewQueryOption(collName).WithFilter(fmt.Sprintf("id == \"%s\"", lowerUUID)).
		WithOutputFields("id").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, 1, queryRes.ResultCount)
	uuidStr, err = queryRes.Fields[0].GetAsString(0)
	common.CheckErr(t, err, true)
	require.Equal(t, lowerUUID, uuidStr)
}

// TestUUIDDelete tests delete with uuid pks and delete with expr
func TestUUIDDelete(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(prefix, 6)
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, uuidSchema(collName)))
	common.CheckErr(t, err, true)
	t.Cleanup(func() {
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	nb := 100
	ids, vectors := uuidColumnData(nb)
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, column.NewColumnUUID("id", ids),
		column.NewColumnFloatVector(common.DefaultFloatVecFieldName, common.DefaultDim, vectors)))
	common.CheckErr(t, err, true)

	task, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	err = task.Await(ctx)
	common.CheckErr(t, err, true)

	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "id", index.NewTrieIndex()))
	common.CheckErr(t, err, true)
	err = idxTask.Await(ctx)
	common.CheckErr(t, err, true)

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// delete with uuid pks
	delRes, err := mc.Delete(ctx, client.NewDeleteOption(collName).WithStringIDs("id", []string{genUUID(0), genUUID(1)}))
	common.CheckErr(t, err, true)
	require.Equal(t, int64(2), delRes.DeleteCount)

	// query, verify delete success
	queryRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter(fmt.Sprintf("id in [\"%s\", \"%s\"]", genUUID(0), genUUID(1))).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Zero(t, queryRes.ResultCount)

	// delete with expr
	delRes, err = mc.Delete(ctx, client.NewDeleteOption(collName).WithExpr(fmt.Sprintf("id in [\"%s\", \"%s\", \"%s\"]", genUUID(2), genUUID(3), genUUID(4))))
	common.CheckErr(t, err, true)
	require.Equal(t, int64(3), delRes.DeleteCount)

	// query, verify delete success
	queryRes, err = mc.Query(ctx, client.NewQueryOption(collName).WithFilter(fmt.Sprintf("id >= \"%s\"", genUUID(0))).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, nb-5, queryRes.ResultCount)
}

// TestUUIDIndexLoad tests trie index on uuid pk and query after load
func TestUUIDIndexLoad(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(prefix, 6)
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, uuidSchema(collName)))
	common.CheckErr(t, err, true)
	t.Cleanup(func() {
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	nb := 100
	ids, vectors := uuidColumnData(nb)
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, column.NewColumnUUID("id", ids),
		column.NewColumnFloatVector(common.DefaultFloatVecFieldName, common.DefaultDim, vectors)))
	common.CheckErr(t, err, true)

	task, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	err = task.Await(ctx)
	common.CheckErr(t, err, true)

	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "id", index.NewTrieIndex()))
	common.CheckErr(t, err, true)
	err = idxTask.Await(ctx)
	common.CheckErr(t, err, true)

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// query == after index and load
	queryRes, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter(fmt.Sprintf("id == \"%s\"", genUUID(0))).
		WithOutputFields("id").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, 1, queryRes.ResultCount)

	// query in after index and load
	queryRes, err = mc.Query(ctx, client.NewQueryOption(collName).WithFilter(fmt.Sprintf("id in [\"%s\", \"%s\"]", genUUID(1), genUUID(2))).
		WithOutputFields("id").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, 2, queryRes.ResultCount)
}

// TestUUIDFlushLoadFilter verifies representation convergence: insert → query before flush (growing) and after flush+load (sealed) must return same rows for both PK and non-PK UUID fields
func TestUUIDFlushLoadFilter(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(prefix, 6)
	// PK id uuid + non-PK device_uuid + vector
	schema := entity.NewSchema().WithName(collName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeUUID).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("device_uuid").WithDataType(entity.FieldTypeUUID)).
		WithField(entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim))
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)
	t.Cleanup(func() { _ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName)) })

	nb := 20
	ids, vectors := uuidColumnData(nb)
	deviceUUIDs := make([]string, nb)
	for i := 0; i < nb; i++ {
		deviceUUIDs[i] = genUUID(i + 1000)
	}
	// insert PK + non-PK uuid
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName,
		column.NewColumnUUID("id", ids),
		column.NewColumnUUID("device_uuid", deviceUUIDs),
		column.NewColumnFloatVector(common.DefaultFloatVecFieldName, common.DefaultDim, vectors)))
	common.CheckErr(t, err, true)

	// query BEFORE flush (growing segment) — must find 1 row
	qBefore, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter(fmt.Sprintf("id == \"%s\"", genUUID(5))).WithOutputFields("id", "device_uuid").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, 1, qBefore.ResultCount, "growing segment filter must match")

	// flush + load to sealed
	task, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	require.NoError(t, task.Await(ctx))
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	require.NoError(t, loadTask.Await(ctx))

	// same PK predicate after flush must still match (sealed) — would be 0 before convergence fix
	qAfter, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter(fmt.Sprintf("id == \"%s\"", genUUID(5))).WithOutputFields("id").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, 1, qAfter.ResultCount, "sealed segment filter must match growing")

	// also test non-PK uuid field after flush
	qNonPK, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter(fmt.Sprintf("device_uuid == \"%s\"", genUUID(1005))).WithOutputFields("id").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, 1, qNonPK.ResultCount, "non-PK UUID filter after flush must match")

	// IN predicate after index
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "id", index.NewAutoIndex(entity.COSINE)))
	common.CheckErr(t, err, true)
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "device_uuid", index.NewInvertedIndex()))
	common.CheckErr(t, err, true)
	loadTask2, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	require.NoError(t, loadTask2.Await(ctx))

	qIn, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter(fmt.Sprintf("id in [\"%s\", \"%s\"]", genUUID(0), genUUID(1))).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Equal(t, 2, qIn.ResultCount, "IN filter after index must match")
}

// TestUUIDSDKPaths tests row based insert, search by ids and query iterator with uuid pk
func TestUUIDSDKPaths(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(prefix, 6)
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, uuidSchema(collName)))
	common.CheckErr(t, err, true)
	t.Cleanup(func() {
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	type UUIDRow struct {
		ID       string    `json:"id,omitempty" milvus:"name:id"`
		FloatVec []float32 `json:"floatVec,omitempty" milvus:"name:floatVec"`
	}

	nb := 100
	ids, vectors := uuidColumnData(nb)
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName, column.NewColumnUUID("id", ids),
		column.NewColumnFloatVector(common.DefaultFloatVecFieldName, common.DefaultDim, vectors)))
	common.CheckErr(t, err, true)

	// row based insert with uuid pk
	insertRes, err := mc.Insert(ctx, client.NewRowBasedInsertOption(collName,
		UUIDRow{ID: genUUID(nb), FloatVec: common.GenFloatVector(common.DefaultDim)},
		UUIDRow{ID: genUUID(nb + 1), FloatVec: common.GenFloatVector(common.DefaultDim)}))
	common.CheckErr(t, err, true)
	require.Equal(t, int64(2), insertRes.InsertCount)
	require.Equal(t, 2, insertRes.IDs.Len())

	task, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	err = task.Await(ctx)
	common.CheckErr(t, err, true)

	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "id", index.NewTrieIndex()))
	common.CheckErr(t, err, true)
	err = idxTask.Await(ctx)
	common.CheckErr(t, err, true)

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// search by ids with uuid pk
	searchOption := client.NewSearchByIDsOption(collName, common.DefaultLimit, column.NewColumnUUID("id", []string{genUUID(0), genUUID(nb), genUUID(nb + 1)})).
		WithANNSField(common.DefaultFloatVecFieldName).
		WithConsistencyLevel(entity.ClStrong)
	resSearch, err := mc.Search(ctx, searchOption)
	common.CheckErr(t, err, true)
	require.Equal(t, 3, len(resSearch))
	for _, resultSet := range resSearch {
		require.Greater(t, resultSet.ResultCount, 0)
	}

	// query iterator with uuid pk
	itr, err := mc.QueryIterator(ctx, client.NewQueryIteratorOption(collName).WithOutputFields("id").WithBatchSize(2).WithIteratorLimit(2))
	common.CheckErr(t, err, true)
	rs, err := itr.Next(ctx)
	common.CheckErr(t, err, true)
	require.Equal(t, 2, rs.ResultCount)
	require.Equal(t, 2, rs.Fields[0].Len())
	uuidStr, err := rs.Fields[0].GetAsString(0)
	common.CheckErr(t, err, true)
	require.NotEmpty(t, uuidStr)

	// iterator exhausted, next returns io.EOF
	_, err = itr.Next(ctx)
	require.ErrorIs(t, err, io.EOF)
}
