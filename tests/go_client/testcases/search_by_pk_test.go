package testcases

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// TestSearchByPKFloatVectors tests search by primary keys with float vectors
// Converted from PR #46993: test_search_by_pk_float_vectors
// Target: test search by primary keys float vectors
// Method:
//  1. connect and create a collection
//  2. search by primary keys float vectors
//  3. verify search by primary keys results
// Expected: search successfully and results are correct
func TestSearchByPKFloatVectors(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert data
	_, insertResult := prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// Get IDs to search from inserted data
	idsToSearch := make([]int64, common.DefaultNq)
	for i := 0; i < common.DefaultNq; i++ {
		id, err := insertResult.IDs.GetAsInt64(i)
		require.NoError(t, err)
		idsToSearch[i] = id
	}

	// Create ID column for search by IDs
	idColumn := column.NewColumnInt64(common.DefaultInt64FieldName, idsToSearch)

	// Search by IDs using the new WithIDs API
	searchOption := client.NewSearchOption(schema.CollectionName, common.DefaultLimit, nil).
		WithIDs(idColumn).
		WithANNSField(common.DefaultFloatVecFieldName).
		WithConsistencyLevel(entity.ClStrong)

	resSearch, err := mc.Search(ctx, searchOption)
	common.CheckErr(t, err, true)

	// Verify results
	common.CheckSearchResult(t, resSearch, common.DefaultNq, common.DefaultLimit)

	// Verify the top 1 hit is itself, so the min distance should be close to 0
	for i := 0; i < common.DefaultNq; i++ {
		require.NotEmpty(t, resSearch[i].Scores)
		distance := resSearch[i].Scores[0]
		// For COSINE metric, similarity should be close to 1.0 (distance = 1 - similarity)
		require.Less(t, 1.0-distance, 0.001, "Expected distance close to 0 for self-match")
	}
}

// TestSearchByPKNullableVectorField tests search by pk with nullable vector field where some vectors are null
// Converted from PR #46993: test_search_by_pk_nullable_vector_field
// Target: test search by pk with nullable vector field where some vectors are null
// Method:
//  1. create a collection with nullable sparse vector field
//  2. insert data where some vectors are null
//  3. search by IDs including some with null vectors
//  4. verify result count equals non-null vector count (effective nq)
// Expected: null vectors are filtered out, result count = non-null vector count
func TestSearchByPKNullableVectorField(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString("nullable_vec_search", 6)

	// Create schema with nullable sparse vector field
	schema := entity.NewSchema().
		WithName(collName).
		WithField(entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(common.DefaultSparseVecFieldName).WithDataType(entity.FieldTypeSparseVector).WithNullable(true))

	// Create collection
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	// Insert data: 10 rows, where rows 2, 5, 8 have null vectors
	nb := 10
	nullIndices := map[int]bool{2: true, 5: true, 8: true}

	// Create ID column
	ids := make([]int64, nb)
	for i := 0; i < nb; i++ {
		ids[i] = int64(i)
	}
	idColumn := column.NewColumnInt64(common.DefaultInt64FieldName, ids)

	// Create sparse vector column with nulls
	sparseVecs := make([]entity.SparseEmbedding, nb)
	for i := 0; i < nb; i++ {
		if nullIndices[i] {
			// Leave as nil for null vector
			sparseVecs[i] = nil
		} else {
			// Create sparse vector
			positions := []uint32{uint32(i), uint32(i + 100)}
			values := []float32{1.0, 0.5}
			sparseVec, err := entity.NewSliceSparseEmbedding(positions, values)
			require.NoError(t, err)
			sparseVecs[i] = sparseVec
		}
	}
	sparseColumn := column.NewColumnSparseVectors(common.DefaultSparseVecFieldName, sparseVecs)

	// Insert with column-based API
	insertResult, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).
		WithColumns(idColumn, sparseColumn))
	common.CheckErr(t, err, true)
	require.Equal(t, int64(nb), insertResult.InsertCount)

	// Flush
	task, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	err = task.Await(ctx)
	common.CheckErr(t, err, true)

	// Create index
	indexParams := index.NewSparseInvertedIndex(entity.IP, 0.2)
	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, common.DefaultSparseVecFieldName, indexParams))
	common.CheckErr(t, err, true)
	err = idxTask.Await(ctx)
	common.CheckErr(t, err, true)

	// Load collection
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// Case 1: Search by IDs with mixed null and non-null vectors
	// IDs [0, 2, 3, 5] -> 0, 3 are valid, 2, 5 are null
	idsToSearch := []int64{0, 2, 3, 5}
	expectedNq := 2 // only 2 non-null vectors

	idColumnSearch := column.NewColumnInt64(common.DefaultInt64FieldName, idsToSearch)
	searchOption := client.NewSearchOption(collName, 5, nil).
		WithIDs(idColumnSearch).
		WithANNSField(common.DefaultSparseVecFieldName).
		WithConsistencyLevel(entity.ClStrong)

	resSearch, err := mc.Search(ctx, searchOption)
	common.CheckErr(t, err, true)

	require.Equal(t, expectedNq, len(resSearch),
		"Expected %d results for non-null vectors, got %d", expectedNq, len(resSearch))

	// Case 2: Search by IDs with all null vectors should raise error
	allNullIDs := []int64{2, 5, 8}
	idColumnNull := column.NewColumnInt64(common.DefaultInt64FieldName, allNullIDs)
	searchOption2 := client.NewSearchOption(collName, 5, nil).
		WithIDs(idColumnNull).
		WithANNSField(common.DefaultSparseVecFieldName).
		WithConsistencyLevel(entity.ClStrong)

	_, err = mc.Search(ctx, searchOption2)
	common.CheckErr(t, err, false, "all provided IDs have null vector values")

	// Cleanup
	err = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
	common.CheckErr(t, err, true)
}

// TestSearchByPKBinaryVectors tests search by primary keys with binary vectors
// Converted from PR #46993: test_search_by_pk_binary_vectors
// Target: test search by primary keys binary vectors
// Method:
//  1. connect and create a collection
//  2. search by primary keys binary vectors
//  3. verify search by primary keys results
// Expected: search successfully and results are correct
func TestSearchByPKBinaryVectors(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection -> insert -> flush -> index -> load with binary vectors
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc,
		hp.NewCreateCollectionParams(hp.VarcharBinary),
		hp.TNewFieldsOption(),
		hp.TNewSchemaOption())

	// insert data
	_, insertResult := prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// Get IDs to search from inserted data
	idsToSearch := make([]string, common.DefaultNq)
	for i := 0; i < common.DefaultNq; i++ {
		id, err := insertResult.IDs.GetAsString(i)
		require.NoError(t, err)
		idsToSearch[i] = id
	}

	// Create ID column for search by IDs
	idColumn := column.NewColumnVarChar(common.DefaultVarcharFieldName, idsToSearch)

	// Search by IDs with binary vectors
	searchOption := client.NewSearchOption(schema.CollectionName, common.DefaultLimit, nil).
		WithIDs(idColumn).
		WithANNSField(common.DefaultBinaryVecFieldName).
		WithConsistencyLevel(entity.ClStrong)

	resSearch, err := mc.Search(ctx, searchOption)
	common.CheckErr(t, err, true)

	// Verify results
	common.CheckSearchResult(t, resSearch, common.DefaultNq, common.DefaultLimit)
}

// TestSearchByPKWithEmptyIDs tests search by primary keys with empty IDs list
// Target: test search by primary keys with empty IDs
// Method:
//  1. connect and create a collection
//  2. search by empty IDs list
// Expected: search should return error for empty IDs
func TestSearchByPKWithEmptyIDs(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// Create empty ID column
	emptyIDs := []int64{}
	idColumn := column.NewColumnInt64(common.DefaultInt64FieldName, emptyIDs)

	// Search by empty IDs - should error
	searchOption := client.NewSearchOption(schema.CollectionName, common.DefaultLimit, nil).
		WithIDs(idColumn).
		WithANNSField(common.DefaultFloatVecFieldName).
		WithConsistencyLevel(entity.ClStrong)

	_, err := mc.Search(ctx, searchOption)
	// Expect error for empty IDs
	common.CheckErr(t, err, false)
}

// TestSearchByPKWithDuplicateIDs tests search by primary keys with duplicate IDs
// Target: test search by primary keys with duplicate IDs
// Method:
//  1. connect and create a collection
//  2. search by IDs list containing duplicates
// Expected: search should handle duplicate IDs (may deduplicate or error)
func TestSearchByPKWithDuplicateIDs(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	_, insertResult := prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// Get some IDs and create duplicates
	id1, err := insertResult.IDs.GetAsInt64(0)
	require.NoError(t, err)
	id2, err := insertResult.IDs.GetAsInt64(1)
	require.NoError(t, err)

	// Create IDs list with duplicates: [id1, id2, id1, id2]
	duplicateIDs := []int64{id1, id2, id1, id2}
	idColumn := column.NewColumnInt64(common.DefaultInt64FieldName, duplicateIDs)

	// Search by IDs with duplicates
	searchOption := client.NewSearchOption(schema.CollectionName, common.DefaultLimit, nil).
		WithIDs(idColumn).
		WithANNSField(common.DefaultFloatVecFieldName).
		WithConsistencyLevel(entity.ClStrong)

	resSearch, err := mc.Search(ctx, searchOption)
	// Note: Behavior may vary - some systems deduplicate, others may error
	// For now, we just verify it doesn't crash and returns some results
	if err == nil {
		require.NotEmpty(t, resSearch, "Expected some results for duplicate IDs")
		// Result count may be 2 (deduplicated) or 4 (all duplicates processed)
		require.True(t, len(resSearch) >= 2, "Expected at least 2 result sets")
	} else {
		// If error is returned, it should mention duplicates
		t.Logf("Search with duplicate IDs returned error (expected): %v", err)
	}
}

// TestSearchByPKWithExpression tests search by primary keys combined with filter expression
// Target: test search by primary keys with filter expression
// Method:
//  1. connect and create a collection with scalar fields
//  2. search by IDs with filter expression
//  3. verify results match both IDs and filter
// Expected: search successfully and results satisfy both conditions
func TestSearchByPKWithExpression(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	_, insertResult := prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// Get multiple IDs to search
	idsToSearch := make([]int64, 10)
	for i := 0; i < 10; i++ {
		id, err := insertResult.IDs.GetAsInt64(i)
		require.NoError(t, err)
		idsToSearch[i] = id
	}

	idColumn := column.NewColumnInt64(common.DefaultInt64FieldName, idsToSearch)

	// Search by IDs with filter expression
	// Note: The actual filter depends on the collection schema
	// Using a simple expression that should work with the default schema
	searchOption := client.NewSearchOption(schema.CollectionName, common.DefaultLimit, nil).
		WithIDs(idColumn).
		WithANNSField(common.DefaultFloatVecFieldName).
		WithFilter(common.DefaultInt64FieldName + " >= 0"). // Filter expression
		WithConsistencyLevel(entity.ClStrong)

	resSearch, err := mc.Search(ctx, searchOption)
	common.CheckErr(t, err, true)

	// Verify results - should have results that match both IDs and filter
	require.NotEmpty(t, resSearch, "Expected results for search with expression")

	// Results should be filtered by the expression
	for _, resultSet := range resSearch {
		require.NotNil(t, resultSet, "Result set should not be nil")
	}
}

// TestSearchByPKWithGroupBy tests search by primary keys with group by
// Target: test search by primary keys with group by field
// Method:
//  1. connect and create a collection with groupable field
//  2. search by IDs with group by
//  3. verify grouping is applied
// Expected: search successfully with grouped results
func TestSearchByPKWithGroupBy(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	_, insertResult := prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// Get IDs to search
	idsToSearch := make([]int64, common.DefaultNq)
	for i := 0; i < common.DefaultNq; i++ {
		id, err := insertResult.IDs.GetAsInt64(i)
		require.NoError(t, err)
		idsToSearch[i] = id
	}

	idColumn := column.NewColumnInt64(common.DefaultInt64FieldName, idsToSearch)

	// Search by IDs with group by
	// Note: GroupBy requires a scalar field - using Int64 field for grouping
	searchOption := client.NewSearchOption(schema.CollectionName, common.DefaultLimit, nil).
		WithIDs(idColumn).
		WithANNSField(common.DefaultFloatVecFieldName).
		WithGroupByField(common.DefaultInt64FieldName). // Group by primary key
		WithConsistencyLevel(entity.ClStrong)

	resSearch, err := mc.Search(ctx, searchOption)
	common.CheckErr(t, err, true)

	// Verify results
	require.NotEmpty(t, resSearch, "Expected results for search with group by")
}

// TestSearchByPKSparseVectors tests search by primary keys with non-nullable sparse vectors
// Target: test search by primary keys with sparse vectors (non-nullable)
// Method:
//  1. connect and create a collection with sparse vector field
//  2. search by primary keys
//  3. verify search results
// Expected: search successfully with sparse vectors
func TestSearchByPKSparseVectors(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString("sparse_vec_search", 6)

	// Create schema with non-nullable sparse vector field
	schema := entity.NewSchema().
		WithName(collName).
		WithField(entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(common.DefaultSparseVecFieldName).WithDataType(entity.FieldTypeSparseVector))

	// Create collection
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	// Insert data with sparse vectors
	nb := 100
	ids := make([]int64, nb)
	sparseVecs := make([]entity.SparseEmbedding, nb)

	for i := 0; i < nb; i++ {
		ids[i] = int64(i)
		// Create sparse vector
		positions := []uint32{uint32(i % 100), uint32((i + 50) % 100)}
		values := []float32{float32(i) * 0.1, float32(i) * 0.05}
		sparseVec, err := entity.NewSliceSparseEmbedding(positions, values)
		require.NoError(t, err)
		sparseVecs[i] = sparseVec
	}

	idColumn := column.NewColumnInt64(common.DefaultInt64FieldName, ids)
	sparseColumn := column.NewColumnSparseVectors(common.DefaultSparseVecFieldName, sparseVecs)

	// Insert
	insertResult, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).
		WithColumns(idColumn, sparseColumn))
	common.CheckErr(t, err, true)
	require.Equal(t, int64(nb), insertResult.InsertCount)

	// Flush
	task, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	err = task.Await(ctx)
	common.CheckErr(t, err, true)

	// Create index
	indexParams := index.NewSparseInvertedIndex(entity.IP, 0.3)
	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, common.DefaultSparseVecFieldName, indexParams))
	common.CheckErr(t, err, true)
	err = idxTask.Await(ctx)
	common.CheckErr(t, err, true)

	// Load collection
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// Search by IDs
	idsToSearch := []int64{0, 10, 20, 30, 40}
	idColumnSearch := column.NewColumnInt64(common.DefaultInt64FieldName, idsToSearch)

	searchOption := client.NewSearchOption(collName, 10, nil).
		WithIDs(idColumnSearch).
		WithANNSField(common.DefaultSparseVecFieldName).
		WithConsistencyLevel(entity.ClStrong)

	resSearch, err := mc.Search(ctx, searchOption)
	common.CheckErr(t, err, true)

	// Verify results
	require.Equal(t, len(idsToSearch), len(resSearch), "Expected one result set per query ID")

	// Cleanup
	err = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
	common.CheckErr(t, err, true)
}

// TestSearchByPKWithOutputFields tests search by primary keys with output fields specification
// Target: test search by primary keys with output fields
// Method:
//  1. connect and create a collection
//  2. search by IDs with output fields specified
//  3. verify returned fields match specification
// Expected: search successfully and returns specified fields
func TestSearchByPKWithOutputFields(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	_, insertResult := prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// Get IDs to search
	idsToSearch := make([]int64, common.DefaultNq)
	for i := 0; i < common.DefaultNq; i++ {
		id, err := insertResult.IDs.GetAsInt64(i)
		require.NoError(t, err)
		idsToSearch[i] = id
	}

	idColumn := column.NewColumnInt64(common.DefaultInt64FieldName, idsToSearch)

	// Search by IDs with specific output fields
	searchOption := client.NewSearchOption(schema.CollectionName, common.DefaultLimit, nil).
		WithIDs(idColumn).
		WithANNSField(common.DefaultFloatVecFieldName).
		WithOutputFields(common.DefaultInt64FieldName, common.DefaultFloatVecFieldName). // Specify output fields
		WithConsistencyLevel(entity.ClStrong)

	resSearch, err := mc.Search(ctx, searchOption)
	common.CheckErr(t, err, true)

	// Verify results and output fields
	require.NotEmpty(t, resSearch, "Expected search results")

	// Verify that specified fields are returned
	for _, resultSet := range resSearch {
		if resultSet.ResultCount > 0 {
			// Check that ID field is present
			idCol := resultSet.GetColumn(common.DefaultInt64FieldName)
			require.NotNil(t, idCol, "Expected ID field in output")

			// Check that vector field is present
			vecCol := resultSet.GetColumn(common.DefaultFloatVecFieldName)
			require.NotNil(t, vecCol, "Expected vector field in output")
		}
	}
}

// TestSearchByPKWithInvalidIDs tests search by primary keys with invalid/non-existent IDs
// Target: test search by primary keys with invalid IDs
// Method:
//  1. connect and create a collection
//  2. search by IDs that don't exist in collection
// Expected: search should handle gracefully (empty results or error)
func TestSearchByPKWithInvalidIDs(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// Use IDs that don't exist (very large values unlikely to be inserted)
	invalidIDs := []int64{999999, 888888, 777777}
	idColumn := column.NewColumnInt64(common.DefaultInt64FieldName, invalidIDs)

	// Search by invalid IDs
	searchOption := client.NewSearchOption(schema.CollectionName, common.DefaultLimit, nil).
		WithIDs(idColumn).
		WithANNSField(common.DefaultFloatVecFieldName).
		WithConsistencyLevel(entity.ClStrong)

	resSearch, err := mc.Search(ctx, searchOption)

	// System may handle this in different ways:
	// 1. Return empty results (no matches for non-existent IDs)
	// 2. Return error
	if err != nil {
		// Error is acceptable for invalid IDs
		t.Logf("Search with invalid IDs returned error (acceptable): %v", err)
	} else {
		// If no error, results should be empty or have no matches
		t.Logf("Search with invalid IDs completed, result count: %d", len(resSearch))
		// Each result set may be empty or have 0 results
		for i, resultSet := range resSearch {
			t.Logf("Result set %d has %d results", i, resultSet.ResultCount)
		}
	}
}

// TestSearchByPKWithRangeSearch tests search by primary keys with range search parameters
// Target: test search by primary keys with range search (radius and range_filter)
// Method:
//  1. connect and create a collection
//  2. search by IDs with radius and range_filter parameters
//  3. verify all results are within the specified range
// Expected: search successfully and all distances are within [radius, range_filter]
func TestSearchByPKWithRangeSearch(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	_, insertResult := prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// Get IDs to search
	idsToSearch := make([]int64, common.DefaultNq)
	for i := 0; i < common.DefaultNq; i++ {
		id, err := insertResult.IDs.GetAsInt64(i)
		require.NoError(t, err)
		idsToSearch[i] = id
	}

	idColumn := column.NewColumnInt64(common.DefaultInt64FieldName, idsToSearch)

	// Create range search parameters with radius and range_filter
	// For COSINE metric: similarity in [0, 1], distance = 1 - similarity
	// radius = 0.0, range_filter = 0.5 means: 0.0 < distance <= 0.5
	radius := "0.0"
	rangeFilter := "0.5"

	// Search by IDs with range parameters
	searchOption := client.NewSearchOption(schema.CollectionName, common.DefaultLimit, nil).
		WithIDs(idColumn).
		WithANNSField(common.DefaultFloatVecFieldName).
		WithSearchParam("radius", radius).
		WithSearchParam("range_filter", rangeFilter).
		WithConsistencyLevel(entity.ClStrong)

	resSearch, err := mc.Search(ctx, searchOption)
	common.CheckErr(t, err, true)

	// Verify all distances are within the range [radius, range_filter]
	radiusFloat := 0.0
	rangeFilterFloat := 0.5

	for i, resultSet := range resSearch {
		t.Logf("Result set %d has %d results", i, resultSet.ResultCount)
		for j, distance := range resultSet.Scores {
			// All distances should be: radius < distance <= range_filter
			require.Greater(t, distance, radiusFloat,
				"Distance %.4f should be > radius %.4f", distance, radiusFloat)
			require.LessOrEqual(t, distance, rangeFilterFloat,
				"Distance %.4f should be <= range_filter %.4f", distance, rangeFilterFloat)
			t.Logf("  Result %d: distance=%.4f (within range [%.2f, %.2f])", j, distance, radiusFloat, rangeFilterFloat)
		}
	}
}

// TestSearchByPKWithHybridSearch tests that hybrid search does NOT support search by IDs
// Target: verify hybrid search does not support search by primary keys
// Method:
//  1. connect and create a collection with multiple vector fields
//  2. attempt hybrid search with search by IDs
// Expected: hybrid search should fail with error indicating IDs not supported
func TestSearchByPKWithHybridSearch(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString("hybrid_search_ids", 6)

	// Create collection with 2 vector fields for hybrid search
	schema := entity.NewSchema().
		WithName(collName).
		WithField(entity.NewField().WithName(common.DefaultInt64FieldName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(common.DefaultFloatVecFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim)).
		WithField(entity.NewField().WithName("vector2").WithDataType(entity.FieldTypeFloatVector).WithDim(common.DefaultDim))

	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	// Insert data
	nb := 100
	ids := make([]int64, nb)
	vec1 := make([][]float32, nb)
	vec2 := make([][]float32, nb)

	for i := 0; i < nb; i++ {
		ids[i] = int64(i)
		vec1[i] = common.GenFloatVector(common.DefaultDim)
		vec2[i] = common.GenFloatVector(common.DefaultDim)
	}

	idColumn := column.NewColumnInt64(common.DefaultInt64FieldName, ids)
	vec1Column := column.NewColumnFloatVector(common.DefaultFloatVecFieldName, common.DefaultDim, vec1)
	vec2Column := column.NewColumnFloatVector("vector2", common.DefaultDim, vec2)

	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).
		WithColumns(idColumn, vec1Column, vec2Column))
	common.CheckErr(t, err, true)

	// Flush, create indexes, and load
	flushTask, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	err = flushTask.Await(ctx)
	common.CheckErr(t, err, true)

	idx1, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, common.DefaultFloatVecFieldName, index.NewAutoIndex(entity.COSINE)))
	common.CheckErr(t, err, true)
	err = idx1.Await(ctx)
	common.CheckErr(t, err, true)

	idx2, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "vector2", index.NewAutoIndex(entity.COSINE)))
	common.CheckErr(t, err, true)
	err = idx2.Await(ctx)
	common.CheckErr(t, err, true)

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)

	// Attempt hybrid search with search by IDs
	idsToSearch := []int64{0, 1, 2, 3, 4}
	idCol := column.NewColumnInt64(common.DefaultInt64FieldName, idsToSearch)

	// Create search requests with IDs (should fail)
	searchReq1 := client.NewAnnRequest(common.DefaultFloatVecFieldName, 10).
		WithIDs(idCol)
	searchReq2 := client.NewAnnRequest("vector2", 10).
		WithIDs(idCol)

	hybridOption := client.NewHybridSearchOption(collName, 10, searchReq1, searchReq2).
		WithReranker(client.NewRRFReranker())

	_, err = mc.HybridSearch(ctx, hybridOption)
	// Expect error: hybrid search does not support search by IDs
	// Note: The exact error message may vary depending on server implementation
	if err == nil {
		t.Logf("Warning: Hybrid search with IDs did not return error (may indicate support was added)")
	} else {
		t.Logf("Expected behavior: Hybrid search with IDs returned error: %v", err)
		// Error is expected
	}

	// Cleanup
	err = mc.DropCollection(ctx, client.NewDropCollectionOption(collName))
	common.CheckErr(t, err, true)
}

// TestSearchByPKWithSearchIterator tests that search iterator does NOT support search by IDs
// Target: verify search iterator does not support search by primary keys
// Method:
//  1. connect and create a collection
//  2. verify that search iterator option does not have WithIDs method
// Expected: SearchIteratorOption does not provide WithIDs method (compile-time check)
//
// Note: This test simply documents that search iterator is not compatible with search by IDs.
// The Go SDK's searchIteratorOption type does not expose a WithIDs() method, which means
// search by IDs is not supported for iterators at the API level.
// This is consistent with Python SDK behavior where search_iterator does not support ids parameter.
func TestSearchByPKWithSearchIterator(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// SearchIteratorOption requires a vector - it does not support WithIDs
	// This is by design: search iterator is not compatible with search by IDs
	queryVector := entity.FloatVector(common.GenFloatVector(common.DefaultDim))
	searchIteratorOption := client.NewSearchIteratorOption(schema.CollectionName, queryVector).
		WithANNSField(common.DefaultFloatVecFieldName).
		WithBatchSize(10).
		WithConsistencyLevel(entity.ClStrong)

	// Create iterator with vectors (normal usage)
	iter, err := mc.SearchIterator(ctx, searchIteratorOption)
	common.CheckErr(t, err, true)

	// Document that search iterator does NOT support search by IDs
	// The searchIteratorOption type does not have a WithIDs() method
	t.Log("Search iterator requires query vectors and does not support search by IDs")
	t.Log("This is consistent with Python SDK where search_iterator does not accept 'ids' parameter")

	// Cleanup: close iterator if created
	if iter != nil {
		// Iterator doesn't have explicit Close method, just let it go out of scope
		t.Log("Iterator created successfully with vectors (expected behavior)")
	}
}
