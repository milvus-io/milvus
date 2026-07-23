package testcases

import (
	"context"
	"fmt"
	"sync"
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

func TestHybridSearchDefault(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert [0, 3000) -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption(), hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// hybrid search
	queryVec1 := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	queryVec2 := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)

	annReq1 := client.NewAnnRequest(common.DefaultFloatVecFieldName, common.DefaultLimit, queryVec1...).WithSearchParam("ef", "100")
	annReq2 := client.NewAnnRequest(common.DefaultFloatVecFieldName, common.DefaultLimit, queryVec2...)

	searchRes, errSearch := mc.HybridSearch(ctx, client.NewHybridSearchOption(schema.CollectionName, common.DefaultLimit, annReq1, annReq2).WithOutputFields("*"))
	common.CheckErr(t, errSearch, true)
	common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
	common.CheckOutputFields(t, []string{common.DefaultInt64FieldName, common.DefaultFloatVecFieldName}, searchRes[0].Fields)

	// add field
	newField := entity.NewField().WithName(common.DefaultNewField).WithDataType(entity.FieldTypeInt64).WithNullable(true).WithDefaultValueLong(100)
	err := mc.AddCollectionField(ctx, client.NewAddCollectionFieldOption(schema.CollectionName, newField))
	common.CheckErr(t, err, true)
	annReq1.WithFilter(common.DefaultNewField + "== 100")
	annReq2.WithFilter(common.DefaultNewField + "== 100")
	searchRes, errSearch = mc.HybridSearch(ctx, client.NewHybridSearchOption(schema.CollectionName, common.DefaultLimit, annReq1, annReq2).WithOutputFields("*"))
	common.CheckErr(t, errSearch, true)
	common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
	common.CheckOutputFields(t, []string{common.DefaultInt64FieldName, common.DefaultFloatVecFieldName, common.DefaultNewField}, searchRes[0].Fields)

	// ignore growing
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithStart(common.DefaultNb).TWithNb(500))
	annReq1.WithIgnoreGrowing(true)
	annReq2.WithIgnoreGrowing(true)
	searchRes, errSearch = mc.HybridSearch(ctx, client.NewHybridSearchOption(schema.CollectionName, common.DefaultLimit, annReq1, annReq2))
	common.CheckErr(t, errSearch, true)
	common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
	for _, hits := range searchRes {
		for _, id := range hits.IDs.(*column.ColumnInt64).Data() {
			require.Less(t, id, int64(common.DefaultNb))
		}
	}
}

func TestHybridSearchTemplateParam(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert [0, 3000) -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64MultiVec), hp.TNewFieldsOption(), hp.TNewSchemaOption(), hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// hybrid search
	queryVec1 := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	queryVec2 := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloat16Vector)

	int64Value := 100
	annReq1 := client.NewAnnRequest(common.DefaultFloatVecFieldName, common.DefaultLimit, queryVec1...).
		WithFilter(fmt.Sprintf("%s > {int64Value}", common.DefaultInt64FieldName)).WithTemplateParam("int64Value", int64Value)
	annReq2 := client.NewAnnRequest(common.DefaultFloat16VecFieldName, common.DefaultLimit, queryVec2...).
		WithFilter(fmt.Sprintf("%s > {int64Value}", common.DefaultInt64FieldName)).WithTemplateParam("int64Value", 200)
	searchRes, errSearch := mc.HybridSearch(ctx, client.NewHybridSearchOption(schema.CollectionName, common.DefaultLimit, annReq1, annReq2))
	common.CheckErr(t, errSearch, true)
	common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
	for _, hits := range searchRes {
		for _, id := range hits.IDs.(*column.ColumnInt64).Data() {
			require.Greater(t, id, int64(int64Value))
		}
	}
}

func TestHybridSearchPartitionKeyIsolationUnsupportedFilter(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	const dim = 5
	collName := common.GenRandomString("pk_iso_hybrid", 6)
	schema := entity.NewSchema().
		WithName(collName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("vector_a").WithDataType(entity.FieldTypeFloatVector).WithDim(dim)).
		WithField(entity.NewField().WithName("vector_b").WithDataType(entity.FieldTypeFloatVector).WithDim(dim)).
		WithField(entity.NewField().WithName("tenant").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64).WithIsPartitionKey(true)).
		WithField(entity.NewField().WithName("color").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64))
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).
		WithNumPartitions(16).
		WithProperty("partitionkey.isolation", true).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	t.Cleanup(func() {
		_ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(collName))
	})

	vectorA := [][]float32{
		{0.10, 0.20, 0.30, 0.40, 0.50},
		{0.11, 0.21, 0.31, 0.41, 0.51},
		{0.90, 0.80, 0.70, 0.60, 0.50},
		{0.91, 0.81, 0.71, 0.61, 0.51},
		{0.10, 0.20, 0.30, 0.40, 0.50},
	}
	vectorB := [][]float32{
		{0.50, 0.40, 0.30, 0.20, 0.10},
		{0.51, 0.41, 0.31, 0.21, 0.11},
		{0.50, 0.60, 0.70, 0.80, 0.90},
		{0.51, 0.61, 0.71, 0.81, 0.91},
		{0.50, 0.40, 0.30, 0.20, 0.10},
	}
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).
		WithColumns(
			column.NewColumnInt64("id", []int64{1, 2, 3, 4, 5}),
			column.NewColumnFloatVector("vector_a", dim, vectorA),
			column.NewColumnFloatVector("vector_b", dim, vectorB),
			column.NewColumnVarChar("tenant", []string{"tenant_a", "tenant_a", "tenant_b", "tenant_b", "tenant_c"}),
			column.NewColumnVarChar("color", []string{"tenant_a_1", "tenant_a_2", "tenant_b_1", "tenant_b_2", "tenant_c_control"}),
		))
	common.CheckErr(t, err, true)

	flushTask, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, flushTask.Await(ctx), true)

	for _, fieldName := range []string{"vector_a", "vector_b"} {
		idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, fieldName, index.NewAutoIndex(entity.COSINE)))
		common.CheckErr(t, err, true)
		common.CheckErr(t, idxTask.Await(ctx), true)
	}

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, loadTask.Await(ctx), true)

	queryA := []entity.Vector{entity.FloatVector([]float32{0.10, 0.20, 0.30, 0.40, 0.50})}
	queryB := []entity.Vector{entity.FloatVector([]float32{0.50, 0.40, 0.30, 0.20, 0.10})}

	searchRes, err := mc.Search(ctx, client.NewSearchOption(collName, 5, queryA).
		WithANNSField("vector_a").
		WithSearchParam("metric_type", "COSINE").
		WithFilter(`tenant == "tenant_a"`).
		WithOutputFields("id", "tenant", "color").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Len(t, searchRes, 1)
	tenants := searchRes[0].GetColumn("tenant").(*column.ColumnVarChar).Data()
	require.NotEmpty(t, tenants)
	require.Subset(t, []string{"tenant_a"}, tenants)

	invalidFilters := map[string]string{
		`tenant in ["tenant_a", "tenant_b"]`: "partition key isolation does not support IN",
		"":                                   "partition key not found in expr",
	}
	for expr, errMsg := range invalidFilters {
		_, err := mc.Search(ctx, client.NewSearchOption(collName, 5, queryA).
			WithANNSField("vector_a").
			WithSearchParam("metric_type", "COSINE").
			WithFilter(expr).
			WithOutputFields("id", "tenant", "color").
			WithConsistencyLevel(entity.ClStrong))
		require.ErrorContains(t, err, errMsg)
	}

	for expr, errMsg := range invalidFilters {
		annReq1 := client.NewAnnRequest("vector_a", 5, queryA...).
			WithSearchParam("metric_type", "COSINE").
			WithFilter(expr)
		annReq2 := client.NewAnnRequest("vector_b", 5, queryB...).
			WithSearchParam("metric_type", "COSINE").
			WithFilter(expr)
		_, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(collName, 5, annReq1, annReq2).
			WithReranker(client.NewRRFReranker()).
			WithOutputFields("id", "tenant", "color").
			WithConsistencyLevel(entity.ClStrong))
		if err == nil {
			t.Skipf("xfail: https://github.com/milvus-io/milvus/issues/50398, hybrid_search accepted unsupported filter %q", expr)
		}
		require.ErrorContains(t, err, errMsg)
	}
}

// hybrid search default -> verify success
func TestHybridSearchMultiVectorsDefault(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	for _, enableDynamic := range []bool{false, true} {
		// create -> insert [0, 3000) -> flush -> index -> load
		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.AllFields),
			hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(enableDynamic), hp.TWithConsistencyLevel(entity.ClStrong))
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb*3))
		prepare.FlushData(ctx, t, mc, schema.CollectionName)
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

		// hybrid search with different limit
		type limitGroup struct {
			subLimit1 int
			subLimit2 int
			limit     int
		}
		// Duplicates when aggregating multiple subquery results
		limits := []limitGroup{
			{subLimit1: 10, subLimit2: 5, limit: 8},  // actual limit 8
			{subLimit1: 10, subLimit2: 5, limit: 15}, // actual limit [10, 15]
			{subLimit1: 10, subLimit2: 5, limit: 20}, // actual limit [10, 15]
		}

		expr := fmt.Sprintf("%s > 5", common.DefaultInt64FieldName)
		var allFieldsName []string
		for _, field := range schema.Fields {
			allFieldsName = append(allFieldsName, field.Name)
		}
		if enableDynamic {
			allFieldsName = append(allFieldsName, common.DefaultDynamicFieldName)
		}

		ch := make(chan struct{}, 3)
		wg := sync.WaitGroup{}
		testFunc := func(reranker client.Reranker) {
			defer func() {
				wg.Done()
				<-ch
			}()
			queryVec1 := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
			queryVec2 := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloat16Vector)

			for _, limit := range limits {
				// hybrid search
				annReq1 := client.NewAnnRequest(common.DefaultFloatVecFieldName, limit.subLimit1, queryVec1...).WithFilter(expr)
				annReq2 := client.NewAnnRequest(common.DefaultFloat16VecFieldName, limit.subLimit2, queryVec2...).WithFilter(expr)

				searchRes, errSearch := mc.HybridSearch(ctx, client.NewHybridSearchOption(schema.CollectionName, limit.limit, annReq1, annReq2).
					WithReranker(reranker).WithOutputFields("*"))
				common.CheckErr(t, errSearch, true)
				actualLimitRange := make([]int, 2)
				actualLimitRange[0] = min(max(limit.subLimit1, limit.subLimit2), limit.limit)
				actualLimitRange[1] = min(limit.subLimit1+limit.subLimit2, limit.limit)
				require.Len(t, searchRes, common.DefaultNq)
				common.CheckOutputFields(t, allFieldsName, searchRes[0].Fields)
				for _, res := range searchRes {
					require.GreaterOrEqual(t, res.ResultCount, actualLimitRange[0])
					require.LessOrEqual(t, res.ResultCount, actualLimitRange[1])
					require.GreaterOrEqual(t, searchRes[0].IDs.Len(), actualLimitRange[0])
				}
			}
		}

		// search with different reranker
		for _, reranker := range []client.Reranker{
			client.NewRRFReranker(),
			client.NewWeightedReranker([]float64{0.8, 0.2}),
			client.NewWeightedReranker([]float64{0.0, 0.2}),
			client.NewWeightedReranker([]float64{0.4, 1.0}),
		} {
			reranker := reranker
			ch <- struct{}{}
			wg.Add(1)
			go testFunc(reranker)
		}
		wg.Wait()
	}
}

// invalid limit: 0, -1, max+1
// invalid WeightedReranker params
// invalid fieldName: not exist
// invalid metric type: mismatch
func TestHybridSearchInvalidParams(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64MultiVec), hp.TNewFieldsOption(), hp.TNewSchemaOption(), hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// hybrid search with invalid limit

	queryVec1 := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloatVector)
	queryVec2 := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeBinaryVector)

	annReq1 := client.NewAnnRequest(common.DefaultFloatVecFieldName, common.DefaultLimit, queryVec1...)
	annReq2 := client.NewAnnRequest(common.DefaultBinaryVecFieldName, common.DefaultLimit, queryVec2...)

	for _, invalidLimit := range []int{-1, 0, common.MaxTopK + 1} {
		// hybrid search with invalid limit
		_, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(schema.CollectionName, invalidLimit, annReq1))
		common.CheckErr(t, err, false, "should be greater than 0", "should be in range [1, 16384]")

		// annRequest with invalid limit
		annReq2 := client.NewAnnRequest(common.DefaultFloatVecFieldName, invalidLimit, queryVec1...)
		_, err = mc.HybridSearch(ctx, client.NewHybridSearchOption(schema.CollectionName, invalidLimit, annReq2))
		common.CheckErr(t, err, false, "should be greater than 0", "should be in range [1, 16384]")
	}

	// hybrid search with invalid WeightedReranker params
	for _, invalidRanker := range []client.Reranker{
		client.NewWeightedReranker([]float64{-1, 0.2}),
		client.NewWeightedReranker([]float64{1.2, 0.2}),
		client.NewWeightedReranker([]float64{0.2}),
		client.NewWeightedReranker([]float64{0.2, 0.7, 0.5}),
	} {
		_, errReranker := mc.HybridSearch(ctx, client.NewHybridSearchOption(schema.CollectionName, common.DefaultLimit, annReq1, annReq2).WithReranker(invalidRanker))
		common.CheckErr(t, errReranker, false, "rank param weight should be in range [0, 1]",
			"the length of weights param mismatch with ann search requests")
	}

	// invalid fieldName: not exist
	annReq3 := client.NewAnnRequest("a", common.DefaultLimit, queryVec1...)
	_, errField := mc.HybridSearch(ctx, client.NewHybridSearchOption(schema.CollectionName, common.DefaultLimit, annReq3))
	common.CheckErr(t, errField, false, "failed to get field schema by name: fieldName(a) not found")

	// invalid metric type: mismatch
	annReq4 := client.NewAnnRequest(common.DefaultFloatVecFieldName, common.DefaultLimit, queryVec1...).WithSearchParam("metric_type", "L2")
	_, errMetric := mc.HybridSearch(ctx, client.NewHybridSearchOption(schema.CollectionName, common.DefaultLimit, annReq4))
	common.CheckErr(t, errMetric, false, "metric type not match: invalid parameter")
}

// vector type mismatch: vectors: float32, queryVec: binary
// vector dim mismatch
func TestHybridSearchInvalidVectors(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption(), hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(500))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// vector dim or type mismatch
	for _, invalidVec := range [][]entity.Vector{
		hp.GenSearchVectors(2, common.DefaultDim*2, entity.FieldTypeFloatVector), // vector dim mismatch
		hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloat16Vector), // vector type mismatch
	} {
		annReq := client.NewAnnRequest(common.DefaultFloatVecFieldName, common.DefaultLimit, invalidVec...)
		_, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(schema.CollectionName, common.DefaultLimit, annReq))
		common.CheckErr(t, err, false, "vector dimension mismatch", "vector type must be the same")
	}
}

// hybrid search Pagination -> verify success
func TestHybridSearchMultiVectorsPagination(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64MultiVec), hp.TNewFieldsOption(), hp.TNewSchemaOption(), hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb*5))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// hybrid search with different offset

	queryVec1 := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	queryVec2 := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloat16Vector)
	annReqDef := client.NewAnnRequest(common.DefaultFloatVecFieldName, common.DefaultLimit, queryVec1...)

	// offset 0, -1 -> 0
	for _, offset := range []int{0, -1} {
		var searchRes []client.ResultSet
		err := common.RetryOnTSafeStalled(ctx, func() error {
			var err error
			searchRes, err = mc.HybridSearch(ctx, client.NewHybridSearchOption(schema.CollectionName, common.DefaultLimit, annReqDef).WithOffset(offset).WithConsistencyLevel(entity.ClStrong))
			return err
		})
		common.CheckErr(t, err, true)
		common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
	}

	// check for invalid offset externally, not internally
	annReqOffset := client.NewAnnRequest(common.DefaultFloat16VecFieldName, common.DefaultLimit, queryVec2...).WithOffset(common.MaxTopK + 1)
	res, errSearch := mc.HybridSearch(ctx, client.NewHybridSearchOption(schema.CollectionName, common.DefaultLimit, annReqDef, annReqOffset))
	common.CheckErr(t, errSearch, true)
	common.CheckSearchResult(t, res, common.DefaultNq, common.DefaultLimit)

	_, errSearch = mc.HybridSearch(ctx, client.NewHybridSearchOption(schema.CollectionName, common.DefaultLimit, annReqDef, annReqOffset).WithOffset(common.MaxTopK+1))
	common.CheckErr(t, errSearch, false, "should be gte than 0", "(offset+limit) should be in range [1, 16384]")

	// search with different reranker and offset
	for _, reranker := range []client.Reranker{
		client.NewRRFReranker(),
		client.NewWeightedReranker([]float64{0.8, 0.2}),
		client.NewWeightedReranker([]float64{0.0, 0.2}),
		client.NewWeightedReranker([]float64{0.4, 1.0}),
	} {
		annReq1 := client.NewAnnRequest(common.DefaultFloatVecFieldName, common.DefaultLimit, queryVec1...)
		annReq2 := client.NewAnnRequest(common.DefaultFloat16VecFieldName, common.DefaultLimit, queryVec2...)
		// hybrid search
		searchRes, errSearch := mc.HybridSearch(ctx, client.NewHybridSearchOption(schema.CollectionName, common.DefaultLimit, annReq1, annReq2).WithReranker(reranker))
		common.CheckErr(t, errSearch, true)

		offsetRes, errSearch := mc.HybridSearch(ctx, client.NewHybridSearchOption(schema.CollectionName, 5, annReq1, annReq2).WithReranker(reranker).WithOffset(5))
		common.CheckErr(t, errSearch, true)
		common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
		common.CheckSearchResult(t, offsetRes, common.DefaultNq, 5)
		for i := 0; i < len(searchRes); i++ {
			require.Equal(t, searchRes[i].IDs.(*column.ColumnInt64).Data()[5:], offsetRes[i].IDs.(*column.ColumnInt64).Data())
		}
	}
}

// hybrid search Pagination -> verify success
func TestHybridSearchMultiVectorsRangeSearch(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create -> insert [0, 3000) -> flush -> index -> load
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64MultiVec), hp.TNewFieldsOption(), hp.TNewSchemaOption(), hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb*3))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// hybrid search
	expr := fmt.Sprintf("%s > 4", common.DefaultInt64FieldName)
	queryVec1 := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloatVector)
	queryVec2 := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloat16Vector)

	// search with different reranker and offset
	for _, reranker := range []client.Reranker{
		client.NewRRFReranker(),
		client.NewWeightedReranker([]float64{0.8, 0.2}),
		client.NewWeightedReranker([]float64{0.5, 0.5}),
	} {
		annReq1 := client.NewAnnRequest(common.DefaultFloatVecFieldName, common.DefaultLimit*2, queryVec1...).WithSearchParam("radius", "20").WithOffset(1).WithFilter(expr)
		annReq2 := client.NewAnnRequest(common.DefaultFloat16VecFieldName, common.DefaultLimit, queryVec2...).WithSearchParam("range_filter", "0.01").WithFilter(expr)
		// hybrid search
		resRange, errSearch := mc.HybridSearch(ctx, client.NewHybridSearchOption(schema.CollectionName, common.DefaultLimit, annReq1, annReq2).WithReranker(reranker))
		common.CheckErr(t, errSearch, true)
		common.CheckSearchResult(t, resRange, 1, common.DefaultLimit)
		for _, res := range resRange {
			for _, score := range res.Scores {
				require.GreaterOrEqual(t, score, float32(0.01))
				require.LessOrEqual(t, score, float32(20))
			}
		}
	}
}

func TestHybridSearchSparseVector(t *testing.T) {
	t.Parallel()
	idxInverted := index.NewSparseInvertedIndex(entity.IP, 0.2)
	idxWand := index.NewSparseWANDIndex(entity.IP, 0.3)

	for _, idx := range []index.Index{idxInverted, idxWand} {
		ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
		mc := hp.CreateDefaultMilvusClient(ctx, t)

		// create -> insert [0, 3000) -> flush -> index -> load
		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), hp.TNewFieldsOption(),
			hp.TNewSchemaOption().TWithEnableDynamicField(true), hp.TWithConsistencyLevel(entity.ClStrong))
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultSparseVecFieldName: idx}))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb*3))
		prepare.FlushData(ctx, t, mc, schema.CollectionName)

		// search
		queryVec1 := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim*2, entity.FieldTypeSparseVector)
		queryVec2 := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeSparseVector)

		expr := fmt.Sprintf("%s > 1", common.DefaultInt64FieldName)
		for _, reranker := range []client.Reranker{
			client.NewRRFReranker(),
			client.NewWeightedReranker([]float64{0.5, 0.6}),
		} {
			// hybrid search
			annReq1 := client.NewAnnRequest(common.DefaultSparseVecFieldName, common.DefaultLimit, queryVec1...).WithFilter(expr)
			annReq2 := client.NewAnnRequest(common.DefaultSparseVecFieldName, common.DefaultLimit, queryVec2...)

			searchRes, errSearch := mc.HybridSearch(ctx, client.NewHybridSearchOption(schema.CollectionName, common.DefaultLimit, annReq1, annReq2).
				WithReranker(reranker).WithOutputFields("*"))
			common.CheckErr(t, errSearch, true)
			common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
			common.CheckErr(t, errSearch, true)
			outputFields := []string{common.DefaultInt64FieldName, common.DefaultVarcharFieldName, common.DefaultSparseVecFieldName, common.DefaultDynamicFieldName}
			common.CheckOutputFields(t, outputFields, searchRes[0].Fields)
		}
	}
}

func TestHybridSearchGroupBy(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// create collection
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.AllFields), hp.TNewFieldsOption(), hp.TNewSchemaOption(), hp.TWithConsistencyLevel(entity.ClStrong))
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	ch := make(chan struct{}, 5)
	wg := sync.WaitGroup{}

	testFunc := func() {
		defer func() {
			wg.Done()
			<-ch
		}()
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(1000))
	}

	for i := 0; i < 10; i++ {
		ch <- struct{}{}
		wg.Add(1)
		go testFunc()
	}
	wg.Wait()

	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// hybrid search with groupby field

	queryVec1 := hp.GenSearchVectors(2, common.DefaultDim, entity.FieldTypeFloatVector)
	queryVec2 := hp.GenSearchVectors(2, common.DefaultDim, entity.FieldTypeBFloat16Vector)
	annReq1 := client.NewAnnRequest(common.DefaultFloatVecFieldName, common.DefaultLimit, queryVec1...).WithGroupByField(common.DefaultVarcharFieldName)
	annReq2 := client.NewAnnRequest(common.DefaultBFloat16VecFieldName, common.DefaultLimit, queryVec2...).WithGroupByField(common.DefaultInt32FieldName)

	res, errSearch := mc.HybridSearch(ctx, client.NewHybridSearchOption(schema.CollectionName, common.DefaultLimit, annReq1, annReq2))
	common.CheckErr(t, errSearch, true)
	common.CheckSearchResult(t, res, 2, common.DefaultLimit)

	// TODO hybrid search WithGroupSize, WithStrictGroupSize
}
