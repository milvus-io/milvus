package testcases

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v2/entity"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

const (
	defaultTimestamp = int64(1700000000)
)

func createRerankFunctionTestCollection(ctx context.Context, t *testing.T, mc *base.MilvusClient, enableText bool) (*hp.CollectionPrepare, *entity.Schema) {
	fields := hp.AllFields
	if enableText {
		fields = hp.FullTextSearch
	}

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(fields),
		hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true), hp.TWithConsistencyLevel(entity.ClStrong))

	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	return prepare, schema
}

func generateTestQueries() []string {
	return []string{
		"machine learning algorithms for time series forecasting",
		"deep neural networks and artificial intelligence",
		"data science and statistical analysis methods",
		"computer vision and image recognition",
		"natural language processing techniques",
	}
}

func validateRerankFunctionResults(t *testing.T, results []client.ResultSet, expectedLimit int) {
	require.Greater(t, len(results), 0, "Should have search results")
	for _, res := range results {
		require.LessOrEqual(t, res.ResultCount, expectedLimit, "Result count should not exceed limit")
		require.Equal(t, res.IDs.Len(), res.ResultCount, "IDs length should match result count")
		require.Equal(t, len(res.Scores), res.ResultCount, "Scores length should match result count")
	}
}

func TestRerankFunctionWeighted(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	_, schema := createRerankFunctionTestCollection(ctx, t, mc, false)

	queryVec1 := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	queryVec2 := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloat16Vector)

	testCases := []struct {
		name      string
		weights   []float64
		normScore bool
	}{
		{"equal_weights", []float64{0.5, 0.5}, true},
		{"prefer_first", []float64{0.8, 0.2}, true},
		{"prefer_second", []float64{0.3, 0.7}, true},
		{"no_normalization", []float64{0.6, 0.4}, false},
		{"sum_not_one", []float64{0.3, 0.4}, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			weightedReranker := entity.NewFunction().
				WithName("test_weighted_"+tc.name).
				WithType(entity.FunctionTypeRerank).
				WithInputFields().
				WithParam("reranker", "weighted").
				WithParam("weights", tc.weights).
				WithParam("norm_score", tc.normScore)

			annReq1 := client.NewAnnRequest(common.DefaultFloatVecFieldName, common.DefaultLimit, queryVec1...)
			annReq2 := client.NewAnnRequest(common.DefaultFloat16VecFieldName, common.DefaultLimit, queryVec2...)

			results, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(
				schema.CollectionName, common.DefaultLimit, annReq1, annReq2,
			).WithFunctionRerankers(weightedReranker).WithOutputFields("*"))

			common.CheckErr(t, err, true)
			validateRerankFunctionResults(t, results, common.DefaultLimit)
		})
	}
}

func TestRerankFunctionDecay(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	_, schema := createRerankFunctionTestCollection(ctx, t, mc, false)

	queryVec1 := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	queryVec2 := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloat16Vector)

	testCases := []struct {
		name     string
		function string
		origin   int64
		scale    int
		decay    float64
	}{
		{"linear_decay", "linear", defaultTimestamp + 3600, 3600, 0.1},
		{"exp_decay", "exp", defaultTimestamp + 7200, 7200, 0.2},
		{"gauss_decay", "gauss", defaultTimestamp + 1800, 1800, 0.15},
		{"large_scale", "linear", defaultTimestamp, 86400, 0.05},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			decayReranker := entity.NewFunction().
				WithName("test_decay_"+tc.name).
				WithType(entity.FunctionTypeRerank).
				WithInputFields(common.DefaultInt64FieldName).
				WithParam("reranker", "decay").
				WithParam("function", tc.function).
				WithParam("origin", tc.origin).
				WithParam("scale", tc.scale).
				WithParam("decay", tc.decay)

			annReq1 := client.NewAnnRequest(common.DefaultFloatVecFieldName, common.DefaultLimit, queryVec1...)
			annReq2 := client.NewAnnRequest(common.DefaultFloat16VecFieldName, common.DefaultLimit, queryVec2...)

			results, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(
				schema.CollectionName, common.DefaultLimit, annReq1, annReq2,
			).WithFunctionRerankers(decayReranker).WithOutputFields("*"))

			common.CheckErr(t, err, true)
			validateRerankFunctionResults(t, results, common.DefaultLimit)

			for _, res := range results {
				require.Greater(t, res.ResultCount, 0, "Should have results for decay reranker")
				timestampCol := res.GetColumn(common.DefaultInt64FieldName)
				require.NotNil(t, timestampCol, "Should have timestamp field in results")
			}
		})
	}
}

func TestRerankFunctionModel(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*3)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	_, schema := createRerankFunctionTestCollection(ctx, t, mc, true)

	queryVec1 := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim*2, entity.FieldTypeSparseVector)
	queryVec2 := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeSparseVector)

	queries := generateTestQueries()

	testCases := []struct {
		name     string
		provider string
		endpoint string
		queries  []string
	}{
		{"tei_provider", "tei", hp.GetTEIRerankerEndpoint(), queries[:common.DefaultNq]},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			modelReranker := entity.NewFunction().
				WithName("test_model_"+tc.name).
				WithType(entity.FunctionTypeRerank).
				WithInputFields(common.DefaultTextFieldName).
				WithParam("reranker", "model").
				WithParam("provider", tc.provider).
				WithParam("queries", tc.queries).
				WithParam("endpoint", tc.endpoint)

			annReq1 := client.NewAnnRequest(common.DefaultTextSparseVecFieldName, common.DefaultLimit, queryVec1...)
			annReq2 := client.NewAnnRequest(common.DefaultTextSparseVecFieldName, common.DefaultLimit, queryVec2...)

			results, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(
				schema.CollectionName, common.DefaultLimit, annReq1, annReq2,
			).WithFunctionRerankers(modelReranker).WithOutputFields("*"))

			common.CheckErr(t, err, true)
			validateRerankFunctionResults(t, results, common.DefaultLimit)

			for _, res := range results {
				textCol := res.GetColumn(common.DefaultTextFieldName)
				require.NotNil(t, textCol, "Should have text field in results")
			}
		})
	}
}

func TestRerankFunctionInvalidParams(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	_, schema := createRerankFunctionTestCollection(ctx, t, mc, false)

	queryVec1 := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloatVector)
	queryVec2 := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloat16Vector)

	annReq1 := client.NewAnnRequest(common.DefaultFloatVecFieldName, common.DefaultLimit, queryVec1...)
	annReq2 := client.NewAnnRequest(common.DefaultFloat16VecFieldName, common.DefaultLimit, queryVec2...)

	testCases := []struct {
		name          string
		function      *entity.Function
		expectedError string
	}{
		{
			"invalid_reranker_type",
			entity.NewFunction().
				WithName("invalid_type").
				WithType(entity.FunctionTypeRerank).
				WithInputFields().
				WithParam("reranker", "invalid_type"),
			"Unsupported rerank function",
		},
		{
			"weighted_invalid_weights",
			entity.NewFunction().
				WithName("invalid_weights").
				WithType(entity.FunctionTypeRerank).
				WithInputFields().
				WithParam("reranker", "weighted").
				WithParam("weights", "invalid_format"),
			"Parse weights param failed",
		},
		{
			"decay_missing_params",
			entity.NewFunction().
				WithName("missing_params").
				WithType(entity.FunctionTypeRerank).
				WithInputFields(common.DefaultInt64FieldName).
				WithParam("reranker", "decay"),
			"Decay function lost param",
		},
		{
			"model_missing_endpoint",
			entity.NewFunction().
				WithName("missing_endpoint").
				WithType(entity.FunctionTypeRerank).
				WithInputFields(common.DefaultVarcharFieldName).
				WithParam("reranker", "model").
				WithParam("provider", "tei"),
			"Rerank function lost params",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(
				schema.CollectionName, common.DefaultLimit, annReq1, annReq2,
			).WithFunctionRerankers(tc.function))

			common.CheckErr(t, err, false, tc.expectedError)
		})
	}
}

func TestRerankFunctionMissingFields(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	_, schema := createRerankFunctionTestCollection(ctx, t, mc, false)

	queryVec1 := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloatVector)
	queryVec2 := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloat16Vector)

	annReq1 := client.NewAnnRequest(common.DefaultFloatVecFieldName, common.DefaultLimit, queryVec1...)
	annReq2 := client.NewAnnRequest(common.DefaultFloat16VecFieldName, common.DefaultLimit, queryVec2...)

	testCases := []struct {
		name     string
		function *entity.Function
	}{
		{
			"decay_nonexistent_field",
			entity.NewFunction().
				WithName("nonexistent_field").
				WithType(entity.FunctionTypeRerank).
				WithInputFields("nonexistent_field").
				WithParam("reranker", "decay").
				WithParam("function", "linear").
				WithParam("origin", "1700000000").
				WithParam("scale", "3600").
				WithParam("decay", "0.1"),
		},
		{
			"model_nonexistent_field",
			entity.NewFunction().
				WithName("nonexistent_text").
				WithType(entity.FunctionTypeRerank).
				WithInputFields("nonexistent_text").
				WithParam("reranker", "model").
				WithParam("provider", "tei").
				WithParam("queries", []string{"test query"}).
				WithParam("endpoint", hp.GetTEIRerankerEndpoint()),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(
				schema.CollectionName, common.DefaultLimit, annReq1, annReq2,
			).WithFunctionRerankers(tc.function))

			common.CheckErr(t, err, false, "field not found", "nonexistent")
		})
	}
}

func TestRerankFunctionRRF(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	_, schema := createRerankFunctionTestCollection(ctx, t, mc, false)

	queryVec1 := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	queryVec2 := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloat16Vector)

	testCases := []struct {
		name string
		k    int
	}{
		{"default_k", 60},
		{"small_k", 10},
		{"large_k", 100},
		{"very_large_k", 1000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rrfReranker := entity.NewFunction().
				WithName("test_rrf_"+tc.name).
				WithType(entity.FunctionTypeRerank).
				WithInputFields().
				WithParam("reranker", "rrf").
				WithParam("k", tc.k)

			annReq1 := client.NewAnnRequest(common.DefaultFloatVecFieldName, common.DefaultLimit, queryVec1...)
			annReq2 := client.NewAnnRequest(common.DefaultFloat16VecFieldName, common.DefaultLimit, queryVec2...)

			results, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(
				schema.CollectionName, common.DefaultLimit, annReq1, annReq2,
			).WithFunctionRerankers(rrfReranker).WithOutputFields("*"))

			common.CheckErr(t, err, true)
			validateRerankFunctionResults(t, results, common.DefaultLimit)
		})
	}
}

func TestRerankFunctionDecaySingleVector(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	_, schema := createRerankFunctionTestCollection(ctx, t, mc, false)

	queryVec := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)

	testCases := []struct {
		name     string
		function string
		origin   int64
		scale    int
		decay    float64
	}{
		{"linear_decay_single", "linear", defaultTimestamp + 3600, 3600, 0.1},
		{"exp_decay_single", "exp", defaultTimestamp + 7200, 7200, 0.2},
		{"gauss_decay_single", "gauss", defaultTimestamp + 1800, 1800, 0.15},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			decayReranker := entity.NewFunction().
				WithName("test_decay_single_"+tc.name).
				WithType(entity.FunctionTypeRerank).
				WithInputFields(common.DefaultInt64FieldName).
				WithParam("reranker", "decay").
				WithParam("function", tc.function).
				WithParam("origin", tc.origin).
				WithParam("scale", tc.scale).
				WithParam("decay", tc.decay)

			results, err := mc.Search(ctx, client.NewSearchOption(
				schema.CollectionName, common.DefaultLimit, queryVec,
			).WithANNSField(common.DefaultFloatVecFieldName).
				WithFunctionReranker(decayReranker).
				WithOutputFields("*"))

			common.CheckErr(t, err, true)
			require.Greater(t, len(results), 0, "Should have search results")

			for _, res := range results {
				require.LessOrEqual(t, res.ResultCount, common.DefaultLimit, "Result count should not exceed limit")
				require.Greater(t, res.ResultCount, 0, "Should have results for decay reranker")
				timestampCol := res.GetColumn(common.DefaultInt64FieldName)
				require.NotNil(t, timestampCol, "Should have timestamp field in results")
			}
		})
	}
}

func TestRerankFunctionModelSingleVector(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*3)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	_, schema := createRerankFunctionTestCollection(ctx, t, mc, true)

	queryVec := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim*2, entity.FieldTypeSparseVector)
	queries := generateTestQueries()

	testCases := []struct {
		name     string
		provider string
		endpoint string
		queries  []string
	}{
		{"tei_provider_single", "tei", hp.GetTEIRerankerEndpoint(), queries[:common.DefaultNq]},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			modelReranker := entity.NewFunction().
				WithName("test_model_single_"+tc.name).
				WithType(entity.FunctionTypeRerank).
				WithInputFields(common.DefaultTextFieldName).
				WithParam("reranker", "model").
				WithParam("provider", tc.provider).
				WithParam("queries", tc.queries).
				WithParam("endpoint", tc.endpoint)

			results, err := mc.Search(ctx, client.NewSearchOption(
				schema.CollectionName, common.DefaultLimit, queryVec,
			).WithANNSField(common.DefaultTextSparseVecFieldName).
				WithFunctionReranker(modelReranker).
				WithOutputFields("*"))

			common.CheckErr(t, err, true)
			require.Greater(t, len(results), 0, "Should have search results")

			for _, res := range results {
				require.LessOrEqual(t, res.ResultCount, common.DefaultLimit, "Result count should not exceed limit")
				textCol := res.GetColumn(common.DefaultTextFieldName)
				require.NotNil(t, textCol, "Should have text field in results")
			}
		})
	}
}

func TestRerankFunctionEmptyResults(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	_, schema := createRerankFunctionTestCollection(ctx, t, mc, false)

	queryVec1 := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	queryVec2 := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloat16Vector)

	impossibleFilter := fmt.Sprintf("%s > %d", common.DefaultInt64FieldName, common.DefaultNb*10)

	weightedReranker := entity.NewFunction().
		WithName("test_empty_results").
		WithType(entity.FunctionTypeRerank).
		WithInputFields().
		WithParam("reranker", "weighted").
		WithParam("weights", []float64{0.5, 0.5}).
		WithParam("norm_score", true)

	annReq1 := client.NewAnnRequest(common.DefaultFloatVecFieldName, common.DefaultLimit, queryVec1...).WithFilter(impossibleFilter)
	annReq2 := client.NewAnnRequest(common.DefaultFloat16VecFieldName, common.DefaultLimit, queryVec2...).WithFilter(impossibleFilter)

	results, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(
		schema.CollectionName, common.DefaultLimit, annReq1, annReq2,
	).WithFunctionRerankers(weightedReranker))

	common.CheckErr(t, err, true)
	require.Len(t, results, common.DefaultNq)
	for _, res := range results {
		require.Equal(t, 0, res.ResultCount, "Should have no results with impossible filter")
	}
}

func TestRerankFunctionWeightedNegative(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	_, schema := createRerankFunctionTestCollection(ctx, t, mc, false)

	queryVec1 := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloatVector)
	queryVec2 := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloat16Vector)

	annReq1 := client.NewAnnRequest(common.DefaultFloatVecFieldName, common.DefaultLimit, queryVec1...)
	annReq2 := client.NewAnnRequest(common.DefaultFloat16VecFieldName, common.DefaultLimit, queryVec2...)

	testCases := []struct {
		name          string
		weights       interface{}
		normScore     bool
		expectedError string
	}{
		{"invalid_weights_format", "invalid_format", true, "Parse weights param failed"},
		{"empty_weights", []float64{}, true, "weights not found"},
		{"negative_weights", []float64{-0.5, 0.5}, true, "rank param weight should be in range [0, 1]"},
		{"mismatched_weights_count", []float64{0.3}, true, "the length of weights param mismatch with ann search requests"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			weightedReranker := entity.NewFunction().
				WithName("test_weighted_negative_"+tc.name).
				WithType(entity.FunctionTypeRerank).
				WithInputFields().
				WithParam("reranker", "weighted").
				WithParam("weights", tc.weights).
				WithParam("norm_score", tc.normScore)

			_, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(
				schema.CollectionName, common.DefaultLimit, annReq1, annReq2,
			).WithFunctionRerankers(weightedReranker))

			common.CheckErr(t, err, false, tc.expectedError)
		})
	}
}

func TestRerankFunctionRRFNegative(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	_, schema := createRerankFunctionTestCollection(ctx, t, mc, false)

	queryVec1 := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloatVector)
	queryVec2 := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloat16Vector)

	annReq1 := client.NewAnnRequest(common.DefaultFloatVecFieldName, common.DefaultLimit, queryVec1...)
	annReq2 := client.NewAnnRequest(common.DefaultFloat16VecFieldName, common.DefaultLimit, queryVec2...)

	testCases := []struct {
		name          string
		k             interface{}
		expectedError string
	}{
		{"negative_k", -10, "k should be in range (0, 16384)"},
		{"zero_k", 0, "k should be in range (0, 16384)"},
		{"too_large_k", 20000, "k should be in range (0, 16384)"},
		{"invalid_k_format", "invalid", "is not a number"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rrfReranker := entity.NewFunction().
				WithName("test_rrf_negative_"+tc.name).
				WithType(entity.FunctionTypeRerank).
				WithInputFields().
				WithParam("reranker", "rrf").
				WithParam("k", tc.k)

			_, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(
				schema.CollectionName, common.DefaultLimit, annReq1, annReq2,
			).WithFunctionRerankers(rrfReranker))

			common.CheckErr(t, err, false, tc.expectedError)
		})
	}
}

func TestRerankFunctionDecayNegative(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	_, schema := createRerankFunctionTestCollection(ctx, t, mc, false)

	queryVec1 := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloatVector)
	queryVec2 := hp.GenSearchVectors(1, common.DefaultDim, entity.FieldTypeFloat16Vector)

	annReq1 := client.NewAnnRequest(common.DefaultFloatVecFieldName, common.DefaultLimit, queryVec1...)
	annReq2 := client.NewAnnRequest(common.DefaultFloat16VecFieldName, common.DefaultLimit, queryVec2...)

	testCases := []struct {
		name          string
		function      interface{}
		origin        interface{}
		scale         interface{}
		decay         interface{}
		expectedError string
	}{
		{"invalid_function_type", "invalid", defaultTimestamp, 3600, 0.1, "Invaild decay function"},
		{"negative_scale", "linear", defaultTimestamp, -3600, 0.1, "scale must > 0"},
		{"invalid_origin_format", "linear", "invalid", 3600, 0.1, "is not a number"},
		{"invalid_decay_range", "linear", defaultTimestamp, 3600, 1.5, "decay must 0 < decay < 1"},
		{"zero_decay", "linear", defaultTimestamp, 3600, 0.0, "decay must 0 < decay < 1"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			decayReranker := entity.NewFunction().
				WithName("test_decay_negative_"+tc.name).
				WithType(entity.FunctionTypeRerank).
				WithInputFields(common.DefaultInt64FieldName).
				WithParam("reranker", "decay").
				WithParam("function", tc.function).
				WithParam("origin", tc.origin).
				WithParam("scale", tc.scale).
				WithParam("decay", tc.decay)

			_, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(
				schema.CollectionName, common.DefaultLimit, annReq1, annReq2,
			).WithFunctionRerankers(decayReranker))

			common.CheckErr(t, err, false, tc.expectedError)
		})
	}
}

func TestRerankFunctionModelNegative(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	_, schema := createRerankFunctionTestCollection(ctx, t, mc, true)

	queryVec1 := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim*2, entity.FieldTypeSparseVector)
	queryVec2 := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeSparseVector)

	queries := generateTestQueries()

	testCases := []struct {
		name          string
		provider      string
		endpoint      string
		queries       []string
		expectedError string
	}{
		{"invalid_endpoint", "tei", "http://invalid:8080", queries[:common.DefaultNq], "Call service failed"},
		{"empty_endpoint", "tei", "", queries[:common.DefaultNq], "is not a valid http/https link"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			modelReranker := entity.NewFunction().
				WithName("test_model_negative_"+tc.name).
				WithType(entity.FunctionTypeRerank).
				WithInputFields(common.DefaultTextFieldName).
				WithParam("reranker", "model").
				WithParam("provider", tc.provider).
				WithParam("queries", tc.queries).
				WithParam("endpoint", tc.endpoint)

			annReq1 := client.NewAnnRequest(common.DefaultTextSparseVecFieldName, common.DefaultLimit, queryVec1...)
			annReq2 := client.NewAnnRequest(common.DefaultTextSparseVecFieldName, common.DefaultLimit, queryVec2...)

			_, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(
				schema.CollectionName, common.DefaultLimit, annReq1, annReq2,
			).WithFunctionRerankers(modelReranker).WithOutputFields("*"))

			common.CheckErr(t, err, false, tc.expectedError)
		})
	}
}
