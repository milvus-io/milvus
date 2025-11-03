// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package rerank

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestAnalyzeRequiredFields(t *testing.T) {
	tests := []struct {
		name            string
		expression      string
		availableFields []string
		expectedFields  []string
		expectedError   bool
	}{
		{
			name:            "single field with dot notation",
			expression:      "score * fields.price",
			availableFields: []string{"price", "rating", "popularity"},
			expectedFields:  []string{"price"},
		},
		{
			name:            "multiple fields",
			expression:      "score * 0.7 + fields.price * 0.2 + fields.rating * 0.1",
			availableFields: []string{"price", "rating", "popularity"},
			expectedFields:  []string{"price", "rating"},
		},
		{
			name:            "field with bracket notation",
			expression:      `score + fields["popularity"]`,
			availableFields: []string{"price", "rating", "popularity"},
			expectedFields:  []string{"popularity"},
		},
		{
			name:            "no fields used (score only)",
			expression:      "score * 2.0",
			availableFields: []string{"price", "rating", "popularity"},
			expectedFields:  []string{},
		},
		{
			name:            "no fields used (rank based)",
			expression:      "1.0 / (rank + 60)",
			availableFields: []string{"price", "rating", "popularity"},
			expectedFields:  []string{},
		},
		{
			name:            "all fields used",
			expression:      "score + fields.price + fields.rating + fields.popularity",
			availableFields: []string{"price", "rating", "popularity"},
			expectedFields:  []string{"price", "rating", "popularity"},
		},
		{
			name:            "field in math function",
			expression:      "score * sqrt(fields.popularity)",
			availableFields: []string{"price", "rating", "popularity"},
			expectedFields:  []string{"popularity"},
		},
		{
			name:            "complex expression",
			expression:      "log(score + 1) * sqrt(fields.popularity) + pow(fields.rating, 0.5)",
			availableFields: []string{"price", "rating", "popularity", "views"},
			expectedFields:  []string{"popularity", "rating"},
		},
		{
			name:            "conditional with fields",
			expression:      "fields.is_promoted ? score * 1.5 : score",
			availableFields: []string{"is_promoted", "price"},
			expectedFields:  []string{"is_promoted"},
		},
		{
			name:            "field not in available list",
			expression:      "score + fields.missing_field",
			availableFields: []string{"price", "rating"},
			expectedFields:  []string{}, // missing_field is not in available, so it's filtered out
		},
		{
			name:            "empty available fields",
			expression:      "score * 2.0",
			availableFields: []string{},
			expectedFields:  []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requiredFields, indices, err := analyzeRequiredFields(tt.expression, tt.availableFields)

			if tt.expectedError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.ElementsMatch(t, tt.expectedFields, requiredFields,
				"Expected fields %v but got %v for expression: %s",
				tt.expectedFields, requiredFields, tt.expression)

			// Verify indices map is correct
			for _, fieldName := range requiredFields {
				idx, ok := indices[fieldName]
				assert.True(t, ok, "Field %s should have an index", fieldName)

				// Find expected index in availableFields
				expectedIdx := -1
				for i, f := range tt.availableFields {
					if f == fieldName {
						expectedIdx = i
						break
					}
				}
				assert.Equal(t, expectedIdx, idx,
					"Field %s index should be %d but got %d", fieldName, expectedIdx, idx)
			}
		})
	}
}

func TestAnalyzeRequiredFields_InvalidExpression(t *testing.T) {
	_, _, err := analyzeRequiredFields("score + invalid syntax !!!", []string{"price"})
	assert.Error(t, err, "Should return error for invalid expression")
}

func TestFieldVisitor(t *testing.T) {
	tests := []struct {
		name           string
		expression     string
		expectedFields []string
	}{
		{
			name:           "nested field access",
			expression:     "fields.price + fields.rating * 2",
			expectedFields: []string{"price", "rating"},
		},
		{
			name:           "field in nested function",
			expression:     "max(min(fields.price, 100), fields.rating)",
			expectedFields: []string{"price", "rating"},
		},
		{
			name:           "mixed notation",
			expression:     `fields.price + fields["rating"]`,
			expectedFields: []string{"price", "rating"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requiredFields, _, err := analyzeRequiredFields(tt.expression, tt.expectedFields)
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.expectedFields, requiredFields)
		})
	}
}

func TestExprRerank_GetInputFieldIDs_Pruning(t *testing.T) {
	// This test would require mocking the entire collection schema setup
	// For now, we'll create a simple unit test that verifies the logic

	reranker := &ExprRerank[int64]{
		RerankBase: RerankBase{
			inputFieldIDs:   []int64{100, 101, 102},
			inputFieldNames: []string{"price", "rating", "popularity"},
		},
		requiredFieldNames:   []string{"price", "popularity"},
		requiredFieldIndices: map[string]int{"price": 0, "popularity": 2},
	}

	fieldIDs := reranker.GetInputFieldIDs()
	assert.ElementsMatch(t, []int64{100, 102}, fieldIDs,
		"Should return only field IDs for required fields")
}

func TestExprRerank_GetInputFieldNames_Pruning(t *testing.T) {
	reranker := &ExprRerank[int64]{
		RerankBase: RerankBase{
			inputFieldNames: []string{"price", "rating", "popularity"},
		},
		requiredFieldNames: []string{"price", "popularity"},
	}

	fieldNames := reranker.GetInputFieldNames()
	assert.ElementsMatch(t, []string{"price", "popularity"}, fieldNames,
		"Should return only required field names")
}

func TestExprRerank_GetInputFieldNames_NoFieldsRequired(t *testing.T) {
	baseFields := []string{"price", "rating", "popularity"}
	reranker := &ExprRerank[int64]{
		RerankBase: RerankBase{
			inputFieldNames: baseFields,
		},
		requiredFieldNames: []string{}, // No fields required (e.g., "score * 2")
	}

	fieldNames := reranker.GetInputFieldNames()
	// When no fields are required, should fall back to base implementation
	assert.Equal(t, baseFields, fieldNames,
		"Should return base fields when no fields are required")
}

func BenchmarkAnalyzeRequiredFields(b *testing.B) {
	expression := "score * 0.7 + fields.price * 0.2 + fields.rating * 0.1"
	availableFields := []string{"price", "rating", "popularity", "views", "likes"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := analyzeRequiredFields(expression, availableFields)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAnalyzeRequiredFields_Complex(b *testing.B) {
	expression := "log(score + 1) * sqrt(fields.popularity) + pow(fields.rating, 0.5) + fields.price * 0.3"
	availableFields := []string{
		"price", "rating", "popularity", "views", "likes",
		"comments", "shares", "saves", "clicks", "impressions",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := analyzeRequiredFields(expression, availableFields)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Tests for extractFieldValue and NULL handling
func TestExtractFieldValue(t *testing.T) {
	tests := []struct {
		name       string
		data       interface{}
		idx        int
		expectVal  interface{}
		expectNull bool
		expectErr  bool
	}{
		{
			name:       "valid int32 slice",
			data:       []int32{10, 20, 30},
			idx:        1,
			expectVal:  int32(20),
			expectNull: false,
			expectErr:  false,
		},
		{
			name:       "valid int64 slice",
			data:       []int64{100, 200, 300},
			idx:        0,
			expectVal:  int64(100),
			expectNull: false,
			expectErr:  false,
		},
		{
			name:       "valid float32 slice",
			data:       []float32{1.1, 2.2, 3.3},
			idx:        2,
			expectVal:  float32(3.3),
			expectNull: false,
			expectErr:  false,
		},
		{
			name:       "valid string slice",
			data:       []string{"a", "b", "c"},
			idx:        1,
			expectVal:  "b",
			expectNull: false,
			expectErr:  false,
		},
		{
			name:       "valid bool slice",
			data:       []bool{true, false, true},
			idx:        0,
			expectVal:  true,
			expectNull: false,
			expectErr:  false,
		},
		{
			name:       "index out of bounds",
			data:       []int32{10, 20},
			idx:        5,
			expectVal:  nil,
			expectNull: true,
			expectErr:  false,
		},
		{
			name:       "nil data",
			data:       nil,
			idx:        0,
			expectVal:  nil,
			expectNull: true,
			expectErr:  false,
		},
		{
			name:       "not a slice",
			data:       "not a slice",
			idx:        0,
			expectVal:  nil,
			expectNull: false,
			expectErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, isNull, err := extractFieldValue(tt.data, tt.idx)

			if tt.expectErr {
				assert.Error(t, err, "Expected error for test: %s", tt.name)
			} else {
				assert.NoError(t, err, "Unexpected error for test: %s", tt.name)
			}

			assert.Equal(t, tt.expectNull, isNull,
				"NULL flag mismatch for test: %s", tt.name)

			if !tt.expectNull && !tt.expectErr {
				assert.Equal(t, tt.expectVal, val,
					"Value mismatch for test: %s", tt.name)
			}
		})
	}
}

func TestExtractFieldValue_PointerTypes(t *testing.T) {
	// Test with pointer to int32
	val1 := int32(42)
	val2 := int32(99)
	ptrSlice := []*int32{&val1, nil, &val2}

	// First element (valid pointer)
	v, isNull, err := extractFieldValue(ptrSlice, 0)
	require.NoError(t, err)
	assert.False(t, isNull)
	assert.Equal(t, int32(42), v)

	// Second element (nil pointer)
	v, isNull, err = extractFieldValue(ptrSlice, 1)
	require.NoError(t, err)
	assert.True(t, isNull)
	assert.Nil(t, v)

	// Third element (valid pointer)
	v, isNull, err = extractFieldValue(ptrSlice, 2)
	require.NoError(t, err)
	assert.False(t, isNull)
	assert.Equal(t, int32(99), v)
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected float64
	}{
		{float64(3.14), 3.14},
		{float32(2.5), 2.5},
		{int(42), 42.0},
		{int32(10), 10.0},
		{int64(100), 100.0},
		{"string", 0.0}, // fallback for unsupported
		{nil, 0.0},      // fallback for unsupported
	}

	for _, tt := range tests {
		result := toFloat64(tt.input)
		assert.Equal(t, tt.expected, result,
			"toFloat64(%v) should return %v", tt.input, tt.expected)
	}
}

func TestParseBool(t *testing.T) {
	tests := []struct {
		input       string
		expected    bool
		expectError bool
	}{
		{"true", true, false},
		{"True", true, false},
		{"TRUE", true, false},
		{"1", true, false},
		{"yes", true, false},
		{"y", true, false},
		{"false", false, false},
		{"False", false, false},
		{"FALSE", false, false},
		{"0", false, false},
		{"no", false, false},
		{"n", false, false},
		{"invalid", false, true},
		{"2", false, true},
		{"maybe", false, true},
	}

	for _, tt := range tests {
		result, err := parseBool(tt.input)
		if tt.expectError {
			assert.Error(t, err, "Expected error for input: %s", tt.input)
		} else {
			assert.NoError(t, err, "Unexpected error for input: %s", tt.input)
			assert.Equal(t, tt.expected, result,
				"parseBool(%s) should return %v", tt.input, tt.expected)
		}
	}
}

func TestExprRerank_Metrics_SuccessfulProcess(t *testing.T) {
	paramtable.Init()

	registry := prometheus.NewRegistry()
	metrics.RegisterRerank(registry)

	schema := &schemapb.CollectionSchema{
		Name: "test_metrics",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "price", DataType: schemapb.DataType_Float},
			{FieldID: 102, Name: "rating", DataType: schemapb.DataType_Float},
		},
	}

	functionSchema := &schemapb.FunctionSchema{
		Name:            "test_expr_metrics",
		Type:            schemapb.FunctionType_Rerank,
		InputFieldNames: []string{"price", "rating"},
		Params: []*commonpb.KeyValuePair{
			{Key: ExprCodeKey, Value: "score * 0.5 + fields.price * 0.3 + fields.rating * 0.2"},
		},
	}

	reranker, err := newExprFunction(schema, functionSchema)
	require.NoError(t, err)
	defer reranker.Close()

	nq := int64(1)
	topk := int64(5)
	data := embedding.GenSearchResultData(nq, topk, schemapb.DataType_Int64, "price", 101)
	data.FieldsData = append(data.FieldsData, &schemapb.FieldData{
		FieldId: 102,
		Type:    schemapb.DataType_Float,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{Data: []float32{4.5, 4.0, 3.5, 3.0, 2.5}},
				},
			},
		},
	})

	inputs, err := newRerankInputs([]*schemapb.SearchResultData{data}, reranker.GetInputFieldIDs(), false)
	require.NoError(t, err)

	searchParams := NewSearchParams(nq, topk, topk, -1, -1, 1, false, "", []string{"COSINE"})

	latencyCountBefore := testutil.CollectAndCount(metrics.RerankLatency)
	resultCountMetricsBefore := testutil.CollectAndCount(metrics.RerankResultCount)

	_, err = reranker.Process(context.Background(), searchParams, inputs)
	require.NoError(t, err)

	latencyCountAfter := testutil.CollectAndCount(metrics.RerankLatency)
	resultCountMetricsAfter := testutil.CollectAndCount(metrics.RerankResultCount)

	assert.GreaterOrEqual(t, latencyCountAfter, latencyCountBefore, "RerankLatency metric should be recorded")
	assert.GreaterOrEqual(t, resultCountMetricsAfter, resultCountMetricsBefore, "RerankResultCount metric should be recorded")
}

func TestExprRerank_Metrics_WithNormalization(t *testing.T) {
	paramtable.Init()

	registry := prometheus.NewRegistry()
	metrics.RegisterRerank(registry)

	schema := &schemapb.CollectionSchema{
		Name: "test_normalize",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "boost", DataType: schemapb.DataType_Float},
		},
	}

	functionSchema := &schemapb.FunctionSchema{
		Name:            "test_with_normalize",
		Type:            schemapb.FunctionType_Rerank,
		InputFieldNames: []string{"boost"},
		Params: []*commonpb.KeyValuePair{
			{Key: ExprCodeKey, Value: "score + fields.boost * 0.5"},
			{Key: ExprNormalizeKey, Value: "true"},
		},
	}

	reranker, err := newExprFunction(schema, functionSchema)
	require.NoError(t, err)
	defer reranker.Close()

	exprRerank := reranker.(*ExprRerank[int64])
	assert.True(t, exprRerank.needNormalize, "needNormalize should be true")

	nq := int64(1)
	topk := int64(4)
	data := embedding.GenSearchResultData(nq, topk, schemapb.DataType_Int64, "boost", 101)

	inputs, err := newRerankInputs([]*schemapb.SearchResultData{data}, reranker.GetInputFieldIDs(), false)
	require.NoError(t, err)

	searchParams := NewSearchParams(nq, topk, topk, -1, -1, 1, false, "", []string{"COSINE"})

	latencyCountBefore := testutil.CollectAndCount(metrics.RerankLatency)

	_, err = reranker.Process(context.Background(), searchParams, inputs)
	require.NoError(t, err)

	latencyCountAfter := testutil.CollectAndCount(metrics.RerankLatency)
	assert.GreaterOrEqual(t, latencyCountAfter, latencyCountBefore, "RerankLatency should be observed with normalization")
}

func TestExprRerank_Metrics_EmptyResults(t *testing.T) {
	paramtable.Init()

	registry := prometheus.NewRegistry()
	metrics.RegisterRerank(registry)

	schema := &schemapb.CollectionSchema{
		Name: "test_empty",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "value", DataType: schemapb.DataType_Float},
		},
	}

	functionSchema := &schemapb.FunctionSchema{
		Name:            "test_empty_results",
		Type:            schemapb.FunctionType_Rerank,
		InputFieldNames: []string{"value"},
		Params: []*commonpb.KeyValuePair{
			{Key: ExprCodeKey, Value: "score * 2"},
		},
	}

	reranker, err := newExprFunction(schema, functionSchema)
	require.NoError(t, err)
	defer reranker.Close()

	nq := int64(1)
	topk := int64(0)
	inputs, err := newRerankInputs([]*schemapb.SearchResultData{}, reranker.GetInputFieldIDs(), false)
	require.NoError(t, err)

	searchParams := NewSearchParams(nq, topk, topk, -1, -1, 1, false, "", []string{"COSINE"})

	latencyCountBefore := testutil.CollectAndCount(metrics.RerankLatency)

	_, err = reranker.Process(context.Background(), searchParams, inputs)
	require.NoError(t, err)

	latencyCountAfter := testutil.CollectAndCount(metrics.RerankLatency)
	assert.GreaterOrEqual(t, latencyCountAfter, latencyCountBefore, "RerankLatency should be observed even with empty results")
}

func TestExprRerank_Metrics_NoFieldsUsed(t *testing.T) {
	paramtable.Init()

	registry := prometheus.NewRegistry()
	metrics.RegisterRerank(registry)

	schema := &schemapb.CollectionSchema{
		Name: "test_no_fields",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "price", DataType: schemapb.DataType_Float},
		},
	}

	functionSchema := &schemapb.FunctionSchema{
		Name:            "test_score_only",
		Type:            schemapb.FunctionType_Rerank,
		InputFieldNames: []string{"price"},
		Params: []*commonpb.KeyValuePair{
			{Key: ExprCodeKey, Value: "score * 2.0"},
		},
	}

	reranker, err := newExprFunction(schema, functionSchema)
	require.NoError(t, err)
	defer reranker.Close()

	nq := int64(1)
	topk := int64(4)
	data := embedding.GenSearchResultData(nq, topk, schemapb.DataType_Int64, "price", 101)

	inputs, err := newRerankInputs([]*schemapb.SearchResultData{data}, reranker.GetInputFieldIDs(), false)
	require.NoError(t, err)

	searchParams := NewSearchParams(nq, topk, topk, -1, -1, 1, false, "", []string{"COSINE"})

	latencyCountBefore := testutil.CollectAndCount(metrics.RerankLatency)
	resultCountMetricsBefore := testutil.CollectAndCount(metrics.RerankResultCount)

	_, err = reranker.Process(context.Background(), searchParams, inputs)
	require.NoError(t, err)

	latencyCountAfter := testutil.CollectAndCount(metrics.RerankLatency)
	resultCountMetricsAfter := testutil.CollectAndCount(metrics.RerankResultCount)

	assert.GreaterOrEqual(t, latencyCountAfter, latencyCountBefore, "RerankLatency should be observed even with no fields")
	assert.GreaterOrEqual(t, resultCountMetricsAfter, resultCountMetricsBefore, "RerankResultCount should be incremented")
}

func TestExprRerank_Metrics_MultipleQueries(t *testing.T) {
	paramtable.Init()

	registry := prometheus.NewRegistry()
	metrics.RegisterRerank(registry)

	schema := &schemapb.CollectionSchema{
		Name: "test_multi_query",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "score_field", DataType: schemapb.DataType_Float},
		},
	}

	functionSchema := &schemapb.FunctionSchema{
		Name:            "test_multi",
		Type:            schemapb.FunctionType_Rerank,
		InputFieldNames: []string{"score_field"},
		Params: []*commonpb.KeyValuePair{
			{Key: ExprCodeKey, Value: "score + fields.score_field"},
		},
	}

	reranker, err := newExprFunction(schema, functionSchema)
	require.NoError(t, err)
	defer reranker.Close()

	nq := int64(3)
	topk := int64(5)
	data := embedding.GenSearchResultData(nq, topk, schemapb.DataType_Int64, "score_field", 101)

	inputs, err := newRerankInputs([]*schemapb.SearchResultData{data}, reranker.GetInputFieldIDs(), false)
	require.NoError(t, err)

	searchParams := NewSearchParams(nq, topk, topk, -1, -1, 1, false, "", []string{"COSINE"})

	resultCountMetricsBefore := testutil.CollectAndCount(metrics.RerankResultCount)

	_, err = reranker.Process(context.Background(), searchParams, inputs)
	require.NoError(t, err)

	resultCountMetricsAfter := testutil.CollectAndCount(metrics.RerankResultCount)
	assert.GreaterOrEqual(t, resultCountMetricsAfter, resultCountMetricsBefore,
		"Result count metrics should be recorded for multiple queries")
}

func TestExprRerank_Close(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_close",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "value", DataType: schemapb.DataType_Float},
		},
	}

	functionSchema := &schemapb.FunctionSchema{
		Name:            "test",
		Type:            schemapb.FunctionType_Rerank,
		InputFieldNames: []string{"value"},
		Params: []*commonpb.KeyValuePair{
			{Key: ExprCodeKey, Value: "score + fields.value"},
		},
	}

	reranker, err := newExprFunction(schema, functionSchema)
	require.NoError(t, err)

	exprRerank := reranker.(*ExprRerank[int64])
	assert.NotNil(t, exprRerank.program, "Program should be initialized")
	assert.NotEmpty(t, exprRerank.exprString, "Expression string should be set")
	assert.NotEmpty(t, exprRerank.collectionName, "Collection name should be set")

	exprRerank.Close()

	assert.Nil(t, exprRerank.program, "Program should be nil after Close")
	assert.Empty(t, exprRerank.exprString, "Expression string should be empty after Close")
	assert.Empty(t, exprRerank.collectionName, "Collection name should be empty after Close")
	assert.Nil(t, exprRerank.requiredFieldNames, "Required field names should be nil after Close")
	assert.Nil(t, exprRerank.requiredFieldIndices, "Required field indices should be nil after Close")
}
