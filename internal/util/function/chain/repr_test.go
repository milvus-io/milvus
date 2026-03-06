/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package chain

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
)

// MockFunctionExpr is a mock implementation of FunctionExpr for testing.
type MockFunctionExpr struct {
	name string
}

func (m *MockFunctionExpr) Name() string {
	return m.name
}

func (m *MockFunctionExpr) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.PrimitiveTypes.Float32}
}

func (m *MockFunctionExpr) IsRunnable(stage string) bool {
	return true
}

func (m *MockFunctionExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	return inputs, nil
}

// MockBooleanFunctionExpr is a mock implementation that returns boolean type.
type MockBooleanFunctionExpr struct {
	name string
}

func (m *MockBooleanFunctionExpr) Name() string {
	return m.name
}

func (m *MockBooleanFunctionExpr) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.FixedWidthTypes.Boolean}
}

func (m *MockBooleanFunctionExpr) IsRunnable(stage string) bool {
	return true
}

func (m *MockBooleanFunctionExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	return inputs, nil
}

func init() {
	// Register mock_filter function for testing
	types.RegisterFunction("mock_filter", func(params map[string]interface{}) (types.FunctionExpr, error) {
		return &MockBooleanFunctionExpr{name: "mock_filter"}, nil
	})
}

func TestParseFuncChainRepr_BasicOperators(t *testing.T) {
	jsonStr := `{
		"name": "test-chain",
		"stage": "L2_rerank",
		"operators": [
			{
				"type": "filter",
				"function": {
					"name": "mock_filter",
					"params": {}
				},
				"inputs": ["score"]
			},
			{
				"type": "select",
				"params": {
					"columns": ["id", "score", "name"]
				}
			},
			{
				"type": "sort",
				"params": {
					"column": "score",
					"desc": true
				}
			},
			{
				"type": "limit",
				"params": {
					"limit": 10,
					"offset": 5
				}
			}
		]
	}`

	chain, err := ParseFuncChainRepr(jsonStr, memory.NewGoAllocator())
	assert.NoError(t, err)
	assert.NotNil(t, chain)
	assert.Equal(t, "test-chain", chain.name)
	assert.Equal(t, types.StageL2Rerank, chain.Stage())
	assert.Len(t, chain.operators, 4)

	// Verify operator types
	assert.IsType(t, &FilterOp{}, chain.operators[0])
	assert.IsType(t, &SelectOp{}, chain.operators[1])
	assert.IsType(t, &SortOp{}, chain.operators[2])
	assert.IsType(t, &LimitOp{}, chain.operators[3])
}

func TestParseFuncChainRepr_InvalidRepr(t *testing.T) {
	jsonStr := `{ invalid json }`
	_, err := ParseFuncChainRepr(jsonStr, memory.NewGoAllocator())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse JSON")
}

func TestParseFuncChainRepr_UnknownOperator(t *testing.T) {
	jsonStr := `{
		"stage": "L2_rerank",
		"operators": [
			{
				"type": "unknown_op"
			}
		]
	}`

	_, err := ParseFuncChainRepr(jsonStr, memory.NewGoAllocator())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown operator type")
}

func TestParseFuncChainRepr_MissingStage(t *testing.T) {
	jsonStr := `{
		"operators": [
			{
				"type": "select",
				"params": {"columns": ["test"]}
			}
		]
	}`

	_, err := ParseFuncChainRepr(jsonStr, memory.NewGoAllocator())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "stage is required")
}

func TestParseFuncChainRepr_MissingParams(t *testing.T) {
	testCases := []struct {
		name    string
		jsonStr string
		errMsg  string
	}{
		{
			name: "filter missing function",
			jsonStr: `{
				"stage": "L2_rerank",
				"operators": [{"type": "filter", "params": {}}]
			}`,
			errMsg: "filter_op: function is required",
		},
		{
			name: "filter missing inputs",
			jsonStr: `{
				"stage": "L2_rerank",
				"operators": [{"type": "filter", "function": {"name": "mock_filter", "params": {}}}]
			}`,
			errMsg: "filter_op: inputs is required",
		},
		{
			name: "sort missing column",
			jsonStr: `{
				"stage": "L2_rerank",
				"operators": [{"type": "sort", "params": {}}]
			}`,
			errMsg: "sort_op: column is required",
		},
		{
			name: "select missing columns",
			jsonStr: `{
				"stage": "L2_rerank",
				"operators": [{"type": "select", "params": {}}]
			}`,
			errMsg: "select_op: columns is required",
		},
		{
			name: "limit invalid limit",
			jsonStr: `{
				"stage": "L2_rerank",
				"operators": [{"type": "limit", "params": {"limit": 0}}]
			}`,
			errMsg: "limit_op: limit must be positive",
		},
		{
			name: "map missing function",
			jsonStr: `{
				"stage": "L2_rerank",
				"operators": [{"type": "map"}]
			}`,
			errMsg: "map operator requires function",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseFuncChainRepr(tc.jsonStr, memory.NewGoAllocator())
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestParseFuncChainRepr_UnknownFunction(t *testing.T) {
	jsonStr := `{
		"stage": "L2_rerank",
		"operators": [
			{
				"type": "map",
				"function": {
					"name": "UNKNOWN_FUNC",
					"params": {}
				}
			}
		]
	}`

	_, err := ParseFuncChainRepr(jsonStr, memory.NewGoAllocator())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown function")
}

func TestFuncChainFromRepr(t *testing.T) {
	repr := &ChainRepr{
		Name:  "repr-chain",
		Stage: types.StageL2Rerank,
		Operators: []OperatorRepr{
			{
				Type: types.OpTypeFilter,
				Function: &FunctionRepr{
					Name:   "mock_filter",
					Params: map[string]interface{}{},
				},
				Inputs: []string{"score"},
			},
			{
				Type: types.OpTypeSelect,
				Params: map[string]interface{}{
					"columns": []string{"a", "b"},
				},
			},
			{
				Type: types.OpTypeSort,
				Params: map[string]interface{}{
					"column": "a",
					"desc":   true,
				},
			},
			{
				Type: types.OpTypeLimit,
				Params: map[string]interface{}{
					"limit":  int64(100),
					"offset": int64(10),
				},
			},
		},
	}

	chain, err := funcChainFromRepr(repr, memory.NewGoAllocator())
	assert.NoError(t, err)
	assert.NotNil(t, chain)
	assert.Equal(t, "repr-chain", chain.name)
	assert.Len(t, chain.operators, 4)
}

// =============================================================================
// FunctionScore Conversion Tests
// =============================================================================

func TestFunctionScoreToRepr_SingleFunction(t *testing.T) {
	funcScore := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "decay"},
					{Key: "input_column", Value: "timestamp"}, // Required: specifies input column
					{Key: "origin", Value: "0"},
					{Key: "scale", Value: "100"},
				},
			},
		},
		Params: []*commonpb.KeyValuePair{
			{Key: "boost_mode", Value: "multiply"},
		},
	}

	repr, err := functionScoreToRepr(funcScore)
	assert.NoError(t, err)
	assert.NotNil(t, repr)
	assert.Equal(t, "function_score_chain", repr.Name)

	// Should have 4 operators: MapOp(decay), MapOp(score_combine), SortOp, SelectOp
	assert.Len(t, repr.Operators, 4)

	// First operator: MapOp for decay function
	assert.Equal(t, types.OpTypeMap, repr.Operators[0].Type)
	assert.NotNil(t, repr.Operators[0].Function)
	assert.Equal(t, "decay", repr.Operators[0].Function.Name)
	// Column mapping is at operator level now
	assert.Equal(t, []string{"timestamp", "$score"}, repr.Operators[0].Inputs)
	assert.Equal(t, []string{"_func_score_0"}, repr.Operators[0].Outputs)

	// Second operator: MapOp for score_combine
	assert.Equal(t, types.OpTypeMap, repr.Operators[1].Type)
	assert.NotNil(t, repr.Operators[1].Function)
	assert.Equal(t, "score_combine", repr.Operators[1].Function.Name)
	assert.Equal(t, "multiply", repr.Operators[1].Function.Params["mode"])
	// Input cols are at operator level
	assert.Equal(t, []string{"$score", "_func_score_0"}, repr.Operators[1].Inputs)
	assert.Equal(t, []string{"$score"}, repr.Operators[1].Outputs)

	// Third operator: SortOp
	assert.Equal(t, types.OpTypeSort, repr.Operators[2].Type)
	assert.Equal(t, "$score", repr.Operators[2].Params["column"])
	assert.True(t, repr.Operators[2].Params["desc"].(bool))
}

func TestFunctionScoreToRepr_MultipleFunctions(t *testing.T) {
	funcScore := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "decay"},
					{Key: "input_column", Value: "timestamp"},
				},
			},
			{
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "model"},
					{Key: "input_column", Value: "text"},
				},
			},
		},
		Params: []*commonpb.KeyValuePair{
			{Key: "boost_mode", Value: "sum"},
		},
	}

	repr, err := functionScoreToRepr(funcScore)
	assert.NoError(t, err)
	assert.NotNil(t, repr)

	// Should have 5 operators: MapOp(decay), MapOp(model), MapOp(score_combine), SortOp, SelectOp
	assert.Len(t, repr.Operators, 5)

	// First function: decay
	assert.Equal(t, "decay", repr.Operators[0].Function.Name)
	assert.Equal(t, []string{"_func_score_0"}, repr.Operators[0].Outputs)

	// Second function: model
	assert.Equal(t, "model", repr.Operators[1].Function.Name)
	assert.Equal(t, []string{"_func_score_1"}, repr.Operators[1].Outputs)

	// score_combine should have 3 input columns at operator level
	assert.Equal(t, []string{"$score", "_func_score_0", "_func_score_1"}, repr.Operators[2].Inputs)
	assert.Equal(t, "sum", repr.Operators[2].Function.Params["mode"])
}

func TestFunctionScoreToRepr_WithWeights(t *testing.T) {
	funcScore := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "decay"},
					{Key: "input_column", Value: "timestamp"},
				},
			},
		},
		Params: []*commonpb.KeyValuePair{
			{Key: "boost_mode", Value: "weighted"},
			{Key: "weights", Value: "[0.5, 0.5]"},
		},
	}

	repr, err := functionScoreToRepr(funcScore)
	assert.NoError(t, err)
	assert.NotNil(t, repr)

	// Check weights are parsed
	assert.Equal(t, "weighted", repr.Operators[1].Function.Params["mode"])
	weights := repr.Operators[1].Function.Params["weights"].([]float64)
	assert.Equal(t, []float64{0.5, 0.5}, weights)
}

func TestFunctionScoreToRepr_DefaultBoostMode(t *testing.T) {
	funcScore := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "decay"},
					{Key: "input_column", Value: "timestamp"},
				},
			},
		},
		Params: []*commonpb.KeyValuePair{}, // no boost_mode specified
	}

	repr, err := functionScoreToRepr(funcScore)
	assert.NoError(t, err)
	assert.NotNil(t, repr)

	// Default boost_mode should be "multiply"
	assert.Equal(t, "multiply", repr.Operators[1].Function.Params["mode"])
}

func TestFunctionScoreToRepr_NilFuncScore(t *testing.T) {
	_, err := functionScoreToRepr(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "funcScore is nil")
}

func TestFunctionScoreToRepr_MissingReranker(t *testing.T) {
	funcScore := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Params: []*commonpb.KeyValuePair{
					{Key: "field", Value: "timestamp"}, // missing "reranker" key
				},
			},
		},
	}

	_, err := functionScoreToRepr(funcScore)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reranker")
}

func TestParseFuncChainFromFunctionScore(t *testing.T) {
	// Register mock functions for testing
	types.RegisterFunction("decay", func(params map[string]interface{}) (types.FunctionExpr, error) {
		return &MockFunctionExpr{name: "decay"}, nil
	})
	types.RegisterFunction("score_combine", func(params map[string]interface{}) (types.FunctionExpr, error) {
		return &MockFunctionExpr{name: "score_combine"}, nil
	})

	funcScore := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "decay"},
					{Key: "input_column", Value: "age"},
					{Key: "function", Value: "gauss"},
					{Key: "origin", Value: "30"},
					{Key: "scale", Value: "10"},
				},
			},
		},
		Params: []*commonpb.KeyValuePair{
			{Key: "boost_mode", Value: "multiply"},
		},
	}

	// This test verifies the chain can be created
	// Note: actual execution would require the decay function to be registered
	chain, err := ParseFuncChainFromFunctionScore(funcScore, memory.NewGoAllocator())
	assert.NoError(t, err)
	assert.NotNil(t, chain)
	assert.Equal(t, "function_score_chain", chain.name)
	assert.Len(t, chain.operators, 4)
}

func TestGetParamValue(t *testing.T) {
	params := []*commonpb.KeyValuePair{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
	}

	// Existing key
	assert.Equal(t, "value1", getParamValue(params, "key1", "default"))
	assert.Equal(t, "value2", getParamValue(params, "key2", "default"))

	// Non-existing key should return default
	assert.Equal(t, "default", getParamValue(params, "key3", "default"))

	// Empty params
	assert.Equal(t, "default", getParamValue(nil, "key1", "default"))
}
