// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expr

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
)

func TestNewXGBoostExprFromParams(t *testing.T) {
	params := map[string]*schemapb.FunctionParamValue{
		xgboostParamModelResource: stringParam("rank_model"),
		xgboostParamOutput:        stringParam(xgboostOutputRaw),
	}
	expr, err := NewXGBoostExprFromParams(types.FunctionBuildContext{}, types.FunctionConfig{Params: params})
	require.NoError(t, err)
	xgb, ok := expr.(*XGBoostExpr)
	require.True(t, ok)
	assert.Equal(t, "rank_model", xgb.modelResource)
	assert.Equal(t, xgboostOutputRaw, xgb.output)
	assert.True(t, xgb.IsRunnable(types.StageL0Rerank))
	assert.False(t, xgb.IsRunnable(types.StageL1Rerank))
	assert.False(t, xgb.IsRunnable(types.StageL2Rerank))
}

func TestNewXGBoostExprFromParamsDefaults(t *testing.T) {
	expr, err := NewXGBoostExprFromParams(types.FunctionBuildContext{}, types.FunctionConfig{Params: map[string]*schemapb.FunctionParamValue{
		xgboostParamModelResource: stringParam("rank_model"),
	}})
	require.NoError(t, err)
	xgb := expr.(*XGBoostExpr)
	assert.Equal(t, xgboostOutputDefault, xgb.output)
}

func TestNewXGBoostExprFromParamsInvalid(t *testing.T) {
	cases := []struct {
		name   string
		params map[string]*schemapb.FunctionParamValue
	}{
		{
			name:   "missing model resource",
			params: map[string]*schemapb.FunctionParamValue{},
		},
		{
			name: "invalid output",
			params: map[string]*schemapb.FunctionParamValue{
				xgboostParamModelResource: stringParam("rank_model"),
				xgboostParamOutput:        stringParam("probability"),
			},
		},
		{
			name: "model_format unsupported",
			params: map[string]*schemapb.FunctionParamValue{
				xgboostParamModelResource: stringParam("rank_model"),
				"model_format":            stringParam("json"),
			},
		},
		{
			name: "feature_names unsupported",
			params: map[string]*schemapb.FunctionParamValue{
				xgboostParamModelResource: stringParam("rank_model"),
				xgboostParamFeatureNames:  stringParam("feature"),
			},
		},
		{
			name: "objective unsupported",
			params: map[string]*schemapb.FunctionParamValue{
				xgboostParamModelResource: stringParam("rank_model"),
				xgboostParamObjective:     stringParam("binary:logistic"),
			},
		},
		{
			name: "unknown param",
			params: map[string]*schemapb.FunctionParamValue{
				xgboostParamModelResource: stringParam("rank_model"),
				"unknown":                 stringParam("value"),
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewXGBoostExprFromParams(types.FunctionBuildContext{}, types.FunctionConfig{Params: tc.params})
			assert.Error(t, err)
		})
	}
}

func TestXGBoostExprValidateArgs(t *testing.T) {
	expr, err := NewXGBoostExpr("rank_model", "", nil)
	require.NoError(t, err)

	assert.Error(t, expr.ValidateArgs(nil))
	assert.NoError(t, expr.ValidateArgs([]*schemapb.FunctionChainExprArg{xgboostColumnArg("price")}))
	assert.Error(t, expr.ValidateArgs([]*schemapb.FunctionChainExprArg{xgboostLiteralStringArg("price")}))
}

func TestValidateXGBoostInputChunks(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)

	col1 := newFloat32Chunked(pool, [][]float32{{1, 2}, {3}})
	defer col1.Release()
	col2 := newFloat32Chunked(pool, [][]float32{{4, 5}, {6}})
	defer col2.Release()
	assert.NoError(t, validateXGBoostInputChunks([]*arrow.Chunked{col1, col2}))

	badChunkCount := newFloat32Chunked(pool, [][]float32{{1, 2}})
	defer badChunkCount.Release()
	assert.Error(t, validateXGBoostInputChunks([]*arrow.Chunked{col1, badChunkCount}))

	badChunkLen := newFloat32Chunked(pool, [][]float32{{1}, {2}})
	defer badChunkLen.Release()
	assert.Error(t, validateXGBoostInputChunks([]*arrow.Chunked{col1, badChunkLen}))
	assert.Error(t, validateXGBoostInputChunks([]*arrow.Chunked{nil}))
}

func xgboostColumnArg(name string) *schemapb.FunctionChainExprArg {
	return &schemapb.FunctionChainExprArg{Arg: &schemapb.FunctionChainExprArg_Column{Column: &schemapb.FunctionChainColumnArg{Name: name}}}
}

func xgboostLiteralStringArg(value string) *schemapb.FunctionChainExprArg {
	return &schemapb.FunctionChainExprArg{Arg: &schemapb.FunctionChainExprArg_Literal{Literal: stringParam(value)}}
}

func newFloat32Chunked(pool memory.Allocator, values [][]float32) *arrow.Chunked {
	chunks := make([]arrow.Array, 0, len(values))
	for _, chunkValues := range values {
		builder := array.NewFloat32Builder(pool)
		builder.AppendValues(chunkValues, nil)
		chunk := builder.NewArray()
		builder.Release()
		chunks = append(chunks, chunk)
	}
	chunked := arrow.NewChunked(arrow.PrimitiveTypes.Float32, chunks)
	for _, chunk := range chunks {
		chunk.Release()
	}
	return chunked
}
