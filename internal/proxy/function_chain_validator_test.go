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
package proxy

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	chainexpr "github.com/milvus-io/milvus/internal/util/function/chain/expr"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestValidateFunctionChainSearchRequest(t *testing.T) {
	t.Run("ordinary function chains", func(t *testing.T) {
		err := validateFunctionChainSearchRequest(&milvuspb.SearchRequest{
			FunctionChains: []*schemapb.FunctionChain{l2FunctionChain(mapOp(types.ScoreFieldName, "expr", columnArg(types.ScoreFieldName)))},
		}, false)
		require.NoError(t, err)
	})

	t.Run("ordinary request without function chains", func(t *testing.T) {
		err := validateFunctionChainSearchRequest(&milvuspb.SearchRequest{}, false)
		require.NoError(t, err)
	})

	t.Run("function score and function chains", func(t *testing.T) {
		err := validateFunctionChainSearchRequest(&milvuspb.SearchRequest{
			FunctionScore:  &schemapb.FunctionScore{},
			FunctionChains: []*schemapb.FunctionChain{l2FunctionChain(mapOp(types.ScoreFieldName, "expr", columnArg(types.ScoreFieldName)))},
		}, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "function_score and function_chains cannot be used together")
	})

	t.Run("hybrid function chains are validated by hybrid selector", func(t *testing.T) {
		err := validateFunctionChainSearchRequest(&milvuspb.SearchRequest{
			FunctionChains: []*schemapb.FunctionChain{l2FunctionChain(mapOp(types.ScoreFieldName, "expr", columnArg(types.ScoreFieldName)))},
		}, true)
		require.NoError(t, err)
	})
}

func TestSelectHybridRerankMeta(t *testing.T) {
	schema := newFunctionChainTestSchema()
	mergeOp := &schemapb.FunctionChainOp{
		Op: types.OpTypeMerge,
		Params: map[string]*schemapb.FunctionParamValue{
			"strategy": chainStringParam("rrf"),
		},
	}
	chainPB := l2FunctionChain(mergeOp)
	responseParams := []*commonpb.KeyValuePair{
		{Key: LimitKey, Value: "10"},
		{Key: OffsetKey, Value: "2"},
		{Key: RoundDecimalKey, Value: "3"},
	}
	subReqs := []*milvuspb.SubSearchRequest{{}, {}}

	t.Run("chain accepts response controls", func(t *testing.T) {
		meta, err := selectHybridRerankMeta(&milvuspb.SearchRequest{
			FunctionChains: []*schemapb.FunctionChain{chainPB},
			SearchParams:   responseParams,
			SubReqs:        subReqs,
		}, schema)
		require.NoError(t, err)
		_, ok := meta.(*functionChainRerankMeta)
		assert.True(t, ok)

		meta, err = selectHybridRerankMeta(&milvuspb.SearchRequest{
			FunctionChains: []*schemapb.FunctionChain{chainPB},
			SearchParams: append(responseParams,
				&commonpb.KeyValuePair{Key: RankTypeKey, Value: ""},
				&commonpb.KeyValuePair{Key: ParamsKey, Value: "null"},
			),
			SubReqs: subReqs,
		}, schema)
		require.NoError(t, err)
		_, ok = meta.(*functionChainRerankMeta)
		assert.True(t, ok)
	})

	t.Run("chain rejects other rerank sources", func(t *testing.T) {
		_, err := selectHybridRerankMeta(&milvuspb.SearchRequest{
			FunctionChains: []*schemapb.FunctionChain{chainPB},
			FunctionScore:  &schemapb.FunctionScore{},
			SearchParams:   responseParams,
			SubReqs:        subReqs,
		}, schema)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "cannot be used with function_score")

		for _, key := range []string{RankTypeKey, ParamsKey, strings.ToUpper(RankTypeKey)} {
			params := append([]*commonpb.KeyValuePair{}, responseParams...)
			params = append(params, &commonpb.KeyValuePair{Key: key, Value: "configured"})
			_, err = selectHybridRerankMeta(&milvuspb.SearchRequest{
				FunctionChains: []*schemapb.FunctionChain{chainPB},
				SearchParams:   params,
				SubReqs:        subReqs,
			}, schema)
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
			assert.Contains(t, err.Error(), "rank_params strategy or params")
		}
	})

	t.Run("preserves existing sources", func(t *testing.T) {
		meta, err := selectHybridRerankMeta(&milvuspb.SearchRequest{SearchParams: responseParams}, schema)
		require.NoError(t, err)
		_, ok := meta.(*legacyRerankMeta)
		assert.True(t, ok)

		meta, err = selectHybridRerankMeta(&milvuspb.SearchRequest{
			FunctionScore: &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{{
				Type: schemapb.FunctionType_Rerank,
			}}},
		}, schema)
		require.NoError(t, err)
		_, ok = meta.(*funcScoreRerankMeta)
		assert.True(t, ok)
	})

	t.Run("function score rejects nested function chains", func(t *testing.T) {
		_, err := selectHybridRerankMeta(&milvuspb.SearchRequest{
			FunctionScore: &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "boost"},
					{Key: "weight", Value: "2.0"},
				},
			}}},
			SubReqs: []*milvuspb.SubSearchRequest{{
				FunctionChains: []*schemapb.FunctionChain{l0FunctionChain()},
			}},
		}, schema)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "function_score cannot be used with function_chains in sub-search[0]")
	})
}

func TestSplitFunctionChainsByStage(t *testing.T) {
	t.Run("split l0 l1 and l2 chains", func(t *testing.T) {
		l0Chain := l0FunctionChain(mapOp(types.ScoreFieldName, "xgboost", columnArg("pk")))
		l1Chain := l1FunctionChain(mapOp(types.ScoreFieldName, "expr", columnArg(types.ScoreFieldName)))
		l2Chain := l2FunctionChain(mapOp(types.ScoreFieldName, "expr", columnArg(types.ScoreFieldName)))

		l2Chains, querynodeChains, err := splitFunctionChainsByStage([]*schemapb.FunctionChain{l0Chain, l1Chain, l2Chain})
		require.NoError(t, err)
		assert.Equal(t, []*schemapb.FunctionChain{l2Chain}, l2Chains)
		assert.Equal(t, []*schemapb.FunctionChain{l0Chain, l1Chain}, querynodeChains)
	})

	t.Run("empty l0 chain", func(t *testing.T) {
		_, _, err := splitFunctionChainsByStage([]*schemapb.FunctionChain{l0FunctionChain()})
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "function chain[0] must contain at least one op")
	})

	t.Run("nil chain", func(t *testing.T) {
		_, _, err := splitFunctionChainsByStage([]*schemapb.FunctionChain{nil})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "function chain[0] is nil")
	})

	t.Run("duplicate stage", func(t *testing.T) {
		_, _, err := splitFunctionChainsByStage([]*schemapb.FunctionChain{
			l0FunctionChain(mapOp(types.ScoreFieldName, "xgboost", columnArg("pk"))),
			l0FunctionChain(mapOp(types.ScoreFieldName, "xgboost", columnArg("pk"))),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "appears more than once")
	})

	t.Run("empty l1 chain", func(t *testing.T) {
		_, _, err := splitFunctionChainsByStage([]*schemapb.FunctionChain{l1FunctionChain()})
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "function chain[0] must contain at least one op")
	})

	t.Run("duplicate l1 stage", func(t *testing.T) {
		_, _, err := splitFunctionChainsByStage([]*schemapb.FunctionChain{
			l1FunctionChain(mapOp(types.ScoreFieldName, "expr", columnArg(types.ScoreFieldName))),
			l1FunctionChain(mapOp(types.ScoreFieldName, "expr", columnArg(types.ScoreFieldName))),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "appears more than once")
	})
}

func TestNewFunctionChainRerankMeta(t *testing.T) {
	schema := newFunctionChainTestSchema()

	t.Run("empty chains", func(t *testing.T) {
		meta, err := newFunctionChainRerankMeta(nil, schema)
		require.NoError(t, err)
		assert.Nil(t, meta)
	})

	t.Run("score only chain", func(t *testing.T) {
		chainPB := l2FunctionChain(mapOp(types.ScoreFieldName, "expr", columnArg(types.ScoreFieldName)))

		meta, err := newFunctionChainRerankMeta([]*schemapb.FunctionChain{chainPB}, schema)
		require.NoError(t, err)
		require.NotNil(t, meta)
		assert.Empty(t, meta.GetInputFieldNames())
		assert.Empty(t, meta.GetInputFieldIDs())
		assert.Equal(t, chainPB, meta.chainPB)
		assert.NotNil(t, meta.repr)
	})

	t.Run("schema field and score", func(t *testing.T) {
		chainPB := l2FunctionChain(
			mapOp("score1", "decay", columnArg("ts")),
			mapOp(types.ScoreFieldName, "sum", columnArg("score1"), columnArg(types.ScoreFieldName)),
		)

		meta, err := newFunctionChainRerankMeta([]*schemapb.FunctionChain{chainPB}, schema)
		require.NoError(t, err)
		require.NotNil(t, meta)
		assert.Equal(t, []string{"ts"}, meta.GetInputFieldNames())
		assert.Equal(t, []int64{101}, meta.GetInputFieldIDs())
	})

	t.Run("ordinary search rejects merge", func(t *testing.T) {
		chainPB := l2FunctionChain(&schemapb.FunctionChainOp{
			Op: types.OpTypeMerge,
			Params: map[string]*schemapb.FunctionParamValue{
				"strategy": chainStringParam("rrf"),
			},
		})

		_, err := newFunctionChainRerankMeta([]*schemapb.FunctionChain{chainPB}, schema)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "merge is not supported in ordinary search")
	})

	t.Run("group by system field is not planned as schema input", func(t *testing.T) {
		chainPB := l2FunctionChain(&schemapb.FunctionChainOp{
			Op: types.OpTypeGroupBy,
			Params: map[string]*schemapb.FunctionParamValue{
				"field":      chainStringParam("$group_by_101"),
				"group_size": chainIntParam(2),
				"limit":      chainIntParam(10),
			},
		})

		meta, err := newFunctionChainRerankMeta([]*schemapb.FunctionChain{chainPB}, schema)
		require.NoError(t, err)
		require.NotNil(t, meta)
		assert.Empty(t, meta.GetInputFieldNames())
		assert.Empty(t, meta.GetInputFieldIDs())
	})

	t.Run("duplicate schema input is planned once", func(t *testing.T) {
		chainPB := l2FunctionChain(
			mapOp("score1", "expr", columnArg("ts"), columnArg(types.ScoreFieldName)),
			mapOp(types.ScoreFieldName, "expr", columnArg("ts"), columnArg("score1")),
		)

		meta, err := newFunctionChainRerankMeta([]*schemapb.FunctionChain{chainPB}, schema)
		require.NoError(t, err)
		require.NotNil(t, meta)
		assert.Equal(t, []string{"ts"}, meta.GetInputFieldNames())
		assert.Equal(t, []int64{101}, meta.GetInputFieldIDs())
	})

	t.Run("struct array sub field input is unsupported", func(t *testing.T) {
		structSchema := newFunctionChainStructTestSchema()
		chainPB := l2FunctionChain(mapOp("score1", "expr", columnArg("struct_scalar_array")))

		_, err := newFunctionChainRerankMeta([]*schemapb.FunctionChain{chainPB}, structSchema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported field type")
		assert.Contains(t, err.Error(), "Array")
	})

	t.Run("nil schema", func(t *testing.T) {
		_, _, err := getFunctionChainInputField(nil, "ts")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "neither a previous output nor a collection field")
	})

	t.Run("duplicate stage", func(t *testing.T) {
		_, err := newFunctionChainRerankMeta([]*schemapb.FunctionChain{
			l2FunctionChain(mapOp(types.ScoreFieldName, "expr", columnArg(types.ScoreFieldName))),
			l2FunctionChain(mapOp(types.ScoreFieldName, "expr", columnArg(types.ScoreFieldName))),
		}, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "appears more than once")
	})

	t.Run("non l2 stage", func(t *testing.T) {
		_, err := newFunctionChainRerankMeta([]*schemapb.FunctionChain{
			{
				Stage: schemapb.FunctionChainStage_FunctionChainStageL1Rerank,
				Ops:   []*schemapb.FunctionChainOp{mapOp(types.ScoreFieldName, "expr", columnArg(types.ScoreFieldName))},
			},
		}, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is not supported in search request")
	})

	t.Run("empty l2 chain", func(t *testing.T) {
		_, err := newFunctionChainRerankMeta([]*schemapb.FunctionChain{
			l2FunctionChain(),
		}, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "function chain[0] must contain at least one op")
	})

	t.Run("unknown field", func(t *testing.T) {
		_, err := newFunctionChainRerankMeta([]*schemapb.FunctionChain{
			l2FunctionChain(mapOp("score1", "decay", columnArg("unknown"))),
		}, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown")
		assert.Contains(t, err.Error(), "neither a previous output nor a collection field")
	})

	t.Run("unsupported system input", func(t *testing.T) {
		_, err := newFunctionChainRerankMeta([]*schemapb.FunctionChain{
			l2FunctionChain(mapOp("score1", "expr", columnArg("$timestamp"))),
		}, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "system input \"$timestamp\" is not supported")
	})

	t.Run("unsupported system output", func(t *testing.T) {
		_, err := newFunctionChainRerankMeta([]*schemapb.FunctionChain{
			l2FunctionChain(mapOp(types.IDFieldName, "expr", columnArg(types.ScoreFieldName), columnArg("ts"))),
		}, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "system output \"$id\" is not writable")
	})

	t.Run("reserved temporary system output", func(t *testing.T) {
		_, err := newFunctionChainRerankMeta([]*schemapb.FunctionChain{
			l2FunctionChain(mapOp("$tmp_score", "expr", columnArg(types.ScoreFieldName), columnArg("ts"))),
		}, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "system output \"$tmp_score\" is not writable")
	})

	t.Run("score system output is writable", func(t *testing.T) {
		_, err := newFunctionChainRerankMeta([]*schemapb.FunctionChain{
			l2FunctionChain(mapOp(types.ScoreFieldName, "expr", columnArg(types.ScoreFieldName), columnArg("ts"))),
		}, schema)
		require.NoError(t, err)
	})

	t.Run("xgboost supports l2 stage", func(t *testing.T) {
		xgboostOp := mapOp(
			types.ScoreFieldName,
			chainexpr.XGBoostFuncName,
			columnArg("price"),
		)
		xgboostOp.GetExpr().Params = map[string]*schemapb.FunctionParamValue{
			"model_resource": chainStringParam("rank_model"),
		}
		chainPB := l2FunctionChain(xgboostOp)

		meta, err := newFunctionChainRerankMeta([]*schemapb.FunctionChain{chainPB}, schema)
		require.NoError(t, err)
		require.NotNil(t, meta)
		assert.Equal(t, []string{"price"}, meta.GetInputFieldNames())
		assert.Equal(t, []int64{105}, meta.GetInputFieldIDs())
	})

	t.Run("unsupported field type", func(t *testing.T) {
		_, err := newFunctionChainRerankMeta([]*schemapb.FunctionChain{
			l2FunctionChain(mapOp("score1", "expr", columnArg("vec"))),
		}, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported field type")
		assert.Contains(t, err.Error(), "FloatVector")
	})
}

func TestNewHybridFunctionChainRerankMeta(t *testing.T) {
	schema := newFunctionChainTestSchema()
	mergeOp := func(strategy string) *schemapb.FunctionChainOp {
		return &schemapb.FunctionChainOp{
			Op: types.OpTypeMerge,
			Params: map[string]*schemapb.FunctionParamValue{
				"strategy": chainStringParam(strategy),
			},
		}
	}

	t.Run("valid merge and downstream scalar input", func(t *testing.T) {
		chainPB := l2FunctionChain(
			mergeOp("rrf"),
			mapOp(types.ScoreFieldName, "expr", columnArg(types.ScoreFieldName), columnArg("ts")),
		)

		meta, err := newHybridFunctionChainRerankMeta([]*schemapb.FunctionChain{chainPB}, schema, 2)
		require.NoError(t, err)
		require.NotNil(t, meta)
		assert.Equal(t, []string{"ts"}, meta.GetInputFieldNames())
		assert.Equal(t, []int64{101}, meta.GetInputFieldIDs())
	})

	t.Run("valid merge and downstream xgboost", func(t *testing.T) {
		xgboostOp := mapOp(
			types.ScoreFieldName,
			chainexpr.XGBoostFuncName,
			columnArg("price"),
		)
		xgboostOp.GetExpr().Params = map[string]*schemapb.FunctionParamValue{
			"model_resource": chainStringParam("rank_model"),
		}
		chainPB := l2FunctionChain(mergeOp("rrf"), xgboostOp)

		meta, err := newHybridFunctionChainRerankMeta([]*schemapb.FunctionChain{chainPB}, schema, 2)
		require.NoError(t, err)
		require.NotNil(t, meta)
		assert.Equal(t, []string{"price"}, meta.GetInputFieldNames())
		assert.Equal(t, []int64{105}, meta.GetInputFieldIDs())
	})

	t.Run("requires exactly one chain", func(t *testing.T) {
		_, err := newHybridFunctionChainRerankMeta(nil, schema, 2)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires exactly one function chain")

		_, err = newHybridFunctionChainRerankMeta([]*schemapb.FunctionChain{
			l2FunctionChain(mergeOp("rrf")),
			l2FunctionChain(mergeOp("rrf")),
		}, schema, 2)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires exactly one function chain")
	})

	t.Run("requires l2 stage", func(t *testing.T) {
		_, err := newHybridFunctionChainRerankMeta([]*schemapb.FunctionChain{{
			Stage: schemapb.FunctionChainStage_FunctionChainStageL1Rerank,
			Ops:   []*schemapb.FunctionChainOp{mergeOp("rrf")},
		}}, schema, 2)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "stage FunctionChainStageL1Rerank is not supported")
	})

	t.Run("requires one first merge", func(t *testing.T) {
		_, err := newHybridFunctionChainRerankMeta([]*schemapb.FunctionChain{
			l2FunctionChain(mapOp(types.ScoreFieldName, "expr", columnArg(types.ScoreFieldName))),
		}, schema, 2)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must contain exactly one merge operator")

		_, err = newHybridFunctionChainRerankMeta([]*schemapb.FunctionChain{
			l2FunctionChain(
				mapOp(types.ScoreFieldName, "expr", columnArg(types.ScoreFieldName)),
				mergeOp("rrf"),
			),
		}, schema, 2)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "merge operator must be first")

		_, err = newHybridFunctionChainRerankMeta([]*schemapb.FunctionChain{
			l2FunctionChain(mergeOp("rrf"), mergeOp("rrf")),
		}, schema, 2)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must contain exactly one merge operator")
	})

	t.Run("validates merge parameters and input count", func(t *testing.T) {
		_, err := newHybridFunctionChainRerankMeta([]*schemapb.FunctionChain{
			l2FunctionChain(mergeOp("unsupported")),
		}, schema, 2)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "unsupported strategy")

		weightedOp := mergeOp("weighted")
		weightedOp.Params["weights"] = chainArrayParam(chainDoubleParam(1))
		_, err = newHybridFunctionChainRerankMeta([]*schemapb.FunctionChain{
			l2FunctionChain(weightedOp),
		}, schema, 2)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "weights count 1 does not match search input count 2")

		rrfOp := mergeOp("rrf")
		rrfOp.Params["weights"] = chainArrayParam(chainDoubleParam(1))
		_, err = newHybridFunctionChainRerankMeta([]*schemapb.FunctionChain{
			l2FunctionChain(rrfOp),
		}, schema, 2)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "weights count 1 does not match search input count 2")
	})
}

func newFunctionChainTestSchema() *schemaInfo {
	return mustNewSchemaInfo(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "ts", DataType: schemapb.DataType_Int64},
			{FieldID: 102, Name: "tag", DataType: schemapb.DataType_VarChar},
			{FieldID: 103, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
			{FieldID: 104, Name: "flag", DataType: schemapb.DataType_Bool},
			{FieldID: 105, Name: "price", DataType: schemapb.DataType_Double},
		},
	})
}

func newFunctionChainStructTestSchema() *schemaInfo {
	return mustNewSchemaInfo(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				Name: "structArray",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 201, Name: "struct_scalar_array", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32},
				},
			},
		},
	})
}

func l2FunctionChain(ops ...*schemapb.FunctionChainOp) *schemapb.FunctionChain {
	return &schemapb.FunctionChain{
		Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank,
		Ops:   ops,
	}
}

func l0FunctionChain(ops ...*schemapb.FunctionChainOp) *schemapb.FunctionChain {
	return &schemapb.FunctionChain{
		Stage: schemapb.FunctionChainStage_FunctionChainStageL0Rerank,
		Ops:   ops,
	}
}

func l1FunctionChain(ops ...*schemapb.FunctionChainOp) *schemapb.FunctionChain {
	return &schemapb.FunctionChain{
		Stage: schemapb.FunctionChainStage_FunctionChainStageL1Rerank,
		Ops:   ops,
	}
}

func l1LimitFunctionChain(limit int64) *schemapb.FunctionChain {
	return l1FunctionChain(&schemapb.FunctionChainOp{
		Op: types.OpTypeLimit,
		Params: map[string]*schemapb.FunctionParamValue{
			"limit": {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: limit}},
		},
	})
}

func l2LimitFunctionChain(limit int64) *schemapb.FunctionChain {
	return l2FunctionChain(&schemapb.FunctionChainOp{
		Op: types.OpTypeLimit,
		Params: map[string]*schemapb.FunctionParamValue{
			"limit": {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: limit}},
		},
	})
}

func chainStringParam(value string) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{
		Value: &schemapb.FunctionParamValue_StringValue{StringValue: value},
	}
}

func chainIntParam(value int64) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{
		Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: value},
	}
}

func chainDoubleParam(value float64) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{
		Value: &schemapb.FunctionParamValue_DoubleValue{DoubleValue: value},
	}
}

func chainArrayParam(values ...*schemapb.FunctionParamValue) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{
		Value: &schemapb.FunctionParamValue_ArrayValue{
			ArrayValue: &schemapb.FunctionParamArray{Values: values},
		},
	}
}

func mapOp(output string, exprName string, args ...*schemapb.FunctionChainExprArg) *schemapb.FunctionChainOp {
	return &schemapb.FunctionChainOp{
		Op:      types.OpTypeMap,
		Outputs: []string{output},
		Expr: &schemapb.FunctionChainExpr{
			Name:   exprName,
			Args:   args,
			Params: map[string]*schemapb.FunctionParamValue{},
		},
	}
}

func columnArg(name string) *schemapb.FunctionChainExprArg {
	return &schemapb.FunctionChainExprArg{Arg: &schemapb.FunctionChainExprArg_Column{Column: &schemapb.FunctionChainColumnArg{Name: name}}}
}
