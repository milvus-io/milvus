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

package dql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// A group-by field name used to resolve to a field id with no look at the
// type: grouping by a vector field was accepted here and silently degenerated
// or failed deep in the query. The supported list mirrors the switch in
// SearchGroupByOperator.cpp.
func TestParseGroupByFieldChecksFieldType(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "coll",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector},
			{FieldID: 102, Name: "bvec", DataType: schemapb.DataType_BinaryVector},
			{FieldID: 103, Name: "price", DataType: schemapb.DataType_Double},
			{FieldID: 104, Name: "ratio", DataType: schemapb.DataType_Float},
			{FieldID: 105, Name: "tag", DataType: schemapb.DataType_VarChar},
			{FieldID: 106, Name: "flag", DataType: schemapb.DataType_Bool},
			{FieldID: 107, Name: "meta", DataType: schemapb.DataType_JSON},
			{FieldID: 108, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
		},
	}

	for _, name := range []string{"pk", "tag", "flag", "meta", `meta["k"]`} {
		_, _, err := parseGroupByField(name, schema)
		assert.NoError(t, err, name)
	}
	_, jsonPath, err := parseGroupByField("free_key", schema)
	assert.NoError(t, err)
	assert.Equal(t, "free_key", jsonPath)

	schema.Fields = append(schema.Fields,
		&schemapb.FieldSchema{FieldID: 109, Name: "legacy_str", DataType: schemapb.DataType_String})

	for _, name := range []string{"vec", "price", "ratio", `vec["x"]`, "legacy_str"} {
		_, _, err := parseGroupByField(name, schema)
		assert.Error(t, err, name)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid, name)
		assert.Contains(t, err.Error(), "unsupported data type for group by", name)
	}
	_, _, err = parseGroupByField("bvec", schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not support search_group_by operation based on binary vector column")
}

// The plan carries a single json_path/json_type and the executor asserts at
// most one JSON group-by field; two bare JSON fields used to pass the proxy
// (neither produces a jsonPath) and fail only deep in execution.
func TestParseGroupByInfoRejectsMultipleJSONFields(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "meta1", DataType: schemapb.DataType_JSON},
			{FieldID: 102, Name: "meta2", DataType: schemapb.DataType_JSON},
			{FieldID: 103, Name: "tag", DataType: schemapb.DataType_VarChar},
		},
	}

	_, err := parseGroupByInfo([]*commonpb.KeyValuePair{
		{Key: GroupByFieldsKey, Value: "meta1,meta2"},
	}, schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "at most one JSON field")

	info, err := parseGroupByInfo([]*commonpb.KeyValuePair{
		{Key: GroupByFieldsKey, Value: "meta1,tag"},
	}, schema)
	assert.NoError(t, err)
	assert.Equal(t, []int64{101, 103}, info.groupByFieldIds)
}

func TestRankParamsCarryJSONGroupAttributes(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "metadata", DataType: schemapb.DataType_JSON},
			{FieldID: 102, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
		},
	}
	for _, tc := range []struct {
		field string
		path  string
		id    int64
	}{
		{`metadata["group"]`, "/group", 101},
		{`$meta["profile"]["group"]`, "/profile/group", 102},
	} {
		t.Run(tc.field, func(t *testing.T) {
			parsed, err := parseRankParams([]*commonpb.KeyValuePair{
				{Key: LimitKey, Value: "10"},
				{Key: GroupByFieldKey, Value: tc.field},
				{Key: JSONType, Value: "VarChar"},
				{Key: StrictCastKey, Value: "true"},
			}, schema, false)
			require.NoError(t, err)
			assert.Equal(t, tc.path, parsed.GetJSONPath())
			info, err := parseSearchInfo(getValidSearchParams(), schema, parsed, false)
			require.NoError(t, err)
			assert.Equal(t, tc.id, info.planInfo.GetGroupByFieldId())
			assert.Equal(t, tc.path, info.planInfo.GetJsonPath())
			assert.Equal(t, schemapb.DataType_VarChar, info.planInfo.GetJsonType())
			assert.True(t, info.planInfo.GetStrictCast())
		})
	}
}

func TestConvertHybridSearchToSearchKeepsNamespace(t *testing.T) {
	namespace := "tenant_a"

	searchReq := ConvertHybridSearchToSearch(&milvuspb.HybridSearchRequest{
		CollectionName: "coll",
		Namespace:      &namespace,
		Requests: []*milvuspb.SearchRequest{
			{Nq: 1},
		},
	})

	assert.Equal(t, namespace, searchReq.GetNamespace())
}

func TestConvertHybridSearchToSearchKeepsFunctionChains(t *testing.T) {
	functionChains := []*schemapb.FunctionChain{
		{
			Name:  "hybrid_rerank",
			Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank,
			Ops: []*schemapb.FunctionChainOp{
				{Op: "merge"},
				{Op: "limit"},
			},
		},
	}

	searchReq := ConvertHybridSearchToSearch(&milvuspb.HybridSearchRequest{
		CollectionName: "coll",
		FunctionChains: functionChains,
		Requests: []*milvuspb.SearchRequest{
			{Nq: 1},
		},
	})

	assert.Equal(t, functionChains, searchReq.GetFunctionChains())
}

func TestConvertHybridSearchToSearchKeepsSubSearchFunctionChainsByIndex(t *testing.T) {
	l0Chain := &schemapb.FunctionChain{
		Stage: schemapb.FunctionChainStage_FunctionChainStageL0Rerank,
		Ops:   []*schemapb.FunctionChainOp{{Op: "map"}},
	}
	l1Chain := &schemapb.FunctionChain{
		Stage: schemapb.FunctionChainStage_FunctionChainStageL1Rerank,
		Ops:   []*schemapb.FunctionChainOp{{Op: "limit"}},
	}

	searchReq := ConvertHybridSearchToSearch(&milvuspb.HybridSearchRequest{
		CollectionName: "coll",
		Requests: []*milvuspb.SearchRequest{
			{Nq: 1, FunctionChains: []*schemapb.FunctionChain{l0Chain}},
			{Nq: 1},
			{Nq: 1, FunctionChains: []*schemapb.FunctionChain{l1Chain}},
		},
	})

	require.Len(t, searchReq.GetSubReqs(), 3)
	assert.Equal(t, []*schemapb.FunctionChain{l0Chain}, searchReq.GetSubReqs()[0].GetFunctionChains())
	assert.Empty(t, searchReq.GetSubReqs()[1].GetFunctionChains())
	assert.Equal(t, []*schemapb.FunctionChain{l1Chain}, searchReq.GetSubReqs()[2].GetFunctionChains())
}

func TestSubSearchRequestFunctionChainsWireRoundTrip(t *testing.T) {
	chain := &schemapb.FunctionChain{
		Name:  "sub_rerank",
		Stage: schemapb.FunctionChainStage_FunctionChainStageL1Rerank,
		Ops: []*schemapb.FunctionChainOp{
			{
				Op: "limit",
				Params: map[string]*schemapb.FunctionParamValue{
					"limit": {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 5}},
				},
			},
		},
	}
	original := &milvuspb.SubSearchRequest{
		Nq:             1,
		FunctionChains: []*schemapb.FunctionChain{chain},
	}

	field := original.ProtoReflect().Descriptor().Fields().ByName("function_chains")
	require.NotNil(t, field)
	assert.Equal(t, int32(8), int32(field.Number()))

	wire, err := proto.Marshal(original)
	require.NoError(t, err)

	roundTrip := &milvuspb.SubSearchRequest{}
	require.NoError(t, proto.Unmarshal(wire, roundTrip))
	require.Len(t, roundTrip.GetFunctionChains(), 1)
	assert.True(t, proto.Equal(chain, roundTrip.GetFunctionChains()[0]))
}

func TestConvertHybridSearchToSearchUsesTopLevelRLS(t *testing.T) {
	searchReq := ConvertHybridSearchToSearch(&milvuspb.HybridSearchRequest{
		RlsPrincipal: "top-level-principal",
		SkipRls:      true,
		Requests: []*milvuspb.SearchRequest{
			{
				RlsPrincipal: "nested-principal",
				SkipRls:      false,
			},
		},
	})

	assert.Equal(t, "top-level-principal", searchReq.GetRlsPrincipal())
	assert.True(t, searchReq.GetSkipRls())
}

func TestConvertHybridSearchToSearchDoesNotFallbackToNestedRLS(t *testing.T) {
	searchReq := ConvertHybridSearchToSearch(&milvuspb.HybridSearchRequest{
		Requests: []*milvuspb.SearchRequest{
			{
				RlsPrincipal: "nested-principal",
				SkipRls:      true,
			},
		},
	})

	assert.Empty(t, searchReq.GetRlsPrincipal())
	assert.False(t, searchReq.GetSkipRls())
}
