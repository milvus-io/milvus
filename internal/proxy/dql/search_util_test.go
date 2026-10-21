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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/search_agg"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSearchTaskCheckNqMalformedPlaceholder(t *testing.T) {
	for _, advanced := range []bool{false, true} {
		name := "search"
		if advanced {
			name = "hybrid search"
		}
		t.Run(name, func(t *testing.T) {
			request := &milvuspb.SearchRequest{
				SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: []byte{0xff}},
			}
			if advanced {
				request = ConvertHybridSearchToSearch(&milvuspb.HybridSearchRequest{
					Requests: []*milvuspb.SearchRequest{request},
				})
			}
			task := &SearchTask{
				SearchRequest: &internalpb.SearchRequest{IsAdvanced: advanced},
				request:       request,
			}
			_, err := task.checkNq(context.Background())
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			assert.ErrorIs(t, err, proto.Error)
			status := merr.Status(err)
			assert.Equal(t, int32(1100), status.GetCode())
			assert.Equal(t, "true", status.GetExtraInfo()[merr.InputErrorFlagKey])
			assert.False(t, status.GetRetriable())
			assert.Contains(t, status.GetReason(), "placeholder group")
			assert.Contains(t, status.GetReason(), "proto:")
		})
	}
}

func TestSearchTaskCheckNqCompatibility(t *testing.T) {
	placeholder, err := proto.Marshal(&commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			{Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: [][]byte{{0, 0, 0, 0}, {0, 0, 0, 0}}},
		},
	})
	require.NoError(t, err)
	for _, tc := range []struct {
		name     string
		advanced bool
		nq       int64
	}{
		{name: "search infers nq"},
		{name: "search explicit nq", nq: 2},
		{name: "hybrid infers nq", advanced: true},
		{name: "hybrid explicit nq", advanced: true, nq: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := &milvuspb.SearchRequest{
				Nq:          tc.nq,
				SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: placeholder},
			}
			if tc.advanced {
				request = ConvertHybridSearchToSearch(&milvuspb.HybridSearchRequest{
					Requests: []*milvuspb.SearchRequest{request},
				})
			}
			task := &SearchTask{
				SearchRequest: &internalpb.SearchRequest{IsAdvanced: tc.advanced},
				request:       request,
			}
			nq, err := task.checkNq(context.Background())
			require.NoError(t, err)
			assert.Equal(t, int64(2), nq)
			assert.Equal(t, nq, task.request.GetNq())
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
	// an unknown name still falls through to the dynamic field
	_, jsonPath, err := parseGroupByField("free_key", schema)
	assert.NoError(t, err)
	assert.Equal(t, "free_key", jsonPath)

	// DataType_String has no case in the executor's switch, unlike VarChar
	schema.Fields = append(schema.Fields,
		&schemapb.FieldSchema{FieldID: 109, Name: "legacy_str", DataType: schemapb.DataType_String})

	// The wording is what clients read, so it is pinned: the binary-vector case
	// keeps the executor's own sentence, everything else says why plainly.
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

// The JSON group-by attributes resolved by parseGroupByInfo used to be
// dropped when parseRankParams packed the rank params, so a hybrid group-by
// on a JSON key grouped by the whole field instead of the key.
func TestRankParamsCarryJSONGroupAttributes(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "meta", DataType: schemapb.DataType_JSON},
			{FieldID: 102, Name: "tag", DataType: schemapb.DataType_VarChar},
		},
	}
	pairs := []*commonpb.KeyValuePair{
		{Key: LimitKey, Value: "10"},
		{Key: GroupByFieldKey, Value: `meta["brand"]`},
	}
	parsed, err := parseRankParams(pairs, schema, false)
	assert.NoError(t, err)
	assert.Equal(t, "/brand", parsed.GetJSONPath())

	searchInfo, err := parseSearchInfo(getValidSearchParams(), schema, parsed, false)
	assert.NoError(t, err)
	assert.Equal(t, "/brand", searchInfo.planInfo.GetJsonPath())
}

func TestSearchInfoDetermineSearchTypeWithPluralGroupByFieldIDs(t *testing.T) {
	info := &SearchInfo{
		planInfo: &planpb.QueryInfo{
			GroupByFieldIds: []int64{101},
		},
	}

	assert.Equal(t, internalpb.SearchType_DEFAULT, info.DetermineSearchType(false))
}

func TestSearchInfoDetermineSearchTypeWithOrderBy(t *testing.T) {
	info := &SearchInfo{
		planInfo:      &planpb.QueryInfo{},
		orderByFields: []OrderByField{{FieldName: "price"}},
	}

	assert.Equal(t, internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER, info.DetermineSearchType(false))
}

func TestSearchTaskSupportsStreamingReduce(t *testing.T) {
	plain := func() *SearchTask {
		return &SearchTask{SearchRequest: &internalpb.SearchRequest{
			SearchType: internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER,
		}}
	}

	tests := []struct {
		name string
		task *SearchTask
		want bool
	}{
		{name: "plain ANN", task: plain(), want: true},
		{name: "filtered plain ANN", task: &SearchTask{SearchRequest: &internalpb.SearchRequest{SearchType: internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER}}, want: true},
		{name: "iterator", task: &SearchTask{SearchRequest: &internalpb.SearchRequest{IsIterator: true}}, want: true},
		{name: "default search type", task: &SearchTask{SearchRequest: &internalpb.SearchRequest{}}, want: false},
		{name: "advanced search", task: &SearchTask{SearchRequest: &internalpb.SearchRequest{IsAdvanced: true}}, want: false},
		{name: "sub requests", task: &SearchTask{SearchRequest: &internalpb.SearchRequest{SubReqs: []*internalpb.SubSearchRequest{{}}}}, want: false},
		{name: "group by", task: &SearchTask{SearchRequest: &internalpb.SearchRequest{GroupByFieldId: 100}}, want: true},
		{name: "multi-field group by", task: &SearchTask{SearchRequest: &internalpb.SearchRequest{GroupByFieldIds: []int64{100, 101}}}, want: true},
		{name: "group by iterator", task: &SearchTask{SearchRequest: &internalpb.SearchRequest{GroupByFieldId: 100, IsIterator: true}}, want: false},
		{name: "search aggregation", task: &SearchTask{SearchRequest: &internalpb.SearchRequest{SearchType: internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER}, aggCtx: &search_agg.SearchAggregationContext{}}, want: false},
		{name: "order by", task: &SearchTask{SearchRequest: &internalpb.SearchRequest{SearchType: internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER}, orderByFields: []OrderByField{{FieldName: "price"}}}, want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, test.task.supportsStreamingReduce())
		})
	}
}

func TestParseGroupByInfoLegacyFieldPrecedence(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 101, Name: "brand", DataType: schemapb.DataType_VarChar},
		{FieldID: 102, Name: "category", DataType: schemapb.DataType_VarChar},
	}}

	info, err := parseGroupByInfo([]*commonpb.KeyValuePair{
		{Key: GroupByFieldKey, Value: "brand"},
		{Key: GroupByFieldsKey, Value: "category"},
	}, schema)
	assert.NoError(t, err)
	assert.Equal(t, []int64{101}, info.groupByFieldIds)
	assert.Equal(t, []string{"brand"}, info.groupByFieldNames)

	info, err = parseGroupByInfo([]*commonpb.KeyValuePair{
		{Key: GroupByFieldKey, Value: " "},
		{Key: GroupByFieldsKey, Value: "brand, category"},
	}, schema)
	assert.NoError(t, err)
	assert.Equal(t, []int64{101, 102}, info.groupByFieldIds)
	assert.Equal(t, []string{"brand", "category"}, info.groupByFieldNames)
}
