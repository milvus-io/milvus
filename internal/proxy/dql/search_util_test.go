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
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

// A declared nq that disagrees with the placeholder group is refused by the
// Proxy; it used to be forwarded and fail on a query node.
func TestSearchTaskCheckNqMismatch(t *testing.T) {
	placeholder, err := proto.Marshal(&commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			{Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: [][]byte{{0, 0, 0, 0}, {0, 0, 0, 0}}},
		},
	})
	require.NoError(t, err)
	for _, advanced := range []bool{false, true} {
		for _, nq := range []int64{1, 3} {
			request := &milvuspb.SearchRequest{
				Nq:          nq,
				SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: placeholder},
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
			require.ErrorIs(t, err, merr.ErrParameterInvalid, "advanced=%v nq=%d", advanced, nq)
			assert.Contains(t, err.Error(), "does not match the 2 query rows")
		}
	}

	// A group the row counter refuses is judged by proto.Unmarshal, with its
	// error: the counter alone never turns a request away.
	invalidTag := []byte{0x0a, 0x03, 0x0a, 0x01, 0xff}
	_, err = resolveNq(1, invalidTag, "failed to unmarshal placeholder group")
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.ErrorIs(t, err, proto.Error)
	_, _, convertErr := ConvertPlaceholderGroup(invalidTag, &schemapb.FieldSchema{DataType: schemapb.DataType_FloatVector})
	require.Error(t, convertErr, "the pre-existing decode refuses the same bytes")

	// Nothing to compare a declared nq with: left to the query nodes, as before.
	nq, err := resolveNq(4, nil, "unused")
	require.NoError(t, err)
	assert.Equal(t, int64(4), nq)
}

func TestNormalizeMultiSearchNQ(t *testing.T) {
	limit := &paramtable.Get().QuotaConfig.NQLimit
	previous := limit.SwapTempValue("2")
	t.Cleanup(func() { limit.SwapTempValue(previous) })

	placeholderInput := func(values ...[]byte) *milvuspb.SearchRequest_PlaceholderGroup {
		group, err := proto.Marshal(&commonpb.PlaceholderGroup{
			Placeholders: []*commonpb.PlaceholderValue{
				{Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: values},
			},
		})
		require.NoError(t, err)
		return &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: group}
	}
	idInput := func(ids ...int64) *milvuspb.SearchRequest_Ids {
		return &milvuspb.SearchRequest_Ids{Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}},
		}}
	}

	row := []byte{0, 0, 0, 0}
	twoRows := placeholderInput(row, row)
	malformed := &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: []byte{0xff}}
	for _, tc := range []struct {
		name      string
		request   *milvuspb.SearchRequest
		wantNq    int64
		wantError string
		wantCause error
	}{
		{name: "infer nq", request: &milvuspb.SearchRequest{SearchInput: twoRows}, wantNq: 2},
		{name: "matching nq", request: &milvuspb.SearchRequest{Nq: 2, SearchInput: twoRows}, wantNq: 2},
		{
			name: "mismatched nq", request: &milvuspb.SearchRequest{Nq: 1, SearchInput: twoRows},
			wantError: "nq 1 does not match the 2 query rows in the placeholder group",
		},
		{name: "negative nq", request: &milvuspb.SearchRequest{Nq: -1, SearchInput: twoRows}, wantError: "nq must be nonnegative"},
		{name: "hybrid", request: &milvuspb.SearchRequest{
			SearchInput: twoRows, SubReqs: []*milvuspb.SubSearchRequest{{PlaceholderGroup: twoRows.PlaceholderGroup}},
		}, wantError: "MultiSearch supports only ordinary Search requests"},
		{name: "empty input", request: &milvuspb.SearchRequest{}, wantError: "should be in range [1, 2], but got 0"},
		{name: "empty group with declared nq", request: &milvuspb.SearchRequest{
			Nq: 1, SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{},
		}, wantError: "should be in range [1, 2], but got 0"},
		{
			name: "placeholder without rows", request: &milvuspb.SearchRequest{Nq: 1, SearchInput: placeholderInput()},
			wantError: "should be in range [1, 2], but got 0",
		},
		{
			name: "malformed group", request: &milvuspb.SearchRequest{SearchInput: malformed},
			wantError: "failed to unmarshal placeholder group", wantCause: proto.Error,
		},
		{
			name: "malformed group with declared nq", request: &milvuspb.SearchRequest{Nq: 1, SearchInput: malformed},
			wantError: "failed to unmarshal placeholder group", wantCause: proto.Error,
		},
		{
			name: "too many rows", request: &milvuspb.SearchRequest{SearchInput: placeholderInput(row, row, row)},
			wantError: "should be in range [1, 2], but got 3",
		},
		{name: "charge by integer ID count", request: &milvuspb.SearchRequest{Nq: 1, SearchInput: idInput(10, 20)}, wantNq: 2},
		{name: "charge by string ID count", request: &milvuspb.SearchRequest{SearchInput: &milvuspb.SearchRequest_Ids{
			Ids: &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"a", "b"}}}},
		}}, wantNq: 2},
		{
			name: "empty IDs", request: &milvuspb.SearchRequest{Nq: 1, SearchInput: idInput()},
			wantError: "search by primary key requires at least one ID",
		},
		{
			name: "too many IDs", request: &milvuspb.SearchRequest{Nq: 1, SearchInput: idInput(10, 20, 30)},
			wantError: "should be in range [1, 2], but got 3",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			declared := tc.request.GetNq()
			err := NormalizeMultiSearchNQ(tc.request)
			if tc.wantError == "" {
				require.NoError(t, err)
				require.Equal(t, tc.wantNq, tc.request.GetNq())
				return
			}
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.ErrorContains(t, err, tc.wantError)
			if tc.wantCause != nil {
				require.ErrorIs(t, err, tc.wantCause)
			}
			require.Equal(t, declared, tc.request.GetNq())
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
