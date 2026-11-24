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
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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

func TestSearchIteratorV2CursorRoundTrip(t *testing.T) {
	t.Run("int64 row cursor", func(t *testing.T) {
		res := &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{10, 42}},
				},
			},
		}
		hasPK, pkBytes, elemIdx := lastPageCursorState(res)
		require.True(t, hasPK)
		assert.Nil(t, elemIdx)
		token := encodeSearchIterCursor(1.25, schemapb.DataType_Int64, hasPK, pkBytes, elemIdx)

		var topK int64
		info, err := parseSearchIteratorV2Info([]*commonpb.KeyValuePair{
			{Key: SearchIterV2Key, Value: "true"},
			{Key: SearchIterIdKey, Value: token},
			{Key: SearchIterBatchSizeKey, Value: "4"},
		}, 0, true, 0, &topK, false)
		require.NoError(t, err)
		assert.Equal(t, float32(1.25), info.GetLastBound())
		assert.Equal(t, int64ToBigEndianBytes(42), info.GetLastPrimaryKey())
		assert.Equal(t, int32(schemapb.DataType_Int64), info.GetPrimaryKeyType())
		assert.Nil(t, info.LastElementIndex)
	})

	t.Run("empty string varchar pk preserves hasPK", func(t *testing.T) {
		res := &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{Data: []string{""}},
				},
			},
		}
		hasPK, pkBytes, elemIdx := lastPageCursorState(res)
		require.True(t, hasPK)
		assert.Empty(t, pkBytes)
		assert.Nil(t, elemIdx)
		token := encodeSearchIterCursor(0.5, schemapb.DataType_VarChar, hasPK, pkBytes, elemIdx)

		var topK int64
		info, err := parseSearchIteratorV2Info([]*commonpb.KeyValuePair{
			{Key: SearchIterV2Key, Value: "true"},
			{Key: SearchIterIdKey, Value: token},
			{Key: SearchIterBatchSizeKey, Value: "2"},
		}, 0, true, 0, &topK, false)
		require.NoError(t, err)
		assert.NotNil(t, info.LastPrimaryKey)
		assert.Empty(t, info.GetLastPrimaryKey())
		assert.Equal(t, int32(schemapb.DataType_VarChar), info.GetPrimaryKeyType())
	})

	t.Run("empty page omits primary key", func(t *testing.T) {
		hasPK, pkBytes, elemIdx := lastPageCursorState(&schemapb.SearchResultData{})
		assert.False(t, hasPK)
		assert.Nil(t, pkBytes)
		assert.Nil(t, elemIdx)
		token := encodeSearchIterCursor(2.0, schemapb.DataType_VarChar, hasPK, pkBytes, elemIdx)

		var topK int64
		info, err := parseSearchIteratorV2Info([]*commonpb.KeyValuePair{
			{Key: SearchIterV2Key, Value: "true"},
			{Key: SearchIterIdKey, Value: token},
			{Key: SearchIterBatchSizeKey, Value: "2"},
		}, 0, true, 0, &topK, false)
		require.NoError(t, err)
		assert.Equal(t, float32(2.0), info.GetLastBound())
		assert.Nil(t, info.LastPrimaryKey)
		assert.Nil(t, info.PrimaryKeyType)
		assert.Nil(t, info.LastElementIndex)
	})

	t.Run("element-level cursor carries element index", func(t *testing.T) {
		res := &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{5, 5}},
				},
			},
			ElementIndices: &schemapb.LongArray{Data: []int64{0, 2}},
		}
		hasPK, pkBytes, elemIdx := lastPageCursorState(res)
		require.True(t, hasPK)
		require.NotNil(t, elemIdx)
		assert.Equal(t, int32(2), *elemIdx)
		token := encodeSearchIterCursor(1.0, schemapb.DataType_Int64, hasPK, pkBytes, elemIdx)

		var topK int64
		info, err := parseSearchIteratorV2Info([]*commonpb.KeyValuePair{
			{Key: SearchIterV2Key, Value: "true"},
			{Key: SearchIterIdKey, Value: token},
			{Key: SearchIterBatchSizeKey, Value: "2"},
		}, 0, true, 0, &topK, false)
		require.NoError(t, err)
		assert.Equal(t, int64ToBigEndianBytes(5), info.GetLastPrimaryKey())
		require.NotNil(t, info.LastElementIndex)
		assert.Equal(t, int32(2), info.GetLastElementIndex())
	})

	t.Run("legacy uuid token fallback", func(t *testing.T) {
		var topK int64
		info, err := parseSearchIteratorV2Info([]*commonpb.KeyValuePair{
			{Key: SearchIterV2Key, Value: "true"},
			{Key: SearchIterIdKey, Value: "123e4567-e89b-12d3-a456-426614174000"},
			{Key: SearchIterBatchSizeKey, Value: "3"},
			{Key: SearchIterLastBoundKey, Value: "1.5"},
		}, 0, true, 0, &topK, false)
		require.NoError(t, err)
		assert.Equal(t, float32(1.5), info.GetLastBound())
		assert.Nil(t, info.LastPrimaryKey)
		assert.Nil(t, info.LastElementIndex)
	})

	t.Run("CompareSearchResultTieKey orders by pk then element index", func(t *testing.T) {
		a := &schemapb.SearchResultData{
			Ids:            &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{5, 10}}}},
			ElementIndices: &schemapb.LongArray{Data: []int64{2, 0}},
		}
		b := &schemapb.SearchResultData{
			Ids:            &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{5, 5}}}},
			ElementIndices: &schemapb.LongArray{Data: []int64{1, 3}},
		}
		assert.False(t, typeutil.CompareSearchResultTieKey(a, 0, b, 0)) // (5,2) > (5,1)
		assert.True(t, typeutil.CompareSearchResultTieKey(a, 0, b, 1))  // (5,2) < (5,3)
		assert.False(t, typeutil.CompareSearchResultTieKey(a, 1, b, 1)) // (10,0) > (5,3)
	})
}
