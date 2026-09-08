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

package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The search path marks features into the task's set and flushes once; these
// wrappers give a single call the same "hit now" shape the assertions below
// were written against.
func recordSearchRequestFeatures(req *milvuspb.SearchRequest) {
	hitViaSet(func(set *featureusage.FeatureSet) { collectSearchRequestFeatures(req, set) })
}

func recordSearchInfoFeatures(isIterator, isRangeSearch bool, groupByFieldID int64, isIteratorV2 bool) {
	hitViaSet(func(set *featureusage.FeatureSet) {
		collectSearchInfoFeatures(isIterator, isRangeSearch, groupByFieldID, isIteratorV2, set)
	})
}

func recordLegacyRankStrategy(params []*commonpb.KeyValuePair) {
	hitViaSet(func(set *featureusage.FeatureSet) { collectLegacyRankStrategy(params, set) })
}

func recordSearchParamKeyFeatures(params []*commonpb.KeyValuePair) {
	hitViaSet(func(set *featureusage.FeatureSet) { collectSearchParamKeyFeatures(params, set) })
}

func recordOutputDynamicField(userDynamicFields []string) {
	recordOutputFieldFeatures(userDynamicFields, nil, nil)
}

func recordOutputVectorField(translatedOutputFields []string, schema *schemaInfo) {
	recordOutputFieldFeatures(nil, translatedOutputFields, schema)
}

func hitViaSet(collect func(*featureusage.FeatureSet)) {
	if !featureusage.Enabled() {
		return
	}
	var set featureusage.FeatureSet
	collect(&set)
	set.HitAll()
}

// idsOf builds the primary-key ids a search-by-primary-key request carries.
func idsOf(ids ...int64) *milvuspb.SearchRequest_Ids {
	return &milvuspb.SearchRequest_Ids{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}},
	}
}

// counterSnapshot reads the process-wide request counters by name.
func counterSnapshot() map[string]int64 {
	out := make(map[string]int64)
	for _, e := range featureusage.Snapshot() {
		out[e.Name] = e.Value
	}
	return out
}

// delta returns after[name] - before[name] for every counter, dropping zeros,
// so a test states exactly which counters moved.
func delta(before, after map[string]int64) map[string]int64 {
	out := make(map[string]int64)
	for name, v := range after {
		if d := v - before[name]; d != 0 {
			out[name] = d
		}
	}
	return out
}

func TestFeatureUsageHooks(t *testing.T) {
	featureusage.SetEnabled(true)
	t.Cleanup(func() { featureusage.SetEnabled(true) })
	ns := "tenant-a"

	t.Run("common fields on default request move nothing", func(t *testing.T) {
		before := counterSnapshot()
		// This is what pymilvus sends by default: default consistency,
		// guarantee_timestamp populated, no namespace, meta returned.
		recordCommonRequestFeatures(&milvuspb.QueryRequest{UseDefaultConsistency: true, GuaranteeTimestamp: 12345})
		assert.Empty(t, delta(before, counterSnapshot()))
	})

	t.Run("common fields on effective values", func(t *testing.T) {
		before := counterSnapshot()
		recordCommonRequestFeatures(&milvuspb.QueryRequest{
			UseDefaultConsistency: false,
			ConsistencyLevel:      commonpb.ConsistencyLevel_Strong,
			NotReturnAllMeta:      true,
			TravelTimestamp:       5,
			Namespace:             &ns,
		})
		assert.Equal(t, map[string]int64{
			"consistency_level=Strong":    1,
			"not_return_all_meta":         1,
			"deprecated_travel_timestamp": 1,
			"namespace":                   1,
		}, delta(before, counterSnapshot()))
	})

	t.Run("unknown consistency level creates no slot", func(t *testing.T) {
		before := counterSnapshot()
		recordCommonRequestFeatures(&milvuspb.QueryRequest{UseDefaultConsistency: false, ConsistencyLevel: commonpb.ConsistencyLevel(99)})
		assert.Empty(t, delta(before, counterSnapshot()))
		assert.Equal(t, featureusage.NumFeatures(), len(counterSnapshot()))
	})

	t.Run("search-only fields, function score fold, highlighter type", func(t *testing.T) {
		before := counterSnapshot()
		recordSearchRequestFeatures(&milvuspb.SearchRequest{
			UseDefaultConsistency: true,

			FunctionScore: &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{
				{Params: []*commonpb.KeyValuePair{{Key: "reranker", Value: "RRF"}}},
				{Params: []*commonpb.KeyValuePair{{Key: "reranker", Value: "decay"}}},
				{Params: []*commonpb.KeyValuePair{{Key: "reranker", Value: "whatever the client sent"}}},
				{Params: []*commonpb.KeyValuePair{{Key: "reranker", Value: "another unknown"}}},
			}},
			Highlighter: &commonpb.Highlighter{Type: commonpb.HighlightType_Semantic},
		})
		assert.Equal(t, map[string]int64{
			"function_score=rrf":   1,
			"function_score=decay": 1,
			// Two unknown rerankers in one request are one request that used an
			// unknown reranker: the counters answer "how many requests used X".
			"function_score=_other": 1,
			"highlighter=Semantic":  1,
		}, delta(before, counterSnapshot()))
	})

	t.Run("search params", func(t *testing.T) {
		before := counterSnapshot()
		recordSearchInfoFeatures(true, true, 100, true) // v2 iterator also sends iterator=True
		recordSearchInfoFeatures(true, false, 0, false) // old protocol
		recordSearchInfoFeatures(false, false, 0, false)
		assert.Equal(t, map[string]int64{
			"iterator":       1,
			"search_iter_v2": 1,
			"radius":         1,
			"group_by_field": 1,
		}, delta(before, counterSnapshot()))
	})

	t.Run("legacy rank strategy", func(t *testing.T) {
		before := counterSnapshot()
		recordLegacyRankStrategy([]*commonpb.KeyValuePair{{Key: RankTypeKey, Value: "rrf"}})
		recordLegacyRankStrategy([]*commonpb.KeyValuePair{{Key: RankTypeKey, Value: "weighted"}})
		recordLegacyRankStrategy([]*commonpb.KeyValuePair{{Key: RankTypeKey, Value: "made up"}})
		recordLegacyRankStrategy([]*commonpb.KeyValuePair{{Key: "limit", Value: "10"}}) // no strategy key
		assert.Equal(t, map[string]int64{
			"strategy=rrf":      1,
			"strategy=weighted": 1,
			"strategy=_other":   1,
		}, delta(before, counterSnapshot()))
	})

	t.Run("query iterator", func(t *testing.T) {
		before := counterSnapshot()
		recordQueryIteratorFeature(true)
		recordQueryIteratorFeature(false)
		assert.Equal(t, map[string]int64{"iterator": 1}, delta(before, counterSnapshot()))
	})

	t.Run("plan predicates are walked for search, query and delete plans", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "s", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "64"}, {Key: "enable_match", Value: "true"}, {Key: "enable_analyzer", Value: "true"}}},
			{FieldID: 102, Name: "j", DataType: schemapb.DataType_JSON},
			{FieldID: 103, Name: "v", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
		}}
		helper, err := typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		before := counterSnapshot()
		queryPlan, err := planparserv2.CreateRetrievePlan(helper, `text_match(s, "x") and j["a"] > 1`, nil)
		require.NoError(t, err)
		recordPlanExprFeatures(queryPlan, map[string]*schemapb.TemplateValue{"x": {}})

		searchPlan, err := planparserv2.CreateSearchPlan(helper, `s like "a%"`, "v", &planpb.QueryInfo{Topk: 10, MetricType: "L2", SearchParams: "{}"}, nil, nil)
		require.NoError(t, err)
		recordPlanExprFeatures(searchPlan, nil)

		recordPlanExprFeatures(nil, nil)
		recordPlanExprFeatures(&planpb.PlanNode{}, nil)
		assert.Equal(t, map[string]int64{
			"text_match":           1,
			"json_identifier":      1,
			"expr_template_values": 1,
			"like":                 1,
		}, delta(before, counterSnapshot()))
	})

	t.Run("search_params keys by presence, ignore_growing by value", func(t *testing.T) {
		before := counterSnapshot()
		recordSearchParamKeyFeatures([]*commonpb.KeyValuePair{
			{Key: GroupSizeKey, Value: "3"},
			{Key: StrictGroupSize, Value: "true"},
			{Key: RankGroupScorer, Value: "max"},
			{Key: "hints", Value: "iterative_filter"},
			{Key: AnalyzerKey, Value: "en"},
			{Key: IgnoreGrowingKey, Value: "False"}, // pymilvus default: must not count
			{Key: "topk", Value: "10"},
		})
		recordSearchParamKeyFeatures([]*commonpb.KeyValuePair{{Key: IgnoreGrowingKey, Value: "true"}})
		assert.Equal(t, map[string]int64{
			"group_size": 1, "strict_group_size": 1, "rank_group_scorer": 1, "hints": 1, "analyzer_name": 1, "ignore_growing": 1,
		}, delta(before, counterSnapshot()))
	})

	t.Run("query_params keys", func(t *testing.T) {
		before := counterSnapshot()
		recordQueryParamKeyFeatures([]*commonpb.KeyValuePair{{Key: GroupByFieldsKey, Value: "a,b"}, {Key: IgnoreGrowingKey, Value: "False"}})
		recordQueryParamKeyFeatures([]*commonpb.KeyValuePair{{Key: IgnoreGrowingKey, Value: "True"}})
		assert.Equal(t, map[string]int64{"group_by_fields": 1, "ignore_growing": 1}, delta(before, counterSnapshot()))
	})

	t.Run("output dynamic field, norm_score, highlighter fragments", func(t *testing.T) {
		before := counterSnapshot()
		recordOutputDynamicField(nil)
		recordOutputDynamicField([]string{"extra"})
		// norm_score lives inside the params JSON, which is where
		// convertLegacyParams reads it from; a top-level key is dropped there.
		recordLegacyRankStrategy([]*commonpb.KeyValuePair{
			{Key: RankTypeKey, Value: "weighted"},
			{Key: ParamsKey, Value: `{"weights": [0.1, 0.9], "norm_score": true}`},
		})
		recordSearchRequestFeatures(&milvuspb.SearchRequest{UseDefaultConsistency: true, Highlighter: &commonpb.Highlighter{
			Type:   commonpb.HighlightType_Lexical,
			Params: []*commonpb.KeyValuePair{{Key: FragmentSizeKey, Value: "50"}, {Key: FragmentNumKey, Value: "2"}},
		}})
		assert.Equal(t, map[string]int64{
			"output_dynamic_field": 1, "strategy=weighted": 1, "norm_score": 1,
			"highlighter=Lexical": 1, "fragment_size": 1, "num_of_fragments": 1,
		}, delta(before, counterSnapshot()))
	})

	t.Run("norm_score only counts the shape that takes effect", func(t *testing.T) {
		cases := []struct {
			name   string
			params []*commonpb.KeyValuePair
			want   map[string]int64
		}{
			{
				name: "top level norm_score is dropped by convertLegacyParams",
				params: []*commonpb.KeyValuePair{
					{Key: RankTypeKey, Value: "weighted"},
					{Key: NormScoreKey, Value: "true"},
				},
				want: map[string]int64{"strategy=weighted": 1},
			},
			{
				name: "norm_score false is not a use of normalization",
				params: []*commonpb.KeyValuePair{
					{Key: RankTypeKey, Value: "weighted"},
					{Key: ParamsKey, Value: `{"norm_score": false}`},
				},
				want: map[string]int64{"strategy=weighted": 1},
			},
			{
				name: "the SDKs also send it as a string",
				params: []*commonpb.KeyValuePair{
					{Key: RankTypeKey, Value: "weighted"},
					{Key: ParamsKey, Value: `{"norm_score": "True"}`},
				},
				want: map[string]int64{"strategy=weighted": 1, "norm_score": 1},
			},
			{
				name: "malformed params json counts the strategy only",
				params: []*commonpb.KeyValuePair{
					{Key: RankTypeKey, Value: "rrf"},
					{Key: ParamsKey, Value: "not json"},
				},
				want: map[string]int64{"strategy=rrf": 1},
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				before := counterSnapshot()
				recordLegacyRankStrategy(tc.params)
				assert.Equal(t, tc.want, delta(before, counterSnapshot()))
			})
		}
	})

	t.Run("norm_score on the function_score path", func(t *testing.T) {
		before := counterSnapshot()
		recordSearchRequestFeatures(&milvuspb.SearchRequest{
			UseDefaultConsistency: true,
			FunctionScore: &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{{
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "weighted"},
					{Key: NormScoreKey, Value: "true"},
				},
			}}},
		})
		assert.Equal(t, map[string]int64{"function_score=weighted": 1, "norm_score": 1},
			delta(before, counterSnapshot()))
	})

	t.Run("search by primary keys is not decided from the request here", func(t *testing.T) {
		// Neither shape counts at this hook. The deprecated
		// search_by_primary_keys bool is never set by anything, and by the time
		// a search task exists handleIfSearchByPK has already replaced the ids
		// in search_input with the resolved placeholder group. searchTask
		// carries searchedByPrimaryKeys, captured before that rewrite, and
		// tests/integration/featureusage drives the real request.
		for _, req := range []*milvuspb.SearchRequest{
			{UseDefaultConsistency: true, SearchByPrimaryKeys: true},
			{UseDefaultConsistency: true, SearchInput: idsOf(1)},
		} {
			before := counterSnapshot()
			recordSearchRequestFeatures(req)
			assert.Empty(t, delta(before, counterSnapshot()))
		}
	})

	t.Run("output vector field", func(t *testing.T) {
		schema, err := newSchemaInfo(&schemapb.CollectionSchema{
			Name: "coll",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "title", DataType: schemapb.DataType_VarChar},
				{FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector},
			},
			StructArrayFields: []*schemapb.StructArrayFieldSchema{{
				FieldID: 200, Name: "chunks",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 201, Name: "chunk_text", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
					{FieldID: 202, Name: "chunk_vec", DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector},
				},
			}},
		})
		require.NoError(t, err)

		cases := []struct {
			name         string
			outputFields []string
			want         int64
		}{
			{name: "no output fields", outputFields: nil},
			{name: "scalar only", outputFields: []string{"pk", "title"}},
			{name: "top level vector", outputFields: []string{"pk", "vec"}, want: 1},
			{name: "struct sub vector", outputFields: []string{"chunks", "chunk_vec"}, want: 1},
			{name: "struct scalar sub field only", outputFields: []string{"chunk_text"}},
			// A name that is not in the schema cannot be a vector field: the
			// hook must not fall back to counting on length alone.
			{name: "unknown field name", outputFields: []string{"vec_not_really"}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				before := counterSnapshot()
				recordOutputVectorField(tc.outputFields, schema)
				want := map[string]int64{}
				if tc.want != 0 {
					want["output_vector_field"] = tc.want
				}
				assert.Equal(t, want, delta(before, counterSnapshot()))
			})
		}

		t.Run("a request with two vector output fields counts once", func(t *testing.T) {
			before := counterSnapshot()
			recordOutputVectorField([]string{"vec", "chunk_vec"}, schema)
			assert.Equal(t, map[string]int64{"output_vector_field": 1}, delta(before, counterSnapshot()))
		})

		t.Run("nil schema", func(t *testing.T) {
			before := counterSnapshot()
			recordOutputVectorField([]string{"vec"}, nil)
			assert.Empty(t, delta(before, counterSnapshot()))
		})
	})

	t.Run("disabled counters move nothing", func(t *testing.T) {
		featureusage.SetEnabled(false)
		defer featureusage.SetEnabled(true)
		before := counterSnapshot()
		recordSearchRequestFeatures(&milvuspb.SearchRequest{SearchInput: idsOf(1, 2), Namespace: &ns})
		recordSearchInfoFeatures(true, true, 1, true)
		recordQueryIteratorFeature(true)
		recordOutputVectorField([]string{"vec"}, nil)
		assert.Empty(t, delta(before, counterSnapshot()))
	})
}
