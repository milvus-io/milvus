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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	chaintypes "github.com/milvus-io/milvus/internal/util/function/chain/types"
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
		collectSearchInfoFeatures(isIterator, isRangeSearch, groupByFieldID, isIteratorV2, false, set)
	})
}

func recordLegacyRankStrategy(params []*commonpb.KeyValuePair) {
	hitViaSet(func(set *featureusage.FeatureSet) { collectLegacyRankStrategy(params, set) })
}

func recordSearchParamKeyFeatures(params []*commonpb.KeyValuePair) {
	hitViaSet(func(set *featureusage.FeatureSet) { collectSearchParamKeyFeatures(params, scopeSearch, set) })
}

// recordExprFeatures parses expr the way the tasks do -- with the observer
// on the parser -- and counts what it saw plus the template features.
func recordExprFeatures(t *testing.T, helper *typeutil.SchemaHelper, expr string, tmpl map[string]*schemapb.TemplateValue) {
	t.Helper()
	hitViaSet(func(set *featureusage.FeatureSet) {
		// The template map is counted, not parsed: the expressions here carry
		// no placeholders, and the map only needs its keys and bool values.
		_, err := planparserv2.CreateRetrievePlanArgs(helper, expr, nil, &planparserv2.ParserVisitorArgs{OnParsedExpr: exprFeatureObserver(set)})
		require.NoError(t, err)
		collectExprTemplateFeatures(tmpl, set)
	})
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
		name := e.Name
		if e.Bucket != "" {
			name += "|" + e.Bucket
		}
		out[name] = e.Value
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
			"reranker=rrf":   1,
			"reranker=decay": 1,
			// Two unknown rerankers in one request are one request that used an
			// unknown reranker: the counters answer "how many requests used X".
			"reranker=_other":      1,
			"highlighter=Semantic": 1,
		}, delta(before, counterSnapshot()))
	})

	t.Run("search params", func(t *testing.T) {
		before := counterSnapshot()
		recordSearchInfoFeatures(true, false, 100, true) // v2 iterator also sends iterator=True
		recordSearchInfoFeatures(true, false, 0, false)  // old protocol
		recordSearchInfoFeatures(false, true, 0, false)
		assert.Equal(t, map[string]int64{
			"search_iterator=v1": 1,
			"search_iterator=v2": 1,
			"range_search":       1,
			"grouping_search":    1,
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
		assert.Equal(t, map[string]int64{"query_iterator": 1}, delta(before, counterSnapshot()))
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
		recordExprFeatures(t, helper, `text_match(s, "x") and j["a"] > 1`, map[string]*schemapb.TemplateValue{"x": {}})
		recordExprFeatures(t, helper, `s like "a%"`, nil)
		// A search plan's scorer filters go through the same parse, so the
		// observer sees them too.
		var set featureusage.FeatureSet
		_, err = planparserv2.CreateSearchPlanArgs(helper, ``, "v", &planpb.QueryInfo{Topk: 10, MetricType: "L2", SearchParams: "{}", GroupByFieldId: -1}, nil,
			&schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{{
				Name: "boost", Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{{Key: "reranker", Value: "boost"}, {Key: "filter", Value: `pk in [1]`}, {Key: "weight", Value: "2"}},
			}}},
			&planparserv2.ParserVisitorArgs{OnParsedExpr: exprFeatureObserver(&set)})
		require.NoError(t, err)
		set.HitAll()
		assert.Equal(t, map[string]int64{
			"text_match":        1,
			"json_path":         1,
			"filter_templating": 1,
			// j["a"] > 1 is also a comparison.
			"comparison_operators=relational": 1,
			"like":                            1,
			// The scorer filter, as written: the rewriter would have made the
			// one-value IN an equality.
			"in_operator": 1,
		}, delta(before, counterSnapshot()))
	})

	t.Run("expressions are counted as written, before the rewriter", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "arr", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64, TypeParams: []*commonpb.KeyValuePair{{Key: "max_capacity", Value: "8"}}},
			{FieldID: 102, Name: "ts", DataType: schemapb.DataType_Timestamptz},
			{FieldID: 103, Name: "v", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
		}}
		helper, err := typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		cases := []struct {
			expr string
			want map[string]int64
		}{
			{`pk in [1]`, map[string]int64{"in_operator": 1}},
			{`pk == 1 or pk == 2`, map[string]int64{"comparison_operators=equality": 1}},
			{`pk != 1 and pk != 2`, map[string]int64{"comparison_operators=equality": 1}},
			{`array_length(arr) > 2`, map[string]int64{"array_length": 1, "comparison_operators=relational": 1}},
			{`ts + INTERVAL 'P1D' > ISO '2026-01-01T00:00:00Z'`, map[string]int64{"timestamptz_compare": 1, "comparison_operators=relational": 1}},
			{`ts > ISO '2026-01-01T00:00:00Z'`, map[string]int64{"timestamptz_compare": 1, "comparison_operators=relational": 1}},
			{`ts is null`, map[string]int64{"is_null": 1}},
		}
		for _, tc := range cases {
			before := counterSnapshot()
			recordExprFeatures(t, helper, tc.expr, nil)
			assert.Equal(t, tc.want, delta(before, counterSnapshot()), tc.expr)
		}

		// expr_use_json_stats counts the value, not the key.
		before := counterSnapshot()
		recordExprFeatures(t, helper, `pk > 0`, map[string]*schemapb.TemplateValue{
			"expr_use_json_stats": {Val: &schemapb.TemplateValue_BoolVal{BoolVal: false}},
		})
		assert.Equal(t, map[string]int64{"comparison_operators=relational": 1}, delta(before, counterSnapshot()))
		before = counterSnapshot()
		recordExprFeatures(t, helper, `pk > 0`, map[string]*schemapb.TemplateValue{
			"expr_use_json_stats": {Val: &schemapb.TemplateValue_BoolVal{BoolVal: true}},
		})
		assert.Equal(t, map[string]int64{"comparison_operators=relational": 1, "expr_use_json_stats": 1}, delta(before, counterSnapshot()))
	})

	t.Run("search_params keys count only where the server reads them", func(t *testing.T) {
		params := []*commonpb.KeyValuePair{
			{Key: GroupSizeKey, Value: "3"},
			{Key: OrderByFieldsKey, Value: "price:desc"},
			{Key: "hints", Value: "iterative_filter"},
			{Key: AnalyzerKey, Value: "en"},
			{Key: IgnoreGrowingKey, Value: "true"},
		}
		before := counterSnapshot()
		hitViaSet(func(set *featureusage.FeatureSet) { collectSearchParamKeyFeatures(params, scopeHybridRequest, set) })
		assert.Equal(t, map[string]int64{"group_size": 1, "order_by_fields": 1}, delta(before, counterSnapshot()), "rank_params: grouping and ordering only")
		before = counterSnapshot()
		hitViaSet(func(set *featureusage.FeatureSet) { collectSearchParamKeyFeatures(params, scopeHybridSub, set) })
		assert.Equal(t, map[string]int64{"hints=iterative_filter": 1, "analyzer_name": 1, "ignore_growing": 1}, delta(before, counterSnapshot()), "sub-request: per-ANN keys only")
	})

	t.Run("search info: hints from the params JSON, range only outside an iterator", func(t *testing.T) {
		before := counterSnapshot()
		hitViaSet(func(set *featureusage.FeatureSet) { collectSearchInfoFeatures(false, true, 0, false, true, set) })
		assert.Equal(t, map[string]int64{"range_search": 1, "hints=iterative_filter": 1}, delta(before, counterSnapshot()))
		before = counterSnapshot()
		// The v1 iterator sends radius on every page after the first.
		hitViaSet(func(set *featureusage.FeatureSet) { collectSearchInfoFeatures(true, true, 0, false, false, set) })
		assert.Equal(t, map[string]int64{"search_iterator=v1": 1}, delta(before, counterSnapshot()))
	})

	t.Run("legacy level 0 with a guarantee timestamp is not a chosen level", func(t *testing.T) {
		before := counterSnapshot()
		// REST v1 and pre-2.3 SDKs: the timestamp is the request.
		recordCommonRequestFeatures(&milvuspb.SearchRequest{UseDefaultConsistency: false, ConsistencyLevel: commonpb.ConsistencyLevel_Strong, GuaranteeTimestamp: 2})
		assert.Empty(t, delta(before, counterSnapshot()))
		// pymilvus: an explicit Strong comes with timestamp 0.
		recordCommonRequestFeatures(&milvuspb.SearchRequest{UseDefaultConsistency: false, ConsistencyLevel: commonpb.ConsistencyLevel_Strong})
		assert.Equal(t, map[string]int64{"consistency_level=Strong": 1}, delta(before, counterSnapshot()))
		// A level other than Strong is a chosen level whatever the timestamp.
		recordCommonRequestFeatures(&milvuspb.SearchRequest{UseDefaultConsistency: false, ConsistencyLevel: commonpb.ConsistencyLevel_Bounded, GuaranteeTimestamp: 2})
		assert.Equal(t, map[string]int64{"consistency_level=Strong": 1, "consistency_level=Bounded": 1}, delta(before, counterSnapshot()))
	})

	t.Run("an empty legacy strategy is no ranker", func(t *testing.T) {
		before := counterSnapshot()
		recordLegacyRankStrategy([]*commonpb.KeyValuePair{{Key: RankTypeKey, Value: ""}})
		assert.Empty(t, delta(before, counterSnapshot()))
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
			"group_size": 1, "strict_group_size": 1, "rank_group_scorer": 1, "hints=iterative_filter": 1, "analyzer_name": 1, "ignore_growing": 1,
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
			"output_fields=dynamic": 1, "strategy=weighted": 1, "norm_score": 1,
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
		assert.Equal(t, map[string]int64{"reranker=weighted": 1, "norm_score": 1},
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
		schema, err := metacache.NewSchemaInfo(&schemapb.CollectionSchema{
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
					want["output_fields=vector"] = tc.want
				}
				assert.Equal(t, want, delta(before, counterSnapshot()))
			})
		}

		t.Run("a request with two vector output fields counts once", func(t *testing.T) {
			before := counterSnapshot()
			recordOutputVectorField([]string{"vec", "chunk_vec"}, schema)
			assert.Equal(t, map[string]int64{"output_fields=vector": 1}, delta(before, counterSnapshot()))
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
		recordQueryAggregationFeatures(aggregatesOf(t, "count(*)"))
		recordQueryParamKeyFeatures([]*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "pk"}})
		var units featureusage.Tally
		collectSearchParamUnits(`{"ef": 64}`, 10, &units)
		units.HitAll()
		assert.Empty(t, delta(before, counterSnapshot()))
	})
}

// aggregatesOf resolves aggregation output expressions the way the query task
// does, into the per-operator aggregates it keeps.
func aggregatesOf(t *testing.T, exprs ...string) []agg.AggregateBase {
	t.Helper()
	var out []agg.AggregateBase
	for _, e := range exprs {
		_, op, _ := agg.MatchAggregationExpression(e)
		fieldType := schemapb.DataType_Int64
		aggs, err := agg.NewAggregate(op, 100, e, fieldType)
		require.NoError(t, err, e)
		out = append(out, aggs...)
	}
	return out
}

func TestFeatureUsageHooksBlockA(t *testing.T) {
	featureusage.SetEnabled(true)
	t.Cleanup(func() { featureusage.SetEnabled(true) })

	t.Run("search parameter units, one per subrequest", func(t *testing.T) {
		before := counterSnapshot()
		var units featureusage.Tally
		collectSearchParamUnits(`{"ef": 64}`, 10, &units)
		collectSearchParamUnits(`{"nprobe": "16"}`, 100, &units)
		collectSearchParamUnits(``, 20000, &units)
		units.HitAll()
		assert.Equal(t, map[string]int64{
			"ef|17-64":       1,
			"ef|omitted":     2,
			"nprobe|omitted": 2,
			"nprobe|9-32":    1,
			"limit|<=10":     1,
			"limit|11-100":   1,
			"limit|>16384":   1,
		}, delta(before, counterSnapshot()))
	})

	t.Run("nil tally records nothing", func(t *testing.T) {
		before := counterSnapshot()
		collectSearchParamUnits(`{"ef": 64}`, 10, nil)
		assert.Equal(t, 0, collectSubSearchUnits(nil, 0, 0, 1, nil))
		assert.Empty(t, delta(before, counterSnapshot()))
	})

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "dense", DataType: schemapb.DataType_FloatVector},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
			{FieldID: 103, Name: "bin", DataType: schemapb.DataType_BinaryVector},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: 200, Name: "chunks",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 202, Name: "chunk_vec", DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector},
			},
		}},
	}

	t.Run("retrieval kind", func(t *testing.T) {
		cases := []struct {
			fieldID int64
			ph      commonpb.PlaceholderType
			want    featureusage.Feature
		}{
			{101, commonpb.PlaceholderType_FloatVector, featureusage.FeatureRetrievalDense},
			{101, commonpb.PlaceholderType_VarChar, featureusage.FeatureRetrievalDense}, // text embedding function
			{103, commonpb.PlaceholderType_BinaryVector, featureusage.FeatureRetrievalDense},
			{102, commonpb.PlaceholderType_SparseFloatVector, featureusage.FeatureRetrievalSparse},
			{102, commonpb.PlaceholderType_VarChar, featureusage.FeatureRetrievalFullText},
			{202, commonpb.PlaceholderType_EmbListFloatVector, featureusage.FeatureRetrievalEmbeddingList},
			{202, 0, featureusage.FeatureRetrievalEmbeddingList},
			{202, commonpb.PlaceholderType_FloatVector, featureusage.FeatureRetrievalElementLevel},
		}
		for _, tc := range cases {
			assert.Equal(t, tc.want, retrievalFeature(schema, tc.fieldID, tc.ph), "field %d placeholder %v", tc.fieldID, tc.ph)
		}
	})

	t.Run("hybrid search: units per subrequest, shape once", func(t *testing.T) {
		before := counterSnapshot()
		var units featureusage.Tally
		var set featureusage.FeatureSet
		families := 0
		families |= collectSubSearchUnits(schema, 101, commonpb.PlaceholderType_FloatVector, 1, &units)
		families |= collectSubSearchUnits(schema, 102, commonpb.PlaceholderType_VarChar, 1, &units)
		families |= collectSubSearchUnits(schema, 101, commonpb.PlaceholderType_FloatVector, 5, &units)
		collectHybridShape(3, families, &set)
		units.HitAll()
		set.HitAll()
		assert.Equal(t, map[string]int64{
			"retrieval=dense_vector":     2,
			"retrieval=full_text_search": 1,
			"nq|1":                       2,
			"nq|2-10":                    1,
			"hybrid_search_reqs|3":       1,
			"hybrid_search=dense_vector+full_text_search": 1,
		}, delta(before, counterSnapshot()))
	})

	t.Run("hybrid shape with a nil set", func(t *testing.T) {
		before := counterSnapshot()
		collectHybridShape(2, featureusage.HybridFamilyDense, nil)
		assert.Empty(t, delta(before, counterSnapshot()))
	})

	t.Run("query aggregation: avg counts as avg, not sum and count", func(t *testing.T) {
		before := counterSnapshot()
		recordQueryAggregationFeatures(aggregatesOf(t, "avg(age)", "max(age)", "MAX(score)"))
		assert.Equal(t, map[string]int64{
			"query_aggregation=avg": 1,
			"query_aggregation=max": 1,
		}, delta(before, counterSnapshot()))

		before = counterSnapshot()
		recordQueryAggregationFeatures(aggregatesOf(t, "count(*)", "sum(age)", "min(age)"))
		assert.Equal(t, map[string]int64{
			"query_aggregation=count": 1,
			"query_aggregation=sum":   1,
			"query_aggregation=min":   1,
		}, delta(before, counterSnapshot()))

		before = counterSnapshot()
		recordQueryAggregationFeatures(nil)
		assert.Empty(t, delta(before, counterSnapshot()))
	})

	t.Run("order by: query and search keys, search aggregation", func(t *testing.T) {
		before := counterSnapshot()
		recordQueryParamKeyFeatures([]*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "price:desc"}})
		assert.Equal(t, map[string]int64{"order_by": 1}, delta(before, counterSnapshot()))

		before = counterSnapshot()
		recordSearchParamKeyFeatures([]*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "price:desc"}})
		assert.Equal(t, map[string]int64{"order_by_fields": 1}, delta(before, counterSnapshot()))

		before = counterSnapshot()
		recordSearchRequestFeatures(&milvuspb.SearchRequest{UseDefaultConsistency: true, SearchAggregation: &commonpb.SearchAggregationSpec{}})
		assert.Equal(t, map[string]int64{"search_aggregation": 1}, delta(before, counterSnapshot()))
	})
}

// Function chains are the third reranking client shape; they share the
// reranker=* counters with function_score and are read from the proto before
// validation, like every other request hook.
func TestFeatureUsageHooksFunctionChains(t *testing.T) {
	featureusage.SetEnabled(true)
	t.Cleanup(func() { featureusage.SetEnabled(true) })

	strParam := func(v string) *schemapb.FunctionParamValue {
		return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_StringValue{StringValue: v}}
	}
	mergeOp := func(strategy string) *schemapb.FunctionChainOp {
		return &schemapb.FunctionChainOp{
			Op:     chaintypes.OpTypeMerge,
			Params: map[string]*schemapb.FunctionParamValue{chain.MergeParamStrategy: strParam(strategy)},
		}
	}
	fnOp := func(opType, fn string) *schemapb.FunctionChainOp {
		return &schemapb.FunctionChainOp{Op: opType, Expr: &schemapb.FunctionChainExpr{Name: fn}}
	}
	l2 := func(ops ...*schemapb.FunctionChainOp) *schemapb.FunctionChain {
		return &schemapb.FunctionChain{Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank, Ops: ops}
	}

	cases := []struct {
		name   string
		chains []*schemapb.FunctionChain
		want   map[string]int64
	}{
		{"merge rrf", []*schemapb.FunctionChain{l2(mergeOp("rrf"))}, map[string]int64{"reranker=rrf": 1}},
		{"merge weighted", []*schemapb.FunctionChain{l2(mergeOp("Weighted"))}, map[string]int64{"reranker=weighted": 1}},
		{"merge max has no function_score equivalent", []*schemapb.FunctionChain{l2(mergeOp("max"))}, map[string]int64{"reranker=_other": 1}},
		{"map decay", []*schemapb.FunctionChain{l2(fnOp(chaintypes.OpTypeMap, "decay"))}, map[string]int64{"reranker=decay": 1}},
		{"filter rerank_model", []*schemapb.FunctionChain{l2(fnOp(chaintypes.OpTypeFilter, "rerank_model"))}, map[string]int64{"reranker=model": 1}},
		{"map boost_score", []*schemapb.FunctionChain{l2(fnOp(chaintypes.OpTypeMap, "boost_score"))}, map[string]int64{"reranker=boost": 1}},
		{
			"helpers alone fold to _other",
			[]*schemapb.FunctionChain{l2(fnOp(chaintypes.OpTypeMap, "num_combine"), fnOp(chaintypes.OpTypeMap, "round_decimal"))},
			map[string]int64{"reranker=_other": 1},
		},
		{
			"helpers beside a reranker add nothing",
			[]*schemapb.FunctionChain{l2(mergeOp("rrf"), fnOp(chaintypes.OpTypeMap, "decay"), fnOp(chaintypes.OpTypeMap, "round_decimal"))},
			map[string]int64{"reranker=rrf": 1, "reranker=decay": 1},
		},
		{
			"two chains, one request, one count per feature",
			[]*schemapb.FunctionChain{
				{Stage: schemapb.FunctionChainStage_FunctionChainStageL1Rerank, Ops: []*schemapb.FunctionChainOp{fnOp(chaintypes.OpTypeMap, "decay")}},
				l2(fnOp(chaintypes.OpTypeMap, "decay")),
			},
			map[string]int64{"reranker=decay": 1},
		},
		{"sort/limit only", []*schemapb.FunctionChain{l2(&schemapb.FunctionChainOp{Op: chaintypes.OpTypeSort})}, map[string]int64{"reranker=_other": 1}},
		{"no chains", nil, map[string]int64{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := counterSnapshot()
			recordSearchRequestFeatures(&milvuspb.SearchRequest{UseDefaultConsistency: true, FunctionChains: tc.chains})
			assert.Equal(t, tc.want, delta(before, counterSnapshot()))
		})
	}
}

func TestFeatureUsageHooksExecBits(t *testing.T) {
	featureusage.SetEnabled(true)
	t.Cleanup(func() { featureusage.SetEnabled(true) })

	scalar, _ := featureusage.ExecBit(featureusage.FeatureFilterPathScalarIndex)
	inverted, _ := featureusage.ExecBit(featureusage.FeatureScalarIndexInverted)
	cold, _ := featureusage.ExecBit(featureusage.FeatureTieredStorageColdRead)

	t.Run("bits of a counted request count once each", func(t *testing.T) {
		before := counterSnapshot()
		recordExecFeatures(true, scalar|inverted, 0)
		assert.Equal(t, map[string]int64{
			"filter_exec_path=scalar_index": 1,
			"scalar_index_type=INVERTED":    1,
		}, delta(before, counterSnapshot()))
	})

	t.Run("remote bytes or the cold bit count as a cold read", func(t *testing.T) {
		before := counterSnapshot()
		recordExecFeatures(true, 0, 10)
		recordExecFeatures(true, cold, 0)
		assert.Equal(t, map[string]int64{"tiered_storage_cold_read": 2}, delta(before, counterSnapshot()))
	})

	t.Run("an uncounted request or disabled counters move nothing", func(t *testing.T) {
		before := counterSnapshot()
		recordExecFeatures(false, scalar, 10)
		featureusage.SetEnabled(false)
		recordExecFeatures(true, scalar, 10)
		featureusage.SetEnabled(true)
		assert.Empty(t, delta(before, counterSnapshot()))
	})

	t.Run("the plan option is set only on counted requests", func(t *testing.T) {
		plan := &planpb.PlanNode{}
		collectExecFeatureBits(plan, false)
		assert.False(t, plan.GetPlanOptions().GetCollectFeatureBits())
		collectExecFeatureBits(plan, true)
		assert.True(t, plan.GetPlanOptions().GetCollectFeatureBits())

		withOptions := &planpb.PlanNode{PlanOptions: &planpb.PlanOption{ExprUseJsonStats: true}}
		collectExecFeatureBits(withOptions, true)
		assert.True(t, withOptions.GetPlanOptions().GetExprUseJsonStats(), "existing options are kept")
		assert.True(t, withOptions.GetPlanOptions().GetCollectFeatureBits())
		collectExecFeatureBits(nil, true)
	})
}

// Wrappers that count one class of features on its own, for the tests; the
// tasks mark into their own set and flush once.

// recordCommonRequestFeatures counts them for a request that is counted in one
// place, as the query path is.
func recordCommonRequestFeatures(r requestFeatureSource) {
	if !featureusage.Enabled() {
		return
	}
	var set featureusage.FeatureSet
	collectCommonRequestFeatures(r, &set)
	set.HitAll()
}

// recordQueryParamKeyFeatures counts them on their own.
func recordQueryParamKeyFeatures(params []*commonpb.KeyValuePair) {
	if !featureusage.Enabled() {
		return
	}
	var set featureusage.FeatureSet
	collectQueryParamKeyFeatures(params, &set)
	set.HitAll()
}

// recordOutputFieldFeatures counts them for a request counted in one place,
// as the query path is.
func recordOutputFieldFeatures(userDynamicFields, translatedOutputFields []string, schema *schemaInfo) {
	if !featureusage.Enabled() {
		return
	}
	var set featureusage.FeatureSet
	collectOutputFieldFeatures(userDynamicFields, translatedOutputFields, schema, &set)
	set.HitAll()
}

// recordQueryIteratorFeature counts it on its own.
func recordQueryIteratorFeature(isIterator bool) {
	if isIterator && featureusage.Enabled() {
		featureusage.Hit(featureusage.FeatureQueryIterator)
	}
}

// recordQueryAggregationFeatures counts them on their own, once each.
func recordQueryAggregationFeatures(aggs []agg.AggregateBase) {
	if len(aggs) == 0 || !featureusage.Enabled() {
		return
	}
	var set featureusage.FeatureSet
	collectQueryAggregationFeatures(aggs, &set)
	set.HitAll()
}
