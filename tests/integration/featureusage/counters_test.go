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
package featureusage

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

// CountersSuite drives one request per Proxy counter and asserts that exactly
// that counter moved. The "exactly" is the point: a hook that fires on the
// wrong request, or one that silently stops firing, fails here even though
// nothing else in the build notices.
type CountersSuite struct {
	Suite

	collection string
	dim        int
}

const (
	pkField     = "pk"
	textField   = "text"
	jsonField   = "meta"
	arrayField  = "tags"
	vectorField = "vec"
	// nullableField is left unset on half the rows so is_null and is_not_null
	// both match something.
	nullableField = "note"
)

// ensureCollection builds the shared workload collection on first use: a
// primary key, an analyzer-enabled VarChar, a JSON field, an Int32 array and a
// float vector, indexed and loaded. Every counter test reads from it.
func (s *Suite) ensureCollection(ctx context.Context) {
	if s.collection != "" {
		return
	}
	s.dim = 8
	s.collection = "fu_counters_" + funcutil.GenRandomStr()

	schema := &schemapb.CollectionSchema{
		Name:               s.collection,
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: pkField, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{
				FieldID: 101, Name: textField, DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.MaxLengthKey, Value: "256"},
					{Key: "enable_analyzer", Value: "true"},
					{Key: "enable_match", Value: "true"},
				},
			},
			{FieldID: 102, Name: jsonField, DataType: schemapb.DataType_JSON},
			{
				FieldID: 105, Name: nullableField, DataType: schemapb.DataType_VarChar, Nullable: true,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "64"}},
			},
			{
				FieldID: 103, Name: arrayField, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "16"}},
			},
			{
				FieldID: 104, Name: vectorField, DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: fmt.Sprint(s.dim)}},
			},
		},
	}
	marshaled, err := proto.Marshal(schema)
	s.Require().NoError(err)

	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: s.collection,
		Schema:         marshaled,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, status.GetErrorCode(), status.GetReason())

	const rowNum = 300
	texts := make([]string, rowNum)
	metas := make([][]byte, rowNum)
	for i := range texts {
		texts[i] = fmt.Sprintf("milvus feature usage row %d", i)
		metas[i] = []byte(fmt.Sprintf(`{"k": %d, "arr": [%d, %d]}`, i, i, i+1))
	}
	insert, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: s.collection,
		FieldsData: []*schemapb.FieldData{
			newVarCharColumn(textField, texts),
			newJSONColumn(jsonField, metas),
			newInt32ArrayColumn(arrayField, rowNum),
			newNullableVarCharColumn(nullableField, rowNum),
			integration.NewFloatVectorFieldData(vectorField, rowNum, s.dim),
		},
		HashKeys: integration.GenerateHashKeys(rowNum),
		NumRows:  uint32(rowNum),
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, insert.GetStatus().GetErrorCode(), insert.GetStatus().GetReason())

	flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{s.collection}})
	s.Require().NoError(err)
	ids := flush.GetCollSegIDs()[s.collection].GetData()
	s.WaitForFlush(ctx, ids, flush.GetCollFlushTs()[s.collection], "", s.collection)

	index, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: s.collection,
		FieldName:      vectorField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(s.dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, index.GetErrorCode(), index.GetReason())
	s.WaitForIndexBuilt(ctx, s.collection, vectorField)

	load, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: s.collection})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, load.GetErrorCode(), load.GetReason())
	s.WaitForLoad(ctx, s.collection)
}

// baseSearchRequest is a plain top-k search: no counted option set, and
// UseDefaultConsistency true so the consistency counter stays still unless a
// case asks for it.
func (s *Suite) baseSearchRequest() *milvuspb.SearchRequest {
	req := integration.ConstructSearchRequest("", s.collection, "", vectorField,
		schemapb.DataType_FloatVector, nil, metric.L2, map[string]any{"nprobe": 4}, 1, s.dim, 5, -1)
	req.UseDefaultConsistency = true
	return req
}

// searchWith issues one search, letting the caller shape the request. The
// counters are recorded at the top of PreExecute, so a request that is later
// rejected still counts; the test asserts on the counters, not on the result.
func (s *Suite) searchWith(ctx context.Context, shape func(*milvuspb.SearchRequest)) {
	req := s.baseSearchRequest()
	shape(req)
	_, err := s.Cluster.MilvusClient.Search(ctx, req)
	s.Require().NoError(err, "transport error")
}

func (s *Suite) queryWith(ctx context.Context, shape func(*milvuspb.QueryRequest)) {
	req := &milvuspb.QueryRequest{
		CollectionName:        s.collection,
		OutputFields:          []string{pkField},
		UseDefaultConsistency: true,
		QueryParams:           []*commonpb.KeyValuePair{{Key: "limit", Value: "5"}},
	}
	shape(req)
	_, err := s.Cluster.MilvusClient.Query(ctx, req)
	s.Require().NoError(err, "transport error")
}

// withSearchParam sets one search parameter, replacing an existing value.
// Appending a duplicate would be silently ignored: the proxy reads the first
// occurrence of a key.
func withSearchParam(key, value string) func(*milvuspb.SearchRequest) {
	return func(r *milvuspb.SearchRequest) {
		for _, kv := range r.SearchParams {
			if kv.GetKey() == key {
				kv.Value = value
				return
			}
		}
		r.SearchParams = append(r.SearchParams, &commonpb.KeyValuePair{Key: key, Value: value})
	}
}

// TestSearchCounters walks the search-side counters one request at a time.
func (s *Suite) TestSearchCounters() {
	ctx := context.Background()
	s.ensureCollection(ctx)
	cases := []struct {
		name string
		run  func()
		want map[string]int64
	}{
		{
			name: "group_by_field",
			run:  func() { s.searchWith(ctx, withSearchParam("group_by_field", textField)) },
			want: map[string]int64{"group_by_field": 1},
		},
		{
			name: "range search reports radius",
			run: func() {
				s.searchWith(ctx, withSearchParam("params", `{"nprobe": 4, "radius": 100.0, "range_filter": 0.0}`))
			},
			want: map[string]int64{"radius": 1},
		},
		{
			name: "explicit consistency level",
			run: func() {
				s.searchWith(ctx, func(r *milvuspb.SearchRequest) {
					r.UseDefaultConsistency = false
					r.ConsistencyLevel = commonpb.ConsistencyLevel_Eventually
				})
			},
			want: map[string]int64{"consistency_level=Eventually": 1},
		},
		{
			name: "default consistency counts nothing",
			run:  func() { s.searchWith(ctx, func(*milvuspb.SearchRequest) {}) },
			want: map[string]int64{},
		},
		{
			name: "not_return_all_meta",
			run: func() {
				s.searchWith(ctx, func(r *milvuspb.SearchRequest) { r.NotReturnAllMeta = true })
			},
			want: map[string]int64{"not_return_all_meta": 1},
		},
		{
			name: "deprecated travel timestamp",
			run: func() {
				s.searchWith(ctx, func(r *milvuspb.SearchRequest) { r.TravelTimestamp = 1 })
			},
			want: map[string]int64{"deprecated_travel_timestamp": 1},
		},
		{
			name: "group_size and strict_group_size",
			run: func() {
				s.searchWith(ctx, func(r *milvuspb.SearchRequest) {
					withSearchParam("group_by_field", textField)(r)
					withSearchParam("group_size", "2")(r)
					withSearchParam("strict_group_size", "true")(r)
				})
			},
			want: map[string]int64{"group_by_field": 1, "group_size": 1, "strict_group_size": 1},
		},
		{
			name: "hints",
			run:  func() { s.searchWith(ctx, withSearchParam(common.HintsKey, "iterative_filter")) },
			want: map[string]int64{"hints": 1},
		},
		{
			name: "namespace",
			run: func() {
				ns := "tenant_a"
				s.searchWith(ctx, func(r *milvuspb.SearchRequest) { r.Namespace = &ns })
			},
			want: map[string]int64{"namespace": 1},
		},
		{
			name: "search_by_primary_keys",
			run: func() {
				s.searchWith(ctx, func(r *milvuspb.SearchRequest) { r.SearchByPrimaryKeys = true })
			},
			want: map[string]int64{"search_by_primary_keys": 1},
		},
		{
			name: "analyzer_name",
			run:  func() { s.searchWith(ctx, withSearchParam("analyzer_name", "standard")) },
			want: map[string]int64{"analyzer_name": 1},
		},
		{
			name: "rank_group_scorer",
			run: func() {
				s.searchWith(ctx, func(r *milvuspb.SearchRequest) {
					withSearchParam("group_by_field", textField)(r)
					withSearchParam("rank_group_scorer", "max")(r)
				})
			},
			want: map[string]int64{"group_by_field": 1, "rank_group_scorer": 1},
		},
		{
			name: "old iterator protocol",
			run:  func() { s.searchWith(ctx, withSearchParam("iterator", "true")) },
			want: map[string]int64{"iterator": 1},
		},
		{
			name: "iterator v2 excludes the old counter",
			run: func() {
				s.searchWith(ctx, func(r *milvuspb.SearchRequest) {
					withSearchParam("iterator", "true")(r)
					withSearchParam("search_iter_v2", "true")(r)
					// v2 needs a batch size; without it the parse fails before
					// the counters are recorded.
					withSearchParam("search_iter_batch_size", "5")(r)
				})
			},
			want: map[string]int64{"search_iter_v2": 1},
		},
		{
			name: "highlighter with fragment parameters",
			run: func() {
				s.searchWith(ctx, func(r *milvuspb.SearchRequest) {
					r.Highlighter = &commonpb.Highlighter{
						Type: commonpb.HighlightType_Lexical,
						Params: []*commonpb.KeyValuePair{
							{Key: "fragment_size", Value: "50"},
							{Key: "num_of_fragments", Value: "2"},
						},
					}
				})
			},
			want: map[string]int64{"highlighter=Lexical": 1, "fragment_size": 1, "num_of_fragments": 1},
		},
		{
			name: "function_score names the rerank function",
			run: func() {
				s.searchWith(ctx, func(r *milvuspb.SearchRequest) {
					r.FunctionScore = &schemapb.FunctionScore{
						Functions: []*schemapb.FunctionSchema{{
							Name:   "rerank",
							Type:   schemapb.FunctionType_Rerank,
							Params: []*commonpb.KeyValuePair{{Key: "reranker", Value: "decay"}},
						}},
					}
				})
			},
			want: map[string]int64{"function_score=decay": 1},
		},
		{
			name: "unknown rerank function folds to _other",
			run: func() {
				s.searchWith(ctx, func(r *milvuspb.SearchRequest) {
					r.FunctionScore = &schemapb.FunctionScore{
						Functions: []*schemapb.FunctionSchema{{
							Name:   "rerank",
							Type:   schemapb.FunctionType_Rerank,
							Params: []*commonpb.KeyValuePair{{Key: "reranker", Value: "not_a_reranker"}},
						}},
					}
				})
			},
			want: map[string]int64{"function_score=_other": 1},
		},
		{
			name: "ignore_growing counts only when true",
			run: func() {
				s.searchWith(ctx, withSearchParam("ignore_growing", "false"))
				s.searchWith(ctx, withSearchParam("ignore_growing", "true"))
			},
			want: map[string]int64{"ignore_growing": 1},
		},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			before := counters(s.report(ctx), typeutil.ProxyRole)
			tc.run()
			after := counters(s.report(ctx), typeutil.ProxyRole)
			requireOnlyDelta(s.T(), before, after, tc.want)
		})
	}
}

// TestQueryCounters walks the query-side counters, which have their own hook
// sites and their own parameter names.
func (s *Suite) TestQueryCounters() {
	ctx := context.Background()
	s.ensureCollection(ctx)
	cases := []struct {
		name string
		run  func()
		want map[string]int64
	}{
		{
			name: "query iterator uses the old protocol counter",
			run: func() {
				s.queryWith(ctx, func(r *milvuspb.QueryRequest) {
					r.QueryParams = append(r.QueryParams, &commonpb.KeyValuePair{Key: "iterator", Value: "true"})
				})
			},
			want: map[string]int64{"iterator": 1},
		},
		{
			name: "group_by_fields on the query path",
			run: func() {
				s.queryWith(ctx, func(r *milvuspb.QueryRequest) {
					r.QueryParams = append(r.QueryParams, &commonpb.KeyValuePair{Key: "group_by_fields", Value: textField})
				})
			},
			want: map[string]int64{"group_by_fields": 1},
		},
		{
			name: "output_dynamic_field",
			run: func() {
				s.queryWith(ctx, func(r *milvuspb.QueryRequest) {
					r.Expr = pkField + " >= 0"
					r.OutputFields = []string{"a_dynamic_key"}
				})
			},
			want: map[string]int64{"output_dynamic_field": 1},
		},
		{
			name: "expression template values",
			run: func() {
				s.queryWith(ctx, func(r *milvuspb.QueryRequest) {
					r.Expr = pkField + " > {v}"
					r.ExprTemplateValues = map[string]*schemapb.TemplateValue{
						"v": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 0}},
					}
				})
			},
			want: map[string]int64{"expr_template_values": 1},
		},
		{
			name: "explicit consistency level on the query path",
			run: func() {
				s.queryWith(ctx, func(r *milvuspb.QueryRequest) {
					r.Expr = pkField + " >= 0"
					r.UseDefaultConsistency = false
					r.ConsistencyLevel = commonpb.ConsistencyLevel_Bounded
				})
			},
			want: map[string]int64{"consistency_level=Bounded": 1},
		},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			before := counters(s.report(ctx), typeutil.ProxyRole)
			tc.run()
			after := counters(s.report(ctx), typeutil.ProxyRole)
			requireOnlyDelta(s.T(), before, after, tc.want)
		})
	}
}

// TestExpressionCounters walks the expression counters. They all come from one
// walk over the parsed predicate, so this also proves the walk reaches each
// node kind.
func (s *Suite) TestExpressionCounters() {
	ctx := context.Background()
	s.ensureCollection(ctx)
	cases := []struct {
		name string
		expr string
		want map[string]int64
	}{
		{"like", fmt.Sprintf(`%s like "milvus%%"`, textField), map[string]int64{"like": 1}},
		{"text_match", fmt.Sprintf(`text_match(%s, "milvus")`, textField), map[string]int64{"text_match": 1}},
		{"phrase_match", fmt.Sprintf(`phrase_match(%s, "feature usage")`, textField), map[string]int64{"phrase_match": 1}},
		{"json_identifier", fmt.Sprintf(`%s["k"] > 0`, jsonField), map[string]int64{"json_identifier": 1}},
		{"json_contains", fmt.Sprintf(`json_contains(%s["arr"], 1)`, jsonField), map[string]int64{"json_contains": 1, "json_identifier": 1}},
		{"array_contains", fmt.Sprintf(`array_contains(%s, 1)`, arrayField), map[string]int64{"array_contains": 1}},
		{"array_length", fmt.Sprintf(`array_length(%s) == 2`, arrayField), map[string]int64{"array_length": 1}},
		{"random_sample", `random_sample(0.5)`, map[string]int64{"random_sample": 1}},
		{"exists", fmt.Sprintf(`exists %s["k"]`, jsonField), map[string]int64{"exists": 1, "json_identifier": 1}},
		{"is_not_null", fmt.Sprintf(`%s is not null`, textField), map[string]int64{"is_not_null": 1}},
		{"regex_match", fmt.Sprintf(`%s like "mil%%us%%"`, textField), map[string]int64{"like": 1}},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			before := counters(s.report(ctx), typeutil.ProxyRole)
			s.queryWith(ctx, func(r *milvuspb.QueryRequest) { r.Expr = tc.expr })
			after := counters(s.report(ctx), typeutil.ProxyRole)
			requireOnlyDelta(s.T(), before, after, tc.want)
		})
	}
}

// The parsed-expression cache keys on the expression string. A counter placed
// inside the parser would fire only on cache misses; this asserts the count
// tracks requests, not parses.
func (s *Suite) TestExpressionCounterIsNotCached() {
	ctx := context.Background()
	s.ensureCollection(ctx)
	expr := fmt.Sprintf(`%s like "milvus%%"`, textField)
	before := counters(s.report(ctx), typeutil.ProxyRole)
	for i := 0; i < 5; i++ {
		s.queryWith(ctx, func(r *milvuspb.QueryRequest) { r.Expr = expr })
	}
	after := counters(s.report(ctx), typeutil.ProxyRole)
	requireOnlyDelta(s.T(), before, after, map[string]int64{"like": 5})
}

// An unrecognized rank strategy is a user string. It must fold into the single
// _other slot and create nothing else, or the key space would not be closed.
func (s *Suite) TestUnknownRankStrategyFoldsToOther() {
	ctx := context.Background()
	s.ensureCollection(ctx)
	before := counters(s.report(ctx), typeutil.ProxyRole)
	sub := s.baseSearchRequest()
	sub.UseDefaultConsistency = false
	_, err := s.Cluster.MilvusClient.HybridSearch(ctx, &milvuspb.HybridSearchRequest{
		CollectionName: s.collection,
		Requests:       []*milvuspb.SearchRequest{sub},
		RankParams: []*commonpb.KeyValuePair{
			{Key: "strategy", Value: "a_strategy_that_does_not_exist"},
			{Key: "limit", Value: "5"},
		},
		UseDefaultConsistency: true,
	})
	s.Require().NoError(err, "transport error")
	after := counters(s.report(ctx), typeutil.ProxyRole)
	s.Equal(before["strategy=_other"]+1, after["strategy=_other"], "the unknown value folds into _other")
	s.Equal(before["strategy=rrf"], after["strategy=rrf"], "no recognized slot moved")
	s.Len(after, len(before), "no slot was created for the user string")
}

func newVarCharColumn(name string, values []string) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}},
		}},
	}
}

func newJSONColumn(name string, values [][]byte) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: values}},
		}},
	}
}

// newInt32ArrayColumn gives every row a two-element array, so array_length
// and array_contains both have something to match.
func newInt32ArrayColumn(name string, numRows int) *schemapb.FieldData {
	rows := make([]*schemapb.ScalarField, 0, numRows)
	for i := 0; i < numRows; i++ {
		rows = append(rows, &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{int32(i), int32(i + 1)}}},
		})
	}
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
				Data: rows, ElementType: schemapb.DataType_Int32,
			}},
		}},
	}
}

// newNullableVarCharColumn leaves every other row null.
func newNullableVarCharColumn(name string, numRows int) *schemapb.FieldData {
	values := make([]string, 0, numRows)
	valid := make([]bool, 0, numRows)
	for i := 0; i < numRows; i++ {
		if i%2 == 0 {
			values = append(values, fmt.Sprintf("note-%d", i))
			valid = append(valid, true)
		} else {
			valid = append(valid, false)
		}
	}
	return &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}},
		}},
		ValidData: valid,
	}
}
