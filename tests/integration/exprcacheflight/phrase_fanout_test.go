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

package exprcacheflight

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

const rowCount = 2400

// This tests real RPC query/group-by results, not performance or the number of
// leaders. Deterministic coalescing and timeout assertions live in the C++ UTs.
type phraseFanoutSuite struct {
	integration.MiniClusterSuite
	mode          string
	enabled       bool
	limited       bool
	cacheDisabled bool
	deleted       map[int]bool
}

func (s *phraseFanoutSuite) SetupSuite() {
	paramtable.Init()
	cfg := &paramtable.Get().QueryNodeCfg
	s.WithMilvusConfig(cfg.ExprResCacheEnabled.Key, strconv.FormatBool(!s.cacheDisabled))
	s.WithMilvusConfig(cfg.ExprResCacheMode.Key, s.mode)
	s.WithMilvusConfig(cfg.ExprResCacheAdmissionThreshold.Key, "1")
	s.WithMilvusConfig(cfg.ExprResCacheMinEvalDurationUs.Key, "0")
	s.WithMilvusConfig(cfg.ExprResCacheSingleFlightEnabled.Key, strconv.FormatBool(s.enabled))
	s.WithMilvusConfig(cfg.ExprResCacheSingleFlightMaxWaitMs.Key, "5000")
	if s.limited {
		s.WithMilvusConfig(cfg.ExprResCacheSingleFlightMaxWaitMs.Key, "1")
		s.WithMilvusConfig(cfg.ExprResCacheSingleFlightMaxFlights.Key, "1")
		s.WithMilvusConfig(cfg.ExprResCacheSingleFlightMaxWaiters.Key, "1")
		s.WithMilvusConfig(cfg.ExprResCacheSingleFlightMaxWaitersPerKey.Key, "1")
	}
	s.MiniClusterSuite.SetupSuite()
}

func intColumn(name string, data []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type: schemapb.DataType_Int64, FieldName: name,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}},
		}},
	}
}

func category(id, facet int) int64 { return int64((id/6 + id%6) % (facet + 2)) }

func (s *phraseFanoutSuite) create(ctx context.Context) string {
	name := "expr_fanout_" + funcutil.GenRandomStr()
	schema := &schemapb.CollectionSchema{Name: name, Fields: []*schemapb.FieldSchema{
		{Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{Name: "vector", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}},
		{Name: "text", DataType: schemapb.DataType_VarChar, Nullable: true, TypeParams: []*commonpb.KeyValuePair{
			{Key: "max_length", Value: "4096"}, {Key: "enable_analyzer", Value: "true"}, {Key: "enable_match", Value: "true"},
		}},
	}}
	pks := make([]int64, rowCount)
	valid := make([]bool, rowCount)
	texts := make([]string, 0, rowCount)
	literals := []string{"red blue", "red green blue", "blue red", "red blue red blue", "red yellow green blue", ""}
	for i := range pks {
		pks[i] = int64(i)
		valid[i] = i%6 != 5
		if valid[i] {
			texts = append(texts, strings.Repeat("padding ", 128)+literals[i%6])
		}
	}
	text := &schemapb.FieldData{
		Type: schemapb.DataType_VarChar, FieldName: "text",
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: texts}},
		}},
	}
	typeutil.SetFieldDataValidData(text, valid)
	data := []*schemapb.FieldData{intColumn("pk", pks), integration.NewFloatVectorFieldData("vector", rowCount, 8), text}
	schema.Fields = append(schema.Fields,
		&schemapb.FieldSchema{Name: "score", DataType: schemapb.DataType_Int64, Nullable: true},
		&schemapb.FieldSchema{Name: "tag", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "32"}}},
		&schemapb.FieldSchema{Name: "items", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "max_capacity", Value: "8"}}},
		&schemapb.FieldSchema{Name: "meta", DataType: schemapb.DataType_JSON, Nullable: true})
	scores := make([]int64, 0, rowCount)
	tags := make([]string, rowCount)
	arrays := make([]*schemapb.ScalarField, 0, rowCount)
	documents := make([][]byte, 0, rowCount)
	for i := 0; i < rowCount; i++ {
		tags[i] = fmt.Sprintf("tag%d", i%3)
		if !valid[i] {
			continue
		}
		scores = append(scores, int64(i%10))
		arrays = append(arrays, &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{int64(i % 4), int64((i + 1) % 4)}}}})
		document := fmt.Sprintf(`{"n":%d,"arr":[%d,%d]}`, i%10, i%4, (i+1)%4)
		switch i % 6 {
		case 2:
			document = `{"n":"bad","arr":[]}`
		case 3:
			document = fmt.Sprintf(`{"n":null,"arr":[%d]}`, i%4)
		case 4:
			document = `{}`
		}
		documents = append(documents, []byte(document))
	}
	score := intColumn("score", scores)
	items := &schemapb.FieldData{Type: schemapb.DataType_Array, FieldName: "items", Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: arrays, ElementType: schemapb.DataType_Int64}}}}}
	meta := &schemapb.FieldData{Type: schemapb.DataType_JSON, FieldName: "meta", Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: documents}}}}}
	tag := &schemapb.FieldData{Type: schemapb.DataType_VarChar, FieldName: "tag", Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: tags}}}}}
	for _, nullable := range []*schemapb.FieldData{score, items, meta} {
		typeutil.SetFieldDataValidData(nullable, valid)
	}
	data = append(data, score, tag, items, meta)
	for facet := 0; facet < 6; facet++ {
		field := fmt.Sprintf("facet%d", facet)
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{Name: field, DataType: schemapb.DataType_Int64})
		values := make([]int64, rowCount)
		for i := range values {
			values[i] = category(i, facet)
		}
		data = append(data, intColumn(field, values))
	}
	encoded, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: name, Schema: encoded, ShardsNum: 1,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	inserted, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: name, FieldsData: data, HashKeys: integration.GenerateHashKeys(rowCount), NumRows: rowCount,
	})
	s.Require().NoError(merr.CheckRPCCall(inserted, err))
	flushed, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{name}})
	s.Require().NoError(merr.CheckRPCCall(flushed, err))
	s.WaitForFlush(ctx, flushed.GetCollSegIDs()[name].GetData(), flushed.GetCollFlushTs()[name], "", name)
	status, err = s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: name, FieldName: "vector", IndexName: "vector_idx", ExtraParams: []*commonpb.KeyValuePair{
			{Key: "index_type", Value: "FLAT"}, {Key: "metric_type", Value: "L2"},
		},
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForIndexBuilt(ctx, name, "vector")
	for _, field := range []string{"score", "tag", "items", "meta"} {
		params := []*commonpb.KeyValuePair{{Key: "index_type", Value: "INVERTED"}}
		if field == "meta" {
			params = append(params, &commonpb.KeyValuePair{Key: "json_cast_type", Value: "json"})
		}
		status, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{CollectionName: name, FieldName: field, IndexName: field + "_idx", ExtraParams: params})
		s.Require().NoError(merr.CheckRPCCall(status, err))
		s.WaitForIndexBuilt(ctx, name, field)
	}
	s.load(ctx, name)
	return name
}

func (s *phraseFanoutSuite) load(ctx context.Context, name string) {
	status, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, name)
}

type phraseCase struct {
	name, expr string
	matches    func(int) bool
}

func cases() []phraseCase {
	valid := func(i int) bool { return i%6 != 5 }
	numeric := func(i int) bool { return i%6 < 2 }
	arrayHas := func(i, value int) bool { return valid(i) && (i%4 == value || (i+1)%4 == value) }
	jsonHas := func(i, value int) bool {
		if i%6 < 2 {
			return arrayHas(i, value)
		}
		return i%6 == 3 && i%4 == value
	}
	exact := func(i int) bool { return i%6 == 0 || i%6 == 3 }
	slop := func(i int) bool { return exact(i) || i%6 == 1 }
	reverse := func(i int) bool { return i%6 == 2 || i%6 == 3 }
	const p = `phrase_match(text, "red blue", 0)`
	const r = `phrase_match(text, "blue red", 0)`
	return []phraseCase{
		{"scalar_equal", "score == 3", func(i int) bool { return valid(i) && i%10 == 3 }},
		{"scalar_range", "2 <= score < 6", func(i int) bool { return valid(i) && i%10 >= 2 && i%10 < 6 }},
		{"scalar_in", "score in [1, 3, 7]", func(i int) bool { return valid(i) && (i%10 == 1 || i%10 == 3 || i%10 == 7) }},
		{"scalar_not_nullable", "not (score >= 4)", func(i int) bool { return valid(i) && i%10 < 4 }},
		{"string_prefix", `tag like "tag1%"`, func(i int) bool { return i%3 == 1 }},
		{"array_contains", "array_contains(items, 1)", func(i int) bool { return arrayHas(i, 1) }},
		{"array_contains_any", "array_contains_any(items, [1, 3])", func(i int) bool { return arrayHas(i, 1) || arrayHas(i, 3) }},
		{"array_contains_all", "array_contains_all(items, [0, 1])", func(i int) bool { return arrayHas(i, 0) && arrayHas(i, 1) }},
		{"array_equality", "items == [0, 1]", func(i int) bool { return valid(i) && i%4 == 0 }},
		{"json_range", `2 <= meta["n"] < 6`, func(i int) bool { return numeric(i) && i%10 >= 2 && i%10 < 6 }},
		{"json_in", `meta["n"] in [1, 3, 7]`, func(i int) bool { return numeric(i) && (i%10 == 1 || i%10 == 3 || i%10 == 7) }},
		// EXISTS excludes explicit JSON null as well as missing paths/NULL rows.
		{"json_exists", `exists meta["n"]`, func(i int) bool { return i%6 < 3 }},
		{"json_missing", `exists meta["missing"]`, func(int) bool { return false }},
		{"json_contains", `json_contains(meta["arr"], 1)`, func(i int) bool { return jsonHas(i, 1) }},
		{"json_contains_any", `json_contains_any(meta["arr"], [1,3])`, func(i int) bool { return jsonHas(i, 1) || jsonHas(i, 3) }},
		{"json_contains_all", `json_contains_all(meta["arr"], [0,1])`, func(i int) bool { return jsonHas(i, 0) && jsonHas(i, 1) }},
		{"text_match", `text_match(text, "green")`, func(i int) bool { return i%6 == 1 || i%6 == 4 }},
		{"text_fuzzy", `text_match_fuzzy(text, "gren", max_edit_distance=1)`, func(i int) bool { return i%6 == 1 || i%6 == 4 }},
		{"mixed_leaf_filters", `score >= 3 and phrase_match(text, "red blue", 0)`, func(i int) bool { return i%10 >= 3 && exact(i) }},
		{"exact", p, exact},
		{"slop_one", `phrase_match(text, "red blue", 1)`, slop},
		{"different_phrase", r, reverse},
		{"no_matches", `phrase_match(text, "violet orange", 0)`, func(int) bool { return false }},
		{"not_nullable", "not (" + p + ")", func(i int) bool { return i%6 != 5 && !exact(i) }},
		{"and_scalar_first", "pk < 1200 and " + p, func(i int) bool { return i < 1200 && exact(i) }},
		{"and_phrase_first", p + " and pk < 1200", func(i int) bool { return i < 1200 && exact(i) }},
		{"or_including_null", p + " or pk % 6 == 5", func(i int) bool { return exact(i) || i%6 == 5 }},
		{"two_leaves", p + " and " + r, func(i int) bool { return exact(i) && reverse(i) }},
		{"two_leaves_reversed", r + " and " + p, func(i int) bool { return exact(i) && reverse(i) }},
	}
}

func (s *phraseFanoutSuite) fanout(ctx context.Context, name string, tc phraseCase) {
	requests := make([]*milvuspb.QueryRequest, 7)
	for i := range requests {
		requests[i] = &milvuspb.QueryRequest{
			CollectionName: name, Expr: tc.expr,
			ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
			OutputFields:     []string{"pk"}, QueryParams: []*commonpb.KeyValuePair{{Key: "limit", Value: strconv.Itoa(rowCount)}},
		}
		if i > 0 {
			field := fmt.Sprintf("facet%d", i-1)
			requests[i].OutputFields = []string{field, "count(pk)"}
			requests[i].QueryParams = []*commonpb.KeyValuePair{{Key: "group_by_fields", Value: field}}
		}
	}
	type outcome struct {
		response *milvuspb.QueryResults
		err      error
	}
	results := make([]outcome, len(requests))
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(len(requests))
	done.Add(len(requests))
	for i := range requests {
		go func(i int) {
			defer done.Done()
			ready.Done()
			<-start
			queryCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			results[i].response, results[i].err = s.Cluster.MilvusClient.Query(queryCtx, requests[i])
		}(i)
	}
	ready.Wait()
	close(start)
	done.Wait()
	expectedIDs := make([]int64, 0)
	for id := 0; id < rowCount; id++ {
		if tc.matches(id) && !s.deleted[id] {
			expectedIDs = append(expectedIDs, int64(id))
		}
	}
	for i, out := range results {
		s.Require().NoError(merr.CheckRPCCall(out.response, out.err), "request %d: %s", i, tc.expr)
		fields := make(map[string][]int64)
		for _, f := range out.response.GetFieldsData() {
			fields[f.GetFieldName()] = f.GetScalars().GetLongData().GetData()
		}
		if i == 0 {
			actual := append([]int64{}, fields["pk"]...)
			sort.Slice(actual, func(a, b int) bool { return actual[a] < actual[b] })
			s.Equal(expectedIDs, actual, tc.expr)
			continue
		}
		expected := map[int64]int64{}
		for _, id := range expectedIDs {
			expected[category(int(id), i-1)]++
		}
		actual := map[int64]int64{}
		keys, counts := fields[fmt.Sprintf("facet%d", i-1)], fields["count(pk)"]
		s.Require().Len(counts, len(keys), "aggregation fields: %v", fields)
		for row, key := range keys {
			_, duplicate := actual[key]
			s.False(duplicate, "duplicate group %d", key)
			actual[key] = counts[row]
		}
		s.Equal(expected, actual, "facet %d: %s", i-1, tc.expr)
	}
}

func (s *phraseFanoutSuite) TestQueryAndSixFacets() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	name := s.create(ctx)
	for _, tc := range cases() {
		s.Run(tc.name, func() {
			// First wave plus repeated warm requests. Composite cases reuse
			// leaf keys across different query shapes deliberately.
			for wave := 0; wave < 2; wave++ {
				s.fanout(ctx, name, tc)
			}
		})
	}
	s.Run("delete_after_warm_cache", func() {
		deleted, err := s.Cluster.MilvusClient.Delete(ctx, &milvuspb.DeleteRequest{CollectionName: name, Expr: "pk in [3, 7, 13]"})
		s.Require().NoError(merr.CheckRPCCall(deleted, err))
		s.deleted = map[int]bool{3: true, 7: true, 13: true}
		for _, tc := range cases() {
			if tc.name == "scalar_equal" || tc.name == "json_exists" || tc.name == "exact" {
				s.fanout(ctx, name, tc)
			}
		}
	})
	s.Run("release_and_reload", func() {
		status, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: name})
		s.Require().NoError(merr.CheckRPCCall(status, err))
		s.load(ctx, name)
		s.fanout(ctx, name, cases()[0])
	})
}

func TestExprCacheFlightFanout(t *testing.T) {
	for _, cfg := range []struct {
		name, mode                      string
		enabled, limited, cacheDisabled bool
	}{
		{"cache_disabled", "memory", false, false, true},
		{"disabled_memory", "memory", false, false, false},
		{"enabled_memory", "memory", true, false, false},
		{"enabled_disk", "disk", true, false, false},
		{"limited_memory", "memory", true, true, false},
		{"limited_disk", "disk", true, true, false},
	} {
		t.Run(cfg.name, func(t *testing.T) {
			suite.Run(t, &phraseFanoutSuite{mode: cfg.mode, enabled: cfg.enabled, limited: cfg.limited, cacheDisabled: cfg.cacheDisabled})
		})
	}
}
