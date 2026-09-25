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
	"math"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

// execField is one scalar field of the execution-feature collection and the
// index built on it, if any.
type execField struct {
	schema *schemapb.FieldSchema
	index  []*commonpb.KeyValuePair
}

func varcharField(id int64, name string, extra ...*commonpb.KeyValuePair) *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		FieldID: id, Name: name, DataType: schemapb.DataType_VarChar,
		TypeParams: append([]*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "64"}}, extra...),
	}
}

func indexOf(indexType string, extra ...*commonpb.KeyValuePair) []*commonpb.KeyValuePair {
	return append([]*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: indexType}}, extra...)
}

// createIndexedCollection creates a collection with an 8-dim vector plus the
// given fields, inserts rows built by columns, flushes, builds every index and
// loads it, so each query meets sealed segments with loaded indexes.
func (s *Suite) createIndexedCollection(ctx context.Context, prefix string, fields []execField, rows int,
	columns func(rows int) []*schemapb.FieldData, properties ...*commonpb.KeyValuePair,
) string {
	name := prefix + funcutil.GenRandomStr()
	schema := &schemapb.CollectionSchema{
		Name: name,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: pkField, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID: 101, Name: vectorField, DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: fmt.Sprint(s.dim)}},
			},
		},
	}
	for _, f := range fields {
		schema.Fields = append(schema.Fields, f.schema)
	}
	marshaled, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: name, Schema: marshaled, ShardsNum: 1, Properties: properties,
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, status.GetErrorCode(), status.GetReason())

	pks := make([]int64, rows)
	for i := range pks {
		pks[i] = int64(i)
	}
	data := append([]*schemapb.FieldData{newInt64Column(pkField, pks)}, columns(rows)...)
	insert, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: name, FieldsData: data,
		HashKeys: integration.GenerateHashKeys(rows), NumRows: uint32(rows),
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, insert.GetStatus().GetErrorCode(), insert.GetStatus().GetReason())
	flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{name}})
	s.Require().NoError(err)
	s.WaitForFlush(ctx, flush.GetCollSegIDs()[name].GetData(), flush.GetCollFlushTs()[name], "", name)

	indexes := map[string][]*commonpb.KeyValuePair{
		vectorField: integration.ConstructIndexParam(s.dim, integration.IndexFaissIvfFlat, metric.L2),
	}
	for _, f := range fields {
		if f.index != nil {
			indexes[f.schema.GetName()] = f.index
		}
	}
	for field, params := range indexes {
		index, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
			CollectionName: name, FieldName: field, IndexName: field + "_idx", ExtraParams: params,
		})
		s.Require().NoError(err)
		s.Require().Equal(commonpb.ErrorCode_Success, index.GetErrorCode(), "%s: %s", field, index.GetReason())
		s.WaitForIndexBuilt(ctx, name, field)
	}
	load, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, load.GetErrorCode(), load.GetReason())
	s.WaitForLoad(ctx, name)
	return name
}

// queryExec issues one query with a filter and returns the execution features
// it moved.
func (s *Suite) queryExec(ctx context.Context, collection, expr string) map[string]int64 {
	return s.queryExecWith(ctx, collection, expr, nil)
}

func (s *Suite) queryExecWith(ctx context.Context, collection, expr string, templates map[string]*schemapb.TemplateValue) map[string]int64 {
	before := execFeatures(s.report(ctx))
	resp, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:        collection,
		Expr:                  expr,
		ExprTemplateValues:    templates,
		OutputFields:          []string{"count(*)"},
		UseDefaultConsistency: true,
	})
	s.Require().NoError(err, "transport error")
	s.Require().Equal(commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode(), "%s: %s", expr, resp.GetStatus().GetReason())
	return deltaOf(before, execFeatures(s.report(ctx)))
}

func deltaOf(before, after map[string]int64) map[string]int64 {
	moved := map[string]int64{}
	for name, v := range after {
		if d := v - before[name]; d != 0 {
			moved[name] = d
		}
	}
	return moved
}

// eventuallyExec repeats run until the feature appears among the execution
// features it moved. For features that depend on background work finishing
// first -- JSON statistics built after a flush, the interim index trained on
// growing data -- a single request cannot be timed exactly.
func (s *Suite) eventuallyExec(ctx context.Context, feature string, run func() map[string]int64) {
	deadline := time.Now().Add(2 * time.Minute)
	for {
		moved := run()
		if moved[feature] > 0 {
			return
		}
		if time.Now().After(deadline) {
			s.FailNowf("execution feature never moved", "%s; last request moved %v", feature, moved)
		}
		time.Sleep(time.Second)
	}
}

// eventuallyExactly repeats run until the execution features it moved are
// exactly want, for state the test cannot wait on directly, such as the
// indexes of a segment that compaction just produced. A retry repeats the
// same filter, which the expression cache then rightly serves, so a cache
// hit on a retry is not held against it.
func (s *Suite) eventuallyExactly(ctx context.Context, want map[string]int64, run func() map[string]int64) {
	deadline := time.Now().Add(3 * time.Minute)
	for attempt := 0; ; attempt++ {
		moved := run()
		if _, wanted := want["expr_cache_hit"]; attempt > 0 && !wanted {
			delete(moved, "expr_cache_hit")
		}
		if len(moved) == len(want) {
			same := true
			for k, v := range want {
				if moved[k] != v {
					same = false
				}
			}
			if same {
				return
			}
		}
		if time.Now().After(deadline) {
			s.Require().Equal(want, moved, "execution features moved by the last request")
		}
		time.Sleep(time.Second)
	}
}

const (
	execSort    = "f_sort"
	execInv     = "f_inv"
	execInvStr  = "f_inv_str"
	execBitmap  = "f_bitmap"
	execTrie    = "f_trie"
	execHybrid  = "f_hybrid"
	execNgram   = "f_ngram"
	execFM      = "f_fm"
	execGeo     = "f_geo"
	execJSON    = "f_json"
	execFlat    = "f_flat"
	execRaw     = "f_raw"
	execText    = "f_text"
	execGroupBy = "f_group"
)

// TestExecFeatures drives every execution feature the QueryNodes report: each
// filter path and scalar index kind one query at a time on sealed segments
// with loaded indexes, an index the operator cannot use, a cached expression
// result, JSON statistics, the interim index on growing data, the second
// phase of a strict grouping search, and a read from remote storage.
func (s *Suite) TestExecFeatures() {
	ctx := context.Background()
	s.ensureCollection(ctx)

	// Above indexCoord.segment.minSegmentNumRowsToEnableIndex (1024): a
	// smaller segment is never indexed, and its filters rightly report raw data.
	const rows = 2000
	texts := func(prefix string, n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("%s row %d", prefix, i)
		}
		return out
	}
	ints := func(n int) []int64 {
		out := make([]int64, n)
		for i := range out {
			out[i] = int64(i % 50)
		}
		return out
	}
	name := s.createIndexedCollection(ctx, "fu_exec_", []execField{
		{&schemapb.FieldSchema{FieldID: 102, Name: execSort, DataType: schemapb.DataType_Int64}, indexOf("STL_SORT")},
		{&schemapb.FieldSchema{FieldID: 103, Name: execInv, DataType: schemapb.DataType_Int64}, indexOf("INVERTED")},
		{varcharField(104, execInvStr), indexOf("INVERTED")},
		{varcharField(105, execBitmap), indexOf("BITMAP")},
		{varcharField(106, execTrie), indexOf("Trie")},
		// HYBRID cannot be named directly; it is what AUTOINDEX builds on an integer.
		{&schemapb.FieldSchema{FieldID: 107, Name: execHybrid, DataType: schemapb.DataType_Int64}, indexOf("AUTOINDEX")},
		{varcharField(108, execNgram), indexOf("NGRAM", &commonpb.KeyValuePair{Key: "min_gram", Value: "2"}, &commonpb.KeyValuePair{Key: "max_gram", Value: "3"})},
		{varcharField(109, execFM), indexOf("FMINDEX")},
		{&schemapb.FieldSchema{FieldID: 110, Name: execGeo, DataType: schemapb.DataType_Geometry}, indexOf("RTREE")},
		{&schemapb.FieldSchema{FieldID: 111, Name: execJSON, DataType: schemapb.DataType_JSON}, nil},
		{&schemapb.FieldSchema{FieldID: 112, Name: execFlat, DataType: schemapb.DataType_JSON}, indexOf("INVERTED",
			&commonpb.KeyValuePair{Key: common.JSONCastTypeKey, Value: "JSON"},
			&commonpb.KeyValuePair{Key: common.JSONPathKey, Value: execFlat})},
		{&schemapb.FieldSchema{FieldID: 113, Name: execRaw, DataType: schemapb.DataType_Int64}, nil},
		{varcharField(114, execText, &commonpb.KeyValuePair{Key: "enable_analyzer", Value: "true"}, &commonpb.KeyValuePair{Key: "enable_match", Value: "true"}), nil},
	}, rows, func(n int) []*schemapb.FieldData {
		wkt := make([]string, n)
		docs := make([][]byte, n)
		for i := range wkt {
			wkt[i] = fmt.Sprintf("POINT (%d %d)", i%20, i%20)
			docs[i] = []byte(fmt.Sprintf(`{"a": %d, "b": "v%d"}`, i%50, i))
		}
		return []*schemapb.FieldData{
			integration.NewFloatVectorFieldData(vectorField, n, s.dim),
			newInt64Column(execSort, ints(n)),
			newInt64Column(execInv, ints(n)),
			newVarCharColumn(execInvStr, texts("inv", n)),
			newVarCharColumn(execBitmap, texts("b", n)),
			newVarCharColumn(execTrie, texts("trie", n)),
			newInt64Column(execHybrid, ints(n)),
			newVarCharColumn(execNgram, texts("ngram", n)),
			newVarCharColumn(execFM, texts("fm", n)),
			newGeometryColumn(execGeo, wkt),
			newJSONColumn(execJSON, docs),
			newJSONColumn(execFlat, docs),
			newInt64Column(execRaw, ints(n)),
			newVarCharColumn(execText, texts("milvus", n)),
		}
	})

	scalar := func(indexType string) map[string]int64 {
		return map[string]int64{"filter_exec_path=scalar_index": 1, "scalar_index_type=" + indexType: 1}
	}
	cases := []struct {
		name string
		expr string
		want map[string]int64
	}{
		{"STL_SORT", execSort + " == 5", scalar("STL_SORT")},
		{"INVERTED", execInv + " == 5", scalar("INVERTED")},
		{"BITMAP", execBitmap + ` == "b row 5"`, scalar("BITMAP")},
		{"Trie", execTrie + ` == "trie row 5"`, scalar("Trie")},
		{"HYBRID", execHybrid + " == 5", scalar("HYBRID")},
		// A literal matching few rows: FMINDEX declines high-hit literals to raw data.
		{"FMINDEX", execFM + ` like "%fm row 1234%"`, scalar("FMINDEX")},
		{"RTREE", fmt.Sprintf(`st_intersects(%s, "POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0))")`, execGeo), scalar("RTREE")},
		{"ngram_index", execNgram + ` like "%row 17%"`, map[string]int64{"filter_exec_path=ngram_index": 1, "scalar_index_type=NGRAM": 1}},
		{"pk_index", pkField + " == 7", map[string]int64{"filter_exec_path=pk_index": 1}},
		{"text_match_index", fmt.Sprintf(`text_match(%s, "milvus")`, execText), map[string]int64{"filter_exec_path=text_match_index": 1}},
		{"brute_force", execRaw + " > 5", map[string]int64{"filter_exec_path=brute_force": 1}},
		// An INVERTED index serves prefix LIKE only; an infix LIKE on the same
		// field falls back to raw data with the index in place.
		{"filter_index_declined", execInvStr + ` like "%row 1%"`, map[string]int64{"filter_exec_path=brute_force": 1, "filter_index_declined": 1}},
		{"indexed and raw in one filter", execSort + " == 13 and " + execRaw + " > 5", map[string]int64{
			"filter_exec_path=scalar_index": 1, "scalar_index_type=STL_SORT": 1, "filter_exec_path=brute_force": 1,
		}},
	}
	// The suite compacts eagerly, so the flushed segment may be replaced by a
	// sorted one whose scalar indexes are built after it is loaded; until they
	// are, filters on it run on raw data. Wait for the indexes to serve.
	s.eventuallyExactly(ctx, scalar("INVERTED"), func() map[string]int64 {
		return s.queryExec(ctx, name, execInv+" == 1")
	})
	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.eventuallyExactly(ctx, tc.want, func() map[string]int64 {
				return s.queryExec(ctx, name, tc.expr)
			})
		})
	}

	s.Run("json_shredding", func() {
		// JSON statistics are built in the background after the flush.
		s.eventuallyExec(ctx, "filter_exec_path=json_shredding", func() map[string]int64 {
			return s.queryExec(ctx, name, execJSON+`["a"] == 7`)
		})
	})

	s.Run("json_flat", func() {
		// JSON statistics take precedence over a JSON index on the same
		// field, so the request turns them off to reach the flat index.
		noStats := map[string]*schemapb.TemplateValue{
			common.ExprUseJSONStatsKey: {Val: &schemapb.TemplateValue_BoolVal{BoolVal: false}},
		}
		s.eventuallyExactly(ctx, scalar("json_flat"), func() map[string]int64 {
			return s.queryExecWith(ctx, name, execFlat+`["a"] == 7`, noStats)
		})
	})

	s.Run("expr_cache_hit", func() {
		// The JSON statistics path caches its result per segment and filter:
		// the first run of a filter fills the cache, the same filter again is
		// served from it.
		expr := execJSON + `["a"] == 11`
		s.Equal(map[string]int64{"filter_exec_path=json_shredding": 1}, s.queryExec(ctx, name, expr))
		s.Equal(map[string]int64{"filter_exec_path=json_shredding": 1, "expr_cache_hit": 1}, s.queryExec(ctx, name, expr))
	})

	s.Run("interim_index_search", s.driveInterimIndex)
	s.Run("strict_group_size_effective", s.driveStrictGroupPhase2)
	s.Run("tiered_storage_cold_read", s.driveColdRead)
}

// driveInterimIndex searches data that is still growing, which the interim
// index serves once it has trained on enough rows.
func (s *Suite) driveInterimIndex() {
	ctx := context.Background()
	name := "fu_growing_" + funcutil.GenRandomStr()
	schema := integration.ConstructSchema(name, s.dim, true)
	marshaled, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: name, Schema: marshaled, ShardsNum: 1,
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, status.GetErrorCode(), status.GetReason())
	index, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: name, FieldName: integration.FloatVecField, IndexName: "_default",
		ExtraParams: integration.ConstructIndexParam(s.dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, index.GetErrorCode(), index.GetReason())
	load, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, load.GetErrorCode(), load.GetReason())
	s.WaitForLoad(ctx, name)

	const rows = 20000
	insert, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: name,
		FieldsData:     []*schemapb.FieldData{integration.NewFloatVectorFieldData(integration.FloatVecField, rows, s.dim)},
		HashKeys:       integration.GenerateHashKeys(rows),
		NumRows:        uint32(rows),
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, insert.GetStatus().GetErrorCode(), insert.GetStatus().GetReason())

	s.eventuallyExec(ctx, "interim_index_search", func() map[string]int64 {
		before := execFeatures(s.report(ctx))
		req := integration.ConstructSearchRequest("", name, "", integration.FloatVecField,
			schemapb.DataType_FloatVector, nil, metric.L2, map[string]any{"nprobe": 4}, 1, s.dim, 5, -1)
		req.UseDefaultConsistency = false
		req.ConsistencyLevel = commonpb.ConsistencyLevel_Strong
		_, err := s.Cluster.MilvusClient.Search(ctx, req)
		s.Require().NoError(err, "transport error")
		return deltaOf(before, execFeatures(s.report(ctx)))
	})
}

// driveStrictGroupPhase2 builds a sealed collection where one group is rare
// and the query vector sits on it: the first pass finds both groups, fills the
// common one, and probes almost nothing from the rare one, which is when the
// search re-runs restricted to the unfinished group.
func (s *Suite) driveStrictGroupPhase2() {
	ctx := context.Background()
	const rows = 1000
	name := s.createIndexedCollection(ctx, "fu_strict_", []execField{
		{&schemapb.FieldSchema{FieldID: 102, Name: execGroupBy, DataType: schemapb.DataType_Int64}, nil},
	}, rows, func(n int) []*schemapb.FieldData {
		groups := make([]int64, n)
		vectors := make([]float32, 0, n*s.dim)
		for i := 0; i < n; i++ {
			v := float32(i%97) / 97
			switch i {
			case 0: // the rare group's row next to the query
				groups[i] = 1
				v = 0.5
			case n - 1, n - 2: // the rest of the rare group, far away
				groups[i] = 1
				v = 100
			}
			for d := 0; d < s.dim; d++ {
				vectors = append(vectors, v)
			}
		}
		return []*schemapb.FieldData{
			{
				Type: schemapb.DataType_FloatVector, FieldName: vectorField,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim: int64(s.dim), Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: vectors}},
				}},
			},
			newInt64Column(execGroupBy, groups),
		}
	})

	query := make([]float32, s.dim)
	for d := range query {
		query[d] = 0.5
	}
	before := execFeatures(s.report(ctx))
	req := integration.ConstructSearchRequest("", name, "", vectorField,
		schemapb.DataType_FloatVector, nil, metric.L2, map[string]any{"nprobe": 128}, 1, s.dim, 2, -1)
	req.SearchInput = &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: floatPlaceholder(s, query)}
	req.UseDefaultConsistency = true
	withSearchParam("group_by_field", execGroupBy)(req)
	withSearchParam("group_size", "3")(req)
	withSearchParam("strict_group_size", "true")(req)
	resp, err := s.Cluster.MilvusClient.Search(ctx, req)
	s.Require().NoError(err, "transport error")
	s.Require().Equal(commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode(), resp.GetStatus().GetReason())
	s.Equal(map[string]int64{"strict_group_size_effective": 1}, deltaOf(before, execFeatures(s.report(ctx))))
}

func floatPlaceholder(s *Suite, vector []float32) []byte {
	value := make([]byte, 0, len(vector)*4)
	for _, f := range vector {
		value = common.Endian.AppendUint32(value, math.Float32bits(f))
	}
	plg, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{
		Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: [][]byte{value},
	}}})
	s.Require().NoError(err)
	return plg
}

// driveColdRead queries a collection whose scalar data is not warmed up at
// load, so the first filter over it reads from remote storage.
func (s *Suite) driveColdRead() {
	ctx := context.Background()
	name := s.createIndexedCollection(ctx, "fu_cold_", []execField{
		{&schemapb.FieldSchema{FieldID: 102, Name: execRaw, DataType: schemapb.DataType_Int64}, nil},
	}, 300, func(n int) []*schemapb.FieldData {
		vals := make([]int64, n)
		for i := range vals {
			vals[i] = int64(i)
		}
		return []*schemapb.FieldData{
			integration.NewFloatVectorFieldData(vectorField, n, s.dim),
			newInt64Column(execRaw, vals),
		}
	}, &commonpb.KeyValuePair{Key: common.WarmupScalarFieldKey, Value: "disable"})
	s.Equal(map[string]int64{"filter_exec_path=brute_force": 1, "tiered_storage_cold_read": 1},
		s.queryExec(ctx, name, execRaw+" > 5"))
}
