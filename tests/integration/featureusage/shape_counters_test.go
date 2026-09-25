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

// searchUnits is what one ANN subrequest adds to the per-subrequest counters:
// one bucket of each distribution and one retrieval kind.
func searchUnits(ef, nprobe, limit, nq, retrieval string) map[string]int64 {
	return map[string]int64{
		"ef|" + ef:               1,
		"nprobe|" + nprobe:       1,
		"limit|" + limit:         1,
		"nq|" + nq:               1,
		"retrieval=" + retrieval: 1,
	}
}

// TestSearchParameterDistributions drives every bucket of the ef, nprobe,
// limit and nq distributions with one plain search each, and asserts on the
// per-subrequest counters only: which request-level counters a search moves
// is what the other tests pin.
func (s *Suite) TestSearchParameterDistributions() {
	ctx := context.Background()
	s.ensureCollection(ctx)

	search := func(params map[string]any, topk, nq int, shape func(*milvuspb.SearchRequest)) {
		req := integration.ConstructSearchRequest("", s.collection, "", vectorField,
			schemapb.DataType_FloatVector, nil, metric.L2, params, nq, s.dim, topk, -1)
		req.UseDefaultConsistency = true
		if shape != nil {
			shape(req)
		}
		_, err := s.Cluster.MilvusClient.Search(ctx, req)
		s.Require().NoError(err, "transport error")
	}

	cases := []struct {
		name   string
		params map[string]any
		topk   int
		nq     int
		shape  func(*milvuspb.SearchRequest)
		want   map[string]int64
	}{
		{"nothing tuned", map[string]any{}, 5, 1, nil, searchUnits("omitted", "omitted", "<=10", "1", "dense_vector")},
		{"ef <=16", map[string]any{"ef": 8}, 5, 1, nil, searchUnits("<=16", "omitted", "<=10", "1", "dense_vector")},
		{"ef 17-64", map[string]any{"ef": 64}, 5, 1, nil, searchUnits("17-64", "omitted", "<=10", "1", "dense_vector")},
		{"ef 65-256", map[string]any{"ef": 200}, 5, 1, nil, searchUnits("65-256", "omitted", "<=10", "1", "dense_vector")},
		{"ef 257-1024", map[string]any{"ef": 512}, 5, 1, nil, searchUnits("257-1024", "omitted", "<=10", "1", "dense_vector")},
		{"ef >1024", map[string]any{"ef": 2048}, 5, 1, nil, searchUnits(">1024", "omitted", "<=10", "1", "dense_vector")},
		{"nprobe <=8", map[string]any{"nprobe": 4}, 5, 1, nil, searchUnits("omitted", "<=8", "<=10", "1", "dense_vector")},
		{"nprobe 9-32", map[string]any{"nprobe": 16}, 5, 1, nil, searchUnits("omitted", "9-32", "<=10", "1", "dense_vector")},
		{"nprobe 33-128", map[string]any{"nprobe": 64}, 5, 1, nil, searchUnits("omitted", "33-128", "<=10", "1", "dense_vector")},
		{"nprobe 129-1024", map[string]any{"nprobe": 512}, 5, 1, nil, searchUnits("omitted", "129-1024", "<=10", "1", "dense_vector")},
		{"nprobe >1024", map[string]any{"nprobe": 2048}, 5, 1, nil, searchUnits("omitted", ">1024", "<=10", "1", "dense_vector")},
		{"limit 11-100", map[string]any{"nprobe": 4}, 50, 1, nil, searchUnits("omitted", "<=8", "11-100", "1", "dense_vector")},
		{"limit 101-1000", map[string]any{"nprobe": 4}, 500, 1, nil, searchUnits("omitted", "<=8", "101-1000", "1", "dense_vector")},
		{"limit 1001-16384", map[string]any{"nprobe": 4}, 2000, 1, nil, searchUnits("omitted", "<=8", "1001-16384", "1", "dense_vector")},
		// A limit above the top-k cap is rejected, except on the old search
		// iterator, which clamps it; the requested value is what counts.
		{
			"limit >16384 on the search iterator",
			map[string]any{"nprobe": 4},
			20000, 1,
			withSearchParam("iterator", "true"),
			searchUnits("omitted", "<=8", ">16384", "1", "dense_vector"),
		},
		{"nq 2-10", map[string]any{"nprobe": 4}, 5, 5, nil, searchUnits("omitted", "<=8", "<=10", "2-10", "dense_vector")},
		{"nq 11-100", map[string]any{"nprobe": 4}, 5, 50, nil, searchUnits("omitted", "<=8", "<=10", "11-100", "dense_vector")},
		{"nq 101-1000", map[string]any{"nprobe": 4}, 5, 500, nil, searchUnits("omitted", "<=8", "<=10", "101-1000", "dense_vector")},
		{"nq >1000", map[string]any{"nprobe": 4}, 5, 1001, nil, searchUnits("omitted", "<=8", "<=10", ">1000", "dense_vector")},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			before := units(s.report(ctx))
			search(tc.params, tc.topk, tc.nq, tc.shape)
			requireOnlyDelta(s.T(), before, units(s.report(ctx)), tc.want)
		})
	}

	s.Run("a hybrid search counts each subrequest", func() {
		before := units(s.report(ctx))
		beforeReq := counters(s.report(ctx), typeutil.ProxyRole)
		subs := make([]*milvuspb.SearchRequest, 3)
		for i := range subs {
			subs[i] = s.baseSearchRequest()
		}
		// Subrequests must agree on nq; HybridSearch rejects a mix before a
		// task exists.
		subs[2] = integration.ConstructSearchRequest("", s.collection, "", vectorField,
			schemapb.DataType_FloatVector, nil, metric.L2, map[string]any{"ef": 64}, 1, s.dim, 50, -1)
		s.hybridSearchOf(ctx, s.collection, subs)
		requireOnlyDelta(s.T(), before, units(s.report(ctx)), map[string]int64{
			"ef|omitted":             2,
			"ef|17-64":               1,
			"nprobe|<=8":             2,
			"nprobe|omitted":         1,
			"limit|<=10":             2,
			"limit|11-100":           1,
			"nq|1":                   3,
			"retrieval=dense_vector": 3,
		})
		requireOnlyDelta(s.T(), beforeReq, counters(s.report(ctx), typeutil.ProxyRole), map[string]int64{
			"strategy=rrf":               1,
			"hybrid_search_reqs|3":       1,
			"hybrid_search=dense_vector": 1,
		})
	})

	for _, tc := range []struct {
		n      int
		bucket string
	}{{5, "4-5"}, {8, "6-10"}, {11, ">10"}} {
		s.Run("hybrid_search_reqs|"+tc.bucket, func() {
			before := counters(s.report(ctx), typeutil.ProxyRole)
			subs := make([]*milvuspb.SearchRequest, tc.n)
			for i := range subs {
				subs[i] = s.baseSearchRequest()
			}
			s.hybridSearchOf(ctx, s.collection, subs)
			requireOnlyDelta(s.T(), before, counters(s.report(ctx), typeutil.ProxyRole), map[string]int64{
				"strategy=rrf":                    1,
				"hybrid_search_reqs|" + tc.bucket: 1,
				"hybrid_search=dense_vector":      1,
			})
		})
	}
}

// hybridSearchOf issues a hybrid search over the given subrequests with an
// RRF ranker.
func (s *Suite) hybridSearchOf(ctx context.Context, collection string, subs []*milvuspb.SearchRequest) {
	for _, sub := range subs {
		sub.CollectionName = collection
		sub.UseDefaultConsistency = true
	}
	_, err := s.Cluster.MilvusClient.HybridSearch(ctx, &milvuspb.HybridSearchRequest{
		CollectionName: collection,
		Requests:       subs,
		RankParams: []*commonpb.KeyValuePair{
			{Key: "strategy", Value: "rrf"},
			{Key: "params", Value: `{"k": 60}`},
			{Key: "limit", Value: "5"},
		},
		UseDefaultConsistency: true,
	})
	s.Require().NoError(err, "transport error")
}

const (
	retrievalDense  = "dense"
	retrievalSparse = "sparse"
	retrievalText   = "text"
	retrievalBM25   = "bm25"
)

// createRetrievalCollection builds an empty, loaded collection with a dense
// vector, a sparse vector, and a BM25 function output, so one collection can
// take every retrieval family.
func (s *Suite) createRetrievalCollection(ctx context.Context) string {
	name := "fu_retrieval_" + funcutil.GenRandomStr()
	schema := &schemapb.CollectionSchema{
		Name: name,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: pkField, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{
				FieldID: 101, Name: retrievalText, DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.MaxLengthKey, Value: "256"},
					{Key: "enable_analyzer", Value: "true"},
				},
			},
			{
				FieldID: 102, Name: retrievalDense, DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: fmt.Sprint(s.dim)}},
			},
			{FieldID: 103, Name: retrievalSparse, DataType: schemapb.DataType_SparseFloatVector},
			{FieldID: 104, Name: retrievalBM25, DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		Functions: []*schemapb.FunctionSchema{{
			Name:             "bm25",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{retrievalText},
			OutputFieldNames: []string{retrievalBM25},
		}},
	}
	marshaled, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: name, Schema: marshaled, ShardsNum: 1,
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, status.GetErrorCode(), status.GetReason())

	for field, params := range map[string][]*commonpb.KeyValuePair{
		retrievalDense:  integration.ConstructIndexParam(s.dim, integration.IndexFaissIvfFlat, metric.L2),
		retrievalSparse: {{Key: common.IndexTypeKey, Value: "SPARSE_INVERTED_INDEX"}, {Key: common.MetricTypeKey, Value: metric.IP}},
		retrievalBM25:   {{Key: common.IndexTypeKey, Value: "SPARSE_INVERTED_INDEX"}, {Key: common.MetricTypeKey, Value: metric.BM25}},
	} {
		index, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
			CollectionName: name, FieldName: field, IndexName: field + "_idx", ExtraParams: params,
		})
		s.Require().NoError(err)
		s.Require().Equal(commonpb.ErrorCode_Success, index.GetErrorCode(), index.GetReason())
		s.WaitForIndexBuilt(ctx, name, field)
	}

	load, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, load.GetErrorCode(), load.GetReason())
	s.WaitForLoad(ctx, name)
	return name
}

// retrievalSearch builds one search of the given family against the
// retrieval collection.
func (s *Suite) retrievalSearch(collection, family string) *milvuspb.SearchRequest {
	switch family {
	case retrievalSparse:
		return integration.ConstructSearchRequest("", collection, "", retrievalSparse,
			schemapb.DataType_SparseFloatVector, nil, metric.IP, map[string]any{}, 1, s.dim, 5, -1)
	case retrievalBM25:
		req := integration.ConstructSearchRequest("", collection, "", retrievalBM25,
			schemapb.DataType_SparseFloatVector, nil, metric.BM25, map[string]any{}, 1, s.dim, 5, -1)
		plg, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{
			Tag:    "$0",
			Type:   commonpb.PlaceholderType_VarChar,
			Values: [][]byte{[]byte("milvus feature usage")},
		}}})
		s.Require().NoError(err)
		req.SearchInput = &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: plg}
		return req
	default:
		return integration.ConstructSearchRequest("", collection, "", retrievalDense,
			schemapb.DataType_FloatVector, nil, metric.L2, map[string]any{}, 1, s.dim, 5, -1)
	}
}

// TestRetrievalKinds drives the sparse and full-text retrieval kinds with a
// plain search each, then every combination of retrieval families a hybrid
// search can put together.
func (s *Suite) TestRetrievalKinds() {
	ctx := context.Background()
	s.ensureCollection(ctx)
	name := s.createRetrievalCollection(ctx)

	for _, tc := range []struct {
		family string
		want   string
	}{{retrievalSparse, "sparse_vector"}, {retrievalBM25, "full_text_search"}, {retrievalDense, "dense_vector"}} {
		s.Run("retrieval="+tc.want, func() {
			before := units(s.report(ctx))
			req := s.retrievalSearch(name, tc.family)
			req.UseDefaultConsistency = true
			_, err := s.Cluster.MilvusClient.Search(ctx, req)
			s.Require().NoError(err, "transport error")
			requireOnlyDelta(s.T(), before, units(s.report(ctx)), searchUnits("omitted", "omitted", "<=10", "1", tc.want))
		})
	}

	for _, tc := range []struct {
		families []string
		want     string
	}{
		{[]string{retrievalSparse, retrievalSparse}, "sparse_vector"},
		{[]string{retrievalBM25}, "full_text_search"},
		{[]string{retrievalDense, retrievalSparse}, "dense_vector+sparse_vector"},
		{[]string{retrievalBM25, retrievalDense}, "dense_vector+full_text_search"},
		{[]string{retrievalSparse, retrievalBM25}, "sparse_vector+full_text_search"},
		{[]string{retrievalDense, retrievalSparse, retrievalBM25}, "dense_vector+sparse_vector+full_text_search"},
	} {
		s.Run("hybrid_search="+tc.want, func() {
			before := counters(s.report(ctx), typeutil.ProxyRole)
			subs := make([]*milvuspb.SearchRequest, 0, len(tc.families))
			for _, f := range tc.families {
				subs = append(subs, s.retrievalSearch(name, f))
			}
			s.hybridSearchOf(ctx, name, subs)
			reqs := "<=2"
			if len(subs) == 3 {
				reqs = "3"
			}
			requireOnlyDelta(s.T(), before, counters(s.report(ctx), typeutil.ProxyRole), map[string]int64{
				"strategy=rrf":               1,
				"hybrid_search_reqs|" + reqs: 1,
				"hybrid_search=" + tc.want:   1,
			})
		})
	}
}

// TestAggregationAndOrdering drives the query aggregation operators, order by
// on query and search, and search aggregation.
func (s *Suite) TestAggregationAndOrdering() {
	ctx := context.Background()
	s.ensureCollection(ctx)

	for _, op := range []string{"count", "sum", "min", "max", "avg"} {
		s.Run("query_aggregation="+op, func() {
			arg := pkField
			if op == "count" {
				arg = "*"
			}
			before := counters(s.report(ctx), typeutil.ProxyRole)
			s.queryWith(ctx, func(r *milvuspb.QueryRequest) {
				r.OutputFields = []string{fmt.Sprintf("%s(%s)", op, arg)}
				r.QueryParams = nil
			})
			requireOnlyDelta(s.T(), before, counters(s.report(ctx), typeutil.ProxyRole),
				map[string]int64{"query_aggregation=" + op: 1})
		})
	}

	s.Run("avg next to its own parts counts each operator once", func() {
		before := counters(s.report(ctx), typeutil.ProxyRole)
		s.queryWith(ctx, func(r *milvuspb.QueryRequest) {
			r.OutputFields = []string{"avg(pk)", "sum(pk)", "count(*)"}
			r.QueryParams = nil
		})
		requireOnlyDelta(s.T(), before, counters(s.report(ctx), typeutil.ProxyRole), map[string]int64{
			"query_aggregation=avg":   1,
			"query_aggregation=sum":   1,
			"query_aggregation=count": 1,
		})
	})

	s.Run("order_by on query", func() {
		before := counters(s.report(ctx), typeutil.ProxyRole)
		s.queryWith(ctx, func(r *milvuspb.QueryRequest) {
			r.QueryParams = append(r.QueryParams, &commonpb.KeyValuePair{Key: "order_by_fields", Value: pkField + ":desc"})
		})
		requireOnlyDelta(s.T(), before, counters(s.report(ctx), typeutil.ProxyRole), map[string]int64{"order_by": 1})
	})

	s.Run("order_by_fields on search", func() {
		before := counters(s.report(ctx), typeutil.ProxyRole)
		s.searchWith(ctx, withSearchParam("order_by_fields", pkField+":desc"))
		requireOnlyDelta(s.T(), before, counters(s.report(ctx), typeutil.ProxyRole), map[string]int64{"order_by_fields": 1})
	})

	s.Run("search_aggregation", func() {
		before := counters(s.report(ctx), typeutil.ProxyRole)
		s.searchWith(ctx, func(r *milvuspb.SearchRequest) {
			r.SearchAggregation = &commonpb.SearchAggregationSpec{}
		})
		requireOnlyDelta(s.T(), before, counters(s.report(ctx), typeutil.ProxyRole), map[string]int64{"search_aggregation": 1})
	})
}

const (
	writesTags = "tags"
	writesMeta = "meta"
	// writesScalars is how many plain Int64 columns the write collection has
	// besides pk, vec, tags and meta, so a full row carries more than 16 fields.
	writesScalars = 14
)

// createWritesCollection builds an empty, loaded collection for the upsert
// and delete cases: a primary key the client sets, a vector, an Int32 array
// for the array operators, a JSON field for PATH_REPLACE, and enough plain
// columns that a full row has more than sixteen fields.
func (s *Suite) createWritesCollection(ctx context.Context) string {
	name := "fu_writes_" + funcutil.GenRandomStr()
	fields := []*schemapb.FieldSchema{
		{FieldID: 100, Name: pkField, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{
			FieldID: 101, Name: vectorField, DataType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: fmt.Sprint(s.dim)}},
		},
		{
			FieldID: 102, Name: writesTags, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "16"}},
		},
		{FieldID: 103, Name: writesMeta, DataType: schemapb.DataType_JSON},
	}
	for i := 0; i < writesScalars; i++ {
		fields = append(fields, &schemapb.FieldSchema{FieldID: int64(104 + i), Name: fmt.Sprintf("f%d", i), DataType: schemapb.DataType_Int64})
	}
	marshaled, err := proto.Marshal(&schemapb.CollectionSchema{Name: name, Fields: fields})
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: name, Schema: marshaled, ShardsNum: 1,
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, status.GetErrorCode(), status.GetReason())

	index, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: name, FieldName: vectorField, IndexName: "_default",
		ExtraParams: integration.ConstructIndexParam(s.dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, index.GetErrorCode(), index.GetReason())
	s.WaitForIndexBuilt(ctx, name, vectorField)
	load, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, load.GetErrorCode(), load.GetReason())
	s.WaitForLoad(ctx, name)
	return name
}

// TestDeleteAndUpsertModes drives both delete modes, the upsert modes, every
// partial-update operator and the fields-per-upsert buckets. It must sort
// before TestUnreachableNodeIsReported: the QueryNode that test kills stays
// listed as unreachable for a while, and s.report refuses such a report.
func (s *Suite) TestDeleteAndUpsertModes() {
	ctx := context.Background()
	s.ensureCollection(ctx)
	name := s.createWritesCollection(ctx)
	const rows = 2

	pk := func() *schemapb.FieldData { return newInt64Column(pkField, []int64{1, 2}) }
	vec := func() *schemapb.FieldData { return integration.NewFloatVectorFieldData(vectorField, rows, s.dim) }
	scalar := func(i int) *schemapb.FieldData {
		return newInt64Column(fmt.Sprintf("f%d", i), []int64{int64(i), int64(i)})
	}
	tags := func() *schemapb.FieldData { return newInt32ArrayColumn(writesTags, rows) }
	meta := func() *schemapb.FieldData { return newJSONColumn(writesMeta, [][]byte{[]byte(`1`), []byte(`2`)}) }
	fullRow := func() []*schemapb.FieldData {
		out := []*schemapb.FieldData{pk(), vec(), tags(), newJSONColumn(writesMeta, [][]byte{[]byte(`{"k": 1}`), []byte(`{"k": 2}`)})}
		for i := 0; i < writesScalars; i++ {
			out = append(out, scalar(i))
		}
		return out
	}
	upsert := func(partial bool, fields []*schemapb.FieldData, ops ...*schemapb.FieldPartialUpdateOp) {
		_, err := s.Cluster.MilvusClient.Upsert(ctx, &milvuspb.UpsertRequest{
			CollectionName: name,
			FieldsData:     fields,
			HashKeys:       integration.GenerateHashKeys(rows),
			NumRows:        rows,
			PartialUpdate:  partial,
			FieldOps:       ops,
		})
		s.Require().NoError(err, "transport error")
	}
	op := func(field string, t schemapb.FieldPartialUpdateOp_OpType, path string) *schemapb.FieldPartialUpdateOp {
		return &schemapb.FieldPartialUpdateOp{FieldName: field, Op: t, Path: path}
	}

	cases := []struct {
		name string
		run  func()
		want map[string]int64
	}{
		{
			name: "override with a full row",
			run:  func() { upsert(false, fullRow()) },
			want: map[string]int64{"upsert_mode=override": 1, "upsert_fields|>16": 1},
		},
		{
			name: "merge with the primary key alone",
			run:  func() { upsert(true, []*schemapb.FieldData{pk()}) },
			want: map[string]int64{"upsert_mode=merge": 1, "upsert_fields|1": 1},
		},
		{
			name: "merge with seven fields",
			run: func() {
				fields := []*schemapb.FieldData{pk()}
				for i := 0; i < 6; i++ {
					fields = append(fields, scalar(i))
				}
				upsert(true, fields)
			},
			want: map[string]int64{"upsert_mode=merge": 1, "upsert_fields|5-16": 1},
		},
		{
			// An explicit REPLACE does not turn the request into a merge.
			name: "field_ops=REPLACE",
			run: func() {
				upsert(false, []*schemapb.FieldData{pk(), vec(), scalar(0)}, op("f0", schemapb.FieldPartialUpdateOp_REPLACE, ""))
			},
			want: map[string]int64{"upsert_mode=override": 1, "field_ops=REPLACE": 1, "upsert_fields|2-4": 1},
		},
		{
			// A non-REPLACE operator promotes the request to a merge, and the
			// mode is counted after that.
			name: "field_ops=ARRAY_APPEND",
			run: func() {
				upsert(false, []*schemapb.FieldData{pk(), tags()}, op(writesTags, schemapb.FieldPartialUpdateOp_ARRAY_APPEND, ""))
			},
			want: map[string]int64{"upsert_mode=merge": 1, "field_ops=ARRAY_APPEND": 1, "upsert_fields|2-4": 1},
		},
		{
			name: "field_ops=ARRAY_REMOVE",
			run: func() {
				upsert(true, []*schemapb.FieldData{pk(), tags()}, op(writesTags, schemapb.FieldPartialUpdateOp_ARRAY_REMOVE, ""))
			},
			want: map[string]int64{"upsert_mode=merge": 1, "field_ops=ARRAY_REMOVE": 1, "upsert_fields|2-4": 1},
		},
		{
			name: "field_ops=PATH_REPLACE",
			run: func() {
				upsert(true, []*schemapb.FieldData{pk(), meta()}, op(writesMeta, schemapb.FieldPartialUpdateOp_PATH_REPLACE, `["k"]`))
			},
			want: map[string]int64{"upsert_mode=merge": 1, "field_ops=PATH_REPLACE": 1, "upsert_fields|2-4": 1},
		},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			before := counters(s.report(ctx), typeutil.ProxyRole)
			tc.run()
			requireOnlyDelta(s.T(), before, counters(s.report(ctx), typeutil.ProxyRole), tc.want)
		})
	}

	for _, tc := range []struct {
		name string
		expr string
		want map[string]int64
	}{
		// delete(ids=...) reaches the server as a primary key match.
		{"delete_mode=ids", pkField + " in [1, 2]", map[string]int64{"delete_mode=ids": 1, "in_operator": 1}},
		{"delete_mode=filter", "f0 > 5", map[string]int64{"delete_mode=filter": 1, "comparison_operators=relational": 1}},
	} {
		s.Run(tc.name, func() {
			before := counters(s.report(ctx), typeutil.ProxyRole)
			_, err := s.Cluster.MilvusClient.Delete(ctx, &milvuspb.DeleteRequest{CollectionName: name, Expr: tc.expr})
			s.Require().NoError(err, "transport error")
			requireOnlyDelta(s.T(), before, counters(s.report(ctx), typeutil.ProxyRole), tc.want)
		})
	}
}
