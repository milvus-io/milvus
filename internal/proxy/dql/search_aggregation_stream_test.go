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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/searchutil"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSearchAggregationStreamingMatchesBatch(t *testing.T) {
	for _, tc := range []struct {
		name        string
		composite   bool
		nested      bool
		noTopHits   bool
		emptyWorker bool
		emptyQuery  bool
		allEmpty    bool
	}{
		{name: "single field"},
		{name: "composite fields", composite: true},
		{name: "nested buckets", nested: true},
		{name: "metrics without top hits", nested: true, noTopHits: true},
		{name: "empty worker", nested: true, emptyWorker: true},
		{name: "empty query", nested: true, emptyQuery: true},
		{name: "all empty", nested: true, allEmpty: true},
	} {
		for _, metric := range []string{"IP", "L2"} {
			for _, chunkBytes := range []int{1, 96, 256 * 1024} {
				t.Run(fmt.Sprintf("%s/%s/bytes_%d", tc.name, metric, chunkBytes), func(t *testing.T) {
					ctx := context.Background()
					span := trace.SpanFromContext(ctx)
					legacy := &fakeLegacyQueryClient{reduceStreamEnabled: true}
					task := newQueryViewSearchTask(legacy, 1)
					task.ctx = ctx
					task.Nq, task.IsIterator, task.MetricType = 2, false, metric
					task.OutputFieldsId = []int64{104}
					task.userOutputFields = []string{"value"}
					task.schema = &schemaInfo{CollectionSchema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
						{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
						{FieldID: 101, Name: "category", DataType: schemapb.DataType_Int64, Nullable: true},
						{FieldID: 102, Name: "brand", DataType: schemapb.DataType_Int64},
						{FieldID: 104, Name: "value", DataType: schemapb.DataType_Int64},
						{FieldID: 105, Name: "price", DataType: schemapb.DataType_Double, Nullable: true},
					}}}
					task.schema.PkField = task.schema.Fields[0]
					spec := &commonpb.SearchAggregationSpec{
						Fields: []string{"category"}, Size: 2, SearchSize: 3,
						Metrics: map[string]*commonpb.MetricAggSpec{
							"count": {Op: "count", FieldName: "*"},
							"sum":   {Op: "sum", FieldName: "price"},
							"avg":   {Op: "avg", FieldName: "price"},
							"min":   {Op: "min", FieldName: "price"},
							"max":   {Op: "max", FieldName: "price"},
						},
						Order: []*commonpb.OrderSpec{{Key: "sum", Direction: "desc"}, {Key: "_key", Direction: "asc"}},
						TopHits: &commonpb.TopHitsSpec{Size: 2, Sort: []*commonpb.SortSpec{
							{FieldName: "price", Direction: "desc", NullFirst: true},
							{FieldName: "_score"},
						}},
					}
					if tc.composite {
						spec.Fields = append(spec.Fields, "brand")
					}
					if tc.nested {
						spec.SubAggregation = &commonpb.SearchAggregationSpec{
							Fields: []string{"brand"}, Size: 1, SearchSize: 2,
							Metrics: map[string]*commonpb.MetricAggSpec{"score": {Op: "avg", FieldName: "_score"}},
							Order:   []*commonpb.OrderSpec{{Key: "_key", Direction: "asc"}},
							TopHits: &commonpb.TopHitsSpec{Size: 3},
						}
					}
					if tc.noTopHits {
						spec.TopHits, spec.SubAggregation.TopHits = nil, nil
					}
					task.request.SearchAggregation = spec
					require.NoError(t, task.initSearchAggregation())
					task.Topk, task.GroupSize = task.aggCtx.DerivedTopK, task.aggCtx.DerivedGroupSize
					require.Contains(t, task.GetOutputFieldsId(), int64(105))
					require.NotContains(t, task.GetOutputFieldsId(), int64(101))
					task.queryInfos = []*planpb.QueryInfo{{
						Topk: task.Topk, GroupSize: task.GroupSize, GroupByFieldIds: task.GetGroupByFieldIds(), RoundDecimal: 2,
					}}

					info := reduce.NewReduceSearchResultInfo(task.GetNq(), task.GetTopk()).
						WithMetricType(metric).WithGroupSize(task.GetGroupSize()).
						WithGroupByFieldIdsFromProto(0, task.GetGroupByFieldIds())
					inputs := [][]groupStreamHit{
						{{1, .99, int64(10)}, {2, .98, int64(20)}, {3, .94, int64(10)}, {4, .90, nil}, {5, .80, int64(20)}},
						{{1, .99, int64(10)}, {6, .98, int64(20)}, {7, .95, nil}, {8, .85, int64(30)}},
						{{9, 1, int64(20)}, {10, .96, int64(30)}, {11, .92, int64(20)}, {12, .81, int64(40)}},
					}
					nodes := make([]*internalpb.SearchResults, len(inputs))
					for i, firstQuery := range inputs {
						hits := [][]groupStreamHit{firstQuery, {{13, .9, int64(50)}, {14, .8, int64(50)}}}
						if tc.emptyQuery {
							hits[1] = nil
						}
						if tc.allEmpty || (tc.emptyWorker && i == 1) {
							hits = make([][]groupStreamHit, 2)
						}
						node := groupStreamResult(t, task.SearchRequest, hits, false)
						prices := &schemapb.DoubleArray{}
						field := &schemapb.FieldData{FieldId: 105, Type: schemapb.DataType_Double,
							Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
								Data: &schemapb.ScalarField_DoubleData{DoubleData: prices},
							}},
						}
						for _, query := range hits {
							for _, hit := range query {
								valid := hit.id%3 != 0
								field.ValidData = append(field.ValidData, valid)
								var price float64
								if valid {
									price = float64(hit.id) * .5
								}
								prices.Data = append(prices.Data, price)
								node.ResultData.AllSearchCount++
							}
						}
						node.ResultData.FieldsData = append(node.ResultData.FieldsData, field)
						// Both paths start after the same existing node-local group selection.
						data, err := (&segments.SearchGroupByReduce{}).ReduceSearchResultData(ctx, []*schemapb.SearchResultData{node.ResultData}, info)
						require.NoError(t, err)
						node.ResultData = data
						nodes[i] = node
					}

					var batchShards []*internalpb.SearchResults
					var vchannels []searchutil.ReduceStream
					for _, workers := range [][]*internalpb.SearchResults{nodes[:2], nodes[2:]} {
						var batchInputs []*internalpb.SearchResults
						var children []searchutil.ReduceStream
						for _, node := range workers {
							batchInputs = append(batchInputs, proto.Clone(node).(*internalpb.SearchResults))
							chunks, err := searchutil.SplitSearchResult(proto.Clone(node).(*internalpb.SearchResults), chunkBytes)
							require.NoError(t, err)
							children = append(children, &searchTaskReduceStream{chunks: chunks})
						}
						batch, err := segments.ReduceSearchOnQueryNode(ctx, batchInputs, info)
						require.NoError(t, err)
						batchShards = append(batchShards, batch)
						stream, err := searchutil.NewShardReduceStream(task.SearchRequest, children, chunkBytes)
						require.NoError(t, err)
						vchannels = append(vchannels, stream)
					}
					stream, err := searchutil.NewReduceStream(task.SearchRequest, vchannels, chunkBytes)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, stream.Close()) })
					legacy.searchResult = &queryclient.LegacySearchResult{Stream: stream}
					require.NoError(t, task.executeByQueryView(ctx))
					require.Equal(t, 1, legacy.searchCalled)
					require.Empty(t, task.resultBuf.Collect(), "streamed candidates must not enter Batch reduction")
					candidates, err := reduceResults(ctx, batchShards, task.GetNq(), task.GetTopk(), 0,
						metric, schemapb.DataType_Int64, task.queryInfos[0], false, true, 1, nil)
					require.NoError(t, err)
					if !tc.allEmpty {
						require.True(t, proto.Equal(candidates.GetResults(), task.reducedResult.GetResults()),
							"Batch=%s\nStreaming=%s", candidates.GetResults(), task.reducedResult.GetResults())
					}
					require.True(t, task.supportsStreamingReduce())
					pipeline, err := newSearchPipeline(task)
					require.NoError(t, err)
					expected, _, err := pipeline.Run(ctx, span, batchShards, segcore.StorageCost{})
					require.NoError(t, err)
					actual, _, err := pipeline.RunFromReduced(ctx, span, task.reducedResult, metric, segcore.StorageCost{})
					require.NoError(t, err)
					require.True(t, proto.Equal(expected, actual), "Batch=%s\nStreaming=%s", expected, actual)
					require.NoError(t, task.PostExecute(ctx))
					expected.Results.PrimaryFieldName = "pk"
					expected.Results.OutputFields = task.userOutputFields
					require.True(t, proto.Equal(expected.GetResults(), task.result.GetResults()),
						"Batch=%s\nStreaming=%s", expected.GetResults(), task.result.GetResults())
				})
			}
		}
	}
}

func TestAppendFinalSearchChunkPreservesGroupValidity(t *testing.T) {
	output := &schemapb.SearchResultData{NumQueries: 1, Ids: &schemapb.IDs{}, Topks: []int64{0}}
	seen := []int64{0}
	for row, group := range []any{int64(10), nil, int64(20)} {
		request := &internalpb.SearchRequest{Nq: 1, Topk: 3, MetricType: "IP", GroupByFieldIds: []int64{101}}
		chunk := groupStreamResult(t, request, [][]groupStreamHit{{{int64(row + 1), .9, group}}}, false)
		_, err := appendFinalSearchChunk(output, chunk.GetResultData(), seen, 0, 3, "IP")
		require.NoError(t, err)
	}
	field := output.GetGroupByFieldValues()[0]
	require.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(field))
	value := typeutil.GetDataIterator(field)
	require.Equal(t, []any{int64(10), nil, int64(20)}, []any{value(0), value(1), value(2)})
}

func TestSearchAggregationStreamingFailures(t *testing.T) {
	for _, receiveFailure := range []bool{true, false} {
		t.Run(map[bool]string{true: "receive error", false: "missing metric field"}[receiveFailure], func(t *testing.T) {
			ctx := context.Background()
			legacy := &fakeLegacyQueryClient{reduceStreamEnabled: true}
			task := newQueryViewSearchTask(legacy, 1)
			task.ctx, task.IsIterator = ctx, false
			task.schema = &schemaInfo{CollectionSchema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "category", DataType: schemapb.DataType_Int64},
				{FieldID: 102, Name: "brand", DataType: schemapb.DataType_Int64},
				{FieldID: 105, Name: "price", DataType: schemapb.DataType_Double},
			}}}
			task.schema.PkField = task.schema.Fields[0]
			task.request.SearchAggregation = &commonpb.SearchAggregationSpec{
				Fields: []string{"category", "brand"}, Size: 1, TopHits: &commonpb.TopHitsSpec{Size: 2},
				Metrics: map[string]*commonpb.MetricAggSpec{"sum": {Op: "sum", FieldName: "price"}},
			}
			require.NoError(t, task.initSearchAggregation())
			task.Topk, task.GroupSize = task.aggCtx.DerivedTopK, task.aggCtx.DerivedGroupSize
			task.queryInfos = []*planpb.QueryInfo{{GroupByFieldIds: task.GetGroupByFieldIds(), GroupSize: task.GroupSize, RoundDecimal: -1}}
			chunk := groupStreamResult(t, task.SearchRequest, [][]groupStreamHit{{{1, .9, int64(10)}}}, false)
			stream := &fakeLegacySearchStream{recv: []fakeLegacySearchRecv{{chunk: chunk}}}
			legacy.searchResult = &queryclient.LegacySearchResult{Stream: stream}
			if receiveFailure {
				failure := merr.WrapErrServiceInternalMsg("child receive failed")
				stream.recv = append(stream.recv, fakeLegacySearchRecv{err: failure})
				require.ErrorIs(t, task.executeByQueryView(ctx), failure)
				require.Nil(t, task.reducedResult)
			} else {
				require.NoError(t, task.executeByQueryView(ctx))
				err := task.PostExecute(ctx)
				require.ErrorIs(t, err, merr.ErrServiceInternal)
				require.ErrorContains(t, err, "field 105 missing")
				require.Nil(t, task.result)
			}
			require.Equal(t, 1, legacy.searchCalled)
			require.Equal(t, 1, stream.closeCalls)
			require.Empty(t, task.resultBuf.Collect())
		})
	}
}
