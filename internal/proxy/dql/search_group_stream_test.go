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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/searchutil"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type groupStreamHit struct {
	id    int64
	score float32
	group any
}

func groupStreamResult(t *testing.T, request *internalpb.SearchRequest, hits [][]groupStreamHit, stringPK bool) *internalpb.SearchResults {
	t.Helper()
	data := &schemapb.SearchResultData{NumQueries: request.GetNq(), TopK: request.GetTopk(), Ids: &schemapb.IDs{}}
	groups, err := typeutil.NewFieldDataBuilder(schemapb.DataType_Int64, true, 16)
	require.NoError(t, err)
	var values, secondKey []int64
	for _, queryHits := range hits {
		data.Topks = append(data.Topks, int64(len(queryHits)))
		for _, hit := range queryHits {
			if stringPK {
				typeutil.AppendPKs(data.Ids, fmt.Sprintf("pk-%03d", hit.id))
			} else {
				typeutil.AppendPKs(data.Ids, hit.id)
			}
			score := hit.score
			if request.GetMetricType() == "L2" {
				score -= 2 // Internal scores are always ordered with larger values first.
			}
			data.Scores = append(data.Scores, score)
			groups.Add(hit.group)
			values = append(values, hit.id*10)
			secondKey = append(secondKey, hit.id%2)
		}
	}
	field := groups.Build()
	field.FieldId = 101
	data.GroupByFieldValues = []*schemapb.FieldData{field}
	if len(request.GetGroupByFieldIds()) > 1 {
		data.GroupByFieldValues = append(data.GroupByFieldValues, multiGroupByTestLongField(102, secondKey))
	}
	data.FieldsData = []*schemapb.FieldData{multiGroupByTestLongField(104, values)}
	return &internalpb.SearchResults{
		Status: merr.Success(), NumQueries: request.GetNq(), TopK: request.GetTopk(),
		MetricType: request.GetMetricType(), ResultData: data,
	}
}

func runGroupedSearchComparison(t *testing.T, request *internalpb.SearchRequest, workers [][]*internalpb.SearchResults, chunkBytes int, pkType schemapb.DataType) *schemapb.SearchResultData {
	t.Helper()
	info := reduce.NewReduceSearchResultInfo(request.GetNq(), request.GetTopk()).
		WithMetricType(request.GetMetricType()).WithGroupSize(request.GetGroupSize()).
		WithGroupByFieldIdsFromProto(request.GetGroupByFieldId(), request.GetGroupByFieldIds())
	var batchShards []*internalpb.SearchResults
	var streams []searchutil.ReduceStream
	for _, nodes := range workers {
		batchInputs := make([]*internalpb.SearchResults, len(nodes))
		var children []searchutil.ReduceStream
		for i, node := range nodes {
			batchInputs[i] = proto.Clone(node).(*internalpb.SearchResults)
			chunks, err := searchutil.SplitSearchResult(proto.Clone(node).(*internalpb.SearchResults), chunkBytes)
			require.NoError(t, err)
			children = append(children, &searchTaskReduceStream{chunks: chunks})
		}
		batch, err := segments.ReduceSearchOnQueryNode(context.Background(), batchInputs, info)
		require.NoError(t, err)
		batchShards = append(batchShards, batch)
		stream, err := searchutil.NewShardReduceStream(request, children, chunkBytes)
		require.NoError(t, err)
		streams = append(streams, stream)
	}
	batch, err := reduceResults(context.Background(), batchShards, request.GetNq(), request.GetTopk(), request.GetOffset(),
		request.GetMetricType(), pkType, &planpb.QueryInfo{
			GroupByFieldId: request.GetGroupByFieldId(), GroupByFieldIds: request.GetGroupByFieldIds(), GroupSize: request.GetGroupSize(),
		}, false, false, 1, nil)
	require.NoError(t, err)
	stream, err := searchutil.NewReduceStream(request, streams, chunkBytes)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stream.Close()) })
	task := &SearchTask{SearchRequest: request}
	streamed, _, err := task.consumeResultStream(stream, request.GetMetricType(), func(*internalpb.SearchResults) {})
	require.NoError(t, err)
	require.True(t, proto.Equal(batch.GetResults(), streamed.GetResults()), "batch=%s\nstream=%s", batch.GetResults(), streamed.GetResults())
	return streamed.GetResults()
}

func TestSearchGroupByStreamingMatchesBatch(t *testing.T) {
	for _, tc := range []struct {
		name      string
		groupSize int64
		offset    int64
		composite bool
		stringPK  bool
		legacy    bool
	}{
		{name: "one member", groupSize: 1},
		{name: "two members", groupSize: 2},
		{name: "offset and underfilled groups", groupSize: 3, offset: 1},
		{name: "composite keys", groupSize: 2, composite: true},
		{name: "string PK and composite offset", groupSize: 2, offset: 1, composite: true, stringPK: true},
		{name: "legacy group column", groupSize: 2, legacy: true},
	} {
		for _, metric := range []string{"IP", "L2"} {
			for _, chunkBytes := range []int{1, 90, 256 * 1024} {
				t.Run(fmt.Sprintf("%s/%s/bytes_%d", tc.name, metric, chunkBytes), func(t *testing.T) {
					request := &internalpb.SearchRequest{Nq: 2, Topk: 2 + tc.offset, Offset: tc.offset,
						MetricType: metric, GroupSize: tc.groupSize, GroupByFieldIds: []int64{101}}
					if tc.composite {
						request.GroupByFieldIds = append(request.GroupByFieldIds, 102)
					}
					if tc.legacy {
						request.GroupByFieldId = 101
						request.GroupByFieldIds = nil
					}
					inputs := [][]groupStreamHit{
						{{1, .99, int64(10)}, {2, .98, int64(20)}, {3, .94, int64(10)}, {4, .90, nil}, {5, .80, int64(20)}},
						{{1, .99, int64(10)}, {6, .98, int64(20)}, {7, .95, nil}, {8, .85, int64(30)}},
						{{9, 1, int64(20)}, {10, .96, int64(30)}, {11, .92, int64(20)}, {12, .81, int64(40)}},
					}
					var nodes []*internalpb.SearchResults
					info := reduce.NewReduceSearchResultInfo(2, request.GetTopk()).WithGroupSize(tc.groupSize).
						WithGroupByFieldIdsFromProto(request.GetGroupByFieldId(), request.GetGroupByFieldIds())
					for _, input := range inputs {
						node := groupStreamResult(t, request, [][]groupStreamHit{input, {{13, .9, int64(50)}, {14, .8, int64(50)}}}, tc.stringPK)
						// Model the existing node-local group selection before either transport path.
						reduced, err := (&segments.SearchGroupByReduce{}).ReduceSearchResultData(context.Background(), []*schemapb.SearchResultData{node.ResultData}, info)
						require.NoError(t, err)
						node.ResultData = reduced
						if tc.legacy {
							node.ResultData.GroupByFieldValue = node.ResultData.GroupByFieldValues[0]
							node.ResultData.GroupByFieldValue.FieldId = 0
							node.ResultData.GroupByFieldValues = nil
						}
						nodes = append(nodes, node)
					}
					pkType := schemapb.DataType_Int64
					if tc.stringPK {
						pkType = schemapb.DataType_VarChar
					}
					runGroupedSearchComparison(t, request, [][]*internalpb.SearchResults{nodes[:2], nodes[2:]}, chunkBytes, pkType)
				})
			}
		}
	}
}

func TestSearchGroupByStreamingPreservesBatchLocalPruning(t *testing.T) {
	request := &internalpb.SearchRequest{Nq: 1, Topk: 1, GroupSize: 2, GroupByFieldIds: []int64{101}, MetricType: "IP"}
	qn1 := groupStreamResult(t, request, [][]groupStreamHit{{{1, .99, int64(10)}, {2, .90, int64(10)}}}, false)
	qn2 := groupStreamResult(t, request, [][]groupStreamHit{{{3, .98, int64(20)}, {4, .85, int64(20)}}}, false)
	qn3 := groupStreamResult(t, request, [][]groupStreamHit{{{5, 1, int64(20)}, {6, .80, int64(20)}}}, false)
	for _, chunkBytes := range []int{1, 256 * 1024} {
		result := runGroupedSearchComparison(t, request, [][]*internalpb.SearchResults{{qn1, qn2}, {qn3}}, chunkBytes, schemapb.DataType_Int64)
		require.Equal(t, []int64{5, 6}, result.GetIds().GetIntId().GetData())
		// QN2's better second member (PK 3) was discarded by vchannel 1's local reduction.
		require.Equal(t, []float32{1, .80}, result.GetScores())
	}
}

func TestSearchGroupByStreamingOrderByAppliesOffsetAfterSorting(t *testing.T) {
	legacy := &fakeLegacyQueryClient{reduceStreamEnabled: true}
	task := newQueryViewSearchTask(legacy, 3)
	task.IsIterator = false
	task.Offset, task.GroupSize = 1, 2
	task.GroupByFieldIds = []int64{101}
	task.schema = &schemaInfo{CollectionSchema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 104, Name: "value", DataType: schemapb.DataType_Int64},
	}}}
	task.orderByFields = []OrderByField{{FieldName: "value", OutputFieldName: "value", FieldID: 104, Ascending: true}}
	request := proto.Clone(task.SearchRequest).(*internalpb.SearchRequest)
	input := groupStreamResult(t, request, [][]groupStreamHit{{
		{30, .99, int64(30)}, {10, .98, int64(10)}, {20, .97, int64(20)},
		{31, .90, int64(30)}, {11, .85, int64(10)}, {21, .80, int64(20)},
	}}, false)
	input.ResultData.FieldsData[0].FieldName = "value"
	patch := mockey.Mock((*fakeLegacyQueryClient).Search).To(func(_ *fakeLegacyQueryClient, _ context.Context, req *queryclient.LegacySearchRequest) (*queryclient.LegacySearchResult, error) {
		require.True(t, req.AllowStreaming)
		require.Zero(t, req.Req.GetOffset())
		chunks, err := searchutil.SplitSearchResult(input, 1)
		require.NoError(t, err)
		stream, err := searchutil.NewReduceStream(req.Req, []searchutil.ReduceStream{&searchTaskReduceStream{chunks: chunks}}, 1)
		require.NoError(t, err)
		return &queryclient.LegacySearchResult{Stream: stream}, nil
	}).Build()
	defer patch.UnPatch()
	require.NoError(t, task.executeByQueryView(context.Background()))
	require.True(t, proto.Equal(request, task.SearchRequest), "the original request must keep its offset")
	require.Equal(t, []int64{6}, task.reducedResult.GetResults().GetTopks())
	op := &orderByOperator{orderByFields: task.orderByFields, groupByFieldId: 101, groupSize: 2, offset: 1, limit: 2}
	result, err := op.run(context.Background(), trace.SpanFromContext(context.Background()), task.reducedResult)
	require.NoError(t, err)
	data := result[0].(*milvuspb.SearchResults).GetResults()
	require.Equal(t, []int64{20, 21, 30, 31}, data.GetIds().GetIntId().GetData())
	require.Equal(t, []int64{20, 20, 30, 30}, data.GetGroupByFieldValues()[0].GetScalars().GetLongData().GetData())
}
