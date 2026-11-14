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

package tasks

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	mock_segcore "github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/reduce"
	searchreducestream "github.com/milvus-io/milvus/internal/util/searchutil/reduce_stream"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Feed real sealed-segment L0/L1 output through both reduction levels. Proxy
// pipeline parity, including optional L2, is covered separately in dql.
func TestWorkerRerankStreamingMatchesBatch(t *testing.T) {
	const (
		nq      = 2
		topK    = 4
		rows    = 32
		workers = 4
		metric  = "L2"
	)
	ts := setupTestSegments(t, workers*2, rows, setupOpts{
		SkipSearchReq: true,
		MutateInsertData: func(segment int, data *storage.InsertData) {
			vectors := data.Data[107].(*storage.FloatVectorFieldData)
			pks := data.Data[109].(*storage.Int64FieldData)
			ranks := data.Data[105].(*storage.DoubleFieldData)
			clear(vectors.Data)
			clear(data.Data[104].(*storage.FloatFieldData).Data)
			for row := 0; row < rows; row++ {
				pks.Data[row] = int64(row*workers*2 + segment + 1)
				vectors.Data[row*vectors.Dim] = float32(pks.Data[row]) / 8
				ranks.Data[row] = float64(row + 1)
			}
		},
	})
	defer ts.cleanup()
	ctx := context.Background()

	mapScore := mapOpWithParamsForTest("$score", "num_combine",
		map[string]*schemapb.FunctionParamValue{"mode": stringParamForTest("sum")},
		columnArgForTest("doubleField"), columnArgForTest("floatField"))
	l0 := l0FunctionChainForTest(mapScore)
	l1 := l1FunctionChainForTest(
		&schemapb.FunctionChainOp{Op: "sort", Inputs: []string{"$id"}, Params: map[string]*schemapb.FunctionParamValue{
			"desc": {Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: true}},
		}},
		&schemapb.FunctionChainOp{Op: "limit", Params: map[string]*schemapb.FunctionParamValue{
			"limit": {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 2}},
		}},
		mapScore,
	)
	for _, tc := range []struct {
		name   string
		chains []*schemapb.FunctionChain
		boost  bool
		count  int64
	}{
		{name: "boost", boost: true, count: topK},
		{name: "L0", chains: []*schemapb.FunctionChain{l0}, count: topK},
		{name: "L1", chains: []*schemapb.FunctionChain{l1}, count: 2},
		{name: "L0_L1", chains: []*schemapb.FunctionChain{l0, l1}, count: 2},
	} {
		t.Run(metric+"/"+tc.name, func(t *testing.T) {
			var workerResults []*internalpb.SearchResults
			for worker := 0; worker < workers; worker++ {
				request, err := mock_segcore.GenQueryRequest(ts.collection.GetCCollection(), ts.segIDs[worker*2:worker*2+2], nq, topK, testCollectionID)
				require.NoError(t, err)
				plan := &planpb.PlanNode{}
				require.NoError(t, proto.Unmarshal(request.Req.SerializedExprPlan, plan))
				plan.QuerynodeFunctionChains = tc.chains
				if tc.boost {
					plan.Scorers = []*planpb.ScoreFunction{{
						Weight: 2,
						Filter: &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{UnaryRangeExpr: &planpb.UnaryRangeExpr{
							ColumnInfo: &planpb.ColumnInfo{FieldId: 109, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
							Op:         planpb.OpType_GreaterThan, Value: &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 4}},
						}}},
					}}
					plan.ScoreOption = &planpb.ScoreOption{BoostMode: planpb.BoostMode_BoostModeSum}
				}
				plan.OutputFieldIds = []int64{109, 105}
				plan.GetVectorAnns().QueryInfo.MetricType = metric
				request.Req.SerializedExprPlan, err = proto.Marshal(plan)
				require.NoError(t, err)
				request.Req.MetricType, request.Req.OutputFieldsId = metric, plan.OutputFieldIds
				placeholder := &commonpb.PlaceholderGroup{}
				require.NoError(t, proto.Unmarshal(request.Req.PlaceholderGroup, placeholder))
				for _, value := range placeholder.GetPlaceholders() {
					for _, vector := range value.GetValues() {
						clear(vector)
					}
				}
				request.Req.PlaceholderGroup, err = proto.Marshal(placeholder)
				require.NoError(t, err)
				task := NewSearchTask(ctx, ts.collection, ts.manager, request, int64(worker+1))
				require.NoError(t, task.ExecuteOnSegments(ts.segs[worker*2:worker*2+2]))
				result := task.SearchResult()
				require.NoError(t, merr.Error(result.GetStatus()))
				data, err := segments.DecodeSearchResults(ctx, []*internalpb.SearchResults{result})
				require.NoError(t, err)
				require.Len(t, data, 1)
				require.Equal(t, []int64{tc.count, tc.count}, data[0].Topks, "L1 limit is per worker and vector")
				if tc.boost && worker == 0 {
					plan.Scorers, plan.ScoreOption = nil, nil
					request.Req.SerializedExprPlan, err = proto.Marshal(plan)
					require.NoError(t, err)
					baseline := NewSearchTask(ctx, ts.collection, ts.manager, request, int64(worker+1))
					require.NoError(t, baseline.ExecuteOnSegments(ts.segs[worker*2:worker*2+2]))
					unboosted, err := segments.DecodeSearchResults(ctx, []*internalpb.SearchResults{baseline.SearchResult()})
					require.NoError(t, err)
					require.Len(t, unboosted, 1)
					require.NotEqual(t, unboosted[0].GetScores(), data[0].GetScores(), "boost must affect worker scores")
				}
				var rankField *schemapb.FieldData
				for _, field := range data[0].FieldsData {
					if field.FieldId == 105 {
						rankField = field
					}
				}
				require.NotNil(t, rankField)
				for row, score := range data[0].Scores {
					rank := rankField.GetScalars().GetDoubleData().GetData()[row]
					pk := data[0].GetIds().GetIntId().GetData()[row]
					require.Equal(t, float64((pk-1)/(workers*2)+1), rank, "late materialization must follow reranked rows")
					if !tc.boost {
						require.Equal(t, float32(rank), score, "worker score mapping must execute")
					}
					if row%int(tc.count) > 0 {
						require.GreaterOrEqual(t, data[0].Scores[row-1], score)
						if data[0].Scores[row-1] == score {
							require.Less(t, data[0].GetIds().GetIntId().GetData()[row-1], pk)
						}
					}
				}
				workerResults = append(workerResults, result)
			}

			for _, chunkBytes := range []int{1, 256 * 1024} {
				t.Run(fmt.Sprintf("bytes_%d", chunkBytes), func(t *testing.T) {
					request := &internalpb.SearchRequest{Nq: nq, Topk: topK, MetricType: metric}
					info := reduce.NewReduceSearchResultInfo(nq, topK).WithMetricType(metric)
					var batchShards []*internalpb.SearchResults
					var shardStreams []searchreducestream.ReduceStream
					for channel := 0; channel < 2; channel++ {
						inputs := workerResults[channel*2 : channel*2+2]
						batch, err := segments.ReduceSearchOnQueryNode(ctx, inputs, info)
						require.NoError(t, err)
						batchShards = append(batchShards, batch)
						var children []searchreducestream.ReduceStream
						for _, result := range inputs {
							chunks, err := searchreducestream.SplitSearchResult(result, chunkBytes)
							require.NoError(t, err)
							children = append(children, &rerankChunkStream{chunks: chunks})
						}
						stream, err := searchreducestream.NewShardReduceStream(request, children, chunkBytes)
						require.NoError(t, err)
						shardStreams = append(shardStreams, stream)
					}
					// Distinct PKs across vchannels make Batch shard reduction a valid
					// reference for the final score-ordered candidate window as well.
					batch, err := segments.ReduceSearchOnQueryNode(ctx, batchShards, info)
					require.NoError(t, err)
					decoded, err := segments.DecodeSearchResults(ctx, []*internalpb.SearchResults{batch})
					require.NoError(t, err)
					require.Len(t, decoded, 1)
					type hit struct {
						id    int64
						score float32
						rank  float64
					}
					appendHits := func(output [][]hit, data *schemapb.SearchResultData) {
						t.Helper()
						var ranks []float64
						for _, field := range data.FieldsData {
							if field.FieldId == 105 {
								ranks = field.GetScalars().GetDoubleData().GetData()
							}
						}
						require.Len(t, ranks, len(data.Scores))
						row := 0
						for query, count := range data.Topks {
							for range count {
								if len(output[query]) < topK {
									output[query] = append(output[query], hit{data.GetIds().GetIntId().Data[row], data.Scores[row], ranks[row]})
								}
								row++
							}
						}
					}
					expected, actual := make([][]hit, nq), make([][]hit, nq)
					appendHits(expected, decoded[0])
					stream, err := searchreducestream.NewReduceStream(request, shardStreams, chunkBytes)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, stream.Close()) })
					for len(actual[nq-1]) < topK {
						chunk, err := stream.Recv()
						require.NoError(t, err)
						appendHits(actual, chunk.GetResultData())
					}
					require.Equal(t, expected, actual)
				})
			}
		})
	}
}

type rerankChunkStream struct {
	chunks []*internalpb.SearchResults
}

func (s *rerankChunkStream) Recv() (*internalpb.SearchResults, error) {
	if len(s.chunks) == 0 {
		return nil, io.EOF
	}
	chunk := s.chunks[0]
	s.chunks = s.chunks[1:]
	return chunk, nil
}

func (*rerankChunkStream) Close() error { return nil }

func (*rerankChunkStream) Interrupt() (*internalpb.SearchResults, error) {
	return nil, merr.WrapErrServiceInternalMsg("Interrupt is not implemented")
}
