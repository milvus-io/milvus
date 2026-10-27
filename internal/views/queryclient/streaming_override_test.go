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
package queryclient

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/searchutil"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestLegacySearchStreamingOverrides(t *testing.T) {
	setupTestReplicaRouting(t)
	for _, defaultOn := range []bool{false, true} {
		t.Run(fmt.Sprint(defaultOn), func(t *testing.T) {
			shard := qviews.ShardID{ReplicaID: 1, VChannel: "stream-overrides"}
			input := newTestSearchChunk(3, []int64{1, 2, 3}, []float32{.9, .8, .7})
			service := &legacyServiceClient{
				searchOnView: func(context.Context, qviews.WorkNode, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
					return &viewpb.SearchOnViewResponse{LegacyResults: proto.Clone(input).(*internalpb.SearchResults)}, nil
				},
				searchOnViewStream: func(ctx context.Context, _ qviews.WorkNode, request *viewpb.SearchOnViewRequest) (searchutil.ReduceStream, error) {
					if request.GetStreamChunkBytes() != request.GetLegacyReq().GetReqID() {
						return nil, fmt.Errorf("received Chunk bytes %d, expected %d", request.GetStreamChunkBytes(), request.GetLegacyReq().GetReqID())
					}
					chunks, err := searchutil.SplitSearchResult(proto.Clone(input).(*internalpb.SearchResults), int(request.GetStreamChunkBytes()))
					if err != nil {
						return nil, err
					}
					stream := &fakeSearchStream{ctx: ctx}
					for _, chunk := range chunks {
						stream.recv = append(stream.recv, fakeSearchStreamRecv{chunk: chunk})
					}
					return stream, nil
				},
			}
			client := newLegacyClient(ViewQueryClientConfig{EnableReduceStream: defaultOn, ReduceStreamChunkBytes: 64},
				&legacyPlanClient{getQueryPlan: func(_ context.Context, _ qviews.ShardID, request *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
					plan := legacySearchPlan(shard, qviews.NewQueryNode(11))
					plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: proto.Clone(request.GetLegacySearchRequest()).(*internalpb.SearchRequest)}
					return &viewpb.GetQueryPlanResponse{Plan: plan}, nil
				}},
				service, &legacyResolver{vchannels: []string{shard.VChannel}})
			on, off := true, false
			t.Run("concurrent", func(t *testing.T) {
				for _, tc := range []struct {
					name       string
					enabled    *bool
					chunkBytes int
				}{
					{"inherit", nil, 0}, {"Batch", &off, 1},
					{"one-byte", &on, 1}, {"larger", &on, 128}, {"inherited-Chunk", &on, 0},
				} {
					t.Run(tc.name, func(t *testing.T) {
						t.Parallel()
						wantStream := defaultOn
						if tc.enabled != nil {
							wantStream = *tc.enabled
						}
						wantBytes := tc.chunkBytes
						if wantBytes == 0 {
							wantBytes = 64
						}
						request := &internalpb.SearchRequest{CollectionID: 100, ReqID: int64(wantBytes), Nq: 1, Topk: 3, MetricType: "IP"}
						result, err := client.Search(context.Background(), &LegacySearchRequest{Req: request, AllowStreaming: true, EnableReduceStream: tc.enabled, ReduceStreamChunkBytes: tc.chunkBytes})
						require.NoError(t, err)
						if !wantStream {
							require.Nil(t, result.Stream)
							require.Len(t, result.Results, 1)
							return
						}
						require.NotNil(t, result.Stream)
						t.Cleanup(func() { require.NoError(t, result.Stream.Close()) })
						var ids []int64
						for {
							chunk, err := result.Stream.Recv()
							if err == io.EOF {
								break
							}
							require.NoError(t, err)
							if wantBytes == 1 {
								require.Equal(t, 1, typeutil.GetSizeOfIDs(chunk.GetResultData().GetIds()))
							}
							ids = append(ids, chunk.GetResultData().GetIds().GetIntId().GetData()...)
						}
						require.Equal(t, []int64{1, 2, 3}, ids)
					})
				}
			})
			require.Equal(t, defaultOn, client.ReduceStreamEnabled())
			require.Equal(t, 64, client.reduceStreamChunkBytes)
			_, err := client.Search(context.Background(), &LegacySearchRequest{EnableReduceStream: &on})
			require.ErrorContains(t, err, "not supported")
		})
	}
}

func TestHybridStreamingChunkOverride(t *testing.T) {
	setupTestReplicaRouting(t)
	request := hybridStreamRequest()
	shard := qviews.ShardID{ReplicaID: 1, VChannel: "hybrid-overrides"}
	var calls int
	service := &legacyServiceClient{searchOnViewStream: func(ctx context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (searchutil.ReduceStream, error) {
		require.EqualValues(t, 1, req.GetStreamChunkBytes())
		calls++
		chunk := newTestSearchChunk(req.GetLegacyReq().GetTopk(), []int64{1, 2}, []float32{.9, .8})
		chunk.MetricType = req.GetLegacyReq().GetMetricType()
		chunks, err := searchutil.SplitSearchResult(chunk, 1)
		require.NoError(t, err)
		stream := &fakeSearchStream{ctx: ctx}
		for _, part := range chunks {
			stream.recv = append(stream.recv, fakeSearchStreamRecv{chunk: part})
		}
		return stream, nil
	}}
	client := newLegacyClient(ViewQueryClientConfig{ReduceStreamChunkBytes: 64},
		&legacyPlanClient{getQueryPlan: func(_ context.Context, _ qviews.ShardID, request *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
			plan := legacySearchPlan(shard, qviews.NewQueryNode(11))
			plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: proto.Clone(request.GetLegacySearchRequest()).(*internalpb.SearchRequest)}
			return &viewpb.GetQueryPlanResponse{Plan: plan}, nil
		}},
		service, &legacyResolver{vchannels: []string{shard.VChannel}})
	on := true
	result, err := client.Search(context.Background(), &LegacySearchRequest{Req: request, AllowStreaming: true, EnableReduceStream: &on, ReduceStreamChunkBytes: 1})
	require.NoError(t, err)
	require.Len(t, result.SubStreams, 2)
	require.Equal(t, 2, calls)
	for _, stream := range result.SubStreams {
		chunk, err := stream.Recv()
		require.NoError(t, err)
		require.Equal(t, 1, typeutil.GetSizeOfIDs(chunk.GetResultData().GetIds()))
		require.NoError(t, stream.Close())
	}
	require.False(t, client.enableReduceStream)
	require.Equal(t, 64, client.reduceStreamChunkBytes)
}

func TestLegacyQueryStreamingOverrides(t *testing.T) {
	setupTestReplicaRouting(t)
	for _, defaultOn := range []bool{false, true} {
		t.Run(fmt.Sprint(defaultOn), func(t *testing.T) {
			inputs := map[string]*internalpb.RetrieveResults{
				"left": newTestQueryChunk([]int64{1, 3}), "right": newTestQueryChunk([]int64{2, 4}),
			}
			service := &legacyServiceClient{
				queryOnView: func(_ context.Context, _ qviews.WorkNode, request *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
					return &viewpb.QueryOnViewResponse{LegacyResults: proto.Clone(inputs[request.GetShardId().GetVchannel()]).(*internalpb.RetrieveResults)}, nil
				},
				queryOnViewStream: func(ctx context.Context, _ qviews.WorkNode, request *viewpb.QueryOnViewRequest) (queryutil.ReduceStream, error) {
					if request.GetStreamChunkBytes() != request.GetLegacyReq().GetBase().GetMsgID() {
						return nil, fmt.Errorf("received Chunk bytes %d, expected %d", request.GetStreamChunkBytes(), request.GetLegacyReq().GetBase().GetMsgID())
					}
					chunks, err := queryutil.SplitRetrieveResult(proto.Clone(inputs[request.GetShardId().GetVchannel()]).(*internalpb.RetrieveResults), int(request.GetStreamChunkBytes()))
					if err != nil {
						return nil, err
					}
					stream := &fakeQueryStream{ctx: ctx}
					for _, chunk := range chunks {
						stream.recv = append(stream.recv, fakeQueryStreamRecv{chunk: chunk})
					}
					return stream, nil
				},
			}
			client := newLegacyClient(ViewQueryClientConfig{EnableReduceStream: defaultOn, ReduceStreamChunkBytes: 64},
				&legacyPlanClient{getQueryPlan: func(_ context.Context, shard qviews.ShardID, request *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
					plan := legacyQueryPlan(shard, qviews.NewQueryNode(11))
					plan.Request = &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: proto.Clone(request.GetLegacyRetrieveRequest()).(*internalpb.RetrieveRequest)}
					return &viewpb.GetQueryPlanResponse{Plan: plan}, nil
				}}, service, &legacyResolver{vchannels: []string{"left", "right"}})
			on, off := true, false
			t.Run("concurrent", func(t *testing.T) {
				for _, tc := range []struct {
					name       string
					enabled    *bool
					chunkBytes int
					iterator   bool
				}{
					{"inherit", nil, 0, false}, {"Batch", &off, 1, false},
					{"one-byte", &on, 1, false}, {"larger", &on, 128, false},
					{"inherited-Chunk", &on, 0, false}, {"Chunk-does-not-enable", nil, 1, false},
					{"iterator", &on, 1, true},
				} {
					t.Run(tc.name, func(t *testing.T) {
						t.Parallel()
						wantStream := defaultOn
						if tc.enabled != nil {
							wantStream = *tc.enabled
						}
						wantBytes := tc.chunkBytes
						if wantBytes == 0 {
							wantBytes = 64
						}
						request := &internalpb.RetrieveRequest{CollectionID: 100, Base: &commonpb.MsgBase{MsgID: int64(wantBytes)}, Limit: 4, IsIterator: tc.iterator}
						result, err := client.Query(context.Background(), &LegacyQueryRequest{Req: request, AllowStreaming: true, EnableReduceStream: tc.enabled, ReduceStreamChunkBytes: tc.chunkBytes})
						require.NoError(t, err)
						if !wantStream {
							require.Nil(t, result.Stream)
							require.Len(t, result.Results, 2)
							return
						}
						require.NotNil(t, result.Stream)
						t.Cleanup(func() { require.NoError(t, result.Stream.Close()) })
						var ids, values []int64
						for {
							chunk, err := result.Stream.Recv()
							if err == io.EOF {
								break
							}
							require.NoError(t, err)
							if wantBytes == 1 {
								require.Equal(t, 1, typeutil.GetSizeOfIDs(chunk.GetIds()))
							}
							ids = append(ids, chunk.GetIds().GetIntId().GetData()...)
							values = append(values, chunk.GetFieldsData()[0].GetScalars().GetLongData().GetData()...)
						}
						require.Equal(t, []int64{1, 2, 3, 4}, ids)
						require.Equal(t, []int64{10, 20, 30, 40}, values)
					})
				}
			})
			require.Equal(t, defaultOn, client.ReduceStreamEnabled())
			require.Equal(t, 64, client.reduceStreamChunkBytes)
			_, err := client.Query(context.Background(), &LegacyQueryRequest{EnableReduceStream: &on})
			require.ErrorContains(t, err, "not supported")
		})
	}
}

func TestLegacyQueryStreamingOverridesLeaveRetainedPlansOnBatch(t *testing.T) {
	setupTestReplicaRouting(t)
	on := true
	for _, plans := range [][]ShardPlan{{}, {{
		ShardID: qviews.ShardID{ReplicaID: 1, VChannel: "retained"},
		Mvcc:    &viewpb.QueryPlanMVCC{GrowingTimetick: 100}, WorkNodes: []qviews.WorkNode{qviews.NewQueryNode(11)},
	}}} {
		service := &legacyServiceClient{queryOnView: func(context.Context, qviews.WorkNode, *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
			return &viewpb.QueryOnViewResponse{LegacyResults: newTestQueryChunk([]int64{1})}, nil
		}}
		client := newLegacyClient(ViewQueryClientConfig{EnableReduceStream: true}, nil, service, nil)
		result, err := client.Query(context.Background(), &LegacyQueryRequest{
			Req: &internalpb.RetrieveRequest{Limit: 1}, Plans: plans,
			AllowStreaming: true, EnableReduceStream: &on, ReduceStreamChunkBytes: 1,
		})
		require.NoError(t, err)
		require.Nil(t, result.Stream)
		require.Len(t, result.Results, len(plans))
		require.Equal(t, len(plans), service.queryCallCount)
	}
}
