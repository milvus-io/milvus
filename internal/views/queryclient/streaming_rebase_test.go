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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	queryreducestream "github.com/milvus-io/milvus/internal/util/queryutil/reduce_stream"
	"github.com/milvus-io/milvus/internal/util/reduce"
	searchreducestream "github.com/milvus-io/milvus/internal/util/searchutil/reduce_stream"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestWorkerReduceStreamSharedContract(t *testing.T) {
	queryChunk := newTestQueryChunk([]int64{1})
	queryChild := &fakeQueryStream{recv: []fakeQueryStreamRecv{{chunk: queryChunk}}}
	var queryStream reduce.ReduceStream[*internalpb.RetrieveResults] = &workerReduceStream[*internalpb.RetrieveResults]{ReduceStream: queryChild}
	gotQuery, err := queryStream.Recv()
	require.NoError(t, err)
	require.Same(t, queryChunk, gotQuery)
	_, err = queryStream.Recv()
	require.ErrorIs(t, err, io.EOF)
	_, err = queryStream.Interrupt()
	require.ErrorContains(t, err, "not implemented")
	require.NoError(t, queryStream.Close())
	require.Equal(t, 1, queryChild.closeCount())

	searchChunk := newTestSearchChunk(1, []int64{1}, []float32{1})
	searchChild := &fakeSearchStream{recv: []fakeSearchStreamRecv{{chunk: searchChunk}}}
	var searchStream reduce.ReduceStream[*internalpb.SearchResults] = &workerReduceStream[*internalpb.SearchResults]{ReduceStream: searchChild}
	gotSearch, err := searchStream.Recv()
	require.NoError(t, err)
	require.Same(t, searchChunk, gotSearch)
	_, err = searchStream.Recv()
	require.ErrorIs(t, err, io.EOF)
	_, err = searchStream.Interrupt()
	require.ErrorContains(t, err, "not implemented")
	require.NoError(t, searchStream.Close())
	require.Equal(t, 1, searchChild.closeCount())
}

func TestStreamingReceiveFailureRetryBoundary(t *testing.T) {
	for _, query := range []bool{false, true} {
		for _, stage := range []string{"before_data", "buffered", "emitted"} {
			t.Run(fmt.Sprintf("query_%t/%s", query, stage), func(t *testing.T) {
				afterChunk := stage == "emitted"
				chunkBytes := 1
				if stage == "buffered" {
					chunkBytes = 1 << 20
				}
				router, directory := newTestReplicaRouter(t)
				channels := []string{"p_1v0", "p_1v1"}
				version := qviews.QueryViewVersion{QueryVersion: 1}
				for index, channel := range channels {
					directory.Set(channel, 1, &version, false)
					replicas := 1
					if index == 0 {
						directory.Set(channel, 2, &version, false)
						replicas++
					}
					route, err := router.route(channel)
					require.NoError(t, err)
					require.Eventually(t, func() bool {
						route.mu.Lock()
						defer route.mu.Unlock()
						return len(route.snapshot.Replicas) == replicas
					}, time.Second, time.Millisecond)
				}
				var mu sync.Mutex
				selected := make(map[string][]int64)
				var queryWorkers []*fakeQueryStream
				var searchWorkers []*fakeSearchStream
				source := &legacyPlanClient{getQueryPlan: func(_ context.Context, shard qviews.ShardID, request *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
					mu.Lock()
					selected[shard.VChannel] = append(selected[shard.VChannel], shard.ReplicaID)
					attempt := len(selected[shard.VChannel])
					mu.Unlock()
					workerID := int64(attempt * 100)
					if shard.VChannel == channels[1] {
						workerID++
					}
					plan := legacySearchPlan(shard, qviews.NewQueryNode(workerID))
					plan.Mvcc = &viewpb.QueryPlanMVCC{GrowingTimetick: uint64(attempt * 100), TransformingTimetick: uint64(attempt*100 - 10)}
					plan.Version = &viewpb.QueryViewVersion{QueryVersion: int64(attempt)}
					plan.WorkNodes[0].SegmentSelection = &viewpb.SegmentSelection{SegmentIds: []int64{workerID}, Mode: viewpb.SegmentSelection_Include}
					if query {
						plan.Request = &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: request.GetLegacyRetrieveRequest()}
					} else {
						plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: request.GetLegacySearchRequest()}
					}
					return &viewpb.GetQueryPlanResponse{Plan: plan, ExecutionSummary: &viewpb.ExecutionSummary{
						GlobalOptimizer: &viewpb.GlobalOptimizerExecutionSummary{Flags: uint32(attempt)},
					}}, nil
				}}
				failure := status.Error(codes.Unavailable, "worker disconnected")
				service := &legacyServiceClient{
					queryOnViewStream: func(_ context.Context, node qviews.WorkNode, _ *viewpb.QueryOnViewRequest) (queryreducestream.ReduceStream, error) {
						id := node.(qviews.QueryNode).ID
						worker := &fakeQueryStream{}
						if id != 100 || stage != "before_data" {
							chunk := newTestQueryChunk([]int64{id, id + 2, id + 4})
							chunk.AllRetrieveCount = id
							worker.recv = append(worker.recv, fakeQueryStreamRecv{chunk: chunk})
						}
						if id == 100 {
							worker.recv = append(worker.recv, fakeQueryStreamRecv{err: failure})
						}
						mu.Lock()
						queryWorkers = append(queryWorkers, worker)
						mu.Unlock()
						return worker, nil
					},
					searchOnViewStream: func(_ context.Context, node qviews.WorkNode, _ *viewpb.SearchOnViewRequest) (searchreducestream.ReduceStream, error) {
						id := node.(qviews.QueryNode).ID
						worker := &fakeSearchStream{}
						if id != 100 || stage != "before_data" {
							score := float32(1) - float32(id%100)*0.01
							chunk := newTestSearchChunk(6, []int64{id, id + 2, id + 4}, []float32{score, score - .02, score - .04})
							chunk.ResultData.AllSearchCount = id
							worker.recv = append(worker.recv, fakeSearchStreamRecv{chunk: chunk})
						}
						if id == 100 {
							worker.recv = append(worker.recv, fakeSearchStreamRecv{err: failure})
						}
						mu.Lock()
						searchWorkers = append(searchWorkers, worker)
						mu.Unlock()
						return worker, nil
					},
				}
				client := NewLegacyViewQueryClient(ViewQueryClientConfig{MaxRetries: 2}, source, service, &legacyResolver{vchannels: channels}).(*legacyOnlyClient)
				client.legacy.shardClient.replicas = router
				defer client.Close()
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				var retryErrors []error
				attempts := 1
				if afterChunk {
					attempts = 2
				}
				for attempt := 0; attempt < attempts; attempt++ {
					var ids []int64
					var plans []ShardPlan
					var recvErr error
					var totalCount int64
					if query {
						result, err := client.Legacy().Query(ctx, &LegacyQueryRequest{
							Req: &internalpb.RetrieveRequest{Limit: 6}, AllowStreaming: true, ReduceStreamChunkBytes: chunkBytes, RetryErrors: retryErrors,
						})
						require.NoError(t, err)
						require.Len(t, selected[channels[0]], attempt+1, "opening must not prefetch")
						for {
							chunk, err := result.Stream.Recv()
							if err != nil {
								recvErr = err
								break
							}
							ids = append(ids, chunk.GetIds().GetIntId().GetData()...)
							totalCount += chunk.GetAllRetrieveCount()
						}
						require.NoError(t, result.Stream.Close())
						plans = result.GetPlans()
					} else {
						request := newTestSearchRequest(6)
						request.IsIterator = false
						result, err := client.Legacy().Search(ctx, &LegacySearchRequest{
							Req: request, AllowStreaming: true, ReduceStreamChunkBytes: chunkBytes, RetryErrors: retryErrors,
						})
						require.NoError(t, err)
						require.Len(t, selected[channels[0]], attempt+1, "opening must not prefetch")
						for {
							chunk, err := result.Stream.Recv()
							if err != nil {
								recvErr = err
								break
							}
							ids = append(ids, chunk.GetResultData().GetIds().GetIntId().GetData()...)
							totalCount += chunk.GetResultData().GetAllSearchCount()
						}
						require.NoError(t, result.Stream.Close())
						plans = result.GetPlans()
					}
					require.Len(t, plans, 2)
					if !afterChunk {
						require.ErrorIs(t, recvErr, io.EOF)
						want := []int64{200, 101, 202, 103, 204, 105}
						if query {
							want = []int64{101, 103, 105, 200, 202, 204}
						}
						require.Equal(t, want, ids)
						require.Equal(t, []int64{1, 2}, selected[channels[0]])
						require.Equal(t, []int64{1}, selected[channels[1]], "local retry must retain the healthy shard")
						require.EqualValues(t, 200, plans[0].Mvcc.GetGrowingTimetick())
						require.EqualValues(t, 100, plans[1].Mvcc.GetGrowingTimetick())
						require.EqualValues(t, 2, plans[0].ShardID.ReplicaID)
						require.EqualValues(t, 2, plans[0].Version.GetQueryVersion())
						require.Equal(t, []int64{200}, plans[0].Selections[0].GetSegmentSelection().GetSegmentIds())
						require.EqualValues(t, 2, plans[0].ExecutionSummary.GetGlobalOptimizer().GetFlags())
						require.EqualValues(t, 301, totalCount, "failed attempt metadata must be discarded")
						if !query {
							service.queryOnView = func(_ context.Context, node qviews.WorkNode, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
								plan := plans[0]
								if req.GetShardId().GetVchannel() == channels[1] {
									plan = plans[1]
								}
								require.Equal(t, plan.WorkNodes[0], node)
								require.True(t, proto.Equal(plan.ShardID.IntoProto(), req.GetShardId()))
								require.True(t, proto.Equal(plan.Version, req.GetVersion()))
								require.True(t, proto.Equal(plan.Mvcc, req.GetMvcc()))
								require.True(t, proto.Equal(plan.Selections[0].GetSegmentSelection(), req.GetSegmentSelection()))
								require.Equal(t, plan.Mvcc.GetTransformingTimetick(), req.GetLegacyReq().GetMvccTimestamp())
								return &viewpb.QueryOnViewResponse{LegacyResults: &internalpb.RetrieveResults{Status: merr.Success()}}, nil
							}
							_, err := client.Legacy().Query(ctx, &LegacyQueryRequest{Req: &internalpb.RetrieveRequest{Limit: 6}, Plans: plans})
							require.NoError(t, err)
							require.Len(t, selected[channels[0]], 2, "requery must retain the successful plan")
							require.Len(t, selected[channels[1]], 1)
						}
					} else if attempt == 0 {
						require.Equal(t, codes.Unavailable, status.Code(recvErr))
						var failed *workNodeFailure
						require.True(t, errors.As(recvErr, &failed))
						require.Equal(t, qviews.NewQueryNode(100), failed.node)
						require.Equal(t, qviews.ShardID{ReplicaID: 1, VChannel: channels[0]}, failed.shardID)
						require.NotEmpty(t, ids, "disconnect must occur after the final stream emits a Chunk")
						require.Len(t, selected[channels[0]], 1, "output forbids local retry")
						require.Len(t, selected[channels[1]], 1)
						retryErrors = append(retryErrors, recvErr)
					} else {
						require.ErrorIs(t, recvErr, io.EOF)
						require.Equal(t, []int64{200, 201, 202, 203, 204, 205}, ids)
						require.Equal(t, []int64{1, 2}, selected[channels[0]])
						require.Equal(t, []int64{1, 1}, selected[channels[1]], "healthy shard is reopened without excluding its replica")
						for _, plan := range plans {
							require.EqualValues(t, 200, plan.Mvcc.GetGrowingTimetick())
						}
					}
					for _, worker := range queryWorkers {
						require.Equal(t, 1, worker.closeCount())
					}
					for _, worker := range searchWorkers {
						require.Equal(t, 1, worker.closeCount())
					}
				}
			})
		}
	}
}

func TestStreamingShardRetryBudget(t *testing.T) {
	setupTestReplicaRouting(t)
	for _, query := range []bool{false, true} {
		for _, scenario := range []string{"receive", "open_then_receive", "terminal", "eof"} {
			t.Run(fmt.Sprintf("query_%t/%s", query, scenario), func(t *testing.T) {
				var plans atomic.Int32
				source := &legacyPlanClient{getQueryPlan: func(_ context.Context, shard qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
					plans.Add(1)
					plan := legacySearchPlan(shard, qviews.NewQueryNode(11))
					if query {
						plan.Request = &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: req.GetLegacyRetrieveRequest()}
					} else {
						plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: req.GetLegacySearchRequest()}
					}
					return &viewpb.GetQueryPlanResponse{Plan: plan}, nil
				}}
				failure := status.Error(codes.Unavailable, "receive failed")
				wantAttempts := int32(2)
				switch scenario {
				case "terminal":
					failure = status.Error(codes.InvalidArgument, "invalid request")
					wantAttempts = 1
				case "eof":
					failure = io.EOF
					wantAttempts = 1
				}
				var queryWorkers []*fakeQueryStream
				var searchWorkers []*fakeSearchStream
				service := &legacyServiceClient{
					queryOnViewStream: func(context.Context, qviews.WorkNode, *viewpb.QueryOnViewRequest) (queryreducestream.ReduceStream, error) {
						if scenario == "open_then_receive" && plans.Load() == 1 {
							return nil, status.Error(codes.Unavailable, "opening failed")
						}
						worker := &fakeQueryStream{recv: []fakeQueryStreamRecv{{err: failure}}}
						queryWorkers = append(queryWorkers, worker)
						return worker, nil
					},
					searchOnViewStream: func(context.Context, qviews.WorkNode, *viewpb.SearchOnViewRequest) (searchreducestream.ReduceStream, error) {
						if scenario == "open_then_receive" && plans.Load() == 1 {
							return nil, status.Error(codes.Unavailable, "opening failed")
						}
						worker := &fakeSearchStream{recv: []fakeSearchStreamRecv{{err: failure}}}
						searchWorkers = append(searchWorkers, worker)
						return worker, nil
					},
				}
				client := newShardViewQueryClient(2, source, service)
				defer client.replicas.Close()
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				var err, again error
				if query {
					stream, _, openErr := client.QueryStream(ctx, &ShardQueryRequest{VChannel: "p_1v0", Req: &internalpb.RetrieveRequest{Limit: 1}}, 128)
					require.NoError(t, openErr)
					_, err = stream.Recv()
					_, again = stream.Recv()
					require.NoError(t, stream.Close())
					require.NoError(t, stream.Close())
				} else {
					streams, _, openErr := client.SearchStreams(ctx, &ShardSearchRequest{VChannel: "p_1v0", Req: newTestSearchRequest(1)}, 128)
					require.NoError(t, openErr)
					_, err = streams[0].Recv()
					_, again = streams[0].Recv()
					require.NoError(t, streams[0].Close())
					require.NoError(t, streams[0].Close())
				}
				require.ErrorIs(t, err, failure)
				require.ErrorIs(t, again, failure)
				require.Equal(t, wantAttempts, plans.Load(), "opening and Recv must share one shard budget")
				for _, worker := range queryWorkers {
					require.Equal(t, 1, worker.closeCount())
				}
				for _, worker := range searchWorkers {
					require.Equal(t, 1, worker.closeCount())
				}
			})
		}
	}
}

func TestShardReduceStreamMetadataCommitsAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	failure := status.Error(codes.Unavailable, "after metadata")
	child := &fakeQueryStream{recv: []fakeQueryStreamRecv{
		{chunk: &internalpb.RetrieveResults{AllRetrieveCount: 7}}, {err: failure},
	}}
	stream := &shardReduceStream[*internalpb.RetrieveResults]{
		ctx: ctx, cancel: cancel, stream: child,
		restart: func(error) (reduce.ReduceStream[*internalpb.RetrieveResults], *ShardPlan, error) {
			t.Fatal("metadata-only output also forbids local retry")
			return nil, nil, nil
		},
	}
	chunk, err := stream.Recv()
	require.NoError(t, err)
	require.EqualValues(t, 7, chunk.GetAllRetrieveCount())
	_, err = stream.Recv()
	require.ErrorIs(t, err, failure)
	require.NoError(t, stream.Close())
	require.Equal(t, 1, child.closeCount())
}

type blockingRetryTestStream struct {
	fakeQueryStream
	ctx     context.Context
	entered chan struct{}
}

func (s *blockingRetryTestStream) Recv() (*internalpb.RetrieveResults, error) {
	close(s.entered)
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func TestShardReduceStreamCloseDuringReceiveOrReplan(t *testing.T) {
	for _, replanning := range []bool{false, true} {
		t.Run(fmt.Sprintf("replanning_%t", replanning), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			entered := make(chan struct{})
			child := &blockingRetryTestStream{ctx: ctx, entered: entered}
			stream := &shardReduceStream[*internalpb.RetrieveResults]{ctx: ctx, cancel: cancel, stream: child}
			if replanning {
				stream.stream = &fakeQueryStream{recv: []fakeQueryStreamRecv{{err: status.Error(codes.Unavailable, "retry")}}}
				stream.restart = func(error) (reduce.ReduceStream[*internalpb.RetrieveResults], *ShardPlan, error) {
					close(entered)
					<-ctx.Done()
					return nil, nil, ctx.Err()
				}
			}
			received := make(chan error, 1)
			go func() { _, err := stream.Recv(); received <- err }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("receive/replan did not start")
			}
			closed := make(chan error, 1)
			go func() { closed <- stream.Close() }()
			select {
			case err := <-closed:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("Close did not unblock receive/replan")
			}
			require.ErrorIs(t, <-received, context.Canceled)
			require.NoError(t, stream.Close())
			_, err := stream.Recv()
			require.ErrorIs(t, err, io.ErrClosedPipe)
			if !replanning {
				require.Equal(t, 1, child.closeCount())
			}
		})
	}
}

func TestStreamingProxyRetryPrefersUntriedReplica(t *testing.T) {
	for _, query := range []bool{false, true} {
		for _, alternative := range []bool{false, true} {
			t.Run(fmt.Sprintf("query_%t/alternative_%t", query, alternative), func(t *testing.T) {
				router, directory := newTestReplicaRouter(t)
				version := qviews.QueryViewVersion{QueryVersion: 1}
				directory.Set("p_1v0", 1, &version, false)
				if alternative {
					directory.Set("p_1v0", 2, &version, false)
				}
				route, err := router.route("p_1v0")
				require.NoError(t, err)
				wantCount := 1
				if alternative {
					wantCount = 2
				}
				require.Eventually(t, func() bool {
					route.mu.Lock()
					defer route.mu.Unlock()
					return len(route.snapshot.Replicas) == wantCount
				}, time.Second, time.Millisecond)
				var selected []int64
				source := &legacyPlanClient{getQueryPlan: func(_ context.Context, shard qviews.ShardID, request *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
					selected = append(selected, shard.ReplicaID)
					plan := legacySearchPlan(shard, qviews.NewQueryNode(10+shard.ReplicaID))
					if query {
						plan.Request = &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: proto.Clone(request.GetLegacyRetrieveRequest()).(*internalpb.RetrieveRequest)}
					} else {
						plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: proto.Clone(request.GetLegacySearchRequest()).(*internalpb.SearchRequest)}
					}
					return &viewpb.GetQueryPlanResponse{Plan: plan}, nil
				}}
				service := &legacyServiceClient{
					queryOnViewStream: func(context.Context, qviews.WorkNode, *viewpb.QueryOnViewRequest) (queryreducestream.ReduceStream, error) {
						return &fakeQueryStream{}, nil
					},
					searchOnViewStream: func(context.Context, qviews.WorkNode, *viewpb.SearchOnViewRequest) (searchreducestream.ReduceStream, error) {
						return &fakeSearchStream{}, nil
					},
				}
				failure := &workNodeFailure{node: qviews.NewQueryNode(11), shardID: qviews.ShardID{ReplicaID: 1, VChannel: "p_1v0"}, err: status.Error(codes.Unavailable, "worker unavailable")}
				client := newShardViewQueryClient(1, source, service)
				client.replicas = router
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				var plan *ShardPlan
				if query {
					stream, got, err := client.QueryStream(ctx, &ShardQueryRequest{VChannel: "p_1v0", Req: &internalpb.RetrieveRequest{Limit: 1}, retryErrors: []error{failure}}, 128)
					require.NoError(t, err)
					plan = got
					require.NoError(t, stream.Close())
				} else {
					streams, got, err := client.SearchStreams(ctx, &ShardSearchRequest{VChannel: "p_1v0", Req: newTestSearchRequest(1), retryErrors: []error{failure}}, 128)
					require.NoError(t, err)
					plan = got
					require.NoError(t, streams[0].Close())
				}
				wantReplica := int64(1)
				if alternative {
					wantReplica = 2
				}
				require.Equal(t, []int64{wantReplica}, selected)
				require.Equal(t, wantReplica, plan.ShardID.ReplicaID)
			})
		}
	}
}

func TestStreamingShardFailureAfterFirstChunkRetainsWorker(t *testing.T) {
	setupTestReplicaRouting(t)
	for _, query := range []bool{false, true} {
		t.Run(fmt.Sprintf("query_%t", query), func(t *testing.T) {
			shard := qviews.ShardID{ReplicaID: 1, VChannel: "p_1v0"}
			node := qviews.NewQueryNode(11)
			var plans atomic.Int32
			source := &legacyPlanClient{getQueryPlan: func(_ context.Context, shard qviews.ShardID, request *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
				plans.Add(1)
				plan := legacySearchPlan(shard, node)
				if query {
					plan.Request = &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: proto.Clone(request.GetLegacyRetrieveRequest()).(*internalpb.RetrieveRequest)}
				} else {
					plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: proto.Clone(request.GetLegacySearchRequest()).(*internalpb.SearchRequest)}
				}
				return &viewpb.GetQueryPlanResponse{Plan: plan}, nil
			}}
			failure := status.Error(codes.Unavailable, "worker unavailable")
			service := &legacyServiceClient{
				queryOnViewStream: func(_ context.Context, _ qviews.WorkNode, _ *viewpb.QueryOnViewRequest) (queryreducestream.ReduceStream, error) {
					return &fakeQueryStream{recv: []fakeQueryStreamRecv{{chunk: newTestQueryChunk([]int64{1, 2})}, {err: failure}}}, nil
				},
				searchOnViewStream: func(_ context.Context, _ qviews.WorkNode, _ *viewpb.SearchOnViewRequest) (searchreducestream.ReduceStream, error) {
					return &fakeSearchStream{recv: []fakeSearchStreamRecv{{chunk: newTestSearchChunk(3, []int64{1, 2}, []float32{0.9, 0.8})}, {err: failure}}}, nil
				},
			}
			client := newShardViewQueryClient(2, source, service)
			var recvErr error
			if query {
				stream, _, err := client.QueryStream(t.Context(), &ShardQueryRequest{VChannel: shard.VChannel, Req: &internalpb.RetrieveRequest{Limit: 2}}, 1)
				require.NoError(t, err)
				defer stream.Close()
				_, err = stream.Recv()
				require.NoError(t, err)
				_, recvErr = stream.Recv()
			} else {
				streams, _, err := client.SearchStreams(t.Context(), &ShardSearchRequest{VChannel: shard.VChannel, Req: newTestSearchRequest(3)}, 1)
				require.NoError(t, err)
				defer streams[0].Close()
				_, err = streams[0].Recv()
				require.NoError(t, err)
				for recvErr == nil {
					_, recvErr = streams[0].Recv()
				}
			}
			require.Equal(t, codes.Unavailable, status.Code(recvErr))
			var failed *workNodeFailure
			require.True(t, errors.As(recvErr, &failed))
			require.Equal(t, node, failed.node)
			require.Equal(t, shard, failed.shardID)
			require.EqualValues(t, 1, plans.Load(), "after output, recovery belongs to Proxy")
		})
	}
}

func TestHybridReceiveFailureDoesNotReplanShard(t *testing.T) {
	setupTestReplicaRouting(t)
	var plans atomic.Int32
	request := hybridStreamRequest()
	source := &legacyPlanClient{getQueryPlan: func(_ context.Context, shard qviews.ShardID, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
		plans.Add(1)
		plan := legacySearchPlan(shard, qviews.NewQueryNode(11))
		plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: request}
		return &viewpb.GetQueryPlanResponse{Plan: plan}, nil
	}}
	var children []*fakeSearchStream
	service := &legacyServiceClient{searchOnViewStream: func(context.Context, qviews.WorkNode, *viewpb.SearchOnViewRequest) (searchreducestream.ReduceStream, error) {
		child := &fakeSearchStream{recv: []fakeSearchStreamRecv{{err: status.Error(codes.Unavailable, "receive failed")}}}
		children = append(children, child)
		return child, nil
	}}
	client := newShardViewQueryClient(3, source, service)
	defer client.replicas.Close()
	streams, _, err := client.SearchStreams(t.Context(), &ShardSearchRequest{VChannel: "p_1v0", Req: request}, 128)
	require.NoError(t, err)
	_, err = streams[0].Recv()
	require.Equal(t, codes.Unavailable, status.Code(err))
	for _, stream := range streams {
		require.NoError(t, stream.Close())
	}
	require.EqualValues(t, 1, plans.Load())
	for _, child := range children {
		require.Equal(t, 1, child.closeCount())
	}
}

func TestStreamingPreservesPlanSelectionsAndSummary(t *testing.T) {
	setupTestReplicaRouting(t)
	for _, kind := range []string{"Search", "Hybrid", "Query"} {
		t.Run(kind, func(t *testing.T) {
			selection := &viewpb.SegmentSelection{SegmentIds: []int64{123}, Mode: viewpb.SegmentSelection_Include}
			subSelections := []*viewpb.SegmentSelection{{SegmentIds: []int64{456}, Mode: viewpb.SegmentSelection_Include}, {Mode: viewpb.SegmentSelection_All}}
			summary := &viewpb.ExecutionSummary{GlobalOptimizer: &viewpb.GlobalOptimizerExecutionSummary{Flags: 1 << viewpb.GlobalOptimizerExecutionSummary_PrunedByScalarFiltering}}
			source := &legacyPlanClient{getQueryPlan: func(_ context.Context, shard qviews.ShardID, request *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
				require.EqualValues(t, 1, shard.ReplicaID)
				plan := legacySearchPlan(shard, qviews.NewQueryNode(11))
				if kind == "Query" {
					plan.Request = &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: proto.Clone(request.GetLegacyRetrieveRequest()).(*internalpb.RetrieveRequest)}
				} else {
					plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: proto.Clone(request.GetLegacySearchRequest()).(*internalpb.SearchRequest)}
				}
				plan.WorkNodes[0].SegmentSelection = selection
				if kind == "Hybrid" {
					plan.WorkNodes[0].SubSelections = subSelections
				}
				return &viewpb.GetQueryPlanResponse{Plan: plan, ExecutionSummary: summary}, nil
			}}
			var workerCtx context.Context
			service := &legacyServiceClient{
				searchOnViewStream: func(ctx context.Context, _ qviews.WorkNode, request *viewpb.SearchOnViewRequest) (searchreducestream.ReduceStream, error) {
					workerCtx = ctx
					want := selection
					if kind == "Hybrid" {
						want = subSelections[request.GetLegacyReq().GetFieldId()-101]
					}
					require.True(t, proto.Equal(want, request.GetSegmentSelection()))
					require.EqualValues(t, 128, request.GetStreamChunkBytes())
					return &fakeSearchStream{ctx: ctx}, nil
				},
				queryOnViewStream: func(ctx context.Context, _ qviews.WorkNode, request *viewpb.QueryOnViewRequest) (queryreducestream.ReduceStream, error) {
					workerCtx = ctx
					require.True(t, proto.Equal(selection, request.GetSegmentSelection()))
					require.EqualValues(t, 128, request.GetStreamChunkBytes())
					return &fakeQueryStream{ctx: ctx}, nil
				},
			}
			client := newShardViewQueryClient(1, source, service)
			var retained *ShardPlan
			if kind == "Query" {
				stream, plan, err := client.QueryStream(t.Context(), &ShardQueryRequest{VChannel: "p_1v0", Req: &internalpb.RetrieveRequest{Limit: 1}}, 128)
				require.NoError(t, err)
				retained = plan
				_, err = stream.Recv()
				require.ErrorIs(t, err, io.EOF)
				require.NoError(t, workerCtx.Err(), "opening must retain the stream context")
				require.NoError(t, stream.Close())
			} else {
				request := newTestSearchRequest(1)
				if kind == "Hybrid" {
					request = hybridStreamRequest()
				}
				streams, plan, err := client.SearchStreams(t.Context(), &ShardSearchRequest{VChannel: "p_1v0", Req: request}, 128)
				require.NoError(t, err)
				retained = plan
				for _, stream := range streams {
					_, err := stream.Recv()
					require.ErrorIs(t, err, io.EOF)
					require.NoError(t, workerCtx.Err(), "opening must retain the stream context")
					require.NoError(t, stream.Close())
				}
			}
			require.True(t, proto.Equal(summary, retained.ExecutionSummary))
			require.Len(t, retained.Selections, 1)
			require.True(t, proto.Equal(selection, retained.Selections[0].GetSegmentSelection()))
		})
	}
}

func TestStreamingShardFailsOverDiscoveredReplicas(t *testing.T) {
	for _, scenario := range []struct{ query, receive bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
		t.Run(fmt.Sprintf("query_%t/receive_%t", scenario.query, scenario.receive), func(t *testing.T) {
			query := scenario.query
			router, directory := newTestReplicaRouter(t)
			version := qviews.QueryViewVersion{QueryVersion: 1}
			directory.Set("p_1v0", 1, &version, false)
			directory.Set("p_1v0", 2, &version, false)
			route, err := router.route("p_1v0")
			require.NoError(t, err)
			require.Eventually(t, func() bool { route.mu.Lock(); defer route.mu.Unlock(); return len(route.snapshot.Replicas) == 2 }, time.Second, time.Millisecond)
			var selected []int64
			source := &legacyPlanClient{getQueryPlan: func(_ context.Context, shard qviews.ShardID, request *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
				selected = append(selected, shard.ReplicaID)
				plan := legacySearchPlan(shard, qviews.NewQueryNode(10+shard.ReplicaID))
				if query {
					plan.Request = &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: proto.Clone(request.GetLegacyRetrieveRequest()).(*internalpb.RetrieveRequest)}
				} else {
					plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: proto.Clone(request.GetLegacySearchRequest()).(*internalpb.SearchRequest)}
				}
				return &viewpb.GetQueryPlanResponse{Plan: plan}, nil
			}}
			service := &legacyServiceClient{
				searchOnViewStream: func(ctx context.Context, node qviews.WorkNode, _ *viewpb.SearchOnViewRequest) (searchreducestream.ReduceStream, error) {
					if node.(qviews.QueryNode).ID == 11 {
						if scenario.receive {
							return &fakeSearchStream{ctx: ctx, recv: []fakeSearchStreamRecv{{err: picker.ErrSubConnNotExist}}}, nil
						}
						return nil, picker.ErrSubConnNotExist
					}
					return &fakeSearchStream{ctx: ctx}, nil
				},
				queryOnViewStream: func(ctx context.Context, node qviews.WorkNode, _ *viewpb.QueryOnViewRequest) (queryreducestream.ReduceStream, error) {
					if node.(qviews.QueryNode).ID == 11 {
						if scenario.receive {
							return &fakeQueryStream{ctx: ctx, recv: []fakeQueryStreamRecv{{err: picker.ErrSubConnNotExist}}}, nil
						}
						return nil, picker.ErrSubConnNotExist
					}
					return &fakeQueryStream{ctx: ctx}, nil
				},
			}
			client := newShardViewQueryClient(1, source, service)
			client.replicas = router
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			var retained *ShardPlan
			if query {
				stream, plan, err := client.QueryStream(ctx, &ShardQueryRequest{VChannel: "p_1v0", Req: &internalpb.RetrieveRequest{Limit: 1}}, 128)
				require.NoError(t, err)
				retained = plan
				_, err = stream.Recv()
				require.ErrorIs(t, err, io.EOF)
				require.NoError(t, stream.Close())
			} else {
				streams, plan, err := client.SearchStreams(ctx, &ShardSearchRequest{VChannel: "p_1v0", Req: newTestSearchRequest(1)}, 128)
				require.NoError(t, err)
				retained = plan
				_, err = streams[0].Recv()
				require.ErrorIs(t, err, io.EOF)
				require.NoError(t, streams[0].Close())
			}
			require.Equal(t, []int64{1, 2}, selected)
			require.EqualValues(t, 2, retained.ShardID.ReplicaID)
		})
	}
}
