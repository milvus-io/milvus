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
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	queryreducestream "github.com/milvus-io/milvus/internal/util/queryutil/reduce_stream"
	"github.com/milvus-io/milvus/internal/util/reduce"
	searchreducestream "github.com/milvus-io/milvus/internal/util/searchutil/reduce_stream"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestStreamingReceiveOverloadFailover(t *testing.T) {
	failure := viewerror.NewGRPCStatusFromError(merr.WrapErrTooManyRequests(10)).Err()
	require.Equal(t, codes.Unknown, status.Code(failure))
	for _, query := range []bool{false, true} {
		for _, scenario := range []string{"success", "twice", "single", "removed", "same_replica_new_version", "setup_then_receive", "view_error_phase1", "view_error_phase2"} {
			t.Run(fmt.Sprintf("query=%v/%s", query, scenario), func(t *testing.T) {
				var selected []int64
				source := &legacyPlanClient{getQueryPlan: func(_ context.Context, shard qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
					selected = append(selected, shard.ReplicaID)
					if scenario == "view_error_phase1" && len(selected) == 2 {
						return nil, viewerror.NewViewInvalidated("alternate view changed")
					}
					if scenario == "setup_then_receive" && len(selected) == 1 {
						return nil, failure
					}
					plan := legacySearchPlan(shard, qviews.NewQueryNode(shard.ReplicaID))
					if query {
						plan.Request = &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: req.GetLegacyRetrieveRequest()}
					} else {
						plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: req.GetLegacySearchRequest()}
					}
					return &viewpb.GetQueryPlanResponse{Plan: plan}, nil
				}}
				var queryWorkers []*fakeQueryStream
				var searchWorkers []*fakeSearchStream
				service := &legacyServiceClient{
					queryOnViewStream: func(_ context.Context, node qviews.WorkNode, _ *viewpb.QueryOnViewRequest) (queryreducestream.ReduceStream, error) {
						if scenario == "view_error_phase2" && len(selected) == 2 {
							return nil, viewerror.NewViewInvalidated("alternate view changed")
						}
						worker := &fakeQueryStream{}
						if node.(qviews.QueryNode).ID == 1 || scenario == "twice" || scenario == "setup_then_receive" {
							worker.recv = []fakeQueryStreamRecv{{err: failure}}
						} else {
							worker.recv = []fakeQueryStreamRecv{{chunk: newTestQueryChunk([]int64{2})}}
						}
						queryWorkers = append(queryWorkers, worker)
						return worker, nil
					},
					searchOnViewStream: func(_ context.Context, node qviews.WorkNode, _ *viewpb.SearchOnViewRequest) (searchreducestream.ReduceStream, error) {
						if scenario == "view_error_phase2" && len(selected) == 2 {
							return nil, viewerror.NewViewInvalidated("alternate view changed")
						}
						worker := &fakeSearchStream{}
						if node.(qviews.QueryNode).ID == 1 || scenario == "twice" || scenario == "setup_then_receive" {
							worker.recv = []fakeSearchStreamRecv{{err: failure}}
						} else {
							worker.recv = []fakeSearchStreamRecv{{chunk: newTestSearchChunk(1, []int64{2}, []float32{1})}}
						}
						searchWorkers = append(searchWorkers, worker)
						return worker, nil
					},
				}
				client := newShardViewQueryClient(1, source, service)
				route := &replicaRoute{changed: make(chan struct{}), snapshot: qviews.ReplicaSnapshot{Valid: true, Replicas: []qviews.Replica{{ReplicaID: 1}, {ReplicaID: 2}, {ReplicaID: 3}}}}
				retryRound := scenario == "view_error_phase1" || scenario == "view_error_phase2"
				if retryRound {
					client.maxRetries = 4
					route.snapshot.Replicas = route.snapshot.Replicas[:2]
				}
				if scenario == "single" {
					route.snapshot.Replicas = route.snapshot.Replicas[:1]
				}
				client.replicas.channels["channel"] = route
				// Update topology after the stream opens, before its first Recv.
				update := func() {
					route.mu.Lock()
					defer route.mu.Unlock()
					if scenario == "removed" {
						route.snapshot.Replicas = nil
					}
					if scenario == "same_replica_new_version" {
						candidate := route.snapshot.Replicas[0]
						candidate.MaxUpViewVersion.QueryVersion++
						route.snapshot.Replicas = []qviews.Replica{candidate}
					}
				}
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				var recvErr error
				if query {
					stream, _, err := client.QueryStream(ctx, &ShardQueryRequest{VChannel: "channel", Req: &internalpb.RetrieveRequest{Limit: 1}}, 128)
					require.NoError(t, err)
					update()
					_, recvErr = stream.Recv()
					require.NoError(t, stream.Close())
				} else {
					streams, _, err := client.SearchStreams(ctx, &ShardSearchRequest{VChannel: "channel", Req: newTestSearchRequest(1)}, 128)
					require.NoError(t, err)
					update()
					_, recvErr = streams[0].Recv()
					require.NoError(t, streams[0].Close())
				}
				if scenario == "success" || retryRound {
					require.NoError(t, recvErr)
				} else {
					require.True(t, isOverloadErr(recvErr), "%v", recvErr)
				}
				require.NoError(t, ctx.Err(), "overload must not wait for directory refresh")
				if scenario == "single" || scenario == "removed" || scenario == "same_replica_new_version" {
					require.Equal(t, []int64{1}, selected)
				} else if retryRound {
					require.Equal(t, []int64{1, 2, 2}, selected, "a ViewError on the alternative must not re-enable the refusing version")
				} else {
					require.Equal(t, []int64{1, 2}, selected)
				}
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

func TestStreamingOverloadAfterOutputIsTerminal(t *testing.T) {
	failure := viewerror.NewGRPCStatusFromError(merr.WrapErrTooManyRequests(10)).Err()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	child := &fakeQueryStream{recv: []fakeQueryStreamRecv{{chunk: newTestQueryChunk([]int64{1})}, {err: failure}}}
	stream := &shardReduceStream[*internalpb.RetrieveResults]{
		ctx: ctx, cancel: cancel, stream: child,
		restart: func(error) (reduce.ReduceStream[*internalpb.RetrieveResults], context.CancelFunc, *ShardPlan, error) {
			t.Fatal("must not retry after publishing output")
			return nil, nil, nil, io.EOF
		},
	}
	_, err := stream.Recv()
	require.NoError(t, err)
	_, err = stream.Recv()
	require.True(t, isOverloadErr(err))
	require.NoError(t, stream.Close())
}
