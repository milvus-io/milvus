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
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/searchutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

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
				searchOnViewStream: func(ctx context.Context, _ qviews.WorkNode, request *viewpb.SearchOnViewRequest) (searchutil.ReduceStream, error) {
					workerCtx = ctx
					want := selection
					if kind == "Hybrid" {
						want = subSelections[request.GetLegacyReq().GetFieldId()-101]
					}
					require.True(t, proto.Equal(want, request.GetSegmentSelection()))
					require.EqualValues(t, 128, request.GetStreamChunkBytes())
					return &fakeSearchStream{ctx: ctx}, nil
				},
				queryOnViewStream: func(ctx context.Context, _ qviews.WorkNode, request *viewpb.QueryOnViewRequest) (queryutil.ReduceStream, error) {
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
					require.NoError(t, stream.Close())
				}
			}
			require.NoError(t, workerCtx.Err(), "opening fan-out must not cancel the request-scoped stream context")
			require.True(t, proto.Equal(summary, retained.ExecutionSummary))
			require.Len(t, retained.Selections, 1)
			require.True(t, proto.Equal(selection, retained.Selections[0].GetSegmentSelection()))
		})
	}
}

func TestStreamingShardFailsOverDiscoveredReplicas(t *testing.T) {
	for _, query := range []bool{false, true} {
		t.Run(map[bool]string{false: "Search", true: "Query"}[query], func(t *testing.T) {
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
				searchOnViewStream: func(ctx context.Context, node qviews.WorkNode, _ *viewpb.SearchOnViewRequest) (searchutil.ReduceStream, error) {
					if node.(qviews.QueryNode).ID == 11 {
						return nil, picker.ErrSubConnNotExist
					}
					return &fakeSearchStream{ctx: ctx}, nil
				},
				queryOnViewStream: func(ctx context.Context, node qviews.WorkNode, _ *viewpb.QueryOnViewRequest) (queryutil.ReduceStream, error) {
					if node.(qviews.QueryNode).ID == 11 {
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
				require.NoError(t, stream.Close())
			} else {
				streams, plan, err := client.SearchStreams(ctx, &ShardSearchRequest{VChannel: "p_1v0", Req: newTestSearchRequest(1)}, 128)
				require.NoError(t, err)
				retained = plan
				require.NoError(t, streams[0].Close())
			}
			require.Equal(t, []int64{1, 2}, selected)
			require.EqualValues(t, 2, retained.ShardID.ReplicaID)
		})
	}
}
