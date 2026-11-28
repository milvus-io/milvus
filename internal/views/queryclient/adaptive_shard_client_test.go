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
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/searchutil/optimizers"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type adaptiveTestPlanner struct {
	QueryPlanClient
	plan func(context.Context, qviews.ShardID, *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error)
}

func (p *adaptiveTestPlanner) GetQueryPlan(ctx context.Context, shard qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	return p.plan(ctx, shard, req)
}

type adaptiveTestService struct {
	ViewQueryServiceClient
	query  func(context.Context, qviews.WorkNode, *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error)
	search func(context.Context, qviews.WorkNode, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error)
}

func (s *adaptiveTestService) SearchOnView(ctx context.Context, node qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	return s.search(ctx, node, req)
}

func adaptiveTestConfig() ReplicaSelectionConfig {
	return ReplicaSelectionConfig{LatencyWeighted: true, Decay: 10 * time.Second, MinShare: .001, HedgeEnabled: true, HedgeMinDelay: time.Millisecond}
}

func adaptiveTestClient(planner QueryPlanClient, service ViewQueryServiceClient) *shardViewQueryClient {
	client := newShardViewQueryClient(2, planner, service)
	client.selection.rng = rand.New(rand.NewSource(2)) // First selection chooses replica 1.
	client.replicas.channels["channel"] = &replicaRoute{changed: make(chan struct{}), snapshot: qviews.ReplicaSnapshot{Valid: true, Replicas: []qviews.Replica{{ReplicaID: 1}, {ReplicaID: 2}}}}
	client.selectionConfig = adaptiveTestConfig
	return client
}

func adaptiveTestPlan(shard qviews.ShardID) *viewpb.GetQueryPlanResponse {
	plan := newTestSearchQueryPlan(shard, &viewpb.QueryPlanMVCC{GrowingTimetick: uint64(shard.ReplicaID * 100), TransformingTimetick: uint64(shard.ReplicaID*100 - 1)})
	plan.WorkNodes[0].SegmentSelection = &viewpb.SegmentSelection{SegmentIds: []int64{shard.ReplicaID * 1000}}
	return &viewpb.GetQueryPlanResponse{Plan: plan}
}

// Control both worker completions so an error must be processed while its
// sibling is still pending. Exercise the actual Search/Query publication paths
// with the same business-error wire encoding used by View RPCs.
func TestAdaptiveAttemptErrorWaitsForPendingSibling(t *testing.T) {
	wireError := func(err error) error {
		return viewerror.ConvertViewError("worker", viewerror.NewGRPCStatusFromError(err).Err())
	}
	oom := wireError(merr.SegcoreError(int32(merr.CodeMemAllocateFailed), "worker allocation failed"))
	input := wireError(merr.WrapErrParameterInvalidMsg("invalid user query"))
	unavailable := status.Error(codes.Unavailable, "worker disconnected")
	require.Equal(t, codes.Unknown, status.Code(oom))
	require.True(t, merr.IsRetryableErr(oom))
	require.False(t, retryableQueryError(oom), "this fix must not broaden retry classification")

	for _, operation := range []string{"search", "query"} {
		for _, tc := range []struct {
			name      string
			first     int64
			firstErr  error
			secondErr error
			wantErr   error
			immediate bool
			cancel    bool
			noHedge   bool
		}{
			{name: "hedge OOM then primary success", first: 2, firstErr: oom},
			{name: "primary OOM then hedge success", first: 1, firstErr: oom},
			{name: "hedge unavailable then primary success", first: 2, firstErr: unavailable},
			{name: "primary unavailable then hedge success", first: 1, firstErr: unavailable},
			{name: "OOM then unavailable retains OOM", first: 2, firstErr: oom, secondErr: unavailable, wantErr: oom},
			{name: "unavailable then OOM returns OOM", first: 1, firstErr: unavailable, secondErr: oom, wantErr: oom},
			{name: "both unavailable exhaust attempts", first: 2, firstErr: unavailable, secondErr: unavailable, wantErr: unavailable},
			{name: "hedge input error then primary success", first: 2, firstErr: input},
			{name: "primary input error then hedge success", first: 1, firstErr: input},
			{name: "input then unavailable retains input", first: 2, firstErr: input, secondErr: unavailable, wantErr: input},
			{name: "unavailable then input returns input", first: 1, firstErr: unavailable, secondErr: input, wantErr: input},
			{name: "OOM then input returns input", first: 1, firstErr: oom, secondErr: input, wantErr: input},
			{name: "input then OOM retains input", first: 2, firstErr: input, secondErr: oom, wantErr: input},
			{name: "both input errors stop retrying", first: 2, firstErr: input, secondErr: input, wantErr: input},
			{name: "caller cancels after input error", first: 2, firstErr: input, wantErr: context.Canceled, cancel: true},
			{name: "single attempt input exits", first: 1, firstErr: input, wantErr: input, immediate: true, noHedge: true},
			{name: "caller cancels after hedge error", first: 2, firstErr: oom, wantErr: context.Canceled, cancel: true},
			{name: "single attempt OOM exits", first: 1, firstErr: oom, wantErr: oom, immediate: true, noHedge: true},
		} {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				started := make(chan int64, 2)
				release := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
				var calls atomic.Int32
				worker := func(ctx context.Context, replica int64) error {
					calls.Add(1)
					select {
					case started <- replica:
					case <-ctx.Done():
						return ctx.Err()
					}
					select {
					case <-release[replica-1]:
					case <-ctx.Done():
						return ctx.Err()
					}
					if replica == tc.first {
						return tc.firstErr
					}
					return tc.secondErr
				}
				planner := &adaptiveTestPlanner{plan: func(_ context.Context, shard qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
					response := adaptiveTestPlan(shard)
					if operation == "query" {
						response.Plan.Request = &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: proto.Clone(req.GetLegacyRetrieveRequest()).(*internalpb.RetrieveRequest)}
					}
					return response, nil
				}}
				service := &adaptiveTestService{
					search: func(ctx context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
						if err := worker(ctx, req.GetShardId().GetReplicaId()); err != nil {
							return nil, err
						}
						return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success(), NumQueries: req.GetShardId().GetReplicaId()}}, nil
					},
					query: func(ctx context.Context, _ qviews.WorkNode, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
						if err := worker(ctx, req.GetShardId().GetReplicaId()); err != nil {
							return nil, err
						}
						return &viewpb.QueryOnViewResponse{LegacyResults: &internalpb.RetrieveResults{Status: merr.Success(), AllRetrieveCount: req.GetShardId().GetReplicaId()}}, nil
					},
				}
				client := adaptiveTestClient(planner, service)
				client.selectionConfig = func() ReplicaSelectionConfig {
					cfg := adaptiveTestConfig()
					cfg.HedgeEnabled = !tc.noHedge
					return cfg
				}
				searchCollector, queryCollector := newLegacySearchCollector(), newLegacyQueryCollector()
				type outcome struct {
					plan *ShardPlan
					err  error
				}
				done := make(chan outcome, 1)
				go func() {
					var result outcome
					if operation == "search" {
						result.plan, result.err = client.Search(ctx, &ShardSearchRequest{VChannel: "channel", Req: &internalpb.SearchRequest{CollectionID: 100}, Reducer: searchCollector})
					} else {
						result.plan, result.err = client.Query(ctx, &ShardQueryRequest{VChannel: "channel", Req: &internalpb.RetrieveRequest{CollectionID: 100}, Reducer: queryCollector})
					}
					done <- result
				}()
				attempts := 2
				if tc.noHedge {
					attempts = 1
				}
				for range attempts {
					select {
					case <-started:
					case <-ctx.Done():
						t.Fatal("attempt was not dispatched")
					}
				}
				close(release[tc.first-1])
				if !tc.immediate {
					select {
					case result := <-done:
						t.Fatalf("request ended while sibling was pending: %v", result.err)
					case <-time.After(25 * time.Millisecond):
					}
					if tc.cancel {
						cancel()
					} else {
						close(release[2-tc.first])
					}
				}
				var result outcome
				select {
				case result = <-done:
				case <-time.After(time.Second):
					t.Fatal("request did not complete")
				}
				require.EqualValues(t, attempts, calls.Load(), "must not dispatch another round after a non-retryable failure")
				if tc.wantErr != nil {
					require.ErrorIs(t, result.err, tc.wantErr)
					require.Nil(t, result.plan)
					require.Empty(t, searchCollector.results)
					require.Empty(t, queryCollector.results)
					return
				}
				require.NoError(t, result.err)
				winner := 3 - tc.first
				require.Equal(t, winner, result.plan.ShardID.ReplicaID)
				require.Equal(t, uint64(winner*100), result.plan.Mvcc.GrowingTimetick)
				if operation == "search" {
					require.Len(t, searchCollector.results, 1)
					require.Len(t, searchCollector.results[result.plan.ShardID.String()], 1)
					require.Equal(t, winner, searchCollector.results[result.plan.ShardID.String()][0].GetNumQueries())
				} else {
					require.Len(t, queryCollector.results, 1)
					require.Len(t, queryCollector.results[result.plan.ShardID.String()], 1)
					require.Equal(t, winner, queryCollector.results[result.plan.ShardID.String()][0].GetAllRetrieveCount())
				}
			})
		}
	}
}

// The baseline waits for the selected QN, even with another ready replica. An
// enabled hedge completes through the other QN without sharing partial results.
func TestAdaptiveSearchHangingWorkerAndWinnerOwnership(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "existing router", true: "hedged router"}[enabled], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			planner := &adaptiveTestPlanner{plan: func(_ context.Context, shard qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
				require.Equal(t, []byte{1, 2}, req.GetLegacySearchRequest().GetSerializedExprPlan())
				return adaptiveTestPlan(shard), nil
			}}
			service := &adaptiveTestService{search: func(ctx context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
				if req.GetShardId().GetReplicaId() == 1 {
					close(started)
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					close(finished)
				}
				return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success(), NumQueries: req.GetShardId().GetReplicaId()}}, nil
			}}
			client := adaptiveTestClient(planner, service)
			client.selectionConfig = func() ReplicaSelectionConfig {
				cfg := adaptiveTestConfig()
				cfg.LatencyWeighted = enabled
				cfg.HedgeEnabled = enabled
				return cfg
			}
			collector := newLegacySearchCollector()
			request := &internalpb.SearchRequest{CollectionID: 100, SerializedExprPlan: []byte{1, 2}}
			original := proto.Clone(request)
			type result struct {
				plan *ShardPlan
				err  error
			}
			done := make(chan result, 1)
			go func() {
				plan, err := client.Search(ctx, &ShardSearchRequest{VChannel: "channel", Req: request, Reducer: collector})
				done <- result{plan, err}
			}()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("primary was not dispatched")
			}
			if !enabled {
				select {
				case <-done:
					t.Fatal("baseline unexpectedly bypassed blocked QN")
				case <-time.After(20 * time.Millisecond):
				}
				once.Do(func() { close(release) })
			}
			var got result
			select {
			case got = <-done:
			case <-ctx.Done():
				t.Fatal("search did not complete")
			}
			require.NoError(t, got.err)
			winner := int64(1)
			if enabled {
				winner = 2
			}
			require.Equal(t, winner, got.plan.ShardID.ReplicaID)
			require.Equal(t, uint64(winner*100), got.plan.Mvcc.GrowingTimetick)
			require.Equal(t, []int64{winner * 1000}, got.plan.Selections[0].GetSegmentSelection().GetSegmentIds())
			require.True(t, proto.Equal(original, request), "attempt restoration must not mutate the original request")
			once.Do(func() { close(release) })
			select {
			case <-finished:
			case <-ctx.Done():
				t.Fatal("loser was canceled instead of allowed to finish")
			}
			require.Eventually(t, func() bool {
				client.selection.mu.Lock()
				defer client.selection.mu.Unlock()
				for _, stats := range client.selection.keys {
					for _, replica := range stats.replicas {
						if replica.inflight != 0 {
							return false
						}
					}
				}
				return true
			}, time.Second, time.Millisecond)
			collector.mu.Lock()
			defer collector.mu.Unlock()
			require.Len(t, collector.results, 1, "late loser must not publish into the winner reducer")
			require.Len(t, collector.results[got.plan.ShardID.String()], 1)
			require.Equal(t, winner, collector.results[got.plan.ShardID.String()][0].GetNumQueries())
		})
	}
}

func (s *adaptiveTestService) QueryOnView(ctx context.Context, node qviews.WorkNode, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	return s.query(ctx, node, req)
}

func TestAdaptiveQueryEmptyWinnerRetainsPlan(t *testing.T) {
	for _, empty := range []bool{true, false} {
		t.Run(map[bool]string{true: "empty", false: "rows"}[empty], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			primaryStarted, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			fixed := &viewpb.QueryPlanMVCC{GrowingTimetick: 300, TransformingTimetick: 299}
			planner := &adaptiveTestPlanner{plan: func(ctx context.Context, shard qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
				require.True(t, proto.Equal(fixed, req.GetQueryPlanMvcc()))
				if shard.ReplicaID == 2 {
					select {
					case <-primaryStarted:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				response := adaptiveTestPlan(shard)
				response.Plan.Request = &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: proto.Clone(req.GetLegacyRetrieveRequest()).(*internalpb.RetrieveRequest)}
				response.Plan.Mvcc = proto.Clone(fixed).(*viewpb.QueryPlanMVCC)
				return response, nil
			}}
			service := &adaptiveTestService{query: func(ctx context.Context, _ qviews.WorkNode, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
				if req.GetShardId().GetReplicaId() == 1 {
					close(primaryStarted)
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				count := int64(0)
				if !empty {
					count = req.GetShardId().GetReplicaId()
				}
				return &viewpb.QueryOnViewResponse{LegacyResults: &internalpb.RetrieveResults{Status: merr.Success(), AllRetrieveCount: count}}, nil
			}}
			client := adaptiveTestClient(planner, service)
			collector := newLegacyQueryCollector()
			plan, err := client.Query(ctx, &ShardQueryRequest{VChannel: "channel", Req: &internalpb.RetrieveRequest{CollectionID: 100}, MVCC: fixed, Reducer: collector})
			require.NoError(t, err)
			require.Equal(t, int64(2), plan.ShardID.ReplicaID)
			require.True(t, proto.Equal(fixed, plan.Mvcc))
			once.Do(func() { close(release) })
			require.Eventually(t, func() bool {
				client.selection.mu.Lock()
				defer client.selection.mu.Unlock()
				for _, stats := range client.selection.keys {
					for _, replica := range stats.replicas {
						if replica.inflight != 0 {
							return false
						}
					}
				}
				return true
			}, time.Second, time.Millisecond)
			collector.mu.Lock()
			defer collector.mu.Unlock()
			if empty {
				require.Empty(t, collector.results)
			} else {
				require.Len(t, collector.results, 1)
				require.Equal(t, int64(2), collector.results[plan.ShardID.String()][0].GetAllRetrieveCount())
			}
		})
	}
}

func TestAdaptiveTwoStageAndRetainedWinner(t *testing.T) {
	enableTwoStageSearch(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	filtering, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	planner := &adaptiveTestPlanner{plan: func(ctx context.Context, shard qviews.ShardID, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
		if shard.ReplicaID == 2 {
			select {
			case <-filtering:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		response := twoStageResponse(shard)
		response.Plan.WorkNodes = response.Plan.WorkNodes[:1]
		response.Plan.Mvcc.TransformingTimetick = uint64(shard.ReplicaID * 100)
		return response, nil
	}}
	service := &adaptiveTestService{search: func(ctx context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
		id := req.GetShardId().GetReplicaId()
		if req.GetExecutionOptions().GetFilterOnly() {
			if id == 1 {
				close(filtering)
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success(), SealedSegmentIDsSearched: []int64{10, 20}, FilterValidCounts: []int64{id, id}, ScannedTotalBytes: id}}, nil
		}
		require.True(t, req.GetExecutionOptions().GetEnableExprCache())
		return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success(), NumQueries: id}}, nil
	}}
	tune := mockey.Mock(optimizers.OptimizeSearchParams).To(func(_ context.Context, req *querypb.SearchRequest, _ optimizers.QueryHook, _ int, _ bool, _ func(int64) int64, _ string) (*querypb.SearchRequest, error) {
		req.Req.Topk = 3
		return req, nil
	}).Build()
	defer tune.UnPatch()
	client := adaptiveTestClient(planner, service)
	collector := newLegacySearchCollector()
	plan, err := client.Search(ctx, &ShardSearchRequest{VChannel: "channel", Req: &internalpb.SearchRequest{CollectionID: 100, Topk: 5}, Reducer: collector})
	require.NoError(t, err)
	require.Equal(t, int64(2), plan.ShardID.ReplicaID)
	once.Do(func() { close(release) })
	require.Eventually(t, func() bool {
		client.selection.mu.Lock()
		defer client.selection.mu.Unlock()
		for _, stats := range client.selection.keys {
			for _, replica := range stats.replicas {
				if replica.inflight != 0 {
					return false
				}
			}
		}
		return true
	}, time.Second, time.Millisecond)
	require.Len(t, collector.results, 1)
	require.Equal(t, int64(2), collector.results[plan.ShardID.String()][0].GetScannedTotalBytes())
	// Retained requery stays on the winner's exact view even with adaptive routing
	// enabled. A retryable view error must not trigger a fresh plan or hedge.
	var queryFailure error
	calls := 0
	service.query = func(_ context.Context, node qviews.WorkNode, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
		calls++
		require.Equal(t, plan.WorkNodes[0], node)
		require.True(t, proto.Equal(plan.Version, req.GetVersion()))
		require.True(t, proto.Equal(plan.Mvcc, req.GetMvcc()))
		require.True(t, proto.Equal(plan.Selections[0].GetSegmentSelection(), req.GetSegmentSelection()))
		if queryFailure != nil {
			return nil, queryFailure
		}
		return &viewpb.QueryOnViewResponse{LegacyResults: &internalpb.RetrieveResults{Status: merr.Success()}}, nil
	}
	planner.plan = func(context.Context, qviews.ShardID, *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
		t.Error("retained winner was replanned")
		return nil, merr.WrapErrServiceInternalMsg("unexpected replanning")
	}
	legacy := &legacyClient{shardClient: client}
	req := &LegacyQueryRequest{Req: &internalpb.RetrieveRequest{CollectionID: 100}, Plans: []ShardPlan{*plan}}
	_, err = legacy.Query(ctx, req)
	require.NoError(t, err)
	queryFailure = viewerror.NewViewInvalidated("winner view retired")
	_, err = legacy.Query(ctx, req)
	require.ErrorIs(t, err, merr.ErrQueryViewNotReady)
	require.Equal(t, 2, calls)
}

func TestAdaptiveExecutionAttributesOnlyQueryNodeErrors(t *testing.T) {
	for _, origin := range []string{"planning", "streaming node", "query node"} {
		t.Run(origin, func(t *testing.T) {
			planner := &adaptiveTestPlanner{plan: func(_ context.Context, shard qviews.ShardID, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
				if origin == "planning" {
					return nil, viewerror.NewViewInvalidated("plan unavailable")
				}
				response := adaptiveTestPlan(shard)
				if origin == "streaming node" {
					response.Plan.WorkNodes = []*viewpb.QueryPlanWorkNode{{Node: &viewpb.QueryPlanWorkNode_StreamingNode{StreamingNode: &viewpb.StreamingWorkNode{Pchannel: "p"}}}}
				}
				return response, nil
			}}
			service := &adaptiveTestService{search: func(context.Context, qviews.WorkNode, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
				return nil, viewerror.NewViewInvalidated("worker view unavailable")
			}}
			client := adaptiveTestClient(planner, service)
			client.selectionConfig = func() ReplicaSelectionConfig { cfg := adaptiveTestConfig(); cfg.HedgeEnabled = false; return cfg }
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := client.Search(ctx, &ShardSearchRequest{VChannel: "channel", Req: &internalpb.SearchRequest{CollectionID: 100}, Reducer: newLegacySearchCollector()})
			require.Error(t, err)
			require.NotErrorIs(t, err, context.DeadlineExceeded)
			key := replicaPolicyKey{channel: "channel", class: "search/Strong"}
			require.Contains(t, client.selection.keys, key)
			for _, stats := range client.selection.keys[key].replicas {
				expected := 0
				if origin == "query node" {
					expected = 1
				}
				require.Equal(t, expected, stats.failures)
				require.Zero(t, stats.inflight)
			}
		})
	}
}

type rejectingSearchReducer struct {
	fakeSearchResultReducer
	added int
	reset int
}

func (r *rejectingSearchReducer) Add(qviews.ShardID, *viewpb.SearchOnViewResponse) error {
	r.added++
	if r.added == 2 {
		return merr.WrapErrServiceInternalMsg("reducer rejected complete shard")
	}
	return nil
}
func (r *rejectingSearchReducer) ResetShard(qviews.ShardID) { r.added = 0; r.reset++ }

func TestAdaptivePublishFailureResetsShard(t *testing.T) {
	planner := &adaptiveTestPlanner{plan: func(_ context.Context, shard qviews.ShardID, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
		response := adaptiveTestPlan(shard)
		response.Plan.WorkNodes = append(response.Plan.WorkNodes, &viewpb.QueryPlanWorkNode{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: 12}}})
		return response, nil
	}}
	service := &adaptiveTestService{search: func(context.Context, qviews.WorkNode, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
		return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success()}}, nil
	}}
	client := adaptiveTestClient(planner, service)
	client.selectionConfig = func() ReplicaSelectionConfig { cfg := adaptiveTestConfig(); cfg.HedgeEnabled = false; return cfg }
	reducer := &rejectingSearchReducer{}
	plan, err := client.Search(t.Context(), &ShardSearchRequest{VChannel: "channel", Req: &internalpb.SearchRequest{CollectionID: 100}, Reducer: reducer})
	require.Error(t, err)
	require.Nil(t, plan)
	require.Zero(t, reducer.added)
	require.Equal(t, 1, reducer.reset)
}

func TestAdaptiveConfigIsCapturedForOutstandingRequest(t *testing.T) {
	cfg := adaptiveTestConfig()
	var enabled atomic.Bool
	enabled.Store(true)
	primaryStarted, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	planner := &adaptiveTestPlanner{plan: func(ctx context.Context, shard qviews.ShardID, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
		if shard.ReplicaID == 2 {
			select {
			case <-primaryStarted:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return adaptiveTestPlan(shard), nil
	}}
	service := &adaptiveTestService{search: func(ctx context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
		if req.GetShardId().GetReplicaId() == 1 {
			enabled.Store(false)
			close(primaryStarted)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success()}}, nil
	}}
	client := adaptiveTestClient(planner, service)
	client.selectionConfig = func() ReplicaSelectionConfig {
		result := cfg
		result.LatencyWeighted = enabled.Load()
		result.HedgeEnabled = enabled.Load()
		return result
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	plan, err := client.Search(ctx, &ShardSearchRequest{VChannel: "channel", Req: &internalpb.SearchRequest{CollectionID: 100}, Reducer: newLegacySearchCollector()})
	require.NoError(t, err)
	require.Equal(t, int64(2), plan.ShardID.ReplicaID)
	require.False(t, client.replicaSelectionConfig().LatencyWeighted)
	once.Do(func() { close(release) })
	require.Eventually(t, func() bool {
		client.selection.mu.Lock()
		defer client.selection.mu.Unlock()
		for _, stats := range client.selection.keys {
			for _, replica := range stats.replicas {
				if replica.inflight != 0 {
					return false
				}
			}
		}
		return true
	}, time.Second, time.Millisecond)
}

func TestAdaptiveWinnerExcludesPrimaryPartialResults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	partial, blocked, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	planner := &adaptiveTestPlanner{plan: func(ctx context.Context, shard qviews.ShardID, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
		if shard.ReplicaID == 2 {
			for _, ready := range []chan struct{}{partial, blocked} {
				select {
				case <-ready:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
		}
		response := adaptiveTestPlan(shard)
		response.Plan.WorkNodes = append(response.Plan.WorkNodes, &viewpb.QueryPlanWorkNode{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: 12}}})
		return response, nil
	}}
	service := &adaptiveTestService{search: func(ctx context.Context, node qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
		if req.GetShardId().GetReplicaId() == 1 {
			if node.(qviews.QueryNode).ID == 11 {
				close(partial)
			} else {
				close(blocked)
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				return nil, viewerror.NewViewInvalidated("late primary worker error")
			}
		}
		return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success(), NumQueries: req.GetShardId().GetReplicaId()}}, nil
	}}
	client := adaptiveTestClient(planner, service)
	collector := newLegacySearchCollector()
	plan, err := client.Search(ctx, &ShardSearchRequest{VChannel: "channel", Req: &internalpb.SearchRequest{CollectionID: 100}, Reducer: collector})
	require.NoError(t, err)
	require.Equal(t, int64(2), plan.ShardID.ReplicaID)
	require.Len(t, plan.WorkNodes, 2)
	require.Equal(t, uint64(200), plan.Mvcc.GrowingTimetick)
	once.Do(func() { close(release) })
	require.Eventually(t, func() bool {
		client.selection.mu.Lock()
		defer client.selection.mu.Unlock()
		for _, stats := range client.selection.keys {
			for _, replica := range stats.replicas {
				if replica.inflight != 0 {
					return false
				}
			}
		}
		return true
	}, time.Second, time.Millisecond)
	require.Len(t, collector.results, 1)
	results := collector.results[plan.ShardID.String()]
	require.Len(t, results, 2)
	for _, result := range results {
		require.Equal(t, int64(2), result.NumQueries)
	}
	client.selection.mu.Lock()
	defer client.selection.mu.Unlock()
	require.Equal(t, 1, client.selection.keys[replicaPolicyKey{channel: "channel", class: "search/Strong"}].replicas[1].failures, "overtake plus late worker error is one negative observation")
}

func TestAdaptiveSharedStallDoesNotPenalizeReplica(t *testing.T) {
	for _, origin := range []string{"planning", "streaming node"} {
		t.Run(origin, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			blocked, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			planner := &adaptiveTestPlanner{plan: func(ctx context.Context, shard qviews.ShardID, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
				if shard.ReplicaID == 2 {
					select {
					case <-blocked:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				if shard.ReplicaID == 1 && origin == "planning" {
					close(blocked)
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				response := adaptiveTestPlan(shard)
				if shard.ReplicaID == 1 && origin == "streaming node" {
					response.Plan.WorkNodes = []*viewpb.QueryPlanWorkNode{{Node: &viewpb.QueryPlanWorkNode_StreamingNode{StreamingNode: &viewpb.StreamingWorkNode{Pchannel: "p"}}}}
				}
				return response, nil
			}}
			service := &adaptiveTestService{search: func(ctx context.Context, node qviews.WorkNode, _ *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
				if node.NodeType() == qviews.NodeTypeStreamingNode {
					close(blocked)
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success()}}, nil
			}}
			client := adaptiveTestClient(planner, service)
			plan, err := client.Search(ctx, &ShardSearchRequest{VChannel: "channel", Req: &internalpb.SearchRequest{CollectionID: 100}, Reducer: newLegacySearchCollector()})
			require.NoError(t, err)
			require.Equal(t, int64(2), plan.ShardID.ReplicaID)
			key := replicaPolicyKey{channel: "channel", class: "search/Strong"}
			client.selection.mu.Lock()
			failures := client.selection.keys[key].replicas[1].failures
			client.selection.mu.Unlock()
			require.Zero(t, failures)
			once.Do(func() { close(release) })
			require.Eventually(t, func() bool {
				client.selection.mu.Lock()
				defer client.selection.mu.Unlock()
				return client.selection.keys[key].replicas[1].inflight == 0
			}, time.Second, time.Millisecond)
		})
	}
}

func TestHedgingDecoupledFromReplicaSelectionPolicy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	release := make(chan struct{})
	defer close(release)

	planner := &adaptiveTestPlanner{plan: func(_ context.Context, shard qviews.ShardID, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
		return adaptiveTestPlan(shard), nil
	}}
	service := &adaptiveTestService{search: func(ctx context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
		if req.GetShardId().GetReplicaId() == 1 {
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success()}}, nil
	}}
	client := adaptiveTestClient(planner, service)
	client.replicas.channels["channel"] = &replicaRoute{
		changed:  make(chan struct{}),
		snapshot: qviews.ReplicaSnapshot{Valid: true, Replicas: []qviews.Replica{{ReplicaID: 1}, {ReplicaID: 2}, {ReplicaID: 3}}},
	}
	client.selectionConfig = func() ReplicaSelectionConfig {
		cfg := adaptiveTestConfig()
		cfg.LatencyWeighted = false
		cfg.HedgeEnabled = true
		return cfg
	}

	// First request starts at round-robin index 0 -> replica 1 (stalls), hedges to round-robin index 1 -> replica 2.
	plan, err := client.Search(ctx, &ShardSearchRequest{VChannel: "channel", Req: &internalpb.SearchRequest{CollectionID: 100}, Reducer: newLegacySearchCollector()})
	require.NoError(t, err)
	require.Equal(t, int64(2), plan.ShardID.ReplicaID)

	// Next request advances round-robin to replica 3 without latency_weighted stats tracking.
	plan2, err := client.Search(ctx, &ShardSearchRequest{VChannel: "channel", Req: &internalpb.SearchRequest{CollectionID: 100}, Reducer: newLegacySearchCollector()})
	require.NoError(t, err)
	require.Equal(t, int64(3), plan2.ShardID.ReplicaID)

	key := replicaPolicyKey{channel: "channel", class: "search/Strong"}
	client.selection.mu.Lock()
	defer client.selection.mu.Unlock()
	require.Empty(t, client.selection.keys[key].replicas, "non-latency_weighted policies must not maintain per-replica EWMA entries")
}

func TestAtMostOneHedgeAcrossRetries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var totalCalls atomic.Int64
	hedgeStarted := make(chan struct{})
	planner := &adaptiveTestPlanner{plan: func(_ context.Context, shard qviews.ShardID, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
		return adaptiveTestPlan(shard), nil
	}}
	service := &adaptiveTestService{search: func(ctx context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
		callNum := totalCalls.Add(1)
		switch callNum {
		case 1:
			// Round 1 primary stalls until hedge starts, then fails with retryable Unavailable.
			select {
			case <-hedgeStarted:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return nil, status.Error(codes.Unavailable, "primary transient failure")
		case 2:
			// Round 1 hedge also fails with retryable Unavailable.
			close(hedgeStarted)
			return nil, status.Error(codes.Unavailable, "hedge transient failure")
		default:
			// Round 2 primary stalls well past HedgeMinDelay (1ms), but no second hedge may be launched.
			time.Sleep(15 * time.Millisecond)
			return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success(), NumQueries: req.GetShardId().GetReplicaId()}}, nil
		}
	}}

	client := adaptiveTestClient(planner, service)
	client.maxRetries = 5
	// Provide 4 replicas so Round 2 still has an untried replica (4) available;
	// only hedgedOnce prevents dispatching a second hedge on Round 2.
	client.replicas.channels["channel"] = &replicaRoute{
		changed:  make(chan struct{}),
		snapshot: qviews.ReplicaSnapshot{Valid: true, Replicas: []qviews.Replica{{ReplicaID: 1}, {ReplicaID: 2}, {ReplicaID: 3}, {ReplicaID: 4}}},
	}

	nodeID := paramtable.GetStringNodeID()
	sentCounter := metrics.ProxyReplicaHedgeCount.WithLabelValues(nodeID, "sent")
	noReplicaCounter := metrics.ProxyReplicaHedgeCount.WithLabelValues(nodeID, "skipped_no_replica")
	baseSent := testutil.ToFloat64(sentCounter)
	baseNoReplica := testutil.ToFloat64(noReplicaCounter)

	plan, err := client.Search(ctx, &ShardSearchRequest{VChannel: "channel", Req: &internalpb.SearchRequest{CollectionID: 100}, Reducer: newLegacySearchCollector()})
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Equal(t, int64(3), totalCalls.Load(), "only 1 hedge may be dispatched across all retry rounds")
	require.Equal(t, 1.0, testutil.ToFloat64(sentCounter)-baseSent)
	require.Zero(t, testutil.ToFloat64(noReplicaCounter)-baseNoReplica)
}

func overloadViewStatus() *commonpb.Status {
	return merr.Status(merr.WrapErrTooManyRequests(200, "read task queue"))
}

func overloadTestClient(t *testing.T, latencyWeighted, hedgeEnabled bool, replicas []int64, searchFn func(context.Context, qviews.WorkNode, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error)) *shardViewQueryClient {
	t.Helper()
	planner := &adaptiveTestPlanner{plan: func(_ context.Context, shard qviews.ShardID, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
		return adaptiveTestPlan(shard), nil
	}}
	service := &adaptiveTestService{search: searchFn}
	client := adaptiveTestClient(planner, service)
	r := make([]qviews.Replica, 0, len(replicas))
	for _, id := range replicas {
		r = append(r, qviews.Replica{ReplicaID: id})
	}
	client.replicas.channels["channel"] = &replicaRoute{
		changed:  make(chan struct{}),
		snapshot: qviews.ReplicaSnapshot{Valid: true, Replicas: r},
	}
	client.selectionConfig = func() ReplicaSelectionConfig {
		cfg := adaptiveTestConfig()
		cfg.LatencyWeighted = latencyWeighted
		cfg.HedgeEnabled = hedgeEnabled
		cfg.HedgeMinDelay = 20 * time.Millisecond
		return cfg
	}
	return client
}

func TestAdaptiveOverloadGetsOneExtraAttempt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var mu sync.Mutex
	var nodes []int64
	client := overloadTestClient(t, true, true, []int64{1, 2, 3}, func(_ context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
		mu.Lock()
		nodes = append(nodes, req.GetShardId().GetReplicaId())
		first := len(nodes) == 1
		mu.Unlock()
		if first {
			return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: overloadViewStatus()}}, nil
		}
		return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success()}}, nil
	})
	plan, err := client.Search(ctx, &ShardSearchRequest{VChannel: "channel", Req: &internalpb.SearchRequest{CollectionID: 100}, Reducer: newLegacySearchCollector()})
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Len(t, nodes, 2)
	require.NotEqual(t, nodes[0], nodes[1])

	key := replicaPolicyKey{channel: "channel", class: "search/Strong"}
	client.selection.mu.Lock()
	defer client.selection.mu.Unlock()
	require.Zero(t, client.selection.keys[key].replicas[nodes[0]].failures)
}

func TestAdaptiveOverloadTwiceEndsRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var mu sync.Mutex
	var nodes []int64
	client := overloadTestClient(t, true, true, []int64{1, 2, 3}, func(_ context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
		mu.Lock()
		nodes = append(nodes, req.GetShardId().GetReplicaId())
		mu.Unlock()
		return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: overloadViewStatus()}}, nil
	})
	_, err := client.Search(ctx, &ShardSearchRequest{VChannel: "channel", Req: &internalpb.SearchRequest{CollectionID: 100}, Reducer: newLegacySearchCollector()})
	require.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	require.Len(t, nodes, 2)
	require.NotEqual(t, nodes[0], nodes[1])
}

func TestAdaptiveOverloadOnSingleReplicaEndsRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var calls atomic.Int32
	client := overloadTestClient(t, true, true, []int64{1}, func(_ context.Context, _ qviews.WorkNode, _ *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
		calls.Add(1)
		return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: overloadViewStatus()}}, nil
	})
	_, err := client.Search(ctx, &ShardSearchRequest{VChannel: "channel", Req: &internalpb.SearchRequest{CollectionID: 100}, Reducer: newLegacySearchCollector()})
	require.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	require.Equal(t, int32(1), calls.Load())
}

func TestAdaptiveOverloadAfterHedgeEndsRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var calls atomic.Int32
	client := overloadTestClient(t, true, true, []int64{1, 2, 3}, func(_ context.Context, _ qviews.WorkNode, _ *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
		c := calls.Add(1)
		if c == 1 {
			time.Sleep(60 * time.Millisecond)
		} else {
			time.Sleep(80 * time.Millisecond)
		}
		return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: overloadViewStatus()}}, nil
	})
	_, err := client.Search(ctx, &ShardSearchRequest{VChannel: "channel", Req: &internalpb.SearchRequest{CollectionID: 100}, Reducer: newLegacySearchCollector()})
	require.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	require.Equal(t, int32(2), calls.Load())
}

func TestAdaptiveOverloadedHedgeLeavesPrimary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var calls atomic.Int32
	client := overloadTestClient(t, true, true, []int64{1, 2}, func(_ context.Context, _ qviews.WorkNode, _ *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
		if calls.Add(1) == 1 {
			time.Sleep(60 * time.Millisecond)
			return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success()}}, nil
		}
		return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: overloadViewStatus()}}, nil
	})
	plan, err := client.Search(ctx, &ShardSearchRequest{VChannel: "channel", Req: &internalpb.SearchRequest{CollectionID: 100}, Reducer: newLegacySearchCollector()})
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Equal(t, int32(2), calls.Load())
}

func TestAdaptiveFailedReplicaDoesNotUseExtraAttempt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var calls atomic.Int32
	client := overloadTestClient(t, true, false, []int64{1, 2, 3}, func(_ context.Context, _ qviews.WorkNode, _ *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
		switch calls.Add(1) {
		case 1:
			return nil, status.Error(codes.Unavailable, "node down")
		case 2:
			return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: overloadViewStatus()}}, nil
		default:
			return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success()}}, nil
		}
	})
	plan, err := client.Search(ctx, &ShardSearchRequest{VChannel: "channel", Req: &internalpb.SearchRequest{CollectionID: 100}, Reducer: newLegacySearchCollector()})
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Equal(t, int32(3), calls.Load())
}

func TestAdaptiveOverloadUnderDefaultPolicy(t *testing.T) {
	run := func(refusals int) ([]int64, error) {
		// less than replicaRetryInterval (100ms) so overload failover must happen immediately
		ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
		defer cancel()
		var mu sync.Mutex
		var nodes []int64
		client := overloadTestClient(t, false, false, []int64{1, 2, 3}, func(_ context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
			mu.Lock()
			nodes = append(nodes, req.GetShardId().GetReplicaId())
			n := len(nodes)
			mu.Unlock()
			if n <= refusals {
				return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: overloadViewStatus()}}, nil
			}
			return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success()}}, nil
		})
		_, err := client.Search(ctx, &ShardSearchRequest{VChannel: "channel", Req: &internalpb.SearchRequest{CollectionID: 100}, Reducer: newLegacySearchCollector()})
		return nodes, err
	}

	nodes, err := run(1)
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	require.NotEqual(t, nodes[0], nodes[1])

	nodes, err = run(3)
	require.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	require.Len(t, nodes, 2)
}

func TestOverloadAlternativeRemovedReturnsOriginalError(t *testing.T) {
	for _, weighted := range []bool{false, true} {
		t.Run(fmt.Sprint(weighted), func(t *testing.T) {
			var client *shardViewQueryClient
			client = overloadTestClient(t, weighted, false, []int64{1, 2}, func(_ context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
				route := client.replicas.channels["channel"]
				route.mu.Lock()
				route.snapshot.Replicas = []qviews.Replica{{ReplicaID: req.GetShardId().GetReplicaId()}}
				route.mu.Unlock()
				return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: overloadViewStatus()}}, nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := client.Search(ctx, &ShardSearchRequest{VChannel: "channel", Req: &internalpb.SearchRequest{}, Reducer: newLegacySearchCollector()})
			require.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
			require.NoError(t, ctx.Err())
		})
	}
}

// The default streaming setting must not silently bypass adaptive routing.
func TestLegacyAdaptiveRoutingUsesBatchWithStreamingAllowed(t *testing.T) {
	for _, operation := range []string{"search", "query"} {
		for _, weighted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/weighted=%v", operation, weighted), func(t *testing.T) {
				planner := &adaptiveTestPlanner{plan: func(_ context.Context, shard qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
					response := adaptiveTestPlan(shard)
					if operation == "query" {
						response.Plan.Request = &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: req.GetLegacyRetrieveRequest()}
					}
					return response, nil
				}}
				service := &adaptiveTestService{
					search: func(context.Context, qviews.WorkNode, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
						return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success(), NumQueries: 1}}, nil
					},
					query: func(context.Context, qviews.WorkNode, *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
						return &viewpb.QueryOnViewResponse{LegacyResults: &internalpb.RetrieveResults{Status: merr.Success()}}, nil
					},
				}
				client := newLegacyClient(ViewQueryClientConfig{ReplicaSelection: func() ReplicaSelectionConfig {
					cfg := adaptiveTestConfig()
					cfg.LatencyWeighted, cfg.HedgeEnabled = weighted, !weighted
					return cfg
				}}, planner, service, &legacyResolver{vchannels: []string{"channel"}})
				client.shardClient.replicas.channels["channel"] = &replicaRoute{changed: make(chan struct{}), snapshot: qviews.ReplicaSnapshot{Valid: true, Replicas: []qviews.Replica{{ReplicaID: 1}}}}
				if operation == "search" {
					result, err := client.Search(t.Context(), &LegacySearchRequest{AllowStreaming: true, Req: &internalpb.SearchRequest{CollectionID: 100, Nq: 1, Topk: 1}})
					require.NoError(t, err)
					require.Nil(t, result.Stream)
					require.Len(t, result.Results, 1)
				} else {
					result, err := client.Query(t.Context(), &LegacyQueryRequest{AllowStreaming: true, Req: &internalpb.RetrieveRequest{CollectionID: 100}})
					require.NoError(t, err)
					require.Nil(t, result.Stream)
					require.Len(t, result.Results, 1)
				}
				client.shardClient.selection.mu.Lock()
				defer client.shardClient.selection.mu.Unlock()
				require.NotEmpty(t, client.shardClient.selection.keys, "adaptive feedback must be recorded")
			})
		}
	}
}

func TestOverloadExtraAttemptBudgetSharedAcrossOpenCalls(t *testing.T) {
	planner := &adaptiveTestPlanner{plan: func(_ context.Context, shard qviews.ShardID, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
		return adaptiveTestPlan(shard), nil
	}}
	client := adaptiveTestClient(planner, nil)
	var dispatched []int64
	params := &shardExecParams{
		retryState: &shardRetryState{},
		buildPlanReq: func(qviews.ShardID) *viewpb.GetQueryPlanRequest {
			return &viewpb.GetQueryPlanRequest{Request: &viewpb.GetQueryPlanRequest_LegacySearchRequest{LegacySearchRequest: &internalpb.SearchRequest{}}}
		},
		dispatchNode: func(_ context.Context, _ qviews.WorkNode, _ *viewpb.QueryPlan, shard qviews.ShardID) error {
			dispatched = append(dispatched, shard.ReplicaID)
			if shard.ReplicaID != 2 {
				return merr.ErrServiceTooManyRequests
			}
			return nil
		},
		resetShard: func(qviews.ShardID) {},
	}
	plan, err := client.executeShard(t.Context(), "channel", params)
	require.NoError(t, err)
	require.Equal(t, int64(2), plan.ShardID.ReplicaID)
	require.Equal(t, []int64{1, 2}, dispatched)

	// This tests shared opening state only; streaming receive failures are
	// covered by TestStreamingReceiveOverloadFailover.
	route := client.replicas.channels["channel"]
	route.mu.Lock()
	route.snapshot.Replicas = []qviews.Replica{{ReplicaID: 3}, {ReplicaID: 4}}
	route.mu.Unlock()
	_, err = client.executeShard(t.Context(), "channel", params)
	require.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	require.Len(t, dispatched, 3, "a stream restart must not grant another overload failover")
}
