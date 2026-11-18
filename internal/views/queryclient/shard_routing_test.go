package queryclient

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestShardRoutingUsesServerSelectedReplicaAndSkipsKnownDeadNode(t *testing.T) {
	plans, calls := 0, 0
	planPatch := mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).To(func(_ *fakeQueryPlanClient, _ context.Context, vchannel string, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
		plans++
		require.Equal(t, vchannel, req.Vchannel)
		require.EqualValues(t, 123, req.GetBarrierMvcc())
		id := qviews.ShardID{VChannel: vchannel, ReplicaID: int64(plans)}
		plan := newTestSearchQueryPlan(id, &viewpb.QueryPlanMVCC{GrowingTimetick: 123, TransformingTimetick: 123})
		if plans == 3 {
			plan.WorkNodes[0].GetQueryNode().NodeId = 12
		}
		return &viewpb.GetQueryPlanResponse{Plan: plan}, nil
	}).Build()
	defer planPatch.UnPatch()
	rpcPatch := mockey.Mock((*fakeViewQueryServiceClient).SearchOnView).To(func(_ *fakeViewQueryServiceClient, _ context.Context, node qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
		calls++
		if node.(qviews.QueryNode).ID == 11 {
			return nil, picker.ErrSubConnNotExist
		}
		require.EqualValues(t, 3, req.GetShardId().GetReplicaId())
		require.EqualValues(t, 10, req.GetVersion().GetQueryVersion())
		return &viewpb.SearchOnViewResponse{}, nil
	}).Build()
	defer rpcPatch.UnPatch()
	client := newShardViewQueryClient(3, &fakeQueryPlanClient{}, &fakeViewQueryServiceClient{})
	defer client.Close()
	plan, err := client.Search(t.Context(), &ShardSearchRequest{VChannel: "p_1v0", Req: &internalpb.SearchRequest{}, BarrierMVCC: 123, Reducer: fakeSearchResultReducer{}})
	require.NoError(t, err)
	require.EqualValues(t, 3, plan.ShardID.ReplicaID)
	require.Equal(t, 3, plans)
	require.Equal(t, 2, calls)
}

func TestShardRoutingReconsidersNodeAfterSessionUpdate(t *testing.T) {
	id := qviews.ShardID{ReplicaID: 22, VChannel: "p_1v0"}
	plans := &fakeQueryPlanClient{plan: newTestSearchQueryPlan(id, &viewpb.QueryPlanMVCC{})}
	changed := make(chan struct{})
	calls := 0
	patch := mockey.Mock((*fakeViewQueryServiceClient).SearchOnView).To(func(_ *fakeViewQueryServiceClient, _ context.Context, _ qviews.WorkNode, _ *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
		calls++
		if calls == 1 {
			close(changed)
			return nil, &viewerror.NodeSessionUnavailable{Cause: viewerror.ConvertViewError("SearchOnView", picker.ErrSubConnNotExist), Changed: changed}
		}
		return &viewpb.SearchOnViewResponse{}, nil
	}).Build()
	defer patch.UnPatch()
	client := newShardViewQueryClient(2, plans, &fakeViewQueryServiceClient{})
	defer client.Close()
	_, err := client.Search(t.Context(), &ShardSearchRequest{VChannel: id.VChannel, Req: &internalpb.SearchRequest{}, Reducer: fakeSearchResultReducer{}})
	require.NoError(t, err)
	require.Equal(t, 2, calls)
}

func TestShardRoutingPreservesFailureAtRetryLimit(t *testing.T) {
	failure := status.Error(codes.Unavailable, "owner unreachable")
	calls := 0
	patch := mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).To(func(*fakeQueryPlanClient, context.Context, string, *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
		calls++
		return nil, failure
	}).Build()
	defer patch.UnPatch()
	client := newShardViewQueryClient(2, &fakeQueryPlanClient{}, nil)
	defer client.Close()
	_, err := client.Search(t.Context(), &ShardSearchRequest{VChannel: "p_1v0", Req: &internalpb.SearchRequest{}, Reducer: fakeSearchResultReducer{}})
	require.ErrorIs(t, err, failure)
	require.Equal(t, 2, calls)
}

func TestShardRoutingWaitsForLoadedCollectionAndRechecksRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		patch := mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).Return(nil, viewerror.NewViewNotFound("no view")).Build()
		defer patch.UnPatch()
		checks := 0
		loadChecks := patchLoadCheck(t, func(context.Context, *querypb.GetQueryViewLoadInfoRequest) (*querypb.GetQueryViewLoadInfoResponse, error) {
			checks++
			if checks == 1 {
				return &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success()}, nil
			}
			return nil, merr.WrapErrCollectionNotLoaded(100)
		})
		client := newShardViewQueryClient(1, &fakeQueryPlanClient{}, nil)
		defer client.Close()
		client.loadChecks = loadChecks
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		_, err := client.Search(ctx, &ShardSearchRequest{VChannel: "p_100v0", Req: &internalpb.SearchRequest{CollectionID: 100}, Reducer: fakeSearchResultReducer{}})
		require.ErrorIs(t, err, merr.ErrCollectionNotLoaded)
		require.Equal(t, 2, checks)
		require.Empty(t, loadChecks.collections)
	})
}

func TestShardRoutingCloseCancelsOutstandingPlanning(t *testing.T) {
	entered := make(chan struct{})
	patch := mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).To(func(_ *fakeQueryPlanClient, ctx context.Context, _ string, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}).Build()
	defer patch.UnPatch()
	client := newShardViewQueryClient(1, &fakeQueryPlanClient{}, nil)
	done := make(chan error, 1)
	go func() {
		_, err := client.Search(t.Context(), &ShardSearchRequest{VChannel: "p_1v0", Req: &internalpb.SearchRequest{}, Reducer: fakeSearchResultReducer{}})
		done <- err
	}()
	<-entered
	client.Close()
	require.ErrorIs(t, <-done, context.Canceled)
	client.Close()
	_, err := client.Search(t.Context(), &ShardSearchRequest{VChannel: "p_1v0", Req: &internalpb.SearchRequest{}, Reducer: fakeSearchResultReducer{}})
	require.ErrorIs(t, err, context.Canceled, "a closed client must not begin another planning RPC")
}

func TestPlanningFailuresDoNotCreateExclusions(t *testing.T) {
	for _, failure := range []error{
		viewerror.NewViewNotFound("no serving view"),
		viewerror.NewViewInvalidated("planning interrupted by teardown"),
		status.Error(codes.Unavailable, "WAL owner unavailable"),
	} {
		t.Run(failure.Error(), func(t *testing.T) {
			patch := mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).Return(nil, failure).Build()
			defer patch.UnPatch()
			client := newShardViewQueryClient(1, &fakeQueryPlanClient{}, nil)
			defer client.Close()
			ctx, permit := beginBreakerTest(t, client.breaker, "p_1v0")
			plan, err := client.executeRoutingAttempt(ctx, "p_1v0", &shardExecParams{
				buildPlanReq: func(channel string) *viewpb.GetQueryPlanRequest {
					return &viewpb.GetQueryPlanRequest{Vchannel: channel}
				},
			}, permit)
			permit.release()
			require.ErrorIs(t, err, failure)
			require.Nil(t, plan)
			require.Empty(t, client.breaker.nodes)
			require.Empty(t, client.breaker.views)
			require.Empty(t, permit.failedNodes)
			require.Empty(t, permit.failedViews)
		})
	}
}

func TestShardRoutingSharesActualViewExclusionAcrossRequests(t *testing.T) {
	id := qviews.ShardID{ReplicaID: 22, VChannel: "p_1v0"}
	plans := 0
	patchPlan := mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).To(func(_ *fakeQueryPlanClient, _ context.Context, channel string, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
		plans++
		require.Equal(t, id.VChannel, channel)
		plan := newTestSearchQueryPlan(id, &viewpb.QueryPlanMVCC{})
		if plans > 1 {
			require.Len(t, req.GetRoutingConstraints().GetExcludedQueryViews(), 1)
			require.EqualValues(t, 22, req.RoutingConstraints.ExcludedQueryViews[0].ReplicaId)
			require.EqualValues(t, 10, req.RoutingConstraints.ExcludedQueryViews[0].Version.QueryVersion)
			plan.Version.QueryVersion = 11
		}
		return &viewpb.GetQueryPlanResponse{Plan: plan}, nil
	}).Build()
	defer patchPlan.UnPatch()
	patchRPC := mockey.Mock((*fakeViewQueryServiceClient).SearchOnView).To(func(_ *fakeViewQueryServiceClient, _ context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
		if req.Version.QueryVersion == 10 {
			return nil, viewerror.NewViewInvalidated("retired")
		}
		return &viewpb.SearchOnViewResponse{}, nil
	}).Build()
	defer patchRPC.UnPatch()
	client := newShardViewQueryClient(2, &fakeQueryPlanClient{}, &fakeViewQueryServiceClient{})
	defer client.Close()
	for range 2 {
		plan, err := client.Search(t.Context(), &ShardSearchRequest{VChannel: id.VChannel, Req: &internalpb.SearchRequest{}, Reducer: fakeSearchResultReducer{}})
		require.NoError(t, err)
		require.EqualValues(t, 11, plan.Version.QueryVersion)
	}
	require.Equal(t, 3, plans)
}

func TestShardRoutingAllExcludedHasSeparateBoundedWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		patch := mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).To(func(*fakeQueryPlanClient, context.Context, string, *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
			calls++
			return nil, viewerror.ConvertViewError("GetQueryPlan", viewerror.NewGRPCStatusFromError(viewerror.NewAllCandidatesExcluded()).Err())
		}).Build()
		defer patch.UnPatch()
		client := newShardViewQueryClient(1, &fakeQueryPlanClient{}, nil)
		defer client.Close()
		client.breaker = NewQueryViewCircuitBreaker(QueryViewCircuitBreakerConfig{MaxRoutingWait: 2 * time.Second})
		started := time.Now()
		_, err := client.Search(t.Context(), &ShardSearchRequest{VChannel: "p_1v0", Req: &internalpb.SearchRequest{}, Reducer: fakeSearchResultReducer{}})
		require.True(t, viewerror.AsViewError(err).IsAllCandidatesExcluded())
		require.Equal(t, 2*time.Second, time.Since(started))
		require.LessOrEqual(t, calls, 3, "all-excluded must neither busy-loop nor call load-info")
	})
}

func TestRetainedRequeryRecordsViewFailureWithoutReplanning(t *testing.T) {
	id := qviews.ShardID{ReplicaID: 22, VChannel: "p_1v0"}
	patch := mockey.Mock((*fakeViewQueryServiceClient).QueryOnView).Return(nil, viewerror.NewViewNotFound("retained version removed")).Build()
	defer patch.UnPatch()
	client := newLegacyClient(ViewQueryClientConfig{}, nil, &fakeViewQueryServiceClient{}, nil)
	defer client.shardClient.Close()
	_, err := client.queryRetainedPlans(t.Context(), &LegacyQueryRequest{Req: &internalpb.RetrieveRequest{}, Plans: []ShardPlan{{ShardID: id, Version: &viewpb.QueryViewVersion{QueryVersion: 10}, Mvcc: &viewpb.QueryPlanMVCC{}, WorkNodes: []qviews.WorkNode{qviews.NewQueryNode(11)}}}})
	require.True(t, viewerror.AsViewError(err).IsViewNotFound())
	_, snapshot := beginBreakerTest(t, client.shardClient.breaker, id.VChannel)
	require.Len(t, snapshot.constraints.ExcludedQueryViews, 1)
}

func TestShardRoutingRefreshesConstraintsAfterWireExclusion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		id := qviews.ShardID{ReplicaID: 22, VChannel: "p_1v0"}
		plans := &fakeQueryPlanClient{plan: newTestSearchQueryPlan(id, &viewpb.QueryPlanMVCC{})}
		client := newShardViewQueryClient(1, plans, &fakeViewQueryServiceClient{})
		defer client.Close()
		client.breaker = NewQueryViewCircuitBreaker(QueryViewCircuitBreakerConfig{MaxRoutingWait: 3 * time.Second})
		client.breaker.nodes[11] = &nodeCircuit{retryAt: time.Now().Add(2 * time.Second), expires: time.Now().Add(time.Minute)}
		calls := 0
		patch := mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).To(func(_ *fakeQueryPlanClient, _ context.Context, channel string, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
			calls++
			if qviews.NewQueryRoutingExclusions(channel, req.RoutingConstraints).ExcludesNode(11) {
				return nil, viewerror.ConvertViewError("GetQueryPlan", viewerror.NewGRPCStatusFromError(viewerror.NewAllCandidatesExcluded()).Err())
			}
			return &viewpb.GetQueryPlanResponse{Plan: plans.plan}, nil
		}).Build()
		defer patch.UnPatch()
		started := time.Now()
		_, err := client.Search(t.Context(), &ShardSearchRequest{VChannel: id.VChannel, Req: &internalpb.SearchRequest{}, Reducer: fakeSearchResultReducer{}})
		require.NoError(t, err)
		require.Equal(t, 2*time.Second, time.Since(started))
		require.Equal(t, 3, calls)
		require.True(t, client.breaker.nodes[11].retryAt.IsZero())
	})
}

func TestSlowPlanningDoesNotReserveAnotherShardsProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{})
		resume := make(chan struct{})
		patch := mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).To(func(_ *fakeQueryPlanClient, ctx context.Context, channel string, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
			plan := newTestSearchQueryPlan(qviews.ShardID{ReplicaID: 1, VChannel: channel}, &viewpb.QueryPlanMVCC{})
			if channel == "slow_1v0" {
				close(entered)
				select {
				case <-resume:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				plan.WorkNodes[0].GetQueryNode().NodeId = 12
			}
			return &viewpb.GetQueryPlanResponse{Plan: plan}, nil
		}).Build()
		defer patch.UnPatch()
		client := newShardViewQueryClient(1, &fakeQueryPlanClient{}, &fakeViewQueryServiceClient{})
		defer client.Close()
		client.breaker.nodes[11] = &nodeCircuit{retryAt: time.Now().Add(-time.Second), expires: time.Now().Add(time.Minute)}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := client.Search(ctx, &ShardSearchRequest{VChannel: "slow_1v0", Req: &internalpb.SearchRequest{}, Reducer: fakeSearchResultReducer{}})
			done <- err
		}()
		<-entered
		started := time.Now()
		_, err := client.Search(ctx, &ShardSearchRequest{VChannel: "fast_1v0", Req: &internalpb.SearchRequest{}, Reducer: fakeSearchResultReducer{}})
		require.NoError(t, err)
		require.Equal(t, started, time.Now(), "the slow plan must not delay the other shard's probe")
		require.Empty(t, done)
		require.True(t, client.breaker.nodes[11].retryAt.IsZero())
		close(resume)
		require.NoError(t, <-done)
	})
}

func TestPlanProbeContentionReplansBeforeDispatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := newShardViewQueryClient(1, &fakeQueryPlanClient{}, &fakeViewQueryServiceClient{})
		defer client.Close()
		key := breakerTestKey("p_1v0", 1, 1, 1)
		b := client.breaker
		b.nodes[11] = &nodeCircuit{retryAt: time.Now().Add(-time.Second), expires: time.Now().Add(time.Minute)}
		_, rival := beginBreakerTest(t, b, "another_1v0")
		defer rival.release()
		calls := 0
		patch := mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).To(func(_ *fakeQueryPlanClient, _ context.Context, channel string, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
			calls++
			plan := newTestSearchQueryPlan(qviews.ShardID{ReplicaID: 1, VChannel: channel}, &viewpb.QueryPlanMVCC{})
			if calls == 1 {
				require.Empty(t, req.GetRoutingConstraints().GetExcludedQueryNodeIds())
				require.True(t, rival.acquireProbes(breakerTestPlan(key, 11)))
			} else {
				require.Equal(t, []int64{11}, req.GetRoutingConstraints().GetExcludedQueryNodeIds())
				plan.WorkNodes[0].GetQueryNode().NodeId = 12
			}
			return &viewpb.GetQueryPlanResponse{Plan: plan}, nil
		}).Build()
		defer patch.UnPatch()
		dispatches := 0
		rpc := mockey.Mock((*fakeViewQueryServiceClient).SearchOnView).To(func(_ *fakeViewQueryServiceClient, _ context.Context, node qviews.WorkNode, _ *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
			dispatches++
			require.Equal(t, qviews.NewQueryNode(12), node)
			return &viewpb.SearchOnViewResponse{}, nil
		}).Build()
		defer rpc.UnPatch()
		_, err := client.Search(t.Context(), &ShardSearchRequest{VChannel: key.ShardID.VChannel, Req: &internalpb.SearchRequest{}, Reducer: fakeSearchResultReducer{}})
		require.NoError(t, err)
		require.Equal(t, 2, calls)
		require.Equal(t, 1, dispatches)
		require.Same(t, rival, b.nodes[11].probe)
	})
}

func TestCanceledExecutionReleasesSelectedProbe(t *testing.T) {
	client := newShardViewQueryClient(1, &fakeQueryPlanClient{}, &fakeViewQueryServiceClient{})
	defer client.Close()
	b := client.breaker
	b.nodes[11] = &nodeCircuit{retryAt: time.Now().Add(-time.Second), expires: time.Now().Add(time.Minute), backoff: time.Second}
	plan := newTestSearchQueryPlan(qviews.ShardID{ReplicaID: 1, VChannel: "p_1v0"}, &viewpb.QueryPlanMVCC{})
	patch := mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).Return(&viewpb.GetQueryPlanResponse{Plan: plan}, nil).Build()
	defer patch.UnPatch()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	rpc := mockey.Mock((*fakeViewQueryServiceClient).SearchOnView).To(func(*fakeViewQueryServiceClient, context.Context, qviews.WorkNode, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
		cancel()
		return nil, context.Canceled
	}).Build()
	defer rpc.UnPatch()
	_, err := client.Search(ctx, &ShardSearchRequest{VChannel: "p_1v0", Req: &internalpb.SearchRequest{}, Reducer: fakeSearchResultReducer{}})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, b.nodes[11].probe)
	require.Equal(t, time.Second, b.nodes[11].backoff)
	require.Zero(t, b.nodes[11].failures)
}

func TestShardRoutingReturnsSchemaMismatchOnFirstAttempt(t *testing.T) {
	failure := viewerror.ConvertViewError("GetQueryPlan", viewerror.NewGRPCStatusFromError(viewerror.NewSchemaMismatch("schema retired")).Err())
	patch := mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).Return(nil, failure).Build()
	defer patch.UnPatch()
	client := newShardViewQueryClient(defaultMaxRetries, &fakeQueryPlanClient{}, nil)
	defer client.Close()
	_, err := client.Search(t.Context(), &ShardSearchRequest{VChannel: "p_1v0", Req: &internalpb.SearchRequest{}, Reducer: fakeSearchResultReducer{}})
	require.ErrorIs(t, toMilvusError(err), merr.ErrQueryViewSchemaMismatch)
	require.Equal(t, 1, patch.Times())
}
