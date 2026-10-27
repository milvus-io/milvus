package queryclient

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/replicadiscovery"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// Existing reducer/MVCC tests isolate discovery while keeping explicit routing.
func setupTestReplicaRouting(t *testing.T) {
	patch := mockey.Mock((*replicaRouter).route).To(func(*replicaRouter, string) (*replicaRoute, error) {
		return &replicaRoute{changed: make(chan struct{}), snapshot: replicadiscovery.Snapshot{Valid: true, Replicas: []replicadiscovery.Replica{{ReplicaID: 1}}}}, nil
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
}

type replicaTestSource struct{ QueryPlanClient }

func (*replicaTestSource) SubscribeReplicas(string, replicadiscovery.Handler) (replicadiscovery.Subscription, error) {
	panic("mockey")
}

func newTestReplicaRouter(t *testing.T) (*replicaRouter, *replicadiscovery.Directory) {
	d := replicadiscovery.NewDirectory()
	manager := d.AcquireStream(t.Context(), "p", nil)
	patch := mockey.Mock((*replicaTestSource).SubscribeReplicas).To(func(_ *replicaTestSource, vc string, h replicadiscovery.Handler) (replicadiscovery.Subscription, error) {
		return manager.Subscribe(t.Context(), vc, h)
	}).Build()
	r := newReplicaRouter(&replicaTestSource{})
	t.Cleanup(func() { r.Close(); manager.Close(); d.Close(); patch.UnPatch() })
	return r, d
}

func TestReplicaRouterWaitsForNonemptyAndRotates(t *testing.T) {
	r, d := newTestReplicaRouter(t)
	route, err := r.route("p_1v0")
	require.NoError(t, err)
	same, err := r.route("p_1v0")
	require.NoError(t, err)
	require.Same(t, route, same)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	_, _, err = route.nextReplica(ctx, map[replicadiscovery.Replica]bool{}, nil)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	version := qviews.QueryViewVersion{QueryVersion: 1}
	d.Set("p_1v0", 1, &version)
	d.Set("p_1v0", 2, &version)
	require.Eventually(t, func() bool { route.mu.Lock(); defer route.mu.Unlock(); return len(route.snapshot.Replicas) == 2 }, time.Second, time.Millisecond)
	tried := make(map[replicadiscovery.Replica]bool)
	first, _, err := route.nextReplica(t.Context(), tried, nil)
	require.NoError(t, err)
	tried[first] = true
	second, _, err := route.nextReplica(t.Context(), tried, nil)
	require.NoError(t, err)
	require.NotEqual(t, first.ReplicaID, second.ReplicaID)
	tried[second] = true
	ctx, cancel = context.WithTimeout(t.Context(), 2*replicaRetryInterval)
	_, _, err = route.nextReplica(ctx, tried, nil)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded, "dead candidates must not be retried on timer")
	version.QueryVersion++
	d.Set("p_1v0", first.ReplicaID, &version)
	ctx, cancel = context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	next, _, err := route.nextReplica(ctx, tried, nil)
	require.NoError(t, err)
	require.Equal(t, first.ReplicaID, next.ReplicaID)
	require.Equal(t, version, next.MaxUpViewVersion)
	r.Close()
	_, _, err = route.nextReplica(t.Context(), tried, nil)
	require.Error(t, err)
	_, err = r.route("p_1v0")
	require.Error(t, err)
}

func TestReplicaRouterTransientFailureRetriesWithoutDirectoryChange(t *testing.T) {
	r, d := newTestReplicaRouter(t)
	version := qviews.QueryViewVersion{QueryVersion: 1}
	d.Set("p_1v0", 1, &version)
	route, err := r.route("p_1v0")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	candidate, _, err := route.nextReplica(ctx, map[replicadiscovery.Replica]bool{}, nil)
	require.NoError(t, err)
	start := time.Now()
	next, _, err := route.nextReplica(ctx, map[replicadiscovery.Replica]bool{candidate: false}, nil)
	require.NoError(t, err)
	require.Equal(t, candidate, next)
	require.GreaterOrEqual(t, time.Since(start), replicaRetryInterval)
}

func TestShardFailoverRechecksDeadNodesAfterVersionChange(t *testing.T) {
	r, d := newTestReplicaRouter(t)
	version := qviews.QueryViewVersion{DataVersion: qviews.DataVersion{StreamingVersion: 1}, QueryVersion: 1}
	d.Set("p_1v0", 1, &version)
	source := &fakeQueryPlanClient{}
	service := &fakeViewQueryServiceClient{}
	plans, calls := 0, 0
	planPatch := mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).To(func(_ *fakeQueryPlanClient, _ context.Context, id qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
		plans++
		require.EqualValues(t, 1, id.ReplicaID)
		require.EqualValues(t, 123, req.GetBarrierMvcc())
		node := int64(11)
		if plans == 2 {
			next := version
			next.QueryVersion = 3
			d.Set("p_1v0", 1, &next)
		}
		if plans == 3 {
			node = 12
		}
		plan := newTestSearchQueryPlan(id, &viewpb.QueryPlanMVCC{GrowingTimetick: 123, TransformingTimetick: 123})
		plan.WorkNodes = []*viewpb.QueryPlanWorkNode{{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: node}}}}
		return &viewpb.GetQueryPlanResponse{Plan: plan}, nil
	}).Build()
	defer planPatch.UnPatch()
	rpcPatch := mockey.Mock((*fakeViewQueryServiceClient).SearchOnView).To(func(_ *fakeViewQueryServiceClient, _ context.Context, node qviews.WorkNode, _ *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
		calls++
		if node.(qviews.QueryNode).ID == 11 {
			next := version
			next.QueryVersion = 2
			d.Set("p_1v0", 1, &next)
			return nil, picker.ErrSubConnNotExist
		}
		return &viewpb.SearchOnViewResponse{}, nil
	}).Build()
	defer rpcPatch.UnPatch()
	client := newShardViewQueryClient(0, source, service)
	client.replicas = r
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := client.Search(ctx, &ShardSearchRequest{VChannel: "p_1v0", Req: &internalpb.SearchRequest{}, BarrierMVCC: 123, Reducer: fakeSearchResultReducer{}})
	require.NoError(t, err)
	require.Equal(t, 3, plans)
	require.Equal(t, 2, calls, "new maximum with same dead worker must not dispatch")
}

func TestShardFailoverTriesHealthyReplica(t *testing.T) {
	for _, failure := range []error{picker.ErrSubConnNotExist, picker.ErrSubConnNotReady} {
		t.Run(failure.Error(), func(t *testing.T) {
			r, d := newTestReplicaRouter(t)
			version := qviews.QueryViewVersion{QueryVersion: 1}
			d.Set("p_1v0", 1, &version)
			d.Set("p_1v0", 2, &version)
			route, err := r.route("p_1v0")
			require.NoError(t, err)
			require.Eventually(t, func() bool { route.mu.Lock(); defer route.mu.Unlock(); return len(route.snapshot.Replicas) == 2 }, time.Second, time.Millisecond)
			source := &fakeQueryPlanClient{}
			service := &fakeViewQueryServiceClient{}
			var selected []int64
			planPatch := mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).To(func(_ *fakeQueryPlanClient, _ context.Context, id qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
				selected = append(selected, id.ReplicaID)
				plan := newTestSearchQueryPlan(id, &viewpb.QueryPlanMVCC{})
				plan.WorkNodes = []*viewpb.QueryPlanWorkNode{{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: id.ReplicaID + 10}}}}
				return &viewpb.GetQueryPlanResponse{Plan: plan}, nil
			}).Build()
			defer planPatch.UnPatch()
			rpcPatch := mockey.Mock((*fakeViewQueryServiceClient).SearchOnView).To(func(_ *fakeViewQueryServiceClient, _ context.Context, node qviews.WorkNode, _ *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
				if node.(qviews.QueryNode).ID == 11 {
					return nil, failure
				}
				return &viewpb.SearchOnViewResponse{}, nil
			}).Build()
			defer rpcPatch.UnPatch()
			client := newShardViewQueryClient(1, source, service)
			client.replicas = r
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			plan, err := client.Search(ctx, &ShardSearchRequest{VChannel: "p_1v0", Req: &internalpb.SearchRequest{}, Reducer: fakeSearchResultReducer{}})
			require.NoError(t, err)
			require.EqualValues(t, 2, plan.ShardID.ReplicaID)
			require.Equal(t, []int64{1, 2}, selected)
		})
	}
}

func TestShardReconsidersMissingSessionOnResolverUpdate(t *testing.T) {
	r, d := newTestReplicaRouter(t)
	version := qviews.QueryViewVersion{QueryVersion: 1}
	d.Set("p_1v0", 1, &version)
	source := &fakeQueryPlanClient{}
	service := &fakeViewQueryServiceClient{}
	changed := make(chan struct{})
	calls := 0
	planPatch := mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).To(func(_ *fakeQueryPlanClient, _ context.Context, id qviews.ShardID, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
		plan := newTestSearchQueryPlan(id, &viewpb.QueryPlanMVCC{})
		plan.WorkNodes = []*viewpb.QueryPlanWorkNode{{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: 11}}}}
		return &viewpb.GetQueryPlanResponse{Plan: plan}, nil
	}).Build()
	defer planPatch.UnPatch()
	rpcPatch := mockey.Mock((*fakeViewQueryServiceClient).SearchOnView).To(func(_ *fakeViewQueryServiceClient, _ context.Context, _ qviews.WorkNode, _ *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
		calls++
		if calls == 1 {
			// The directory caught up before the failed RPC returned. No View update.
			close(changed)
			return nil, &viewerror.NodeSessionUnavailable{Cause: viewerror.ConvertViewError("SearchOnView", picker.ErrSubConnNotExist), Changed: changed}
		}
		return &viewpb.SearchOnViewResponse{}, nil
	}).Build()
	defer rpcPatch.UnPatch()
	client := newShardViewQueryClient(0, source, service)
	client.replicas = r
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := client.Search(ctx, &ShardSearchRequest{VChannel: "p_1v0", Req: &internalpb.SearchRequest{}, Reducer: fakeSearchResultReducer{}})
	require.NoError(t, err)
	require.Equal(t, 2, calls)
}

func TestReplicaRouterRejectsInvalidSubscriptionAndCancelsBackoff(t *testing.T) {
	r, d := newTestReplicaRouter(t)
	_, err := r.route("")
	require.Error(t, err)
	require.Empty(t, r.channels)
	version := qviews.QueryViewVersion{QueryVersion: 1}
	d.Set("p_1v0", 1, &version)
	route, err := r.route("p_1v0")
	require.NoError(t, err)
	candidate, _, err := route.nextReplica(t.Context(), map[replicadiscovery.Replica]bool{}, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	_, _, err = route.nextReplica(ctx, map[replicadiscovery.Replica]bool{candidate: false}, nil)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	ctx, cancel = context.WithCancel(t.Context())
	cancel()
	_, _, err = route.nextReplica(ctx, map[replicadiscovery.Replica]bool{}, nil)
	require.ErrorIs(t, err, context.Canceled, "cached replicas cannot bypass caller cancellation")
}

func TestShardExhaustedAttemptsPreserveTransportError(t *testing.T) {
	setupTestReplicaRouting(t)
	id := qviews.ShardID{ReplicaID: 1, VChannel: "p_1v0"}
	plan := newTestSearchQueryPlan(id, &viewpb.QueryPlanMVCC{})
	plan.WorkNodes = []*viewpb.QueryPlanWorkNode{{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: 11}}}}
	source := &fakeQueryPlanClient{plan: plan}
	service := &fakeViewQueryServiceClient{}
	patch := mockey.Mock((*fakeViewQueryServiceClient).SearchOnView).Return(nil, picker.ErrSubConnNotReady).Build()
	defer patch.UnPatch()
	client := newShardViewQueryClient(1, source, service)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := client.Search(ctx, &ShardSearchRequest{VChannel: id.VChannel, Req: &internalpb.SearchRequest{}, Reducer: fakeSearchResultReducer{}})
	require.ErrorIs(t, err, picker.ErrSubConnNotReady, "Proxy must receive the original cause after bounded attempts")
}
