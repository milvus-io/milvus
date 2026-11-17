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
	planPatch := mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).To(func(_ *fakeQueryPlanClient, _ context.Context, id qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
		plans++
		require.Equal(t, qviews.UnknownReplicaID, id.ReplicaID)
		require.Equal(t, id.IntoProto(), req.ShardId)
		require.EqualValues(t, 123, req.GetBarrierMvcc())
		id.ReplicaID = int64(plans)
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
	patch := mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).To(func(*fakeQueryPlanClient, context.Context, qviews.ShardID, *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
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
	patch := mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).To(func(_ *fakeQueryPlanClient, ctx context.Context, _ qviews.ShardID, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
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
