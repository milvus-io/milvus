package handler

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestHandlerClientGetQueryPlanRoutesByShardPChannel(t *testing.T) {
	shardID := qviews.ShardID{
		ReplicaID: 1,
		VChannel:  funcutil.GetVirtualChannel("p0", 100, 0),
	}
	planReq := &viewpb.GetQueryPlanRequest{ShardId: shardID.IntoProto()}
	planResp := &viewpb.GetQueryPlanResponse{Plan: &viewpb.QueryPlan{ShardId: shardID.IntoProto()}}
	queryPlanService := &fakeQueryPlanServiceClient{
		getQueryPlan: func(ctx context.Context, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
			serverID, ok := contextutil.GetPickServerID(ctx)
			require.True(t, ok)
			require.Equal(t, int64(101), serverID)
			pchannel, err := worknodehandler.DecodeQueryViewPChannelFromOutgoingContext(ctx)
			require.NoError(t, err)
			require.Equal(t, types.PChannelInfo{
				Name:       "p0",
				Term:       1,
				AccessMode: types.AccessModeRW,
			}, pchannel)
			require.Same(t, planReq, req)
			return planResp, nil
		},
	}
	client := newTestHandlerClient(queryPlanService, nil, nil)

	resp, err := client.QueryViewClient().GetQueryPlan(context.Background(), shardID, planReq)

	require.NoError(t, err)
	require.Same(t, planResp, resp)
}

func TestHandlerClientViewQueryRoutesByStreamingWorkNode(t *testing.T) {
	searchReq := &viewpb.SearchOnViewRequest{}
	searchResp := &viewpb.SearchOnViewResponse{}
	viewQueryService := &fakeViewQueryServiceClient{
		searchOnView: func(ctx context.Context, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
			serverID, ok := contextutil.GetPickServerID(ctx)
			require.True(t, ok)
			require.Equal(t, int64(101), serverID)
			pchannel, err := worknodehandler.DecodeQueryViewPChannelFromOutgoingContext(ctx)
			require.NoError(t, err)
			require.Equal(t, types.PChannelInfo{
				Name:       "p0",
				Term:       1,
				AccessMode: types.AccessModeRW,
			}, pchannel)
			require.Same(t, searchReq, req)
			return searchResp, nil
		},
	}
	client := newTestHandlerClient(nil, viewQueryService, nil)

	resp, err := client.QueryViewClient().SearchOnView(context.Background(), types.PChannelInfo{Name: "p0"}, searchReq)

	require.NoError(t, err)
	require.Same(t, searchResp, resp)
}

func TestHandlerClientConvertsViewQueryRPCError(t *testing.T) {
	viewErr := viewerror.NewViewNotFound("missing view")
	viewQueryService := &fakeViewQueryServiceClient{
		searchOnView: func(context.Context, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
			return nil, viewerror.NewGRPCStatusFromViewError(viewErr).Err()
		},
	}
	client := newTestHandlerClient(nil, viewQueryService, nil)

	_, err := client.QueryViewClient().SearchOnView(context.Background(), types.PChannelInfo{Name: "p0"}, &viewpb.SearchOnViewRequest{})

	require.Error(t, err)
	require.True(t, viewerror.AsViewError(err).IsViewNotFound())
}

func TestHandlerClientViewSyncRoutesByPChannelAssignment(t *testing.T) {
	viewSyncService := &fakeViewSyncServiceClient{
		syncQueryView: func(ctx context.Context) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
			serverID, ok := contextutil.GetPickServerID(ctx)
			require.True(t, ok)
			require.Equal(t, int64(101), serverID)
			pchannel, err := worknodehandler.DecodeQueryViewPChannelFromOutgoingContext(ctx)
			require.NoError(t, err)
			require.Equal(t, types.PChannelInfo{
				Name:       "p0",
				Term:       1,
				AccessMode: types.AccessModeRW,
			}, pchannel)
			return &noopViewSyncClientStream{ctx: ctx}, nil
		},
	}
	client := newTestHandlerClient(nil, nil, viewSyncService)

	_, err := client.QueryViewSyncClient().SyncQueryView(context.Background(), "p0")

	require.NoError(t, err)
}

func newTestHandlerClient(queryPlanService viewpb.QueryPlanServiceClient, viewQueryService viewpb.ViewQueryServiceClient, viewSyncService viewpb.ViewSyncServiceClient) *handlerClientImpl {
	client := &handlerClientImpl{
		lifetime: typeutil.NewLifetime(),
		watcher: fakeAssignmentWatcher{
			assignment: &types.PChannelInfoAssigned{
				Channel: types.PChannelInfo{Name: "p0", Term: 1, AccessMode: types.AccessModeRW},
				Node:    types.StreamingNodeInfo{ServerID: 101, Address: "localhost"},
			},
		},
	}
	client.queryViewClient = &queryViewClient{
		owner:            client,
		queryPlanService: fakeLazyService[viewpb.QueryPlanServiceClient]{service: queryPlanService},
		viewQueryService: fakeLazyService[viewpb.ViewQueryServiceClient]{service: viewQueryService},
	}
	client.queryViewSyncClient = &queryViewSyncClient{
		owner:           client,
		viewSyncService: fakeLazyService[viewpb.ViewSyncServiceClient]{service: viewSyncService},
	}
	return client
}

type fakeAssignmentWatcher struct {
	assignment *types.PChannelInfoAssigned
}

func (w fakeAssignmentWatcher) Get(context.Context, string) *types.PChannelInfoAssigned {
	return w.assignment
}

func (w fakeAssignmentWatcher) Watch(context.Context, string, *types.PChannelInfoAssigned) error {
	return context.Canceled
}

func (w fakeAssignmentWatcher) Close() {}

type fakeLazyService[T any] struct {
	service T
}

func (s fakeLazyService[T]) GetConn(context.Context) (*grpc.ClientConn, error) {
	return nil, nil
}

func (s fakeLazyService[T]) GetService(context.Context) (T, error) {
	return s.service, nil
}

func (s fakeLazyService[T]) Close() {}

type fakeQueryPlanServiceClient struct {
	getQueryPlan     func(context.Context, *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error)
	getMVCCTimestamp func(context.Context, *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error)
}

func (c *fakeQueryPlanServiceClient) GetQueryPlan(ctx context.Context, req *viewpb.GetQueryPlanRequest, _ ...grpc.CallOption) (*viewpb.GetQueryPlanResponse, error) {
	return c.getQueryPlan(ctx, req)
}

func (c *fakeQueryPlanServiceClient) GetMVCCTimestamp(ctx context.Context, req *viewpb.GetMVCCTimestampRequest, _ ...grpc.CallOption) (*viewpb.GetMVCCTimestampResponse, error) {
	return c.getMVCCTimestamp(ctx, req)
}

type fakeViewQueryServiceClient struct {
	searchOnView  func(context.Context, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error)
	queryOnView   func(context.Context, *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error)
	requeryOnView func(context.Context, *viewpb.RequeryOnViewRequest) (*viewpb.RequeryOnViewResponse, error)
}

func (c *fakeViewQueryServiceClient) SearchOnView(ctx context.Context, req *viewpb.SearchOnViewRequest, _ ...grpc.CallOption) (*viewpb.SearchOnViewResponse, error) {
	return c.searchOnView(ctx, req)
}

func (c *fakeViewQueryServiceClient) QueryOnView(ctx context.Context, req *viewpb.QueryOnViewRequest, _ ...grpc.CallOption) (*viewpb.QueryOnViewResponse, error) {
	return c.queryOnView(ctx, req)
}

func (c *fakeViewQueryServiceClient) RequeryOnView(ctx context.Context, req *viewpb.RequeryOnViewRequest, _ ...grpc.CallOption) (*viewpb.RequeryOnViewResponse, error) {
	return c.requeryOnView(ctx, req)
}

type fakeViewSyncServiceClient struct {
	syncQueryView func(context.Context) (viewpb.ViewSyncService_SyncQueryViewClient, error)
}

func (c *fakeViewSyncServiceClient) SyncQueryView(ctx context.Context, _ ...grpc.CallOption) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
	return c.syncQueryView(ctx)
}

func (c *fakeViewSyncServiceClient) SyncDataView(context.Context, *viewpb.SyncDataViewRequest, ...grpc.CallOption) (*viewpb.SyncDataViewResponse, error) {
	return &viewpb.SyncDataViewResponse{}, nil
}

type noopViewSyncClientStream struct {
	ctx context.Context
}

func (s *noopViewSyncClientStream) Send(*viewpb.SyncRequest) error {
	return nil
}

func (s *noopViewSyncClientStream) Recv() (*viewpb.SyncResponse, error) {
	return nil, io.EOF
}

func (s *noopViewSyncClientStream) Header() (metadata.MD, error) {
	return nil, nil
}

func (s *noopViewSyncClientStream) Trailer() metadata.MD {
	return nil
}

func (s *noopViewSyncClientStream) CloseSend() error {
	return nil
}

func (s *noopViewSyncClientStream) Context() context.Context {
	return s.ctx
}

func (s *noopViewSyncClientStream) SendMsg(interface{}) error {
	return nil
}

func (s *noopViewSyncClientStream) RecvMsg(interface{}) error {
	return io.EOF
}
