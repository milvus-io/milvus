package handler

import (
	"context"
	"io"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
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
			pchannel, err := decodeOutgoingQueryViewPChannel(ctx)
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

	resp, _, err := client.QueryViewClient().GetQueryPlan(context.Background(), shardID, planReq)

	require.NoError(t, err)
	require.Same(t, planResp, resp)
}

func TestQueryPlanExecutionKeepsMVCCSourceAcrossAssignmentChange(t *testing.T) {
	shard := qviews.ShardID{VChannel: "p0_100v0"}
	source := types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "p0", AccessMode: types.AccessModeRO, WALReplicaID: 5},
		Node:    types.StreamingNodeInfo{ServerID: 101},
	}
	planService := &fakeQueryPlanServiceClient{
		getQueryPlan: func(context.Context, *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
			return &viewpb.GetQueryPlanResponse{}, nil
		},
	}
	viewService := &fakeViewQueryServiceClient{
		searchOnView: func(ctx context.Context, _ *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
			serverID, ok := contextutil.GetPickServerID(ctx)
			require.True(t, ok)
			require.EqualValues(t, 101, serverID)
			channel, err := decodeOutgoingQueryViewPChannel(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 5, channel.WALReplicaID)
			require.Zero(t, channel.Term)
			require.True(t, contextutil.IsFailFastPick(ctx))
			return nil, viewerror.NewViewInvalidated("replica retired")
		},
	}
	client := newTestHandlerClient(planService, viewService, nil)
	client.readable = func(string) ([]types.PChannelInfoAssigned, <-chan struct{}) {
		return []types.PChannelInfoAssigned{source}, nil
	}
	_, selected, err := client.QueryViewClient().GetQueryPlan(t.Context(), shard,
		&viewpb.GetQueryPlanRequest{Mvcc: &viewpb.GetQueryPlanRequest_ConsistencyLevel{ConsistencyLevel: commonpb.ConsistencyLevel_Bounded}})
	require.NoError(t, err)
	require.Equal(t, source, *selected)
	source.Node.ServerID = 102
	source.Channel.WALReplicaID = 6
	ctx := contextutil.WithQueryWALReplica(t.Context(), selected)
	_, err = client.QueryViewClient().SearchOnView(ctx, types.PChannelInfo{Name: "p0"}, &viewpb.SearchOnViewRequest{})
	require.True(t, viewerror.AsViewError(err).IsRetryable())
}

func TestHandlerClientViewQueryRoutesByStreamingWorkNode(t *testing.T) {
	searchReq := &viewpb.SearchOnViewRequest{}
	searchResp := &viewpb.SearchOnViewResponse{}
	viewQueryService := &fakeViewQueryServiceClient{
		searchOnView: func(ctx context.Context, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
			serverID, ok := contextutil.GetPickServerID(ctx)
			require.True(t, ok)
			require.Equal(t, int64(101), serverID)
			pchannel, err := decodeOutgoingQueryViewPChannel(ctx)
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

func TestHandlerClientSearchOnViewStreamRoutesByStreamingWorkNode(t *testing.T) {
	searchReq := &viewpb.SearchOnViewRequest{}
	clientStream := &fakeSearchOnViewClientStream{}
	viewQueryService := &fakeViewQueryServiceClient{
		searchOnViewStream: func(ctx context.Context) (viewpb.ViewQueryService_SearchOnViewStreamClient, error) {
			serverID, ok := contextutil.GetPickServerID(ctx)
			require.True(t, ok)
			require.Equal(t, int64(101), serverID)
			pchannel, err := decodeOutgoingQueryViewPChannel(ctx)
			require.NoError(t, err)
			require.Equal(t, types.PChannelInfo{
				Name:       "p0",
				Term:       1,
				AccessMode: types.AccessModeRW,
			}, pchannel)
			clientStream.ctx = ctx
			return clientStream, nil
		},
	}
	client := newTestHandlerClient(nil, viewQueryService, nil)

	stream, err := client.QueryViewClient().SearchOnViewStream(context.Background(), types.PChannelInfo{Name: "p0"}, searchReq)

	require.NoError(t, err)
	require.Same(t, searchReq, clientStream.sent.GetRequest())
	chunk, err := stream.Recv()
	require.Nil(t, chunk)
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, stream.Close())
	require.Equal(t, 1, clientStream.closeCalls)
}

func TestHandlerClientQueryOnViewStreamRoutesByStreamingWorkNode(t *testing.T) {
	queryReq := &viewpb.QueryOnViewRequest{}
	clientStream := &fakeQueryOnViewClientStream{}
	viewQueryService := &fakeViewQueryServiceClient{
		queryOnViewStream: func(ctx context.Context) (viewpb.ViewQueryService_QueryOnViewStreamClient, error) {
			serverID, ok := contextutil.GetPickServerID(ctx)
			require.True(t, ok)
			require.Equal(t, int64(101), serverID)
			pchannel, err := decodeOutgoingQueryViewPChannel(ctx)
			require.NoError(t, err)
			require.Equal(t, types.PChannelInfo{
				Name:       "p0",
				Term:       1,
				AccessMode: types.AccessModeRW,
			}, pchannel)
			clientStream.ctx = ctx
			return clientStream, nil
		},
	}
	client := newTestHandlerClient(nil, viewQueryService, nil)

	stream, err := client.QueryViewClient().QueryOnViewStream(context.Background(), types.PChannelInfo{Name: "p0"}, queryReq)

	require.NoError(t, err)
	require.Same(t, queryReq, clientStream.sent.GetRequest())
	chunk, err := stream.Recv()
	require.Nil(t, chunk)
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, stream.Close())
	require.Equal(t, 1, clientStream.closeCalls)
}

func TestHandlerClientPreservesViewQueryRPCError(t *testing.T) {
	viewErr := viewerror.NewViewNotFound("missing view")
	viewQueryService := &fakeViewQueryServiceClient{
		searchOnView: func(context.Context, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
			return nil, viewerror.ConvertViewError(viewpb.ViewQueryService_SearchOnView_FullMethodName, viewerror.NewGRPCStatusFromViewError(viewErr).Err())
		},
	}
	client := newTestHandlerClient(nil, viewQueryService, nil)

	_, err := client.QueryViewClient().SearchOnView(context.Background(), types.PChannelInfo{Name: "p0"}, &viewpb.SearchOnViewRequest{})

	require.Error(t, err)
	require.True(t, viewerror.AsViewError(err).IsViewNotFound())
}

func TestHandlerClientViewSyncRoutesByPChannelAssignment(t *testing.T) {
	mockey.PatchConvey("view sync uses its current pchannel assignment", t, func() {
		service := &syncViewRPC{}
		mockey.Mock((*syncViewRPC).SyncQueryView).To(func(_ *syncViewRPC, ctx context.Context, _ ...grpc.CallOption) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
			serverID, ok := contextutil.GetPickServerID(ctx)
			require.True(t, ok)
			require.Equal(t, int64(101), serverID)
			pchannel, err := decodeOutgoingQueryViewPChannel(ctx)
			require.NoError(t, err)
			require.Equal(t, types.PChannelInfo{Name: "p0", Term: 1, AccessMode: types.AccessModeRW}, pchannel)
			return &syncViewStream{}, nil
		}).Build()
		client := newTestHandlerClient(nil, nil, service)
		_, err := client.QueryViewSyncClient().SyncQueryView(t.Context(), "p0")
		require.NoError(t, err)
	})
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
	viewpb.ViewQueryServiceClient // Retain the unused Requery RPC contract.
	searchOnView                  func(context.Context, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error)
	searchOnViewStream            func(context.Context) (viewpb.ViewQueryService_SearchOnViewStreamClient, error)
	queryOnView                   func(context.Context, *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error)
	queryOnViewStream             func(context.Context) (viewpb.ViewQueryService_QueryOnViewStreamClient, error)
}

func (c *fakeViewQueryServiceClient) SearchOnView(ctx context.Context, req *viewpb.SearchOnViewRequest, _ ...grpc.CallOption) (*viewpb.SearchOnViewResponse, error) {
	return c.searchOnView(ctx, req)
}

func (c *fakeViewQueryServiceClient) SearchOnViewStream(ctx context.Context, _ ...grpc.CallOption) (viewpb.ViewQueryService_SearchOnViewStreamClient, error) {
	return c.searchOnViewStream(ctx)
}

func (c *fakeViewQueryServiceClient) QueryOnView(ctx context.Context, req *viewpb.QueryOnViewRequest, _ ...grpc.CallOption) (*viewpb.QueryOnViewResponse, error) {
	return c.queryOnView(ctx, req)
}

func (c *fakeViewQueryServiceClient) QueryOnViewStream(ctx context.Context, _ ...grpc.CallOption) (viewpb.ViewQueryService_QueryOnViewStreamClient, error) {
	return c.queryOnViewStream(ctx)
}

type fakeSearchOnViewClientStream struct {
	ctx        context.Context
	sent       *viewpb.SearchOnViewStreamRequest
	closeCalls int
}

func (s *fakeSearchOnViewClientStream) Send(request *viewpb.SearchOnViewStreamRequest) error {
	s.sent = request
	return nil
}

func (*fakeSearchOnViewClientStream) Recv() (*viewpb.SearchOnViewStreamResponse, error) {
	return nil, io.EOF
}

func (*fakeSearchOnViewClientStream) Header() (metadata.MD, error) {
	return nil, nil
}

func (*fakeSearchOnViewClientStream) Trailer() metadata.MD {
	return nil
}

func (s *fakeSearchOnViewClientStream) CloseSend() error {
	s.closeCalls++
	return nil
}

func (s *fakeSearchOnViewClientStream) Context() context.Context {
	return s.ctx
}

func (*fakeSearchOnViewClientStream) SendMsg(interface{}) error {
	return nil
}

func (*fakeSearchOnViewClientStream) RecvMsg(interface{}) error {
	return io.EOF
}

type fakeQueryOnViewClientStream struct {
	ctx        context.Context
	sent       *viewpb.QueryOnViewStreamRequest
	closeCalls int
}

func (s *fakeQueryOnViewClientStream) Send(request *viewpb.QueryOnViewStreamRequest) error {
	s.sent = request
	return nil
}

func (*fakeQueryOnViewClientStream) Recv() (*viewpb.QueryOnViewStreamResponse, error) {
	return nil, io.EOF
}

func (*fakeQueryOnViewClientStream) Header() (metadata.MD, error) {
	return nil, nil
}

func (*fakeQueryOnViewClientStream) Trailer() metadata.MD {
	return nil
}

func (s *fakeQueryOnViewClientStream) CloseSend() error {
	s.closeCalls++
	return nil
}

func (s *fakeQueryOnViewClientStream) Context() context.Context {
	return s.ctx
}

func (*fakeQueryOnViewClientStream) SendMsg(interface{}) error {
	return nil
}

func (*fakeQueryOnViewClientStream) RecvMsg(interface{}) error {
	return io.EOF
}

type fakeViewSyncServiceClient struct {
	syncQueryView func(context.Context) (viewpb.ViewSyncService_SyncQueryViewClient, error)
}

func (c *fakeViewSyncServiceClient) SyncQueryView(ctx context.Context, _ ...grpc.CallOption) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
	return c.syncQueryView(ctx)
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

func decodeOutgoingQueryViewPChannel(ctx context.Context) (types.PChannelInfo, error) {
	md, _ := metadata.FromOutgoingContext(ctx)
	return worknodehandler.DecodeQueryViewPChannelFromIncomingContext(metadata.NewIncomingContext(ctx, md))
}

func TestBoundedPlanTriesReadersBeforeWriter(t *testing.T) {
	shard := qviews.ShardID{VChannel: "p0_100v0"}
	routes := []types.PChannelInfoAssigned{
		{Node: types.StreamingNodeInfo{ServerID: 1}, Channel: types.PChannelInfo{Name: "p0", WALReplicaID: 1, AccessMode: types.AccessModeRO}},
		{Node: types.StreamingNodeInfo{ServerID: 2}, Channel: types.PChannelInfo{Name: "p0", WALReplicaID: 2, AccessMode: types.AccessModeRO}},
		{Node: types.StreamingNodeInfo{ServerID: 3}, Channel: types.PChannelInfo{Name: "p0", WALReplicaID: 3, Term: 8, AccessMode: types.AccessModeRW}},
	}
	var selected []int64
	client := newTestHandlerClient(&fakeQueryPlanServiceClient{getQueryPlan: func(ctx context.Context, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
		target, _ := contextutil.GetPickServerID(ctx)
		selected = append(selected, target)
		switch target {
		case 1:
			return nil, viewerror.NewViewNotFound("not locally up")
		case 2:
			return nil, viewerror.NewOnShutdownError("closing")
		}
		return &viewpb.GetQueryPlanResponse{}, nil
	}}, nil, nil)
	client.readable = func(string) ([]types.PChannelInfoAssigned, <-chan struct{}) { return routes, nil }
	req := &viewpb.GetQueryPlanRequest{Mvcc: &viewpb.GetQueryPlanRequest_ConsistencyLevel{ConsistencyLevel: commonpb.ConsistencyLevel_Bounded}}
	_, source, err := client.QueryViewClient().GetQueryPlan(t.Context(), shard, req)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3}, selected)
	require.EqualValues(t, 3, source.Node.ServerID)
	require.Zero(t, source.Channel.Term)
	routes = routes[:2]
	_, _, err = client.QueryViewClient().GetQueryPlan(t.Context(), shard, req)
	require.True(t, viewerror.AsViewError(err).IsViewNotFound())
	routes = nil
	_, _, err = client.QueryViewClient().GetQueryPlan(t.Context(), shard, req)
	require.ErrorIs(t, err, ErrClientClosed)
}
