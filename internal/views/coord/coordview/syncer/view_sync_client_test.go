package syncer

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	qnmanager "github.com/milvus-io/milvus/internal/querynodev2/client/manager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestDefaultViewSyncClientRoutesQueryNodeAndStreamingNode(t *testing.T) {
	queryClient := &fakeQueryNodeViewSyncClient{
		nodes:      map[int64]*qnmanager.NodeInfo{1: {ServerID: 1}},
		viewClient: &capturingViewSyncServiceClient{},
	}
	streamingClient := &fakeStreamingNodeViewSyncClient{
		viewClient: &capturingViewSyncServiceClient{},
	}
	channels := &fakeChannelAssignmentLookup{
		located: map[string]int64{"p0": 11},
	}

	client := NewDefaultViewSyncClient(queryClient, streamingClient, channels)
	defer client.Close()

	assert.True(t, client.IsNodeAlive(context.Background(), qviews.StreamingNode{PChannel: "p0"}))
	assert.True(t, client.IsNodeAlive(context.Background(), qviews.StreamingNode{PChannel: "missing"}))
	assert.True(t, client.IsNodeAlive(context.Background(), qviews.NewQueryNode(1)))

	_, err := client.OpenSyncStream(context.Background(), qviews.NewQueryNode(1))
	require.NoError(t, err)
	queryNodeID, ok := contextutil.GetPickServerID(queryClient.viewClient.queryCtx)
	require.True(t, ok)
	assert.Equal(t, int64(1), queryNodeID)

	_, err = client.OpenSyncStream(context.Background(), qviews.StreamingNode{PChannel: "p0"})
	require.NoError(t, err)
	streamingNodeID, ok := contextutil.GetPickServerID(streamingClient.viewClient.queryCtx)
	require.True(t, ok)
	assert.Equal(t, int64(11), streamingNodeID)
}

func TestDefaultViewSyncClientForwardsNodeChangedNotifier(t *testing.T) {
	queryClient := &fakeQueryNodeViewSyncClient{
		nodes:      map[int64]*qnmanager.NodeInfo{},
		viewClient: &capturingViewSyncServiceClient{},
	}
	client := NewDefaultViewSyncClient(queryClient, nil, nil)
	defer client.Close()

	var called atomic.Bool
	client.RegisterNodeChangedNotifier(func() {
		called.Store(true)
	})

	queryClient.notifyNodeChanged()
	assert.True(t, called.Load())
}

func TestDefaultViewSyncClientCloseDoesNotCloseManagerClients(t *testing.T) {
	queryClient := &fakeQueryNodeViewSyncClient{}
	streamingClient := &fakeStreamingNodeViewSyncClient{}
	client := NewDefaultViewSyncClient(queryClient, streamingClient, nil)

	client.Close()

	assert.False(t, queryClient.closed)
	assert.False(t, streamingClient.closed)
}

type fakeQueryNodeViewSyncClient struct {
	nodes      map[int64]*qnmanager.NodeInfo
	notifiers  []func()
	viewClient *capturingViewSyncServiceClient
	closed     bool
}

func (c *fakeQueryNodeViewSyncClient) RegisterNodeChangedNotifier(notifier func()) {
	c.notifiers = append(c.notifiers, notifier)
}

func (c *fakeQueryNodeViewSyncClient) GetAllQueryNodes(ctx context.Context) (map[int64]*qnmanager.NodeInfo, error) {
	return c.nodes, nil
}

func (c *fakeQueryNodeViewSyncClient) CreateViewSyncClient(ctx context.Context, queryNodeID int64) (viewpb.ViewSyncServiceClient, error) {
	return &routedTestViewSyncServiceClient{serverID: queryNodeID, client: c.viewClient}, nil
}

func (c *fakeQueryNodeViewSyncClient) Close() {
	c.closed = true
}

func (c *fakeQueryNodeViewSyncClient) notifyNodeChanged() {
	for _, notifier := range c.notifiers {
		notifier()
	}
}

type fakeStreamingNodeViewSyncClient struct {
	viewClient *capturingViewSyncServiceClient
	closed     bool
}

func (c *fakeStreamingNodeViewSyncClient) WatchNodeChanged(ctx context.Context) (<-chan struct{}, error) {
	return nil, nil
}

func (c *fakeStreamingNodeViewSyncClient) GetAllStreamingNodes(ctx context.Context) (map[int64]*types.StreamingNodeInfoWithResourceGroup, error) {
	return nil, nil
}

func (c *fakeStreamingNodeViewSyncClient) CollectAllStatus(ctx context.Context, resourceGroupHint string) (map[int64]*types.StreamingNodeStatus, error) {
	return nil, nil
}

func (c *fakeStreamingNodeViewSyncClient) Assign(ctx context.Context, pchannel types.PChannelInfoAssigned) error {
	return nil
}

func (c *fakeStreamingNodeViewSyncClient) Remove(ctx context.Context, pchannel types.PChannelInfoAssigned) error {
	return nil
}

func (c *fakeStreamingNodeViewSyncClient) CreateViewSyncClient(ctx context.Context, streamingNodeID int64) (viewpb.ViewSyncServiceClient, error) {
	return &routedTestViewSyncServiceClient{serverID: streamingNodeID, client: c.viewClient}, nil
}

func (c *fakeStreamingNodeViewSyncClient) Close() {
	c.closed = true
}

type fakeChannelAssignmentLookup struct {
	located map[string]int64
}

func (l *fakeChannelAssignmentLookup) GetLatestWALLocated(ctx context.Context, pchannel string) (int64, bool) {
	serverID, ok := l.located[pchannel]
	return serverID, ok
}

type routedTestViewSyncServiceClient struct {
	serverID int64
	client   *capturingViewSyncServiceClient
}

func (c *routedTestViewSyncServiceClient) SyncQueryView(ctx context.Context, opts ...grpc.CallOption) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
	return c.client.SyncQueryView(contextutil.WithPickServerID(ctx, c.serverID), opts...)
}

func (c *routedTestViewSyncServiceClient) SyncDataView(ctx context.Context, in *viewpb.SyncDataViewRequest, opts ...grpc.CallOption) (*viewpb.SyncDataViewResponse, error) {
	return c.client.SyncDataView(contextutil.WithPickServerID(ctx, c.serverID), in, opts...)
}

type capturingViewSyncServiceClient struct {
	queryCtx context.Context
	dataCtx  context.Context
}

func (c *capturingViewSyncServiceClient) SyncQueryView(ctx context.Context, opts ...grpc.CallOption) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
	c.queryCtx = ctx
	return nil, nil
}

func (c *capturingViewSyncServiceClient) SyncDataView(ctx context.Context, in *viewpb.SyncDataViewRequest, opts ...grpc.CallOption) (*viewpb.SyncDataViewResponse, error) {
	c.dataCtx = ctx
	return &viewpb.SyncDataViewResponse{}, nil
}
