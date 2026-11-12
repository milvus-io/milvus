package handler

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	streaminginterceptor "github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/util/mock_types"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Keep the address fixed while the server ID changes, as happens when an SN
// restarts in the same pod. Use the production resolver, picker and interceptors.
func TestQueryPlanNodeMismatchWaitsForAssignment(t *testing.T) {
	paramtable.Init()
	for _, method := range []string{"GetQueryPlan", "GetMVCCTimestamp"} {
		t.Run(method, func(t *testing.T) {
			fixture := newNodeMismatchFixture(t, false)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- fixture.read(ctx, method) }()
			fixture.waitForReport(t, ctx)
			select {
			case err := <-done:
				t.Fatalf("RPC returned before the assignment changed: %v", err)
			default:
			}

			fixture.publish(2, 2, 1683)
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			// The rejected attempts never reach the handler; only the attempt
			// on the new assignment does.
			require.EqualValues(t, 1, fixture.handled.Load())
		})
	}
}

func TestQueryPlanNodeMismatchCancellation(t *testing.T) {
	paramtable.Init()
	for _, mode := range []string{"cancel", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			fixture := newNodeMismatchFixture(t, false)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- fixture.read(ctx, "GetQueryPlan") }()
			fixture.waitForReport(t, ctx)
			wantErr := context.DeadlineExceeded
			if mode == "cancel" {
				cancel()
				wantErr = context.Canceled
			}
			select {
			case err := <-done:
				require.ErrorIs(t, err, wantErr)
			case <-time.After(2 * time.Second):
				t.Fatal("assignment wait ignored request cancellation")
			}
			require.Zero(t, fixture.handled.Load())
		})
	}
}

func TestPhaseTwoNodeMismatchWaitsForAssignmentThroughGRPC(t *testing.T) {
	paramtable.Init()
	fixture := newNodeMismatchFixture(t, true)
	plans := &recordingQueryPlanClient{QueryViewClient: fixture.client.QueryViewClient()}
	service := queryclient.NewCompositeViewQueryServiceClient(fixture.client.QueryViewClient(), nil)
	query := queryclient.NewLegacyViewQueryClient(queryclient.ViewQueryClientConfig{}, plans, service,
		&retryShardResolver{vchannel: fixture.vchannel})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := query.Legacy().Search(ctx, &queryclient.LegacySearchRequest{Req: &internalpb.SearchRequest{CollectionID: 100}})
		done <- err
	}()
	fixture.waitForReport(t, ctx)
	// The plan succeeded and its execution was rejected: the request waits for
	// the replacement SN and replays the same plan there.
	fixture.publish(2, 2, 1683)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.Equal(t, []codes.Code{codes.OK}, plans.codes)
	require.EqualValues(t, 1, fixture.executions.Load())
}

func TestQueryPlanOrdinaryErrorsBypassAssignmentRecovery(t *testing.T) {
	for _, rpcErr := range []error{
		status.Error(codes.Unknown, "node not match in an unrelated error"),
		status.Error(codes.FailedPrecondition, "unrelated precondition"),
		status.Error(codes.InvalidArgument, "invalid query"),
		viewerror.NewGRPCStatusFromViewError(viewerror.NewViewNotFound("missing view")).Err(),
	} {
		t.Run(rpcErr.Error(), func(t *testing.T) {
			calls := 0
			client := newTestHandlerClient(&fakeQueryPlanServiceClient{
				getQueryPlan: func(context.Context, *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
					calls++
					return nil, rpcErr
				},
			}, nil, nil)
			_, err := client.QueryViewClient().GetQueryPlan(context.Background(), qviews.ShardID{VChannel: "p0"}, &viewpb.GetQueryPlanRequest{})
			require.Equal(t, status.Code(rpcErr), status.Code(err))
			require.Equal(t, 1, calls)
		})
	}
}

type nodeMismatchFixture struct {
	client     *handlerClientImpl
	updates    chan *types.VersionedStreamingNodeAssignments
	reported   chan types.PChannelInfo
	address    string
	vchannel   string
	handled    atomic.Int32
	executions atomic.Int32
	serverID   atomic.Int64
}

func newNodeMismatchFixture(t *testing.T, restartAfterPlan bool) *nodeMismatchFixture {
	t.Helper()
	f := &nodeMismatchFixture{
		updates:  make(chan *types.VersionedStreamingNodeAssignments, 2),
		reported: make(chan types.PChannelInfo, 1),
		vchannel: funcutil.GetVirtualChannel("p0", 100, 0),
	}
	f.serverID.Store(1683)
	if restartAfterPlan {
		f.serverID.Store(1682)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	f.address = listener.Addr().String()
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(
		streaminginterceptor.NewServiceUnaryServerInterceptor(),
		interceptor.ServerIDValidationUnaryServerInterceptor(f.serverID.Load),
		func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			f.handled.Add(1)
			return next(ctx, req)
		},
	))
	service := &nodeMismatchQueryServer{fixture: f, t: t, restartAfterPlan: restartAfterPlan}
	viewpb.RegisterQueryPlanServiceServer(server, service)
	viewpb.RegisterViewQueryServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	f.publish(1, 1, 1682)
	watcher := mock_types.NewMockAssignmentDiscoverWatcher(t)
	watcher.EXPECT().AssignmentDiscover(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, callback func(*types.VersionedStreamingNodeAssignments) error) error {
			for {
				select {
				case update := <-f.updates:
					if err := callback(update); err != nil {
						return err
					}
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	watcher.EXPECT().ReportAssignmentError(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, channel types.PChannelInfo, err error) error {
			assert.True(t, viewerror.AsViewError(err).IsNotPrimary())
			// The retry loop reports again on each attempt against the old term.
			select {
			case f.reported <- channel:
			default:
			}
			return nil
		})
	f.client = NewHandlerClient(watcher).(*handlerClientImpl)
	t.Cleanup(f.client.Close)
	return f
}

func (f *nodeMismatchFixture) publish(version, term, owner int64) {
	nodes := map[int64]types.StreamingNodeAssignment{
		1682: {NodeInfo: types.StreamingNodeInfo{ServerID: 1682, Address: f.address}, Channels: map[string]types.PChannelInfo{}},
		1683: {NodeInfo: types.StreamingNodeInfo{ServerID: 1683, Address: f.address}, Channels: map[string]types.PChannelInfo{}},
	}
	nodes[owner].Channels["p0"] = types.PChannelInfo{Name: "p0", Term: term, AccessMode: types.AccessModeRW}
	f.updates <- &types.VersionedStreamingNodeAssignments{Version: typeutil.VersionInt64Pair{Global: 1, Local: version}, Assignments: nodes}
}

func (f *nodeMismatchFixture) waitForReport(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case channel := <-f.reported:
		require.Equal(t, types.PChannelInfo{Name: "p0", Term: 1, AccessMode: types.AccessModeRW}, channel)
	case <-ctx.Done():
		t.Fatal("old assignment was not reported:", ctx.Err())
	}
}

func (f *nodeMismatchFixture) read(ctx context.Context, method string) error {
	shard := qviews.ShardID{VChannel: f.vchannel, ReplicaID: qviews.UnknownReplicaID}
	if method == "GetMVCCTimestamp" {
		_, err := f.client.QueryViewClient().GetMVCCTimestamp(ctx, shard, &viewpb.GetMVCCTimestampRequest{Vchannel: f.vchannel})
		return err
	}
	_, err := f.client.QueryViewClient().GetQueryPlan(ctx, shard, &viewpb.GetQueryPlanRequest{ShardId: shard.IntoProto()})
	return err
}

type nodeMismatchQueryServer struct {
	viewpb.UnimplementedQueryPlanServiceServer
	viewpb.UnimplementedViewQueryServiceServer
	fixture          *nodeMismatchFixture
	t                *testing.T
	restartAfterPlan bool
}

func (s *nodeMismatchQueryServer) GetQueryPlan(ctx context.Context, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	pchannel, err := worknodehandler.DecodeQueryViewPChannelFromIncomingContext(ctx)
	assert.NoError(s.t, err)
	if s.restartAfterPlan && pchannel.Term == 1 {
		s.fixture.serverID.Store(1683)
	} else {
		assert.EqualValues(s.t, 2, pchannel.Term)
	}
	return &viewpb.GetQueryPlanResponse{Plan: &viewpb.QueryPlan{
		ShardId: &viewpb.ShardID{Vchannel: s.fixture.vchannel, ReplicaId: 22},
		Version: &viewpb.QueryViewVersion{QueryVersion: pchannel.Term},
		Request: &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: req.GetLegacySearchRequest()},
		WorkNodes: []*viewpb.QueryPlanWorkNode{{Node: &viewpb.QueryPlanWorkNode_StreamingNode{
			StreamingNode: &viewpb.StreamingWorkNode{Pchannel: "p0"},
		}}},
	}}, nil
}

func (s *nodeMismatchQueryServer) GetMVCCTimestamp(ctx context.Context, _ *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error) {
	pchannel, err := worknodehandler.DecodeQueryViewPChannelFromIncomingContext(ctx)
	assert.NoError(s.t, err)
	assert.EqualValues(s.t, 2, pchannel.Term)
	return &viewpb.GetMVCCTimestampResponse{}, nil
}

func (s *nodeMismatchQueryServer) SearchOnView(ctx context.Context, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	s.fixture.executions.Add(1)
	pchannel, err := worknodehandler.DecodeQueryViewPChannelFromIncomingContext(ctx)
	assert.NoError(s.t, err)
	assert.EqualValues(s.t, 2, pchannel.Term)
	// The plan produced on term 1 is replayed unchanged.
	assert.EqualValues(s.t, 1, req.GetVersion().GetQueryVersion())
	md, ok := metadata.FromIncomingContext(ctx)
	assert.True(s.t, ok)
	assert.Equal(s.t, []string{"1683"}, md.Get(interceptor.ServerIDKey))
	return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success()}}, nil
}

type recordingQueryPlanClient struct {
	QueryViewClient
	codes []codes.Code
}

func (c *recordingQueryPlanClient) SubscribeReplicas(_ string, handler wal.ReplicaWatchHandler) (wal.ReplicaWatchSubscription, error) {
	subscription := staticReplicaSubscription{}
	handler(subscription.Snapshot())
	return subscription, nil
}

type staticReplicaSubscription struct{}

func (staticReplicaSubscription) Snapshot() qviews.ReplicaSnapshot {
	return qviews.ReplicaSnapshot{Valid: true, Replicas: []qviews.Replica{{ReplicaID: 22}}}
}

func (staticReplicaSubscription) Close() error { return nil }

func (c *recordingQueryPlanClient) GetQueryPlan(ctx context.Context, shard qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	resp, err := c.QueryViewClient.GetQueryPlan(ctx, shard, req)
	c.codes = append(c.codes, status.Code(err))
	return resp, err
}

type retryShardResolver struct{ vchannel string }

func (r *retryShardResolver) ResolveVChannels(context.Context, int64) ([]string, error) {
	return []string{r.vchannel}, nil
}
