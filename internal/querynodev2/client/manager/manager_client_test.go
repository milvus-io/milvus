package manager

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	grpcresolver "google.golang.org/grpc/resolver"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/attributes"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	streamingresolver "github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestGetAllQueryNodesReturnsSessionNodeInfo(t *testing.T) {
	r := &fakeResolver{state: newQueryNodeVersionedState(1, map[int64]queryNodeSessionInfo{
		10: {},
		20: {stopping: true, labels: map[string]string{sessionutil.LabelResourceGroup: "rg-from-session"}},
	})}
	m := &managerClientImpl{
		lifetime: typeutil.NewLifetime(),
		stopped:  make(chan struct{}),
		rb:       &fakeResolverBuilder{resolver: r},
		service:  &fakeViewSyncService{},
	}

	nodes, err := m.GetAllQueryNodes(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, map[int64]*NodeInfo{
		10: {ServerID: 10, Address: "localhost:10"},
		20: {ServerID: 20, Address: "localhost:20", Stopping: true, ServerLabels: map[string]string{sessionutil.LabelResourceGroup: "rg-from-session"}},
	}, nodes)
}

func TestCreateViewSyncClientRoutesByQueryNodeID(t *testing.T) {
	baseClient := &capturingViewSyncServiceClient{}
	m := &managerClientImpl{
		lifetime: typeutil.NewLifetime(),
		stopped:  make(chan struct{}),
		rb:       &fakeResolverBuilder{},
		service:  &fakeViewSyncService{client: baseClient},
	}

	queryNodeID := int64(101)
	client, err := m.CreateViewSyncClient(context.Background(), queryNodeID)
	assert.NoError(t, err)

	stream, err := client.SyncQueryView(context.Background())
	assert.NoError(t, err)
	assert.Nil(t, stream)
	picked, ok := contextutil.GetPickServerID(baseClient.queryCtx)
	assert.True(t, ok)
	assert.Equal(t, queryNodeID, picked)

	resp, err := client.SyncDataView(context.Background(), &viewpb.SyncDataViewRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	picked, ok = contextutil.GetPickServerID(baseClient.dataCtx)
	assert.True(t, ok)
	assert.Equal(t, queryNodeID, picked)
}

func TestRegisterNodeChangedNotifier(t *testing.T) {
	r := &fakeResolver{state: newQueryNodeVersionedState(1, map[int64]queryNodeSessionInfo{
		10: {},
	})}
	m := &managerClientImpl{
		lifetime: typeutil.NewLifetime(),
		stopped:  make(chan struct{}),
		rb:       &fakeResolverBuilder{resolver: r},
		service:  &fakeViewSyncService{},
	}
	defer m.Close()

	var called atomic.Bool
	m.RegisterNodeChangedNotifier(func() {
		called.Store(true)
	})

	assert.Eventually(t, called.Load, time.Second, 10*time.Millisecond)
}

type queryNodeSessionInfo struct {
	stopping bool
	labels   map[string]string
}

func newQueryNodeVersionedState(version int64, sessions map[int64]queryNodeSessionInfo) discoverer.VersionedState {
	state := discoverer.VersionedState{
		Version: typeutil.VersionInt64(version),
		State: grpcresolver.State{
			Addresses: make([]grpcresolver.Address, 0, len(sessions)),
		},
	}
	for serverID, info := range sessions {
		session := &sessionutil.SessionRaw{
			ServerID:     serverID,
			Address:      fmt.Sprintf("localhost:%d", serverID),
			Stopping:     info.stopping,
			ServerLabels: info.labels,
		}
		state.State.Addresses = append(state.State.Addresses, grpcresolver.Address{
			Addr:               session.Address,
			BalancerAttributes: attributes.WithSession(new(attributes.Attributes), session),
		})
	}
	return state
}

type fakeResolverBuilder struct {
	resolver streamingresolver.Resolver
	closed   bool
}

func (b *fakeResolverBuilder) Build(grpcresolver.Target, grpcresolver.ClientConn, grpcresolver.BuildOptions) (grpcresolver.Resolver, error) {
	return nil, nil
}

func (b *fakeResolverBuilder) Scheme() string {
	return "fake-querynode-manager"
}

func (b *fakeResolverBuilder) Resolver() streamingresolver.Resolver {
	return b.resolver
}

func (b *fakeResolverBuilder) Close() {
	b.closed = true
}

type fakeResolver struct {
	state discoverer.VersionedState
}

func (r *fakeResolver) GetLatestState(context.Context) (streamingresolver.VersionedState, error) {
	return r.state, nil
}

func (r *fakeResolver) Watch(ctx context.Context, cb func(streamingresolver.VersionedState) error) error {
	return cb(r.state)
}

type fakeViewSyncService struct {
	client viewpb.ViewSyncServiceClient
	closed bool
}

func (s *fakeViewSyncService) GetConn(context.Context) (*grpc.ClientConn, error) {
	return nil, nil
}

func (s *fakeViewSyncService) Close() {
	s.closed = true
}

func (s *fakeViewSyncService) GetService(context.Context) (viewpb.ViewSyncServiceClient, error) {
	return s.client, nil
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
