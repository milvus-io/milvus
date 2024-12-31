package manager

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"

	"github.com/milvus-io/milvus/internal/mocks/util/streamingutil/service/mock_lazygrpc"
	"github.com/milvus-io/milvus/internal/mocks/util/streamingutil/service/mock_resolver"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/attributes"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	"github.com/milvus-io/milvus/pkg/mocks/streaming/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestManager(t *testing.T) {
	rb := mock_resolver.NewMockBuilder(t)
	managerService := mock_lazygrpc.NewMockService[streamingpb.StreamingNodeManagerServiceClient](t)
	m := &managerClientImpl{
		lifetime: typeutil.NewLifetime(),
		stopped:  make(chan struct{}),
		rb:       rb,
		service:  managerService,
	}
	r := mock_resolver.NewMockResolver(t)
	rb.EXPECT().Resolver().Return(r)
	r.EXPECT().Watch(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(discoverer.VersionedState) error) error {
		f(discoverer.VersionedState{})
		return context.Canceled
	})

	// Test WatchNodeChanged.
	resultCh, err := m.WatchNodeChanged(context.Background())
	assert.NoError(t, err)
	_, ok := <-resultCh
	assert.True(t, ok)
	_, ok = <-resultCh
	assert.False(t, ok)

	// Test CollectAllStatus
	r.EXPECT().GetLatestState().RunAndReturn(func() discoverer.VersionedState {
		return discoverer.VersionedState{
			Version: typeutil.VersionInt64(1),
			State:   resolver.State{},
		}
	})
	// Not address here.
	nodes, err := m.CollectAllStatus(context.Background())
	assert.NoError(t, err)
	assert.Len(t, nodes, 0)

	// Has address.
	managerServiceClient := mock_streamingpb.NewMockStreamingNodeManagerServiceClient(t)
	managerService.EXPECT().GetService(mock.Anything).RunAndReturn(func(ctx context.Context) (streamingpb.StreamingNodeManagerServiceClient, error) {
		return managerServiceClient, nil
	})
	managerServiceClient.EXPECT().CollectStatus(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, snmcsr *streamingpb.StreamingNodeManagerCollectStatusRequest, co ...grpc.CallOption) (*streamingpb.StreamingNodeManagerCollectStatusResponse, error) {
		return &streamingpb.StreamingNodeManagerCollectStatusResponse{}, nil
	})

	i := 0
	states := []map[uint64]bool{
		{1: false, 2: false, 3: true},
		{1: true, 2: false},
	}
	r.EXPECT().GetLatestState().Unset()
	r.EXPECT().GetLatestState().RunAndReturn(func() discoverer.VersionedState {
		s := newVersionedState(int64(i), states[i])
		i++
		return s
	})

	nodes, err = m.CollectAllStatus(context.Background())
	assert.NoError(t, err)
	assert.Len(t, nodes, 3)
	assert.ErrorIs(t, nodes[3].Err, types.ErrNotAlive)
	assert.ErrorIs(t, nodes[1].Err, types.ErrStopping)

	// Test Assign
	serverID := int64(2)
	managerServiceClient.EXPECT().Assign(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, snmcsr *streamingpb.StreamingNodeManagerAssignRequest, co ...grpc.CallOption) (*streamingpb.StreamingNodeManagerAssignResponse, error) {
			pickedServerID, ok := contextutil.GetPickServerID(ctx)
			assert.True(t, ok)
			assert.Equal(t, serverID, pickedServerID)
			return nil, nil
		})
	err = m.Assign(context.Background(), types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "p", Term: 1},
		Node:    types.StreamingNodeInfo{ServerID: serverID},
	})
	assert.NoError(t, err)

	// Test Remove
	serverID = int64(2)
	managerServiceClient.EXPECT().Remove(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, snmrr *streamingpb.StreamingNodeManagerRemoveRequest, co ...grpc.CallOption) (*streamingpb.StreamingNodeManagerRemoveResponse, error) {
			pickedServerID, ok := contextutil.GetPickServerID(ctx)
			assert.True(t, ok)
			assert.Equal(t, serverID, pickedServerID)
			return nil, nil
		})
	err = m.Remove(context.Background(), types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "p", Term: 1},
		Node:    types.StreamingNodeInfo{ServerID: serverID},
	})
	assert.NoError(t, err)

	// Test Close
	managerService.EXPECT().Close().Return()
	rb.EXPECT().Close().Return()
	m.Close()

	nodes, err = m.CollectAllStatus(context.Background())
	assert.Nil(t, nodes)
	assert.Error(t, err)
	err = m.Assign(context.Background(), types.PChannelInfoAssigned{})
	assert.Error(t, err)
	err = m.Remove(context.Background(), types.PChannelInfoAssigned{})
	assert.Error(t, err)
	resultCh, err = m.WatchNodeChanged(context.Background())
	assert.Nil(t, resultCh)
	assert.Error(t, err)
}

func newVersionedState(version int64, serverIDs map[uint64]bool) discoverer.VersionedState {
	state := discoverer.VersionedState{
		Version: typeutil.VersionInt64(version),
		State: resolver.State{
			Addresses: make([]resolver.Address, 0, len(serverIDs)),
		},
	}

	for serverID, stopping := range serverIDs {
		state.State.Addresses = append(state.State.Addresses, resolver.Address{
			Addr: fmt.Sprintf("localhost:%d", serverID),
			BalancerAttributes: attributes.WithSession(
				new(attributes.Attributes), &sessionutil.SessionRaw{
					ServerID: int64(serverID),
					Stopping: stopping,
				},
			),
		})
	}
	return state
}

func TestDial(t *testing.T) {
	paramtable.Init()

	err := etcd.InitEtcdServer(true, "", t.TempDir(), "stdout", "info")
	assert.NoError(t, err)
	defer etcd.StopEtcdServer()
	c, err := etcd.GetEmbedEtcdClient()
	assert.NoError(t, err)
	assert.NotNil(t, c)

	client := NewManagerClient(c)
	assert.NotNil(t, client)
	time.Sleep(100 * time.Millisecond)
	client.Close()
}
