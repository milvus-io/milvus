package manager

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"

	"github.com/milvus-io/milvus/internal/mocks/util/streamingutil/service/mock_lazygrpc"
	"github.com/milvus-io/milvus/internal/mocks/util/streamingutil/service/mock_resolver"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/attributes"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	streamingserviceinterceptor "github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	sresolver "github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	pkginterceptor "github.com/milvus-io/milvus/pkg/v3/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	r.EXPECT().GetLatestState(mock.Anything).RunAndReturn(func(ctx context.Context) (discoverer.VersionedState, error) {
		return discoverer.VersionedState{
			Version: typeutil.VersionInt64(1),
			State:   resolver.State{},
		}, nil
	})
	// Not address here.
	nodes, err := m.CollectAllStatus(context.Background(), "")
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
		{1: true, 2: false},
		{1: true, 2: false},
	}
	r.EXPECT().GetLatestState(mock.Anything).Unset()
	r.EXPECT().GetLatestState(mock.Anything).RunAndReturn(func(ctx context.Context) (discoverer.VersionedState, error) {
		s := newVersionedState(int64(i), states[i])
		i++
		return s, nil
	})

	nodes, err = m.CollectAllStatus(context.Background(), "")
	assert.NoError(t, err)
	assert.Len(t, nodes, 3)
	assert.ErrorIs(t, nodes[3].Err, types.ErrNotAlive)
	assert.ErrorIs(t, nodes[1].Err, types.ErrStopping)

	nodeInfos, err := m.GetAllStreamingNodes(context.Background())
	assert.NoError(t, err)
	assert.Len(t, nodeInfos, 2)

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
		}).Once()
	err = m.Remove(context.Background(), types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "p", Term: 1},
		Node:    types.StreamingNodeInfo{ServerID: serverID},
	})
	assert.NoError(t, err)

	managerServiceClient.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil, picker.ErrSubConnNotExist).Once()
	err = m.Remove(context.Background(), types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "p", Term: 1},
		Node:    types.StreamingNodeInfo{ServerID: serverID},
	})
	assert.ErrorIs(t, err, types.ErrNotAlive)

	managerServiceClient.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil, status.NewIgnoreOperation("not the owner of the wal")).Once()
	err = m.Remove(context.Background(), types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "p", Term: 1},
		Node:    types.StreamingNodeInfo{ServerID: serverID},
	})
	assert.NoError(t, err)

	removeErr := errors.New("remove failed")
	managerServiceClient.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil, removeErr).Once()
	err = m.Remove(context.Background(), types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "p", Term: 1},
		Node:    types.StreamingNodeInfo{ServerID: serverID},
	})
	assert.ErrorIs(t, err, removeErr)

	// Test Close
	managerService.EXPECT().Close().Return()
	rb.EXPECT().Close().Return()
	m.Close()

	nodeInfos, err = m.GetAllStreamingNodes(context.Background())
	assert.Nil(t, nodeInfos)
	assert.Error(t, err)
	nodes, err = m.CollectAllStatus(context.Background(), "")
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

type serverInfo struct {
	stopping      bool
	resourceGroup string
}

func newVersionedState(version int64, serverIDs map[uint64]bool) discoverer.VersionedState {
	infos := make(map[uint64]serverInfo, len(serverIDs))
	for id, stopping := range serverIDs {
		infos[id] = serverInfo{stopping: stopping}
	}
	return newVersionedStateWithRG(version, infos)
}

func newVersionedStateWithRG(version int64, servers map[uint64]serverInfo) discoverer.VersionedState {
	state := discoverer.VersionedState{
		Version: typeutil.VersionInt64(version),
		State: resolver.State{
			Addresses: make([]resolver.Address, 0, len(servers)),
		},
	}

	for serverID, info := range servers {
		session := &sessionutil.SessionRaw{
			ServerID: int64(serverID),
			Stopping: info.stopping,
		}
		if info.resourceGroup != "" {
			session.ServerLabels = map[string]string{
				sessionutil.LabelResourceGroup: info.resourceGroup,
			}
		}
		state.State.Addresses = append(state.State.Addresses, resolver.Address{
			Addr:               fmt.Sprintf("localhost:%d", serverID),
			BalancerAttributes: attributes.WithSession(new(attributes.Attributes), session),
		})
	}
	return state
}

func TestGetAllStreamingNodesWithResourceGroup(t *testing.T) {
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

	// Return sessions with resource group labels
	r.EXPECT().GetLatestState(mock.Anything).RunAndReturn(func(ctx context.Context) (discoverer.VersionedState, error) {
		return newVersionedStateWithRG(1, map[uint64]serverInfo{
			1: {resourceGroup: "rg_a"},
			2: {resourceGroup: "rg_b"},
			3: {resourceGroup: ""}, // empty RG should default to __default_resource_group
		}), nil
	})

	nodes, err := m.GetAllStreamingNodes(context.Background())
	assert.NoError(t, err)
	assert.Len(t, nodes, 3)
	assert.Equal(t, "rg_a", nodes[1].ResourceGroup)
	assert.Equal(t, "rg_b", nodes[2].ResourceGroup)
	assert.Equal(t, "__default_resource_group", nodes[3].ResourceGroup)

	managerService.EXPECT().Close().Return()
	rb.EXPECT().Close().Return()
	m.Close()
}

func TestCollectAllStatusWithResourceGroupFilter(t *testing.T) {
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

	managerServiceClient := mock_streamingpb.NewMockStreamingNodeManagerServiceClient(t)
	managerService.EXPECT().GetService(mock.Anything).RunAndReturn(func(ctx context.Context) (streamingpb.StreamingNodeManagerServiceClient, error) {
		return managerServiceClient, nil
	})
	managerServiceClient.EXPECT().CollectStatus(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, snmcsr *streamingpb.StreamingNodeManagerCollectStatusRequest, co ...grpc.CallOption) (*streamingpb.StreamingNodeManagerCollectStatusResponse, error) {
		return &streamingpb.StreamingNodeManagerCollectStatusResponse{}, nil
	})

	// Two calls to GetLatestState: first for initial state, second for re-check
	callCount := 0
	r.EXPECT().GetLatestState(mock.Anything).RunAndReturn(func(ctx context.Context) (discoverer.VersionedState, error) {
		callCount++
		return newVersionedStateWithRG(int64(callCount), map[uint64]serverInfo{
			1: {resourceGroup: "rg_a"},
			2: {resourceGroup: "rg_a"},
			3: {resourceGroup: "rg_b"},
			4: {resourceGroup: ""}, // defaults to __default_resource_group
		}), nil
	})

	// Hint rg_a - should only return status from servers 1 and 2 while rg_a is available.
	nodes, err := m.CollectAllStatus(context.Background(), "rg_a")
	assert.NoError(t, err)
	assert.Len(t, nodes, 2)
	assert.Contains(t, nodes, int64(1))
	assert.Contains(t, nodes, int64(2))
	assert.NotContains(t, nodes, int64(3))
	assert.NotContains(t, nodes, int64(4))

	// Empty resource group falls back to __default_resource_group when unlabeled legacy nodes exist.
	callCount = 0
	nodes, err = m.CollectAllStatus(context.Background(), "")
	assert.NoError(t, err)
	assert.Len(t, nodes, 1)
	assert.Contains(t, nodes, int64(4))

	// Hint __default_resource_group - should only return status from server 4.
	callCount = 0
	nodes, err = m.CollectAllStatus(context.Background(), "__default_resource_group")
	assert.NoError(t, err)
	assert.Len(t, nodes, 1)
	assert.Contains(t, nodes, int64(4))

	// Unavailable hint falls back to the RG with the most available nodes.
	callCount = 0
	nodes, err = m.CollectAllStatus(context.Background(), "non_existent_rg")
	assert.NoError(t, err)
	assert.Len(t, nodes, 2)
	assert.Contains(t, nodes, int64(1))
	assert.Contains(t, nodes, int64(2))

	managerService.EXPECT().Close().Return()
	rb.EXPECT().Close().Return()
	m.Close()
}

func TestCollectAllStatusFallbackWhenHintUnavailable(t *testing.T) {
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

	managerServiceClient := mock_streamingpb.NewMockStreamingNodeManagerServiceClient(t)
	managerService.EXPECT().GetService(mock.Anything).RunAndReturn(func(ctx context.Context) (streamingpb.StreamingNodeManagerServiceClient, error) {
		return managerServiceClient, nil
	})
	var mu sync.Mutex
	pickedServerIDs := make([]int64, 0, 2)
	managerServiceClient.EXPECT().CollectStatus(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, snmcsr *streamingpb.StreamingNodeManagerCollectStatusRequest, co ...grpc.CallOption) (*streamingpb.StreamingNodeManagerCollectStatusResponse, error) {
		serverID, ok := contextutil.GetPickServerID(ctx)
		assert.True(t, ok)
		mu.Lock()
		pickedServerIDs = append(pickedServerIDs, serverID)
		mu.Unlock()
		return &streamingpb.StreamingNodeManagerCollectStatusResponse{}, nil
	})

	callCount := 0
	r.EXPECT().GetLatestState(mock.Anything).RunAndReturn(func(ctx context.Context) (discoverer.VersionedState, error) {
		callCount++
		return newVersionedStateWithRG(int64(callCount), map[uint64]serverInfo{
			2: {resourceGroup: "rg_a"},
			3: {resourceGroup: "rg_a"},
			4: {resourceGroup: "rg_b"},
		}), nil
	})

	nodes, err := m.CollectAllStatus(context.Background(), "rg_primary")
	assert.NoError(t, err)
	assert.Len(t, nodes, 2)
	assert.Contains(t, nodes, int64(2))
	assert.Contains(t, nodes, int64(3))
	assert.ElementsMatch(t, []int64{2, 3}, pickedServerIDs)

	managerService.EXPECT().Close().Return()
	rb.EXPECT().Close().Return()
	m.Close()
}

func TestCollectAllStatusFallbackTieBreakByMaxServerID(t *testing.T) {
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

	managerServiceClient := mock_streamingpb.NewMockStreamingNodeManagerServiceClient(t)
	managerService.EXPECT().GetService(mock.Anything).RunAndReturn(func(ctx context.Context) (streamingpb.StreamingNodeManagerServiceClient, error) {
		return managerServiceClient, nil
	})
	managerServiceClient.EXPECT().CollectStatus(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, snmcsr *streamingpb.StreamingNodeManagerCollectStatusRequest, co ...grpc.CallOption) (*streamingpb.StreamingNodeManagerCollectStatusResponse, error) {
		return &streamingpb.StreamingNodeManagerCollectStatusResponse{}, nil
	})

	callCount := 0
	r.EXPECT().GetLatestState(mock.Anything).RunAndReturn(func(ctx context.Context) (discoverer.VersionedState, error) {
		callCount++
		return newVersionedStateWithRG(int64(callCount), map[uint64]serverInfo{
			1: {resourceGroup: "rg_a"},
			5: {resourceGroup: "rg_b"},
		}), nil
	})

	nodes, err := m.CollectAllStatus(context.Background(), "non_existent_rg")
	assert.NoError(t, err)
	assert.Len(t, nodes, 1)
	assert.Contains(t, nodes, int64(5))

	managerService.EXPECT().Close().Return()
	rb.EXPECT().Close().Return()
	m.Close()
}

func TestCollectAllStatusWithEmptyResourceGroupAndExplicitDefaultRG(t *testing.T) {
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

	managerServiceClient := mock_streamingpb.NewMockStreamingNodeManagerServiceClient(t)
	managerService.EXPECT().GetService(mock.Anything).RunAndReturn(func(ctx context.Context) (streamingpb.StreamingNodeManagerServiceClient, error) {
		return managerServiceClient, nil
	})
	managerServiceClient.EXPECT().CollectStatus(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, snmcsr *streamingpb.StreamingNodeManagerCollectStatusRequest, co ...grpc.CallOption) (*streamingpb.StreamingNodeManagerCollectStatusResponse, error) {
		return &streamingpb.StreamingNodeManagerCollectStatusResponse{}, nil
	})

	callCount := 0
	r.EXPECT().GetLatestState(mock.Anything).RunAndReturn(func(ctx context.Context) (discoverer.VersionedState, error) {
		callCount++
		return newVersionedStateWithRG(int64(callCount), map[uint64]serverInfo{
			1: {resourceGroup: "__default_resource_group"},
			2: {resourceGroup: "rg_a"},
		}), nil
	})

	nodes, err := m.CollectAllStatus(context.Background(), "")
	assert.NoError(t, err)
	assert.Len(t, nodes, 1)
	assert.Contains(t, nodes, int64(1))
	assert.NotContains(t, nodes, int64(2))

	managerService.EXPECT().Close().Return()
	rb.EXPECT().Close().Return()
	m.Close()
}

func TestCollectAllStatusWithEmptyResourceGroupAndDefaultRGAbsent(t *testing.T) {
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

	managerServiceClient := mock_streamingpb.NewMockStreamingNodeManagerServiceClient(t)
	managerService.EXPECT().GetService(mock.Anything).RunAndReturn(func(ctx context.Context) (streamingpb.StreamingNodeManagerServiceClient, error) {
		return managerServiceClient, nil
	})
	managerServiceClient.EXPECT().CollectStatus(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, snmcsr *streamingpb.StreamingNodeManagerCollectStatusRequest, co ...grpc.CallOption) (*streamingpb.StreamingNodeManagerCollectStatusResponse, error) {
		return &streamingpb.StreamingNodeManagerCollectStatusResponse{}, nil
	})

	callCount := 0
	r.EXPECT().GetLatestState(mock.Anything).RunAndReturn(func(ctx context.Context) (discoverer.VersionedState, error) {
		callCount++
		return newVersionedStateWithRG(int64(callCount), map[uint64]serverInfo{
			1: {resourceGroup: "rg_a"},
			2: {resourceGroup: "rg_b"},
		}), nil
	})

	nodes, err := m.CollectAllStatus(context.Background(), "")
	assert.NoError(t, err)
	assert.Len(t, nodes, 2)
	assert.Contains(t, nodes, int64(1))
	assert.Contains(t, nodes, int64(2))

	managerService.EXPECT().Close().Return()
	rb.EXPECT().Close().Return()
	m.Close()
}

func TestDial(t *testing.T) {
	paramtable.Init()

	c, _ := kvfactory.GetEtcdAndPath()
	assert.NotNil(t, c)

	client := NewManagerClient(c)
	assert.NotNil(t, client)
	time.Sleep(100 * time.Millisecond)
	client.Close()
}

func TestCollectAllStatusFallbackWhenPrimaryUnhealthyOrUnreachable(t *testing.T) {
	newClient := func() (*managerClientImpl, *mock_resolver.MockResolver, *mock_streamingpb.MockStreamingNodeManagerServiceClient) {
		rb := mock_resolver.NewMockBuilder(t)
		managerService := mock_lazygrpc.NewMockService[streamingpb.StreamingNodeManagerServiceClient](t)
		m := &managerClientImpl{
			lifetime:     typeutil.NewLifetime(),
			stopped:      make(chan struct{}),
			rb:           rb,
			service:      managerService,
			probeTimeout: 100 * time.Millisecond,
		}
		r := mock_resolver.NewMockResolver(t)
		rb.EXPECT().Resolver().Return(r)
		client := mock_streamingpb.NewMockStreamingNodeManagerServiceClient(t)
		managerService.EXPECT().GetService(mock.Anything).Return(client, nil)
		t.Cleanup(func() {
			managerService.EXPECT().Close().Return()
			rb.EXPECT().Close().Return()
			m.Close()
		})
		return m, r, client
	}

	t.Run("fallback to backup rg when primary returns rpc error", func(t *testing.T) {
		m, r, client := newClient()
		r.EXPECT().GetLatestState(mock.Anything).Return(newVersionedStateWithRG(1, map[uint64]serverInfo{
			7: {resourceGroup: "rg1"},
			6: {resourceGroup: "rg2"},
		}), nil)

		var probed []int64
		var mu sync.Mutex
		client.EXPECT().CollectStatus(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, _ *streamingpb.StreamingNodeManagerCollectStatusRequest, _ ...grpc.CallOption) (*streamingpb.StreamingNodeManagerCollectStatusResponse, error) {
				id, ok := contextutil.GetPickServerID(ctx)
				require.True(t, ok)
				mu.Lock()
				probed = append(probed, id)
				mu.Unlock()
				if id == 7 {
					return nil, status.NewIgnoreOperation("node not match")
				}
				return &streamingpb.StreamingNodeManagerCollectStatusResponse{}, nil
			})

		nodes, err := m.CollectAllStatus(context.Background(), "rg1")
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		require.Contains(t, nodes, int64(6))
		require.NoError(t, nodes[6].Err)
		require.Equal(t, []int64{7, 6}, probed)
	})

	t.Run("fallback to backup rg when primary has only stopping nodes", func(t *testing.T) {
		m, r, client := newClient()
		calls := 0
		r.EXPECT().GetLatestState(mock.Anything).RunAndReturn(func(context.Context) (discoverer.VersionedState, error) {
			calls++
			if calls == 1 {
				return newVersionedStateWithRG(1, map[uint64]serverInfo{
					7: {resourceGroup: "rg1"},
					6: {resourceGroup: "rg2"},
				}), nil
			}
			return newVersionedStateWithRG(2, map[uint64]serverInfo{
				7: {resourceGroup: "rg1", stopping: true},
				6: {resourceGroup: "rg2"},
			}), nil
		})
		client.EXPECT().CollectStatus(mock.Anything, mock.Anything).Return(&streamingpb.StreamingNodeManagerCollectStatusResponse{}, nil)

		nodes, err := m.CollectAllStatus(context.Background(), "rg1")
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		require.Contains(t, nodes, int64(6))
		require.NoError(t, nodes[6].Err)
	})

	t.Run("keep primary rg when at least one primary node is healthy", func(t *testing.T) {
		m, r, client := newClient()
		r.EXPECT().GetLatestState(mock.Anything).Return(newVersionedStateWithRG(1, map[uint64]serverInfo{
			7: {resourceGroup: "rg1"},
			8: {resourceGroup: "rg1"},
			6: {resourceGroup: "rg2"},
		}), nil)

		var probed []int64
		var mu sync.Mutex
		client.EXPECT().CollectStatus(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, _ *streamingpb.StreamingNodeManagerCollectStatusRequest, _ ...grpc.CallOption) (*streamingpb.StreamingNodeManagerCollectStatusResponse, error) {
				id, ok := contextutil.GetPickServerID(ctx)
				require.True(t, ok)
				mu.Lock()
				probed = append(probed, id)
				mu.Unlock()
				if id == 7 {
					return nil, errors.New("unreachable")
				}
				return &streamingpb.StreamingNodeManagerCollectStatusResponse{}, nil
			})

		nodes, err := m.CollectAllStatus(context.Background(), "rg1")
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		require.Contains(t, nodes, int64(8))
		require.NotContains(t, probed, int64(6))
	})

	t.Run("return empty without hanging when all rgs fail", func(t *testing.T) {
		m, r, client := newClient()
		r.EXPECT().GetLatestState(mock.Anything).Return(newVersionedStateWithRG(1, map[uint64]serverInfo{
			7: {resourceGroup: "rg1"},
			6: {resourceGroup: "rg2"},
		}), nil)
		client.EXPECT().CollectStatus(mock.Anything, mock.Anything).Return(nil, errors.New("unreachable"))

		nodes, err := m.CollectAllStatus(context.Background(), "rg1")
		require.NoError(t, err)
		require.Empty(t, nodes)
	})
}

type fakeStreamingManagerServer struct {
	streamingpb.UnimplementedStreamingNodeManagerServiceServer
	collectCalls atomic.Int64
	removeCalls  atomic.Int64
}

func (s *fakeStreamingManagerServer) CollectStatus(context.Context, *streamingpb.StreamingNodeManagerCollectStatusRequest) (*streamingpb.StreamingNodeManagerCollectStatusResponse, error) {
	s.collectCalls.Add(1)
	return &streamingpb.StreamingNodeManagerCollectStatusResponse{}, nil
}

func (s *fakeStreamingManagerServer) Remove(context.Context, *streamingpb.StreamingNodeManagerRemoveRequest) (*streamingpb.StreamingNodeManagerRemoveResponse, error) {
	s.removeCalls.Add(1)
	return &streamingpb.StreamingNodeManagerRemoveResponse{}, nil
}

type testManagerResolverBuilder struct {
	mu      sync.RWMutex
	state   discoverer.VersionedState
	cc      resolver.ClientConn
	version int64
}

func (b *testManagerResolverBuilder) Build(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	b.mu.Lock()
	b.cc = cc
	state := b.state
	b.mu.Unlock()
	if len(state.State.Addresses) > 0 {
		_ = cc.UpdateState(state.State)
	}
	return b, nil
}

func (b *testManagerResolverBuilder) Scheme() string {
	return "test-sn-manager"
}

func (b *testManagerResolverBuilder) ResolveNow(resolver.ResolveNowOptions) {}

func (b *testManagerResolverBuilder) Close() {}

func (b *testManagerResolverBuilder) Resolver() sresolver.Resolver {
	return b
}

func (b *testManagerResolverBuilder) GetLatestState(context.Context) (discoverer.VersionedState, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.state, nil
}

func (b *testManagerResolverBuilder) Watch(ctx context.Context, _ func(discoverer.VersionedState) error) error {
	<-ctx.Done()
	return ctx.Err()
}

func (b *testManagerResolverBuilder) UpdateServers(servers map[uint64]struct {
	addr          string
	resourceGroup string
},
) {
	b.mu.Lock()
	b.version++
	addrs := make([]resolver.Address, 0, len(servers))
	for serverID, info := range servers {
		session := &sessionutil.SessionRaw{
			ServerID: int64(serverID),
			Address:  info.addr,
			ServerLabels: map[string]string{
				sessionutil.LabelResourceGroup: info.resourceGroup,
			},
		}
		addrs = append(addrs, resolver.Address{
			Addr:               info.addr,
			Attributes:         attributes.WithServerID(new(attributes.Attributes), int64(serverID)),
			BalancerAttributes: attributes.WithSession(new(attributes.Attributes), session),
		})
	}
	state := discoverer.VersionedState{
		Version: typeutil.VersionInt64(b.version),
		State:   resolver.State{Addresses: addrs},
	}
	b.state = state
	cc := b.cc
	b.mu.Unlock()
	if cc != nil {
		_ = cc.UpdateState(state.State)
	}
}

func startTestSNManagerServer(t *testing.T, serverID int64) (string, *fakeStreamingManagerServer) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	svc := &fakeStreamingManagerServer{}
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			streamingserviceinterceptor.NewServiceUnaryServerInterceptor(),
			pkginterceptor.ServerIDValidationUnaryServerInterceptor(func() int64 { return serverID }),
		),
	)
	streamingpb.RegisterStreamingNodeManagerServiceServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})
	return lis.Addr().String(), svc
}

func newClosedTCPAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	return addr
}

func TestCollectAllStatusAndRemoveRealGRPC(t *testing.T) {
	paramtable.Init()

	rb := &testManagerResolverBuilder{}
	dialTimeout := paramtable.Get().StreamingNodeGrpcClientCfg.DialTimeout.GetAsDuration(time.Millisecond)
	dialOptions := getDialOptions(rb)
	conn := lazygrpc.NewConn(func(ctx context.Context) (*grpc.ClientConn, error) {
		ctx, cancel := context.WithTimeout(ctx, dialTimeout)
		defer cancel()
		return grpc.DialContext(ctx, rb.Scheme()+":///streamingnode", dialOptions...)
	})
	m := &managerClientImpl{
		lifetime:     typeutil.NewLifetime(),
		stopped:      make(chan struct{}),
		rb:           rb,
		service:      lazygrpc.WithServiceCreator(conn, streamingpb.NewStreamingNodeManagerServiceClient),
		probeTimeout: 150 * time.Millisecond,
	}
	defer m.Close()

	rg2Addr, rg2Svc := startTestSNManagerServer(t, 6)
	deadAddr := newClosedTCPAddr(t)

	// 1. Stale primary RG1 registration pointing to an unreachable port while RG2 is healthy:
	// CollectAllStatus must fail over to RG2 after probeTimeout without blocking indefinitely.
	rb.UpdateServers(map[uint64]struct {
		addr          string
		resourceGroup string
	}{
		7: {addr: deadAddr, resourceGroup: "rg1"},
		6: {addr: rg2Addr, resourceGroup: "rg2"},
	})

	start := time.Now()
	nodes, err := m.CollectAllStatus(context.Background(), "rg1")
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Contains(t, nodes, int64(6))
	require.Less(t, elapsed, 2*time.Second)
	require.GreaterOrEqual(t, rg2Svc.collectCalls.Load(), int64(1))

	// 2. Stale primary RG1 registration whose pod IP now serves a restarted StreamingNode (serverID=10):
	// CollectStatus hits ErrNodeNotMatch and must immediately fall back to RG2.
	restartedRG1Addr, restartedRG1Svc := startTestSNManagerServer(t, 10)
	rb.UpdateServers(map[uint64]struct {
		addr          string
		resourceGroup string
	}{
		7: {addr: restartedRG1Addr, resourceGroup: "rg1"},
		6: {addr: rg2Addr, resourceGroup: "rg2"},
	})

	// Remove for stale serverID=7 hitting serverID=10 returns nil via SkippedOperation.
	// Wait briefly until SubConn 7 connects to restartedRG1Addr.
	require.Eventually(t, func() bool {
		removeCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		err := m.Remove(removeCtx, types.PChannelInfoAssigned{
			Channel: types.PChannelInfo{Name: "ch1", Term: 4, AccessMode: types.AccessModeRW},
			Node:    types.StreamingNodeInfo{ServerID: 7, Address: restartedRG1Addr},
		})
		return err == nil
	}, 3*time.Second, 20*time.Millisecond)
	require.Zero(t, restartedRG1Svc.removeCalls.Load())

	fallbackStart := time.Now()
	nodes, err = m.CollectAllStatus(context.Background(), "rg1")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Contains(t, nodes, int64(6))
	require.Less(t, time.Since(fallbackStart), 100*time.Millisecond)

	// 3. Remove for a disappeared session (serverID=99) must return ErrNotAlive immediately:
	// ErrSubConnNotExist is returned as a status error from Pick, so gRPC drops it instead
	// of retrying through the service-level UNAVAILABLE retryPolicy.
	removeStart := time.Now()
	err = m.Remove(context.Background(), types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "ch1", Term: 4, AccessMode: types.AccessModeRW},
		Node:    types.StreamingNodeInfo{ServerID: 99, Address: deadAddr},
	})
	require.ErrorIs(t, err, types.ErrNotAlive)
	require.Less(t, time.Since(removeStart), 200*time.Millisecond)
}
