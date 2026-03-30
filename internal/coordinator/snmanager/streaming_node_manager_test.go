package snmanager

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type pChannelInfoAssigned struct {
	version   typeutil.VersionInt64Pair
	pchannels []types.PChannelInfoAssigned
}

func TestStreamingNodeManager(t *testing.T) {
	StaticStreamingNodeManager.Close()
	m := newStreamingNodeManager()
	b := mock_balancer.NewMockBalancer(t)

	ch := make(chan pChannelInfoAssigned, 1)
	b.EXPECT().GetAvailableStreamingNodes(mock.Anything).Return(map[int64]*types.StreamingNodeInfoWithResourceGroup{}, nil)
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case p := <-ch:
					cb(balancer.WatchChannelAssignmentsCallbackParam{
						Version:            p.version,
						CChannelAssignment: &streamingpb.CChannelAssignment{Meta: &streamingpb.CChannelMeta{Pchannel: "pchannel"}},
						Relations:          p.pchannels,
					})
				}
			}
		})
	b.EXPECT().RegisterStreamingEnabledNotifier(mock.Anything).Return()
	balance.Register(b)

	streamingNodes := m.GetStreamingQueryNodeIDs()
	assert.Empty(t, streamingNodes)

	ch <- pChannelInfoAssigned{
		version: typeutil.VersionInt64Pair{
			Global: 1,
			Local:  1,
		},
		pchannels: []types.PChannelInfoAssigned{
			{
				Channel: types.PChannelInfo{Name: "a_test", Term: 1},
				Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
			},
		},
	}

	listener := m.ListenNodeChanged()
	err := listener.Wait(context.Background())
	assert.NoError(t, err)

	node := m.GetWALLocated("a_test")
	assert.Equal(t, node, int64(1))

	b.EXPECT().GetAvailableStreamingNodes(mock.Anything).Unset()
	b.EXPECT().GetAvailableStreamingNodes(mock.Anything).Return(map[int64]*types.StreamingNodeInfoWithResourceGroup{
		1: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"}, ResourceGroup: "rg1"},
	}, nil)
	streamingNodes = m.GetStreamingQueryNodeIDs()
	assert.Equal(t, len(streamingNodes), 1)

	assert.NoError(t, m.RegisterStreamingEnabledListener(context.Background(), NewStreamingReadyNotifier()))

	// --- Test GetStreamingQueryNodeIDsByResourceGroup ---
	// Return multiple nodes across multiple resource groups
	b.EXPECT().GetAvailableStreamingNodes(mock.Anything).Unset()
	b.EXPECT().GetAvailableStreamingNodes(mock.Anything).Return(map[int64]*types.StreamingNodeInfoWithResourceGroup{
		1: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"}, ResourceGroup: "rg1"},
		2: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 2, Address: "localhost:2"}, ResourceGroup: "rg1"},
		3: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 3, Address: "localhost:3"}, ResourceGroup: "rg2"},
		4: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 4, Address: "localhost:4"}, ResourceGroup: "rg3"},
	}, nil)

	nodesByRG := m.GetStreamingQueryNodeIDsByResourceGroup()
	assert.Len(t, nodesByRG, 3)
	assert.True(t, nodesByRG["rg1"].Contain(1))
	assert.True(t, nodesByRG["rg1"].Contain(2))
	assert.Equal(t, 2, nodesByRG["rg1"].Len())
	assert.True(t, nodesByRG["rg2"].Contain(3))
	assert.Equal(t, 1, nodesByRG["rg2"].Len())
	assert.True(t, nodesByRG["rg3"].Contain(4))
	assert.Equal(t, 1, nodesByRG["rg3"].Len())

	// Test empty nodes returns empty map
	b.EXPECT().GetAvailableStreamingNodes(mock.Anything).Unset()
	b.EXPECT().GetAvailableStreamingNodes(mock.Anything).Return(map[int64]*types.StreamingNodeInfoWithResourceGroup{}, nil)
	nodesByRG = m.GetStreamingQueryNodeIDsByResourceGroup()
	assert.Len(t, nodesByRG, 0)

	// --- Test fetchStreamingNodes error fallback (cache path) ---
	// Populate the cache with known nodes
	b.EXPECT().GetAvailableStreamingNodes(mock.Anything).Unset()
	b.EXPECT().GetAvailableStreamingNodes(mock.Anything).Return(map[int64]*types.StreamingNodeInfoWithResourceGroup{
		10: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 10, Address: "localhost:10"}, ResourceGroup: "cached_rg"},
	}, nil)
	nodesByRG = m.GetStreamingQueryNodeIDsByResourceGroup()
	assert.Len(t, nodesByRG, 1)
	assert.True(t, nodesByRG["cached_rg"].Contain(10))

	// Simulate error from balancer - should fall back to cached nodes
	b.EXPECT().GetAvailableStreamingNodes(mock.Anything).Unset()
	b.EXPECT().GetAvailableStreamingNodes(mock.Anything).Return(nil, errors.New("balancer shutting down"))
	nodesByRG = m.GetStreamingQueryNodeIDsByResourceGroup()
	assert.Len(t, nodesByRG, 1)
	assert.True(t, nodesByRG["cached_rg"].Contain(10))

	// Also verify GetStreamingQueryNodeIDs uses cache fallback
	streamingNodes = m.GetStreamingQueryNodeIDs()
	assert.Equal(t, 1, streamingNodes.Len())
	assert.True(t, streamingNodes.Contain(10))

	m.Close()
}

func TestStreamingReadyNotifier(t *testing.T) {
	n := NewStreamingReadyNotifier()
	assert.False(t, n.IsReady())
	n.inner.Cancel()
	<-n.Ready()
	assert.True(t, n.IsReady())
	n.Release()
	assert.True(t, n.IsReady())
}
