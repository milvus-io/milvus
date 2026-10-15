package snmanager

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestWALLocationUnavailableIsNotChannelRetirement(t *testing.T) {
	b := &mock_balancer.MockBalancer{}
	ready := mockey.Mock(balance.TryGet).Return(b, true).Build()
	defer ready.UnPatch()
	assignment := mockey.Mock(mockey.GetMethod(b, "GetLatestWALLocated")).Return(int64(0), false).Build()
	defer assignment.UnPatch()
	m := &StreamingNodeManager{}
	_, err := m.GetLatestWALLocated(context.Background(), "p1_1v0")
	require.ErrorIs(t, err, merr.ErrChannelNotAvailable)
	require.NotErrorIs(t, err, merr.ErrChannelNotFound)
	assignment.Return(int64(12), true)
	owner, err := m.GetLatestWALLocated(context.Background(), "p1_1v0")
	require.NoError(t, err)
	require.Equal(t, int64(12), owner)
	ready.Return(nil, false)
	_, err = m.GetLatestWALLocated(context.Background(), "p1_1v0")
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = m.GetLatestWALLocated(ctx, "p1_1v0")
	require.ErrorIs(t, err, context.Canceled)
}

func TestWALPlanningSnapshotsDoNotWaitForBalancerRegistration(t *testing.T) {
	ResetStreamingNodeManager()
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, _ balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	b.EXPECT().GetLatestWALLocated(mock.Anything, "pending").Return(int64(9), true).Maybe()
	b.EXPECT().GetAvailableStreamingNodes(mock.Anything).Return(map[int64]*types.StreamingNodeInfoWithResourceGroup{
		9: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 9}},
	}, nil).Maybe()
	var register sync.Once
	done := make(chan struct{})
	var snapshotErr error
	var initialNodes int
	t.Cleanup(func() {
		// Release even the old blocking implementation if the assertion fails.
		register.Do(func() { balance.Register(b) })
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("snapshot lookup did not complete after registration")
		}
		ResetStreamingNodeManager()
	})
	go func() {
		_, snapshotErr = StaticStreamingNodeManager.GetLatestWALLocated(context.Background(), "pending_1v0")
		initialNodes = StaticStreamingNodeManager.GetStreamingQueryNodeIDs().Len()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("WAL/node snapshot waited for balancer registration")
	}
	require.ErrorIs(t, snapshotErr, merr.ErrServiceUnavailable)
	require.Zero(t, initialNodes)
	register.Do(func() { balance.Register(b) })
	owner, err := StaticStreamingNodeManager.GetLatestWALLocated(context.Background(), "pending_1v0")
	require.NoError(t, err)
	require.EqualValues(t, 9, owner)
	require.True(t, StaticStreamingNodeManager.GetStreamingQueryNodeIDs().Contain(9))
}

func TestStreamingNodeSnapshotTimeoutPreservesCachedNodes(t *testing.T) {
	b := mock_balancer.NewMockBalancer(t)
	ready := mockey.Mock(balance.TryGet).Return(b, true).Build()
	defer ready.UnPatch()
	b.EXPECT().GetAvailableStreamingNodes(mock.Anything).RunAndReturn(func(ctx context.Context) (map[int64]*types.StreamingNodeInfoWithResourceGroup, error) {
		if _, bounded := ctx.Deadline(); !bounded {
			t.Error("node discovery snapshot must have a deadline")
			return nil, context.Canceled
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}).Once()
	m := &StreamingNodeManager{previousNodesByRG: map[int64]*types.StreamingNodeInfoWithResourceGroup{
		7: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 7}, ResourceGroup: "cached"},
	}}
	for i := 0; i < 20; i++ {
		nodes := m.GetStreamingQueryNodeIDs()
		require.True(t, nodes.Contain(7), "a slow resolver must not erase the last known node pool")
	}
	// All candidate groups above share one failed discovery attempt. Once the
	// retry interval expires, a successful sample replaces the cached pool.
	m.nodesRetryAfter = time.Time{}
	b.EXPECT().GetAvailableStreamingNodes(mock.Anything).Unset()
	b.EXPECT().GetAvailableStreamingNodes(mock.Anything).Return(map[int64]*types.StreamingNodeInfoWithResourceGroup{
		9: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 9}},
	}, nil).Once()
	nodes := m.GetStreamingQueryNodeIDs()
	require.True(t, nodes.Contain(9))
	require.False(t, nodes.Contain(7))
}
