package streaming

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestBalancer(t *testing.T) {
	mockey.PatchConvey("WAL management does not fall back to QueryCoord", t, func() {
		var readyErr, nodesErr, assignmentErr, updateErr error
		mockey.Mock(balancerImpl.checkIfStreamingServiceReady).To(func(balancerImpl, context.Context) error { return readyErr }).Build()
		native := &mock_balancer.MockBalancer{}
		mockey.Mock((*snmanager.StreamingNodeManager).GetBalancer).Return(native).Build()
		mockey.Mock((*mock_balancer.MockBalancer).GetAllStreamingNodes).To(func(*mock_balancer.MockBalancer, context.Context) (map[int64]*types.StreamingNodeInfoWithResourceGroup, error) {
			return map[int64]*types.StreamingNodeInfoWithResourceGroup{
				1: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 1}},
				2: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 2}},
			}, nodesErr
		}).Build()
		mockey.Mock((*mock_balancer.MockBalancer).GetLatestChannelAssignment).To(func(*mock_balancer.MockBalancer) (*balancer.WatchChannelAssignmentsCallbackParam, error) {
			return &balancer.WatchChannelAssignmentsCallbackParam{Relations: []types.PChannelInfoAssigned{
				{Node: types.StreamingNodeInfo{ServerID: 1}, Channel: types.PChannelInfo{Name: "p1"}},
			}}, assignmentErr
		}).Build()
		var last *types.UpdateWALBalancePolicyRequest
		mockey.Mock((*mock_balancer.MockBalancer).UpdateBalancePolicy).To(func(_ *mock_balancer.MockBalancer, _ context.Context, req *types.UpdateWALBalancePolicyRequest) (*types.UpdateWALBalancePolicyResponse, error) {
			last = proto.Clone(req).(*types.UpdateWALBalancePolicyRequest)
			return &streamingpb.UpdateWALBalancePolicyResponse{FreezeNodeIds: []int64{2}}, updateErr
		}).Build()
		b := balancerImpl{}
		ctx := t.Context()
		nodes, err := b.ListStreamingNode(ctx)
		require.NoError(t, err)
		require.Len(t, nodes, 2)
		dist, err := b.GetWALDistribution(ctx, 1)
		require.NoError(t, err)
		require.Contains(t, dist.Channels, "p1")
		dist, err = b.GetWALDistribution(ctx, 2)
		require.NoError(t, err)
		require.Equal(t, int64(2), dist.NodeInfo.ServerID)
		require.Empty(t, dist.Channels)
		_, err = b.GetWALDistribution(ctx, 99)
		require.ErrorIs(t, err, merr.ErrNodeNotFound)

		require.NoError(t, b.FreezeNodeIDs(ctx, []int64{1}))
		require.Equal(t, []int64{1}, last.Nodes.FreezeNodeIds)
		require.NotNil(t, last.UpdateMask)
		require.Empty(t, last.UpdateMask.Paths)
		require.NoError(t, b.DefreezeNodeIDs(ctx, []int64{1}))
		require.Equal(t, []int64{1}, last.Nodes.DefreezeNodeIds)
		frozen, err := b.GetFrozenNodeIDs(ctx)
		require.NoError(t, err)
		require.Equal(t, []int64{2}, frozen)

		nodesErr = merr.ErrServiceUnavailable
		_, err = b.ListStreamingNode(ctx)
		require.ErrorIs(t, err, nodesErr)
		_, err = b.GetWALDistribution(ctx, 1)
		require.ErrorIs(t, err, nodesErr)
		nodesErr = nil
		assignmentErr = merr.ErrServiceUnavailable
		_, err = b.GetWALDistribution(ctx, 1)
		require.ErrorIs(t, err, assignmentErr)
		updateErr = merr.ErrServiceUnavailable
		require.ErrorIs(t, b.FreezeNodeIDs(ctx, []int64{1}), updateErr)
		require.ErrorIs(t, b.DefreezeNodeIDs(ctx, []int64{1}), updateErr)
		_, err = b.GetFrozenNodeIDs(ctx)
		require.ErrorIs(t, err, updateErr)

		readyErr = snmanager.ErrStreamingServiceNotReady
		_, err = b.ListStreamingNode(ctx)
		require.ErrorIs(t, err, readyErr)
		_, err = b.GetWALDistribution(ctx, 1)
		require.ErrorIs(t, err, readyErr)
		_, err = b.GetFrozenNodeIDs(ctx)
		require.ErrorIs(t, err, readyErr)
		require.ErrorIs(t, b.FreezeNodeIDs(ctx, []int64{1}), readyErr)
		require.ErrorIs(t, b.DefreezeNodeIDs(ctx, []int64{1}), readyErr)
	})
}
