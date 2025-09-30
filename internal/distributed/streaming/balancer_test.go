package streaming

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestBalancer(t *testing.T) {
	paramtable.SetLocalComponentEnabled(typeutil.MixCoordRole)
	sbalancer := mock_balancer.NewMockBalancer(t)
	sbalancer.EXPECT().GetAllStreamingNodes(mock.Anything).Return(map[int64]*types.StreamingNodeInfo{
		1: {ServerID: 1},
		2: {ServerID: 2},
	}, nil)
	sbalancer.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		if err := cb(balancer.WatchChannelAssignmentsCallbackParam{
			Version: typeutil.VersionInt64Pair{
				Global: 1,
				Local:  1,
			},
			Relations: []types.PChannelInfoAssigned{
				{
					Channel: types.PChannelInfo{Name: "v1"},
					Node:    types.StreamingNodeInfo{ServerID: 1},
				},
				{
					Channel: types.PChannelInfo{Name: "v2"},
					Node:    types.StreamingNodeInfo{ServerID: 1},
				},
			},
		}); err != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond)
		return nil
	})
	sbalancer.EXPECT().RegisterStreamingEnabledNotifier(mock.Anything).RunAndReturn(func(notifier *syncutil.AsyncTaskNotifier[struct{}]) {
		notifier.Cancel()
	})

	snmanager.ResetStreamingNodeManager()
	balance.Register(sbalancer)

	balancer := balancerImpl{
		walAccesserImpl: &walAccesserImpl{},
	}

	nodes, err := balancer.ListStreamingNode(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 2, len(nodes))
	assignment, err := balancer.GetWALDistribution(context.Background(), 1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(assignment.Channels))

	assignment, err = balancer.GetWALDistribution(context.Background(), 2)
	assert.True(t, errors.Is(err, merr.ErrNodeNotFound))
	assert.Nil(t, assignment)

	sbalancer.EXPECT().GetAllStreamingNodes(mock.Anything).Unset()
	sbalancer.EXPECT().GetAllStreamingNodes(mock.Anything).Return(nil, errors.New("test"))
	nodes, err = balancer.ListStreamingNode(context.Background())
	assert.Error(t, err)
	assert.Nil(t, nodes)

	sbalancer.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).Unset()
	sbalancer.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).Return(errors.New("test"))
	assignment, err = balancer.GetWALDistribution(context.Background(), 1)
	assert.Error(t, err)
	assert.Nil(t, assignment)

	sbalancer.EXPECT().UpdateBalancePolicy(mock.Anything, mock.Anything).Return(&streamingpb.UpdateWALBalancePolicyResponse{}, nil)
	err = balancer.SuspendRebalance(context.Background())
	assert.NoError(t, err)
	err = balancer.ResumeRebalance(context.Background())
	assert.NoError(t, err)
	err = balancer.FreezeNodeIDs(context.Background(), []int64{1})
	assert.NoError(t, err)
	err = balancer.DefreezeNodeIDs(context.Background(), []int64{1})
	assert.NoError(t, err)
	_, err = balancer.GetFrozenNodeIDs(context.Background())
	assert.NoError(t, err)
	_, err = balancer.IsRebalanceSuspended(context.Background())
	assert.NoError(t, err)

	sbalancer.EXPECT().UpdateBalancePolicy(mock.Anything, mock.Anything).Unset()
	sbalancer.EXPECT().UpdateBalancePolicy(mock.Anything, mock.Anything).Return(nil, errors.New("test"))
	err = balancer.SuspendRebalance(context.Background())
	assert.Error(t, err)
	err = balancer.ResumeRebalance(context.Background())
	assert.Error(t, err)
	err = balancer.FreezeNodeIDs(context.Background(), []int64{1})
	assert.Error(t, err)
	err = balancer.DefreezeNodeIDs(context.Background(), []int64{1})
	assert.Error(t, err)
	_, err = balancer.GetFrozenNodeIDs(context.Background())
	assert.Error(t, err)
	_, err = balancer.IsRebalanceSuspended(context.Background())
	assert.Error(t, err)

	sbalancer.EXPECT().RegisterStreamingEnabledNotifier(mock.Anything).Unset()
	sbalancer.EXPECT().RegisterStreamingEnabledNotifier(mock.Anything).RunAndReturn(func(notifier *syncutil.AsyncTaskNotifier[struct{}]) {
	})

	_, err = balancer.ListStreamingNode(context.Background())
	assert.NoError(t, err)
	_, err = balancer.GetWALDistribution(context.Background(), 1)
	assert.NoError(t, err)
	err = balancer.SuspendRebalance(context.Background())
	assert.NoError(t, err)
	err = balancer.ResumeRebalance(context.Background())
	assert.NoError(t, err)
	err = balancer.FreezeNodeIDs(context.Background(), []int64{1})
	assert.NoError(t, err)
	err = balancer.DefreezeNodeIDs(context.Background(), []int64{1})
	assert.NoError(t, err)
	_, err = balancer.GetFrozenNodeIDs(context.Background())
	assert.NoError(t, err)
	_, err = balancer.IsRebalanceSuspended(context.Background())
	assert.Error(t, err)
}
