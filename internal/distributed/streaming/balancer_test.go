package streaming

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/mock_client"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestBalancer(t *testing.T) {
	scClient := mock_client.NewMockClient(t)
	assignmentService := mock_client.NewMockAssignmentService(t)
	scClient.EXPECT().Assignment().Return(assignmentService)
	assignmentService.EXPECT().GetLatestAssignments(mock.Anything).Return(&types.VersionedStreamingNodeAssignments{
		Assignments: map[int64]types.StreamingNodeAssignment{
			1: {
				NodeInfo: types.StreamingNodeInfo{ServerID: 1},
				Channels: map[string]types.PChannelInfo{
					"v1": {},
				},
			},
		},
	}, nil)

	balancer := balancerImpl{
		walAccesserImpl: &walAccesserImpl{
			streamingCoordClient: scClient,
		},
	}

	nodes, err := balancer.ListStreamingNode(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, len(nodes))
	assignment, err := balancer.GetWALDistribution(context.Background(), 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(assignment.Channels))

	assignment, err = balancer.GetWALDistribution(context.Background(), 2)
	assert.True(t, errors.Is(err, merr.ErrNodeNotFound))
	assert.Nil(t, assignment)

	assignmentService.EXPECT().GetLatestAssignments(mock.Anything).Unset()
	assignmentService.EXPECT().GetLatestAssignments(mock.Anything).Return(nil, errors.New("test"))
	nodes, err = balancer.ListStreamingNode(context.Background())
	assert.Error(t, err)
	assert.Nil(t, nodes)

	assignment, err = balancer.GetWALDistribution(context.Background(), 1)
	assert.Error(t, err)
	assert.Nil(t, assignment)

	assignmentService.EXPECT().UpdateWALBalancePolicy(mock.Anything, mock.Anything).Return(&types.UpdateWALBalancePolicyResponse{}, nil)
	err = balancer.SuspendRebalance(context.Background())
	assert.NoError(t, err)
	err = balancer.ResumeRebalance(context.Background())
	assert.NoError(t, err)
	err = balancer.FreezeNodeIDs(context.Background(), []int64{1})
	assert.NoError(t, err)
	err = balancer.DefreezeNodeIDs(context.Background(), []int64{1})
	assert.NoError(t, err)

	assignmentService.EXPECT().UpdateWALBalancePolicy(mock.Anything, mock.Anything).Unset()
	assignmentService.EXPECT().UpdateWALBalancePolicy(mock.Anything, mock.Anything).Return(nil, errors.New("test"))
	err = balancer.SuspendRebalance(context.Background())
	assert.Error(t, err)
	err = balancer.ResumeRebalance(context.Background())
	assert.Error(t, err)
	err = balancer.FreezeNodeIDs(context.Background(), []int64{1})
	assert.Error(t, err)
	err = balancer.DefreezeNodeIDs(context.Background(), []int64{1})
	assert.Error(t, err)
}
