package streaming

import (
	"context"

	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type balancerImpl struct {
	*walAccesserImpl
}

// GetWALDistribution returns the wal distribution of the streaming node.
func (b balancerImpl) ListStreamingNode(ctx context.Context) ([]types.StreamingNodeInfo, error) {
	assignments, err := b.streamingCoordClient.Assignment().GetLatestAssignments(ctx)
	if err != nil {
		return nil, err
	}

	nodes := make([]types.StreamingNodeInfo, 0, len(assignments.Assignments))
	for _, assignment := range assignments.Assignments {
		nodes = append(nodes, assignment.NodeInfo)
	}
	return nodes, nil
}

// GetWALDistribution returns the wal distribution of the streaming node.
func (b balancerImpl) GetWALDistribution(ctx context.Context, nodeID int64) (*types.StreamingNodeAssignment, error) {
	assignments, err := b.streamingCoordClient.Assignment().GetLatestAssignments(ctx)
	if err != nil {
		return nil, err
	}
	for _, assignment := range assignments.Assignments {
		if assignment.NodeInfo.ServerID == nodeID {
			return &assignment, nil
		}
	}
	return nil, merr.WrapErrNodeNotFound(nodeID, "streaming node not found")
}

func (b balancerImpl) SuspendRebalance(ctx context.Context) error {
	return b.streamingCoordClient.Assignment().UpdateWALBalancePolicy(ctx, &types.UpdateWALBalancePolicyRequest{
		Config: &streamingpb.WALBalancePolicyConfig{
			AllowRebalance: false,
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{types.UpdateMaskPathWALBalancePolicyAllowRebalance},
		},
	})
}

func (b balancerImpl) ResumeRebalance(ctx context.Context) error {
	return b.streamingCoordClient.Assignment().UpdateWALBalancePolicy(ctx, &types.UpdateWALBalancePolicyRequest{
		Config: &streamingpb.WALBalancePolicyConfig{
			AllowRebalance: true,
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{types.UpdateMaskPathWALBalancePolicyAllowRebalance},
		},
	})
}

func (b balancerImpl) FreezeNodeIDs(ctx context.Context, nodeIDs []int64) error {
	return b.streamingCoordClient.Assignment().UpdateWALBalancePolicy(ctx, &types.UpdateWALBalancePolicyRequest{
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{}},
		Nodes: &streamingpb.WALBalancePolicyNodes{
			FreezeNodeIds: nodeIDs,
		},
	})
}

func (b balancerImpl) DefreezeNodeIDs(ctx context.Context, nodeIDs []int64) error {
	return b.streamingCoordClient.Assignment().UpdateWALBalancePolicy(ctx, &types.UpdateWALBalancePolicyRequest{
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{}},
		Nodes: &streamingpb.WALBalancePolicyNodes{
			DefreezeNodeIds: nodeIDs,
		},
	})
}
