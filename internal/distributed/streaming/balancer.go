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

// GetFrozenNodeIDs returns the frozen node ids.
func (b balancerImpl) GetFrozenNodeIDs(ctx context.Context) ([]int64, error) {
	// Update nothing, just fetch the current resp back.
	resp, err := b.streamingCoordClient.Assignment().UpdateWALBalancePolicy(ctx, &types.UpdateWALBalancePolicyRequest{
		Config:     &streamingpb.WALBalancePolicyConfig{},
		UpdateMask: &fieldmaskpb.FieldMask{},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetFreezeNodeIds(), nil
}

// IsRebalanceSuspended returns whether the rebalance of the wal is suspended.
func (b balancerImpl) IsRebalanceSuspended(ctx context.Context) (bool, error) {
	// Update nothing, just fetch the current resp back.
	resp, err := b.streamingCoordClient.Assignment().UpdateWALBalancePolicy(ctx, &types.UpdateWALBalancePolicyRequest{
		Config:     &streamingpb.WALBalancePolicyConfig{},
		UpdateMask: &fieldmaskpb.FieldMask{},
	})
	if err != nil {
		return false, err
	}
	return !resp.GetConfig().GetAllowRebalance(), nil
}

func (b balancerImpl) SuspendRebalance(ctx context.Context) error {
	_, err := b.streamingCoordClient.Assignment().UpdateWALBalancePolicy(ctx, &types.UpdateWALBalancePolicyRequest{
		Config: &streamingpb.WALBalancePolicyConfig{
			AllowRebalance: false,
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{types.UpdateMaskPathWALBalancePolicyAllowRebalance},
		},
	})
	return err
}

func (b balancerImpl) ResumeRebalance(ctx context.Context) error {
	_, err := b.streamingCoordClient.Assignment().UpdateWALBalancePolicy(ctx, &types.UpdateWALBalancePolicyRequest{
		Config: &streamingpb.WALBalancePolicyConfig{
			AllowRebalance: true,
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{types.UpdateMaskPathWALBalancePolicyAllowRebalance},
		},
	})
	return err
}

func (b balancerImpl) FreezeNodeIDs(ctx context.Context, nodeIDs []int64) error {
	_, err := b.streamingCoordClient.Assignment().UpdateWALBalancePolicy(ctx, &types.UpdateWALBalancePolicyRequest{
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{}},
		Nodes: &streamingpb.WALBalancePolicyNodes{
			FreezeNodeIds: nodeIDs,
		},
	})
	return err
}

func (b balancerImpl) DefreezeNodeIDs(ctx context.Context, nodeIDs []int64) error {
	_, err := b.streamingCoordClient.Assignment().UpdateWALBalancePolicy(ctx, &types.UpdateWALBalancePolicyRequest{
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{}},
		Nodes: &streamingpb.WALBalancePolicyNodes{
			DefreezeNodeIds: nodeIDs,
		},
	})
	return err
}
