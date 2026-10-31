package streaming

import (
	"context"

	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type balancerImpl struct {
	*walAccesserImpl
}

// ListStreamingNode returns all the streaming nodes.
func (b balancerImpl) ListStreamingNode(ctx context.Context) ([]types.StreamingNodeInfo, error) {
	err := b.checkIfStreamingServiceReady(ctx)
	if err != nil {
		return nil, err
	}

	nodes, err2 := snmanager.StaticStreamingNodeManager.GetBalancer().GetAllStreamingNodes(ctx)
	if err2 != nil {
		return nil, err2
	}
	nodeInfos := make([]types.StreamingNodeInfo, 0, len(nodes))
	for _, node := range nodes {
		nodeInfos = append(nodeInfos, node.StreamingNodeInfo)
	}
	return nodeInfos, nil
}

// GetWALDistribution distinguishes an empty registered SN from an unknown node.
func (b balancerImpl) GetWALDistribution(ctx context.Context, nodeID int64) (*types.StreamingNodeAssignment, error) {
	if err := b.checkIfStreamingServiceReady(ctx); err != nil {
		return nil, err
	}
	sbalancer := snmanager.StaticStreamingNodeManager.GetBalancer()
	nodes, err := sbalancer.GetAllStreamingNodes(ctx)
	if err != nil {
		return nil, err
	}
	node, ok := nodes[nodeID]
	if !ok {
		return nil, merr.WrapErrNodeNotFound(nodeID, "StreamingNode not found")
	}
	assignment, err := sbalancer.GetLatestChannelAssignment()
	if err != nil {
		return nil, err
	}
	result := &types.StreamingNodeAssignment{NodeInfo: node.StreamingNodeInfo, Channels: make(map[string]types.PChannelInfo)}
	for _, relation := range assignment.Relations {
		if relation.Node.ServerID == nodeID {
			result.Channels[relation.Channel.Name] = relation.Channel
		}
	}
	return result, nil
}

// GetFrozenNodeIDs returns the frozen node ids.
func (b balancerImpl) GetFrozenNodeIDs(ctx context.Context) ([]int64, error) {
	err := b.checkIfStreamingServiceReady(ctx)
	if err != nil {
		return nil, err
	}

	sbalancer := snmanager.StaticStreamingNodeManager.GetBalancer()
	resp, err := sbalancer.UpdateBalancePolicy(ctx, &types.UpdateWALBalancePolicyRequest{
		Config:     &streamingpb.WALBalancePolicyConfig{},
		UpdateMask: &fieldmaskpb.FieldMask{},
	})
	if err != nil {
		return nil, err
	}
	return resp.FreezeNodeIds, nil
}

func (b balancerImpl) FreezeNodeIDs(ctx context.Context, nodeIDs []int64) error {
	err := b.checkIfStreamingServiceReady(ctx)
	if err != nil {
		return err
	}

	sbalancer := snmanager.StaticStreamingNodeManager.GetBalancer()
	_, err = sbalancer.UpdateBalancePolicy(ctx, &types.UpdateWALBalancePolicyRequest{
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{}},
		Nodes: &streamingpb.WALBalancePolicyNodes{
			FreezeNodeIds: nodeIDs,
		},
	})
	return err
}

func (b balancerImpl) DefreezeNodeIDs(ctx context.Context, nodeIDs []int64) error {
	err := b.checkIfStreamingServiceReady(ctx)
	if err != nil {
		return err
	}

	sbalancer := snmanager.StaticStreamingNodeManager.GetBalancer()
	_, err = sbalancer.UpdateBalancePolicy(ctx, &types.UpdateWALBalancePolicyRequest{
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{}},
		Nodes: &streamingpb.WALBalancePolicyNodes{
			DefreezeNodeIds: nodeIDs,
		},
	})
	return err
}

func (b balancerImpl) checkIfStreamingServiceReady(ctx context.Context) error {
	if !paramtable.IsLocalComponentEnabled(typeutil.MixCoordRole) {
		panic("should be only called at mix coord")
	}
	return snmanager.StaticStreamingNodeManager.CheckIfStreamingServiceReady(ctx)
}
