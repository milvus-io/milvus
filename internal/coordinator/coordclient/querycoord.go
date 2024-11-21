package coordclient

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

var _ types.QueryCoordClient = &queryCoordLocalClientImpl{}

// newQueryCoordLocalClient creates a new local client for query coordinator server.
func newQueryCoordLocalClient() *queryCoordLocalClientImpl {
	return &queryCoordLocalClientImpl{
		localQueryCoordServer: syncutil.NewFuture[querypb.QueryCoordServer](),
	}
}

// queryCoordLocalClientImpl is used to implement a local client for query coordinator server.
// We need to merge all the coordinator into one server, so use those client to erase the rpc layer between different coord.
type queryCoordLocalClientImpl struct {
	localQueryCoordServer *syncutil.Future[querypb.QueryCoordServer]
}

func (c *queryCoordLocalClientImpl) setReadyServer(server querypb.QueryCoordServer) {
	c.localQueryCoordServer.Set(server)
}

func (c *queryCoordLocalClientImpl) waitForReady(ctx context.Context) (querypb.QueryCoordServer, error) {
	return c.localQueryCoordServer.GetWithContext(ctx)
}

func (c *queryCoordLocalClientImpl) GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetComponentStates(ctx, in)
}

func (c *queryCoordLocalClientImpl) GetTimeTickChannel(ctx context.Context, in *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetTimeTickChannel(ctx, in)
}

func (c *queryCoordLocalClientImpl) GetStatisticsChannel(ctx context.Context, in *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetStatisticsChannel(ctx, in)
}

func (c *queryCoordLocalClientImpl) ShowCollections(ctx context.Context, in *querypb.ShowCollectionsRequest, opts ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ShowCollections(ctx, in)
}

func (c *queryCoordLocalClientImpl) ShowPartitions(ctx context.Context, in *querypb.ShowPartitionsRequest, opts ...grpc.CallOption) (*querypb.ShowPartitionsResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ShowPartitions(ctx, in)
}

func (c *queryCoordLocalClientImpl) LoadPartitions(ctx context.Context, in *querypb.LoadPartitionsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.LoadPartitions(ctx, in)
}

func (c *queryCoordLocalClientImpl) ReleasePartitions(ctx context.Context, in *querypb.ReleasePartitionsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ReleasePartitions(ctx, in)
}

func (c *queryCoordLocalClientImpl) LoadCollection(ctx context.Context, in *querypb.LoadCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.LoadCollection(ctx, in)
}

func (c *queryCoordLocalClientImpl) ReleaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ReleaseCollection(ctx, in)
}

func (c *queryCoordLocalClientImpl) SyncNewCreatedPartition(ctx context.Context, in *querypb.SyncNewCreatedPartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.SyncNewCreatedPartition(ctx, in)
}

func (c *queryCoordLocalClientImpl) GetPartitionStates(ctx context.Context, in *querypb.GetPartitionStatesRequest, opts ...grpc.CallOption) (*querypb.GetPartitionStatesResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetPartitionStates(ctx, in)
}

func (c *queryCoordLocalClientImpl) GetSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest, opts ...grpc.CallOption) (*querypb.GetSegmentInfoResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetSegmentInfo(ctx, in)
}

func (c *queryCoordLocalClientImpl) LoadBalance(ctx context.Context, in *querypb.LoadBalanceRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.LoadBalance(ctx, in)
}

func (c *queryCoordLocalClientImpl) ShowConfigurations(ctx context.Context, in *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ShowConfigurations(ctx, in)
}

func (c *queryCoordLocalClientImpl) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetMetrics(ctx, in)
}

func (c *queryCoordLocalClientImpl) GetReplicas(ctx context.Context, in *milvuspb.GetReplicasRequest, opts ...grpc.CallOption) (*milvuspb.GetReplicasResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetReplicas(ctx, in)
}

func (c *queryCoordLocalClientImpl) GetShardLeaders(ctx context.Context, in *querypb.GetShardLeadersRequest, opts ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetShardLeaders(ctx, in)
}

func (c *queryCoordLocalClientImpl) CheckHealth(ctx context.Context, in *milvuspb.CheckHealthRequest, opts ...grpc.CallOption) (*milvuspb.CheckHealthResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.CheckHealth(ctx, in)
}

func (c *queryCoordLocalClientImpl) CreateResourceGroup(ctx context.Context, in *milvuspb.CreateResourceGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.CreateResourceGroup(ctx, in)
}

func (c *queryCoordLocalClientImpl) UpdateResourceGroups(ctx context.Context, in *querypb.UpdateResourceGroupsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.UpdateResourceGroups(ctx, in)
}

func (c *queryCoordLocalClientImpl) DropResourceGroup(ctx context.Context, in *milvuspb.DropResourceGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.DropResourceGroup(ctx, in)
}

func (c *queryCoordLocalClientImpl) TransferNode(ctx context.Context, in *milvuspb.TransferNodeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.TransferNode(ctx, in)
}

func (c *queryCoordLocalClientImpl) TransferReplica(ctx context.Context, in *querypb.TransferReplicaRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.TransferReplica(ctx, in)
}

func (c *queryCoordLocalClientImpl) ListResourceGroups(ctx context.Context, in *milvuspb.ListResourceGroupsRequest, opts ...grpc.CallOption) (*milvuspb.ListResourceGroupsResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ListResourceGroups(ctx, in)
}

func (c *queryCoordLocalClientImpl) DescribeResourceGroup(ctx context.Context, in *querypb.DescribeResourceGroupRequest, opts ...grpc.CallOption) (*querypb.DescribeResourceGroupResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.DescribeResourceGroup(ctx, in)
}

func (c *queryCoordLocalClientImpl) ListCheckers(ctx context.Context, in *querypb.ListCheckersRequest, opts ...grpc.CallOption) (*querypb.ListCheckersResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ListCheckers(ctx, in)
}

func (c *queryCoordLocalClientImpl) ActivateChecker(ctx context.Context, in *querypb.ActivateCheckerRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ActivateChecker(ctx, in)
}

func (c *queryCoordLocalClientImpl) DeactivateChecker(ctx context.Context, in *querypb.DeactivateCheckerRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.DeactivateChecker(ctx, in)
}

func (c *queryCoordLocalClientImpl) ListQueryNode(ctx context.Context, in *querypb.ListQueryNodeRequest, opts ...grpc.CallOption) (*querypb.ListQueryNodeResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ListQueryNode(ctx, in)
}

func (c *queryCoordLocalClientImpl) GetQueryNodeDistribution(ctx context.Context, in *querypb.GetQueryNodeDistributionRequest, opts ...grpc.CallOption) (*querypb.GetQueryNodeDistributionResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetQueryNodeDistribution(ctx, in)
}

func (c *queryCoordLocalClientImpl) SuspendBalance(ctx context.Context, in *querypb.SuspendBalanceRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.SuspendBalance(ctx, in)
}

func (c *queryCoordLocalClientImpl) ResumeBalance(ctx context.Context, in *querypb.ResumeBalanceRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ResumeBalance(ctx, in)
}

func (c *queryCoordLocalClientImpl) SuspendNode(ctx context.Context, in *querypb.SuspendNodeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.SuspendNode(ctx, in)
}

func (c *queryCoordLocalClientImpl) ResumeNode(ctx context.Context, in *querypb.ResumeNodeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ResumeNode(ctx, in)
}

func (c *queryCoordLocalClientImpl) TransferSegment(ctx context.Context, in *querypb.TransferSegmentRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.TransferSegment(ctx, in)
}

func (c *queryCoordLocalClientImpl) TransferChannel(ctx context.Context, in *querypb.TransferChannelRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.TransferChannel(ctx, in)
}

func (c *queryCoordLocalClientImpl) CheckQueryNodeDistribution(ctx context.Context, in *querypb.CheckQueryNodeDistributionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.CheckQueryNodeDistribution(ctx, in)
}

func (c *queryCoordLocalClientImpl) UpdateLoadConfig(ctx context.Context, in *querypb.UpdateLoadConfigRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.UpdateLoadConfig(ctx, in)
}

func (c *queryCoordLocalClientImpl) Close() error {
	return nil
}
