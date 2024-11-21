package coordclient

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

var _ types.DataCoordClient = &dataCoordLocalClientImpl{}

// newDataCoordLocalClient creates a new local client for data coordinator server.
func newDataCoordLocalClient() *dataCoordLocalClientImpl {
	return &dataCoordLocalClientImpl{
		localDataCoordServer: syncutil.NewFuture[datapb.DataCoordServer](),
	}
}

// dataCoordLocalClientImpl is used to implement a local client for data coordinator server.
// We need to merge all the coordinator into one server, so use those client to erase the rpc layer between different coord.
type dataCoordLocalClientImpl struct {
	localDataCoordServer *syncutil.Future[datapb.DataCoordServer]
}

func (c *dataCoordLocalClientImpl) setReadyServer(server datapb.DataCoordServer) {
	c.localDataCoordServer.Set(server)
}

func (c *dataCoordLocalClientImpl) waitForReady(ctx context.Context) (datapb.DataCoordServer, error) {
	return c.localDataCoordServer.GetWithContext(ctx)
}

func (c *dataCoordLocalClientImpl) GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetComponentStates(ctx, in)
}

func (c *dataCoordLocalClientImpl) GetTimeTickChannel(ctx context.Context, in *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetTimeTickChannel(ctx, in)
}

func (c *dataCoordLocalClientImpl) GetStatisticsChannel(ctx context.Context, in *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetStatisticsChannel(ctx, in)
}

func (c *dataCoordLocalClientImpl) Flush(ctx context.Context, in *datapb.FlushRequest, opts ...grpc.CallOption) (*datapb.FlushResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.Flush(ctx, in)
}

func (c *dataCoordLocalClientImpl) AssignSegmentID(ctx context.Context, in *datapb.AssignSegmentIDRequest, opts ...grpc.CallOption) (*datapb.AssignSegmentIDResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.AssignSegmentID(ctx, in)
}

func (c *dataCoordLocalClientImpl) GetSegmentInfo(ctx context.Context, in *datapb.GetSegmentInfoRequest, opts ...grpc.CallOption) (*datapb.GetSegmentInfoResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetSegmentInfo(ctx, in)
}

func (c *dataCoordLocalClientImpl) GetSegmentStates(ctx context.Context, in *datapb.GetSegmentStatesRequest, opts ...grpc.CallOption) (*datapb.GetSegmentStatesResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetSegmentStates(ctx, in)
}

func (c *dataCoordLocalClientImpl) GetInsertBinlogPaths(ctx context.Context, in *datapb.GetInsertBinlogPathsRequest, opts ...grpc.CallOption) (*datapb.GetInsertBinlogPathsResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetInsertBinlogPaths(ctx, in)
}

func (c *dataCoordLocalClientImpl) GetCollectionStatistics(ctx context.Context, in *datapb.GetCollectionStatisticsRequest, opts ...grpc.CallOption) (*datapb.GetCollectionStatisticsResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetCollectionStatistics(ctx, in)
}

func (c *dataCoordLocalClientImpl) GetPartitionStatistics(ctx context.Context, in *datapb.GetPartitionStatisticsRequest, opts ...grpc.CallOption) (*datapb.GetPartitionStatisticsResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetPartitionStatistics(ctx, in)
}

func (c *dataCoordLocalClientImpl) GetSegmentInfoChannel(ctx context.Context, in *datapb.GetSegmentInfoChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetSegmentInfoChannel(ctx, in)
}

func (c *dataCoordLocalClientImpl) SaveBinlogPaths(ctx context.Context, in *datapb.SaveBinlogPathsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.SaveBinlogPaths(ctx, in)
}

func (c *dataCoordLocalClientImpl) GetRecoveryInfo(ctx context.Context, in *datapb.GetRecoveryInfoRequest, opts ...grpc.CallOption) (*datapb.GetRecoveryInfoResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetRecoveryInfo(ctx, in)
}

func (c *dataCoordLocalClientImpl) GetRecoveryInfoV2(ctx context.Context, in *datapb.GetRecoveryInfoRequestV2, opts ...grpc.CallOption) (*datapb.GetRecoveryInfoResponseV2, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetRecoveryInfoV2(ctx, in)
}

func (c *dataCoordLocalClientImpl) GetFlushedSegments(ctx context.Context, in *datapb.GetFlushedSegmentsRequest, opts ...grpc.CallOption) (*datapb.GetFlushedSegmentsResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetFlushedSegments(ctx, in)
}

func (c *dataCoordLocalClientImpl) GetSegmentsByStates(ctx context.Context, in *datapb.GetSegmentsByStatesRequest, opts ...grpc.CallOption) (*datapb.GetSegmentsByStatesResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetSegmentsByStates(ctx, in)
}

func (c *dataCoordLocalClientImpl) GetFlushAllState(ctx context.Context, in *milvuspb.GetFlushAllStateRequest, opts ...grpc.CallOption) (*milvuspb.GetFlushAllStateResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetFlushAllState(ctx, in)
}

func (c *dataCoordLocalClientImpl) ShowConfigurations(ctx context.Context, in *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ShowConfigurations(ctx, in)
}

func (c *dataCoordLocalClientImpl) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetMetrics(ctx, in)
}

func (c *dataCoordLocalClientImpl) ManualCompaction(ctx context.Context, in *milvuspb.ManualCompactionRequest, opts ...grpc.CallOption) (*milvuspb.ManualCompactionResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ManualCompaction(ctx, in)
}

func (c *dataCoordLocalClientImpl) GetCompactionState(ctx context.Context, in *milvuspb.GetCompactionStateRequest, opts ...grpc.CallOption) (*milvuspb.GetCompactionStateResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetCompactionState(ctx, in)
}

func (c *dataCoordLocalClientImpl) GetCompactionStateWithPlans(ctx context.Context, in *milvuspb.GetCompactionPlansRequest, opts ...grpc.CallOption) (*milvuspb.GetCompactionPlansResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetCompactionStateWithPlans(ctx, in)
}

func (c *dataCoordLocalClientImpl) WatchChannels(ctx context.Context, in *datapb.WatchChannelsRequest, opts ...grpc.CallOption) (*datapb.WatchChannelsResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.WatchChannels(ctx, in)
}

func (c *dataCoordLocalClientImpl) GetFlushState(ctx context.Context, in *datapb.GetFlushStateRequest, opts ...grpc.CallOption) (*milvuspb.GetFlushStateResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetFlushState(ctx, in)
}

func (c *dataCoordLocalClientImpl) DropVirtualChannel(ctx context.Context, in *datapb.DropVirtualChannelRequest, opts ...grpc.CallOption) (*datapb.DropVirtualChannelResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.DropVirtualChannel(ctx, in)
}

func (c *dataCoordLocalClientImpl) SetSegmentState(ctx context.Context, in *datapb.SetSegmentStateRequest, opts ...grpc.CallOption) (*datapb.SetSegmentStateResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.SetSegmentState(ctx, in)
}

func (c *dataCoordLocalClientImpl) UpdateSegmentStatistics(ctx context.Context, in *datapb.UpdateSegmentStatisticsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.UpdateSegmentStatistics(ctx, in)
}

func (c *dataCoordLocalClientImpl) UpdateChannelCheckpoint(ctx context.Context, in *datapb.UpdateChannelCheckpointRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.UpdateChannelCheckpoint(ctx, in)
}

func (c *dataCoordLocalClientImpl) MarkSegmentsDropped(ctx context.Context, in *datapb.MarkSegmentsDroppedRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.MarkSegmentsDropped(ctx, in)
}

func (c *dataCoordLocalClientImpl) BroadcastAlteredCollection(ctx context.Context, in *datapb.AlterCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.BroadcastAlteredCollection(ctx, in)
}

func (c *dataCoordLocalClientImpl) CheckHealth(ctx context.Context, in *milvuspb.CheckHealthRequest, opts ...grpc.CallOption) (*milvuspb.CheckHealthResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.CheckHealth(ctx, in)
}

func (c *dataCoordLocalClientImpl) CreateIndex(ctx context.Context, in *indexpb.CreateIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.CreateIndex(ctx, in)
}

func (c *dataCoordLocalClientImpl) AlterIndex(ctx context.Context, in *indexpb.AlterIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.AlterIndex(ctx, in)
}

func (c *dataCoordLocalClientImpl) GetIndexState(ctx context.Context, in *indexpb.GetIndexStateRequest, opts ...grpc.CallOption) (*indexpb.GetIndexStateResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetIndexState(ctx, in)
}

func (c *dataCoordLocalClientImpl) GetSegmentIndexState(ctx context.Context, in *indexpb.GetSegmentIndexStateRequest, opts ...grpc.CallOption) (*indexpb.GetSegmentIndexStateResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetSegmentIndexState(ctx, in)
}

func (c *dataCoordLocalClientImpl) GetIndexInfos(ctx context.Context, in *indexpb.GetIndexInfoRequest, opts ...grpc.CallOption) (*indexpb.GetIndexInfoResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetIndexInfos(ctx, in)
}

func (c *dataCoordLocalClientImpl) DropIndex(ctx context.Context, in *indexpb.DropIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.DropIndex(ctx, in)
}

func (c *dataCoordLocalClientImpl) DescribeIndex(ctx context.Context, in *indexpb.DescribeIndexRequest, opts ...grpc.CallOption) (*indexpb.DescribeIndexResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.DescribeIndex(ctx, in)
}

func (c *dataCoordLocalClientImpl) GetIndexStatistics(ctx context.Context, in *indexpb.GetIndexStatisticsRequest, opts ...grpc.CallOption) (*indexpb.GetIndexStatisticsResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetIndexStatistics(ctx, in)
}

func (c *dataCoordLocalClientImpl) GetIndexBuildProgress(ctx context.Context, in *indexpb.GetIndexBuildProgressRequest, opts ...grpc.CallOption) (*indexpb.GetIndexBuildProgressResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetIndexBuildProgress(ctx, in)
}

func (c *dataCoordLocalClientImpl) ListIndexes(ctx context.Context, in *indexpb.ListIndexesRequest, opts ...grpc.CallOption) (*indexpb.ListIndexesResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ListIndexes(ctx, in)
}

func (c *dataCoordLocalClientImpl) GcConfirm(ctx context.Context, in *datapb.GcConfirmRequest, opts ...grpc.CallOption) (*datapb.GcConfirmResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GcConfirm(ctx, in)
}

func (c *dataCoordLocalClientImpl) ReportDataNodeTtMsgs(ctx context.Context, in *datapb.ReportDataNodeTtMsgsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ReportDataNodeTtMsgs(ctx, in)
}

func (c *dataCoordLocalClientImpl) GcControl(ctx context.Context, in *datapb.GcControlRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GcControl(ctx, in)
}

func (c *dataCoordLocalClientImpl) ImportV2(ctx context.Context, in *internalpb.ImportRequestInternal, opts ...grpc.CallOption) (*internalpb.ImportResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ImportV2(ctx, in)
}

func (c *dataCoordLocalClientImpl) GetImportProgress(ctx context.Context, in *internalpb.GetImportProgressRequest, opts ...grpc.CallOption) (*internalpb.GetImportProgressResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetImportProgress(ctx, in)
}

func (c *dataCoordLocalClientImpl) ListImports(ctx context.Context, in *internalpb.ListImportsRequestInternal, opts ...grpc.CallOption) (*internalpb.ListImportsResponse, error) {
	s, err := c.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ListImports(ctx, in)
}

func (c *dataCoordLocalClientImpl) Close() error {
	return nil
}
