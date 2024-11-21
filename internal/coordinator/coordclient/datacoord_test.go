package coordclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

func TestDataCoordLocalClient(t *testing.T) {
	c := newDataCoordLocalClient()
	c.setReadyServer(datapb.UnimplementedDataCoordServer{})

	_, err := c.GetComponentStates(context.Background(), &milvuspb.GetComponentStatesRequest{})
	assert.Error(t, err)

	_, err = c.GetTimeTickChannel(context.Background(), &internalpb.GetTimeTickChannelRequest{})
	assert.Error(t, err)

	_, err = c.GetStatisticsChannel(context.Background(), &internalpb.GetStatisticsChannelRequest{})
	assert.Error(t, err)

	_, err = c.Flush(context.Background(), &datapb.FlushRequest{})
	assert.Error(t, err)

	_, err = c.AssignSegmentID(context.Background(), &datapb.AssignSegmentIDRequest{})
	assert.Error(t, err)

	_, err = c.GetSegmentInfo(context.Background(), &datapb.GetSegmentInfoRequest{})
	assert.Error(t, err)

	_, err = c.GetSegmentStates(context.Background(), &datapb.GetSegmentStatesRequest{})
	assert.Error(t, err)

	_, err = c.GetInsertBinlogPaths(context.Background(), &datapb.GetInsertBinlogPathsRequest{})
	assert.Error(t, err)

	_, err = c.GetCollectionStatistics(context.Background(), &datapb.GetCollectionStatisticsRequest{})
	assert.Error(t, err)

	_, err = c.GetPartitionStatistics(context.Background(), &datapb.GetPartitionStatisticsRequest{})
	assert.Error(t, err)

	_, err = c.GetSegmentInfoChannel(context.Background(), &datapb.GetSegmentInfoChannelRequest{})
	assert.Error(t, err)

	_, err = c.SaveBinlogPaths(context.Background(), &datapb.SaveBinlogPathsRequest{})
	assert.Error(t, err)

	_, err = c.GetRecoveryInfo(context.Background(), &datapb.GetRecoveryInfoRequest{})
	assert.Error(t, err)

	_, err = c.GetRecoveryInfoV2(context.Background(), &datapb.GetRecoveryInfoRequestV2{})
	assert.Error(t, err)

	_, err = c.GetFlushedSegments(context.Background(), &datapb.GetFlushedSegmentsRequest{})
	assert.Error(t, err)

	_, err = c.GetSegmentsByStates(context.Background(), &datapb.GetSegmentsByStatesRequest{})
	assert.Error(t, err)

	_, err = c.GetFlushAllState(context.Background(), &milvuspb.GetFlushAllStateRequest{})
	assert.Error(t, err)

	_, err = c.ShowConfigurations(context.Background(), &internalpb.ShowConfigurationsRequest{})
	assert.Error(t, err)

	_, err = c.GetMetrics(context.Background(), &milvuspb.GetMetricsRequest{})
	assert.Error(t, err)

	_, err = c.ManualCompaction(context.Background(), &milvuspb.ManualCompactionRequest{})
	assert.Error(t, err)

	_, err = c.GetCompactionState(context.Background(), &milvuspb.GetCompactionStateRequest{})
	assert.Error(t, err)

	_, err = c.GetCompactionStateWithPlans(context.Background(), &milvuspb.GetCompactionPlansRequest{})
	assert.Error(t, err)

	_, err = c.WatchChannels(context.Background(), &datapb.WatchChannelsRequest{})
	assert.Error(t, err)

	_, err = c.GetFlushState(context.Background(), &datapb.GetFlushStateRequest{})
	assert.Error(t, err)

	_, err = c.DropVirtualChannel(context.Background(), &datapb.DropVirtualChannelRequest{})
	assert.Error(t, err)

	_, err = c.SetSegmentState(context.Background(), &datapb.SetSegmentStateRequest{})
	assert.Error(t, err)

	_, err = c.UpdateSegmentStatistics(context.Background(), &datapb.UpdateSegmentStatisticsRequest{})
	assert.Error(t, err)

	_, err = c.UpdateChannelCheckpoint(context.Background(), &datapb.UpdateChannelCheckpointRequest{})
	assert.Error(t, err)

	_, err = c.MarkSegmentsDropped(context.Background(), &datapb.MarkSegmentsDroppedRequest{})
	assert.Error(t, err)

	_, err = c.BroadcastAlteredCollection(context.Background(), &datapb.AlterCollectionRequest{})
	assert.Error(t, err)

	_, err = c.CheckHealth(context.Background(), &milvuspb.CheckHealthRequest{})
	assert.Error(t, err)

	_, err = c.CreateIndex(context.Background(), &indexpb.CreateIndexRequest{})
	assert.Error(t, err)

	_, err = c.AlterIndex(context.Background(), &indexpb.AlterIndexRequest{})
	assert.Error(t, err)

	_, err = c.GetIndexState(context.Background(), &indexpb.GetIndexStateRequest{})
	assert.Error(t, err)

	_, err = c.GetSegmentIndexState(context.Background(), &indexpb.GetSegmentIndexStateRequest{})
	assert.Error(t, err)

	_, err = c.GetIndexInfos(context.Background(), &indexpb.GetIndexInfoRequest{})
	assert.Error(t, err)

	_, err = c.DropIndex(context.Background(), &indexpb.DropIndexRequest{})
	assert.Error(t, err)

	_, err = c.DescribeIndex(context.Background(), &indexpb.DescribeIndexRequest{})
	assert.Error(t, err)

	_, err = c.GetIndexStatistics(context.Background(), &indexpb.GetIndexStatisticsRequest{})
	assert.Error(t, err)

	_, err = c.GetIndexBuildProgress(context.Background(), &indexpb.GetIndexBuildProgressRequest{})
	assert.Error(t, err)

	_, err = c.ListIndexes(context.Background(), &indexpb.ListIndexesRequest{})
	assert.Error(t, err)

	_, err = c.GcConfirm(context.Background(), &datapb.GcConfirmRequest{})
	assert.Error(t, err)

	_, err = c.ReportDataNodeTtMsgs(context.Background(), &datapb.ReportDataNodeTtMsgsRequest{})
	assert.Error(t, err)

	_, err = c.GcControl(context.Background(), &datapb.GcControlRequest{})
	assert.Error(t, err)

	_, err = c.ImportV2(context.Background(), &internalpb.ImportRequestInternal{})
	assert.Error(t, err)

	_, err = c.GetImportProgress(context.Background(), &internalpb.GetImportProgressRequest{})
	assert.Error(t, err)

	_, err = c.ListImports(context.Background(), &internalpb.ListImportsRequestInternal{})
	assert.Error(t, err)
}

func TestDataCoordLocalClientWithTimeout(t *testing.T) {
	c := newDataCoordLocalClient()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	assert.Error(t, err)

	_, err = c.GetTimeTickChannel(ctx, &internalpb.GetTimeTickChannelRequest{})
	assert.Error(t, err)

	_, err = c.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	assert.Error(t, err)

	_, err = c.Flush(ctx, &datapb.FlushRequest{})
	assert.Error(t, err)

	_, err = c.AssignSegmentID(ctx, &datapb.AssignSegmentIDRequest{})
	assert.Error(t, err)

	_, err = c.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{})
	assert.Error(t, err)

	_, err = c.GetSegmentStates(ctx, &datapb.GetSegmentStatesRequest{})
	assert.Error(t, err)

	_, err = c.GetInsertBinlogPaths(ctx, &datapb.GetInsertBinlogPathsRequest{})
	assert.Error(t, err)

	_, err = c.GetCollectionStatistics(ctx, &datapb.GetCollectionStatisticsRequest{})
	assert.Error(t, err)

	_, err = c.GetPartitionStatistics(ctx, &datapb.GetPartitionStatisticsRequest{})
	assert.Error(t, err)

	_, err = c.GetSegmentInfoChannel(ctx, &datapb.GetSegmentInfoChannelRequest{})
	assert.Error(t, err)

	_, err = c.SaveBinlogPaths(ctx, &datapb.SaveBinlogPathsRequest{})
	assert.Error(t, err)

	_, err = c.GetRecoveryInfo(ctx, &datapb.GetRecoveryInfoRequest{})
	assert.Error(t, err)

	_, err = c.GetRecoveryInfoV2(ctx, &datapb.GetRecoveryInfoRequestV2{})
	assert.Error(t, err)

	_, err = c.GetFlushedSegments(ctx, &datapb.GetFlushedSegmentsRequest{})
	assert.Error(t, err)

	_, err = c.GetSegmentsByStates(ctx, &datapb.GetSegmentsByStatesRequest{})
	assert.Error(t, err)

	_, err = c.GetFlushAllState(ctx, &milvuspb.GetFlushAllStateRequest{})
	assert.Error(t, err)

	_, err = c.ShowConfigurations(ctx, &internalpb.ShowConfigurationsRequest{})
	assert.Error(t, err)

	_, err = c.GetMetrics(ctx, &milvuspb.GetMetricsRequest{})
	assert.Error(t, err)

	_, err = c.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{})
	assert.Error(t, err)

	_, err = c.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{})
	assert.Error(t, err)

	_, err = c.GetCompactionStateWithPlans(ctx, &milvuspb.GetCompactionPlansRequest{})
	assert.Error(t, err)

	_, err = c.WatchChannels(ctx, &datapb.WatchChannelsRequest{})
	assert.Error(t, err)

	_, err = c.GetFlushState(ctx, &datapb.GetFlushStateRequest{})
	assert.Error(t, err)

	_, err = c.DropVirtualChannel(ctx, &datapb.DropVirtualChannelRequest{})
	assert.Error(t, err)

	_, err = c.SetSegmentState(ctx, &datapb.SetSegmentStateRequest{})
	assert.Error(t, err)

	_, err = c.UpdateSegmentStatistics(ctx, &datapb.UpdateSegmentStatisticsRequest{})
	assert.Error(t, err)

	_, err = c.UpdateChannelCheckpoint(ctx, &datapb.UpdateChannelCheckpointRequest{})
	assert.Error(t, err)

	_, err = c.MarkSegmentsDropped(ctx, &datapb.MarkSegmentsDroppedRequest{})
	assert.Error(t, err)

	_, err = c.BroadcastAlteredCollection(ctx, &datapb.AlterCollectionRequest{})
	assert.Error(t, err)

	_, err = c.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
	assert.Error(t, err)

	_, err = c.CreateIndex(ctx, &indexpb.CreateIndexRequest{})
	assert.Error(t, err)

	_, err = c.AlterIndex(ctx, &indexpb.AlterIndexRequest{})
	assert.Error(t, err)

	_, err = c.GetIndexState(ctx, &indexpb.GetIndexStateRequest{})
	assert.Error(t, err)

	_, err = c.GetSegmentIndexState(ctx, &indexpb.GetSegmentIndexStateRequest{})
	assert.Error(t, err)

	_, err = c.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{})
	assert.Error(t, err)

	_, err = c.DropIndex(ctx, &indexpb.DropIndexRequest{})
	assert.Error(t, err)

	_, err = c.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{})
	assert.Error(t, err)

	_, err = c.GetIndexStatistics(ctx, &indexpb.GetIndexStatisticsRequest{})
	assert.Error(t, err)

	_, err = c.GetIndexBuildProgress(ctx, &indexpb.GetIndexBuildProgressRequest{})
	assert.Error(t, err)

	_, err = c.ListIndexes(ctx, &indexpb.ListIndexesRequest{})
	assert.Error(t, err)

	_, err = c.GcConfirm(ctx, &datapb.GcConfirmRequest{})
	assert.Error(t, err)

	_, err = c.ReportDataNodeTtMsgs(ctx, &datapb.ReportDataNodeTtMsgsRequest{})
	assert.Error(t, err)

	_, err = c.GcControl(ctx, &datapb.GcControlRequest{})
	assert.Error(t, err)

	_, err = c.ImportV2(ctx, &internalpb.ImportRequestInternal{})
	assert.Error(t, err)

	_, err = c.GetImportProgress(ctx, &internalpb.GetImportProgressRequest{})
	assert.Error(t, err)

	_, err = c.ListImports(ctx, &internalpb.ListImportsRequestInternal{})
	assert.Error(t, err)

	c.Close()
}
