// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mock

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/indexpb"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

var _ datapb.DataCoordClient = &GrpcDataCoordClient{}

// GrpcDataCoordClient mocks of GrpcDataCoordClient
type GrpcDataCoordClient struct {
	Err error
}

func (m *GrpcDataCoordClient) GcConfirm(ctx context.Context, in *datapb.GcConfirmRequest, opts ...grpc.CallOption) (*datapb.GcConfirmResponse, error) {
	return &datapb.GcConfirmResponse{}, m.Err
}

func (m *GrpcDataCoordClient) CheckHealth(ctx context.Context, in *milvuspb.CheckHealthRequest, opts ...grpc.CallOption) (*milvuspb.CheckHealthResponse, error) {
	return &milvuspb.CheckHealthResponse{}, m.Err
}

func (m *GrpcDataCoordClient) GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{}, m.Err
}

func (m *GrpcDataCoordClient) GetTimeTickChannel(ctx context.Context, in *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, m.Err
}

func (m *GrpcDataCoordClient) GetStatisticsChannel(ctx context.Context, in *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, m.Err
}

func (m *GrpcDataCoordClient) Flush(ctx context.Context, in *datapb.FlushRequest, opts ...grpc.CallOption) (*datapb.FlushResponse, error) {
	return &datapb.FlushResponse{}, m.Err
}

func (m *GrpcDataCoordClient) AssignSegmentID(ctx context.Context, in *datapb.AssignSegmentIDRequest, opts ...grpc.CallOption) (*datapb.AssignSegmentIDResponse, error) {
	return &datapb.AssignSegmentIDResponse{}, m.Err
}

func (m *GrpcDataCoordClient) GetSegmentInfo(ctx context.Context, in *datapb.GetSegmentInfoRequest, opts ...grpc.CallOption) (*datapb.GetSegmentInfoResponse, error) {
	return &datapb.GetSegmentInfoResponse{}, m.Err
}

func (m *GrpcDataCoordClient) GetSegmentStates(ctx context.Context, in *datapb.GetSegmentStatesRequest, opts ...grpc.CallOption) (*datapb.GetSegmentStatesResponse, error) {
	return &datapb.GetSegmentStatesResponse{}, m.Err
}

func (m *GrpcDataCoordClient) GetInsertBinlogPaths(ctx context.Context, in *datapb.GetInsertBinlogPathsRequest, opts ...grpc.CallOption) (*datapb.GetInsertBinlogPathsResponse, error) {
	return &datapb.GetInsertBinlogPathsResponse{}, m.Err
}

func (m *GrpcDataCoordClient) GetCollectionStatistics(ctx context.Context, in *datapb.GetCollectionStatisticsRequest, opts ...grpc.CallOption) (*datapb.GetCollectionStatisticsResponse, error) {
	return &datapb.GetCollectionStatisticsResponse{}, m.Err
}

func (m *GrpcDataCoordClient) GetPartitionStatistics(ctx context.Context, in *datapb.GetPartitionStatisticsRequest, opts ...grpc.CallOption) (*datapb.GetPartitionStatisticsResponse, error) {
	return &datapb.GetPartitionStatisticsResponse{}, m.Err
}

func (m *GrpcDataCoordClient) GetSegmentInfoChannel(ctx context.Context, in *datapb.GetSegmentInfoChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, m.Err
}

func (m *GrpcDataCoordClient) SaveBinlogPaths(ctx context.Context, in *datapb.SaveBinlogPathsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcDataCoordClient) GetRecoveryInfo(ctx context.Context, in *datapb.GetRecoveryInfoRequest, opts ...grpc.CallOption) (*datapb.GetRecoveryInfoResponse, error) {
	return &datapb.GetRecoveryInfoResponse{}, m.Err
}

func (m *GrpcDataCoordClient) GetRecoveryInfoV2(ctx context.Context, in *datapb.GetRecoveryInfoRequestV2, opts ...grpc.CallOption) (*datapb.GetRecoveryInfoResponseV2, error) {
	return &datapb.GetRecoveryInfoResponseV2{}, m.Err
}

func (m *GrpcDataCoordClient) GetFlushedSegments(ctx context.Context, in *datapb.GetFlushedSegmentsRequest, opts ...grpc.CallOption) (*datapb.GetFlushedSegmentsResponse, error) {
	return &datapb.GetFlushedSegmentsResponse{}, m.Err
}

func (m *GrpcDataCoordClient) GetSegmentsByStates(ctx context.Context, in *datapb.GetSegmentsByStatesRequest, opts ...grpc.CallOption) (*datapb.GetSegmentsByStatesResponse, error) {
	return &datapb.GetSegmentsByStatesResponse{}, m.Err
}

func (m *GrpcDataCoordClient) ShowConfigurations(ctx context.Context, in *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	return &internalpb.ShowConfigurationsResponse{}, m.Err
}

func (m *GrpcDataCoordClient) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	return &milvuspb.GetMetricsResponse{}, m.Err
}

func (m *GrpcDataCoordClient) CompleteCompaction(ctx context.Context, req *datapb.CompactionResult, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcDataCoordClient) ManualCompaction(ctx context.Context, in *milvuspb.ManualCompactionRequest, opts ...grpc.CallOption) (*milvuspb.ManualCompactionResponse, error) {
	return &milvuspb.ManualCompactionResponse{}, m.Err
}

func (m *GrpcDataCoordClient) GetCompactionState(ctx context.Context, in *milvuspb.GetCompactionStateRequest, opts ...grpc.CallOption) (*milvuspb.GetCompactionStateResponse, error) {
	return &milvuspb.GetCompactionStateResponse{}, m.Err
}

func (m *GrpcDataCoordClient) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest, opts ...grpc.CallOption) (*milvuspb.GetCompactionPlansResponse, error) {
	return &milvuspb.GetCompactionPlansResponse{}, m.Err
}

func (m *GrpcDataCoordClient) WatchChannels(ctx context.Context, req *datapb.WatchChannelsRequest, opts ...grpc.CallOption) (*datapb.WatchChannelsResponse, error) {
	return &datapb.WatchChannelsResponse{}, m.Err
}

func (m *GrpcDataCoordClient) GetFlushState(ctx context.Context, req *milvuspb.GetFlushStateRequest, opts ...grpc.CallOption) (*milvuspb.GetFlushStateResponse, error) {
	return &milvuspb.GetFlushStateResponse{}, m.Err
}

func (m *GrpcDataCoordClient) GetFlushAllState(ctx context.Context, req *milvuspb.GetFlushAllStateRequest, opts ...grpc.CallOption) (*milvuspb.GetFlushAllStateResponse, error) {
	return &milvuspb.GetFlushAllStateResponse{}, m.Err
}

func (m *GrpcDataCoordClient) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest, opts ...grpc.CallOption) (*datapb.DropVirtualChannelResponse, error) {
	return &datapb.DropVirtualChannelResponse{}, m.Err
}

func (m *GrpcDataCoordClient) SetSegmentState(ctx context.Context, req *datapb.SetSegmentStateRequest, opts ...grpc.CallOption) (*datapb.SetSegmentStateResponse, error) {
	return &datapb.SetSegmentStateResponse{}, m.Err
}

func (m *GrpcDataCoordClient) Import(ctx context.Context, req *datapb.ImportTaskRequest, opts ...grpc.CallOption) (*datapb.ImportTaskResponse, error) {
	return &datapb.ImportTaskResponse{}, m.Err
}

func (m *GrpcDataCoordClient) UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcDataCoordClient) UpdateChannelCheckpoint(ctx context.Context, req *datapb.UpdateChannelCheckpointRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcDataCoordClient) SaveImportSegment(ctx context.Context, in *datapb.SaveImportSegmentRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcDataCoordClient) UnsetIsImportingState(context.Context, *datapb.UnsetIsImportingStateRequest, ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcDataCoordClient) MarkSegmentsDropped(context.Context, *datapb.MarkSegmentsDroppedRequest, ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcDataCoordClient) BroadcastAlteredCollection(ctx context.Context, in *datapb.AlterCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err

}

func (m *GrpcDataCoordClient) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcDataCoordClient) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcDataCoordClient) GetIndexState(ctx context.Context, req *indexpb.GetIndexStateRequest, opts ...grpc.CallOption) (*indexpb.GetIndexStateResponse, error) {
	return &indexpb.GetIndexStateResponse{}, m.Err
}

// GetSegmentIndexState gets the index state of the segments in the request from RootCoord.
func (m *GrpcDataCoordClient) GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest, opts ...grpc.CallOption) (*indexpb.GetSegmentIndexStateResponse, error) {
	return &indexpb.GetSegmentIndexStateResponse{}, m.Err
}

// GetIndexInfos gets the index files of the IndexBuildIDs in the request from RootCoordinator.
func (m *GrpcDataCoordClient) GetIndexInfos(ctx context.Context, req *indexpb.GetIndexInfoRequest, opts ...grpc.CallOption) (*indexpb.GetIndexInfoResponse, error) {
	return &indexpb.GetIndexInfoResponse{}, m.Err
}

// DescribeIndex describe the index info of the collection.
func (m *GrpcDataCoordClient) DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest, opts ...grpc.CallOption) (*indexpb.DescribeIndexResponse, error) {
	return &indexpb.DescribeIndexResponse{}, m.Err
}

// GetIndexStatistics get the information of index.
func (m *GrpcDataCoordClient) GetIndexStatistics(ctx context.Context, in *indexpb.GetIndexStatisticsRequest, opts ...grpc.CallOption) (*indexpb.GetIndexStatisticsResponse, error) {
	return &indexpb.GetIndexStatisticsResponse{}, m.Err
}

// GetIndexBuildProgress get the index building progress by num rows.
func (m *GrpcDataCoordClient) GetIndexBuildProgress(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest, opts ...grpc.CallOption) (*indexpb.GetIndexBuildProgressResponse, error) {
	return &indexpb.GetIndexBuildProgressResponse{}, m.Err
}

func (m *GrpcDataCoordClient) ReportDataNodeTtMsgs(ctx context.Context, in *datapb.ReportDataNodeTtMsgsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}
