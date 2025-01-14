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

package proxy

import (
	"context"

	"go.uber.org/atomic"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/milvus-io/milvus/pkg/util/uniquegenerator"
)

type DataCoordMock struct {
	types.DataCoordClient

	nodeID  typeutil.UniqueID
	address string

	state atomic.Value // internal.StateCode

	getMetricsFunc         getMetricsFuncType
	showConfigurationsFunc showConfigurationsFuncType
	statisticsChannel      string
	timeTickChannel        string
	checkHealthFunc        func(ctx context.Context, req *milvuspb.CheckHealthRequest, opts ...grpc.CallOption) (*milvuspb.CheckHealthResponse, error)
	GetIndexStateFunc      func(ctx context.Context, request *indexpb.GetIndexStateRequest, opts ...grpc.CallOption) (*indexpb.GetIndexStateResponse, error)
	DescribeIndexFunc      func(ctx context.Context, request *indexpb.DescribeIndexRequest, opts ...grpc.CallOption) (*indexpb.DescribeIndexResponse, error)
}

func (coord *DataCoordMock) updateState(state commonpb.StateCode) {
	coord.state.Store(state)
}

func (coord *DataCoordMock) getState() commonpb.StateCode {
	return coord.state.Load().(commonpb.StateCode)
}

func (coord *DataCoordMock) healthy() bool {
	return coord.getState() == commonpb.StateCode_Healthy
}

func (coord *DataCoordMock) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			NodeID:    coord.nodeID,
			Role:      typeutil.DataCoordRole,
			StateCode: coord.getState(),
			ExtraInfo: nil,
		},
		SubcomponentStates: nil,
		Status:             merr.Success(),
	}, nil
}

func (coord *DataCoordMock) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Success(),
		Value:  coord.statisticsChannel,
	}, nil
}

func (coord *DataCoordMock) Register() error {
	return nil
}

func (coord *DataCoordMock) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Success(),
		Value:  coord.timeTickChannel,
	}, nil
}

func (coord *DataCoordMock) Flush(ctx context.Context, req *datapb.FlushRequest, opts ...grpc.CallOption) (*datapb.FlushResponse, error) {
	panic("implement me")
}

func (coord *DataCoordMock) MarkSegmentsDropped(ctx context.Context, req *datapb.MarkSegmentsDroppedRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (coord *DataCoordMock) BroadcastAlteredCollection(ctx context.Context, req *datapb.AlterCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (coord *DataCoordMock) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest, opts ...grpc.CallOption) (*milvuspb.CheckHealthResponse, error) {
	if coord.checkHealthFunc != nil {
		return coord.checkHealthFunc(ctx, req)
	}
	return &milvuspb.CheckHealthResponse{IsHealthy: true}, nil
}

func (coord *DataCoordMock) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest, opts ...grpc.CallOption) (*datapb.AssignSegmentIDResponse, error) {
	panic("implement me")
}

func (coord *DataCoordMock) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest, opts ...grpc.CallOption) (*datapb.GetSegmentStatesResponse, error) {
	panic("implement me")
}

func (coord *DataCoordMock) GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest, opts ...grpc.CallOption) (*datapb.GetInsertBinlogPathsResponse, error) {
	panic("implement me")
}

func (coord *DataCoordMock) GetSegmentInfoChannel(ctx context.Context, in *datapb.GetSegmentInfoChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	panic("implement me")
}

func (coord *DataCoordMock) GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest, opts ...grpc.CallOption) (*datapb.GetCollectionStatisticsResponse, error) {
	panic("implement me")
}

func (coord *DataCoordMock) GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest, opts ...grpc.CallOption) (*datapb.GetPartitionStatisticsResponse, error) {
	panic("implement me")
}

func (coord *DataCoordMock) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest, opts ...grpc.CallOption) (*datapb.GetSegmentInfoResponse, error) {
	panic("implement me")
}

func (coord *DataCoordMock) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest, opts ...grpc.CallOption) (*datapb.GetRecoveryInfoResponse, error) {
	panic("implement me")
}

func (coord *DataCoordMock) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (coord *DataCoordMock) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest, opts ...grpc.CallOption) (*datapb.GetFlushedSegmentsResponse, error) {
	panic("implement me")
}

func (coord *DataCoordMock) GetSegmentsByStates(ctx context.Context, req *datapb.GetSegmentsByStatesRequest, opts ...grpc.CallOption) (*datapb.GetSegmentsByStatesResponse, error) {
	panic("implement me")
}

func (coord *DataCoordMock) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	if !coord.healthy() {
		return &internalpb.ShowConfigurationsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "unhealthy",
			},
		}, nil
	}

	if coord.showConfigurationsFunc != nil {
		return coord.showConfigurationsFunc(ctx, req)
	}

	return &internalpb.ShowConfigurationsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "not implemented",
		},
	}, nil
}

func (coord *DataCoordMock) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	if !coord.healthy() {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "unhealthy",
			},
		}, nil
	}

	if coord.getMetricsFunc != nil {
		return coord.getMetricsFunc(ctx, req)
	}

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "not implemented",
		},
		Response:      "",
		ComponentName: "",
	}, nil
}

func (coord *DataCoordMock) CompleteCompaction(ctx context.Context, req *datapb.CompactionPlanResult, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *DataCoordMock) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest, opts ...grpc.CallOption) (*milvuspb.ManualCompactionResponse, error) {
	return &milvuspb.ManualCompactionResponse{}, nil
}

func (coord *DataCoordMock) GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest, opts ...grpc.CallOption) (*milvuspb.GetCompactionStateResponse, error) {
	return &milvuspb.GetCompactionStateResponse{}, nil
}

func (coord *DataCoordMock) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest, opts ...grpc.CallOption) (*milvuspb.GetCompactionPlansResponse, error) {
	return &milvuspb.GetCompactionPlansResponse{}, nil
}

func (coord *DataCoordMock) WatchChannels(ctx context.Context, req *datapb.WatchChannelsRequest, opts ...grpc.CallOption) (*datapb.WatchChannelsResponse, error) {
	return &datapb.WatchChannelsResponse{}, nil
}

func (coord *DataCoordMock) GetFlushState(ctx context.Context, req *datapb.GetFlushStateRequest, opts ...grpc.CallOption) (*milvuspb.GetFlushStateResponse, error) {
	return &milvuspb.GetFlushStateResponse{}, nil
}

func (coord *DataCoordMock) GetFlushAllState(ctx context.Context, req *milvuspb.GetFlushAllStateRequest, opts ...grpc.CallOption) (*milvuspb.GetFlushAllStateResponse, error) {
	return &milvuspb.GetFlushAllStateResponse{}, nil
}

func (coord *DataCoordMock) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest, opts ...grpc.CallOption) (*datapb.DropVirtualChannelResponse, error) {
	return &datapb.DropVirtualChannelResponse{}, nil
}

func (coord *DataCoordMock) SetSegmentState(ctx context.Context, req *datapb.SetSegmentStateRequest, opts ...grpc.CallOption) (*datapb.SetSegmentStateResponse, error) {
	return &datapb.SetSegmentStateResponse{}, nil
}

func (coord *DataCoordMock) UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *DataCoordMock) UpdateChannelCheckpoint(ctx context.Context, req *datapb.UpdateChannelCheckpointRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *DataCoordMock) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *DataCoordMock) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (coord *DataCoordMock) GetIndexState(ctx context.Context, req *indexpb.GetIndexStateRequest, opts ...grpc.CallOption) (*indexpb.GetIndexStateResponse, error) {
	if coord.GetIndexStateFunc != nil {
		return coord.GetIndexStateFunc(ctx, req, opts...)
	}
	return &indexpb.GetIndexStateResponse{
		Status:     merr.Success(),
		State:      commonpb.IndexState_Finished,
		FailReason: "",
	}, nil
}

// GetSegmentIndexState gets the index state of the segments in the request from RootCoord.
func (coord *DataCoordMock) GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest, opts ...grpc.CallOption) (*indexpb.GetSegmentIndexStateResponse, error) {
	return &indexpb.GetSegmentIndexStateResponse{
		Status: merr.Success(),
	}, nil
}

// GetIndexInfos gets the index files of the IndexBuildIDs in the request from RootCoordinator.
func (coord *DataCoordMock) GetIndexInfos(ctx context.Context, req *indexpb.GetIndexInfoRequest, opts ...grpc.CallOption) (*indexpb.GetIndexInfoResponse, error) {
	return &indexpb.GetIndexInfoResponse{
		Status: merr.Success(),
	}, nil
}

// DescribeIndex describe the index info of the collection.
func (coord *DataCoordMock) DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest, opts ...grpc.CallOption) (*indexpb.DescribeIndexResponse, error) {
	if coord.DescribeIndexFunc != nil {
		return coord.DescribeIndexFunc(ctx, req, opts...)
	}
	return &indexpb.DescribeIndexResponse{
		Status:     merr.Success(),
		IndexInfos: nil,
	}, nil
}

// GetIndexStatistics get the statistics of the index.
func (coord *DataCoordMock) GetIndexStatistics(ctx context.Context, req *indexpb.GetIndexStatisticsRequest, opts ...grpc.CallOption) (*indexpb.GetIndexStatisticsResponse, error) {
	return &indexpb.GetIndexStatisticsResponse{
		Status:     merr.Success(),
		IndexInfos: nil,
	}, nil
}

// GetIndexBuildProgress get the index building progress by num rows.
func (coord *DataCoordMock) GetIndexBuildProgress(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest, opts ...grpc.CallOption) (*indexpb.GetIndexBuildProgressResponse, error) {
	return &indexpb.GetIndexBuildProgressResponse{
		Status: merr.Success(),
	}, nil
}

func (coord *DataCoordMock) Close() error {
	return nil
}

func NewDataCoordMock() *DataCoordMock {
	dc := &DataCoordMock{
		nodeID:            typeutil.UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
		address:           funcutil.GenRandomStr(), // random address
		state:             atomic.Value{},
		getMetricsFunc:    nil,
		statisticsChannel: funcutil.GenRandomStr(),
		timeTickChannel:   funcutil.GenRandomStr(),
	}
	dc.updateState(commonpb.StateCode_Healthy)
	return dc
}
