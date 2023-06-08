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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/milvus-io/milvus/pkg/util/uniquegenerator"
	"go.uber.org/atomic"
)

type DataCoordMock struct {
	types.DataCoord

	nodeID  typeutil.UniqueID
	address string

	state atomic.Value // internal.StateCode

	getMetricsFunc         getMetricsFuncType
	showConfigurationsFunc showConfigurationsFuncType
	statisticsChannel      string
	timeTickChannel        string
	checkHealthFunc        func(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error)
	GetIndexStateFunc      func(ctx context.Context, request *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error)
	DescribeIndexFunc      func(ctx context.Context, request *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error)
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

func (coord *DataCoordMock) Init() error {
	coord.updateState(commonpb.StateCode_Initializing)
	return nil
}

func (coord *DataCoordMock) Start() error {
	defer coord.updateState(commonpb.StateCode_Healthy)

	return nil
}

func (coord *DataCoordMock) Stop() error {
	defer coord.updateState(commonpb.StateCode_Abnormal)

	return nil
}

func (coord *DataCoordMock) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			NodeID:    coord.nodeID,
			Role:      typeutil.DataCoordRole,
			StateCode: coord.getState(),
			ExtraInfo: nil,
		},
		SubcomponentStates: nil,
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

func (coord *DataCoordMock) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: coord.statisticsChannel,
	}, nil
}

func (coord *DataCoordMock) Register() error {
	return nil
}

func (coord *DataCoordMock) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: coord.timeTickChannel,
	}, nil
}

func (coord *DataCoordMock) Flush(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error) {
	panic("implement me")
}

func (coord *DataCoordMock) SaveImportSegment(ctx context.Context, req *datapb.SaveImportSegmentRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (coord *DataCoordMock) UnsetIsImportingState(context.Context, *datapb.UnsetIsImportingStateRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (coord *DataCoordMock) MarkSegmentsDropped(ctx context.Context, req *datapb.MarkSegmentsDroppedRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (coord *DataCoordMock) BroadcastAlteredCollection(ctx context.Context, req *datapb.AlterCollectionRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (coord *DataCoordMock) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	if coord.checkHealthFunc != nil {
		return coord.checkHealthFunc(ctx, req)
	}
	return &milvuspb.CheckHealthResponse{IsHealthy: true}, nil
}

func (coord *DataCoordMock) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error) {
	panic("implement me")
}

func (coord *DataCoordMock) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
	panic("implement me")
}

func (coord *DataCoordMock) GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest) (*datapb.GetInsertBinlogPathsResponse, error) {
	panic("implement me")
}

func (coord *DataCoordMock) GetSegmentInfoChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	panic("implement me")
}

func (coord *DataCoordMock) GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest) (*datapb.GetCollectionStatisticsResponse, error) {
	panic("implement me")
}

func (coord *DataCoordMock) GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest) (*datapb.GetPartitionStatisticsResponse, error) {
	panic("implement me")
}

func (coord *DataCoordMock) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
	panic("implement me")
}

func (coord *DataCoordMock) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error) {
	panic("implement me")
}

func (coord *DataCoordMock) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (coord *DataCoordMock) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
	panic("implement me")
}

func (coord *DataCoordMock) GetSegmentsByStates(ctx context.Context, req *datapb.GetSegmentsByStatesRequest) (*datapb.GetSegmentsByStatesResponse, error) {
	panic("implement me")
}

func (coord *DataCoordMock) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
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

func (coord *DataCoordMock) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
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

func (coord *DataCoordMock) CompleteCompaction(ctx context.Context, req *datapb.CompactionResult) (*commonpb.Status, error) {
	return &commonpb.Status{}, nil
}

func (coord *DataCoordMock) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error) {
	return &milvuspb.ManualCompactionResponse{}, nil
}

func (coord *DataCoordMock) GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest) (*milvuspb.GetCompactionStateResponse, error) {
	return &milvuspb.GetCompactionStateResponse{}, nil
}

func (coord *DataCoordMock) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest) (*milvuspb.GetCompactionPlansResponse, error) {
	return &milvuspb.GetCompactionPlansResponse{}, nil
}

func (coord *DataCoordMock) WatchChannels(ctx context.Context, req *datapb.WatchChannelsRequest) (*datapb.WatchChannelsResponse, error) {
	return &datapb.WatchChannelsResponse{}, nil
}

func (coord *DataCoordMock) GetFlushState(ctx context.Context, req *milvuspb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
	return &milvuspb.GetFlushStateResponse{}, nil
}

func (coord *DataCoordMock) GetFlushAllState(ctx context.Context, req *milvuspb.GetFlushAllStateRequest) (*milvuspb.GetFlushAllStateResponse, error) {
	return &milvuspb.GetFlushAllStateResponse{}, nil
}

func (coord *DataCoordMock) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest) (*datapb.DropVirtualChannelResponse, error) {
	return &datapb.DropVirtualChannelResponse{}, nil
}

func (coord *DataCoordMock) SetSegmentState(ctx context.Context, req *datapb.SetSegmentStateRequest) (*datapb.SetSegmentStateResponse, error) {
	return &datapb.SetSegmentStateResponse{}, nil
}

func (coord *DataCoordMock) Import(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
	return &datapb.ImportTaskResponse{}, nil
}

func (coord *DataCoordMock) UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (coord *DataCoordMock) UpdateChannelCheckpoint(ctx context.Context, req *datapb.UpdateChannelCheckpointRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (coord *DataCoordMock) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (coord *DataCoordMock) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (coord *DataCoordMock) GetIndexState(ctx context.Context, req *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error) {
	return &indexpb.GetIndexStateResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		State:      commonpb.IndexState_Finished,
		FailReason: "",
	}, nil
}

// GetSegmentIndexState gets the index state of the segments in the request from RootCoord.
func (coord *DataCoordMock) GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error) {
	return &indexpb.GetSegmentIndexStateResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

// GetIndexInfos gets the index files of the IndexBuildIDs in the request from RootCoordinator.
func (coord *DataCoordMock) GetIndexInfos(ctx context.Context, req *indexpb.GetIndexInfoRequest) (*indexpb.GetIndexInfoResponse, error) {
	return &indexpb.GetIndexInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

// DescribeIndex describe the index info of the collection.
func (coord *DataCoordMock) DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error) {
	return &indexpb.DescribeIndexResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		IndexInfos: nil,
	}, nil
}

// GetIndexStatistics get the statistics of the index.
func (coord *DataCoordMock) GetIndexStatistics(ctx context.Context, req *indexpb.GetIndexStatisticsRequest) (*indexpb.GetIndexStatisticsResponse, error) {
	return &indexpb.GetIndexStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		IndexInfos: nil,
	}, nil
}

// GetIndexBuildProgress get the index building progress by num rows.
func (coord *DataCoordMock) GetIndexBuildProgress(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest) (*indexpb.GetIndexBuildProgressResponse, error) {
	return &indexpb.GetIndexBuildProgressResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

func NewDataCoordMock() *DataCoordMock {
	return &DataCoordMock{
		nodeID:            typeutil.UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
		address:           funcutil.GenRandomStr(), // random address
		state:             atomic.Value{},
		getMetricsFunc:    nil,
		statisticsChannel: funcutil.GenRandomStr(),
		timeTickChannel:   funcutil.GenRandomStr(),
	}
}
