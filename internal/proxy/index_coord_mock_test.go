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
	"errors"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/log"

	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/uniquegenerator"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type IndexCoordMock struct {
	nodeID  typeutil.UniqueID
	address string

	state atomic.Value // internal.StateCode

	showConfigurationsFunc showConfigurationsFuncType
	getMetricsFunc         getMetricsFuncType

	statisticsChannel string
	timeTickChannel   string

	minioBucketName string
}

func (coord *IndexCoordMock) updateState(state internalpb.StateCode) {
	coord.state.Store(state)
}

func (coord *IndexCoordMock) getState() internalpb.StateCode {
	return coord.state.Load().(internalpb.StateCode)
}

func (coord *IndexCoordMock) healthy() bool {
	return coord.getState() == internalpb.StateCode_Healthy
}

func (coord *IndexCoordMock) Init() error {
	coord.updateState(internalpb.StateCode_Initializing)
	return nil
}

func (coord *IndexCoordMock) Start() error {
	defer coord.updateState(internalpb.StateCode_Healthy)

	return nil
}

func (coord *IndexCoordMock) Stop() error {
	defer coord.updateState(internalpb.StateCode_Abnormal)

	return nil
}

func (coord *IndexCoordMock) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{
		State: &internalpb.ComponentInfo{
			NodeID:    coord.nodeID,
			Role:      typeutil.IndexCoordRole,
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

func (coord *IndexCoordMock) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: coord.statisticsChannel,
	}, nil
}

func (coord *IndexCoordMock) Register() error {
	return nil
}

func (coord *IndexCoordMock) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: coord.timeTickChannel,
	}, nil
}

func (coord *IndexCoordMock) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (coord *IndexCoordMock) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (coord *IndexCoordMock) GetIndexState(ctx context.Context, req *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error) {
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
func (coord *IndexCoordMock) GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error) {
	return &indexpb.GetSegmentIndexStateResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

// GetIndexInfos gets the index files of the IndexBuildIDs in the request from RootCoordinator.
func (coord *IndexCoordMock) GetIndexInfos(ctx context.Context, req *indexpb.GetIndexInfoRequest) (*indexpb.GetIndexInfoResponse, error) {
	return &indexpb.GetIndexInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

// DescribeIndex describe the index info of the collection.
func (coord *IndexCoordMock) DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error) {
	return &indexpb.DescribeIndexResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		IndexInfos: nil,
	}, nil
}

// GetIndexBuildProgress get the index building progress by num rows.
func (coord *IndexCoordMock) GetIndexBuildProgress(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest) (*indexpb.GetIndexBuildProgressResponse, error) {
	return &indexpb.GetIndexBuildProgressResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

func (coord *IndexCoordMock) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
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

func (coord *IndexCoordMock) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
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

func NewIndexCoordMock() *IndexCoordMock {
	return &IndexCoordMock{
		nodeID:            typeutil.UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
		address:           funcutil.GenRandomStr(), // TODO(dragondriver): random address
		statisticsChannel: funcutil.GenRandomStr(),
		timeTickChannel:   funcutil.GenRandomStr(),
		minioBucketName:   funcutil.GenRandomStr(),
	}
}

type GetIndexStateFunc func(ctx context.Context, request *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error)

type mockIndexCoord struct {
	types.IndexCoord
	GetIndexStateFunc
}

func (m *mockIndexCoord) GetIndexState(ctx context.Context, request *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error) {
	if m.GetIndexStateFunc != nil {
		log.Warn("func not nil")
		return m.GetIndexStateFunc(ctx, request)
	}
	log.Warn("func nil")
	return nil, errors.New("mock")
}

func newMockIndexCoord() *mockIndexCoord {
	return &mockIndexCoord{}
}
