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

package grpcdatanode

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func Test_NewServer(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.NoError(t, err)
	assert.NotNil(t, server)

	mockMixCoord := mocks.NewMockMixCoordClient(t)
	mockMixCoord.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			StateCode: commonpb.StateCode_Healthy,
		},
		Status: merr.Success(),
		SubcomponentStates: []*milvuspb.ComponentInfo{
			{
				StateCode: commonpb.StateCode_Healthy,
			},
		},
	}, nil)
	server.mixCoordClient = func() (types.MixCoordClient, error) {
		return mockMixCoord, nil
	}

	t.Run("Run", func(t *testing.T) {
		datanode := mocks.NewMockDataNode(t)
		datanode.EXPECT().SetEtcdClient(mock.Anything).Return()
		datanode.EXPECT().SetAddress(mock.Anything).Return()
		datanode.EXPECT().SetMixCoordClient(mock.Anything).Return(nil)
		datanode.EXPECT().UpdateStateCode(mock.Anything).Return()
		datanode.EXPECT().Register().Return(nil)
		datanode.EXPECT().Init().Return(nil)
		datanode.EXPECT().Start().Return(nil)
		datanode.EXPECT().GetStateCode().Return(commonpb.StateCode_Healthy)
		server.datanode = datanode
		err = server.Prepare()
		assert.NoError(t, err)
		err = server.Run()
		assert.NoError(t, err)
	})

	t.Run("GetComponentStates", func(t *testing.T) {
		datanode := mocks.NewMockDataNode(t)
		datanode.EXPECT().GetComponentStates(mock.Anything, mock.Anything).
			Return(&milvuspb.ComponentStates{State: &milvuspb.ComponentInfo{StateCode: commonpb.StateCode_Healthy}}, nil)
		server.datanode = datanode
		states, err := server.GetComponentStates(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, states)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		datanode := mocks.NewMockDataNode(t)
		datanode.EXPECT().GetStatisticsChannel(mock.Anything, mock.Anything).
			Return(&milvuspb.StringResponse{Status: merr.Success()}, nil)
		server.datanode = datanode
		states, err := server.GetStatisticsChannel(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, states)
	})

	t.Run("WatchDmChannels", func(t *testing.T) {
		datanode := mocks.NewMockDataNode(t)
		datanode.EXPECT().WatchDmChannels(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		server.datanode = datanode
		states, err := server.WatchDmChannels(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, states)
	})

	t.Run("FlushSegments", func(t *testing.T) {
		datanode := mocks.NewMockDataNode(t)
		datanode.EXPECT().GetStateCode().Return(commonpb.StateCode_Healthy)
		datanode.EXPECT().FlushSegments(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		server.datanode = datanode
		states, err := server.FlushSegments(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, states)
	})

	t.Run("ShowConfigurations", func(t *testing.T) {
		datanode := mocks.NewMockDataNode(t)
		datanode.EXPECT().ShowConfigurations(mock.Anything, mock.Anything).Return(&internalpb.ShowConfigurationsResponse{}, nil)
		server.datanode = datanode
		resp, err := server.ShowConfigurations(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		datanode := mocks.NewMockDataNode(t)
		datanode.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{}, nil)
		server.datanode = datanode
		resp, err := server.GetMetrics(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("Compaction", func(t *testing.T) {
		datanode := mocks.NewMockDataNode(t)
		datanode.EXPECT().CompactionV2(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		server.datanode = datanode
		resp, err := server.CompactionV2(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("ResendSegmentStats", func(t *testing.T) {
		datanode := mocks.NewMockDataNode(t)
		datanode.EXPECT().ResendSegmentStats(mock.Anything, mock.Anything).Return(&datapb.ResendSegmentStatsResponse{}, nil)
		server.datanode = datanode
		resp, err := server.ResendSegmentStats(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("NotifyChannelOperation", func(t *testing.T) {
		datanode := mocks.NewMockDataNode(t)
		datanode.EXPECT().NotifyChannelOperation(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		server.datanode = datanode
		resp, err := server.NotifyChannelOperation(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("CheckChannelOperationProgress", func(t *testing.T) {
		datanode := mocks.NewMockDataNode(t)
		datanode.EXPECT().CheckChannelOperationProgress(mock.Anything, mock.Anything).Return(&datapb.ChannelOperationProgressResponse{}, nil)
		server.datanode = datanode
		resp, err := server.CheckChannelOperationProgress(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("DropCompactionPlans", func(t *testing.T) {
		datanode := mocks.NewMockDataNode(t)
		datanode.EXPECT().DropCompactionPlan(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		server.datanode = datanode
		resp, err := server.DropCompactionPlan(ctx, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	server.datanode.(*mocks.MockDataNode).EXPECT().Stop().Return(nil)
	err = server.Stop()
	assert.NoError(t, err)
}

func Test_Run(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.NoError(t, err)
	assert.NotNil(t, server)

	mockRootCoord := mocks.NewMockMixCoordClient(t)
	mockRootCoord.EXPECT().GetComponentStates(mock.Anything, mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			StateCode: commonpb.StateCode_Healthy,
		},
		Status: merr.Success(),
		SubcomponentStates: []*milvuspb.ComponentInfo{
			{
				StateCode: commonpb.StateCode_Healthy,
			},
		},
	}, nil)
	server.mixCoordClient = func() (types.MixCoordClient, error) {
		return mockRootCoord, nil
	}

	datanode := mocks.NewMockDataNode(t)
	datanode.EXPECT().SetEtcdClient(mock.Anything).Return()
	datanode.EXPECT().SetAddress(mock.Anything).Return()
	datanode.EXPECT().SetMixCoordClient(mock.Anything).Return(nil)
	datanode.EXPECT().UpdateStateCode(mock.Anything).Return()
	datanode.EXPECT().Init().Return(errors.New("mock err"))
	server.datanode = datanode

	err = server.Prepare()
	assert.NoError(t, err)
	err = server.Run()
	assert.Error(t, err)

	datanode = mocks.NewMockDataNode(t)
	datanode.EXPECT().SetEtcdClient(mock.Anything).Return()
	datanode.EXPECT().SetAddress(mock.Anything).Return()
	datanode.EXPECT().SetMixCoordClient(mock.Anything).Return(nil)
	datanode.EXPECT().UpdateStateCode(mock.Anything).Return()
	datanode.EXPECT().Register().Return(nil)
	datanode.EXPECT().Init().Return(nil)
	datanode.EXPECT().Start().Return(nil)
	datanode.EXPECT().GetStateCode().Return(commonpb.StateCode_Healthy)
	server.datanode = datanode

	err = server.Run()
	assert.NoError(t, err)
}

func TestIndexService(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.NoError(t, err)
	assert.NotNil(t, server)

	mockRootCoord := mocks.NewMockMixCoordClient(t)
	mockRootCoord.EXPECT().GetComponentStates(mock.Anything, mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			StateCode: commonpb.StateCode_Healthy,
		},
		Status: merr.Success(),
		SubcomponentStates: []*milvuspb.ComponentInfo{
			{
				StateCode: commonpb.StateCode_Healthy,
			},
		},
	}, nil)
	server.mixCoordClient = func() (types.MixCoordClient, error) {
		return mockRootCoord, nil
	}

	dn := mocks.NewMockDataNode(t)
	dn.EXPECT().SetEtcdClient(mock.Anything).Return()
	dn.EXPECT().SetAddress(mock.Anything).Return()
	dn.EXPECT().SetMixCoordClient(mock.Anything).Return(nil)
	dn.EXPECT().UpdateStateCode(mock.Anything).Return()
	dn.EXPECT().Register().Return(nil)
	dn.EXPECT().Init().Return(nil)
	dn.EXPECT().Start().Return(nil)
	dn.EXPECT().GetStateCode().Return(commonpb.StateCode_Healthy)
	server.datanode = dn

	err = server.Prepare()
	assert.NoError(t, err)
	err = server.Run()
	assert.NoError(t, err)

	t.Run("GetComponentStates", func(t *testing.T) {
		dn.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
			State: &milvuspb.ComponentInfo{
				StateCode: commonpb.StateCode_Healthy,
			},
		}, nil)
		req := &milvuspb.GetComponentStatesRequest{}
		states, err := server.GetComponentStates(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.StateCode_Healthy, states.State.StateCode)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		dn.EXPECT().GetStatisticsChannel(mock.Anything, mock.Anything).Return(&milvuspb.StringResponse{
			Status: merr.Success(),
		}, nil)
		req := &internalpb.GetStatisticsChannelRequest{}
		resp, err := server.GetStatisticsChannel(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("CreateJob", func(t *testing.T) {
		dn.EXPECT().CreateJob(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		req := &workerpb.CreateJobRequest{
			ClusterID: "",
			BuildID:   0,
			IndexID:   0,
			DataPaths: []string{},
		}
		resp, err := server.CreateJob(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("QueryJob", func(t *testing.T) {
		dn.EXPECT().QueryJobs(mock.Anything, mock.Anything).Return(&workerpb.QueryJobsResponse{
			Status: merr.Success(),
		}, nil)
		req := &workerpb.QueryJobsRequest{}
		resp, err := server.QueryJobs(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("DropJobs", func(t *testing.T) {
		dn.EXPECT().DropJobs(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		req := &workerpb.DropJobsRequest{}
		resp, err := server.DropJobs(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("ShowConfigurations", func(t *testing.T) {
		dn.EXPECT().ShowConfigurations(mock.Anything, mock.Anything).Return(&internalpb.ShowConfigurationsResponse{
			Status: merr.Success(),
		}, nil)
		req := &internalpb.ShowConfigurationsRequest{
			Pattern: "",
		}
		resp, err := server.ShowConfigurations(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("GetMetrics", func(t *testing.T) {
		dn.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status: merr.Success(),
		}, nil)
		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.NoError(t, err)
		resp, err := server.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("GetTaskSlots", func(t *testing.T) {
		dn.EXPECT().GetJobStats(mock.Anything, mock.Anything).Return(&workerpb.GetJobStatsResponse{
			Status: merr.Success(),
		}, nil)
		req := &workerpb.GetJobStatsRequest{}
		resp, err := server.GetJobStats(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("CreateJobV2", func(t *testing.T) {
		dn.EXPECT().CreateJobV2(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		req := &workerpb.CreateJobV2Request{}
		resp, err := server.CreateJobV2(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("QueryJobsV2", func(t *testing.T) {
		dn.EXPECT().QueryJobsV2(mock.Anything, mock.Anything).Return(&workerpb.QueryJobsV2Response{
			Status: merr.Success(),
		}, nil)
		req := &workerpb.QueryJobsV2Request{}
		resp, err := server.QueryJobsV2(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("DropJobsV2", func(t *testing.T) {
		dn.EXPECT().DropJobsV2(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		req := &workerpb.DropJobsV2Request{}
		resp, err := server.DropJobsV2(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	server.datanode.(*mocks.MockDataNode).EXPECT().Stop().Return(nil)
	err = server.Stop()
	assert.NoError(t, err)
}
