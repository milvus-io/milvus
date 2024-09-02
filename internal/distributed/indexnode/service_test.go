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

package grpcindexnode

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestIndexNodeServer(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	factory := dependency.NewDefaultFactory(true)
	server, err := NewServer(ctx, factory)
	assert.NoError(t, err)
	assert.NotNil(t, server)

	inm := mocks.NewMockIndexNode(t)
	inm.EXPECT().SetEtcdClient(mock.Anything).Return()
	inm.EXPECT().SetAddress(mock.Anything).Return()
	inm.EXPECT().Start().Return(nil)
	inm.EXPECT().Init().Return(nil)
	inm.EXPECT().Register().Return(nil)
	inm.EXPECT().Stop().Return(nil)
	err = server.setServer(inm)
	assert.NoError(t, err)

	err = server.Run()
	assert.NoError(t, err)

	t.Run("GetComponentStates", func(t *testing.T) {
		inm.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
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
		inm.EXPECT().GetStatisticsChannel(mock.Anything, mock.Anything).Return(&milvuspb.StringResponse{
			Status: merr.Success(),
		}, nil)
		req := &internalpb.GetStatisticsChannelRequest{}
		resp, err := server.GetStatisticsChannel(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("CreateJob", func(t *testing.T) {
		inm.EXPECT().CreateJob(mock.Anything, mock.Anything).Return(merr.Success(), nil)
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
		inm.EXPECT().QueryJobs(mock.Anything, mock.Anything).Return(&workerpb.QueryJobsResponse{
			Status: merr.Success(),
		}, nil)
		req := &workerpb.QueryJobsRequest{}
		resp, err := server.QueryJobs(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("DropJobs", func(t *testing.T) {
		inm.EXPECT().DropJobs(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		req := &workerpb.DropJobsRequest{}
		resp, err := server.DropJobs(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("ShowConfigurations", func(t *testing.T) {
		inm.EXPECT().ShowConfigurations(mock.Anything, mock.Anything).Return(&internalpb.ShowConfigurationsResponse{
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
		inm.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status: merr.Success(),
		}, nil)
		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.NoError(t, err)
		resp, err := server.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("GetTaskSlots", func(t *testing.T) {
		inm.EXPECT().GetJobStats(mock.Anything, mock.Anything).Return(&workerpb.GetJobStatsResponse{
			Status: merr.Success(),
		}, nil)
		req := &workerpb.GetJobStatsRequest{}
		resp, err := server.GetJobStats(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("CreateJobV2", func(t *testing.T) {
		inm.EXPECT().CreateJobV2(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		req := &workerpb.CreateJobV2Request{}
		resp, err := server.CreateJobV2(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("QueryJobsV2", func(t *testing.T) {
		inm.EXPECT().QueryJobsV2(mock.Anything, mock.Anything).Return(&workerpb.QueryJobsV2Response{
			Status: merr.Success(),
		}, nil)
		req := &workerpb.QueryJobsV2Request{}
		resp, err := server.QueryJobsV2(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("DropJobsV2", func(t *testing.T) {
		inm.EXPECT().DropJobsV2(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		req := &workerpb.DropJobsV2Request{}
		resp, err := server.DropJobsV2(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	err = server.Stop()
	assert.NoError(t, err)
}
