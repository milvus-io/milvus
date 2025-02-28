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

package grpcdatanodeclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/mock"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func Test_NewClient(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	client, err := NewClient(ctx, "", 1, false)
	assert.Nil(t, client)
	assert.Error(t, err)

	client, err = NewClient(ctx, "test", 2, false)
	assert.NoError(t, err)
	assert.NotNil(t, client)

	checkFunc := func(retNotNil bool) {
		retCheck := func(notNil bool, ret interface{}, err error) {
			if notNil {
				assert.NotNil(t, ret)
				assert.NoError(t, err)
			} else {
				assert.Nil(t, ret)
				assert.Error(t, err)
			}
		}

		r1, err := client.GetComponentStates(ctx, nil)
		retCheck(retNotNil, r1, err)

		r2, err := client.GetStatisticsChannel(ctx, nil)
		retCheck(retNotNil, r2, err)

		r3, err := client.WatchDmChannels(ctx, nil)
		retCheck(retNotNil, r3, err)

		r4, err := client.FlushSegments(ctx, nil)
		retCheck(retNotNil, r4, err)

		r5, err := client.GetMetrics(ctx, nil)
		retCheck(retNotNil, r5, err)

		r6, err := client.CompactionV2(ctx, nil)
		retCheck(retNotNil, r6, err)

		r8, err := client.ResendSegmentStats(ctx, nil)
		retCheck(retNotNil, r8, err)

		r10, err := client.ShowConfigurations(ctx, nil)
		retCheck(retNotNil, r10, err)

		r11, err := client.GetCompactionState(ctx, nil)
		retCheck(retNotNil, r11, err)

		r12, err := client.NotifyChannelOperation(ctx, nil)
		retCheck(retNotNil, r12, err)

		r13, err := client.CheckChannelOperationProgress(ctx, nil)
		retCheck(retNotNil, r13, err)

		r14, err := client.DropCompactionPlan(ctx, nil)
		retCheck(retNotNil, r14, err)
	}

	client.(*Client).grpcClient = mocks.NewMockGrpcClient[DataNodeClient](t)

	newFunc1 := func(cc *grpc.ClientConn) DataNodeClient {
		return DataNodeClient{
			DataNodeClient:  mocks.NewMockDataNodeClient(t),
			IndexNodeClient: mocks.NewMockDataNodeClient(t),
		}
	}
	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc1)

	checkFunc(false)

	client.(*Client).grpcClient = mocks.NewMockGrpcClient[DataNodeClient](t)

	newFunc2 := func(cc *grpc.ClientConn) DataNodeClient {
		return DataNodeClient{
			DataNodeClient:  mocks.NewMockDataNodeClient(t),
			IndexNodeClient: mocks.NewMockDataNodeClient(t),
		}
	}

	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc2)

	checkFunc(false)

	client.(*Client).grpcClient = mocks.NewMockGrpcClient[DataNodeClient](t)

	newFunc3 := func(cc *grpc.ClientConn) DataNodeClient {
		return DataNodeClient{
			DataNodeClient:  mocks.NewMockDataNodeClient(t),
			IndexNodeClient: mocks.NewMockDataNodeClient(t),
		}
	}
	client.(*Client).grpcClient.SetNewGrpcClientFunc(newFunc3)

	checkFunc(true)

	err = client.Close()
	assert.NoError(t, err)
}

func TestIndexClient(t *testing.T) {
	ctx := context.Background()
	client, err := NewClient(ctx, "localhost:1234", 1, false)
	assert.NoError(t, err)
	assert.NotNil(t, client)

	mockIN := mocks.NewMockDataNodeClient(t)

	mockGrpcClient := mocks.NewMockGrpcClient[DataNodeClient](t)
	mockGrpcClient.EXPECT().Close().Return(nil)
	client.(*Client).grpcClient = mockGrpcClient

	t.Run("GetComponentStates", func(t *testing.T) {
		mockIN.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := client.GetComponentStates(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		mockIN.EXPECT().GetStatisticsChannel(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := client.GetStatisticsChannel(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CreatJob", func(t *testing.T) {
		mockIN.EXPECT().CreateJob(mock.Anything, mock.Anything).Return(nil, nil)

		req := &workerpb.CreateJobRequest{
			ClusterID: "0",
			BuildID:   0,
		}
		_, err := client.CreateJob(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("QueryJob", func(t *testing.T) {
		mockIN.EXPECT().QueryJobs(mock.Anything, mock.Anything).Return(nil, nil)

		req := &workerpb.QueryJobsRequest{}
		_, err := client.QueryJobs(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("DropJob", func(t *testing.T) {
		mockIN.EXPECT().DropJobs(mock.Anything, mock.Anything).Return(nil, nil)

		req := &workerpb.DropJobsRequest{}
		_, err := client.DropJobs(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("ShowConfigurations", func(t *testing.T) {
		mockIN.EXPECT().ShowConfigurations(mock.Anything, mock.Anything).Return(nil, nil)

		req := &internalpb.ShowConfigurationsRequest{
			Pattern: "",
		}
		_, err := client.ShowConfigurations(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		mockIN.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(nil, nil)

		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.NoError(t, err)
		_, err = client.GetMetrics(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("GetJobStats", func(t *testing.T) {
		mockIN.EXPECT().GetJobStats(mock.Anything, mock.Anything).Return(nil, nil)

		req := &workerpb.GetJobStatsRequest{}
		_, err := client.GetJobStats(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("CreateJobV2", func(t *testing.T) {
		mockIN.EXPECT().CreateJobV2(mock.Anything, mock.Anything).Return(nil, nil)

		req := &workerpb.CreateJobV2Request{}
		_, err := client.CreateJobV2(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("QueryJobsV2", func(t *testing.T) {
		mockIN.EXPECT().QueryJobsV2(mock.Anything, mock.Anything).Return(nil, nil)

		req := &workerpb.QueryJobsV2Request{}
		_, err := client.QueryJobsV2(ctx, req)
		assert.NoError(t, err)
	})

	t.Run("DropJobsV2", func(t *testing.T) {
		mockIN.EXPECT().DropJobsV2(mock.Anything, mock.Anything).Return(nil, nil)

		req := &workerpb.DropJobsV2Request{}
		_, err := client.DropJobsV2(ctx, req)
		assert.NoError(t, err)
	})

	err = client.Close()
	assert.NoError(t, err)
}
