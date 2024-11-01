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

package indexnode

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
)

func TestAbnormalIndexNode(t *testing.T) {
	in, err := NewMockIndexNodeComponent(context.TODO())
	assert.NoError(t, err)
	assert.Nil(t, in.Stop())
	ctx := context.TODO()
	status, err := in.CreateJob(ctx, &workerpb.CreateJobRequest{})
	assert.NoError(t, err)
	assert.ErrorIs(t, merr.Error(status), merr.ErrServiceNotReady)

	qresp, err := in.QueryJobs(ctx, &workerpb.QueryJobsRequest{})
	assert.NoError(t, err)
	assert.ErrorIs(t, merr.Error(qresp.GetStatus()), merr.ErrServiceNotReady)

	status, err = in.DropJobs(ctx, &workerpb.DropJobsRequest{})
	assert.NoError(t, err)
	assert.ErrorIs(t, merr.Error(status), merr.ErrServiceNotReady)

	jobNumRsp, err := in.GetJobStats(ctx, &workerpb.GetJobStatsRequest{})
	assert.NoError(t, err)
	assert.ErrorIs(t, merr.Error(jobNumRsp.GetStatus()), merr.ErrServiceNotReady)

	metricsResp, err := in.GetMetrics(ctx, &milvuspb.GetMetricsRequest{})
	err = merr.CheckRPCCall(metricsResp, err)
	assert.ErrorIs(t, err, merr.ErrServiceNotReady)

	configurationResp, err := in.ShowConfigurations(ctx, &internalpb.ShowConfigurationsRequest{})
	err = merr.CheckRPCCall(configurationResp, err)
	assert.ErrorIs(t, err, merr.ErrServiceNotReady)
}

func TestGetMetrics(t *testing.T) {
	var (
		ctx          = context.TODO()
		metricReq, _ = metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
	)
	in, err := NewMockIndexNodeComponent(ctx)
	assert.NoError(t, err)
	defer in.Stop()
	resp, err := in.GetMetrics(ctx, metricReq)
	assert.NoError(t, err)
	assert.True(t, merr.Ok(resp.GetStatus()))
	t.Logf("Component: %s, Metrics: %s", resp.ComponentName, resp.Response)
}

func TestGetMetricsError(t *testing.T) {
	ctx := context.TODO()

	in, err := NewMockIndexNodeComponent(ctx)
	assert.NoError(t, err)
	defer in.Stop()
	errReq := &milvuspb.GetMetricsRequest{
		Request: `{"metric_typ": "system_info"}`,
	}
	resp, err := in.GetMetrics(ctx, errReq)
	assert.NoError(t, err)
	assert.Equal(t, resp.GetStatus().GetErrorCode(), commonpb.ErrorCode_UnexpectedError)

	unsupportedReq := &milvuspb.GetMetricsRequest{
		Request: `{"metric_type": "application_info"}`,
	}
	resp, err = in.GetMetrics(ctx, unsupportedReq)
	assert.NoError(t, err)
	assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrMetricNotFound)
}

func TestMockFieldData(t *testing.T) {
	chunkMgr := NewMockChunkManager()

	chunkMgr.mockFieldData(100000, 8, 0, 0, 1)
}

type IndexNodeServiceSuite struct {
	suite.Suite
	cluster      string
	collectionID int64
	partitionID  int64
	taskID       int64
	fieldID      int64
	segmentID    int64
}

func (suite *IndexNodeServiceSuite) SetupTest() {
	suite.cluster = "test_cluster"
	suite.collectionID = 100
	suite.partitionID = 102
	suite.taskID = 11111
	suite.fieldID = 103
	suite.segmentID = 104
}

func (suite *IndexNodeServiceSuite) Test_AbnormalIndexNode() {
	in, err := NewMockIndexNodeComponent(context.TODO())
	suite.NoError(err)
	suite.Nil(in.Stop())

	ctx := context.TODO()
	status, err := in.CreateJob(ctx, &workerpb.CreateJobRequest{})
	suite.NoError(err)
	suite.ErrorIs(merr.Error(status), merr.ErrServiceNotReady)

	qresp, err := in.QueryJobs(ctx, &workerpb.QueryJobsRequest{})
	suite.NoError(err)
	suite.ErrorIs(merr.Error(qresp.GetStatus()), merr.ErrServiceNotReady)

	status, err = in.DropJobs(ctx, &workerpb.DropJobsRequest{})
	suite.NoError(err)
	suite.ErrorIs(merr.Error(status), merr.ErrServiceNotReady)

	jobNumRsp, err := in.GetJobStats(ctx, &workerpb.GetJobStatsRequest{})
	suite.NoError(err)
	suite.ErrorIs(merr.Error(jobNumRsp.GetStatus()), merr.ErrServiceNotReady)

	metricsResp, err := in.GetMetrics(ctx, &milvuspb.GetMetricsRequest{})
	err = merr.CheckRPCCall(metricsResp, err)
	suite.ErrorIs(err, merr.ErrServiceNotReady)

	configurationResp, err := in.ShowConfigurations(ctx, &internalpb.ShowConfigurationsRequest{})
	err = merr.CheckRPCCall(configurationResp, err)
	suite.ErrorIs(err, merr.ErrServiceNotReady)

	status, err = in.CreateJobV2(ctx, &workerpb.CreateJobV2Request{})
	err = merr.CheckRPCCall(status, err)
	suite.ErrorIs(err, merr.ErrServiceNotReady)

	queryAnalyzeResultResp, err := in.QueryJobsV2(ctx, &workerpb.QueryJobsV2Request{})
	err = merr.CheckRPCCall(queryAnalyzeResultResp, err)
	suite.ErrorIs(err, merr.ErrServiceNotReady)

	dropAnalyzeTasksResp, err := in.DropJobsV2(ctx, &workerpb.DropJobsV2Request{})
	err = merr.CheckRPCCall(dropAnalyzeTasksResp, err)
	suite.ErrorIs(err, merr.ErrServiceNotReady)
}

func (suite *IndexNodeServiceSuite) Test_Method() {
	ctx := context.TODO()
	in, err := NewMockIndexNodeComponent(context.TODO())
	suite.NoError(err)
	suite.NoError(in.Stop())

	in.UpdateStateCode(commonpb.StateCode_Healthy)

	suite.Run("CreateJobV2", func() {
		req := &workerpb.AnalyzeRequest{
			ClusterID:    suite.cluster,
			TaskID:       suite.taskID,
			CollectionID: suite.collectionID,
			PartitionID:  suite.partitionID,
			FieldID:      suite.fieldID,
			SegmentStats: map[int64]*indexpb.SegmentStats{
				suite.segmentID: {
					ID:      suite.segmentID,
					NumRows: 1024,
					LogIDs:  []int64{1, 2, 3},
				},
			},
			Version:       1,
			StorageConfig: nil,
		}

		resp, err := in.CreateJobV2(ctx, &workerpb.CreateJobV2Request{
			ClusterID: suite.cluster,
			TaskID:    suite.taskID,
			JobType:   indexpb.JobType_JobTypeAnalyzeJob,
			Request: &workerpb.CreateJobV2Request_AnalyzeRequest{
				AnalyzeRequest: req,
			},
		})
		err = merr.CheckRPCCall(resp, err)
		suite.NoError(err)
	})

	suite.Run("QueryJobsV2", func() {
		req := &workerpb.QueryJobsV2Request{
			ClusterID: suite.cluster,
			TaskIDs:   []int64{suite.taskID},
			JobType:   indexpb.JobType_JobTypeIndexJob,
		}

		resp, err := in.QueryJobsV2(ctx, req)
		err = merr.CheckRPCCall(resp, err)
		suite.NoError(err)
	})

	suite.Run("DropJobsV2", func() {
		req := &workerpb.DropJobsV2Request{
			ClusterID: suite.cluster,
			TaskIDs:   []int64{suite.taskID},
			JobType:   indexpb.JobType_JobTypeIndexJob,
		}

		resp, err := in.DropJobsV2(ctx, req)
		err = merr.CheckRPCCall(resp, err)
		suite.NoError(err)
	})
}

func Test_IndexNodeServiceSuite(t *testing.T) {
	suite.Run(t, new(IndexNodeServiceSuite))
}
