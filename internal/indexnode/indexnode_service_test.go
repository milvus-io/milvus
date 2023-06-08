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
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func genStorageConfig() *indexpb.StorageConfig {
	return &indexpb.StorageConfig{
		Address:         Params.MinioCfg.Address.GetValue(),
		AccessKeyID:     Params.MinioCfg.AccessKeyID.GetValue(),
		SecretAccessKey: Params.MinioCfg.SecretAccessKey.GetValue(),
		BucketName:      Params.MinioCfg.BucketName.GetValue(),
		RootPath:        Params.MinioCfg.RootPath.GetValue(),
		IAMEndpoint:     Params.MinioCfg.IAMEndpoint.GetValue(),
		UseSSL:          Params.MinioCfg.UseSSL.GetAsBool(),
		UseIAM:          Params.MinioCfg.UseIAM.GetAsBool(),
	}
}

func TestIndexNodeSimple(t *testing.T) {
	in, err := NewMockIndexNodeComponent(context.TODO())
	require.Nil(t, err)
	defer in.Stop()
	ctx := context.TODO()
	state, err := in.GetComponentStates(ctx)
	assert.NoError(t, err)
	assert.Equal(t, state.Status.ErrorCode, commonpb.ErrorCode_Success)
	assert.Equal(t, state.State.StateCode, commonpb.StateCode_Healthy)

	assert.Nil(t, err, err)
	var (
		clusterID           = "test-milvus"
		idxFilePrefix       = "mock_idx"
		buildID       int64 = 1
		collID        int64 = 101
		partID        int64 = 201
		segID         int64 = 301
		idxID         int64 = 401
		idxName             = "mock_idx"
		vecDim        int64 = 8
		typeParams          = []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: fmt.Sprintf("%d", vecDim),
			},
		}
		indexParams = []*commonpb.KeyValuePair{
			{
				Key:   common.MetricTypeKey,
				Value: "L2",
			},
			{
				Key:   common.IndexTypeKey,
				Value: "IVF_FLAT",
			},
			{
				Key:   "nlist",
				Value: "128",
			},
		}
		mockChunkMgr = mockChunkMgrIns
	)

	mockChunkMgr.mockFieldData(1000, dim, collID, partID, segID)
	t.Run("create job", func(t *testing.T) {
		createReq := &indexpb.CreateJobRequest{
			ClusterID:       clusterID,
			IndexFilePrefix: idxFilePrefix,
			BuildID:         buildID,
			DataPaths:       []string{dataPath(collID, partID, segID)},
			IndexVersion:    0,
			IndexID:         idxID,
			IndexName:       idxName,
			IndexParams:     indexParams,
			TypeParams:      typeParams,
			StorageConfig:   genStorageConfig(),
		}
		status, err := in.CreateJob(ctx, createReq)
		assert.NoError(t, err)
		assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run(("query job"), func(t *testing.T) {
		queryJob := &indexpb.QueryJobsRequest{
			ClusterID: clusterID,
			BuildIDs:  []int64{buildID},
		}
		var idxInfo *indexpb.IndexTaskInfo
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
	Loop:
		for {
			select {
			case <-timeoutCtx.Done():
				t.Fatal("timeout for querying jobs")
			default:
				time.Sleep(1 * time.Millisecond)
				resp, err := in.QueryJobs(ctx, queryJob)
				assert.NoError(t, err)
				assert.Equal(t, resp.Status.ErrorCode, commonpb.ErrorCode_Success)
				assert.Equal(t, resp.ClusterID, clusterID)

				for _, indexInfo := range resp.IndexInfos {
					if indexInfo.BuildID == buildID {
						if indexInfo.State == commonpb.IndexState_Finished {
							idxInfo = indexInfo
							break Loop
						}
					}
				}

			}
		}

		assert.NotNil(t, idxInfo)
		for _, idxFileID := range idxInfo.IndexFileKeys {
			idxFile := metautil.BuildSegmentIndexFilePath(mockChunkMgr.RootPath(), buildID, 0,
				partID, segID, idxFileID)
			_, ok := mockChunkMgr.indexedData.Load(idxFile)
			assert.True(t, ok)
			t.Logf("indexed file: %s", idxFile)
		}

		jobNumRet, err := in.GetJobStats(ctx, &indexpb.GetJobStatsRequest{})
		assert.NoError(t, err)
		assert.Equal(t, jobNumRet.Status.GetErrorCode(), commonpb.ErrorCode_Success)
		assert.Equal(t, jobNumRet.TotalJobNum, int64(0))
		assert.Equal(t, jobNumRet.InProgressJobNum, int64(0))
		assert.Equal(t, jobNumRet.EnqueueJobNum, int64(0))
		assert.Equal(t, jobNumRet.TaskSlots, int64(1))
		assert.Equal(t, len(jobNumRet.JobInfos), 1)
		jobInfo := jobNumRet.JobInfos[0]

		assert.True(t, jobInfo.Dim == 8)
		assert.True(t, jobInfo.NumRows == 1000)
		assert.True(t, jobInfo.PodID == 1)
		assert.ElementsMatch(t, jobInfo.IndexParams, indexParams)
	})

	t.Run("drop not exists jobs", func(t *testing.T) {
		status, err := in.DropJobs(ctx, &indexpb.DropJobsRequest{
			ClusterID: clusterID,
			BuildIDs:  []int64{100001},
		})
		assert.NoError(t, err)
		assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_Success)
	})
}

type testTask struct {
	buildID    int64
	collID     int64
	partID     int64
	segID      int64
	idxID      int64
	dim        int
	rownum     int
	typeParams []*commonpb.KeyValuePair
	idxParams  []*commonpb.KeyValuePair
}

func TestIndexNodeComplex(t *testing.T) {
	var (
		clusterID        string
		buildID0         int64
		collID0          int64 = 10000
		partID0          int64 = 20000
		segID0           int64 = 30000
		idxID0           int64 = 40000
		typesParamsLists       = [][]*commonpb.KeyValuePair{
			{{
				Key:   common.DimKey,
				Value: fmt.Sprintf("%d", 8),
			}},
			{{
				Key:   common.DimKey,
				Value: fmt.Sprintf("%d", 16),
			}},
			{{
				Key:   common.DimKey,
				Value: fmt.Sprintf("%d", 32),
			}},
		}
		rowNums     = []int{100, 1000, 10000}
		dims        = []int{8, 16, 32}
		indexParams = []*commonpb.KeyValuePair{
			{
				Key:   "nlist",
				Value: "128",
			},
			{
				Key:   common.MetricTypeKey,
				Value: "L2",
			},
			{
				Key:   common.IndexTypeKey,
				Value: "IVF_FLAT",
			},
		}
	)
	in, err := NewMockIndexNodeComponent(context.TODO())
	require.Nil(t, err)
	defer in.Stop()
	ctx := context.TODO()
	state, err := in.GetComponentStates(ctx)
	assert.NoError(t, err)
	assert.Equal(t, state.Status.ErrorCode, commonpb.ErrorCode_Success)
	assert.Equal(t, state.State.StateCode, commonpb.StateCode_Healthy)

	mockChunkMgr := mockChunkMgrIns

	tasks := make([]*testTask, 0)
	var i int64
	t.Logf("preparing mock data...")
	wg := sync.WaitGroup{}
	for i = 0; i < 10; i++ {
		task := &testTask{
			buildID:    i + buildID0,
			collID:     i + collID0,
			partID:     i + partID0,
			segID:      i + segID0,
			idxID:      i + idxID0,
			typeParams: typesParamsLists[i%3],
			dim:        dims[i%3],
			rownum:     rowNums[i%3],
			idxParams:  indexParams,
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if rand.Float32() < 0.5 {
				mockChunkMgr.mockFieldData(task.rownum, task.dim, task.collID, task.partID, task.segID)
			}
		}()
		tasks = append(tasks, task)
	}
	wg.Wait()

	t.Logf("start concurent testing")
	testwg := sync.WaitGroup{}
	for i := 0; i < len(tasks); i++ {
		req := &indexpb.CreateJobRequest{
			ClusterID:       clusterID,
			IndexFilePrefix: "mock_idx",
			BuildID:         tasks[i].buildID,
			DataPaths:       []string{dataPath(tasks[i].collID, tasks[i].partID, tasks[i].segID)},
			IndexVersion:    0,
			IndexID:         tasks[i].idxID,
			IndexName:       fmt.Sprintf("idx%d", tasks[i].idxID),
			IndexParams:     tasks[i].idxParams,
			TypeParams:      tasks[i].typeParams,
			StorageConfig:   genStorageConfig(),
		}
		testwg.Add(1)
		go func() {
			defer testwg.Done()
			status, err := in.CreateJob(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_Success)
		}()

		testwg.Add(1)
		go func(idx int) {
			defer testwg.Done()
			if rand.Float32() < 0.5 {
				status, err := in.DropJobs(ctx, &indexpb.DropJobsRequest{
					ClusterID: clusterID,
					BuildIDs:  []int64{tasks[idx].buildID},
				})
				assert.NoError(t, err)
				assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_Success)
			}
		}(i)
	}
	testwg.Wait()
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()
Loop:
	for {
		select {
		case <-timeoutCtx.Done():
			t.Fatal("timeout testing")
		default:
			jobNumRet, err := in.GetJobStats(ctx, &indexpb.GetJobStatsRequest{})
			assert.NoError(t, err)
			assert.Equal(t, jobNumRet.Status.ErrorCode, commonpb.ErrorCode_Success)
			if jobNumRet.TotalJobNum == 0 {
				break Loop
			}
			time.Sleep(time.Second)
		}
	}
	buildIDs := make([]int64, 0, len(tasks))
	for _, task := range tasks {
		buildIDs = append(buildIDs, task.buildID)
	}
	jobresp, err := in.QueryJobs(ctx, &indexpb.QueryJobsRequest{
		ClusterID: clusterID,
		BuildIDs:  buildIDs,
	})
	assert.NoError(t, err)
	assert.Equal(t, jobresp.Status.ErrorCode, jobresp.Status.ErrorCode)

	for _, job := range jobresp.IndexInfos {
		task := tasks[job.BuildID-buildID0]
		if job.State == commonpb.IndexState_Finished {
			for _, idxFileID := range job.IndexFileKeys {
				idxFile := metautil.BuildSegmentIndexFilePath(mockChunkMgr.RootPath(), task.buildID,
					0, task.partID, task.segID, idxFileID)
				_, ok := mockChunkMgr.indexedData.Load(idxFile)
				assert.True(t, ok)
			}
			t.Logf("buildID: %d, indexFiles: %v", job.BuildID, job.IndexFileKeys)
		} else {
			_, ok := mockChunkMgr.indexedData.Load(dataPath(task.collID, task.partID, task.segID))
			assert.False(t, ok)
		}
	}

	// stop indexnode
	assert.Nil(t, in.Stop())
	node := in.(*mockIndexNodeComponent).IndexNode
	assert.Equal(t, 0, len(node.tasks))
	assert.Equal(t, commonpb.StateCode_Abnormal, node.lifetime.GetState())
}

func TestAbnormalIndexNode(t *testing.T) {
	in, err := NewMockIndexNodeComponent(context.TODO())
	assert.NoError(t, err)
	assert.Nil(t, in.Stop())
	ctx := context.TODO()
	status, err := in.CreateJob(ctx, &indexpb.CreateJobRequest{})
	assert.NoError(t, err)
	assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_UnexpectedError)

	qresp, err := in.QueryJobs(ctx, &indexpb.QueryJobsRequest{})
	assert.NoError(t, err)
	assert.Equal(t, qresp.Status.ErrorCode, commonpb.ErrorCode_UnexpectedError)

	status, err = in.DropJobs(ctx, &indexpb.DropJobsRequest{})
	assert.NoError(t, err)
	assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_UnexpectedError)

	jobNumRsp, err := in.GetJobStats(ctx, &indexpb.GetJobStatsRequest{})
	assert.NoError(t, err)
	assert.Equal(t, jobNumRsp.Status.ErrorCode, commonpb.ErrorCode_UnexpectedError)

	metricsResp, err := in.GetMetrics(ctx, &milvuspb.GetMetricsRequest{})
	assert.NoError(t, err)
	assert.Equal(t, metricsResp.Status.ErrorCode, commonpb.ErrorCode_UnexpectedError)

	configurationResp, err := in.ShowConfigurations(ctx, &internalpb.ShowConfigurationsRequest{})
	assert.NoError(t, err)
	assert.Equal(t, configurationResp.Status.ErrorCode, commonpb.ErrorCode_UnexpectedError)
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
	assert.Equal(t, resp.Status.ErrorCode, commonpb.ErrorCode_Success)
	t.Logf("Component: %s, Metrics: %s", resp.ComponentName, resp.Response)
}

func TestGetMetricsError(t *testing.T) {
	var (
		ctx = context.TODO()
	)

	in, err := NewMockIndexNodeComponent(ctx)
	assert.NoError(t, err)
	defer in.Stop()
	errReq := &milvuspb.GetMetricsRequest{
		Request: `{"metric_typ": "system_info"}`,
	}
	resp, err := in.GetMetrics(ctx, errReq)
	assert.NoError(t, err)
	assert.Equal(t, resp.Status.ErrorCode, commonpb.ErrorCode_UnexpectedError)

	unsupportedReq := &milvuspb.GetMetricsRequest{
		Request: `{"metric_type": "application_info"}`,
	}
	resp, err = in.GetMetrics(ctx, unsupportedReq)
	assert.NoError(t, err)
	assert.Equal(t, resp.Status.ErrorCode, commonpb.ErrorCode_UnexpectedError)
	assert.Equal(t, resp.Status.Reason, metricsinfo.MsgUnimplementedMetric)
}

func TestMockFieldData(t *testing.T) {
	chunkMgr := NewMockChunkManager()

	chunkMgr.mockFieldData(100000, 8, 0, 0, 1)
}
