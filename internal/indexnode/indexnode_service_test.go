package indexnode

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/stretchr/testify/assert"
)

func TestIndexNodeSimple(t *testing.T) {
	in, err := NewMockIndexNodeComponent(context.TODO())
	assert.Nil(t, err)
	ctx := context.TODO()
	state, err := in.GetComponentStates(ctx)
	assert.Nil(t, err)
	assert.Equal(t, state.Status.ErrorCode, commonpb.ErrorCode_Success)
	assert.Equal(t, state.State.StateCode, internalpb.StateCode_Healthy)
	idxParams := map[string]string{
		"nlist": "128",
	}
	idxParamsPayload, err := json.Marshal(idxParams)

	assert.Nil(t, err, err)
	var (
		clusterID     int64 = 0
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
				Key:   "dim",
				Value: fmt.Sprintf("%d", vecDim),
			},
		}
		indexParams = []*commonpb.KeyValuePair{
			{
				Key:   "params",
				Value: string(idxParamsPayload),
			},
			{
				Key:   "metric_type",
				Value: "L2",
			},
			{
				Key:   "index_type",
				Value: "IVF_FLAT",
			},
		}
		mockChunkMgr = mockChunkMgrIns
	)

	mockChunkMgr.mockFieldData(1000, dim, collID, partID, segID)
	t.Run("create job", func(t *testing.T) {
		createReq := &indexpb.CreateJobRequest{
			ClusterID:       clusterID,
			IndexFilePrefix: idxFilePrefix,
			BuildID:         int64(buildID),
			DataPaths:       []string{dataPath(collID, partID, segID)},
			IndexVersion:    0,
			IndexID:         idxID,
			IndexName:       idxName,
			IndexParams:     indexParams,
			TypeParams:      typeParams,
		}
		status, err := in.CreateJob(ctx, createReq)
		assert.Nil(t, err)
		assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_Success)
	})

	t.Run(("query job"), func(t *testing.T) {
		queryJob := &indexpb.QueryJobsRequest{
			ClusterID: clusterID,
			BuildIDs:  []int64{buildID},
		}
		timeout := time.After(time.Second * 10)
		var idxInfo *indexpb.IndexTaskInfo
	Loop:
		for {
			select {
			case <-timeout:
				t.Fatal("timeout for querying jobs")
			default:
				resp, err := in.QueryJobs(ctx, queryJob)
				assert.Nil(t, err)
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
		for _, idxFile := range idxInfo.IndexFiles {
			_, ok := mockChunkMgr.indexedData.Load(idxFile)
			assert.True(t, ok)
			t.Logf("indexed file: %s", idxFile)
		}

		jobNumRet, err := in.GetJobNum(ctx, &indexpb.GetJobNumRequest{})
		assert.Nil(t, err)
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
		assert.Nil(t, err)
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
	idxParams := map[string]string{
		"nlist": "128",
	}
	idxParamsPayload, err := json.Marshal(idxParams)
	assert.Nil(t, err)
	var (
		clusterID        int64 = 0
		buildID0         int64 = 0
		collID0          int64 = 10000
		partID0          int64 = 20000
		segID0           int64 = 30000
		idxID0           int64 = 40000
		typesParamsLists       = [][]*commonpb.KeyValuePair{
			{{
				Key:   "dim",
				Value: fmt.Sprintf("%d", 8),
			}},
			{{
				Key:   "dim",
				Value: fmt.Sprintf("%d", 16),
			}},
			{{
				Key:   "dim",
				Value: fmt.Sprintf("%d", 32),
			}},
		}
		rowNums     = []int{100, 1000, 10000}
		dims        = []int{8, 16, 32}
		indexParams = []*commonpb.KeyValuePair{
			{
				Key:   "params",
				Value: string(idxParamsPayload),
			},
			{
				Key:   "metric_type",
				Value: "L2",
			},
			{
				Key:   "index_type",
				Value: "IVF_FLAT",
			},
		}
	)
	in, err := NewMockIndexNodeComponent(context.TODO())
	assert.Nil(t, err)
	ctx := context.TODO()
	state, err := in.GetComponentStates(ctx)
	assert.Nil(t, err)
	assert.Equal(t, state.Status.ErrorCode, commonpb.ErrorCode_Success)
	assert.Equal(t, state.State.StateCode, internalpb.StateCode_Healthy)

	mockChunkMgr := mockChunkMgrIns

	tasks := make([]*testTask, 0)
	var i int64
	t.Logf("preparing mock data...")
	wg := sync.WaitGroup{}
	for i = 0; i < 256; i++ {
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
		}
		testwg.Add(1)
		go func() {
			defer testwg.Done()
			status, err := in.CreateJob(ctx, req)
			assert.Nil(t, err)
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
				assert.Nil(t, err)
				assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_Success)
			}
		}(i)
	}
	testwg.Wait()
	timeout := time.After(time.Second * 30)
Loop:
	for {
		select {
		case <-timeout:
			t.Fatal("timeout testing")
		default:
			jobNumRet, err := in.GetJobNum(ctx, &indexpb.GetJobNumRequest{})
			assert.Nil(t, err)
			assert.Equal(t, jobNumRet.Status.ErrorCode, commonpb.ErrorCode_Success)
			if jobNumRet.TotalJobNum == 0 {
				break Loop
			}
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
	assert.Nil(t, err)
	assert.Equal(t, jobresp.Status.ErrorCode, jobresp.Status.ErrorCode)

	for _, job := range jobresp.IndexInfos {
		task := tasks[job.BuildID-buildID0]
		if job.State == commonpb.IndexState_Finished {
			for _, idxFile := range job.IndexFiles {
				_, ok := mockChunkMgr.indexedData.Load(idxFile)
				assert.True(t, ok)
			}
			t.Logf("buildID: %d, indexFiles: %v", job.BuildID, job.IndexFiles)
		} else {
			_, ok := mockChunkMgr.indexedData.Load(dataPath(task.collID, task.partID, task.segID))
			assert.False(t, ok)
		}
	}

	// stop indexnode
	assert.Nil(t, in.Stop())
	node := in.(*mockIndexNodeComponent).IndexNode
	assert.Equal(t, 0, len(node.tasks))
	assert.Equal(t, internalpb.StateCode_Abnormal, node.stateCode.Load().(internalpb.StateCode))
}

func TestAbnormalIndexNode(t *testing.T) {
	in, err := NewMockIndexNodeComponent(context.TODO())
	assert.Nil(t, err)
	assert.Nil(t, in.Stop())
	ctx := context.TODO()
	status, err := in.CreateJob(ctx, &indexpb.CreateJobRequest{})
	assert.Nil(t, err)
	assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_UnexpectedError)

	qresp, err := in.QueryJobs(ctx, &indexpb.QueryJobsRequest{})
	assert.Nil(t, err)
	assert.Equal(t, qresp.Status.ErrorCode, commonpb.ErrorCode_UnexpectedError)

	status, err = in.DropJobs(ctx, &indexpb.DropJobsRequest{})
	assert.Nil(t, err)
	assert.Equal(t, status.ErrorCode, commonpb.ErrorCode_UnexpectedError)

	jobNumRsp, err := in.GetJobNum(ctx, &indexpb.GetJobNumRequest{})
	assert.Nil(t, err)
	assert.Equal(t, jobNumRsp.Status.ErrorCode, commonpb.ErrorCode_UnexpectedError)

	metricsResp, err := in.GetMetrics(ctx, &milvuspb.GetMetricsRequest{})
	assert.Nil(t, err)
	assert.Equal(t, metricsResp.Status.ErrorCode, commonpb.ErrorCode_UnexpectedError)
}

func TestGetMetrics(t *testing.T) {
	var (
		ctx          = context.TODO()
		metricReq, _ = metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
	)
	in, err := NewMockIndexNodeComponent(ctx)
	assert.Nil(t, err)
	resp, err := in.GetMetrics(ctx, metricReq)
	assert.Nil(t, err)
	assert.Equal(t, resp.Status.ErrorCode, commonpb.ErrorCode_Success)
	t.Logf("Component: %s, Metrics: %s", resp.ComponentName, resp.Response)
}

func TestGetMetricsError(t *testing.T) {
	var (
		ctx = context.TODO()
	)

	in, err := NewMockIndexNodeComponent(ctx)
	assert.Nil(t, err)
	errReq := &milvuspb.GetMetricsRequest{
		Request: `{"metric_typ": "system_info"}`,
	}
	resp, err := in.GetMetrics(ctx, errReq)
	assert.Nil(t, err)
	assert.Equal(t, resp.Status.ErrorCode, commonpb.ErrorCode_UnexpectedError)

	unsupportedReq := &milvuspb.GetMetricsRequest{
		Request: `{"metric_type": "application_info"}`,
	}
	resp, err = in.GetMetrics(ctx, unsupportedReq)
	assert.Nil(t, err)
	assert.Equal(t, resp.Status.ErrorCode, commonpb.ErrorCode_UnexpectedError)
	assert.Equal(t, resp.Status.Reason, metricsinfo.MsgUnimplementedMetric)
}

func TestMockFieldData(t *testing.T) {
	chunkMgr := NewMockChunkManager()

	chunkMgr.mockFieldData(100000, 8, 0, 0, 1)
}
