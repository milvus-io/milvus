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
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestComponentState(t *testing.T) {
	var (
		factory = &mockFactory{
			chunkMgr: &mockChunkmgr{},
		}
		ctx = context.TODO()
	)
	paramtable.Init()
	in := NewIndexNode(ctx, factory)
	in.SetEtcdClient(getEtcdClient())
	state, err := in.GetComponentStates(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, state.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	assert.Equal(t, state.State.StateCode, commonpb.StateCode_Abnormal)

	assert.Nil(t, in.Init())
	state, err = in.GetComponentStates(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, state.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	assert.Equal(t, state.State.StateCode, commonpb.StateCode_Initializing)

	assert.Nil(t, in.Start())
	state, err = in.GetComponentStates(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, state.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	assert.Equal(t, state.State.StateCode, commonpb.StateCode_Healthy)

	assert.Nil(t, in.Stop())
	assert.Nil(t, in.Stop())
	state, err = in.GetComponentStates(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, state.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	assert.Equal(t, state.State.StateCode, commonpb.StateCode_Abnormal)
}

func TestGetTimeTickChannel(t *testing.T) {
	var (
		factory = &mockFactory{
			chunkMgr: &mockChunkmgr{},
		}
		ctx = context.TODO()
	)
	paramtable.Init()
	in := NewIndexNode(ctx, factory)
	ret, err := in.GetTimeTickChannel(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, ret.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
}

func TestGetStatisticChannel(t *testing.T) {
	var (
		factory = &mockFactory{
			chunkMgr: &mockChunkmgr{},
		}
		ctx = context.TODO()
	)
	paramtable.Init()
	in := NewIndexNode(ctx, factory)

	ret, err := in.GetStatisticsChannel(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, ret.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
}

func TestIndexTaskWhenStoppingNode(t *testing.T) {
	var (
		factory = &mockFactory{
			chunkMgr: &mockChunkmgr{},
		}
		ctx = context.TODO()
	)
	paramtable.Init()
	in := NewIndexNode(ctx, factory)

	in.loadOrStoreIndexTask("cluster-1", 1, &indexTaskInfo{
		state: commonpb.IndexState_InProgress,
	})
	in.loadOrStoreIndexTask("cluster-2", 2, &indexTaskInfo{
		state: commonpb.IndexState_Finished,
	})

	assert.True(t, in.hasInProgressTask())
	go func() {
		time.Sleep(2 * time.Second)
		in.storeIndexTaskState("cluster-1", 1, commonpb.IndexState_Finished, "")
	}()
	noTaskChan := make(chan struct{})
	go func() {
		in.waitTaskFinish()
		close(noTaskChan)
	}()
	select {
	case <-noTaskChan:
	case <-time.After(5 * time.Second):
		assert.Fail(t, "timeout task chan")
	}
}

func TestGetSetAddress(t *testing.T) {
	var (
		factory = &mockFactory{
			chunkMgr: &mockChunkmgr{},
		}
		ctx = context.TODO()
	)
	paramtable.Init()
	in := NewIndexNode(ctx, factory)
	in.SetAddress("address")
	assert.Equal(t, "address", in.GetAddress())
}

func TestInitErr(t *testing.T) {
	// var (
	// 	factory = &mockFactory{}
	// 	ctx     = context.TODO()
	// )
	// in, err := NewIndexNode(ctx, factory)
	// assert.NoError(t, err)
	// in.SetEtcdClient(getEtcdClient())
	// assert.Error(t, in.Init())
}

func setup() {
	startEmbedEtcd()
}

func teardown() {
	stopEmbedEtcd()
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}

type IndexNodeSuite struct {
	suite.Suite

	collID        int64
	partID        int64
	segID         int64
	fieldID       int64
	logID         int64
	numRows       int64
	data          []*Blob
	in            *IndexNode
	storageConfig *indexpb.StorageConfig
	cm            storage.ChunkManager
}

func Test_IndexNodeSuite(t *testing.T) {
	suite.Run(t, new(IndexNodeSuite))
}

func (s *IndexNodeSuite) SetupTest() {
	s.collID = 1
	s.partID = 2
	s.segID = 3
	s.fieldID = 111
	s.logID = 10000
	s.numRows = 3000
	paramtable.Init()
	Params.MinioCfg.RootPath.SwapTempValue("indexnode-ut")

	var err error
	s.data, err = generateTestData(s.collID, s.partID, s.segID, 3000)
	s.NoError(err)

	s.storageConfig = &indexpb.StorageConfig{
		Address:          Params.MinioCfg.Address.GetValue(),
		AccessKeyID:      Params.MinioCfg.AccessKeyID.GetValue(),
		SecretAccessKey:  Params.MinioCfg.SecretAccessKey.GetValue(),
		UseSSL:           Params.MinioCfg.UseSSL.GetAsBool(),
		SslCACert:        Params.MinioCfg.SslCACert.GetValue(),
		BucketName:       Params.MinioCfg.BucketName.GetValue(),
		RootPath:         Params.MinioCfg.RootPath.GetValue(),
		UseIAM:           Params.MinioCfg.UseIAM.GetAsBool(),
		IAMEndpoint:      Params.MinioCfg.IAMEndpoint.GetValue(),
		StorageType:      Params.CommonCfg.StorageType.GetValue(),
		Region:           Params.MinioCfg.Region.GetValue(),
		UseVirtualHost:   Params.MinioCfg.UseVirtualHost.GetAsBool(),
		CloudProvider:    Params.MinioCfg.CloudProvider.GetValue(),
		RequestTimeoutMs: Params.MinioCfg.RequestTimeoutMs.GetAsInt64(),
	}

	var (
		factory = &mockFactory{
			chunkMgr: &mockChunkmgr{},
		}
		ctx = context.TODO()
	)
	s.in = NewIndexNode(ctx, factory)

	err = s.in.Init()
	s.NoError(err)

	err = s.in.Start()
	s.NoError(err)

	s.cm, err = s.in.storageFactory.NewChunkManager(context.Background(), s.storageConfig)
	s.NoError(err)
	logID := int64(10000)
	for i, blob := range s.data {
		fID, _ := strconv.ParseInt(blob.GetKey(), 10, 64)
		filePath, err := binlog.BuildLogPath(storage.InsertBinlog, s.collID, s.partID, s.segID, fID, logID+int64(i))
		s.NoError(err)
		err = s.cm.Write(context.Background(), filePath, blob.GetValue())
		s.NoError(err)
	}
}

func (s *IndexNodeSuite) TearDownSuite() {
	err := s.cm.RemoveWithPrefix(context.Background(), "indexnode-ut")
	s.NoError(err)
	Params.MinioCfg.RootPath.SwapTempValue("files")

	err = s.in.Stop()
	s.NoError(err)
}

func (s *IndexNodeSuite) Test_CreateIndexJob_Compatibility() {
	s.Run("create vec index", func() {
		ctx := context.Background()

		s.Run("v2.3.x", func() {
			buildID := int64(1)
			dataPath, err := binlog.BuildLogPath(storage.InsertBinlog, s.collID, s.partID, s.segID, s.fieldID, s.logID+13)
			s.NoError(err)
			req := &workerpb.CreateJobRequest{
				ClusterID:       "cluster1",
				IndexFilePrefix: "indexnode-ut/index_files",
				BuildID:         buildID,
				DataPaths:       []string{dataPath},
				IndexVersion:    1,
				StorageConfig:   s.storageConfig,
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key: "index_type", Value: "HNSW",
					},
					{
						Key: "metric_type", Value: "L2",
					},
					{
						Key: "M", Value: "4",
					},
					{
						Key: "efConstruction", Value: "16",
					},
				},
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key: "dim", Value: "8",
					},
				},
				NumRows: s.numRows,
			}

			status, err := s.in.CreateJob(ctx, req)
			s.NoError(err)
			err = merr.Error(status)
			s.NoError(err)

			for {
				resp, err := s.in.QueryJobs(ctx, &workerpb.QueryJobsRequest{
					ClusterID: "cluster1",
					BuildIDs:  []int64{buildID},
				})
				s.NoError(err)
				err = merr.Error(resp.GetStatus())
				s.NoError(err)
				s.Equal(1, len(resp.GetIndexInfos()))
				if resp.GetIndexInfos()[0].GetState() == commonpb.IndexState_Finished {
					break
				}
				require.Equal(s.T(), resp.GetIndexInfos()[0].GetState(), commonpb.IndexState_InProgress)
				time.Sleep(time.Second)
			}

			status, err = s.in.DropJobs(ctx, &workerpb.DropJobsRequest{
				ClusterID: "cluster1",
				BuildIDs:  []int64{buildID},
			})
			s.NoError(err)
			err = merr.Error(status)
			s.NoError(err)
		})

		s.Run("v2.4.x", func() {
			buildID := int64(2)
			req := &workerpb.CreateJobRequest{
				ClusterID:       "cluster1",
				IndexFilePrefix: "indexnode-ut/index_files",
				BuildID:         buildID,
				DataPaths:       nil,
				IndexVersion:    1,
				StorageConfig:   s.storageConfig,
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key: "index_type", Value: "HNSW",
					},
					{
						Key: "metric_type", Value: "L2",
					},
					{
						Key: "M", Value: "4",
					},
					{
						Key: "efConstruction", Value: "16",
					},
				},
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key: "dim", Value: "8",
					},
				},
				NumRows:             s.numRows,
				CurrentIndexVersion: 0,
				CollectionID:        s.collID,
				PartitionID:         s.partID,
				SegmentID:           s.segID,
				FieldID:             s.fieldID,
				// v2.4.x does not fill the field type
				Dim:     8,
				DataIds: []int64{s.logID + 13},
			}

			status, err := s.in.CreateJob(ctx, req)
			s.NoError(err)
			err = merr.Error(status)
			s.NoError(err)

			for {
				resp, err := s.in.QueryJobs(ctx, &workerpb.QueryJobsRequest{
					ClusterID: "cluster1",
					BuildIDs:  []int64{buildID},
				})
				s.NoError(err)
				err = merr.Error(resp.GetStatus())
				s.NoError(err)
				s.Equal(1, len(resp.GetIndexInfos()))
				if resp.GetIndexInfos()[0].GetState() == commonpb.IndexState_Finished {
					break
				}
				require.Equal(s.T(), resp.GetIndexInfos()[0].GetState(), commonpb.IndexState_InProgress)
				time.Sleep(time.Second)
			}

			status, err = s.in.DropJobs(ctx, &workerpb.DropJobsRequest{
				ClusterID: "cluster1",
				BuildIDs:  []int64{buildID},
			})
			s.NoError(err)
			err = merr.Error(status)
			s.NoError(err)
		})

		s.Run("v2.5.x", func() {
			buildID := int64(3)
			req := &workerpb.CreateJobRequest{
				ClusterID:       "cluster1",
				IndexFilePrefix: "indexnode-ut/index_files",
				BuildID:         buildID,
				IndexVersion:    1,
				StorageConfig:   s.storageConfig,
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key: "index_type", Value: "HNSW",
					},
					{
						Key: "metric_type", Value: "L2",
					},
					{
						Key: "M", Value: "4",
					},
					{
						Key: "efConstruction", Value: "16",
					},
				},
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key: "dim", Value: "8",
					},
				},
				NumRows:             s.numRows,
				CurrentIndexVersion: 0,
				CollectionID:        s.collID,
				PartitionID:         s.partID,
				SegmentID:           s.segID,
				FieldID:             s.fieldID,
				FieldName:           "floatVector",
				FieldType:           schemapb.DataType_FloatVector,
				Dim:                 8,
				DataIds:             []int64{s.logID + 13},
				Field: &schemapb.FieldSchema{
					FieldID:  s.fieldID,
					Name:     "floatVector",
					DataType: schemapb.DataType_FloatVector,
				},
			}

			status, err := s.in.CreateJob(ctx, req)
			s.NoError(err)
			err = merr.Error(status)
			s.NoError(err)

			for {
				resp, err := s.in.QueryJobs(ctx, &workerpb.QueryJobsRequest{
					ClusterID: "cluster1",
					BuildIDs:  []int64{buildID},
				})
				s.NoError(err)
				err = merr.Error(resp.GetStatus())
				s.NoError(err)
				s.Equal(1, len(resp.GetIndexInfos()))
				if resp.GetIndexInfos()[0].GetState() == commonpb.IndexState_Finished {
					break
				}
				require.Equal(s.T(), resp.GetIndexInfos()[0].GetState(), commonpb.IndexState_InProgress)
				time.Sleep(time.Second)
			}

			status, err = s.in.DropJobs(ctx, &workerpb.DropJobsRequest{
				ClusterID: "cluster1",
				BuildIDs:  []int64{buildID},
			})
			s.NoError(err)
			err = merr.Error(status)
			s.NoError(err)
		})
	})
}

func (s *IndexNodeSuite) Test_CreateIndexJob_ScalarIndex() {
	ctx := context.Background()

	s.Run("int64 inverted", func() {
		buildID := int64(10)
		fieldID := int64(103)
		dataPath, err := binlog.BuildLogPath(storage.InsertBinlog, s.collID, s.partID, s.segID, fieldID, s.logID+5)
		s.NoError(err)
		req := &workerpb.CreateJobRequest{
			ClusterID:       "cluster1",
			IndexFilePrefix: "indexnode-ut/index_files",
			BuildID:         buildID,
			DataPaths:       []string{dataPath},
			IndexVersion:    1,
			StorageConfig:   s.storageConfig,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key: "index_type", Value: "INVERTED",
				},
			},
			TypeParams: nil,
			NumRows:    s.numRows,
			DataIds:    []int64{s.logID + 5},
			Field: &schemapb.FieldSchema{
				FieldID:  fieldID,
				Name:     "int64",
				DataType: schemapb.DataType_Int64,
			},
		}

		status, err := s.in.CreateJob(ctx, req)
		s.NoError(err)
		err = merr.Error(status)
		s.NoError(err)

		for {
			resp, err := s.in.QueryJobs(ctx, &workerpb.QueryJobsRequest{
				ClusterID: "cluster1",
				BuildIDs:  []int64{buildID},
			})
			s.NoError(err)
			err = merr.Error(resp.GetStatus())
			s.NoError(err)
			s.Equal(1, len(resp.GetIndexInfos()))
			if resp.GetIndexInfos()[0].GetState() == commonpb.IndexState_Finished {
				break
			}
			require.Equal(s.T(), commonpb.IndexState_InProgress, resp.GetIndexInfos()[0].GetState())
			time.Sleep(time.Second)
		}

		status, err = s.in.DropJobs(ctx, &workerpb.DropJobsRequest{
			ClusterID: "cluster1",
			BuildIDs:  []int64{buildID},
		})
		s.NoError(err)
		err = merr.Error(status)
		s.NoError(err)
	})
}

func (s *IndexNodeSuite) Test_CreateAnalyzeTask() {
	ctx := context.Background()

	s.Run("normal case", func() {
		taskID := int64(200)
		req := &workerpb.AnalyzeRequest{
			ClusterID:    "cluster1",
			TaskID:       taskID,
			CollectionID: s.collID,
			PartitionID:  s.partID,
			FieldID:      s.fieldID,
			FieldName:    "floatVector",
			FieldType:    schemapb.DataType_FloatVector,
			SegmentStats: map[int64]*indexpb.SegmentStats{
				s.segID: {
					ID:      s.segID,
					NumRows: s.numRows,
					LogIDs:  []int64{s.logID + 13},
				},
			},
			Version:             1,
			StorageConfig:       s.storageConfig,
			Dim:                 8,
			MaxTrainSizeRatio:   0.8,
			NumClusters:         1,
			MinClusterSizeRatio: 0.01,
			MaxClusterSizeRatio: 10,
			MaxClusterSize:      5 * 1024 * 1024 * 1024,
		}

		status, err := s.in.CreateJobV2(ctx, &workerpb.CreateJobV2Request{
			ClusterID: "cluster1",
			TaskID:    taskID,
			JobType:   indexpb.JobType_JobTypeAnalyzeJob,
			Request: &workerpb.CreateJobV2Request_AnalyzeRequest{
				AnalyzeRequest: req,
			},
		})
		s.NoError(err)
		err = merr.Error(status)
		s.NoError(err)

		for {
			resp, err := s.in.QueryJobsV2(ctx, &workerpb.QueryJobsV2Request{
				ClusterID: "cluster1",
				TaskIDs:   []int64{taskID},
				JobType:   indexpb.JobType_JobTypeAnalyzeJob,
			})
			s.NoError(err)
			err = merr.Error(resp.GetStatus())
			s.NoError(err)
			s.Equal(1, len(resp.GetAnalyzeJobResults().GetResults()))
			if resp.GetAnalyzeJobResults().GetResults()[0].GetState() == indexpb.JobState_JobStateFinished {
				s.Equal("", resp.GetAnalyzeJobResults().GetResults()[0].GetCentroidsFile())
				break
			}
			s.Equal(indexpb.JobState_JobStateInProgress, resp.GetAnalyzeJobResults().GetResults()[0].GetState())
			time.Sleep(time.Second)
		}

		status, err = s.in.DropJobsV2(ctx, &workerpb.DropJobsV2Request{
			ClusterID: "cluster1",
			TaskIDs:   []int64{taskID},
			JobType:   indexpb.JobType_JobTypeAnalyzeJob,
		})
		s.NoError(err)
		err = merr.Error(status)
		s.NoError(err)
	})
}

func (s *IndexNodeSuite) Test_CreateStatsTask() {
	ctx := context.Background()

	fieldBinlogs := make([]*datapb.FieldBinlog, 0)
	for i, field := range generateTestSchema().GetFields() {
		fieldBinlogs = append(fieldBinlogs, &datapb.FieldBinlog{
			FieldID: field.GetFieldID(),
			Binlogs: []*datapb.Binlog{{
				LogID: s.logID + int64(i),
			}},
		})
	}
	s.Run("normal case", func() {
		taskID := int64(100)
		req := &workerpb.CreateStatsRequest{
			ClusterID:       "cluster2",
			TaskID:          taskID,
			CollectionID:    s.collID,
			PartitionID:     s.partID,
			InsertChannel:   "ch1",
			SegmentID:       s.segID,
			InsertLogs:      fieldBinlogs,
			DeltaLogs:       nil,
			StorageConfig:   s.storageConfig,
			Schema:          generateTestSchema(),
			TargetSegmentID: s.segID + 1,
			StartLogID:      s.logID + 100,
			EndLogID:        s.logID + 200,
			NumRows:         s.numRows,
			BinlogMaxSize:   131000,
		}

		status, err := s.in.CreateJobV2(ctx, &workerpb.CreateJobV2Request{
			ClusterID: "cluster2",
			TaskID:    taskID,
			JobType:   indexpb.JobType_JobTypeStatsJob,
			Request: &workerpb.CreateJobV2Request_StatsRequest{
				StatsRequest: req,
			},
		})
		s.NoError(err)
		err = merr.Error(status)
		s.NoError(err)

		for {
			resp, err := s.in.QueryJobsV2(ctx, &workerpb.QueryJobsV2Request{
				ClusterID: "cluster2",
				TaskIDs:   []int64{taskID},
				JobType:   indexpb.JobType_JobTypeStatsJob,
			})
			s.NoError(err)
			err = merr.Error(resp.GetStatus())
			s.NoError(err)
			s.Equal(1, len(resp.GetStatsJobResults().GetResults()))
			if resp.GetStatsJobResults().GetResults()[0].GetState() == indexpb.JobState_JobStateFinished {
				s.NotZero(len(resp.GetStatsJobResults().GetResults()[0].GetInsertLogs()))
				s.NotZero(len(resp.GetStatsJobResults().GetResults()[0].GetStatsLogs()))
				s.Zero(len(resp.GetStatsJobResults().GetResults()[0].GetDeltaLogs()))
				s.Equal(s.numRows, resp.GetStatsJobResults().GetResults()[0].GetNumRows())
				break
			}
			s.Equal(indexpb.JobState_JobStateInProgress, resp.GetStatsJobResults().GetResults()[0].GetState())
			time.Sleep(time.Second)
		}

		slotResp, err := s.in.GetJobStats(ctx, &workerpb.GetJobStatsRequest{})
		s.NoError(err)
		err = merr.Error(slotResp.GetStatus())
		s.NoError(err)

		s.Equal(int64(1), slotResp.GetTaskSlots())

		status, err = s.in.DropJobsV2(ctx, &workerpb.DropJobsV2Request{
			ClusterID: "cluster2",
			TaskIDs:   []int64{taskID},
			JobType:   indexpb.JobType_JobTypeStatsJob,
		})
		s.NoError(err)
		err = merr.Error(status)
		s.NoError(err)
	})
}
