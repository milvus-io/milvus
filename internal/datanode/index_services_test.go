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

package datanode

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/index"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestIndexTaskWhenStoppingNode(t *testing.T) {
	ctx := context.TODO()
	paramtable.Init()
	manager := index.NewManager(ctx)

	manager.LoadOrStoreIndexTask("cluster-1", 1, &index.IndexTaskInfo{
		State: commonpb.IndexState_InProgress,
	})
	manager.LoadOrStoreIndexTask("cluster-2", 2, &index.IndexTaskInfo{
		State: commonpb.IndexState_Finished,
	})

	assert.True(t, manager.HasInProgressTask())
	go func() {
		time.Sleep(2 * time.Second)
		manager.StoreIndexTaskState("cluster-1", 1, commonpb.IndexState_Finished, "")
	}()
	noTaskChan := make(chan struct{})
	go func() {
		manager.WaitTaskFinish()
		close(noTaskChan)
	}()
	select {
	case <-noTaskChan:
	case <-time.After(5 * time.Second):
		assert.Fail(t, "timeout task chan")
	}
}

type IndexServiceSuite struct {
	suite.Suite

	collID        int64
	partID        int64
	segID         int64
	fieldID       int64
	logID         int64
	numRows       int64
	data          []*storage.Blob
	deleteData    []*storage.Blob
	in            *DataNode
	storageConfig *indexpb.StorageConfig
	cm            storage.ChunkManager
}

func Test_IndexServiceSuite(t *testing.T) {
	suite.Run(t, new(IndexServiceSuite))
}

func (s *IndexServiceSuite) SetupTest() {
	s.collID = 1
	s.partID = 2
	s.segID = 3
	s.fieldID = 111
	s.logID = 10000
	s.numRows = 3000
	paramtable.Init()
	paramtable.Get().MinioCfg.RootPath.SwapTempValue("index-service-ut")

	var err error
	s.data, err = generateTestData(s.collID, s.partID, s.segID, int(s.numRows))
	s.NoError(err)

	s.deleteData, err = generateDeleteData(s.collID, s.partID, s.segID, int(s.numRows))
	s.NoError(err)

	s.storageConfig = &indexpb.StorageConfig{
		Address:           paramtable.Get().MinioCfg.Address.GetValue(),
		AccessKeyID:       paramtable.Get().MinioCfg.AccessKeyID.GetValue(),
		SecretAccessKey:   paramtable.Get().MinioCfg.SecretAccessKey.GetValue(),
		UseSSL:            paramtable.Get().MinioCfg.UseSSL.GetAsBool(),
		SslCACert:         paramtable.Get().MinioCfg.SslCACert.GetValue(),
		BucketName:        paramtable.Get().MinioCfg.BucketName.GetValue(),
		RootPath:          paramtable.Get().MinioCfg.RootPath.GetValue(),
		UseIAM:            paramtable.Get().MinioCfg.UseIAM.GetAsBool(),
		IAMEndpoint:       paramtable.Get().MinioCfg.IAMEndpoint.GetValue(),
		StorageType:       paramtable.Get().CommonCfg.StorageType.GetValue(),
		Region:            paramtable.Get().MinioCfg.Region.GetValue(),
		UseVirtualHost:    paramtable.Get().MinioCfg.UseVirtualHost.GetAsBool(),
		CloudProvider:     paramtable.Get().MinioCfg.CloudProvider.GetValue(),
		RequestTimeoutMs:  paramtable.Get().MinioCfg.RequestTimeoutMs.GetAsInt64(),
		GcpCredentialJSON: paramtable.Get().MinioCfg.GcpCredentialJSON.GetValue(),
	}

	var (
		factory = dependency.NewMockFactory(s.T())
		ctx     = context.TODO()
	)
	s.in = NewDataNode(ctx, factory)

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
	for i, blob := range s.deleteData {
		fID, _ := strconv.ParseInt(blob.GetKey(), 10, 64)
		filePath, err := binlog.BuildLogPath(storage.DeleteBinlog, s.collID, s.partID, s.segID, fID, logID+int64(i))
		s.NoError(err)
		err = s.cm.Write(context.Background(), filePath, blob.GetValue())
		s.NoError(err)
	}
}

func (s *IndexServiceSuite) TearDownSuite() {
	err := s.cm.RemoveWithPrefix(context.Background(), "index-service-ut")
	s.NoError(err)
	paramtable.Get().MinioCfg.RootPath.SwapTempValue("files")

	err = s.in.Stop()
	s.NoError(err)
}

func (s *IndexServiceSuite) Test_CreateIndexJob_Compatibility() {
	s.Run("create vec index", func() {
		ctx := context.Background()

		s.Run("v2.3.x", func() {
			buildID := int64(1)
			dataPath, err := binlog.BuildLogPath(storage.InsertBinlog, s.collID, s.partID, s.segID, s.fieldID, s.logID+13)
			s.NoError(err)
			req := &workerpb.CreateJobRequest{
				ClusterID:       "cluster1",
				IndexFilePrefix: "index-service-ut/index_files",
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
				IndexFilePrefix: "index-service-ut/index_files",
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
				IndexFilePrefix: "index-service-ut/index_files",
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
				NumRows:                   s.numRows,
				CurrentIndexVersion:       0,
				CurrentScalarIndexVersion: 1,
				CollectionID:              s.collID,
				PartitionID:               s.partID,
				SegmentID:                 s.segID,
				FieldID:                   s.fieldID,
				FieldName:                 "floatVector",
				FieldType:                 schemapb.DataType_FloatVector,
				Dim:                       8,
				DataIds:                   []int64{s.logID + 13},
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

func (s *IndexServiceSuite) Test_CreateIndexJob_ScalarIndex() {
	ctx := context.Background()

	s.Run("int64 inverted", func() {
		buildID := int64(10)
		fieldID := int64(103)
		dataPath, err := binlog.BuildLogPath(storage.InsertBinlog, s.collID, s.partID, s.segID, fieldID, s.logID+5)
		s.NoError(err)
		req := &workerpb.CreateJobRequest{
			ClusterID:       "cluster1",
			IndexFilePrefix: "index-service-ut/index_files",
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
			CurrentScalarIndexVersion: 1,
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

func (s *IndexServiceSuite) Test_CreateAnalyzeTask() {
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

func (s *IndexServiceSuite) Test_CreateStatsTask() {
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
			SubJobType:      indexpb.StatsSubJob_Sort,
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

	s.Run("all deleted", func() {
		deltaLogs := make([]*datapb.FieldBinlog, 0)
		for i := range s.deleteData {
			deltaLogs = append(deltaLogs, &datapb.FieldBinlog{
				Binlogs: []*datapb.Binlog{{
					EntriesNum:    s.numRows,
					LogSize:       int64(len(s.deleteData[0].GetValue())),
					MemorySize:    s.deleteData[0].GetMemorySize(),
					LogID:         s.logID + int64(i),
					TimestampFrom: 1,
					TimestampTo:   3001,
				}},
			})
		}
		taskID := int64(200)
		req := &workerpb.CreateStatsRequest{
			ClusterID:       "cluster2",
			TaskID:          taskID,
			CollectionID:    s.collID,
			PartitionID:     s.partID,
			InsertChannel:   "ch1",
			SegmentID:       s.segID,
			InsertLogs:      fieldBinlogs,
			DeltaLogs:       deltaLogs,
			StorageConfig:   s.storageConfig,
			Schema:          generateTestSchema(),
			TargetSegmentID: s.segID + 1,
			StartLogID:      s.logID + 100,
			EndLogID:        s.logID + 200,
			NumRows:         s.numRows,
			BinlogMaxSize:   131000,
			SubJobType:      indexpb.StatsSubJob_Sort,
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
				s.Zero(len(resp.GetStatsJobResults().GetResults()[0].GetInsertLogs()))
				s.Equal(int64(0), resp.GetStatsJobResults().GetResults()[0].GetNumRows())
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

func generateTestSchema() *schemapb.CollectionSchema {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.TimeStampField, Name: "ts", DataType: schemapb.DataType_Int64},
		{FieldID: common.RowIDField, Name: "rowid", DataType: schemapb.DataType_Int64},
		{FieldID: 100, Name: "bool", DataType: schemapb.DataType_Bool},
		{FieldID: 101, Name: "int8", DataType: schemapb.DataType_Int8},
		{FieldID: 102, Name: "int16", DataType: schemapb.DataType_Int16},
		{FieldID: 103, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 104, Name: "float", DataType: schemapb.DataType_Float},
		{FieldID: 105, Name: "double", DataType: schemapb.DataType_Double},
		{FieldID: 106, Name: "varchar", DataType: schemapb.DataType_VarChar},
		{FieldID: 107, Name: "string", DataType: schemapb.DataType_String},
		{FieldID: 108, Name: "array", DataType: schemapb.DataType_Array},
		{FieldID: 109, Name: "json", DataType: schemapb.DataType_JSON},
		{FieldID: 110, Name: "int32", DataType: schemapb.DataType_Int32},
		{FieldID: 111, Name: "floatVector", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "8"},
		}},
		{FieldID: 112, Name: "binaryVector", DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "8"},
		}},
		{FieldID: 113, Name: "float16Vector", DataType: schemapb.DataType_Float16Vector, TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "8"},
		}},
		{FieldID: 114, Name: "bf16Vector", DataType: schemapb.DataType_BFloat16Vector, TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "8"},
		}},
		{FieldID: 115, Name: "sparseFloatVector", DataType: schemapb.DataType_SparseFloatVector, TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "28433"},
		}},
	}}

	return schema
}

func generateTestData(collID, partID, segID int64, num int) ([]*storage.Blob, error) {
	insertCodec := storage.NewInsertCodecWithSchema(&etcdpb.CollectionMeta{ID: collID, Schema: generateTestSchema()})

	var (
		field0 []int64
		field1 []int64

		field10 []bool
		field11 []int8
		field12 []int16
		field13 []int64
		field14 []float32
		field15 []float64
		field16 []string
		field17 []string
		field18 []*schemapb.ScalarField
		field19 [][]byte

		field101 []int32
		field102 []float32
		field103 []byte

		field104 []byte
		field105 []byte
		field106 [][]byte
	)

	for i := 1; i <= num; i++ {
		field0 = append(field0, int64(i))
		field1 = append(field1, int64(i))
		field10 = append(field10, true)
		field11 = append(field11, int8(i))
		field12 = append(field12, int16(i))
		field13 = append(field13, int64(i))
		field14 = append(field14, float32(i))
		field15 = append(field15, float64(i))
		field16 = append(field16, fmt.Sprint(i))
		field17 = append(field17, fmt.Sprint(i))

		arr := &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: []int32{int32(i), int32(i), int32(i)}},
			},
		}
		field18 = append(field18, arr)

		field19 = append(field19, []byte{byte(i)})
		field101 = append(field101, int32(i))

		f102 := make([]float32, 8)
		for j := range f102 {
			f102[j] = float32(i)
		}

		field102 = append(field102, f102...)
		field103 = append(field103, 0xff)

		f104 := make([]byte, 16)
		for j := range f104 {
			f104[j] = byte(i)
		}
		field104 = append(field104, f104...)
		field105 = append(field105, f104...)

		field106 = append(field106, typeutil.CreateSparseFloatRow([]uint32{0, uint32(18 * i), uint32(284 * i)}, []float32{1.1, 0.3, 2.4}))
	}

	data := &storage.InsertData{Data: map[int64]storage.FieldData{
		common.RowIDField:     &storage.Int64FieldData{Data: field0},
		common.TimeStampField: &storage.Int64FieldData{Data: field1},

		100: &storage.BoolFieldData{Data: field10},
		101: &storage.Int8FieldData{Data: field11},
		102: &storage.Int16FieldData{Data: field12},
		103: &storage.Int64FieldData{Data: field13},
		104: &storage.FloatFieldData{Data: field14},
		105: &storage.DoubleFieldData{Data: field15},
		106: &storage.StringFieldData{Data: field16},
		107: &storage.StringFieldData{Data: field17},
		108: &storage.ArrayFieldData{Data: field18},
		109: &storage.JSONFieldData{Data: field19},
		110: &storage.Int32FieldData{Data: field101},
		111: &storage.FloatVectorFieldData{
			Data: field102,
			Dim:  8,
		},
		112: &storage.BinaryVectorFieldData{
			Data: field103,
			Dim:  8,
		},
		113: &storage.Float16VectorFieldData{
			Data: field104,
			Dim:  8,
		},
		114: &storage.BFloat16VectorFieldData{
			Data: field105,
			Dim:  8,
		},
		115: &storage.SparseFloatVectorFieldData{
			SparseFloatArray: schemapb.SparseFloatArray{
				Dim:      28433,
				Contents: field106,
			},
		},
	}}

	blobs, err := insertCodec.Serialize(partID, segID, data)
	return blobs, err
}

func generateDeleteData(collID, partID, segID int64, num int) ([]*storage.Blob, error) {
	pks := make([]storage.PrimaryKey, 0, num)
	tss := make([]storage.Timestamp, 0, num)
	for i := 1; i <= num; i++ {
		pks = append(pks, storage.NewInt64PrimaryKey(int64(i)))
		tss = append(tss, storage.Timestamp(i+1))
	}

	deleteCodec := storage.NewDeleteCodec()
	blob, err := deleteCodec.Serialize(collID, partID, segID, &storage.DeleteData{
		Pks:      pks,
		Tss:      tss,
		RowCount: int64(num),
	})
	return []*storage.Blob{blob}, err
}
