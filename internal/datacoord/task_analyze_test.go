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

package datacoord

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type analyzeTaskSuite struct {
	suite.Suite
	mt *meta

	collID  int64
	partID  int64
	fieldID int64
	taskID  int64
}

func Test_analyzeTaskSuite(t *testing.T) {
	suite.Run(t, new(analyzeTaskSuite))
}

func (s *analyzeTaskSuite) SetupSuite() {
	s.collID = 1
	s.partID = 2
	s.fieldID = 3
	s.taskID = 1000

	// Mock analyze meta
	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	analyzeMt := &analyzeMeta{
		ctx:     context.Background(),
		catalog: catalog,
		tasks:   make(map[int64]*indexpb.AnalyzeTask),
	}

	// Add task to analyze meta
	analyzeTask := &indexpb.AnalyzeTask{
		CollectionID: s.collID,
		PartitionID:  s.partID,
		FieldID:      s.fieldID,
		FieldName:    "vector_field",
		FieldType:    schemapb.DataType_FloatVector,
		TaskID:       s.taskID,
		Version:      1,
		SegmentIDs:   []int64{101, 102},
		NodeID:       0,
		State:        indexpb.JobState_JobStateInit,
		FailReason:   "",
		Dim:          128,
	}
	analyzeMt.tasks[s.taskID] = analyzeTask

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  s.fieldID,
				Name:     "vector_field",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				},
			},
		},
	}

	collections := typeutil.NewConcurrentMap[int64, *collectionInfo]()
	collections.Insert(s.collID, &collectionInfo{Schema: schema})

	segments := NewSegmentsInfo()
	segments.SetSegment(101, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:           101,
			CollectionID: s.collID,
			PartitionID:  s.partID,
			State:        commonpb.SegmentState_Flushed,
			NumOfRows:    1000,
			Binlogs: []*datapb.FieldBinlog{
				{FieldID: s.fieldID, Binlogs: []*datapb.Binlog{{LogID: 1001}, {LogID: 1002}}},
			},
		},
	})
	segments.SetSegment(102, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:           102,
			CollectionID: s.collID,
			PartitionID:  s.partID,
			State:        commonpb.SegmentState_Flushed,
			NumOfRows:    2000,
			Binlogs: []*datapb.FieldBinlog{
				{FieldID: s.fieldID, Binlogs: []*datapb.Binlog{{LogID: 2001}, {LogID: 2002}}},
			},
		},
	})

	s.mt = &meta{
		analyzeMeta: analyzeMt,
		collections: collections,
		segments:    segments,
	}
}

func (s *analyzeTaskSuite) TestBasicTaskOperations() {
	at := newAnalyzeTask(&indexpb.AnalyzeTask{
		TaskID: s.taskID,
		State:  indexpb.JobState_JobStateInit,
	}, s.mt)

	s.Run("task type and state", func() {
		s.Equal(taskcommon.Analyze, at.GetTaskType())
		s.Equal(at.GetState(), at.GetTaskState())
		s.Equal(Params.DataCoordCfg.AnalyzeTaskSlotUsage.GetAsInt64(), at.GetTaskSlot())
	})

	s.Run("time management", func() {
		now := time.Now()

		at.SetTaskTime(taskcommon.TimeQueue, now)
		s.Equal(now, at.GetTaskTime(taskcommon.TimeQueue))

		at.SetTaskTime(taskcommon.TimeStart, now)
		s.Equal(now, at.GetTaskTime(taskcommon.TimeStart))

		at.SetTaskTime(taskcommon.TimeEnd, now)
		s.Equal(now, at.GetTaskTime(taskcommon.TimeEnd))
	})

	s.Run("state management", func() {
		at.SetState(indexpb.JobState_JobStateInProgress, "test reason")
		s.Equal(indexpb.JobState_JobStateInProgress, at.GetState())
		s.Equal("test reason", at.GetFailReason())
	})
}

func (s *analyzeTaskSuite) TestCreateTaskOnWorker() {
	at := newAnalyzeTask(&indexpb.AnalyzeTask{
		TaskID: s.taskID,
		State:  indexpb.JobState_JobStateInit,
	}, s.mt)

	s.Run("task not exist in meta", func() {
		// Remove task from meta
		originalTask := s.mt.analyzeMeta.tasks[s.taskID]
		delete(s.mt.analyzeMeta.tasks, s.taskID)
		at.CreateTaskOnWorker(1, session.NewMockCluster(s.T()))
		s.Equal(indexpb.JobState_JobStateNone, at.GetState())

		// Restore task
		s.mt.analyzeMeta.tasks[s.taskID] = originalTask
	})

	s.Run("successful creation", func() {
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().CreateAnalyze(mock.Anything, mock.Anything).Return(nil)

		// Mock the UpdateVersion function
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.On("SaveAnalyzeTask", mock.Anything, mock.Anything).Return(nil)
		s.mt.analyzeMeta.catalog = catalog

		at.CreateTaskOnWorker(1, cluster)
		s.Equal(indexpb.JobState_JobStateInProgress, at.GetState())
	})
}

func (s *analyzeTaskSuite) newTask() *analyzeTask {
	return newAnalyzeTask(&indexpb.AnalyzeTask{
		CollectionID: s.collID,
		TaskID:       s.taskID,
		State:        indexpb.JobState_JobStateInit,
	}, s.mt)
}

func (s *analyzeTaskSuite) TestCreateTaskOnWorker_SegmentNil() {
	// Replace segment 102 with a dropped segment so it's filtered out by isSegmentHealthy
	s.mt.segments.SetSegment(102, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:    102,
			State: commonpb.SegmentState_Dropped,
		},
	})
	defer func() {
		s.mt.segments.SetSegment(102, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           102,
				CollectionID: s.collID,
				PartitionID:  s.partID,
				State:        commonpb.SegmentState_Flushed,
				NumOfRows:    2000,
				Binlogs: []*datapb.FieldBinlog{
					{FieldID: s.fieldID, Binlogs: []*datapb.Binlog{{LogID: 2001}, {LogID: 2002}}},
				},
			},
		})
	}()

	at := s.newTask()
	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	catalog.On("SaveAnalyzeTask", mock.Anything, mock.Anything).Return(nil)
	s.mt.analyzeMeta.catalog = catalog

	at.CreateTaskOnWorker(1, session.NewMockCluster(s.T()))
	s.Equal(indexpb.JobState_JobStateFailed, at.GetState())
	s.Contains(at.GetFailReason(), "102")
}

func (s *analyzeTaskSuite) TestCreateTaskOnWorker_DimExtractionError() {
	// Use a schema with missing dim TypeParams
	badSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:    s.fieldID,
				Name:       "vector_field",
				DataType:   schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{}, // no dim
			},
		},
	}
	origCollections := s.mt.collections
	collections := typeutil.NewConcurrentMap[int64, *collectionInfo]()
	collections.Insert(s.collID, &collectionInfo{Schema: badSchema})
	s.mt.collections = collections
	defer func() { s.mt.collections = origCollections }()

	// Must create task AFTER swapping collections so schema is the bad one
	at := s.newTask()
	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	catalog.On("SaveAnalyzeTask", mock.Anything, mock.Anything).Return(nil)
	s.mt.analyzeMeta.catalog = catalog

	at.CreateTaskOnWorker(1, session.NewMockCluster(s.T()))
	// Should reset to Init state on dim error
	s.Equal(indexpb.JobState_JobStateInit, at.GetState())
}

func (s *analyzeTaskSuite) TestCreateTaskOnWorker_DataTooSmall() {
	// Set MinCentroidsNum very high so data is considered too small
	origMin := Params.DataCoordCfg.ClusteringCompactionMinCentroidsNum.SwapTempValue("999999999")
	defer Params.DataCoordCfg.ClusteringCompactionMinCentroidsNum.SwapTempValue(origMin)

	at := s.newTask()
	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	catalog.On("SaveAnalyzeTask", mock.Anything, mock.Anything).Return(nil)
	s.mt.analyzeMeta.catalog = catalog

	at.CreateTaskOnWorker(1, session.NewMockCluster(s.T()))
	// data too small → skip → mark as finished
	s.Equal(indexpb.JobState_JobStateFinished, at.GetState())
}

func (s *analyzeTaskSuite) TestCreateTaskOnWorker_NumClustersCapped() {
	// Set MaxCentroidsNum=1, MinCentroidsNum=1, SegmentMaxSize very small to force numClusters > max
	origMax := Params.DataCoordCfg.ClusteringCompactionMaxCentroidsNum.SwapTempValue("1")
	defer Params.DataCoordCfg.ClusteringCompactionMaxCentroidsNum.SwapTempValue(origMax)
	origMin := Params.DataCoordCfg.ClusteringCompactionMinCentroidsNum.SwapTempValue("1")
	defer Params.DataCoordCfg.ClusteringCompactionMinCentroidsNum.SwapTempValue(origMin)
	origSegSize := Params.DataCoordCfg.SegmentMaxSize.SwapTempValue("0.0001")
	defer Params.DataCoordCfg.SegmentMaxSize.SwapTempValue(origSegSize)

	at := s.newTask()
	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	catalog.On("SaveAnalyzeTask", mock.Anything, mock.Anything).Return(nil)
	s.mt.analyzeMeta.catalog = catalog

	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().CreateAnalyze(mock.Anything, mock.MatchedBy(func(req *workerpb.AnalyzeRequest) bool {
		return req.NumClusters == 1 // capped at MaxCentroidsNum=1
	})).Return(nil)

	at.CreateTaskOnWorker(1, cluster)
	s.Equal(indexpb.JobState_JobStateInProgress, at.GetState())
}

func (s *analyzeTaskSuite) TestCreateTaskOnWorker_CreateAnalyzeError() {
	// Ensure numClusters passes the min check
	origMin := Params.DataCoordCfg.ClusteringCompactionMinCentroidsNum.SwapTempValue("1")
	defer Params.DataCoordCfg.ClusteringCompactionMinCentroidsNum.SwapTempValue(origMin)

	at := s.newTask()
	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	catalog.On("SaveAnalyzeTask", mock.Anything, mock.Anything).Return(nil)
	s.mt.analyzeMeta.catalog = catalog

	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().CreateAnalyze(mock.Anything, mock.Anything).Return(fmt.Errorf("node down"))
	cluster.EXPECT().DropAnalyze(mock.Anything, mock.Anything).Return(nil)

	at.CreateTaskOnWorker(1, cluster)
	// Should NOT be InProgress since CreateAnalyze failed
	s.NotEqual(indexpb.JobState_JobStateInProgress, at.GetState())
}

func (s *analyzeTaskSuite) TestCreateTaskOnWorker_SegmentStatsPopulated() {
	// Ensure numClusters passes the min check
	origMin := Params.DataCoordCfg.ClusteringCompactionMinCentroidsNum.SwapTempValue("1")
	defer Params.DataCoordCfg.ClusteringCompactionMinCentroidsNum.SwapTempValue(origMin)

	at := s.newTask()
	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	catalog.On("SaveAnalyzeTask", mock.Anything, mock.Anything).Return(nil)
	s.mt.analyzeMeta.catalog = catalog

	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().CreateAnalyze(mock.Anything, mock.MatchedBy(func(req *workerpb.AnalyzeRequest) bool {
		// Verify SegmentStats are populated correctly
		if len(req.SegmentStats) != 2 {
			return false
		}
		stat101 := req.SegmentStats[101]
		stat102 := req.SegmentStats[102]
		if stat101 == nil || stat102 == nil {
			return false
		}
		// segment 101: 1000 rows, binlogs [1001, 1002]
		if stat101.NumRows != 1000 || len(stat101.LogIDs) != 2 {
			return false
		}
		// segment 102: 2000 rows, binlogs [2001, 2002]
		if stat102.NumRows != 2000 || len(stat102.LogIDs) != 2 {
			return false
		}
		// Dim should be 128
		if req.Dim != 128 {
			return false
		}
		// Clustering params should be populated
		if req.MaxTrainSizeRatio == 0 || req.MaxClusterSize == 0 || req.TaskSlot == 0 {
			return false
		}
		return true
	})).Return(nil)

	at.CreateTaskOnWorker(1, cluster)
	s.Equal(indexpb.JobState_JobStateInProgress, at.GetState())
}

func (s *analyzeTaskSuite) TestQueryTaskOnWorker() {
	at := newAnalyzeTask(&indexpb.AnalyzeTask{
		TaskID: s.taskID,
		NodeID: 1,
		State:  indexpb.JobState_JobStateInProgress,
	}, s.mt)

	s.Run("query failed", func() {
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryAnalyze(mock.Anything, mock.Anything).Return(nil, fmt.Errorf("mock error"))
		cluster.EXPECT().DropAnalyze(mock.Anything, mock.Anything).Return(nil)

		at.QueryTaskOnWorker(cluster)
		s.Equal(indexpb.JobState_JobStateInit, at.GetState())
	})

	s.Run("node not found", func() {
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryAnalyze(mock.Anything, mock.Anything).Return(nil, merr.ErrNodeNotFound)
		cluster.EXPECT().DropAnalyze(mock.Anything, mock.Anything).Return(nil)

		at.QueryTaskOnWorker(cluster)
		s.Equal(indexpb.JobState_JobStateInit, at.GetState())
	})

	s.Run("task finished", func() {
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryAnalyze(mock.Anything, mock.Anything).Return(&workerpb.AnalyzeResults{
			Results: []*workerpb.AnalyzeResult{{
				TaskID:     s.taskID,
				State:      indexpb.JobState_JobStateFinished,
				FailReason: "",
			}},
		}, nil)

		// Mock the FinishTask function
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.EXPECT().SaveAnalyzeTask(mock.Anything, mock.Anything).Return(nil)
		s.mt.analyzeMeta.catalog = catalog

		at.QueryTaskOnWorker(cluster)
		s.Equal(indexpb.JobState_JobStateFinished, at.GetState())
	})
}

func (s *analyzeTaskSuite) TestDropTaskOnWorker() {
	at := newAnalyzeTask(&indexpb.AnalyzeTask{
		TaskID: s.taskID,
		NodeID: 1,
		State:  indexpb.JobState_JobStateInProgress,
	}, s.mt)

	s.Run("drop failed", func() {
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().DropAnalyze(mock.Anything, mock.Anything).Return(fmt.Errorf("mock error"))

		// This should just log the error and return
		at.DropTaskOnWorker(cluster)
	})

	s.Run("drop success", func() {
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().DropAnalyze(mock.Anything, mock.Anything).Return(nil)

		// This should complete successfully
		at.DropTaskOnWorker(cluster)
	})
}
