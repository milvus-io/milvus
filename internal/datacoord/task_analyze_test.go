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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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

	s.mt = &meta{
		analyzeMeta: analyzeMt,
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
