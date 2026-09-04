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
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
)

type statsTaskMetaSuite struct {
	suite.Suite

	collectionID int64
	partitionID  int64
	segmentID    int64
}

func (s *statsTaskMetaSuite) SetupSuite() {}

func (s *statsTaskMetaSuite) TearDownSuite() {}

func (s *statsTaskMetaSuite) SetupTest() {
	s.collectionID = 100
	s.partitionID = 101
	s.segmentID = 102
}

func (s *statsTaskMetaSuite) Test_Method() {
	s.Run("newStatsTaskMeta", func() {
		s.Run("failed case", func() {
			catalog := mocks.NewDataCoordCatalog(s.T())
			catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, errors.New("mock error"))

			m, err := newStatsTaskMeta(context.Background(), catalog)
			s.Error(err)
			s.Nil(m)
		})

		s.Run("skips sort tasks and loads others", func() {
			catalog := mocks.NewDataCoordCatalog(s.T())
			catalog.EXPECT().ListStatsTasks(mock.Anything).Return([]*indexpb.StatsTask{
				{
					TaskID:     1,
					SegmentID:  100,
					SubJobType: indexpb.StatsSubJob_Sort,
				},
				{
					TaskID:     2,
					SegmentID:  200,
					SubJobType: indexpb.StatsSubJob_TextIndexJob,
				},
				{
					TaskID:     3,
					SegmentID:  300,
					SubJobType: indexpb.StatsSubJob_Sort,
				},
			}, nil)
			catalog.EXPECT().DropStatsTask(mock.Anything, mock.Anything).Return(nil).Times(2)

			m, err := newStatsTaskMeta(context.Background(), catalog)
			s.NoError(err)
			s.NotNil(m)

			_, ok := m.tasks.Get(int64(2))
			s.True(ok)
			_, ok = m.tasks.Get(int64(1))
			s.False(ok)
			_, ok = m.tasks.Get(int64(3))
			s.False(ok)

			s.Equal([]int64{1, 3}, m.deprecatedSortTaskIDs)

			var wg sync.WaitGroup
			m.StartCleanupDeprecatedSortTasks(context.Background(), &wg)
			wg.Wait()

			s.Nil(m.deprecatedSortTaskIDs)
		})
	})

	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)

	m, err := newStatsTaskMeta(context.Background(), catalog)
	s.NoError(err)

	t := &indexpb.StatsTask{
		CollectionID:  s.collectionID,
		PartitionID:   s.partitionID,
		SegmentID:     s.segmentID,
		InsertChannel: "ch1",
		TaskID:        1,
		Version:       0,
		NodeID:        0,
		State:         indexpb.JobState_JobStateInit,
		FailReason:    "",
		SubJobType:    indexpb.StatsSubJob_Sort,
	}

	s.Run("AddStatsTask", func() {
		s.Run("failed case", func() {
			catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(errors.New("mock error")).Once()

			s.Error(m.AddStatsTask(t))
			_, ok := m.tasks.Get(1)
			s.False(ok)
		})

		s.Run("normal case", func() {
			catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil).Once()

			s.NoError(m.AddStatsTask(t))
			_, ok := m.tasks.Get(1)
			s.True(ok)
		})

		s.Run("already exist", func() {
			s.Error(m.AddStatsTask(t))
			_, ok := m.tasks.Get(1)
			s.True(ok)
		})
	})

	s.Run("UpdateBuildingTask", func() {
		s.Run("failed case", func() {
			catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(errors.New("mock error")).Once()

			s.Error(m.UpdateBuildingTask(1, 1180))
			task, ok := m.tasks.Get(1)
			s.True(ok)
			s.Equal(indexpb.JobState_JobStateInit, task.GetState())
			s.Zero(task.GetNodeID())
		})

		s.Run("normal case", func() {
			catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil).Once()

			s.NoError(m.UpdateBuildingTask(1, 1180))
			task, ok := m.tasks.Get(1)
			s.True(ok)
			s.Equal(indexpb.JobState_JobStateInProgress, task.GetState())
			s.Equal(int64(1180), task.GetNodeID())
		})

		s.Run("task not exist", func() {
			_, ok := m.tasks.Get(100)
			s.False(ok)

			s.Error(m.UpdateBuildingTask(100, 1180))
		})
	})

	s.Run("FinishTask", func() {
		result := &workerpb.StatsResult{
			TaskID:       1,
			State:        indexpb.JobState_JobStateFinished,
			FailReason:   "",
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			SegmentID:    s.segmentID,
			Channel:      "ch1",
			InsertLogs: []*datapb.FieldBinlog{
				{FieldID: 0, Binlogs: []*datapb.Binlog{{LogID: 1}, {LogID: 5}}},
				{FieldID: 1, Binlogs: []*datapb.Binlog{{LogID: 2}, {LogID: 6}}},
				{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 3}, {LogID: 7}}},
				{FieldID: 101, Binlogs: []*datapb.Binlog{{LogID: 4}, {LogID: 8}}},
			},
			StatsLogs: []*datapb.FieldBinlog{
				{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 9}}},
			},
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				100: {
					FieldID:    100,
					Version:    1,
					Files:      []string{"file1", "file2", "file3"},
					LogSize:    100,
					MemorySize: 100,
				},
			},
			NumRows: 2048,
		}
		s.Run("failed case", func() {
			catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(errors.New("mock error")).Once()

			s.Error(m.FinishTask(1, result))
			task, ok := m.tasks.Get(1)
			s.True(ok)
			s.Equal(indexpb.JobState_JobStateInProgress, task.GetState())
		})

		s.Run("normal case", func() {
			catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil).Once()

			s.NoError(m.FinishTask(1, result))
			task, ok := m.tasks.Get(1)
			s.True(ok)
			s.Equal(indexpb.JobState_JobStateFinished, task.GetState())
		})

		s.Run("task not exist", func() {
			s.Error(m.FinishTask(100, result))
		})
	})

	s.Run("GetStatsTaskState", func() {
		s.Run("task not exist", func() {
			state := m.GetStatsTaskState(100)
			s.Equal(indexpb.JobState_JobStateNone, state)
		})

		s.Run("normal case", func() {
			state := m.GetStatsTaskState(1)
			s.Equal(indexpb.JobState_JobStateFinished, state)
		})
	})

	s.Run("GetStatsTaskStateBySegmentID", func() {
		s.Run("task not exist", func() {
			state := m.GetStatsTaskStateBySegmentID(100, indexpb.StatsSubJob_Sort)
			s.Equal(indexpb.JobState_JobStateNone, state)

			state = m.GetStatsTaskStateBySegmentID(s.segmentID, indexpb.StatsSubJob_BM25Job)
			s.Equal(indexpb.JobState_JobStateNone, state)
		})

		s.Run("normal case", func() {
			state := m.GetStatsTaskStateBySegmentID(s.segmentID, indexpb.StatsSubJob_Sort)
			s.Equal(indexpb.JobState_JobStateFinished, state)
		})
	})

	s.Run("HasStatsTask", func() {
		s.False(m.HasStatsTask(100, indexpb.StatsSubJob_Sort))
		s.False(m.HasStatsTask(s.segmentID, indexpb.StatsSubJob_BM25Job))
		// The task is already Finished here: it keeps blocking resubmission
		// until GC recycles it, matching AddStatsTask's duplicate guard.
		s.Equal(indexpb.JobState_JobStateFinished, m.GetStatsTaskStateBySegmentID(s.segmentID, indexpb.StatsSubJob_Sort))
		s.True(m.HasStatsTask(s.segmentID, indexpb.StatsSubJob_Sort))
	})

	s.Run("DropStatsTask", func() {
		s.Run("failed case", func() {
			catalog.EXPECT().DropStatsTask(mock.Anything, mock.Anything).Return(errors.New("mock error")).Once()

			s.Error(m.DropStatsTask(context.TODO(), 1))
			_, ok := m.tasks.Get(1)
			s.True(ok)
		})

		s.Run("normal case", func() {
			catalog.EXPECT().DropStatsTask(mock.Anything, mock.Anything).Return(nil).Once()

			s.NoError(m.DropStatsTask(context.TODO(), 1))
			_, ok := m.tasks.Get(1)
			s.False(ok)
			// Once recycled the segment becomes submittable again.
			s.False(m.HasStatsTask(s.segmentID, indexpb.StatsSubJob_Sort))

			s.NoError(m.DropStatsTask(context.TODO(), 1000))
		})
	})
}

func (s *statsTaskMetaSuite) TestReplaceRetryTask() {
	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil).Twice()

	m, err := newStatsTaskMeta(context.Background(), catalog)
	s.Require().NoError(err)
	s.Require().NoError(m.AddStatsTask(&indexpb.StatsTask{
		CollectionID: s.collectionID,
		PartitionID:  s.partitionID,
		SegmentID:    s.segmentID,
		TaskID:       1,
		Version:      9,
		NodeID:       100,
		SubJobType:   indexpb.StatsSubJob_TextIndexJob,
	}))
	s.Require().NoError(m.UpdateTaskState(1, indexpb.JobState_JobStateRetry, "worker unavailable"))

	var actions []metastore.UpdateAction
	catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, got ...metastore.UpdateAction) error {
			actions = got
			return nil
		}).Once()

	replacement, replaced, err := m.ReplaceRetryTask(context.Background(), 1, 2)
	s.Require().NoError(err)
	s.True(replaced)
	s.Require().NotNil(replacement)
	s.Equal(int64(2), replacement.GetTaskID())
	s.Zero(replacement.GetVersion())
	s.Zero(replacement.GetNodeID())
	s.Equal(indexpb.JobState_JobStateInit, replacement.GetState())
	s.Empty(replacement.GetFailReason())
	s.Nil(m.GetStatsTask(1))
	s.Same(replacement, m.GetStatsTask(2))
	s.Same(replacement, m.GetStatsTaskBySegmentID(s.segmentID, indexpb.StatsSubJob_TextIndexJob))

	s.Require().Len(actions, 2)
	drop, ok := actions[0].Entry.(metastore.StatsTaskEntry)
	s.True(ok)
	s.Equal(int64(1), drop.TaskID)
	add, ok := actions[1].Entry.(metastore.StatsTaskEntry)
	s.True(ok)
	s.Equal(int64(2), add.Task.GetTaskID())
}

func (s *statsTaskMetaSuite) TestGetPendingTaskCountFromPersistedMeta() {
	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return([]*indexpb.StatsTask{
		{TaskID: 1, SegmentID: 1, SubJobType: indexpb.StatsSubJob_TextIndexJob, State: indexpb.JobState_JobStateInit},
		{TaskID: 2, SegmentID: 2, SubJobType: indexpb.StatsSubJob_TextIndexJob, State: indexpb.JobState_JobStateRetry},
		{TaskID: 3, SegmentID: 3, SubJobType: indexpb.StatsSubJob_TextIndexJob, State: indexpb.JobState_JobStateInProgress},
		{TaskID: 4, SegmentID: 4, SubJobType: indexpb.StatsSubJob_TextIndexJob, State: indexpb.JobState_JobStateFinished},
		{TaskID: 5, SegmentID: 5, SubJobType: indexpb.StatsSubJob_TextIndexJob, State: indexpb.JobState_JobStateFailed},
	}, nil).Once()

	m, err := newStatsTaskMeta(context.Background(), catalog)
	s.Require().NoError(err)
	s.Equal(2, m.GetPendingTaskCount())
}

func Test_statsTaskMeta(t *testing.T) {
	suite.Run(t, new(statsTaskMetaSuite))
}
