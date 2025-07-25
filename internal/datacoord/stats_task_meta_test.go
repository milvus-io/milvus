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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
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
		s.Run("normal case", func() {
			catalog := mocks.NewDataCoordCatalog(s.T())
			catalog.EXPECT().ListStatsTasks(mock.Anything).Return([]*indexpb.StatsTask{
				{
					CollectionID:    s.collectionID,
					PartitionID:     s.partitionID,
					SegmentID:       10000,
					InsertChannel:   "ch1",
					TaskID:          10001,
					Version:         1,
					NodeID:          0,
					State:           indexpb.JobState_JobStateFinished,
					FailReason:      "",
					TargetSegmentID: 10002,
					SubJobType:      indexpb.StatsSubJob_Sort,
					CanRecycle:      true,
				},
			}, nil)

			catalog.EXPECT().DropStatsTask(mock.Anything, mock.Anything).Return(nil)

			m, err := newStatsTaskMeta(context.Background(), catalog)
			s.NoError(err)
			s.NotNil(m)
		})

		s.Run("failed case", func() {
			catalog := mocks.NewDataCoordCatalog(s.T())
			catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, errors.New("mock error"))

			m, err := newStatsTaskMeta(context.Background(), catalog)
			s.Error(err)
			s.Nil(m)
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

	s.Run("UpdateVersion", func() {
		s.Run("normal case", func() {
			catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil).Once()

			s.NoError(m.UpdateVersion(1, 1180))
			task, ok := m.tasks.Get(1)
			s.True(ok)
			s.Equal(int64(1), task.GetVersion())
		})

		s.Run("task not exist", func() {
			_, ok := m.tasks.Get(100)
			s.False(ok)

			s.Error(m.UpdateVersion(100, 1180))
		})

		s.Run("failed case", func() {
			catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(errors.New("mock error")).Once()

			s.Error(m.UpdateVersion(1, 1180))
			task, ok := m.tasks.Get(1)
			s.True(ok)
			// still 1
			s.Equal(int64(1), task.GetVersion())
		})
	})

	s.Run("UpdateBuildingTask", func() {
		s.Run("failed case", func() {
			catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(errors.New("mock error")).Once()

			s.Error(m.UpdateBuildingTask(1))
			task, ok := m.tasks.Get(1)
			s.True(ok)
			s.Equal(indexpb.JobState_JobStateInit, task.GetState())
			s.Equal(int64(1180), task.GetNodeID())
		})

		s.Run("normal case", func() {
			catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil).Once()

			s.NoError(m.UpdateBuildingTask(1))
			task, ok := m.tasks.Get(1)
			s.True(ok)
			s.Equal(indexpb.JobState_JobStateInProgress, task.GetState())
			s.Equal(int64(1180), task.GetNodeID())
		})

		s.Run("task not exist", func() {
			_, ok := m.tasks.Get(100)
			s.False(ok)

			s.Error(m.UpdateBuildingTask(100))
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

			s.NoError(m.DropStatsTask(context.TODO(), 1000))
		})
	})
}

func Test_statsTaskMeta(t *testing.T) {
	suite.Run(t, new(statsTaskMetaSuite))
}
