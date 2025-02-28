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

package index

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

type statsTaskInfoSuite struct {
	suite.Suite

	ctx     context.Context
	manager *Manager

	cluster string
	taskID  int64
}

func Test_statsTaskInfoSuite(t *testing.T) {
	suite.Run(t, new(statsTaskInfoSuite))
}

func (s *statsTaskInfoSuite) SetupSuite() {
	s.manager = NewManager(context.Background())

	s.cluster = "test"
	s.taskID = 100
}

func (s *statsTaskInfoSuite) Test_Methods() {
	s.Run("loadOrStoreStatsTask", func() {
		_, cancel := context.WithCancel(s.manager.ctx)
		info := &StatsTaskInfo{
			Cancel: cancel,
			State:  indexpb.JobState_JobStateInProgress,
		}

		reInfo := s.manager.LoadOrStoreStatsTask(s.cluster, s.taskID, info)
		s.Nil(reInfo)

		reInfo = s.manager.LoadOrStoreStatsTask(s.cluster, s.taskID, info)
		s.Equal(indexpb.JobState_JobStateInProgress, reInfo.State)
	})

	s.Run("getStatsTaskState", func() {
		s.Equal(indexpb.JobState_JobStateInProgress, s.manager.GetStatsTaskState(s.cluster, s.taskID))
		s.Equal(indexpb.JobState_JobStateNone, s.manager.GetStatsTaskState(s.cluster, s.taskID+1))
	})

	s.Run("storeStatsTaskState", func() {
		s.manager.StoreStatsTaskState(s.cluster, s.taskID, indexpb.JobState_JobStateFinished, "finished")
		s.Equal(indexpb.JobState_JobStateFinished, s.manager.GetStatsTaskState(s.cluster, s.taskID))
	})

	s.Run("storeStatsResult", func() {
		s.manager.StorePKSortStatsResult(s.cluster, s.taskID, 1, 2, 3, "ch1", 65535,
			[]*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 1}}}},
			[]*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 2}}}},
			[]*datapb.FieldBinlog{},
		)
	})

	s.Run("storeStatsTextIndexResult", func() {
		s.manager.StoreStatsTextIndexResult(s.cluster, s.taskID, 1, 2, 3, "ch1",
			map[int64]*datapb.TextIndexStats{
				100: {
					FieldID:    100,
					Version:    1,
					Files:      []string{"file1"},
					LogSize:    1024,
					MemorySize: 1024,
				},
			})
	})

	s.Run("getStatsTaskInfo", func() {
		taskInfo := s.manager.GetStatsTaskInfo(s.cluster, s.taskID)

		s.Equal(indexpb.JobState_JobStateFinished, taskInfo.State)
		s.Equal(int64(1), taskInfo.CollID)
		s.Equal(int64(2), taskInfo.PartID)
		s.Equal(int64(3), taskInfo.SegID)
		s.Equal("ch1", taskInfo.InsertChannel)
		s.Equal(int64(65535), taskInfo.NumRows)
	})

	s.Run("deleteStatsTaskInfos", func() {
		s.manager.DeleteStatsTaskInfos(s.ctx, []Key{{ClusterID: s.cluster, TaskID: s.taskID}})

		s.Nil(s.manager.GetStatsTaskInfo(s.cluster, s.taskID))
	})
}
