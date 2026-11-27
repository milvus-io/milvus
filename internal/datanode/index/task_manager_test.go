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

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

type statsTaskInfoSuite struct {
	suite.Suite

	ctx     context.Context
	manager *TaskManager

	cluster string
	taskID  int64
}

func Test_statsTaskInfoSuite(t *testing.T) {
	suite.Run(t, new(statsTaskInfoSuite))
}

func (s *statsTaskInfoSuite) SetupSuite() {
	s.manager = NewTaskManager(context.Background())

	s.cluster = "test"
	s.taskID = 100
}

func (s *statsTaskInfoSuite) Test_Methods() {
	baseManifest := "test_base_manifest_path"
	textBaseManifest := "test_text_base_manifest_path"
	manifest := "test_manifest_path"
	jsonKeyStatsLogs := map[int64]*datapb.JsonKeyStats{
		100: {
			FieldID:                100,
			Version:                1,
			Files:                  []string{"file1"},
			LogSize:                1024,
			MemorySize:             1024,
			JsonKeyStatsDataFormat: common.JSONStatsDataFormatVersion,
		},
	}

	var attempt *StatsTaskInfo
	s.Run("loadOrStoreStatsTask", func() {
		_, cancel := context.WithCancel(s.manager.ctx) //nolint:gosec // cancel is deferred below
		defer cancel()
		attempt = &StatsTaskInfo{
			Cancel: cancel,
			State:  indexpb.JobState_JobStateInProgress,
		}

		reInfo := s.manager.LoadOrStoreStatsTask(s.cluster, s.taskID, attempt)
		s.Nil(reInfo)

		reInfo = s.manager.LoadOrStoreStatsTask(s.cluster, s.taskID, attempt)
		s.Equal(indexpb.JobState_JobStateInProgress, reInfo.State)
	})

	s.Run("getStatsTaskState", func() {
		s.Equal(indexpb.JobState_JobStateInProgress, s.manager.GetStatsTaskState(s.cluster, s.taskID))
		s.Equal(indexpb.JobState_JobStateNone, s.manager.GetStatsTaskState(s.cluster, s.taskID+1))
	})

	s.Run("storeStatsTaskState", func() {
		s.manager.StoreStatsTaskState(s.cluster, s.taskID, attempt, indexpb.JobState_JobStateFinished, "finished")
		s.Equal(indexpb.JobState_JobStateFinished, s.manager.GetStatsTaskState(s.cluster, s.taskID))
	})

	s.Run("storeStatsResult", func() {
		s.manager.StorePKSortStatsResult(s.cluster, s.taskID, attempt, 1, 2, 3, "ch1", 65535,
			[]*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 1}}}},
			[]*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 2}}}},
			[]*datapb.FieldBinlog{},
			"test_manifest_path",
		)
	})

	s.Run("storeStatsTextIndexResult", func() {
		s.manager.StoreStatsTextIndexResult(s.cluster, s.taskID, attempt, 1, 2, 3, "ch1",
			map[int64]*datapb.TextIndexStats{
				100: {
					FieldID:    100,
					Version:    1,
					Files:      []string{"file1"},
					LogSize:    1024,
					MemorySize: 1024,
				},
			}, textBaseManifest, "test_manifest_path")

		taskInfo := s.manager.GetStatsTaskInfo(s.cluster, s.taskID)
		s.Equal(textBaseManifest, taskInfo.ToStatsResult(s.taskID).GetBaseManifest())
	})

	s.Run("storeStatsJsonIndexResult", func() {
		s.manager.StoreJSONKeyStatsResult(s.cluster, s.taskID, attempt, 1, 2, 3, "ch1",
			jsonKeyStatsLogs, baseManifest, manifest)
	})

	s.Run("getStatsTaskInfo", func() {
		taskInfo := s.manager.GetStatsTaskInfo(s.cluster, s.taskID)

		s.Equal(indexpb.JobState_JobStateFinished, taskInfo.State)
		s.Equal(int64(1), taskInfo.CollID)
		s.Equal(int64(2), taskInfo.PartID)
		s.Equal(int64(3), taskInfo.SegID)
		s.Equal("ch1", taskInfo.InsertChannel)
		s.Equal(int64(65535), taskInfo.NumRows)

		result := taskInfo.ToStatsResult(s.taskID)
		s.Equal(baseManifest, result.GetBaseManifest())
		s.Equal(manifest, result.GetManifest())
		s.Equal(jsonKeyStatsLogs, result.GetJsonKeyStatsLogs())
	})

	s.Run("deleteStatsTaskInfos", func() {
		s.manager.DeleteStatsTaskInfos(s.ctx, []Key{{ClusterID: s.cluster, TaskID: s.taskID}})

		s.Nil(s.manager.GetStatsTaskInfo(s.cluster, s.taskID))
	})
}

func (s *statsTaskInfoSuite) TestIndexTaskInfoReturnsIndexStorePathVersion() {
	info := &IndexTaskInfo{
		State:                 commonpb.IndexState_Finished,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}

	s.Equal(
		indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
		info.ToIndexTaskInfo(100).GetIndexStorePathVersion(),
	)

	cloned := info.Clone()
	s.Equal(indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED, cloned.IndexStorePathVersion)
}

func (s *statsTaskInfoSuite) Test_IndexTaskCostMethods() {
	buildID := int64(200)
	attempt := &IndexTaskInfo{State: commonpb.IndexState_InProgress}
	s.manager.LoadOrStoreIndexTask(s.cluster, buildID, attempt)

	s.manager.StoreIndexTaskExecutionStart(s.cluster, buildID, attempt, 111, 4)
	s.manager.StoreIndexTaskExecutionEndWithState(s.cluster, buildID, attempt, 222, 111, commonpb.IndexState_Failed, "mock failure")

	info := s.manager.GetIndexTaskInfo(s.cluster, buildID)
	s.Require().NotNil(info)
	s.Equal(int64(111), info.ExecStartMs)
	s.Equal(int64(222), info.ExecEndMs)
	s.Equal(int64(111), info.CostTimeMs)
	s.Equal(int64(4), info.CostCPUNum)
	// state/failReason land together with the cost in one critical section
	s.Equal(commonpb.IndexState_Failed, info.State)
	s.Equal("mock failure", info.FailReason)

	info.CostCPUNum = 999
	reloaded := s.manager.GetIndexTaskInfo(s.cluster, buildID)
	s.Equal(int64(4), reloaded.CostCPUNum)

	s.manager.StoreIndexTaskExecutionStart(s.cluster, buildID, attempt, 333, 8)
	reloaded = s.manager.GetIndexTaskInfo(s.cluster, buildID)
	s.Equal(int64(333), reloaded.ExecStartMs)
	s.Equal(int64(8), reloaded.CostCPUNum)
	s.Equal(int64(0), reloaded.ExecEndMs)
	s.Equal(int64(0), reloaded.CostTimeMs)
}

func TestReplacementTaskIgnoresOldAttempt(t *testing.T) {
	ctx := context.Background()
	manager := NewTaskManager(ctx)
	key := Key{ClusterID: "cluster", TaskID: 42}
	keys := []Key{key}
	cluster, id := key.ClusterID, key.TaskID

	t.Run("index", func(t *testing.T) {
		old := &IndexTaskInfo{State: commonpb.IndexState_InProgress}
		current := &IndexTaskInfo{State: commonpb.IndexState_InProgress}
		require.Nil(t, manager.LoadOrStoreIndexTask(cluster, id, old))
		manager.DeleteIndexTaskInfos(ctx, keys)
		require.Nil(t, manager.LoadOrStoreIndexTask(cluster, id, current))
		before := manager.GetIndexTaskInfo(cluster, id)

		manager.StoreIndexTaskExecutionStart(cluster, id, old, 100, 2)
		manager.StoreIndexFilesAndStatistic(cluster, id, old, []string{"old"}, 10, 20, 1, 1, 0)
		manager.StoreIndexTaskState(cluster, id, old, commonpb.IndexState_Failed, "old")
		manager.StoreIndexTaskExecutionEndWithState(cluster, id, old, 110, 10, commonpb.IndexState_Retry, "canceled")
		require.Equal(t, before, manager.GetIndexTaskInfo(cluster, id))
		manager.RemoveIndexTask(key, old)

		manager.StoreIndexTaskExecutionEndWithState(cluster, id, current, 220, 20, commonpb.IndexState_Finished, "")
		require.Equal(t, commonpb.IndexState_Finished, manager.LoadIndexTaskState(cluster, id))
	})
	t.Run("analyze", func(t *testing.T) {
		old := &AnalyzeTaskInfo{State: indexpb.JobState_JobStateInProgress}
		current := &AnalyzeTaskInfo{State: indexpb.JobState_JobStateInProgress}
		require.Nil(t, manager.LoadOrStoreAnalyzeTask(cluster, id, old))
		manager.DeleteAnalyzeTaskInfos(ctx, keys)
		require.Nil(t, manager.LoadOrStoreAnalyzeTask(cluster, id, current))

		manager.StoreAnalyzeFilesAndStatistic(cluster, id, old, "old-centroids")
		manager.StoreAnalyzeTaskState(cluster, id, old, indexpb.JobState_JobStateRetry, "canceled")
		require.Equal(t, &AnalyzeTaskInfo{State: indexpb.JobState_JobStateInProgress}, manager.GetAnalyzeTaskInfo(cluster, id))
		manager.RemoveAnalyzeTask(key, old)

		manager.StoreAnalyzeTaskState(cluster, id, current, indexpb.JobState_JobStateFinished, "")
		require.Equal(t, indexpb.JobState_JobStateFinished, manager.LoadAnalyzeTaskState(cluster, id))
	})
	t.Run("stats", func(t *testing.T) {
		old := &StatsTaskInfo{State: indexpb.JobState_JobStateInProgress}
		current := &StatsTaskInfo{State: indexpb.JobState_JobStateInProgress}
		require.Nil(t, manager.LoadOrStoreStatsTask(cluster, id, old))
		manager.DeleteStatsTaskInfos(ctx, keys)
		require.Nil(t, manager.LoadOrStoreStatsTask(cluster, id, current))
		before := manager.GetStatsTaskInfo(cluster, id)

		manager.StorePKSortStatsResult(cluster, id, old, 1, 2, 3, "ch", 4, nil, nil, nil, "old-sort")
		manager.StoreStatsTextIndexResult(cluster, id, old, 1, 2, 3, "ch", nil, "base", "old-text")
		manager.StoreJSONKeyStatsResult(cluster, id, old, 1, 2, 3, "ch", nil, "base", "old-json")
		manager.StoreStatsTaskState(cluster, id, old, indexpb.JobState_JobStateRetry, "canceled")
		require.Equal(t, before, manager.GetStatsTaskInfo(cluster, id))
		manager.RemoveStatsTask(key, old)

		manager.StoreStatsTaskState(cluster, id, current, indexpb.JobState_JobStateFinished, "")
		require.Equal(t, indexpb.JobState_JobStateFinished, manager.GetStatsTaskState(cluster, id))
	})
}
