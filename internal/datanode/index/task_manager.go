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

import "C"

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type IndexTaskInfo struct {
	Cancel                    context.CancelFunc
	State                     commonpb.IndexState
	FileKeys                  []string
	SerializedSize            uint64
	MemSize                   uint64
	FailReason                string
	CurrentIndexVersion       int32
	IndexStoreVersion         int64
	CurrentScalarIndexVersion int32

	// task statistics
	statistic *indexpb.JobInfo
}

type TaskManager struct {
	ctx          context.Context
	stateLock    sync.Mutex
	indexTasks   map[Key]*IndexTaskInfo
	analyzeTasks map[Key]*AnalyzeTaskInfo
	statsTasks   map[Key]*StatsTaskInfo
}

func NewTaskManager(ctx context.Context) *TaskManager {
	return &TaskManager{
		ctx:          ctx,
		indexTasks:   make(map[Key]*IndexTaskInfo),
		analyzeTasks: make(map[Key]*AnalyzeTaskInfo),
		statsTasks:   make(map[Key]*StatsTaskInfo),
	}
}

func (m *TaskManager) LoadOrStoreIndexTask(ClusterID string, buildID typeutil.UniqueID, info *IndexTaskInfo) *IndexTaskInfo {
	m.stateLock.Lock()
	defer m.stateLock.Unlock()
	key := Key{ClusterID: ClusterID, TaskID: buildID}
	oldInfo, ok := m.indexTasks[key]
	if ok {
		return oldInfo
	}
	m.indexTasks[key] = info
	return nil
}

func (m *TaskManager) LoadIndexTaskState(ClusterID string, buildID typeutil.UniqueID) commonpb.IndexState {
	key := Key{ClusterID: ClusterID, TaskID: buildID}
	m.stateLock.Lock()
	defer m.stateLock.Unlock()
	task, ok := m.indexTasks[key]
	if !ok {
		return commonpb.IndexState_IndexStateNone
	}
	return task.State
}

func (m *TaskManager) StoreIndexTaskState(ClusterID string, buildID typeutil.UniqueID, state commonpb.IndexState, failReason string) {
	key := Key{ClusterID: ClusterID, TaskID: buildID}
	m.stateLock.Lock()
	defer m.stateLock.Unlock()
	if task, ok := m.indexTasks[key]; ok {
		log.Ctx(m.ctx).Debug("store task state", zap.String("clusterID", ClusterID), zap.Int64("buildID", buildID),
			zap.String("state", state.String()), zap.String("fail reason", failReason))
		task.State = state
		task.FailReason = failReason
	}
}

func (m *TaskManager) ForeachIndexTaskInfo(fn func(ClusterID string, buildID typeutil.UniqueID, info *IndexTaskInfo)) {
	m.stateLock.Lock()
	defer m.stateLock.Unlock()
	for key, info := range m.indexTasks {
		fn(key.ClusterID, key.TaskID, info)
	}
}

func (m *TaskManager) StoreIndexFilesAndStatistic(
	ClusterID string,
	buildID typeutil.UniqueID,
	fileKeys []string,
	serializedSize uint64,
	memSize uint64,
	currentIndexVersion int32,
	currentScalarIndexVersion int32,
) {
	key := Key{ClusterID: ClusterID, TaskID: buildID}
	m.stateLock.Lock()
	defer m.stateLock.Unlock()
	if info, ok := m.indexTasks[key]; ok {
		info.FileKeys = common.CloneStringList(fileKeys)
		info.SerializedSize = serializedSize
		info.MemSize = memSize
		info.CurrentIndexVersion = currentIndexVersion
		info.CurrentScalarIndexVersion = currentScalarIndexVersion
		return
	}
}

func (m *TaskManager) DeleteIndexTaskInfos(ctx context.Context, keys []Key) []*IndexTaskInfo {
	m.stateLock.Lock()
	defer m.stateLock.Unlock()
	deleted := make([]*IndexTaskInfo, 0, len(keys))
	for _, key := range keys {
		info, ok := m.indexTasks[key]
		if ok {
			deleted = append(deleted, info)
			delete(m.indexTasks, key)
			log.Ctx(ctx).Info("delete task infos",
				zap.String("cluster_id", key.ClusterID), zap.Int64("build_id", key.TaskID))
		}
	}
	return deleted
}

func (m *TaskManager) deleteAllIndexTasks() []*IndexTaskInfo {
	m.stateLock.Lock()
	deletedTasks := m.indexTasks
	m.indexTasks = make(map[Key]*IndexTaskInfo)
	m.stateLock.Unlock()

	deleted := make([]*IndexTaskInfo, 0, len(deletedTasks))
	for _, info := range deletedTasks {
		deleted = append(deleted, info)
	}
	return deleted
}

type AnalyzeTaskInfo struct {
	Cancel        context.CancelFunc
	State         indexpb.JobState
	FailReason    string
	CentroidsFile string
}

func (m *TaskManager) LoadOrStoreAnalyzeTask(clusterID string, taskID typeutil.UniqueID, info *AnalyzeTaskInfo) *AnalyzeTaskInfo {
	m.stateLock.Lock()
	defer m.stateLock.Unlock()
	key := Key{ClusterID: clusterID, TaskID: taskID}
	oldInfo, ok := m.analyzeTasks[key]
	if ok {
		return oldInfo
	}
	m.analyzeTasks[key] = info
	return nil
}

func (m *TaskManager) LoadAnalyzeTaskState(clusterID string, taskID typeutil.UniqueID) indexpb.JobState {
	key := Key{ClusterID: clusterID, TaskID: taskID}
	m.stateLock.Lock()
	defer m.stateLock.Unlock()
	task, ok := m.analyzeTasks[key]
	if !ok {
		return indexpb.JobState_JobStateNone
	}
	return task.State
}

func (m *TaskManager) StoreAnalyzeTaskState(clusterID string, taskID typeutil.UniqueID, state indexpb.JobState, failReason string) {
	key := Key{ClusterID: clusterID, TaskID: taskID}
	m.stateLock.Lock()
	defer m.stateLock.Unlock()
	if task, ok := m.analyzeTasks[key]; ok {
		log.Info("store analyze task state", zap.String("clusterID", clusterID), zap.Int64("TaskID", taskID),
			zap.String("state", state.String()), zap.String("fail reason", failReason))
		task.State = state
		task.FailReason = failReason
	}
}

func (m *TaskManager) StoreAnalyzeFilesAndStatistic(
	ClusterID string,
	taskID typeutil.UniqueID,
	centroidsFile string,
) {
	key := Key{ClusterID: ClusterID, TaskID: taskID}
	m.stateLock.Lock()
	defer m.stateLock.Unlock()
	if info, ok := m.analyzeTasks[key]; ok {
		info.CentroidsFile = centroidsFile
		return
	}
}

func (m *TaskManager) GetAnalyzeTaskInfo(clusterID string, taskID typeutil.UniqueID) *AnalyzeTaskInfo {
	m.stateLock.Lock()
	defer m.stateLock.Unlock()

	if info, ok := m.analyzeTasks[Key{ClusterID: clusterID, TaskID: taskID}]; ok {
		return &AnalyzeTaskInfo{
			Cancel:        info.Cancel,
			State:         info.State,
			FailReason:    info.FailReason,
			CentroidsFile: info.CentroidsFile,
		}
	}
	return nil
}

func (m *TaskManager) DeleteAnalyzeTaskInfos(ctx context.Context, keys []Key) []*AnalyzeTaskInfo {
	m.stateLock.Lock()
	defer m.stateLock.Unlock()
	deleted := make([]*AnalyzeTaskInfo, 0, len(keys))
	for _, key := range keys {
		info, ok := m.analyzeTasks[key]
		if ok {
			deleted = append(deleted, info)
			delete(m.analyzeTasks, key)
			log.Ctx(ctx).Info("delete analyze task infos",
				zap.String("clusterID", key.ClusterID), zap.Int64("TaskID", key.TaskID))
		}
	}
	return deleted
}

func (m *TaskManager) deleteAllAnalyzeTasks() []*AnalyzeTaskInfo {
	m.stateLock.Lock()
	deletedTasks := m.analyzeTasks
	m.analyzeTasks = make(map[Key]*AnalyzeTaskInfo)
	m.stateLock.Unlock()

	deleted := make([]*AnalyzeTaskInfo, 0, len(deletedTasks))
	for _, info := range deletedTasks {
		deleted = append(deleted, info)
	}
	return deleted
}

func (m *TaskManager) HasInProgressTask() bool {
	m.stateLock.Lock()
	defer m.stateLock.Unlock()
	for _, info := range m.indexTasks {
		if info.State == commonpb.IndexState_InProgress {
			return true
		}
	}

	for _, info := range m.analyzeTasks {
		if info.State == indexpb.JobState_JobStateInProgress {
			return true
		}
	}
	return false
}

func (m *TaskManager) WaitTaskFinish() {
	if !m.HasInProgressTask() {
		return
	}

	gracefulTimeout := &paramtable.Get().DataNodeCfg.GracefulStopTimeout
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	timeoutCtx, cancel := context.WithTimeout(m.ctx, gracefulTimeout.GetAsDuration(time.Second))
	defer cancel()
	for {
		select {
		case <-ticker.C:
			if !m.HasInProgressTask() {
				return
			}
		case <-timeoutCtx.Done():
			log.Warn("timeout, the index node has some progress task")
			for _, info := range m.indexTasks {
				if info.State == commonpb.IndexState_InProgress {
					log.Warn("progress task", zap.Any("info", info))
				}
			}
			for _, info := range m.analyzeTasks {
				if info.State == indexpb.JobState_JobStateInProgress {
					log.Warn("progress task", zap.Any("info", info))
				}
			}
			return
		}
	}
}

type StatsTaskInfo struct {
	Cancel        context.CancelFunc
	State         indexpb.JobState
	FailReason    string
	CollID        typeutil.UniqueID
	PartID        typeutil.UniqueID
	SegID         typeutil.UniqueID
	InsertChannel string
	NumRows       int64
	InsertLogs    []*datapb.FieldBinlog
	StatsLogs     []*datapb.FieldBinlog
	TextStatsLogs map[int64]*datapb.TextIndexStats
	Bm25Logs      []*datapb.FieldBinlog
}

func (m *TaskManager) LoadOrStoreStatsTask(clusterID string, taskID typeutil.UniqueID, info *StatsTaskInfo) *StatsTaskInfo {
	m.stateLock.Lock()
	defer m.stateLock.Unlock()
	key := Key{ClusterID: clusterID, TaskID: taskID}
	oldInfo, ok := m.statsTasks[key]
	if ok {
		return oldInfo
	}
	m.statsTasks[key] = info
	return nil
}

func (m *TaskManager) GetStatsTaskState(clusterID string, taskID typeutil.UniqueID) indexpb.JobState {
	key := Key{ClusterID: clusterID, TaskID: taskID}
	m.stateLock.Lock()
	defer m.stateLock.Unlock()
	task, ok := m.statsTasks[key]
	if !ok {
		return indexpb.JobState_JobStateNone
	}
	return task.State
}

func (m *TaskManager) StoreStatsTaskState(clusterID string, taskID typeutil.UniqueID, state indexpb.JobState, failReason string) {
	key := Key{ClusterID: clusterID, TaskID: taskID}
	m.stateLock.Lock()
	defer m.stateLock.Unlock()
	if task, ok := m.statsTasks[key]; ok {
		log.Info("store stats task state", zap.String("clusterID", clusterID), zap.Int64("TaskID", taskID),
			zap.String("state", state.String()), zap.String("fail reason", failReason))
		task.State = state
		task.FailReason = failReason
	}
}

func (m *TaskManager) StorePKSortStatsResult(
	ClusterID string,
	taskID typeutil.UniqueID,
	collID typeutil.UniqueID,
	partID typeutil.UniqueID,
	segID typeutil.UniqueID,
	channel string,
	numRows int64,
	insertLogs []*datapb.FieldBinlog,
	statsLogs []*datapb.FieldBinlog,
	bm25Logs []*datapb.FieldBinlog,
) {
	key := Key{ClusterID: ClusterID, TaskID: taskID}
	m.stateLock.Lock()
	defer m.stateLock.Unlock()
	if info, ok := m.statsTasks[key]; ok {
		info.CollID = collID
		info.PartID = partID
		info.SegID = segID
		info.InsertChannel = channel
		info.NumRows = numRows
		info.InsertLogs = insertLogs
		info.StatsLogs = statsLogs
		info.Bm25Logs = bm25Logs
		return
	}
}

func (m *TaskManager) StoreStatsTextIndexResult(
	ClusterID string,
	taskID typeutil.UniqueID,
	collID typeutil.UniqueID,
	partID typeutil.UniqueID,
	segID typeutil.UniqueID,
	channel string,
	texIndexLogs map[int64]*datapb.TextIndexStats,
) {
	key := Key{ClusterID: ClusterID, TaskID: taskID}
	m.stateLock.Lock()
	defer m.stateLock.Unlock()
	if info, ok := m.statsTasks[key]; ok {
		info.TextStatsLogs = texIndexLogs
		info.SegID = segID
		info.CollID = collID
		info.PartID = partID
		info.InsertChannel = channel
	}
}

func (m *TaskManager) GetStatsTaskInfo(clusterID string, taskID typeutil.UniqueID) *StatsTaskInfo {
	m.stateLock.Lock()
	defer m.stateLock.Unlock()

	if info, ok := m.statsTasks[Key{ClusterID: clusterID, TaskID: taskID}]; ok {
		return &StatsTaskInfo{
			Cancel:        info.Cancel,
			State:         info.State,
			FailReason:    info.FailReason,
			CollID:        info.CollID,
			PartID:        info.PartID,
			SegID:         info.SegID,
			InsertChannel: info.InsertChannel,
			NumRows:       info.NumRows,
			InsertLogs:    info.InsertLogs,
			StatsLogs:     info.StatsLogs,
			TextStatsLogs: info.TextStatsLogs,
			Bm25Logs:      info.Bm25Logs,
		}
	}
	return nil
}

func (m *TaskManager) DeleteStatsTaskInfos(ctx context.Context, keys []Key) []*StatsTaskInfo {
	m.stateLock.Lock()
	defer m.stateLock.Unlock()
	deleted := make([]*StatsTaskInfo, 0, len(keys))
	for _, key := range keys {
		info, ok := m.statsTasks[key]
		if ok {
			deleted = append(deleted, info)
			delete(m.statsTasks, key)
			log.Ctx(ctx).Info("delete stats task infos",
				zap.String("clusterID", key.ClusterID), zap.Int64("TaskID", key.TaskID))
		}
	}
	return deleted
}

func (m *TaskManager) deleteAllStatsTasks() []*StatsTaskInfo {
	m.stateLock.Lock()
	deletedTasks := m.statsTasks
	m.statsTasks = make(map[Key]*StatsTaskInfo)
	m.stateLock.Unlock()

	deleted := make([]*StatsTaskInfo, 0, len(deletedTasks))
	for _, info := range deletedTasks {
		deleted = append(deleted, info)
	}
	return deleted
}

func (m *TaskManager) DeleteAllTasks() {
	deletedIndexTasks := m.deleteAllIndexTasks()
	for _, t := range deletedIndexTasks {
		if t.Cancel != nil {
			t.Cancel()
		}
	}
	deletedAnalyzeTasks := m.deleteAllAnalyzeTasks()
	for _, t := range deletedAnalyzeTasks {
		if t.Cancel != nil {
			t.Cancel()
		}
	}
	deletedStatsTasks := m.deleteAllStatsTasks()
	for _, t := range deletedStatsTasks {
		if t.Cancel != nil {
			t.Cancel()
		}
	}
}
