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
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
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

func (i *IndexTaskInfo) Clone() *IndexTaskInfo {
	return &IndexTaskInfo{
		Cancel:                    i.Cancel,
		State:                     i.State,
		FileKeys:                  common.CloneStringList(i.FileKeys),
		SerializedSize:            i.SerializedSize,
		MemSize:                   i.MemSize,
		FailReason:                i.FailReason,
		CurrentIndexVersion:       i.CurrentIndexVersion,
		IndexStoreVersion:         i.IndexStoreVersion,
		CurrentScalarIndexVersion: i.CurrentScalarIndexVersion,
		statistic:                 typeutil.Clone(i.statistic),
	}
}

func (i *IndexTaskInfo) ToIndexTaskInfo(buildID int64) *workerpb.IndexTaskInfo {
	return &workerpb.IndexTaskInfo{
		BuildID:                   buildID,
		State:                     i.State,
		IndexFileKeys:             i.FileKeys,
		SerializedSize:            i.SerializedSize,
		MemSize:                   i.MemSize,
		FailReason:                i.FailReason,
		CurrentIndexVersion:       i.CurrentIndexVersion,
		IndexStoreVersion:         i.IndexStoreVersion,
		CurrentScalarIndexVersion: i.CurrentScalarIndexVersion,
	}
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
	Cancel           context.CancelFunc
	State            indexpb.JobState
	FailReason       string
	CollID           typeutil.UniqueID
	PartID           typeutil.UniqueID
	SegID            typeutil.UniqueID
	InsertChannel    string
	NumRows          int64
	InsertLogs       []*datapb.FieldBinlog
	StatsLogs        []*datapb.FieldBinlog
	TextStatsLogs    map[int64]*datapb.TextIndexStats
	Bm25Logs         []*datapb.FieldBinlog
	JSONKeyStatsLogs map[int64]*datapb.JsonKeyStats
}

func (s *StatsTaskInfo) Clone() *StatsTaskInfo {
	return &StatsTaskInfo{
		Cancel:           s.Cancel,
		State:            s.State,
		FailReason:       s.FailReason,
		CollID:           s.CollID,
		PartID:           s.PartID,
		SegID:            s.SegID,
		InsertChannel:    s.InsertChannel,
		NumRows:          s.NumRows,
		InsertLogs:       s.CloneInsertLogs(),
		StatsLogs:        s.CloneStatsLogs(),
		TextStatsLogs:    s.CloneTextStatsLogs(),
		Bm25Logs:         s.CloneBm25Logs(),
		JSONKeyStatsLogs: s.CloneJSONKeyStatsLogs(),
	}
}

func (s *StatsTaskInfo) ToStatsResult(taskID int64) *workerpb.StatsResult {
	return &workerpb.StatsResult{
		TaskID:           taskID,
		State:            s.State,
		FailReason:       s.FailReason,
		CollectionID:     s.CollID,
		PartitionID:      s.PartID,
		SegmentID:        s.SegID,
		Channel:          s.InsertChannel,
		InsertLogs:       s.InsertLogs,
		StatsLogs:        s.StatsLogs,
		TextStatsLogs:    s.TextStatsLogs,
		Bm25Logs:         s.Bm25Logs,
		NumRows:          s.NumRows,
		JsonKeyStatsLogs: s.JSONKeyStatsLogs,
	}
}

func (s *StatsTaskInfo) CloneInsertLogs() []*datapb.FieldBinlog {
	clone := make([]*datapb.FieldBinlog, len(s.InsertLogs))
	for i, log := range s.InsertLogs {
		clone[i] = typeutil.Clone(log)
	}
	return clone
}

func (s *StatsTaskInfo) CloneStatsLogs() []*datapb.FieldBinlog {
	clone := make([]*datapb.FieldBinlog, len(s.StatsLogs))
	for i, log := range s.StatsLogs {
		clone[i] = typeutil.Clone(log)
	}
	return clone
}

func (s *StatsTaskInfo) CloneTextStatsLogs() map[int64]*datapb.TextIndexStats {
	clone := make(map[int64]*datapb.TextIndexStats)
	for k, v := range s.TextStatsLogs {
		clone[k] = typeutil.Clone(v)
	}
	return clone
}

func (s *StatsTaskInfo) CloneBm25Logs() []*datapb.FieldBinlog {
	clone := make([]*datapb.FieldBinlog, len(s.Bm25Logs))
	for i, log := range s.Bm25Logs {
		clone[i] = typeutil.Clone(log)
	}
	return clone
}

func (s *StatsTaskInfo) CloneJSONKeyStatsLogs() map[int64]*datapb.JsonKeyStats {
	clone := make(map[int64]*datapb.JsonKeyStats)
	for k, v := range s.JSONKeyStatsLogs {
		clone[k] = typeutil.Clone(v)
	}
	return clone
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

func (m *TaskManager) StoreJSONKeyStatsResult(
	clusterID string,
	taskID typeutil.UniqueID,
	collID typeutil.UniqueID,
	partID typeutil.UniqueID,
	segID typeutil.UniqueID,
	channel string,
	jsonKeyIndexLogs map[int64]*datapb.JsonKeyStats,
) {
	key := Key{ClusterID: clusterID, TaskID: taskID}
	m.stateLock.Lock()
	defer m.stateLock.Unlock()
	if info, ok := m.statsTasks[key]; ok {
		info.JSONKeyStatsLogs = jsonKeyIndexLogs
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
		return info.Clone()
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
