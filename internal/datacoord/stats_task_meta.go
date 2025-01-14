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
	"strconv"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

type statsTaskMeta struct {
	sync.RWMutex

	ctx     context.Context
	catalog metastore.DataCoordCatalog

	// taskID -> analyzeStats
	tasks map[int64]*indexpb.StatsTask
}

func newStatsTaskMeta(ctx context.Context, catalog metastore.DataCoordCatalog) (*statsTaskMeta, error) {
	stm := &statsTaskMeta{
		ctx:     ctx,
		catalog: catalog,
		tasks:   make(map[int64]*indexpb.StatsTask),
	}
	if err := stm.reloadFromKV(); err != nil {
		return nil, err
	}
	return stm, nil
}

func (stm *statsTaskMeta) reloadFromKV() error {
	record := timerecord.NewTimeRecorder("statsTaskMeta-reloadFromKV")
	// load stats task
	statsTasks, err := stm.catalog.ListStatsTasks(stm.ctx)
	if err != nil {
		log.Error("statsTaskMeta reloadFromKV load stats tasks failed", zap.Error(err))
		return err
	}
	for _, t := range statsTasks {
		stm.tasks[t.GetTaskID()] = t
	}

	log.Info("statsTaskMeta reloadFromKV done", zap.Duration("duration", record.ElapseSpan()))
	return nil
}

func (stm *statsTaskMeta) updateMetrics() {
	taskMetrics := make(map[UniqueID]map[indexpb.JobState]int)
	for _, t := range stm.tasks {
		if _, ok := taskMetrics[t.GetCollectionID()]; !ok {
			taskMetrics[t.GetCollectionID()] = make(map[indexpb.JobState]int)
			taskMetrics[t.GetCollectionID()][indexpb.JobState_JobStateNone] = 0
			taskMetrics[t.GetCollectionID()][indexpb.JobState_JobStateInit] = 0
			taskMetrics[t.GetCollectionID()][indexpb.JobState_JobStateInProgress] = 0
			taskMetrics[t.GetCollectionID()][indexpb.JobState_JobStateFinished] = 0
			taskMetrics[t.GetCollectionID()][indexpb.JobState_JobStateFailed] = 0
			taskMetrics[t.GetCollectionID()][indexpb.JobState_JobStateRetry] = 0
		}
		taskMetrics[t.GetCollectionID()][t.GetState()]++
	}

	jobType := indexpb.JobType_JobTypeStatsJob.String()
	for collID, m := range taskMetrics {
		for k, v := range m {
			metrics.TaskNum.WithLabelValues(strconv.FormatInt(collID, 10), jobType, k.String()).Set(float64(v))
		}
	}
}

func (stm *statsTaskMeta) AddStatsTask(t *indexpb.StatsTask) error {
	stm.Lock()
	defer stm.Unlock()

	for _, st := range stm.tasks {
		if st.GetSegmentID() == t.GetSegmentID() && st.GetSubJobType() == t.GetSubJobType() {
			msg := fmt.Sprintf("stats task already exist in meta of segment %d with subJobType: %s",
				t.GetSegmentID(), t.GetSubJobType().String())
			log.Warn(msg)
			return merr.WrapErrTaskDuplicate(indexpb.JobType_JobTypeStatsJob.String(), msg)
		}
	}

	log.Info("add stats task", zap.Int64("taskID", t.GetTaskID()), zap.Int64("originSegmentID", t.GetSegmentID()),
		zap.Int64("targetSegmentID", t.GetTargetSegmentID()), zap.String("subJobType", t.GetSubJobType().String()))
	t.State = indexpb.JobState_JobStateInit

	if err := stm.catalog.SaveStatsTask(stm.ctx, t); err != nil {
		log.Warn("adding stats task failed",
			zap.Int64("taskID", t.GetTaskID()),
			zap.Int64("segmentID", t.GetSegmentID()),
			zap.String("subJobType", t.GetSubJobType().String()),
			zap.Error(err))
		return err
	}

	stm.tasks[t.GetTaskID()] = t
	stm.updateMetrics()

	log.Info("add stats task success", zap.Int64("taskID", t.GetTaskID()), zap.Int64("originSegmentID", t.GetSegmentID()),
		zap.Int64("targetSegmentID", t.GetTargetSegmentID()), zap.String("subJobType", t.GetSubJobType().String()))
	return nil
}

func (stm *statsTaskMeta) DropStatsTask(taskID int64) error {
	stm.Lock()
	defer stm.Unlock()

	log.Info("drop stats task by taskID", zap.Int64("taskID", taskID))

	t, ok := stm.tasks[taskID]
	if !ok {
		log.Info("remove stats task success, task already not exist", zap.Int64("taskID", taskID))
		return nil
	}
	if err := stm.catalog.DropStatsTask(stm.ctx, taskID); err != nil {
		log.Warn("drop stats task failed",
			zap.Int64("taskID", taskID),
			zap.Int64("segmentID", taskID),
			zap.Error(err))
		return err
	}

	delete(stm.tasks, taskID)
	stm.updateMetrics()

	log.Info("remove stats task success", zap.Int64("taskID", taskID), zap.Int64("segmentID", t.SegmentID))
	return nil
}

func (stm *statsTaskMeta) UpdateVersion(taskID, nodeID int64) error {
	stm.Lock()
	defer stm.Unlock()

	t, ok := stm.tasks[taskID]
	if !ok {
		return fmt.Errorf("task %d not found", taskID)
	}

	cloneT := proto.Clone(t).(*indexpb.StatsTask)
	cloneT.Version++
	cloneT.NodeID = nodeID

	if err := stm.catalog.SaveStatsTask(stm.ctx, cloneT); err != nil {
		log.Warn("update stats task version failed",
			zap.Int64("taskID", t.GetTaskID()),
			zap.Int64("segmentID", t.GetSegmentID()),
			zap.Int64("nodeID", nodeID),
			zap.Error(err))
		return err
	}

	stm.tasks[t.TaskID] = cloneT
	stm.updateMetrics()
	log.Info("update stats task version success", zap.Int64("taskID", taskID), zap.Int64("nodeID", nodeID),
		zap.Int64("newVersion", cloneT.GetVersion()))
	return nil
}

func (stm *statsTaskMeta) UpdateBuildingTask(taskID int64) error {
	stm.Lock()
	defer stm.Unlock()

	t, ok := stm.tasks[taskID]
	if !ok {
		return fmt.Errorf("task %d not found", taskID)
	}

	cloneT := proto.Clone(t).(*indexpb.StatsTask)
	cloneT.State = indexpb.JobState_JobStateInProgress

	if err := stm.catalog.SaveStatsTask(stm.ctx, cloneT); err != nil {
		log.Warn("update stats task state building failed",
			zap.Int64("taskID", t.GetTaskID()),
			zap.Int64("segmentID", t.GetSegmentID()),
			zap.Error(err))
		return err
	}

	stm.tasks[t.TaskID] = cloneT
	stm.updateMetrics()

	log.Info("update building stats task success", zap.Int64("taskID", taskID))
	return nil
}

func (stm *statsTaskMeta) FinishTask(taskID int64, result *workerpb.StatsResult) error {
	stm.Lock()
	defer stm.Unlock()

	t, ok := stm.tasks[taskID]
	if !ok {
		return fmt.Errorf("task %d not found", taskID)
	}

	cloneT := proto.Clone(t).(*indexpb.StatsTask)
	cloneT.State = result.GetState()
	cloneT.FailReason = result.GetFailReason()

	if err := stm.catalog.SaveStatsTask(stm.ctx, cloneT); err != nil {
		log.Warn("finish stats task state failed",
			zap.Int64("taskID", t.GetTaskID()),
			zap.Int64("segmentID", t.GetSegmentID()),
			zap.Error(err))
		return err
	}

	stm.tasks[t.TaskID] = cloneT
	stm.updateMetrics()

	log.Info("finish stats task meta success", zap.Int64("taskID", taskID), zap.Int64("segmentID", t.SegmentID),
		zap.String("state", result.GetState().String()), zap.String("failReason", t.GetFailReason()))
	return nil
}

func (stm *statsTaskMeta) GetStatsTaskState(taskID int64) indexpb.JobState {
	stm.RLock()
	defer stm.RUnlock()

	t, ok := stm.tasks[taskID]
	if !ok {
		return indexpb.JobState_JobStateNone
	}
	return t.GetState()
}

func (stm *statsTaskMeta) GetStatsTaskStateBySegmentID(segmentID int64, jobType indexpb.StatsSubJob) indexpb.JobState {
	stm.RLock()
	defer stm.RUnlock()

	for _, t := range stm.tasks {
		if segmentID == t.GetSegmentID() && jobType == t.GetSubJobType() {
			return t.GetState()
		}
	}

	return indexpb.JobState_JobStateNone
}

func (stm *statsTaskMeta) CanCleanedTasks() []int64 {
	stm.RLock()
	defer stm.RUnlock()

	needCleanedTaskIDs := make([]int64, 0)
	for taskID, t := range stm.tasks {
		if t.GetCanRecycle() && (t.GetState() == indexpb.JobState_JobStateFinished ||
			t.GetState() == indexpb.JobState_JobStateFailed) {
			needCleanedTaskIDs = append(needCleanedTaskIDs, taskID)
		}
	}
	return needCleanedTaskIDs
}

func (stm *statsTaskMeta) GetAllTasks() map[int64]*indexpb.StatsTask {
	tasks := make(map[int64]*indexpb.StatsTask)

	stm.RLock()
	defer stm.RUnlock()
	for k, v := range stm.tasks {
		tasks[k] = proto.Clone(v).(*indexpb.StatsTask)
	}
	return tasks
}

func (stm *statsTaskMeta) GetStatsTaskBySegmentID(segmentID int64, subJobType indexpb.StatsSubJob) *indexpb.StatsTask {
	stm.RLock()
	defer stm.RUnlock()

	log.Info("get stats task by segmentID", zap.Int64("segmentID", segmentID),
		zap.String("subJobType", subJobType.String()))

	for taskID, t := range stm.tasks {
		if t.GetSegmentID() == segmentID && t.GetSubJobType() == subJobType {
			log.Info("get stats task by segmentID success",
				zap.Int64("taskID", taskID),
				zap.Int64("segmentID", segmentID),
				zap.String("subJobType", subJobType.String()))
			return t
		}
	}

	log.Info("get stats task by segmentID failed, task not exist", zap.Int64("segmentID", segmentID),
		zap.String("subJobType", subJobType.String()))
	return nil
}

func (stm *statsTaskMeta) MarkTaskCanRecycle(taskID int64) error {
	stm.Lock()
	defer stm.Unlock()

	log.Info("mark stats task can recycle", zap.Int64("taskID", taskID))

	t, ok := stm.tasks[taskID]
	if !ok {
		return fmt.Errorf("task %d not found", taskID)
	}

	cloneT := proto.Clone(t).(*indexpb.StatsTask)
	cloneT.CanRecycle = true

	if err := stm.catalog.SaveStatsTask(stm.ctx, cloneT); err != nil {
		log.Warn("mark stats task can recycle failed",
			zap.Int64("taskID", taskID),
			zap.Int64("segmentID", t.GetSegmentID()),
			zap.Error(err))
		return err
	}

	stm.tasks[t.TaskID] = cloneT
	stm.updateMetrics()

	log.Info("mark stats task can recycle success", zap.Int64("taskID", taskID),
		zap.Int64("segmentID", t.SegmentID),
		zap.String("subJobType", t.GetSubJobType().String()))
	return nil
}
