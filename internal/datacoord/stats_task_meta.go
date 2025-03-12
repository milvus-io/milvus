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
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"strconv"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type statsTaskMeta struct {
	ctx     context.Context
	catalog metastore.DataCoordCatalog

	keyLock *lock.KeyLock[UniqueID]
	// taskID -> statsTask
	tasks *typeutil.ConcurrentMap[UniqueID, *indexpb.StatsTask]

	// segmentID + SubJobType -> statsTask
	segmentID2Tasks *typeutil.ConcurrentMap[string, *indexpb.StatsTask]
}

func newStatsTaskMeta(ctx context.Context, catalog metastore.DataCoordCatalog) (*statsTaskMeta, error) {
	stm := &statsTaskMeta{
		ctx:             ctx,
		catalog:         catalog,
		keyLock:         lock.NewKeyLock[UniqueID](),
		tasks:           typeutil.NewConcurrentMap[UniqueID, *indexpb.StatsTask](),
		segmentID2Tasks: typeutil.NewConcurrentMap[string, *indexpb.StatsTask](),
	}
	if err := stm.reloadFromKV(); err != nil {
		return nil, err
	}
	return stm, nil
}

func createSecondaryIndexKey(segmentID UniqueID, subJobType string) string {
	return strconv.FormatUint(uint64(segmentID), 10) + "-" + subJobType
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
		stm.tasks.Insert(t.GetTaskID(), t)

		secondaryKey := createSecondaryIndexKey(t.GetSegmentID(), t.GetSubJobType().String())
		stm.segmentID2Tasks.Insert(secondaryKey, t)
	}

	log.Info("statsTaskMeta reloadFromKV done", zap.Duration("duration", record.ElapseSpan()))
	return nil
}

func (stm *statsTaskMeta) updateMetrics() {
	taskMetrics := make(map[indexpb.JobState]int)
	taskMetrics[indexpb.JobState_JobStateNone] = 0
	taskMetrics[indexpb.JobState_JobStateInit] = 0
	taskMetrics[indexpb.JobState_JobStateInProgress] = 0
	taskMetrics[indexpb.JobState_JobStateFinished] = 0
	taskMetrics[indexpb.JobState_JobStateFailed] = 0
	taskMetrics[indexpb.JobState_JobStateRetry] = 0
	allTasks := stm.tasks.Values()
	for _, t := range allTasks {
		taskMetrics[t.GetState()]++
	}

	jobType := indexpb.JobType_JobTypeStatsJob.String()
	for k, v := range taskMetrics {
		metrics.TaskNum.WithLabelValues(jobType, k.String()).Set(float64(v))
	}
}

func (stm *statsTaskMeta) AddStatsTask(t *indexpb.StatsTask) error {
	taskID := t.GetTaskID()

	secondaryKey := createSecondaryIndexKey(t.GetSegmentID(), t.GetSubJobType().String())
	task, alreadyExist := stm.segmentID2Tasks.Get(secondaryKey)
	if alreadyExist {
		msg := fmt.Sprintf("stats task already exist in meta of segment %d with subJobType: %s",
			t.GetSegmentID(), t.GetSubJobType().String())
		log.RatedWarn(10, msg, zap.Int64("taskID", t.GetTaskID()), zap.Int64("exist taskID", task.GetTaskID()))
		return merr.WrapErrTaskDuplicate(indexpb.JobType_JobTypeStatsJob.String(), msg)
	}

	stm.keyLock.Lock(taskID)
	defer stm.keyLock.Unlock(taskID)

	log.Info("add stats task", zap.Int64("taskID", t.GetTaskID()), zap.Int64("originSegmentID", t.GetSegmentID()),
		zap.Int64("targetSegmentID", t.GetTargetSegmentID()), zap.String("subJobType", t.GetSubJobType().String()))
	t.State = indexpb.JobState_JobStateInit

	if err := stm.catalog.SaveStatsTask(stm.ctx, t); err != nil {
		log.Warn("adding stats task failed",
			zap.Int64("taskID", taskID),
			zap.Int64("segmentID", t.GetSegmentID()),
			zap.String("subJobType", t.GetSubJobType().String()),
			zap.Error(err))
		return err
	}

	stm.tasks.Insert(taskID, t)
	stm.segmentID2Tasks.Insert(secondaryKey, t)

	log.Info("add stats task success", zap.Int64("taskID", t.GetTaskID()), zap.Int64("originSegmentID", t.GetSegmentID()),
		zap.Int64("targetSegmentID", t.GetTargetSegmentID()), zap.String("subJobType", t.GetSubJobType().String()))
	return nil
}

func (stm *statsTaskMeta) DropStatsTask(taskID int64) error {
	stm.keyLock.Lock(taskID)
	defer stm.keyLock.Unlock(taskID)

	log.Info("drop stats task by taskID", zap.Int64("taskID", taskID))

	t, ok := stm.tasks.Get(taskID)
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

	stm.tasks.Remove(taskID)
	secondaryKey := createSecondaryIndexKey(t.GetSegmentID(), t.GetSubJobType().String())
	stm.segmentID2Tasks.Remove(secondaryKey)

	log.Info("remove stats task success", zap.Int64("taskID", taskID))
	return nil
}

func (stm *statsTaskMeta) UpdateVersion(taskID, nodeID int64) error {
	stm.keyLock.Lock(taskID)
	defer stm.keyLock.Unlock(taskID)

	t, ok := stm.tasks.Get(taskID)
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

	stm.tasks.Insert(taskID, cloneT)
	secondaryKey := createSecondaryIndexKey(t.GetSegmentID(), t.GetSubJobType().String())
	stm.segmentID2Tasks.Insert(secondaryKey, cloneT)
	log.Info("update stats task version success", zap.Int64("taskID", taskID), zap.Int64("nodeID", nodeID),
		zap.Int64("newVersion", cloneT.GetVersion()))
	return nil
}

func (stm *statsTaskMeta) UpdateBuildingTask(taskID int64) error {
	stm.keyLock.Lock(taskID)
	defer stm.keyLock.Unlock(taskID)

	t, ok := stm.tasks.Get(taskID)
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

	stm.tasks.Insert(taskID, cloneT)
	secondaryKey := createSecondaryIndexKey(t.GetSegmentID(), t.GetSubJobType().String())
	stm.segmentID2Tasks.Insert(secondaryKey, cloneT)

	log.Info("update building stats task success", zap.Int64("taskID", taskID))
	return nil
}

func (stm *statsTaskMeta) FinishTask(taskID int64, result *workerpb.StatsResult) error {
	stm.keyLock.Lock(taskID)
	defer stm.keyLock.Unlock(taskID)

	t, ok := stm.tasks.Get(taskID)
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

	stm.tasks.Insert(taskID, cloneT)
	secondaryKey := createSecondaryIndexKey(t.GetSegmentID(), t.GetSubJobType().String())
	stm.segmentID2Tasks.Insert(secondaryKey, cloneT)

	log.Info("finish stats task meta success", zap.Int64("taskID", taskID), zap.Int64("segmentID", t.SegmentID),
		zap.String("state", result.GetState().String()), zap.String("failReason", t.GetFailReason()))
	return nil
}

func (stm *statsTaskMeta) GetStatsTask(taskID int64) *indexpb.StatsTask {
	t, _ := stm.tasks.Get(taskID)
	return t
}

func (stm *statsTaskMeta) GetStatsTaskState(taskID int64) indexpb.JobState {
	t, ok := stm.tasks.Get(taskID)
	if !ok {
		return indexpb.JobState_JobStateNone
	}
	return t.GetState()
}

func (stm *statsTaskMeta) GetStatsTaskStateBySegmentID(segmentID int64, subJobType indexpb.StatsSubJob) indexpb.JobState {
	state := indexpb.JobState_JobStateNone

	secondaryKey := createSecondaryIndexKey(segmentID, subJobType.String())
	t, exists := stm.segmentID2Tasks.Get(secondaryKey)
	if exists {
		state = t.GetState()
	}
	return state
}

func (stm *statsTaskMeta) CanCleanedTasks() []int64 {
	needCleanedTaskIDs := make([]int64, 0)
	stm.tasks.Range(func(key UniqueID, value *indexpb.StatsTask) bool {
		if value.GetCanRecycle() && (value.GetState() == indexpb.JobState_JobStateFinished ||
			value.GetState() == indexpb.JobState_JobStateFailed) {
			needCleanedTaskIDs = append(needCleanedTaskIDs, key)
		}
		return true
	})
	return needCleanedTaskIDs
}

func (stm *statsTaskMeta) GetAllTasks() map[int64]*indexpb.StatsTask {
	tasks := make(map[int64]*indexpb.StatsTask)

	allTasks := stm.tasks.Values()
	for _, v := range allTasks {
		tasks[v.GetTaskID()] = proto.Clone(v).(*indexpb.StatsTask)
	}
	return tasks
}

func (stm *statsTaskMeta) GetStatsTaskBySegmentID(segmentID int64, subJobType indexpb.StatsSubJob) *indexpb.StatsTask {
	secondaryKey := createSecondaryIndexKey(segmentID, subJobType.String())
	t, exists := stm.segmentID2Tasks.Get(secondaryKey)
	if exists {
		log.Info("get stats task by segmentID success",
			zap.Int64("taskID", t.GetTaskID()),
			zap.Int64("segmentID", segmentID),
			zap.String("subJobType", subJobType.String()))
		return t
	}

	log.Info("get stats task by segmentID failed, task not exist", zap.Int64("segmentID", segmentID),
		zap.String("subJobType", subJobType.String()))
	return nil
}

func (stm *statsTaskMeta) MarkTaskCanRecycle(taskID int64) error {
	log.Info("mark stats task can recycle", zap.Int64("taskID", taskID))

	t, ok := stm.tasks.Get(taskID)
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

	stm.tasks.Insert(taskID, cloneT)
	secondaryKey := createSecondaryIndexKey(t.GetSegmentID(), t.GetSubJobType().String())
	stm.segmentID2Tasks.Insert(secondaryKey, cloneT)

	log.Info("mark stats task can recycle success", zap.Int64("taskID", taskID),
		zap.Int64("segmentID", t.SegmentID),
		zap.String("subJobType", t.GetSubJobType().String()))
	return nil
}
