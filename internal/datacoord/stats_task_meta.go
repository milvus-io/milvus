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

	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
)

type statsTaskMeta struct {
	sync.RWMutex

	ctx     context.Context
	catalog metastore.DataCoordCatalog

	// taskID -> analyzeStats
	// TODO: when to mark as dropped?
	tasks map[int64]*indexpb.StatsTask
}

func newStatsTaskMeta(ctx context.Context, catalog metastore.DataCoordCatalog) *statsTaskMeta {
	return &statsTaskMeta{
		ctx:     ctx,
		catalog: catalog,
		tasks:   make(map[int64]*indexpb.StatsTask),
	}
}

func (stm *statsTaskMeta) saveTask(newTask *indexpb.StatsTask) error {
	if err := stm.catalog.SaveStatsTask(stm.ctx, newTask); err != nil {
		return err
	}

	stm.tasks[newTask.TaskID] = newTask
	stm.updateMetrics()
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

	log.Info("add stats task", zap.Int64("taskID", t.GetTaskID()), zap.Int64("segmentID", t.GetSegmentID()))
	t.State = indexpb.JobState_JobStateInit
	if err := stm.saveTask(t); err != nil {
		log.Warn("adding stats task failed",
			zap.Int64("taskID", t.GetTaskID()),
			zap.Int64("segmentID", t.GetSegmentID()),
			zap.Error(err))
		return err
	}

	log.Info("add stats task success", zap.Int64("taskID", t.GetTaskID()), zap.Int64("segmentID", t.GetSegmentID()))
	return nil
}

func (stm *statsTaskMeta) RemoveStatsTask(taskID int64) error {
	stm.Lock()
	defer stm.Unlock()

	log.Info("remove stats task", zap.Int64("taskID", taskID), zap.Int64("segmentID", taskID))
	if err := stm.catalog.DropStatsTask(stm.ctx, taskID); err != nil {
		log.Warn("meta update: removing stats task failed",
			zap.Int64("taskID", taskID),
			zap.Int64("segmentID", taskID),
			zap.Error(err))
		return err
	}
	log.Info("remove stats task success", zap.Int64("taskID", taskID), zap.Int64("segmentID", taskID))
	return nil
}

func (stm *statsTaskMeta) UpdateVersion(taskID int64) error {
	stm.Lock()
	defer stm.Unlock()

	t, ok := stm.tasks[taskID]
	if !ok {
		return fmt.Errorf("task %d not found", taskID)
	}

	cloneT := proto.Clone(t).(*indexpb.StatsTask)
	cloneT.Version++
	log.Info("update version", zap.Int64("taskID", taskID), zap.Int64("newVersion", cloneT.GetVersion()))
	return stm.saveTask(cloneT)
}

func (stm *statsTaskMeta) UpdateBuildingTask(taskID, nodeID int64) error {
	stm.Lock()
	defer stm.Unlock()

	t, ok := stm.tasks[taskID]
	if !ok {
		return fmt.Errorf("task %d not found", taskID)
	}

	cloneT := proto.Clone(t).(*indexpb.StatsTask)
	cloneT.NodeID = nodeID
	cloneT.State = indexpb.JobState_JobStateInProgress
	log.Info("update building stats task", zap.Int64("taskID", taskID), zap.Int64("nodeID", nodeID))
	return stm.saveTask(cloneT)
}

func (stm *statsTaskMeta) FinishTask(taskID int64, result *workerpb.StatsResult) error {
	stm.Lock()
	defer stm.Unlock()

	t, ok := stm.tasks[taskID]
	if !ok {
		return fmt.Errorf("task %d not found", taskID)
	}

	log.Info("finish task meta", zap.Int64("taskID", taskID), zap.Int64("segmentID", t.SegmentID),
		zap.String("state", result.GetState().String()), zap.String("failReason", t.GetFailReason()))
	cloneT := proto.Clone(t).(*indexpb.StatsTask)
	cloneT.State = result.GetState()
	cloneT.FailReason = result.GetFailReason()
	return stm.saveTask(cloneT)
}
