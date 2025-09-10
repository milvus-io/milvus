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
	"sync"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
)

type globalStatsMeta struct {
	sync.RWMutex

	ctx     context.Context
	catalog metastore.DataCoordCatalog

	tasks map[int64]*datapb.GlobalStatsTask
}

func newGlobalStatsMeta(ctx context.Context, catalog metastore.DataCoordCatalog) (*globalStatsMeta, error) {
	mt := &globalStatsMeta{
		ctx:     ctx,
		catalog: catalog,
		tasks:   make(map[int64]*datapb.GlobalStatsTask),
	}

	if err := mt.reloadFromKV(); err != nil {
		return nil, err
	}
	return mt, nil
}

func (m *globalStatsMeta) reloadFromKV() error {
	record := timerecord.NewTimeRecorder("globalStatsMeta-reloadFromKV")

	globalStatsTasks, err := m.catalog.ListGlobalStatsTask(m.ctx)
	if err != nil {
		log.Warn("globalStatsMeta reloadFromKV load global stats tasks failed", zap.Error(err))
		return err
	}

	for _, globalStatsTask := range globalStatsTasks {
		m.tasks[globalStatsTask.TaskID] = globalStatsTask
	}
	log.Info("globalStatsMeta reloadFromKV done", zap.Duration("duration", record.ElapseSpan()))
	return nil
}

func (m *globalStatsMeta) saveTask(newTask *datapb.GlobalStatsTask) error {
	if err := m.catalog.SaveGlobalStatsTask(m.ctx, newTask); err != nil {
		return err
	}
	m.tasks[newTask.TaskID] = newTask
	return nil
}

func (m *globalStatsMeta) GetTask(taskID int64) *datapb.GlobalStatsTask {
	m.RLock()
	defer m.RUnlock()

	return m.tasks[taskID]
}

func (m *globalStatsMeta) AddGlobalStatsTask(task *datapb.GlobalStatsTask) error {
	m.Lock()
	defer m.Unlock()

	log.Info("add global stats task", zap.Int64("taskID", task.TaskID))
	return m.saveTask(task)
}

func (m *globalStatsMeta) DropGlobalStatsTask(ctx context.Context, taskID int64) error {
	m.Lock()
	defer m.Unlock()

	log.Info("drop global stats task", zap.Int64("taskID", taskID))
	if err := m.catalog.DropGlobalStatsTask(ctx, taskID); err != nil {
		log.Warn("drop global stats task by catalog failed", zap.Int64("taskID", taskID),
			zap.Error(err))
		return err
	}

	delete(m.tasks, taskID)
	return nil
}

func (m *globalStatsMeta) UpdateVersion(taskID int64, nodeID int64) error {
	m.Lock()
	defer m.Unlock()

	t, ok := m.tasks[taskID]
	if !ok {
		return fmt.Errorf("there is no task with taskID: %d", taskID)
	}

	cloneT := proto.Clone(t).(*datapb.GlobalStatsTask)
	cloneT.Version++
	cloneT.NodeID = nodeID
	log.Info("update task version", zap.Int64("taskID", taskID), zap.Int64("newVersion", cloneT.Version),
		zap.Int64("nodeID", nodeID))
	return m.saveTask(cloneT)
}

func (m *globalStatsMeta) UpdateState(taskID int64, state indexpb.JobState, failReason string) error {
	m.Lock()
	defer m.Unlock()

	t, ok := m.tasks[taskID]
	if !ok {
		return fmt.Errorf("there is no task with taskID: %d", taskID)
	}

	cloneT := proto.Clone(t).(*datapb.GlobalStatsTask)
	cloneT.State = state
	cloneT.FailReason = failReason
	log.Info("update global stats task state", zap.Int64("taskID", taskID), zap.String("state", state.String()),
		zap.String("failReason", failReason))

	return m.saveTask(cloneT)
}

func (m *globalStatsMeta) FinishTask(taskID int64, result *workerpb.AnalyzeResult) error {
	m.Lock()
	defer m.Unlock()

	t, ok := m.tasks[taskID]
	if !ok {
		return fmt.Errorf("there is no task with taskID: %d", taskID)
	}

	log.Info("finish task meta...", zap.Int64("taskID", taskID), zap.String("state", result.GetState().String()),
		zap.String("failReason", result.GetFailReason()))

	cloneT := proto.Clone(t).(*datapb.GlobalStatsTask)
	cloneT.State = result.GetState()
	cloneT.FailReason = result.GetFailReason()
	return m.saveTask(cloneT)
}

func (m *globalStatsMeta) GetAllTasks() map[int64]*datapb.GlobalStatsTask {
	m.RLock()
	defer m.RUnlock()

	return m.tasks
}

func (m *globalStatsMeta) CanCleanedTasks() []int64 {
	needCleanedTaskIDs := make([]int64, 0)
	for _, t := range m.tasks {
		if t.GetState() == indexpb.JobState_JobStateFinished ||
			t.GetState() == indexpb.JobState_JobStateFailed {
			needCleanedTaskIDs = append(needCleanedTaskIDs, t.GetTaskID())
		}
	}
	return needCleanedTaskIDs
}

func (m *globalStatsMeta) CheckCleanGlobalStatsTask(taskID UniqueID) (bool, *datapb.GlobalStatsTask) {
	m.RLock()
	defer m.RUnlock()

	if t, ok := m.tasks[taskID]; ok {
		if t.State == indexpb.JobState_JobStateFinished {
			return true, t
		}
		return false, t
	}
	return true, nil
}

func (m *globalStatsMeta) updateMetrics() {
	taskMetrics := make(map[indexpb.JobState]int)
	taskMetrics[indexpb.JobState_JobStateNone] = 0
	taskMetrics[indexpb.JobState_JobStateInit] = 0
	taskMetrics[indexpb.JobState_JobStateInProgress] = 0
	taskMetrics[indexpb.JobState_JobStateFinished] = 0
	taskMetrics[indexpb.JobState_JobStateFailed] = 0
	taskMetrics[indexpb.JobState_JobStateRetry] = 0
	for _, t := range m.tasks {
		taskMetrics[t.GetState()]++
	}

	jobType := indexpb.JobType_JobTypeStatsJob.String()
	for k, v := range taskMetrics {
		metrics.GlobalStatsTaskNum.WithLabelValues(jobType, k.String()).Set(float64(v))
	}
}
