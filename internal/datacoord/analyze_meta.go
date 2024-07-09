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
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

type analyzeMeta struct {
	sync.RWMutex

	ctx     context.Context
	catalog metastore.DataCoordCatalog

	// taskID -> analyzeStats
	// TODO: when to mark as dropped?
	tasks map[int64]*indexpb.AnalyzeTask
}

func newAnalyzeMeta(ctx context.Context, catalog metastore.DataCoordCatalog) (*analyzeMeta, error) {
	mt := &analyzeMeta{
		ctx:     ctx,
		catalog: catalog,
		tasks:   make(map[int64]*indexpb.AnalyzeTask),
	}

	if err := mt.reloadFromKV(); err != nil {
		return nil, err
	}
	return mt, nil
}

func (m *analyzeMeta) reloadFromKV() error {
	record := timerecord.NewTimeRecorder("analyzeMeta-reloadFromKV")

	// load analyze stats
	analyzeTasks, err := m.catalog.ListAnalyzeTasks(m.ctx)
	if err != nil {
		log.Warn("analyzeMeta reloadFromKV load analyze tasks failed", zap.Error(err))
		return err
	}

	for _, analyzeTask := range analyzeTasks {
		m.tasks[analyzeTask.TaskID] = analyzeTask
	}
	log.Info("analyzeMeta reloadFromKV done", zap.Duration("duration", record.ElapseSpan()))
	return nil
}

func (m *analyzeMeta) saveTask(newTask *indexpb.AnalyzeTask) error {
	if err := m.catalog.SaveAnalyzeTask(m.ctx, newTask); err != nil {
		return err
	}
	m.tasks[newTask.TaskID] = newTask
	return nil
}

func (m *analyzeMeta) GetTask(taskID int64) *indexpb.AnalyzeTask {
	m.RLock()
	defer m.RUnlock()

	return m.tasks[taskID]
}

func (m *analyzeMeta) AddAnalyzeTask(task *indexpb.AnalyzeTask) error {
	m.Lock()
	defer m.Unlock()

	log.Info("add analyze task", zap.Int64("taskID", task.TaskID),
		zap.Int64("collectionID", task.CollectionID), zap.Int64("partitionID", task.PartitionID))
	return m.saveTask(task)
}

func (m *analyzeMeta) DropAnalyzeTask(taskID int64) error {
	m.Lock()
	defer m.Unlock()

	log.Info("drop analyze task", zap.Int64("taskID", taskID))
	if err := m.catalog.DropAnalyzeTask(m.ctx, taskID); err != nil {
		log.Warn("drop analyze task by catalog failed", zap.Int64("taskID", taskID),
			zap.Error(err))
		return err
	}

	delete(m.tasks, taskID)
	return nil
}

func (m *analyzeMeta) UpdateVersion(taskID int64) error {
	m.Lock()
	defer m.Unlock()

	t, ok := m.tasks[taskID]
	if !ok {
		return fmt.Errorf("there is no task with taskID: %d", taskID)
	}

	cloneT := proto.Clone(t).(*indexpb.AnalyzeTask)
	cloneT.Version++
	log.Info("update task version", zap.Int64("taskID", taskID), zap.Int64("newVersion", cloneT.Version))
	return m.saveTask(cloneT)
}

func (m *analyzeMeta) BuildingTask(taskID, nodeID int64) error {
	m.Lock()
	defer m.Unlock()

	t, ok := m.tasks[taskID]
	if !ok {
		return fmt.Errorf("there is no task with taskID: %d", taskID)
	}

	cloneT := proto.Clone(t).(*indexpb.AnalyzeTask)
	cloneT.NodeID = nodeID
	cloneT.State = indexpb.JobState_JobStateInProgress
	log.Info("task will be building", zap.Int64("taskID", taskID), zap.Int64("nodeID", nodeID))

	return m.saveTask(cloneT)
}

func (m *analyzeMeta) FinishTask(taskID int64, result *workerpb.AnalyzeResult) error {
	m.Lock()
	defer m.Unlock()

	t, ok := m.tasks[taskID]
	if !ok {
		return fmt.Errorf("there is no task with taskID: %d", taskID)
	}

	log.Info("finish task meta...", zap.Int64("taskID", taskID), zap.String("state", result.GetState().String()),
		zap.String("failReason", result.GetFailReason()))

	cloneT := proto.Clone(t).(*indexpb.AnalyzeTask)
	cloneT.State = result.GetState()
	cloneT.FailReason = result.GetFailReason()
	cloneT.CentroidsFile = result.GetCentroidsFile()
	return m.saveTask(cloneT)
}

func (m *analyzeMeta) GetAllTasks() map[int64]*indexpb.AnalyzeTask {
	m.RLock()
	defer m.RUnlock()

	return m.tasks
}

func (m *analyzeMeta) CheckCleanAnalyzeTask(taskID UniqueID) (bool, *indexpb.AnalyzeTask) {
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
