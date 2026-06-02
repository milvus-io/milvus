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
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
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
		mlog.Warn(m.ctx, "analyzeMeta reloadFromKV load analyze tasks failed", mlog.Err(err))
		return err
	}

	for _, analyzeTask := range analyzeTasks {
		m.tasks[analyzeTask.TaskID] = analyzeTask
	}
	mlog.Info(m.ctx, "analyzeMeta reloadFromKV done", mlog.Duration("duration", record.ElapseSpan()))
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

	mlog.Info(m.ctx, "add analyze task", mlog.FieldTaskID(task.TaskID),
		mlog.FieldCollectionID(task.CollectionID), mlog.FieldPartitionID(task.PartitionID))
	return m.saveTask(task)
}

func (m *analyzeMeta) DropAnalyzeTask(ctx context.Context, taskID int64) error {
	m.Lock()
	defer m.Unlock()

	mlog.Info(ctx, "drop analyze task", mlog.FieldTaskID(taskID))
	if err := m.catalog.DropAnalyzeTask(ctx, taskID); err != nil {
		mlog.Warn(ctx, "drop analyze task by catalog failed", mlog.FieldTaskID(taskID),
			mlog.Err(err))
		return err
	}

	delete(m.tasks, taskID)
	return nil
}

func (m *analyzeMeta) UpdateVersion(taskID int64, nodeID int64) error {
	m.Lock()
	defer m.Unlock()

	t, ok := m.tasks[taskID]
	if !ok {
		return merr.WrapErrServiceInternalMsg("there is no task with taskID: %d", taskID)
	}

	cloneT := proto.Clone(t).(*indexpb.AnalyzeTask)
	cloneT.Version++
	cloneT.NodeID = nodeID
	mlog.Info(m.ctx, "update task version", mlog.FieldTaskID(taskID), mlog.Int64("newVersion", cloneT.Version),
		mlog.FieldNodeID(nodeID))
	return m.saveTask(cloneT)
}

func (m *analyzeMeta) BuildingTask(taskID int64) error {
	m.Lock()
	defer m.Unlock()

	t, ok := m.tasks[taskID]
	if !ok {
		return merr.WrapErrServiceInternalMsg("there is no task with taskID: %d", taskID)
	}

	cloneT := proto.Clone(t).(*indexpb.AnalyzeTask)
	cloneT.State = indexpb.JobState_JobStateInProgress
	mlog.Info(m.ctx, "task will be building", mlog.FieldTaskID(taskID))

	return m.saveTask(cloneT)
}

func (m *analyzeMeta) UpdateState(taskID int64, state indexpb.JobState, failReason string) error {
	m.Lock()
	defer m.Unlock()

	t, ok := m.tasks[taskID]
	if !ok {
		return merr.WrapErrServiceInternalMsg("there is no task with taskID: %d", taskID)
	}

	cloneT := proto.Clone(t).(*indexpb.AnalyzeTask)
	cloneT.State = state
	cloneT.FailReason = failReason
	mlog.Info(m.ctx, "update analyze task state", mlog.FieldTaskID(taskID), mlog.String("state", state.String()),
		mlog.String("failReason", failReason))

	return m.saveTask(cloneT)
}

func (m *analyzeMeta) FinishTask(taskID int64, result *workerpb.AnalyzeResult) error {
	m.Lock()
	defer m.Unlock()

	t, ok := m.tasks[taskID]
	if !ok {
		return merr.WrapErrServiceInternalMsg("there is no task with taskID: %d", taskID)
	}

	mlog.Info(m.ctx, "finish task meta...", mlog.FieldTaskID(taskID), mlog.String("state", result.GetState().String()),
		mlog.String("failReason", result.GetFailReason()))

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
