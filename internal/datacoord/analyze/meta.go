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

package analyze

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

type AnalyzeMeta struct {
	sync.RWMutex

	Ctx     context.Context
	Catalog metastore.DataCoordCatalog

	// taskID -> analyzeStats
	// TODO: when to mark as dropped?
	Tasks map[int64]*indexpb.AnalyzeTask
}

func NewAnalyzeMeta(ctx context.Context, catalog metastore.DataCoordCatalog) (*AnalyzeMeta, error) {
	mt := &AnalyzeMeta{
		Ctx:     ctx,
		Catalog: catalog,
		Tasks:   make(map[int64]*indexpb.AnalyzeTask),
	}

	if err := mt.reloadFromKV(); err != nil {
		return nil, err
	}
	return mt, nil
}

func (m *AnalyzeMeta) reloadFromKV() error {
	record := timerecord.NewTimeRecorder("analyzeMeta-reloadFromKV")

	// load analyze stats
	analyzeTasks, err := m.Catalog.ListAnalyzeTasks(m.Ctx)
	if err != nil {
		mlog.Warn(m.Ctx, "analyzeMeta reloadFromKV load analyze tasks failed", mlog.Err(err))
		return err
	}

	for _, analyzeTask := range analyzeTasks {
		m.Tasks[analyzeTask.TaskID] = analyzeTask
	}
	mlog.Info(m.Ctx, "analyzeMeta reloadFromKV done", mlog.Duration("duration", record.ElapseSpan()))
	return nil
}

func (m *AnalyzeMeta) saveTask(newTask *indexpb.AnalyzeTask) error {
	if err := m.Catalog.SaveAnalyzeTask(m.Ctx, newTask); err != nil {
		return err
	}
	m.Tasks[newTask.TaskID] = newTask
	return nil
}

func (m *AnalyzeMeta) GetTask(taskID int64) *indexpb.AnalyzeTask {
	m.RLock()
	defer m.RUnlock()

	return m.Tasks[taskID]
}

func (m *AnalyzeMeta) AddAnalyzeTask(task *indexpb.AnalyzeTask) error {
	m.Lock()
	defer m.Unlock()

	mlog.Info(m.Ctx, "add analyze task", mlog.FieldTaskID(task.TaskID),
		mlog.FieldCollectionID(task.CollectionID), mlog.FieldPartitionID(task.PartitionID))
	return m.saveTask(task)
}

// dropTaskFromMemoryLocked removes taskID from in-memory state only, issuing
// no catalog write. The caller must hold m's write lock. It is used by
// callers (e.g. partition-stats-and-analyze-task cleanup) that persist the
// drop via a composite catalog.Update and must not mutate memory until that
// write has already succeeded.
func (m *AnalyzeMeta) DropTaskFromMemoryLocked(taskID int64) {
	delete(m.Tasks, taskID)
}

func (m *AnalyzeMeta) UpdateVersion(taskID int64, nodeID int64) error {
	m.Lock()
	defer m.Unlock()

	t, ok := m.Tasks[taskID]
	if !ok {
		return merr.WrapErrServiceInternalMsg("there is no task with taskID: %d", taskID)
	}

	cloneT := proto.Clone(t).(*indexpb.AnalyzeTask)
	cloneT.Version++
	cloneT.NodeID = nodeID
	mlog.Info(m.Ctx, "update task version", mlog.FieldTaskID(taskID), mlog.Int64("newVersion", cloneT.Version),
		mlog.FieldNodeID(nodeID))
	return m.saveTask(cloneT)
}

func (m *AnalyzeMeta) BuildingTask(taskID int64) error {
	m.Lock()
	defer m.Unlock()

	t, ok := m.Tasks[taskID]
	if !ok {
		return merr.WrapErrServiceInternalMsg("there is no task with taskID: %d", taskID)
	}

	cloneT := proto.Clone(t).(*indexpb.AnalyzeTask)
	cloneT.State = indexpb.JobState_JobStateInProgress
	mlog.Info(m.Ctx, "task will be building", mlog.FieldTaskID(taskID))

	return m.saveTask(cloneT)
}

func (m *AnalyzeMeta) UpdateState(taskID int64, state indexpb.JobState, failReason string) error {
	m.Lock()
	defer m.Unlock()

	t, ok := m.Tasks[taskID]
	if !ok {
		return merr.WrapErrServiceInternalMsg("there is no task with taskID: %d", taskID)
	}

	cloneT := proto.Clone(t).(*indexpb.AnalyzeTask)
	cloneT.State = state
	cloneT.FailReason = failReason
	mlog.Info(m.Ctx, "update analyze task state", mlog.FieldTaskID(taskID), mlog.String("state", state.String()),
		mlog.String("failReason", failReason))

	return m.saveTask(cloneT)
}

func (m *AnalyzeMeta) FinishTask(taskID int64, result *workerpb.AnalyzeResult) error {
	m.Lock()
	defer m.Unlock()

	t, ok := m.Tasks[taskID]
	if !ok {
		return merr.WrapErrServiceInternalMsg("there is no task with taskID: %d", taskID)
	}

	mlog.Info(m.Ctx, "finish task meta...", mlog.FieldTaskID(taskID), mlog.String("state", result.GetState().String()),
		mlog.String("failReason", result.GetFailReason()))

	cloneT := proto.Clone(t).(*indexpb.AnalyzeTask)
	cloneT.State = result.GetState()
	cloneT.FailReason = result.GetFailReason()
	cloneT.CentroidsFile = result.GetCentroidsFile()
	return m.saveTask(cloneT)
}

func (m *AnalyzeMeta) GetAllTasks() map[int64]*indexpb.AnalyzeTask {
	m.RLock()
	defer m.RUnlock()

	return m.Tasks
}

func (m *AnalyzeMeta) CheckCleanAnalyzeTask(taskID UniqueID) (bool, *indexpb.AnalyzeTask) {
	m.RLock()
	defer m.RUnlock()

	if t, ok := m.Tasks[taskID]; ok {
		if t.State == indexpb.JobState_JobStateFinished {
			return true, t
		}
		return false, t
	}
	return true, nil
}
