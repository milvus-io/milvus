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

package bulkimport

import (
	"context"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/samber/lo"
	"golang.org/x/exp/maps"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

type ImportMeta interface {
	AddJob(ctx context.Context, job ImportJob) error
	UpdateJob(ctx context.Context, jobID int64, actions ...UpdateJobAction) error
	GetJob(ctx context.Context, jobID int64) ImportJob
	GetJobBy(ctx context.Context, filters ...ImportJobFilter) []ImportJob
	CountJobBy(ctx context.Context, filters ...ImportJobFilter) int
	RemoveJob(ctx context.Context, jobID int64) error
	HandleCommitVchannel(ctx context.Context, jobID int64, vchannel string, callback func() error) error

	AddTask(ctx context.Context, task ImportTask) error
	UpdateTask(ctx context.Context, taskID int64, actions ...UpdateAction) error
	GetTask(ctx context.Context, taskID int64) ImportTask
	GetTaskBy(ctx context.Context, filters ...ImportTaskFilter) []ImportTask
	GetTaskByJob(ctx context.Context, jobID int64, filters ...ImportTaskFilter) []ImportTask
	RemoveTask(ctx context.Context, taskID int64) error
	TaskStatsJSON(ctx context.Context) string
}

type importTasks struct {
	tasks          map[int64]ImportTask
	taskIDsByJobID map[int64]map[int64]struct{}
	taskStats      *expirable.LRU[int64, ImportTask]
}

func newImportTasks() *importTasks {
	return &importTasks{
		tasks:          make(map[int64]ImportTask),
		taskIDsByJobID: make(map[int64]map[int64]struct{}),
		taskStats:      expirable.NewLRU[UniqueID, ImportTask](512, nil, time.Minute*30),
	}
}

func (t *importTasks) get(taskID int64) ImportTask {
	ret, ok := t.tasks[taskID]
	if !ok {
		return nil
	}
	return ret
}

func (t *importTasks) add(task ImportTask) {
	taskID := task.GetTaskID()
	jobID := task.GetJobID()
	if oldTask, ok := t.tasks[taskID]; ok && oldTask.GetJobID() != jobID {
		t.removeFromJob(oldTask.GetJobID(), taskID)
	}
	t.tasks[taskID] = task
	if _, ok := t.taskIDsByJobID[jobID]; !ok {
		t.taskIDsByJobID[jobID] = make(map[int64]struct{})
	}
	t.taskIDsByJobID[jobID][taskID] = struct{}{}
	t.taskStats.Add(taskID, task)
}

func (t *importTasks) remove(taskID int64) {
	task, ok := t.tasks[taskID]
	if ok {
		delete(t.tasks, taskID)
		t.removeFromJob(task.GetJobID(), taskID)
		t.taskStats.Add(task.GetTaskID(), task)
	}
}

func (t *importTasks) removeFromJob(jobID, taskID int64) {
	taskIDs := t.taskIDsByJobID[jobID]
	delete(taskIDs, taskID)
	if len(taskIDs) == 0 {
		delete(t.taskIDsByJobID, jobID)
	}
}

func (t *importTasks) listTasks() []ImportTask {
	return maps.Values(t.tasks)
}

func (t *importTasks) listTasksByJob(jobID int64) []ImportTask {
	taskIDs := t.taskIDsByJobID[jobID]
	tasks := make([]ImportTask, 0, len(taskIDs))
	for taskID := range taskIDs {
		if task, ok := t.tasks[taskID]; ok {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

func (t *importTasks) listTaskStats() []ImportTask {
	return t.taskStats.Values()
}

type Meta struct {
	Mu      lock.RWMutex // guards jobs and tasks
	Jobs    map[int64]ImportJob
	tasks   *importTasks
	Catalog metastore.DataCoordCatalog
}

func normalizeImportVersion(jobVersion internalpb.ImportVersion) (internalpb.ImportVersion, error) {
	switch jobVersion {
	case internalpb.ImportVersion_ImportVersionUnspecified:
		return internalpb.ImportVersion_ImportVersionV2, nil
	case internalpb.ImportVersion_ImportVersionV2, internalpb.ImportVersion_ImportVersionV3:
		return jobVersion, nil
	default:
		return internalpb.ImportVersion_ImportVersionUnspecified, merr.WrapErrDataIntegrityMsg(
			"unsupported import job version %d; supported versions are %d and %d",
			jobVersion,
			internalpb.ImportVersion_ImportVersionV2,
			internalpb.ImportVersion_ImportVersionV3)
	}
}

func NewImportMeta(ctx context.Context, catalog metastore.DataCoordCatalog, alloc allocator.Allocator, mt *meta.Meta) (ImportMeta, error) {
	tasks := newImportTasks()

	importMeta := &Meta{}
	restoredReshardTasks, err := catalog.ListReshardTasks(ctx)
	if err != nil {
		return nil, err
	}
	for _, task := range restoredReshardTasks {
		t := newReshardTask(task, importMeta, mt, alloc)
		tasks.add(t)
	}
	restoredImportTasksV3, err := catalog.ListImportTasksV3(ctx)
	if err != nil {
		return nil, err
	}
	for _, task := range restoredImportTasksV3 {
		t := NewImportTaskV3(task, importMeta, mt, alloc)
		tasks.add(t)
	}
	restoredPreImportTaskV3s, err := catalog.ListPreImportTasksV3(ctx)
	if err != nil {
		return nil, err
	}
	for _, task := range restoredPreImportTaskV3s {
		t := newPreImportTaskV3(task, importMeta)
		tasks.add(t)
	}

	restoredPreImportTasks, err := catalog.ListPreImportTasks(ctx)
	if err != nil {
		return nil, err
	}
	restoredImportTasks, err := catalog.ListImportTasks(ctx)
	if err != nil {
		return nil, err
	}
	restoredJobs, err := catalog.ListImportJobs(ctx)
	if err != nil {
		return nil, err
	}

	for _, task := range restoredPreImportTasks {
		t := &PreImportTask{
			importMeta: importMeta,
			Tr:         newTaskRecorder("preimport task", task.GetCreatedTime()),
			times:      taskcommon.NewTimes(),
		}
		t.Task.Store(task)
		tasks.add(t)
	}
	for _, task := range restoredImportTasks {
		t := &Task{
			alloc:      alloc,
			meta:       mt,
			importMeta: importMeta,
			Tr:         newTaskRecorder("import task", task.GetCreatedTime()),
			times:      taskcommon.NewTimes(),
		}
		t.Task.Store(task)
		tasks.add(t)
	}

	jobs := make(map[int64]ImportJob)
	for _, job := range restoredJobs {
		jobVersion, err := normalizeImportVersion(job.GetVersion())
		if err != nil {
			return nil, err
		}
		job.Version = jobVersion
		jobs[job.GetJobID()] = &Job{
			ImportJob: job,
			Tr:        timerecord.NewTimeRecorder("import job"),
		}
	}

	importMeta.Jobs = jobs
	importMeta.tasks = tasks
	importMeta.Catalog = catalog
	return importMeta, nil
}

func (m *Meta) AddJob(ctx context.Context, job ImportJob) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	jobVersion, err := normalizeImportVersion(job.GetVersion())
	if err != nil {
		return err
	}
	job.(*Job).Version = jobVersion
	if originJob := m.Jobs[job.GetJobID()]; originJob != nil {
		merged := originJob.Clone()
		merged.(*Job).ReadyVchannels = lo.Union(originJob.GetReadyVchannels(), job.GetReadyVchannels())
		job = merged
	}
	err = m.Catalog.SaveImportJob(ctx, job.(*Job).ImportJob)
	if err != nil {
		return err
	}
	m.Jobs[job.GetJobID()] = job
	return nil
}

func (m *Meta) UpdateJob(ctx context.Context, jobID int64, actions ...UpdateJobAction) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	if job, ok := m.Jobs[jobID]; ok {
		if job.GetState() == internalpb.ImportJobState_Completed ||
			job.GetState() == internalpb.ImportJobState_Failed {
			// import job is already completed or failed, no need to update
			return nil
		}
		updatedJob := job.Clone()
		for _, action := range actions {
			action(updatedJob)
		}
		err := m.Catalog.SaveImportJob(ctx, updatedJob.(*Job).ImportJob)
		if err != nil {
			return err
		}
		m.Jobs[updatedJob.GetJobID()] = updatedJob
	}
	return nil
}

func (m *Meta) GetJob(ctx context.Context, jobID int64) ImportJob {
	m.Mu.RLock()
	defer m.Mu.RUnlock()
	return m.Jobs[jobID]
}

func (m *Meta) GetJobBy(ctx context.Context, filters ...ImportJobFilter) []ImportJob {
	m.Mu.RLock()
	defer m.Mu.RUnlock()
	return m.getJobBy(filters...)
}

func (m *Meta) getJobBy(filters ...ImportJobFilter) []ImportJob {
	ret := make([]ImportJob, 0)
OUTER:
	for _, job := range m.Jobs {
		for _, f := range filters {
			if !f(job) {
				continue OUTER
			}
		}
		ret = append(ret, job)
	}
	return ret
}

func (m *Meta) CountJobBy(ctx context.Context, filters ...ImportJobFilter) int {
	m.Mu.RLock()
	defer m.Mu.RUnlock()
	return len(m.getJobBy(filters...))
}

func (m *Meta) RemoveJob(ctx context.Context, jobID int64) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	if _, ok := m.Jobs[jobID]; ok {
		err := m.Catalog.DropImportJob(ctx, jobID)
		if err != nil {
			return err
		}
		delete(m.Jobs, jobID)
	}
	return nil
}

func (m *Meta) AddTask(ctx context.Context, task ImportTask) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	switch task.GetType() {
	case PreImportTaskType:
		err := m.Catalog.SavePreImportTask(ctx, task.(*PreImportTask).Task.Load())
		if err != nil {
			return err
		}
		m.tasks.add(task)
	case ImportTaskType:
		err := m.Catalog.SaveImportTask(ctx, task.(*Task).Task.Load())
		if err != nil {
			return err
		}
		m.tasks.add(task)
	case ReshardTaskType:
		if err := m.Catalog.SaveReshardTask(ctx, task.(*reshardTask).task.Load()); err != nil {
			return err
		}
		m.tasks.add(task)
	case ImportTaskV3Type:
		if err := m.Catalog.SaveImportTaskV3(ctx, task.(*importTaskV3).task.Load()); err != nil {
			return err
		}
		m.tasks.add(task)
	case PreImportTaskV3Type:
		if err := m.Catalog.SavePreImportTaskV3(ctx, task.(*preImportV3Task).task.Load()); err != nil {
			return err
		}
		m.tasks.add(task)
	}
	return nil
}

func (m *Meta) UpdateTask(ctx context.Context, taskID int64, actions ...UpdateAction) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	if task := m.tasks.get(taskID); task != nil {
		updatedTask := task.Clone()
		for _, action := range actions {
			action(updatedTask)
		}
		switch updatedTask.GetType() {
		case PreImportTaskType:
			err := m.Catalog.SavePreImportTask(ctx, updatedTask.(*PreImportTask).Task.Load())
			if err != nil {
				return err
			}
			// update memory task
			task.(*PreImportTask).Task.Store(updatedTask.(*PreImportTask).Task.Load())
		case ImportTaskType:
			err := m.Catalog.SaveImportTask(ctx, updatedTask.(*Task).Task.Load())
			if err != nil {
				return err
			}
			// update memory task
			task.(*Task).Task.Store(updatedTask.(*Task).Task.Load())
		case ReshardTaskType:
			if err := m.Catalog.SaveReshardTask(ctx, updatedTask.(*reshardTask).task.Load()); err != nil {
				return err
			}
			task.(*reshardTask).task.Store(updatedTask.(*reshardTask).task.Load())
		case ImportTaskV3Type:
			if err := m.Catalog.SaveImportTaskV3(ctx, updatedTask.(*importTaskV3).task.Load()); err != nil {
				return err
			}
			task.(*importTaskV3).task.Store(updatedTask.(*importTaskV3).task.Load())
		case PreImportTaskV3Type:
			if err := m.Catalog.SavePreImportTaskV3(ctx, updatedTask.(*preImportV3Task).task.Load()); err != nil {
				return err
			}
			task.(*preImportV3Task).task.Store(updatedTask.(*preImportV3Task).task.Load())
		}
	}

	return nil
}

func (m *Meta) GetTask(ctx context.Context, taskID int64) ImportTask {
	m.Mu.RLock()
	defer m.Mu.RUnlock()
	return m.tasks.get(taskID)
}

func (m *Meta) GetTaskBy(ctx context.Context, filters ...ImportTaskFilter) []ImportTask {
	m.Mu.RLock()
	defer m.Mu.RUnlock()
	return filterImportTasks(m.tasks.listTasks(), filters...)
}

func (m *Meta) GetTaskByJob(ctx context.Context, jobID int64, filters ...ImportTaskFilter) []ImportTask {
	m.Mu.RLock()
	defer m.Mu.RUnlock()
	return filterImportTasks(m.tasks.listTasksByJob(jobID), filters...)
}

func filterImportTasks(tasks []ImportTask, filters ...ImportTaskFilter) []ImportTask {
	ret := make([]ImportTask, 0)
OUTER:
	for _, task := range tasks {
		for _, f := range filters {
			if !f(task) {
				continue OUTER
			}
		}
		ret = append(ret, task)
	}
	return ret
}

func (m *Meta) RemoveTask(ctx context.Context, taskID int64) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	if task := m.tasks.get(taskID); task != nil {
		switch task.GetType() {
		case PreImportTaskType:
			err := m.Catalog.DropPreImportTask(ctx, taskID)
			if err != nil {
				return err
			}
		case ImportTaskType:
			err := m.Catalog.DropImportTask(ctx, taskID)
			if err != nil {
				return err
			}
		case ReshardTaskType:
			if err := m.Catalog.DropReshardTask(ctx, taskID); err != nil {
				return err
			}
		case ImportTaskV3Type:
			if err := m.Catalog.DropImportTaskV3(ctx, taskID); err != nil {
				return err
			}
		case PreImportTaskV3Type:
			if err := m.Catalog.DropPreImportTaskV3(ctx, taskID); err != nil {
				return err
			}
		}
		m.tasks.remove(taskID)
	}
	return nil
}

func (m *Meta) TaskStatsJSON(ctx context.Context) string {
	tasks := m.tasks.listTaskStats()

	ret, err := json.Marshal(tasks)
	if err != nil {
		return ""
	}
	return string(ret)
}

func (m *Meta) HandleCommitVchannel(ctx context.Context, jobID int64, vchannel string, callback func() error) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	job := m.Jobs[jobID]
	if job == nil {
		return merr.WrapErrImportSysFailedMsg("job %d not found", jobID)
	}
	switch job.GetState() {
	case internalpb.ImportJobState_Uncommitted, internalpb.ImportJobState_Committing:
		// continue
	case internalpb.ImportJobState_Completed, internalpb.ImportJobState_Failed:
		return nil
	default:
		// Do not record committed_vchannels while the import task is still
		// importing. The caller must retry after the job becomes Uncommitted;
		// otherwise a later retry would treat this vchannel as committed even
		// though the visibility callback has not run.
		return merr.WrapErrImportSysFailedMsg("job %d is in state %s, waiting for Uncommitted", jobID, job.GetState())
	}
	// Idempotency: if vchannel already committed, skip.
	for _, c := range job.GetCommittedVchannels() {
		if c == vchannel {
			return nil
		}
	}
	if job.GetState() == internalpb.ImportJobState_Uncommitted {
		updatedJob := job.Clone()
		updatedJob.(*Job).State = internalpb.ImportJobState_Committing
		if err := m.Catalog.SaveImportJob(ctx, updatedJob.(*Job).ImportJob); err != nil {
			return err
		}
		m.Jobs[jobID] = updatedJob
		job = updatedJob
	}
	// Move the job into commit phase before making any segment visible, then
	// execute the callback before persisting the committed vchannel.
	// If callback fails, we return error without persisting committed_vchannels;
	// the caller retries and the callback will be invoked again. This avoids the
	// scenario where committed_vchannels is persisted but callback fails, causing
	// the idempotency check to skip the callback on retry (data stays invisible
	// forever).
	// The callback (setting is_importing=false) is idempotent, so re-execution on
	// retry after a persist failure is safe.
	//
	// Visibility ordering note: the callback clears segment meta (is_importing=false)
	// before this function persists job meta (committed_vchannels). Therefore a
	// vchannel's imported data can become visible before the job-level transition
	// to Completed (which happens later in checkCommittingJob once all vchannels
	// have been recorded here). This is inherent to per-vchannel commit fences —
	// 2PC for import is per-vchannel-atomic, not job-atomic. See MEP
	// (milvus-io/milvus-design-docs#29) "Segment Visibility" section.
	if err := callback(); err != nil {
		return err
	}
	updatedJob := job.Clone()
	updatedJob.(*Job).CommittedVchannels = append(updatedJob.GetCommittedVchannels(), vchannel)
	if err := m.Catalog.SaveImportJob(ctx, updatedJob.(*Job).ImportJob); err != nil {
		return err
	}
	m.Jobs[jobID] = updatedJob
	return nil
}
