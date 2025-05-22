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
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/samber/lo"
	"golang.org/x/exp/maps"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
)

type ImportMeta interface {
	AddJob(ctx context.Context, job ImportJob) error
	UpdateJob(ctx context.Context, jobID int64, actions ...UpdateJobAction) error
	GetJob(ctx context.Context, jobID int64) ImportJob
	GetJobBy(ctx context.Context, filters ...ImportJobFilter) []ImportJob
	CountJobBy(ctx context.Context, filters ...ImportJobFilter) int
	RemoveJob(ctx context.Context, jobID int64) error

	AddTask(ctx context.Context, task ImportTask) error
	UpdateTask(ctx context.Context, taskID int64, actions ...UpdateAction) error
	GetTask(ctx context.Context, taskID int64) ImportTask
	GetTaskBy(ctx context.Context, filters ...ImportTaskFilter) []ImportTask
	RemoveTask(ctx context.Context, taskID int64) error
	TaskStatsJSON(ctx context.Context) string
}

type importTasks struct {
	tasks     map[int64]ImportTask
	taskStats *expirable.LRU[int64, ImportTask]
}

func newImportTasks() *importTasks {
	return &importTasks{
		tasks:     make(map[int64]ImportTask),
		taskStats: expirable.NewLRU[UniqueID, ImportTask](512, nil, time.Minute*30),
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
	t.tasks[task.GetTaskID()] = task
	t.taskStats.Add(task.GetTaskID(), task)
}

func (t *importTasks) remove(taskID int64) {
	task, ok := t.tasks[taskID]
	if ok {
		delete(t.tasks, taskID)
		t.taskStats.Add(task.GetTaskID(), task)
	}
}

func (t *importTasks) listTasks() []ImportTask {
	return maps.Values(t.tasks)
}

func (t *importTasks) listTaskStats() []ImportTask {
	return t.taskStats.Values()
}

type importMeta struct {
	mu      lock.RWMutex // guards jobs and tasks
	jobs    map[int64]ImportJob
	tasks   *importTasks
	catalog metastore.DataCoordCatalog
}

func NewImportMeta(ctx context.Context, catalog metastore.DataCoordCatalog, alloc allocator.Allocator, meta *meta) (ImportMeta, error) {
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

	tasks := newImportTasks()
	importMeta := &importMeta{}

	for _, task := range restoredPreImportTasks {
		t := &preImportTask{
			importMeta: importMeta,
			tr:         timerecord.NewTimeRecorder("preimport task"),
			times:      taskcommon.NewTimes(),
		}
		t.task.Store(task)
		tasks.add(t)
	}
	for _, task := range restoredImportTasks {
		t := &importTask{
			alloc:      alloc,
			meta:       meta,
			importMeta: importMeta,
			tr:         timerecord.NewTimeRecorder("import task"),
			times:      taskcommon.NewTimes(),
		}
		t.task.Store(task)
		tasks.add(t)
	}

	jobs := make(map[int64]ImportJob)
	for _, job := range restoredJobs {
		jobs[job.GetJobID()] = &importJob{
			ImportJob: job,
			tr:        timerecord.NewTimeRecorder("import job"),
		}
	}

	importMeta.jobs = jobs
	importMeta.tasks = tasks
	importMeta.catalog = catalog
	return importMeta, nil
}

func (m *importMeta) AddJob(ctx context.Context, job ImportJob) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	originJob := m.jobs[job.GetJobID()]
	if originJob != nil {
		originJob := originJob.Clone()
		internalJob := originJob.(*importJob).ImportJob
		internalJob.ReadyVchannels = lo.Union(originJob.GetReadyVchannels(), job.GetReadyVchannels())
		job = originJob
	}
	err := m.catalog.SaveImportJob(ctx, job.(*importJob).ImportJob)
	if err != nil {
		return err
	}
	m.jobs[job.GetJobID()] = job
	return nil
}

func (m *importMeta) UpdateJob(ctx context.Context, jobID int64, actions ...UpdateJobAction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if job, ok := m.jobs[jobID]; ok {
		updatedJob := job.Clone()
		for _, action := range actions {
			action(updatedJob)
		}
		err := m.catalog.SaveImportJob(ctx, updatedJob.(*importJob).ImportJob)
		if err != nil {
			return err
		}
		m.jobs[updatedJob.GetJobID()] = updatedJob
	}
	return nil
}

func (m *importMeta) GetJob(ctx context.Context, jobID int64) ImportJob {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.jobs[jobID]
}

func (m *importMeta) GetJobBy(ctx context.Context, filters ...ImportJobFilter) []ImportJob {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.getJobBy(filters...)
}

func (m *importMeta) getJobBy(filters ...ImportJobFilter) []ImportJob {
	ret := make([]ImportJob, 0)
OUTER:
	for _, job := range m.jobs {
		for _, f := range filters {
			if !f(job) {
				continue OUTER
			}
		}
		ret = append(ret, job)
	}
	return ret
}

func (m *importMeta) CountJobBy(ctx context.Context, filters ...ImportJobFilter) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.getJobBy(filters...))
}

func (m *importMeta) RemoveJob(ctx context.Context, jobID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.jobs[jobID]; ok {
		err := m.catalog.DropImportJob(ctx, jobID)
		if err != nil {
			return err
		}
		delete(m.jobs, jobID)
	}
	return nil
}

func (m *importMeta) AddTask(ctx context.Context, task ImportTask) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch task.GetType() {
	case PreImportTaskType:
		err := m.catalog.SavePreImportTask(ctx, task.(*preImportTask).task.Load())
		if err != nil {
			return err
		}
		m.tasks.add(task)
	case ImportTaskType:
		err := m.catalog.SaveImportTask(ctx, task.(*importTask).task.Load())
		if err != nil {
			return err
		}
		m.tasks.add(task)
	}
	return nil
}

func (m *importMeta) UpdateTask(ctx context.Context, taskID int64, actions ...UpdateAction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task := m.tasks.get(taskID); task != nil {
		updatedTask := task.Clone()
		for _, action := range actions {
			action(updatedTask)
		}
		switch updatedTask.GetType() {
		case PreImportTaskType:
			err := m.catalog.SavePreImportTask(ctx, updatedTask.(*preImportTask).task.Load())
			if err != nil {
				return err
			}
			// update memory task
			task.(*preImportTask).task.Store(updatedTask.(*preImportTask).task.Load())
		case ImportTaskType:
			err := m.catalog.SaveImportTask(ctx, updatedTask.(*importTask).task.Load())
			if err != nil {
				return err
			}
			// update memory task
			task.(*importTask).task.Store(updatedTask.(*importTask).task.Load())
		}
	}

	return nil
}

func (m *importMeta) GetTask(ctx context.Context, taskID int64) ImportTask {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tasks.get(taskID)
}

func (m *importMeta) GetTaskBy(ctx context.Context, filters ...ImportTaskFilter) []ImportTask {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ret := make([]ImportTask, 0)
OUTER:
	for _, task := range m.tasks.listTasks() {
		for _, f := range filters {
			if !f(task) {
				continue OUTER
			}
		}
		ret = append(ret, task)
	}
	return ret
}

func (m *importMeta) RemoveTask(ctx context.Context, taskID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task := m.tasks.get(taskID); task != nil {
		switch task.GetType() {
		case PreImportTaskType:
			err := m.catalog.DropPreImportTask(ctx, taskID)
			if err != nil {
				return err
			}
		case ImportTaskType:
			err := m.catalog.DropImportTask(ctx, taskID)
			if err != nil {
				return err
			}
		}
		m.tasks.remove(taskID)
	}
	return nil
}

func (m *importMeta) TaskStatsJSON(ctx context.Context) string {
	tasks := m.tasks.listTaskStats()

	ret, err := json.Marshal(tasks)
	if err != nil {
		return ""
	}
	return string(ret)
}
