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
	"encoding/json"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/exp/maps"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

type ImportMeta interface {
	AddJob(job ImportJob) error
	UpdateJob(jobID int64, actions ...UpdateJobAction) error
	GetJob(jobID int64) ImportJob
	GetJobBy(filters ...ImportJobFilter) []ImportJob
	CountJobBy(filters ...ImportJobFilter) int
	RemoveJob(jobID int64) error

	AddTask(task ImportTask) error
	UpdateTask(taskID int64, actions ...UpdateAction) error
	GetTask(taskID int64) ImportTask
	GetTaskBy(filters ...ImportTaskFilter) []ImportTask
	RemoveTask(taskID int64) error
	TaskStatsJSON() string
}

type importTasks struct {
	tasks     map[int64]ImportTask
	taskStats *expirable.LRU[int64, ImportTask]
}

func newImportTasks() *importTasks {
	return &importTasks{
		tasks:     make(map[int64]ImportTask),
		taskStats: expirable.NewLRU[UniqueID, ImportTask](4096, nil, time.Minute*60),
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

func NewImportMeta(catalog metastore.DataCoordCatalog) (ImportMeta, error) {
	restoredPreImportTasks, err := catalog.ListPreImportTasks()
	if err != nil {
		return nil, err
	}
	restoredImportTasks, err := catalog.ListImportTasks()
	if err != nil {
		return nil, err
	}
	restoredJobs, err := catalog.ListImportJobs()
	if err != nil {
		return nil, err
	}

	tasks := newImportTasks()

	for _, task := range restoredPreImportTasks {
		tasks.add(&preImportTask{
			PreImportTask: task,
			tr:            timerecord.NewTimeRecorder("preimport task"),
		})
	}
	for _, task := range restoredImportTasks {
		tasks.add(&importTask{
			ImportTaskV2: task,
			tr:           timerecord.NewTimeRecorder("import task"),
		})
	}

	jobs := make(map[int64]ImportJob)
	for _, job := range restoredJobs {
		jobs[job.GetJobID()] = &importJob{
			ImportJob: job,
			tr:        timerecord.NewTimeRecorder("import job"),
		}
	}

	return &importMeta{
		jobs:    jobs,
		tasks:   tasks,
		catalog: catalog,
	}, nil
}

func (m *importMeta) AddJob(job ImportJob) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.catalog.SaveImportJob(job.(*importJob).ImportJob)
	if err != nil {
		return err
	}
	m.jobs[job.GetJobID()] = job
	return nil
}

func (m *importMeta) UpdateJob(jobID int64, actions ...UpdateJobAction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if job, ok := m.jobs[jobID]; ok {
		updatedJob := job.Clone()
		for _, action := range actions {
			action(updatedJob)
		}
		err := m.catalog.SaveImportJob(updatedJob.(*importJob).ImportJob)
		if err != nil {
			return err
		}
		m.jobs[updatedJob.GetJobID()] = updatedJob
	}
	return nil
}

func (m *importMeta) GetJob(jobID int64) ImportJob {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.jobs[jobID]
}

func (m *importMeta) GetJobBy(filters ...ImportJobFilter) []ImportJob {
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

func (m *importMeta) CountJobBy(filters ...ImportJobFilter) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.getJobBy(filters...))
}

func (m *importMeta) RemoveJob(jobID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.jobs[jobID]; ok {
		err := m.catalog.DropImportJob(jobID)
		if err != nil {
			return err
		}
		delete(m.jobs, jobID)
	}
	return nil
}

func (m *importMeta) AddTask(task ImportTask) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch task.GetType() {
	case PreImportTaskType:
		err := m.catalog.SavePreImportTask(task.(*preImportTask).PreImportTask)
		if err != nil {
			return err
		}
		m.tasks.add(task)
	case ImportTaskType:
		err := m.catalog.SaveImportTask(task.(*importTask).ImportTaskV2)
		if err != nil {
			return err
		}
		m.tasks.add(task)
	}
	return nil
}

func (m *importMeta) UpdateTask(taskID int64, actions ...UpdateAction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task := m.tasks.get(taskID); task != nil {
		updatedTask := task.Clone()
		for _, action := range actions {
			action(updatedTask)
		}
		switch updatedTask.GetType() {
		case PreImportTaskType:
			err := m.catalog.SavePreImportTask(updatedTask.(*preImportTask).PreImportTask)
			if err != nil {
				return err
			}
			m.tasks.add(updatedTask)
		case ImportTaskType:
			err := m.catalog.SaveImportTask(updatedTask.(*importTask).ImportTaskV2)
			if err != nil {
				return err
			}
			m.tasks.add(updatedTask)
		}
	}

	return nil
}

func (m *importMeta) GetTask(taskID int64) ImportTask {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tasks.get(taskID)
}

func (m *importMeta) GetTaskBy(filters ...ImportTaskFilter) []ImportTask {
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

func (m *importMeta) RemoveTask(taskID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task := m.tasks.get(taskID); task != nil {
		switch task.GetType() {
		case PreImportTaskType:
			err := m.catalog.DropPreImportTask(taskID)
			if err != nil {
				return err
			}
		case ImportTaskType:
			err := m.catalog.DropImportTask(taskID)
			if err != nil {
				return err
			}
		}
		m.tasks.remove(taskID)
	}
	return nil
}

func (m *importMeta) TaskStatsJSON() string {
	tasks := m.tasks.listTaskStats()
	if len(tasks) == 0 {
		return ""
	}

	ret, err := json.Marshal(tasks)
	if err != nil {
		return ""
	}
	return string(ret)
}
