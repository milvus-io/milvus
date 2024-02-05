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
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

type ImportMeta interface {
	Add(task ImportTask) error
	Update(taskID int64, actions ...UpdateAction) error
	Get(taskID int64) ImportTask
	GetBy(filters ...ImportTaskFilter) []ImportTask
	Remove(taskID int64) error
}

type importMeta struct {
	mu    sync.RWMutex // guards tasks
	tasks map[int64]ImportTask

	catalog metastore.DataCoordCatalog
}

func NewImportMeta(broker broker.Broker, catalog metastore.DataCoordCatalog) (ImportMeta, error) {
	getSchema := func(collectionID int64) (*schemapb.CollectionSchema, error) {
		var schema *schemapb.CollectionSchema
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := retry.Do(ctx, func() error {
			resp, err := broker.DescribeCollectionInternal(ctx, collectionID)
			schema = resp.GetSchema()
			return err
		})
		return schema, err
	}

	restoredPreImportTasks, err := catalog.ListPreImportTasks()
	if err != nil {
		return nil, err
	}
	restoredImportTasks, err := catalog.ListImportTasks()
	if err != nil {
		return nil, err
	}

	tasks := make(map[int64]ImportTask)
	for _, task := range restoredPreImportTasks {
		tasks[task.GetTaskID()] = &preImportTask{
			PreImportTask:  task,
			lastActiveTime: time.Now(),
		}
	}
	for _, task := range restoredImportTasks {
		tasks[task.GetTaskID()] = &importTask{
			ImportTaskV2:   task,
			lastActiveTime: time.Now(),
		}
	}

	byColl := lo.GroupBy(lo.Values(tasks), func(t ImportTask) int64 {
		return t.GetCollectionID()
	})
	collectionSchemas := make(map[int64]*schemapb.CollectionSchema)
	for collectionID := range byColl {
		schema, err := getSchema(collectionID)
		if err != nil {
			return nil, err
		}
		collectionSchemas[collectionID] = schema
	}
	for i := range tasks {
		switch tasks[i].(type) {
		case *preImportTask:
			tasks[i].(*preImportTask).schema = collectionSchemas[tasks[i].GetCollectionID()]
		case *importTask:
			tasks[i].(*importTask).schema = collectionSchemas[tasks[i].GetCollectionID()]
		}
	}

	return &importMeta{
		tasks:   tasks,
		catalog: catalog,
	}, nil
}

func (m *importMeta) Add(task ImportTask) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch task.GetType() {
	case PreImportTaskType:
		err := m.catalog.SavePreImportTask(task.(*preImportTask).PreImportTask)
		if err != nil {
			return err
		}
		m.tasks[task.GetTaskID()] = task
	case ImportTaskType:
		err := m.catalog.SaveImportTask(task.(*importTask).ImportTaskV2)
		if err != nil {
			return err
		}
		m.tasks[task.GetTaskID()] = task
	}
	return nil
}

func (m *importMeta) Update(taskID int64, actions ...UpdateAction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task, ok := m.tasks[taskID]; ok {
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
			updatedTask.(*preImportTask).lastActiveTime = time.Now()
			m.tasks[updatedTask.GetTaskID()] = updatedTask
		case ImportTaskType:
			err := m.catalog.SaveImportTask(updatedTask.(*importTask).ImportTaskV2)
			if err != nil {
				return err
			}
			updatedTask.(*importTask).lastActiveTime = time.Now()
			m.tasks[updatedTask.GetTaskID()] = updatedTask
		}
	}

	return nil
}

func (m *importMeta) Get(taskID int64) ImportTask {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tasks[taskID]
}

func (m *importMeta) GetBy(filters ...ImportTaskFilter) []ImportTask {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ret := make([]ImportTask, 0)
OUTER:
	for _, task := range m.tasks {
		for _, f := range filters {
			if !f(task) {
				continue OUTER
			}
		}
		ret = append(ret, task)
	}
	return ret
}

func (m *importMeta) Remove(taskID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task, ok := m.tasks[taskID]; ok {
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
		delete(m.tasks, taskID)
	}
	return nil
}
