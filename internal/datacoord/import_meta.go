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
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/proto/datapb"
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
	// TODO: dyh, remove dependency of broker
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
	restoredTasks, err := catalog.ListImportTasks()
	if err != nil {
		return nil, err
	}
	tasks := make(map[int64]ImportTask)
	for _, t := range restoredTasks {
		switch task := t.(type) {
		case *datapb.PreImportTask:
			catalog.DropImportTask(task.GetTaskID())
			continue
			schema, err := getSchema(task.GetCollectionID())
			if err != nil {
				return nil, err
			}
			tasks[task.GetTaskID()] = &preImportTask{
				PreImportTask:  task,
				schema:         schema,
				lastActiveTime: time.Now(),
			}
		case *datapb.ImportTaskV2:
			catalog.DropImportTask(task.GetTaskID())
			continue
			schema, err := getSchema(task.GetCollectionID())
			if err != nil {
				return nil, err
			}
			tasks[task.GetTaskID()] = &importTask{
				ImportTaskV2:   task,
				schema:         schema,
				lastActiveTime: time.Now(),
			}
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
		err := m.catalog.SaveImportTask(task.(*preImportTask).PreImportTask)
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
			err := m.catalog.SaveImportTask(updatedTask.(*preImportTask).PreImportTask)
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
	if _, ok := m.tasks[taskID]; ok {
		err := m.catalog.DropImportTask(taskID)
		if err != nil {
			return err
		}
		delete(m.tasks, taskID)
	}
	return nil
}
