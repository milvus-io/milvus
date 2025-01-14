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

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

type compactionTaskMeta struct {
	lock.RWMutex
	ctx     context.Context
	catalog metastore.DataCoordCatalog
	// currently only clustering compaction task is stored in persist meta
	compactionTasks map[int64]map[int64]*datapb.CompactionTask // triggerID -> planID
}

func newCompactionTaskMeta(ctx context.Context, catalog metastore.DataCoordCatalog) (*compactionTaskMeta, error) {
	csm := &compactionTaskMeta{
		RWMutex:         lock.RWMutex{},
		ctx:             ctx,
		catalog:         catalog,
		compactionTasks: make(map[int64]map[int64]*datapb.CompactionTask, 0),
	}
	if err := csm.reloadFromKV(); err != nil {
		return nil, err
	}
	return csm, nil
}

func (csm *compactionTaskMeta) reloadFromKV() error {
	record := timerecord.NewTimeRecorder("compactionTaskMeta-reloadFromKV")
	compactionTasks, err := csm.catalog.ListCompactionTask(csm.ctx)
	if err != nil {
		return err
	}
	for _, task := range compactionTasks {
		csm.saveCompactionTaskMemory(task)
	}
	log.Info("DataCoord compactionTaskMeta reloadFromKV done", zap.Duration("duration", record.ElapseSpan()))
	return nil
}

// GetCompactionTasks returns clustering compaction tasks from local cache
func (csm *compactionTaskMeta) GetCompactionTasks() map[int64][]*datapb.CompactionTask {
	csm.RLock()
	defer csm.RUnlock()
	res := make(map[int64][]*datapb.CompactionTask, 0)
	for triggerID, tasks := range csm.compactionTasks {
		triggerTasks := make([]*datapb.CompactionTask, 0)
		for _, task := range tasks {
			triggerTasks = append(triggerTasks, proto.Clone(task).(*datapb.CompactionTask))
		}
		res[triggerID] = triggerTasks
	}
	return res
}

func (csm *compactionTaskMeta) GetCompactionTasksByCollection(collectionID int64) map[int64][]*datapb.CompactionTask {
	csm.RLock()
	defer csm.RUnlock()
	res := make(map[int64][]*datapb.CompactionTask, 0)
	for _, tasks := range csm.compactionTasks {
		for _, task := range tasks {
			if task.CollectionID == collectionID {
				_, exist := res[task.TriggerID]
				if !exist {
					res[task.TriggerID] = make([]*datapb.CompactionTask, 0)
				}
				res[task.TriggerID] = append(res[task.TriggerID], proto.Clone(task).(*datapb.CompactionTask))
			} else {
				break
			}
		}
	}
	return res
}

func (csm *compactionTaskMeta) GetCompactionTasksByTriggerID(triggerID int64) []*datapb.CompactionTask {
	csm.RLock()
	defer csm.RUnlock()
	res := make([]*datapb.CompactionTask, 0)
	tasks, triggerIDExist := csm.compactionTasks[triggerID]
	if triggerIDExist {
		for _, task := range tasks {
			res = append(res, proto.Clone(task).(*datapb.CompactionTask))
		}
	}
	return res
}

func (csm *compactionTaskMeta) SaveCompactionTask(task *datapb.CompactionTask) error {
	csm.Lock()
	defer csm.Unlock()
	if err := csm.catalog.SaveCompactionTask(csm.ctx, task); err != nil {
		log.Error("meta update: update compaction task fail", zap.Error(err))
		return err
	}
	return csm.saveCompactionTaskMemory(task)
}

func (csm *compactionTaskMeta) saveCompactionTaskMemory(task *datapb.CompactionTask) error {
	_, triggerIDExist := csm.compactionTasks[task.TriggerID]
	if !triggerIDExist {
		csm.compactionTasks[task.TriggerID] = make(map[int64]*datapb.CompactionTask, 0)
	}
	csm.compactionTasks[task.TriggerID][task.PlanID] = task
	return nil
}

func (csm *compactionTaskMeta) DropCompactionTask(task *datapb.CompactionTask) error {
	csm.Lock()
	defer csm.Unlock()
	if err := csm.catalog.DropCompactionTask(csm.ctx, task); err != nil {
		log.Error("meta update: drop compaction task fail", zap.Int64("triggerID", task.TriggerID), zap.Int64("planID", task.PlanID), zap.Int64("collectionID", task.CollectionID), zap.Error(err))
		return err
	}
	_, triggerIDExist := csm.compactionTasks[task.TriggerID]
	if triggerIDExist {
		delete(csm.compactionTasks[task.TriggerID], task.PlanID)
	}
	if len(csm.compactionTasks[task.TriggerID]) == 0 {
		delete(csm.compactionTasks, task.TriggerID)
	}
	return nil
}
