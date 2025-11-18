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
	"strconv"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type externalCollectionTaskMeta struct {
	ctx     context.Context
	catalog metastore.DataCoordCatalog

	keyLock *lock.KeyLock[UniqueID]
	// taskID -> UpdateExternalCollectionTask
	tasks *typeutil.ConcurrentMap[UniqueID, *indexpb.UpdateExternalCollectionTask]

	// collectionID -> UpdateExternalCollectionTask
	collectionID2Tasks *typeutil.ConcurrentMap[UniqueID, *indexpb.UpdateExternalCollectionTask]
}

func newExternalCollectionTaskMeta(ctx context.Context, catalog metastore.DataCoordCatalog) (*externalCollectionTaskMeta, error) {
	ectm := &externalCollectionTaskMeta{
		ctx:                ctx,
		catalog:            catalog,
		keyLock:            lock.NewKeyLock[UniqueID](),
		tasks:              typeutil.NewConcurrentMap[UniqueID, *indexpb.UpdateExternalCollectionTask](),
		collectionID2Tasks: typeutil.NewConcurrentMap[UniqueID, *indexpb.UpdateExternalCollectionTask](),
	}
	if err := ectm.reloadFromKV(); err != nil {
		return nil, err
	}
	return ectm, nil
}

func (ectm *externalCollectionTaskMeta) reloadFromKV() error {
	record := timerecord.NewTimeRecorder("externalCollectionTaskMeta-reloadFromKV")
	tasks, err := ectm.catalog.ListUpdateExternalCollectionTasks(ectm.ctx)
	if err != nil {
		log.Error("externalCollectionTaskMeta reloadFromKV load tasks failed", zap.Error(err))
		return err
	}
	for _, t := range tasks {
		ectm.tasks.Insert(t.GetTaskID(), t)
		ectm.collectionID2Tasks.Insert(t.GetCollectionID(), t)
	}

	log.Info("externalCollectionTaskMeta reloadFromKV done", zap.Duration("duration", record.ElapseSpan()))
	return nil
}

func (ectm *externalCollectionTaskMeta) AddTask(t *indexpb.UpdateExternalCollectionTask) error {
	ectm.keyLock.Lock(t.GetTaskID())
	defer ectm.keyLock.Unlock(t.GetTaskID())

	log.Ctx(ectm.ctx).Info("add update external collection task",
		zap.Int64("taskID", t.GetTaskID()),
		zap.Int64("collectionID", t.GetCollectionID()))

	if _, ok := ectm.collectionID2Tasks.Get(t.GetCollectionID()); ok {
		log.Warn("update external collection task already exists for collection",
			zap.Int64("collectionID", t.GetCollectionID()))
		return merr.WrapErrTaskDuplicate(strconv.FormatInt(t.GetCollectionID(), 10))
	}

	if err := ectm.catalog.SaveUpdateExternalCollectionTask(ectm.ctx, t); err != nil {
		log.Warn("save update external collection task failed",
			zap.Int64("taskID", t.GetTaskID()),
			zap.Int64("collectionID", t.GetCollectionID()),
			zap.Error(err))
		return err
	}

	ectm.tasks.Insert(t.GetTaskID(), t)
	ectm.collectionID2Tasks.Insert(t.GetCollectionID(), t)

	log.Info("add update external collection task success",
		zap.Int64("taskID", t.GetTaskID()),
		zap.Int64("collectionID", t.GetCollectionID()))
	return nil
}

func (ectm *externalCollectionTaskMeta) DropTask(ctx context.Context, taskID int64) error {
	ectm.keyLock.Lock(taskID)
	defer ectm.keyLock.Unlock(taskID)

	log.Ctx(ctx).Info("drop update external collection task by taskID", zap.Int64("taskID", taskID))

	t, ok := ectm.tasks.Get(taskID)
	if !ok {
		log.Info("remove update external collection task success, task already not exist", zap.Int64("taskID", taskID))
		return nil
	}
	if err := ectm.catalog.DropUpdateExternalCollectionTask(ctx, taskID); err != nil {
		log.Warn("drop update external collection task failed",
			zap.Int64("taskID", taskID),
			zap.Int64("collectionID", t.GetCollectionID()),
			zap.Error(err))
		return err
	}

	ectm.tasks.Remove(taskID)
	ectm.collectionID2Tasks.Remove(t.GetCollectionID())

	log.Info("remove update external collection task success", zap.Int64("taskID", taskID))
	return nil
}

func (ectm *externalCollectionTaskMeta) UpdateVersion(taskID, nodeID int64) error {
	ectm.keyLock.Lock(taskID)
	defer ectm.keyLock.Unlock(taskID)

	t, ok := ectm.tasks.Get(taskID)
	if !ok {
		return fmt.Errorf("task %d not found", taskID)
	}

	cloneT := proto.Clone(t).(*indexpb.UpdateExternalCollectionTask)
	cloneT.Version++
	cloneT.NodeID = nodeID

	if err := ectm.catalog.SaveUpdateExternalCollectionTask(ectm.ctx, cloneT); err != nil {
		log.Warn("update external collection task version failed",
			zap.Int64("taskID", t.GetTaskID()),
			zap.Int64("collectionID", t.GetCollectionID()),
			zap.Int64("nodeID", nodeID),
			zap.Error(err))
		return err
	}

	ectm.tasks.Insert(taskID, cloneT)
	ectm.collectionID2Tasks.Insert(t.GetCollectionID(), cloneT)
	log.Info("update external collection task version success", zap.Int64("taskID", taskID), zap.Int64("nodeID", nodeID),
		zap.Int64("newVersion", cloneT.GetVersion()))
	return nil
}

func (ectm *externalCollectionTaskMeta) UpdateTaskState(taskID int64, state indexpb.JobState, failReason string) error {
	ectm.keyLock.Lock(taskID)
	defer ectm.keyLock.Unlock(taskID)

	t, ok := ectm.tasks.Get(taskID)
	if !ok {
		return fmt.Errorf("task %d not found", taskID)
	}

	cloneT := proto.Clone(t).(*indexpb.UpdateExternalCollectionTask)
	cloneT.State = state
	cloneT.FailReason = failReason

	if err := ectm.catalog.SaveUpdateExternalCollectionTask(ectm.ctx, cloneT); err != nil {
		log.Warn("update external collection task state failed",
			zap.Int64("taskID", t.GetTaskID()),
			zap.Error(err))
		return err
	}

	ectm.tasks.Insert(taskID, cloneT)
	ectm.collectionID2Tasks.Insert(t.GetCollectionID(), cloneT)

	return nil
}

func (ectm *externalCollectionTaskMeta) GetTask(taskID int64) *indexpb.UpdateExternalCollectionTask {
	t, ok := ectm.tasks.Get(taskID)
	if !ok {
		return nil
	}
	return proto.Clone(t).(*indexpb.UpdateExternalCollectionTask)
}

func (ectm *externalCollectionTaskMeta) GetTaskState(taskID int64) indexpb.JobState {
	t, ok := ectm.tasks.Get(taskID)
	if !ok {
		return indexpb.JobState_JobStateNone
	}
	return t.State
}

func (ectm *externalCollectionTaskMeta) GetTaskByCollectionID(collectionID int64) *indexpb.UpdateExternalCollectionTask {
	t, ok := ectm.collectionID2Tasks.Get(collectionID)
	if !ok {
		return nil
	}
	return proto.Clone(t).(*indexpb.UpdateExternalCollectionTask)
}

func (ectm *externalCollectionTaskMeta) GetAllTasks() map[int64]*indexpb.UpdateExternalCollectionTask {
	tasks := make(map[int64]*indexpb.UpdateExternalCollectionTask)
	ectm.tasks.Range(func(taskID int64, task *indexpb.UpdateExternalCollectionTask) bool {
		tasks[taskID] = proto.Clone(task).(*indexpb.UpdateExternalCollectionTask)
		return true
	})
	return tasks
}
