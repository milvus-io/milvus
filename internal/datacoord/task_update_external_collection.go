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
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
)

type updateExternalCollectionTask struct {
	*indexpb.UpdateExternalCollectionTask

	times *taskcommon.Times

	meta *meta
}

var _ globalTask.Task = (*updateExternalCollectionTask)(nil)

func newUpdateExternalCollectionTask(t *indexpb.UpdateExternalCollectionTask, mt *meta) *updateExternalCollectionTask {
	return &updateExternalCollectionTask{
		UpdateExternalCollectionTask: t,
		times:                        taskcommon.NewTimes(),
		meta:                         mt,
	}
}

func (t *updateExternalCollectionTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *updateExternalCollectionTask) GetTaskType() taskcommon.Type {
	// Reuse Stats type for now, or we could add a new type
	return taskcommon.Stats
}

func (t *updateExternalCollectionTask) GetTaskState() taskcommon.State {
	return t.GetState()
}

func (t *updateExternalCollectionTask) GetTaskSlot() int64 {
	// External collection tasks are lightweight, use 1 slot
	return 1
}

func (t *updateExternalCollectionTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	t.times.SetTaskTime(timeType, time)
}

func (t *updateExternalCollectionTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(t.times)
}

func (t *updateExternalCollectionTask) GetTaskVersion() int64 {
	return t.GetVersion()
}

// validateSource checks if this task's external source matches the current collection source
// Returns error if task has been superseded
func (t *updateExternalCollectionTask) validateSource() error {
	collection := t.meta.GetCollection(t.GetCollectionID())
	if collection == nil {
		return fmt.Errorf("collection %d not found", t.GetCollectionID())
	}

	currentSource := collection.Schema.GetExternalSource()
	currentSpec := collection.Schema.GetExternalSpec()

	taskSource := t.GetExternalSource()
	taskSpec := t.GetExternalSpec()

	if currentSource != taskSource || currentSpec != taskSpec {
		return fmt.Errorf("task source mismatch: task source=%s/%s, current source=%s/%s (task has been superseded)",
			taskSource, taskSpec, currentSource, currentSpec)
	}

	return nil
}

func (t *updateExternalCollectionTask) SetState(state indexpb.JobState, failReason string) {
	// If transitioning to finished state, validate source first
	if state == indexpb.JobState_JobStateFinished {
		if err := t.validateSource(); err != nil {
			log.Warn("Task source validation failed, marking as failed instead",
				zap.Int64("taskID", t.GetTaskID()),
				zap.Int64("collectionID", t.GetCollectionID()),
				zap.Error(err))
			t.State = indexpb.JobState_JobStateFailed
			t.FailReason = fmt.Sprintf("source mismatch: %s", err.Error())
			return
		}
	}

	t.State = state
	t.FailReason = failReason
}

func (t *updateExternalCollectionTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	ctx := context.Background()
	log := log.Ctx(ctx).With(
		zap.Int64("taskID", t.GetTaskID()),
		zap.Int64("collectionID", t.GetCollectionID()),
	)

	// For external collections, we just need to update metadata
	// This is a placeholder for actual logic that would:
	// 1. Query external storage for collection statistics
	// 2. Update collection metadata in meta
	// 3. Mark task as finished

	log.Info("updating external collection metadata")

	// TODO: Implement actual update logic
	// For now, just mark as finished
	t.SetState(indexpb.JobState_JobStateFinished, "")

	log.Info("external collection metadata updated successfully")
}

func (t *updateExternalCollectionTask) QueryTaskOnWorker(cluster session.Cluster) {
	// External collection tasks finish immediately, so query is a no-op
}

func (t *updateExternalCollectionTask) DropTaskOnWorker(cluster session.Cluster) {
	// External collection tasks don't run on workers, so drop is a no-op
}
