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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

// externalCollectionRefreshInspector handles task scheduling and recovery for external collection refresh.
//
// This is an internal component of ExternalCollectionRefreshManager, responsible for:
// 1. Reload InProgress/Init tasks to scheduler on DataCoord restart (idempotent recovery)
// 2. Periodically enqueue pending tasks to the global task scheduler for execution
//
// TASK STATE TRANSITIONS:
// Init → InProgress (inspector enqueues to scheduler, scheduler dispatches to DataNode)
// InProgress → Finished/Failed (DataNode reports execution result)
type externalCollectionRefreshInspector struct {
	ctx         context.Context
	refreshMeta *externalCollectionRefreshMeta
	mt          *meta // meta for task operations (CreateTaskOnWorker, SetJobInfo)
	scheduler   task.GlobalScheduler
	allocator   allocator.Allocator
	closeChan   chan struct{}
}

func newRefreshInspector(
	ctx context.Context,
	refreshMeta *externalCollectionRefreshMeta,
	mt *meta,
	scheduler task.GlobalScheduler,
	allocator allocator.Allocator,
	closeChan chan struct{},
) *externalCollectionRefreshInspector {
	return &externalCollectionRefreshInspector{
		ctx:         ctx,
		refreshMeta: refreshMeta,
		mt:          mt,
		scheduler:   scheduler,
		allocator:   allocator,
		closeChan:   closeChan,
	}
}

// run starts the inspector loop.
func (i *externalCollectionRefreshInspector) run() {
	// Reload tasks on startup for idempotent recovery
	i.reloadFromMeta()

	// Log inspection interval for observability
	inspectInterval := Params.DataCoordCfg.ExternalCollectionCheckInterval.GetAsDuration(time.Second)
	log.Ctx(i.ctx).Info("start external collection inspector", zap.Duration("inspectInterval", inspectInterval))

	ticker := time.NewTicker(inspectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-i.closeChan:
			log.Ctx(i.ctx).Info("external collection inspector exited")
			return
		case <-ticker.C:
			i.inspect()
		}
	}
}

// inspect runs a single inspection cycle to re-enqueue any pending tasks.
func (i *externalCollectionRefreshInspector) inspect() {
	tasks := i.refreshMeta.GetAllTasks()
	for _, t := range tasks {
		switch t.GetState() {
		case indexpb.JobState_JobStateInit, indexpb.JobState_JobStateRetry:
			// Re-enqueue pending tasks (scheduler will deduplicate)
			taskWrapper := newRefreshExternalCollectionTask(t, i.refreshMeta, i.mt, i.allocator)
			i.scheduler.Enqueue(taskWrapper)
		}
	}
}

// reloadFromMeta reloads active tasks from metadata on startup.
func (i *externalCollectionRefreshInspector) reloadFromMeta() {
	tasks := i.refreshMeta.GetAllTasks()
	for _, t := range tasks {
		if t.GetState() != indexpb.JobState_JobStateInit &&
			t.GetState() != indexpb.JobState_JobStateRetry &&
			t.GetState() != indexpb.JobState_JobStateInProgress {
			continue
		}
		// Enqueue active tasks for processing
		taskWrapper := newRefreshExternalCollectionTask(t, i.refreshMeta, i.mt, i.allocator)
		i.scheduler.Enqueue(taskWrapper)
	}
}
