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

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// externalCollectionRefreshInspector seeds the global task scheduler from meta.
// That is its entire responsibility — it never mutates a task. It consults only
// the owning job's terminal bit as a dispatch fence; the checker still owns all
// job state transitions and reconciliation.
//
// It exists because the scheduler is in-memory only and can recycle just the
// tasks it already holds (schedule()/check() push an Init/Retry task back onto
// their own pending queue). Two classes of task are outside it and would
// otherwise never run again:
//  1. after a DataCoord restart, every persisted active task — nothing else
//     re-populates the scheduler;
//  2. a task reset in meta by a component the scheduler is not driving. The
//     load-bearing case is resetFinishedTasksLocked, which returns a job's
//     already-Finished tasks to Init after a manifest CAS conflict: the
//     scheduler dropped them when they finished, so this sweep is the only path
//     that re-dispatches them.
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
	// wrapTask builds a scheduler-facing task wrapper with all callbacks
	// wired (processFinishedJob → checker.processJobByID). The manager owns
	// the wiring logic and injects this factory so the inspector doesn't
	// need a direct reference to the checker (avoids construction-order
	// circular dependency).
	wrapTask func(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask
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
	mlog.Info(i.ctx, "start external collection inspector", mlog.Duration("inspectInterval", inspectInterval))

	ticker := time.NewTicker(inspectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-i.closeChan:
			mlog.Info(i.ctx, "external collection inspector exited")
			return
		case <-ticker.C:
			i.inspect()
		}
	}
}

// inspect is the steady-state sweep: hand the scheduler whatever is waiting to
// be dispatched.
func (i *externalCollectionRefreshInspector) inspect() {
	i.enqueueActiveTasks(false)
}

// reloadFromMeta is the startup sweep. It is the only caller that includes
// InProgress: that re-adopts an attempt which was in flight when DataCoord went
// down, so the scheduler resumes polling it instead of leaving it to age out. On
// a steady-state tick an InProgress task is already held by the scheduler, so
// sweeping it in would be a no-op at best.
func (i *externalCollectionRefreshInspector) reloadFromMeta() {
	i.enqueueActiveTasks(true)
}

// enqueueActiveTasks hands every task waiting to run to the scheduler. The job
// lookup is a dispatch fence: async explore and startup recovery can expose an
// Init task after its owning job has already reached Failed/Finished, and such a
// task must never consume a worker slot while waiting for checker reconciliation.
//
// Reach it through inspect() / reloadFromMeta() rather than directly: those name
// what includeInProgress means at each call site.
func (i *externalCollectionRefreshInspector) enqueueActiveTasks(includeInProgress bool) {
	for _, t := range i.refreshMeta.GetAllTasks() {
		job := i.refreshMeta.GetJob(t.GetJobId())
		if job == nil || isTerminalJobState(job.GetState()) {
			continue
		}
		switch t.GetState() {
		case indexpb.JobState_JobStateInit, indexpb.JobState_JobStateRetry:
		case indexpb.JobState_JobStateInProgress:
			if !includeInProgress {
				continue
			}
		default:
			continue
		}
		// The scheduler dedups by taskID, so re-enqueueing one it already holds
		// is a no-op.
		i.scheduler.Enqueue(i.wrapTask(t))
	}
}
