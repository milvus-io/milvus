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

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// externalCollectionRefreshInspector handles task scheduling and recovery for external collection refresh.
//
// This is an internal component of ExternalCollectionRefreshManager, responsible for:
// 1. Reload published InProgress/Init tasks on DataCoord restart
// 2. Re-enqueue InProgress siblings of a Failed job so their DataNode tasks are canceled
// 3. Replace committed Retry tasks with fresh task IDs, preserving their plan
// 4. Periodically enqueue pending tasks to the global task scheduler for execution
//
// TASK STATE TRANSITIONS:
// Init → InProgress (inspector enqueues to scheduler, scheduler dispatches to DataNode)
// InProgress → Finished/Retry/Failed (DataNode reports execution result)
// Retry → replacement Init task (same manifest, file range, and owned segments)
type externalCollectionRefreshInspector struct {
	ctx            context.Context
	refreshMeta    *externalCollectionRefreshMeta
	scheduler      task.GlobalScheduler
	closeChan      chan struct{}
	allocateTaskID func(context.Context) (int64, error)
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
	scheduler task.GlobalScheduler,
	closeChan chan struct{},
) *externalCollectionRefreshInspector {
	return &externalCollectionRefreshInspector{
		ctx:         ctx,
		refreshMeta: refreshMeta,
		scheduler:   scheduler,
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

// inspect runs a single inspection cycle to re-enqueue any pending tasks.
func (i *externalCollectionRefreshInspector) inspect() {
	i.enqueueCommittedTasks(false)
}

// reloadFromMeta reloads active tasks from metadata on startup.
func (i *externalCollectionRefreshInspector) reloadFromMeta() {
	i.enqueueCommittedTasks(true)
}

func (i *externalCollectionRefreshInspector) buildRetryReplacement(
	oldTask *datapb.ExternalCollectionRefreshTask,
) (*datapb.ExternalCollectionRefreshTask, error) {
	if i.allocateTaskID == nil {
		return nil, merr.WrapErrServiceInternalMsg("external refresh replacement task allocator is not configured")
	}
	newTaskID, err := i.allocateTaskID(i.ctx)
	if err != nil {
		return nil, merr.Wrap(err, "allocate external refresh replacement task ID")
	}

	replacement := proto.Clone(oldTask).(*datapb.ExternalCollectionRefreshTask)
	replacement.TaskId = newTaskID
	// Version is retained only as a compatibility field. Current attempts start
	// at zero and use the fresh task ID as their identity.
	replacement.Version = 0
	replacement.NodeId = 0
	replacement.State = indexpb.JobState_JobStateInit
	replacement.FailReason = ""
	replacement.Progress = 0
	replacement.KeptSegments = nil
	replacement.UpdatedSegments = nil
	replacement.ResultReady = false
	replacement.ResultStorageVersion = 0
	replacement.ResultPath = ""
	replacement.ResultChecksum = nil
	replacement.BaseManifests = nil
	return replacement, nil
}

func (i *externalCollectionRefreshInspector) enqueue(taskProto *datapb.ExternalCollectionRefreshTask) bool {
	if i.wrapTask == nil {
		mlog.Warn(i.ctx, "external refresh task wrapper is not configured",
			mlog.FieldJobID(taskProto.GetJobId()),
			mlog.FieldTaskID(taskProto.GetTaskId()))
		return false
	}

	taskWrapper := i.wrapTask(taskProto)
	i.scheduler.Enqueue(taskWrapper)
	return true
}

func (i *externalCollectionRefreshInspector) replaceRetryTask(
	oldTask *datapb.ExternalCollectionRefreshTask,
) {
	replacement, err := i.buildRetryReplacement(oldTask)
	if err != nil {
		mlog.Warn(i.ctx, "failed to build external refresh retry replacement",
			mlog.FieldJobID(oldTask.GetJobId()),
			mlog.FieldTaskID(oldTask.GetTaskId()),
			mlog.Err(err))
		return
	}

	replaced := false
	i.scheduler.Finalize(oldTask.GetTaskId(), func() {
		replaced, err = i.refreshMeta.ReplaceRetryTask(oldTask.GetTaskId(), replacement)
	})
	if err != nil {
		mlog.Warn(i.ctx, "failed to replace external refresh retry task",
			mlog.FieldJobID(oldTask.GetJobId()),
			mlog.FieldTaskID(oldTask.GetTaskId()),
			mlog.Err(err))
		return
	}
	if !replaced {
		return
	}
	i.enqueue(replacement)
}

// enqueueCommittedTasks schedules only tasks referenced by their parent job's
// published task_ids. Periodic scans enqueue Init/InProgress tasks and
// atomically replace Retry tasks. Startup resumes Init/InProgress but leaves
// Retry until the first business inspection interval, matching the other task
// owners. Re-offering InProgress is idempotent while owned and recovers a
// wrapper released after a local persistence failure. For a Failed job,
// startup recovery alone enqueues InProgress siblings to drive cancellation
// and release the DataNode task.
func (i *externalCollectionRefreshInspector) enqueueCommittedTasks(startup bool) {
	for jobID, job := range i.refreshMeta.GetAllJobs() {
		if job.GetState() == indexpb.JobState_JobStateFinished {
			continue
		}
		failedJob := job.GetState() == indexpb.JobState_JobStateFailed
		if failedJob && !startup {
			continue
		}

		tasks, err := i.refreshMeta.GetCommittedTasksByJobID(jobID)
		if err != nil {
			mlog.Warn(i.ctx, "failed to resolve committed external refresh tasks",
				mlog.FieldJobID(jobID),
				mlog.Err(err))
			continue
		}
		for _, task := range tasks {
			if failedJob {
				if task.GetState() == indexpb.JobState_JobStateInProgress {
					i.enqueue(task)
				}
				continue
			}
			switch task.GetState() {
			case indexpb.JobState_JobStateInit:
				i.enqueue(task)
			case indexpb.JobState_JobStateRetry:
				if !startup {
					i.replaceRetryTask(task)
				}
			case indexpb.JobState_JobStateInProgress:
				i.enqueue(task)
			}
		}
	}
}
