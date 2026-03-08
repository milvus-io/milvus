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

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

// externalCollectionRefreshChecker drives the external collection job state machine.
//
// This is an internal component of ExternalCollectionRefreshManager, responsible for:
// 1. Job timeout detection
// 2. Garbage collection for completed/failed jobs (including associated tasks)
// 3. Job statistics reporting
//
// JOB STATE MACHINE:
// Init → InProgress → Finished
//
//	↓        ↓            ↓
//
// Failed  Failed        GC
//
//	↓        ↓            ↓
//
// GC       GC       (removed)
//
// STATE TRANSITIONS:
// 1. Init → InProgress: Task dispatched to DataNode (handled by scheduler)
// 2. InProgress → Finished: All tasks completed successfully
// 3. InProgress → Failed: Any task failed or job timeout
// 4. Finished/Failed → GC: Remove job and tasks after retention period
type externalCollectionRefreshChecker struct {
	ctx         context.Context
	refreshMeta *externalCollectionRefreshMeta
	closeChan   chan struct{}
}

func newRefreshChecker(
	ctx context.Context,
	refreshMeta *externalCollectionRefreshMeta,
	closeChan chan struct{},
) *externalCollectionRefreshChecker {
	return &externalCollectionRefreshChecker{
		ctx:         ctx,
		refreshMeta: refreshMeta,
		closeChan:   closeChan,
	}
}

// run starts the checker loop.
func (c *externalCollectionRefreshChecker) run() {
	checkInterval := Params.DataCoordCfg.ExternalCollectionCheckInterval.GetAsDuration(time.Second)
	log.Info("start external collection checker", zap.Duration("checkInterval", checkInterval))
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.closeChan:
			log.Info("external collection checker exited")
			return
		case <-ticker.C:
			// Fetch all jobs from metadata
			jobs := c.refreshMeta.GetAllJobs()

			// Process each job based on its state
			for _, job := range jobs {
				// Aggregate task states to update job state
				c.aggregateJobState(job)

				// Check timeout for active jobs (Init, Retry, InProgress)
				// Jobs stuck in any active state should be timed out to prevent resource leaks
				switch job.GetState() {
				case indexpb.JobState_JobStateInit, indexpb.JobState_JobStateRetry, indexpb.JobState_JobStateInProgress:
					c.tryTimeoutJob(job)
				}

				// Check GC for terminal states (Finished/Failed)
				c.checkGC(job)
			}

			// Report statistics and metrics
			c.logJobStats(jobs)
		}
	}
}

// aggregateJobState updates job state based on its tasks.
func (c *externalCollectionRefreshChecker) aggregateJobState(job *datapb.ExternalCollectionRefreshJob) {
	// Skip if job is already in terminal state
	if job.GetState() == indexpb.JobState_JobStateFinished ||
		job.GetState() == indexpb.JobState_JobStateFailed {
		return
	}

	// Get aggregated state from tasks
	state, progress := c.refreshMeta.AggregateJobStateFromTasks(job.GetJobId())
	if state == indexpb.JobState_JobStateNone {
		// No tasks yet
		return
	}

	// Update job if state or progress changed
	if state != job.GetState() {
		// State changed - handle state transition
		var failReason string
		if state == indexpb.JobState_JobStateFailed {
			// Get fail reason from first failed task
			tasks := c.refreshMeta.GetTasksByJobID(job.GetJobId())
			for _, task := range tasks {
				if task.GetState() == indexpb.JobState_JobStateFailed {
					failReason = task.GetFailReason()
					break
				}
			}
			// Persist progress snapshot BEFORE transitioning to Failed
			// This captures the last known progress at failure time
			if progress != job.GetProgress() {
				if err := c.refreshMeta.UpdateJobProgress(job.GetJobId(), progress); err != nil {
					log.Warn("failed to update job progress before failure",
						zap.Int64("jobID", job.GetJobId()),
						zap.Error(err))
				}
			}
		}

		if err := c.refreshMeta.UpdateJobState(job.GetJobId(), state, failReason); err != nil {
			log.Warn("failed to update job state from task aggregation",
				zap.Int64("jobID", job.GetJobId()),
				zap.Error(err))
			return
		}

		// For Finished state, UpdateJobState sets Progress=100
		// For Failed state, progress was already persisted above
		// For non-terminal states, update progress if needed
		if state != indexpb.JobState_JobStateFailed && state != indexpb.JobState_JobStateFinished {
			if progress != job.GetProgress() {
				if err := c.refreshMeta.UpdateJobProgress(job.GetJobId(), progress); err != nil {
					log.Warn("failed to update job progress",
						zap.Int64("jobID", job.GetJobId()),
						zap.Error(err))
				}
			}
		}
	} else if progress != job.GetProgress() {
		// Only progress changed
		if err := c.refreshMeta.UpdateJobProgress(job.GetJobId(), progress); err != nil {
			log.Warn("failed to update job progress",
				zap.Int64("jobID", job.GetJobId()),
				zap.Error(err))
		}
	}
}

// logJobStats reports job statistics grouped by state.
func (c *externalCollectionRefreshChecker) logJobStats(jobs map[int64]*datapb.ExternalCollectionRefreshJob) {
	// Group jobs by state
	byState := lo.GroupBy(lo.Values(jobs), func(job *datapb.ExternalCollectionRefreshJob) string {
		return job.GetState().String()
	})

	// Count jobs in each state
	stateNum := make(map[string]int)
	for state := range indexpb.JobState_value {
		if state == indexpb.JobState_JobStateNone.String() {
			continue
		}
		stateNum[state] = len(byState[state])
	}

	if len(jobs) > 0 {
		log.Info("external collection job stats", zap.Any("stateNum", stateNum))
	}
}

// tryTimeoutJob checks if job has exceeded timeout and marks it as failed.
func (c *externalCollectionRefreshChecker) tryTimeoutJob(job *datapb.ExternalCollectionRefreshJob) {
	// Skip if StartTime is not set
	if job.GetStartTime() == 0 {
		return
	}

	// Get timeout configuration
	timeout := Params.DataCoordCfg.ExternalCollectionJobTimeout.GetAsDuration(time.Second)

	// Calculate job age
	startTime := time.UnixMilli(job.GetStartTime())
	age := time.Since(startTime)

	if age > timeout {
		log.Warn("external collection job timeout",
			zap.Int64("jobID", job.GetJobId()),
			zap.Int64("collectionID", job.GetCollectionId()),
			zap.Duration("age", age),
			zap.Duration("timeout", timeout))

		err := c.refreshMeta.UpdateJobState(
			job.GetJobId(),
			indexpb.JobState_JobStateFailed,
			"timeout")
		if err != nil {
			log.Warn("failed to mark job as timed out",
				zap.Int64("jobID", job.GetJobId()),
				zap.Error(err))
			return
		}

		// Also mark all active tasks as failed
		tasks := c.refreshMeta.GetTasksByJobID(job.GetJobId())
		for _, task := range tasks {
			if task.GetState() == indexpb.JobState_JobStateInit ||
				task.GetState() == indexpb.JobState_JobStateRetry ||
				task.GetState() == indexpb.JobState_JobStateInProgress {
				_ = c.refreshMeta.UpdateTaskState(task.GetTaskId(), indexpb.JobState_JobStateFailed, "job timeout")
			}
		}
	}
}

// checkGC performs garbage collection for completed/failed jobs.
func (c *externalCollectionRefreshChecker) checkGC(job *datapb.ExternalCollectionRefreshJob) {
	// Only GC terminal states
	if job.GetState() != indexpb.JobState_JobStateFinished &&
		job.GetState() != indexpb.JobState_JobStateFailed {
		return
	}

	// Check if job has EndTime set
	if job.GetEndTime() == 0 {
		return
	}

	// Get retention configuration
	retention := Params.DataCoordCfg.ExternalCollectionJobRetention.GetAsDuration(time.Second)

	// Calculate time since job ended
	endTime := time.UnixMilli(job.GetEndTime())
	age := time.Since(endTime)

	if age > retention {
		log.Info("external collection job has reached GC retention",
			zap.Int64("jobID", job.GetJobId()),
			zap.Int64("collectionID", job.GetCollectionId()),
			zap.Duration("age", age),
			zap.Duration("retention", retention))

		// DropJob drops job and associated tasks. No in-loop retry: checkGC runs periodically,
		// so the next tick will naturally retry if etcd was temporarily unavailable.
		err := c.refreshMeta.DropJob(c.ctx, job.GetJobId())
		if err != nil {
			log.Warn("failed to remove external collection job during GC, will retry on next check",
				zap.Int64("jobID", job.GetJobId()),
				zap.Error(err))
			return
		}
		log.Info("external collection job removed", zap.Int64("jobID", job.GetJobId()))
	}
}
