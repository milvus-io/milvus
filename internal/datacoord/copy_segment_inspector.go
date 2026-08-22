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
	"sort"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// Copy Segment Task Inspector
//
// The inspector is responsible for task-level scheduling and failure handling during
// snapshot restore operations. It runs in a periodic loop to monitor task states and
// take appropriate actions.
//
// RESPONSIBILITIES:
// 1. Reload InProgress tasks to scheduler on DataCoord restart (idempotent recovery)
// 2. Enqueue Pending tasks to the global task scheduler for execution
// 3. Clean up target segments when tasks fail (drop incomplete segments)
//
// TASK STATE TRANSITIONS:
// Pending → InProgress (inspector enqueues to scheduler)
// InProgress → Completed/Failed (datanode reports execution result)
// Failed → Dropped (inspector drops target segments)
//
// INSPECTION INTERVAL:
// Configured by Params.DataCoordCfg.CopySegmentCheckInterval (default: 2 seconds)
//
// COORDINATION:
// - Works with CopySegmentChecker which manages job-level state machine
// - Uses GlobalScheduler to dispatch tasks to DataNodes
// - Updates segment metadata to mark failed segments as Dropped

// ===========================================================================================
// Inspector Interface and Implementation
// ===========================================================================================

// CopySegmentInspector defines the interface for task-level scheduling and monitoring.
type CopySegmentInspector interface {
	// Start begins the periodic inspection loop in a background goroutine.
	// It first reloads any InProgress tasks from metadata, then enters the inspection loop.
	Start()

	// Close gracefully stops the inspector, ensuring no goroutine leaks.
	// Safe to call multiple times (uses sync.Once).
	Close()
}

// copySegmentInspector implements the CopySegmentInspector interface.
type copySegmentInspector struct {
	ctx       context.Context      // Context for cancellation and logging
	meta      *meta                // Segment metadata (for dropping failed target segments)
	copyMeta  CopySegmentMeta      // Copy job and task metadata
	scheduler task.GlobalScheduler // Task scheduler for dispatching to DataNodes
	cluster   session.Cluster      // Worker cluster, for reclaiming a retired task's worker attempt

	closeOnce sync.Once     // Ensures Close is idempotent
	closeChan chan struct{} // Channel to signal inspector shutdown
}

// ===========================================================================================
// Constructor
// ===========================================================================================

// NewCopySegmentInspector creates a new inspector instance.
//
// Parameters:
//   - ctx: Context for cancellation and logging
//   - meta: Segment metadata for updating segment states
//   - copyMeta: Copy job and task metadata store
//   - scheduler: Global task scheduler for dispatching tasks
//
// Returns:
//
//	A new CopySegmentInspector instance ready to Start.
func NewCopySegmentInspector(
	ctx context.Context,
	meta *meta,
	copyMeta CopySegmentMeta,
	scheduler task.GlobalScheduler,
	cluster session.Cluster,
) CopySegmentInspector {
	return &copySegmentInspector{
		ctx:       ctx,
		meta:      meta,
		copyMeta:  copyMeta,
		scheduler: scheduler,
		cluster:   cluster,
		closeChan: make(chan struct{}),
	}
}

// ===========================================================================================
// Lifecycle Management
// ===========================================================================================

// Start begins the periodic inspection loop.
//
// Process flow:
//  1. Reload InProgress tasks from metadata (for recovery after DataCoord restart)
//  2. Log inspection interval for observability
//  3. Enter periodic inspection loop:
//     a. Wait for ticker or close signal
//     b. Run inspect() to process all pending/failed tasks
//     c. Repeat until Close() is called
//
// Why this design:
// - Reloading ensures tasks don't get lost on DataCoord restart
// - Periodic inspection handles tasks that may have been missed during transitions
// - Separate ticker allows tuning inspection frequency independently
func (s *copySegmentInspector) Start() {
	// Reload tasks on startup for idempotent recovery
	s.reloadFromMeta()

	// Log inspection interval for observability
	inspectInterval := Params.DataCoordCfg.CopySegmentCheckInterval.GetAsDuration(time.Second)
	mlog.Info(s.ctx, "start copy segment inspector", mlog.Duration("inspectInterval", inspectInterval))

	ticker := time.NewTicker(inspectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.closeChan:
			mlog.Info(s.ctx, "copy segment inspector exited")
			return
		case <-ticker.C:
			s.inspect()
		}
	}
}

// Close gracefully shuts down the inspector.
//
// This signals the inspection loop to exit and ensures the goroutine terminates.
// Safe to call multiple times (uses sync.Once internally).
func (s *copySegmentInspector) Close() {
	s.closeOnce.Do(func() {
		close(s.closeChan)
	})
}

// ===========================================================================================
// Task Recovery and Inspection
// ===========================================================================================

// reloadFromMeta reloads InProgress tasks to scheduler on DataCoord restart.
//
// Process flow:
//  1. Retrieve all copy segment jobs from metadata
//  2. Sort jobs by ID for deterministic processing order
//  3. For each job, retrieve all associated tasks
//  4. Enqueue any InProgress tasks to the scheduler
//  5. Log the number of jobs processed for observability
//
// Why this is needed:
// - DataCoord may restart while tasks are executing on DataNodes
// - InProgress tasks need to be re-added to scheduler to continue monitoring
// - This ensures no tasks are orphaned after restart
//
// Idempotency:
// - Safe to call multiple times (scheduler handles duplicate enqueues)
// - Only InProgress tasks are reloaded (Pending will be handled by inspect loop)
func (s *copySegmentInspector) reloadFromMeta() {
	// Retrieve all jobs (no filters)
	jobs := s.copyMeta.GetJobBy(s.ctx)
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].GetJobId() < jobs[j].GetJobId()
	})

	for _, job := range jobs {
		if !s.reconcileReplannedTasks(job.GetJobId()) {
			// A persisted replacement and its predecessor are still sharing the
			// same source work. Do not reload either side until the predecessor's
			// targets are Dropped and its record is retired.
			continue
		}
		tasks := s.copyMeta.GetTasksByJobID(s.ctx, job.GetJobId())
		for _, task := range tasks {
			if task.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskInProgress {
				s.scheduler.Enqueue(task)
			}
		}
	}
	mlog.Info(s.ctx, "copy segment inspector reloaded tasks from meta",
		mlog.Int("jobCount", len(jobs)))
}

// reclaimWorkerAttempt takes a task's attempt off its worker before the task
// record is removed. The record is the only thing that names the node; once it
// is gone, no GC round can ever send the drop. The drop is detached rather than
// awaited: it is an RPC bounded only by requestTimeoutSeconds, and this runs
// inside the per-tick inspect/reload loop, where a node that stopped answering
// must not stall every later copy-segment task for that long. A drop that fails
// after the record is gone leaves the DataNode entry for the shared 24-hour
// sweep -- the same bounded residual the live replan path already relies on.
func (s *copySegmentInspector) reclaimWorkerAttempt(t CopySegmentTask) bool {
	if !isNodeAssigned(t.GetNodeId()) {
		return true
	}
	go t.DropTaskOnWorker(s.cluster)
	return true
}

// reconcileReplannedTasks completes exact predecessor/replacement handoffs.
//
// A replan publishes its Pending task and targets through one composite write,
// with the task first in the ordered over-limit fallback. The explicit
// predecessor edge prevents unrelated tasks with overlapping source segments
// from being paired. A crash at any point leaves enough durable state to finish
// target publication or predecessor cleanup before scheduling either record.
func (s *copySegmentInspector) reconcileReplannedTasks(jobID int64) bool {
	tasks := s.copyMeta.GetTasksByJobID(s.ctx, jobID)
	if len(tasks) == 0 {
		return true
	}

	byID := make(map[int64]CopySegmentTask, len(tasks))
	replacements := make(map[int64][]CopySegmentTask)
	for _, task := range tasks {
		byID[task.GetTaskId()] = task
		if predecessorID := task.GetPredecessorTaskId(); predecessorID != 0 {
			replacements[predecessorID] = append(replacements[predecessorID], task)
		}
	}
	if len(replacements) == 0 {
		return true
	}

	settled := true
	for predecessorID, candidates := range replacements {
		// Source-side uniqueness: replanUnderFreshIdentity adopts an existing
		// successor instead of minting another, so exactly one candidate per
		// predecessor is the steady state. The ascending task-ID order keeps
		// the pick deterministic should residue from before that guarantee
		// still name the same predecessor.
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].GetTaskId() < candidates[j].GetTaskId()
		})
		replacement := candidates[0]
		for _, duplicate := range candidates[1:] {
			if !s.processFailed(duplicate) {
				settled = false
				continue
			}
			s.reclaimWorkerAttempt(duplicate)
			if err := s.copyMeta.RemoveTask(s.ctx, duplicate.GetTaskId()); err != nil {
				settled = false
				mlog.Warn(s.ctx, "failed to retire a duplicate copy segment replan",
					WrapCopySegmentTaskLog(duplicate, mlog.Err(err))...)
			}
		}

		predecessor := byID[predecessorID]
		if predecessor == nil {
			// The predecessor was already retired -- a completed handoff, or job
			// GC removing the pair. The replacement is now an ordinary task the
			// job owns alone, and inspect() dispatches it. The "Pending but
			// targets not yet registered" case cannot occur here: the predecessor
			// is only removed after publishCopySegmentReplacement has registered
			// every target.
			continue
		}

		if predecessor.GetJobId() != replacement.GetJobId() ||
			predecessor.GetCollectionId() != replacement.GetCollectionId() ||
			replacement.GetTaskVersion() != predecessor.GetTaskVersion()+1 {
			settled = false
			// Rated for the same reason as above: a mismatched pair can never
			// self-heal.
			mlog.RatedWarn(s.ctx, rate.Limit(1.0/60), "copy segment replacement does not match its named predecessor",
				WrapCopySegmentTaskLog(replacement,
					mlog.Int64("predecessorTaskID", predecessorID),
					mlog.Int64("predecessorAttempt", predecessor.GetTaskVersion()))...)
			continue
		}

		// If an ambiguous composite write raced with the old attempt actually
		// completing, the completed result wins; no replacement targets were
		// needed and any partially published ones are reclaimed.
		if predecessor.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskCompleted {
			if !s.processFailed(replacement) {
				settled = false
				continue
			}
			s.reclaimWorkerAttempt(replacement)
			if err := s.copyMeta.RemoveTask(s.ctx, replacement.GetTaskId()); err != nil {
				settled = false
				mlog.Warn(s.ctx, "failed to retire an unnecessary copy segment replan",
					WrapCopySegmentTaskLog(replacement, mlog.Err(err))...)
			}
			continue
		}

		// Resume a publication only when the replacement's targets are not yet
		// all registered. Once they are, re-running publishCopySegmentReplacement
		// would rebuild the target inventory from the predecessor's targets --
		// which segment GC may have already reclaimed -- and fail with
		// SegmentNotFound forever, skipping a restore that is in fact complete.
		if replacement.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskPending &&
			!s.replacementTargetsReady(replacement) {
			if err := publishCopySegmentReplacement(s.ctx, predecessor, replacement, s.meta, s.copyMeta); err != nil {
				settled = false
				mlog.Warn(s.ctx, "failed to resume copy segment replacement publication",
					WrapCopySegmentTaskLog(replacement, mlog.Err(err))...)
				continue
			}
		}

		mlog.Info(s.ctx, "retiring a copy segment task superseded by its named replan",
			WrapCopySegmentTaskLog(predecessor,
				mlog.Int64("replacementTaskID", replacement.GetTaskId()),
				mlog.Int64("attempt", replacement.GetTaskVersion()))...)
		if !s.processFailed(predecessor) {
			settled = false
			continue
		}
		s.reclaimWorkerAttempt(predecessor)
		if err := s.copyMeta.RemoveTask(s.ctx, predecessor.GetTaskId()); err != nil {
			settled = false
			mlog.Warn(s.ctx, "failed to retire a superseded copy segment task",
				WrapCopySegmentTaskLog(predecessor, mlog.Err(err))...)
		}
	}
	return settled
}

func (s *copySegmentInspector) replacementTargetsReady(task CopySegmentTask) bool {
	for _, mapping := range task.GetIdMappings() {
		segment := s.meta.GetSegment(s.ctx, mapping.GetTargetSegmentId())
		if segment == nil || segment.GetCollectionID() != task.GetCollectionId() ||
			segment.GetState() != commonpb.SegmentState_Importing {
			return false
		}
	}
	return len(task.GetIdMappings()) > 0
}

// inspect runs a single inspection cycle to process all pending and failed tasks.
//
// Process flow:
//  1. Retrieve all copy segment jobs from metadata
//  2. Sort jobs by ID for deterministic processing order
//  3. For each job, retrieve all associated tasks
//  4. Process tasks based on state:
//     - Pending: Enqueue to scheduler for execution
//     - Failed: Drop target segments to clean up incomplete data
//
// Why periodic inspection:
// - Tasks may transition to Pending state at any time (when checker creates them)
// - Failed tasks need prompt cleanup to prevent orphaned segments
// - Periodic inspection ensures no tasks are missed during state transitions
func (s *copySegmentInspector) inspect() {
	// Retrieve all jobs (no filters)
	jobs := s.copyMeta.GetJobBy(s.ctx)
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].GetJobId() < jobs[j].GetJobId()
	})

	for _, job := range jobs {
		// Before acting on any task, settle a replan that did not finish handing
		// over. A crash is not the only way to leave the pair behind -- a replan
		// whose retirement write failed leaves it too, with the process still
		// running -- so this belongs on the periodic round, not only in recovery.
		if !s.reconcileReplannedTasks(job.GetJobId()) {
			// A replacement stays undispatchable while its predecessor remains
			// the cleanup inventory. Otherwise a transient target-state write
			// failure could leave both attempts live.
			continue
		}
		tasks := s.copyMeta.GetTasksByJobID(s.ctx, job.GetJobId())
		for _, task := range tasks {
			switch task.GetState() {
			case datapb.CopySegmentTaskState_CopySegmentTaskPending:
				s.processPending(task)
			case datapb.CopySegmentTaskState_CopySegmentTaskFailed:
				// Best effort: a failed drop keeps the task Failed, so the next
				// round retries it, and checkGC has an independent hasSegments
				// guard.
				s.processFailed(task)
			}
		}
	}
}

// ===========================================================================================
// Task State Processing
// ===========================================================================================

// processPending enqueues a pending task to the scheduler for execution.
//
// Process flow:
//  1. Enqueue task to global scheduler
//  2. Scheduler will assign task to available DataNode
//  3. DataNode executes CopySegmentTask and reports results
//
// Why this design:
// - Decouples task scheduling from task execution
// - Scheduler handles load balancing across DataNodes
// - Enables concurrent execution of multiple tasks
//
// Idempotency:
// - Safe to enqueue same task multiple times (scheduler handles duplicates)
// - Task state will transition to InProgress when actually dispatched
func (s *copySegmentInspector) processPending(task CopySegmentTask) {
	s.scheduler.Enqueue(task)
}

// processFailed handles cleanup for failed copy segment tasks.
//
// Process flow:
//  1. Iterate through all segment ID mappings in the task
//  2. For each target segment:
//     a. Retrieve segment metadata
//     b. Mark segment as Dropped if it exists and is not already Dropped
//     c. Log success/failure of drop operation
//
// Why drop target segments:
// - Failed tasks may have partially copied data to target segments
// - Incomplete segments should not be visible to queries
// - Dropping ensures consistent state and prevents data corruption
//
// Error handling:
//   - Logs warnings if drop fails but continues processing other segments
//   - Returns false if any target still needs cleanup; retained task records make
//     those failures retryable on the next inspection cycle
func (s *copySegmentInspector) processFailed(task CopySegmentTask) bool {
	cleaned := true
	// Drop target segments if copy failed
	for _, mapping := range task.GetIdMappings() {
		targetSegID := mapping.GetTargetSegmentId()
		segment := s.meta.GetSegment(s.ctx, targetSegID)
		if segment == nil || segment.GetState() == commonpb.SegmentState_Dropped {
			continue
		}

		// is_importing is cleared with the drop: the garbage collector skips
		// is_importing segments to protect the in-flight commit marker, so a
		// dropped target that keeps the flag would never be reclaimed. Copy
		// targets are never that marker.
		op := UpdateStatusOperator(targetSegID, commonpb.SegmentState_Dropped)
		err := s.meta.UpdateSegmentsInfo(s.ctx, op, UpdateIsImporting(targetSegID, false))
		if err != nil {
			cleaned = false
			mlog.Warn(s.ctx, "failed to drop target segment after copy task failed",
				WrapCopySegmentTaskLog(task, mlog.Int64("segmentID", targetSegID), mlog.Err(err))...)
		} else {
			mlog.Info(s.ctx, "dropped target segment after copy task failed",
				WrapCopySegmentTaskLog(task, mlog.Int64("segmentID", targetSegID))...)
		}
	}
	return cleaned
}
