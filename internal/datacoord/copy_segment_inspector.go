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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// copySegmentDropConcurrency bounds the worker-side drops the inspector may have
// in flight at once. The drops are retried every round until they converge, so
// this only needs to be wide enough that one unresponsive node cannot starve the
// others.
const copySegmentDropConcurrency = 8

// untrackedCopySegmentDrop identifies one exact worker-side dispatch to clean
// up. taskVersion is part of the identity: the worker only honors a drop that
// still matches the dispatch it is executing, so a retry of this entry can
// never delete the output of a later dispatch of the same task.
type untrackedCopySegmentDrop struct {
	nodeID      int64
	taskID      int64
	taskVersion int64
}

// Copy Segment Task Inspector
//
// The inspector is responsible for task-level scheduling and failure handling during
// snapshot restore operations. It runs in a periodic loop to monitor task states and
// take appropriate actions.
//
// RESPONSIBILITIES:
// 1. Reload InProgress tasks to scheduler on DataCoord restart (idempotent recovery)
// 2. Enqueue Pending tasks to the global task scheduler for execution
// 3. Clean up the target segments of a failed JOB — a restore is all-or-nothing,
//    so this covers every task of that job, not only the one that failed
// 4. Unassign terminal tasks whose worker-side drop did not complete, so GC can
//    reclaim them (checkGC skips any task that still carries a node assignment)
//
// TASK STATE TRANSITIONS:
// Pending → InProgress (inspector enqueues to scheduler, only while the job is active)
// InProgress → Completed/Failed (datanode reports execution result)
// Terminal task under a Failed job → target segments Dropped (inspector)
//
// Pending/InProgress tasks of a Failed job are not dropped here — checkFailedJob
// converges them to Failed first, and they are cleaned up on a later round.
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
	cluster   session.Cluster      // DataNode cluster (for retrying the drop of terminal tasks)

	// Worker-side drops run off the inspection loop: DropCopySegment blocks for
	// up to DataCoordCfg.RequestTimeoutSeconds (default 600s), and inspect()
	// walks every job and task in one goroutine, so a single black-holing
	// DataNode would otherwise freeze dispatch and cleanup for everything else.
	dropPool          *conc.Pool[struct{}]
	droppingTask      *typeutil.ConcurrentSet[int64] // taskIDs with a drop in flight
	untrackedDrops    *typeutil.ConcurrentSet[untrackedCopySegmentDrop]
	droppingUntracked *typeutil.ConcurrentSet[untrackedCopySegmentDrop]
	dropCtx           context.Context
	dropCancel        context.CancelFunc
	dropLifecycleMu   sync.RWMutex
	dropClosed        bool

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
//   - cluster: DataNode cluster used to retry the drop of terminal tasks
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
	dropCtx, dropCancel := context.WithCancel(ctx) //nolint:gosec // dropCancel is stored on the inspector and called in Close()
	inspector := &copySegmentInspector{
		ctx:       ctx,
		meta:      meta,
		copyMeta:  copyMeta,
		scheduler: scheduler,
		cluster:   cluster,
		dropPool: conc.NewPool[struct{}](copySegmentDropConcurrency,
			conc.WithExpiryDuration(time.Minute), conc.WithNonBlocking(true)),
		droppingTask:      typeutil.NewConcurrentSet[int64](),
		untrackedDrops:    typeutil.NewConcurrentSet[untrackedCopySegmentDrop](),
		droppingUntracked: typeutil.NewConcurrentSet[untrackedCopySegmentDrop](),
		dropCtx:           dropCtx,
		dropCancel:        dropCancel,
		closeChan:         make(chan struct{}),
	}
	if meta, ok := copyMeta.(*copySegmentMeta); ok {
		meta.setUntrackedDropHandler(inspector)
	}
	return inspector
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
		s.dropLifecycleMu.Lock()
		s.dropClosed = true
		s.dropCancel()
		close(s.closeChan)
		s.dropLifecycleMu.Unlock()
		if err := s.dropPool.ReleaseTimeout(30 * time.Second); err != nil {
			mlog.Warn(s.ctx, "timed out draining copy segment drop workers", mlog.Err(err))
		}
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
	s.processUntrackedDrops()

	// Retrieve all jobs (no filters)
	jobs := s.copyMeta.GetJobBy(s.ctx)
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].GetJobId() < jobs[j].GetJobId()
	})

	for _, job := range jobs {
		// Target-segment cleanup is a function of the JOB outcome, not the task
		// outcome: a restore is all-or-nothing, so once the job has failed every
		// target segment it produced must go, including those a sibling task
		// completed successfully before the failure.
		jobFailed := job.GetState() == datapb.CopySegmentJobState_CopySegmentJobFailed
		jobActive := isActiveCopyJobState(job.GetState())

		tasks := s.copyMeta.GetTasksByJobID(s.ctx, job.GetJobId())
		for _, task := range tasks {
			switch task.GetState() {
			case datapb.CopySegmentTaskState_CopySegmentTaskPending:
				// Never dispatch work for a job that is already over.
				// checkFailedJob converges Pending tasks to Failed, but it runs
				// on the checker's interval while the scheduler ticks every
				// 100ms, so without this gate a sibling's failure is followed by
				// full segment copies into object storage for a restore already
				// reported Failed — against a snapshot whose pin has been
				// released. It also matters after a datacoord restart, where the
				// inspector is the only enqueuer (reloadFromMeta re-enqueues
				// InProgress tasks only).
				if jobActive {
					s.processPending(task)
				}
			case datapb.CopySegmentTaskState_CopySegmentTaskCompleted:
				if jobFailed {
					s.processFailed(task)
				}
				s.processTerminal(task)
			case datapb.CopySegmentTaskState_CopySegmentTaskFailed:
				s.processFailed(task)
				s.processTerminal(task)
			}
		}
	}
}

// EnqueueUntrackedDrop records the exact worker-side dispatch that metadata
// could not safely represent. The first attempt is submitted immediately;
// subsequent failures remain in untrackedDrops and retry on every inspection
// round.
func (s *copySegmentInspector) EnqueueUntrackedDrop(nodeID, taskID, taskVersion int64) bool {
	drop := untrackedCopySegmentDrop{nodeID: nodeID, taskID: taskID, taskVersion: taskVersion}
	s.dropLifecycleMu.RLock()
	if s.dropClosed {
		s.dropLifecycleMu.RUnlock()
		return false
	}
	inserted := s.untrackedDrops.Insert(drop)
	s.dropLifecycleMu.RUnlock()
	if inserted {
		s.processUntrackedDrop(drop)
	}
	return true
}

// HasPendingUntrackedDrop reports whether an earlier dispatch of this task is
// still awaiting worker-side cleanup. Dispatch consults it before handing the
// task to a worker again: both dispatches write the same deterministic target
// object keys, so they must never overlap. Entries leave untrackedDrops only
// once the drop is acknowledged or the node is confirmed gone, so this
// converges without an extra timeout.
func (s *copySegmentInspector) HasPendingUntrackedDrop(taskID int64) bool {
	for _, drop := range s.untrackedDrops.Collect() {
		if drop.taskID == taskID {
			return true
		}
	}
	return false
}

func (s *copySegmentInspector) processUntrackedDrops() {
	for _, drop := range s.untrackedDrops.Collect() {
		s.processUntrackedDrop(drop)
	}
}

func (s *copySegmentInspector) processUntrackedDrop(drop untrackedCopySegmentDrop) {
	s.dropLifecycleMu.RLock()
	if s.dropClosed {
		s.dropLifecycleMu.RUnlock()
		return
	}
	if !s.droppingUntracked.Insert(drop) {
		s.dropLifecycleMu.RUnlock()
		return
	}

	future := s.dropPool.Submit(func() (struct{}, error) {
		defer s.droppingUntracked.Remove(drop)
		err := s.cluster.DropCopySegment(s.dropCtx, drop.nodeID, drop.taskID, drop.taskVersion, true)
		if err != nil && !errors.Is(err, merr.ErrNodeNotFound) {
			mlog.RatedWarn(s.dropCtx, 1, "failed to drop untracked copy segment task on datanode, retrying",
				mlog.FieldTaskID(drop.taskID), mlog.FieldNodeID(drop.nodeID),
				mlog.Int64("taskVersion", drop.taskVersion), mlog.Err(err))
			return struct{}{}, nil
		}

		s.untrackedDrops.Remove(drop)
		mlog.Info(s.dropCtx, "dropped untracked copy segment task on datanode",
			mlog.FieldTaskID(drop.taskID), mlog.FieldNodeID(drop.nodeID),
			mlog.Int64("taskVersion", drop.taskVersion))
		return struct{}{}, nil
	})
	s.dropLifecycleMu.RUnlock()

	// The pool is non-blocking. A saturated submission never runs the closure,
	// so release its in-flight marker and leave the pending entry for the next
	// inspection round.
	select {
	case <-future.Inner():
		if err := future.Err(); err != nil {
			s.droppingUntracked.Remove(drop)
			mlog.RatedWarn(s.ctx, 1, "copy segment drop pool is full, retry untracked task in next inspection round",
				mlog.FieldTaskID(drop.taskID), mlog.FieldNodeID(drop.nodeID), mlog.Err(err))
		}
	default:
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

// processFailed drops the target segments a copy task produced.
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
// Called for every task of a Failed job, not only for Failed tasks. A restore
// is all-or-nothing: when a collection splits across several copy tasks
// (MaxSegmentsPerCopyTask) and one of them fails, the siblings that already
// Completed may have had their target segments published by finishJob, so a
// restore reported as failed would otherwise leave partial data visible.
//
// Note this only marks the segments Dropped; it does not remove them from meta.
// checkGC's hasSegments guard uses meta.GetSegment, which deliberately includes
// unhealthy segments, so reclaiming a Failed copy job ultimately depends on the
// GLOBAL segment garbage collector physically deleting the dropped segments
// first. That coupling is intentional — the copy module must not delete segment
// meta out from under the segment GC — but it means job reclamation is not
// self-contained in this module.
//
// Error handling:
// - Logs warnings if drop fails but continues processing other segments
// - Failed drops will be retried on next inspection cycle
func (s *copySegmentInspector) processFailed(task CopySegmentTask) {
	// Drop target segments if copy failed
	for _, mapping := range task.GetIdMappings() {
		targetSegID := mapping.GetTargetSegmentId()
		segment := s.meta.GetSegment(s.ctx, targetSegID)
		if segment == nil || segment.GetState() == commonpb.SegmentState_Dropped {
			continue
		}

		op := UpdateStatusOperator(targetSegID, commonpb.SegmentState_Dropped)
		err := s.meta.UpdateSegmentsInfo(s.ctx, op)
		if err != nil {
			mlog.Warn(s.ctx, "failed to drop target segment after copy task failed",
				WrapCopySegmentTaskLog(task, mlog.Int64("segmentID", targetSegID), mlog.Err(err))...)
		} else {
			mlog.Info(s.ctx, "dropped target segment after copy task failed",
				WrapCopySegmentTaskLog(task, mlog.Int64("segmentID", targetSegID))...)
		}
	}
}

// processTerminal re-attempts the worker-side cleanup of a terminal
// (Completed/Failed) task that still carries a node assignment.
//
// Why this exists:
// The scheduler calls DropTaskOnWorker exactly once, right before removing the
// task from its running set (task/global_scheduler.go), so a drop that hits an
// ambiguous error — an RPC timeout during a DataNode rolling restart, or a
// failed etcd write of NullNodeID — is never retried by the scheduler. Keeping
// the assignment on such an error is deliberate (clearing it could orphan a
// live worker-side task), but without a retry the task is stranded: checkGC
// skips any task whose NodeID is set and therefore never reclaims the task or
// its parent job, past the retention period, forever. This is the "wait for
// inspector to unassign" that checkGC's own guard refers to.
//
// Idempotency:
// DropTaskOnWorker returns immediately when the assignment is already cleared,
// so re-running it every inspection round costs nothing once cleanup lands. It
// touches only NodeID — never state or reason — so retrying cannot disturb the
// terminal outcome the task already reported.
//
// Concurrency:
// The drop is dispatched to a bounded pool rather than run inline. DropCopySegment
// blocks for up to DataCoordCfg.RequestTimeoutSeconds (default 600s) and inspect()
// is a single goroutine walking every job, so an inline call would let one
// unresponsive DataNode stall dispatch and cleanup for every other job. Only one
// drop per task is in flight at a time, and pool submission is non-blocking, so
// neither repeated rounds nor a ninth stuck task can stall the inspection loop.
//
// Racing the scheduler's own drop is harmless: meta writes are serialized under
// the meta lock, the DropTask RPC is idempotent, and clearing an already-cleared
// NodeID is a no-op — so the inspector deliberately does not take the scheduler's
// per-task lock.
func (s *copySegmentInspector) processTerminal(task CopySegmentTask) {
	if task.GetNodeId() == NullNodeID {
		return
	}
	taskID := task.GetTaskId()
	s.dropLifecycleMu.RLock()
	if s.dropClosed {
		s.dropLifecycleMu.RUnlock()
		return
	}
	if !s.droppingTask.Insert(taskID) {
		s.dropLifecycleMu.RUnlock()
		// A drop from an earlier round is still waiting on this node.
		return
	}
	mlog.RatedInfo(s.ctx, 1, "retrying worker-side drop of terminal copy segment task",
		WrapCopySegmentTaskLog(task, mlog.FieldNodeID(task.GetNodeId()))...)
	future := s.dropPool.Submit(func() (struct{}, error) {
		defer s.droppingTask.Remove(taskID)
		nodeID := task.GetNodeId()
		taskVersion := task.GetTaskVersion()
		job := s.copyMeta.GetJob(s.dropCtx, task.GetJobId())
		abort := task.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskFailed ||
			job == nil || job.GetState() == datapb.CopySegmentJobState_CopySegmentJobFailed
		err := s.cluster.DropCopySegment(s.dropCtx, nodeID, taskID, taskVersion, abort)
		if err != nil && !errors.Is(err, merr.ErrNodeNotFound) {
			mlog.Warn(s.dropCtx, "failed to drop copy segment task on datanode",
				WrapCopySegmentTaskLog(task, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
			return struct{}{}, nil
		}

		// Close takes the write side before canceling and draining. A worker that
		// returns after leadership/lifecycle loss therefore cannot mutate task
		// metadata. The expected-node condition also rejects a stale clear after
		// reassignment.
		s.dropLifecycleMu.RLock()
		defer s.dropLifecycleMu.RUnlock()
		if s.dropClosed || s.dropCtx.Err() != nil {
			return struct{}{}, nil
		}
		if _, updateErr := s.copyMeta.ClearTaskNodeAssignment(s.dropCtx, taskID, nodeID); updateErr != nil {
			mlog.Warn(s.dropCtx, "failed to clear copy segment task node assignment after drop",
				WrapCopySegmentTaskLog(task, mlog.FieldNodeID(nodeID), mlog.Err(updateErr))...)
		}
		return struct{}{}, nil
	})
	s.dropLifecycleMu.RUnlock()
	// A non-blocking ants pool reports saturation by returning an already
	// completed future with an error. The closure did not run in that case, so
	// release the in-flight marker here; the next inspection round retries it.
	select {
	case <-future.Inner():
		if err := future.Err(); err != nil {
			s.droppingTask.Remove(taskID)
			mlog.RatedWarn(s.ctx, 1, "copy segment drop pool is full, retry in next inspection round",
				WrapCopySegmentTaskLog(task, mlog.FieldNodeID(task.GetNodeId()), mlog.Err(err))...)
		}
	default:
	}
}
