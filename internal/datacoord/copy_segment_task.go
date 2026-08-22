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
	"net/url"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore/model"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Copy Segment Task Management
//
// This file implements the task layer for copy segment operations during snapshot restore.
// It manages the DataCoord-side lifecycle of copy tasks and coordinates with DataNodes for
// execution.
//
// TASK LIFECYCLE:
// 1. Pending: Task created by checker, waiting in inspector queue
// 2. InProgress: Task dispatched to DataNode via CreateTaskOnWorker
// 3. Executing: DataNode performs file copying (queried via QueryTaskOnWorker)
// 4. Completed/Failed: Final state reported by DataNode
// 5. Cleanup: Task dropped from DataNode via DropTaskOnWorker
//
// TASK RESPONSIBILITIES:
// - CreateTaskOnWorker: Assemble request from snapshot data and dispatch to DataNode
// - QueryTaskOnWorker: Poll DataNode for task status and sync results
// - DropTaskOnWorker: Clean up task resources on DataNode
// - SyncCopySegmentTask: Update segment binlogs and indexes after successful copy
//
// DATA FLOW:
// 1. Read snapshot data from S3 (contains source segment binlogs)
// 2. Build CopySegmentRequest with source/target segment mappings
// 3. DataNode copies files and generates new binlog paths
// 4. Sync binlogs, indexes (vector/scalar/text/JSON) to segment metadata
// 5. Mark segments as Flushed for query availability
//
// FAILURE HANDLING:
// - Task failure immediately marks job as failed (fail-fast)
// - Failed segments are dropped by inspector
// - Metrics recorded for pending and executing duration

// ===========================================================================================
// Task Filters and Update Actions
// ===========================================================================================

// CopySegmentTaskFilter defines a predicate function for filtering copy segment tasks.
type CopySegmentTaskFilter func(task CopySegmentTask) bool

// WithCopyTaskJob creates a filter that matches tasks belonging to a specific job.
//
// Use case: Retrieving all tasks for a job to check progress or handle failures
func WithCopyTaskJob(jobID int64) CopySegmentTaskFilter {
	return func(task CopySegmentTask) bool {
		return task.GetJobId() == jobID
	}
}

// WithCopyTaskStates creates a filter that matches tasks in any of the provided states.
//
// Use case: Finding all pending tasks for scheduling, or failed tasks for cleanup
func WithCopyTaskStates(states ...datapb.CopySegmentTaskState) CopySegmentTaskFilter {
	return func(task CopySegmentTask) bool {
		for _, state := range states {
			if task.GetState() == state {
				return true
			}
		}
		return false
	}
}

// UpdateCopySegmentTaskAction defines a functional update operation on a task.
type UpdateCopySegmentTaskAction func(task CopySegmentTask)

// UpdateCopyTaskState creates an action that updates the task state.
//
// State transitions:
//   - Pending → InProgress (when dispatched to DataNode)
//   - InProgress → Completed/Failed (when DataNode reports result)
func UpdateCopyTaskState(state datapb.CopySegmentTaskState) UpdateCopySegmentTaskAction {
	return func(t CopySegmentTask) {
		t.(*copySegmentTask).task.Load().State = state
	}
}

// UpdateCopyTaskReason creates an action that updates the task failure reason.
//
// Use case: Recording error message when task fails
func UpdateCopyTaskReason(reason string) UpdateCopySegmentTaskAction {
	return func(t CopySegmentTask) {
		t.(*copySegmentTask).task.Load().Reason = reason
	}
}

// UpdateCopyTaskNodeID creates an action that updates the assigned DataNode ID.
//
// Use case: Recording which DataNode is executing the task
func UpdateCopyTaskNodeID(nodeID int64) UpdateCopySegmentTaskAction {
	return func(t CopySegmentTask) {
		t.(*copySegmentTask).task.Load().NodeId = nodeID
	}
}

// isNodeAssigned reports whether the task still names a worker that
// may be holding it.
//
// Two values mean "nobody": NullNodeID, which the checker writes when it builds
// a task and DropTaskOnWorker writes when it releases one, and 0, the proto
// default carried by any record persisted without an explicit assignment. Node
// IDs are allocated from the session registry and are always positive, so
// neither can name a real DataNode. Treating only NullNodeID as unassigned
// would leave a 0-valued record permanently un-droppable and, because
// copySegmentChecker.checkGC keys off the same question, permanently
// un-removable.
func isNodeAssigned(nodeID int64) bool {
	return nodeID != NullNodeID && nodeID != 0
}

// UpdateCopyTaskCompleteTs creates an action that updates the task completion timestamp.
//
// Use case: Recording when the task finished for metrics and debugging
func UpdateCopyTaskCompleteTs(completeTs uint64) UpdateCopySegmentTaskAction {
	return func(t CopySegmentTask) {
		t.(*copySegmentTask).task.Load().CompleteTs = completeTs
	}
}

// ===========================================================================================
// Task Interface and Implementation
// ===========================================================================================

// CopySegmentTask defines the interface for copy segment task operations.
//
// Extends task.Task interface with copy-segment-specific methods.
type CopySegmentTask interface {
	task.Task
	GetTaskId() int64
	GetJobId() int64
	GetCollectionId() int64
	GetNodeId() int64
	GetState() datapb.CopySegmentTaskState
	GetReason() string
	GetPredecessorTaskId() int64
	GetIdMappings() []*datapb.CopySegmentIDMapping // Lightweight ID mappings
	GetTR() *timerecord.TimeRecorder
	Clone() CopySegmentTask
}

// copySegmentTask implements CopySegmentTask with atomic state updates.
type copySegmentTask struct {
	task atomic.Pointer[datapb.CopySegmentTask] // Atomic pointer for concurrent access
	// replanCommitted is an in-process scheduler fence. Once a replacement is
	// durable, this attempt must leave the scheduler even if retiring its target
	// segments or task record fails. The protobuf state deliberately stays
	// InProgress so the job checker does not mistake a superseded attempt for a
	// failed restore; recovery derives the same fence from predecessor_task_id
	// before it reloads either task.
	replanCommitted atomic.Bool

	ctx          context.Context
	copyMeta     CopySegmentMeta          // For accessing job metadata and updating task state
	meta         *meta                    // For accessing segment metadata and collection schema
	snapshotMeta *snapshotMeta            // For accessing snapshot data (source binlogs)
	alloc        allocator.Allocator      // For allocating new build IDs to avoid buildID reuse
	tr           *timerecord.TimeRecorder // For measuring task duration (pending, executing, total)
	times        *taskcommon.Times        // For tracking task lifecycle timestamps
}

type copySegmentSnapshotCache struct {
	mu   sync.Mutex
	data *snapshotstorage.SnapshotData
}

func (c *copySegmentSnapshotCache) load(
	loader func() (*snapshotstorage.SnapshotData, error),
) (*snapshotstorage.SnapshotData, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.data != nil {
		return c.data, nil
	}
	data, err := loader()
	if err != nil {
		return nil, err
	}
	c.data = data
	return data, nil
}

// ===========================================================================================
// Task Getters
// ===========================================================================================

// GetTaskId returns the unique task identifier.
func (t *copySegmentTask) GetTaskId() int64 {
	return t.task.Load().GetTaskId()
}

// GetJobId returns the parent job identifier.
func (t *copySegmentTask) GetJobId() int64 {
	return t.task.Load().GetJobId()
}

// GetCollectionId returns the target collection identifier.
func (t *copySegmentTask) GetCollectionId() int64 {
	return t.task.Load().GetCollectionId()
}

// GetNodeId returns the assigned DataNode identifier (0 if not yet assigned).
func (t *copySegmentTask) GetNodeId() int64 {
	return t.task.Load().GetNodeId()
}

// GetState returns the current task state.
func (t *copySegmentTask) GetState() datapb.CopySegmentTaskState {
	return t.task.Load().GetState()
}

// GetReason returns the failure reason (empty if task succeeded).
func (t *copySegmentTask) GetReason() string {
	return t.task.Load().GetReason()
}

func (t *copySegmentTask) GetPredecessorTaskId() int64 {
	return t.task.Load().GetPredecessorTaskId()
}

// GetIdMappings returns the source-to-target segment ID mappings.
//
// Each mapping contains:
//   - SourceSegmentId: Segment ID in snapshot
//   - TargetSegmentId: Newly allocated segment ID in target collection
//   - PartitionId: Target partition ID
func (t *copySegmentTask) GetIdMappings() []*datapb.CopySegmentIDMapping {
	return t.task.Load().GetIdMappings()
}

// GetTR returns the time recorder for measuring task duration.
func (t *copySegmentTask) GetTR() *timerecord.TimeRecorder {
	return t.tr
}

// Clone creates a deep copy of the task for safe concurrent modification.
//
// Why needed:
// - UpdateTask clones before applying actions to avoid race conditions
// - Original task remains accessible to other goroutines during update
//
// The protobuf payload must be deep-copied (proto.Clone): update actions
// mutate the proto in place, so sharing the pointer would leak mutations
// into the cached task before the catalog save succeeds — a failed save
// would leave memory and etcd out of sync. tr and times are shared pointers
// by design: taskcommon.Times is mutex-guarded, and the update path only
// touches the proto payload, never the timers.
func (t *copySegmentTask) Clone() CopySegmentTask {
	cloned := &copySegmentTask{
		ctx:          t.ctx,
		copyMeta:     t.copyMeta,
		meta:         t.meta,
		snapshotMeta: t.snapshotMeta,
		alloc:        t.alloc,
		tr:           t.tr,
		times:        t.times,
	}
	cloned.task.Store(proto.Clone(t.task.Load()).(*datapb.CopySegmentTask))
	cloned.replanCommitted.Store(t.replanCommitted.Load())
	return cloned
}

// ===========================================================================================
// task.Task Interface Implementation
// ===========================================================================================

// GetTaskID implements task.Task interface.
func (t *copySegmentTask) GetTaskID() int64 {
	return t.GetTaskId()
}

// GetTaskType returns the task type for scheduler categorization.
func (t *copySegmentTask) GetTaskType() taskcommon.Type {
	return taskcommon.CopySegment
}

// GetTaskState returns the generic task state for scheduler.
func (t *copySegmentTask) GetTaskState() taskcommon.State {
	if t.replanCommitted.Load() {
		return taskcommon.Failed
	}
	return taskcommon.FromCopySegmentState(t.GetState())
}

// GetTaskSlot returns the number of task slots this task consumes.
//
// Used for resource quota enforcement across different task types.
func (t *copySegmentTask) GetTaskSlot() int64 {
	return t.task.Load().GetTaskSlot()
}

// SetTaskTime records a task lifecycle timestamp.
func (t *copySegmentTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	t.times.SetTaskTime(timeType, time)
}

// GetTaskTime retrieves a task lifecycle timestamp.
func (t *copySegmentTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(t.times)
}

// GetTaskVersion returns the persisted attempt counter. It is incremented by
// the replan (each replacement inherits predecessor+1) and read by the
// scheduler and the attempt cap; it is not an optimistic-concurrency token.
func (t *copySegmentTask) GetTaskVersion() int64 {
	return t.task.Load().GetTaskVersion()
}

// ===========================================================================================
// Task Lifecycle: Dispatch to DataNode
// ===========================================================================================

// CreateTaskOnWorker dispatches the task to a DataNode for execution.
//
// Process flow:
//  1. Retrieve parent job metadata
//  2. Read snapshot data from S3 to get source segment binlogs
//  3. Build source-target segment mappings from task's ID mappings
//  4. Assemble CopySegmentRequest with full binlog information
//  5. Persist InProgress and the assigned node ID
//  6. Send request to DataNode via cluster.CreateCopySegment
//  7. Record pending duration metric
//
// Parameters:
//   - nodeID: ID of DataNode selected by scheduler
//   - cluster: Cluster session manager for RPC communication
//
// Error handling:
//   - Permanent snapshot assembly errors mark the task and job failed
//   - An external task rejected as unsupported marks the task and job failed
//   - Transient assembly errors leave the task Pending for scheduler retry
//   - Any other Create RPC error abandons this identity and replans with fresh
//     task and target segment IDs because the worker outcome is unknown
//
// Why load the snapshot during dispatch:
// - Snapshot data contains full binlog paths needed for copy
// - The first task for a job reads it from storage to populate CopySegmentRequest
// - Tasks in the same job share a cache to avoid redundant remote reads
func (t *copySegmentTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	ctx := t.ctx
	mlog.Info(ctx, "processing pending copy segment task...", WrapCopySegmentTaskLog(t)...)
	job := t.copyMeta.GetJob(ctx, t.GetJobId())
	req, err := AssembleCopySegmentRequest(t, job)
	if err != nil {
		mlog.Warn(ctx, "failed to assemble copy segment request",
			WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
		if isPermanentSnapshotError(err) {
			t.markTaskAndJobFailed(merr.Wrap(err, "failed to assemble copy segment request").Error())
		}
		return
	}
	// Persist the assignment before crossing the at-least-once Create boundary.
	// Otherwise an accepted request followed by a failed state write leaves this
	// identity Pending and allows the scheduler to send the same target IDs to a
	// second node. The CAS guards the reverse race: a concurrent failure path
	// (checkFailedJob) may have marked this task Failed under a terminal job,
	// and dispatching it now would resurrect the work.
	assigned, err := t.copyMeta.UpdateTaskInState(ctx, t.GetTaskId(),
		datapb.CopySegmentTaskState_CopySegmentTaskPending,
		UpdateCopyTaskNodeID(nodeID),
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskInProgress))
	if err != nil {
		mlog.Warn(ctx, "failed to persist copy segment assignment, not sending task",
			WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
		return
	}
	if !assigned {
		mlog.Warn(ctx, "copy segment task left Pending before dispatch; skipping",
			WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID))...)
		return
	}

	err = cluster.CreateCopySegment(nodeID, req, t.GetCollectionId(), job.GetExternal())
	if err != nil {
		if job.GetExternal() && errors.Is(err, merr.ErrServiceUnimplemented) {
			// A confirmed unsupported request is permanent, not an unknown
			// outcome: fail rather than replan.
			t.markTaskAndJobFailed(merr.Wrap(err,
				"datanode does not support external copy segment tasks").Error())
			return
		}
		// Any other Create error does not say whether the worker accepted the
		// request. Never dispatch this identity again: rebuild the work under
		// fresh task and target segment IDs, matching compaction/external refresh
		// semantics.
		mlog.Warn(ctx, "copy segment create outcome is unknown, abandoning attempt for replan",
			WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
		t.abandonAttempt(ctx, fmt.Sprintf("create on node %d returned no success: %v", nodeID, err))
		return
	}
	mlog.Info(ctx, "create copy segment task on datanode done",
		WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID))...)
	// Record pending duration
	pendingDuration := t.GetTR().RecordSpan()
	metrics.CopySegmentTaskLatency.WithLabelValues(metrics.Pending).Observe(float64(pendingDuration.Milliseconds()))
	mlog.Info(ctx, "copy segment task start to execute",
		WrapCopySegmentTaskLog(t, mlog.Int64("scheduledNodeID", nodeID),
			mlog.Duration("taskTimeCost/pending", pendingDuration))...)
}

// ===========================================================================================
// Task Lifecycle: Query DataNode Status
// ===========================================================================================

// markTaskAndJobFailed marks both task and job as failed with the given reason.
// This implements fail-fast design: user should know immediately if restore is failing.
// A failed task write leaves the task InProgress, so the next query round walks
// this path again; a failed job write leaves the job Executing with the task
// Failed, which tryTimeoutJob eventually settles.
func (t *copySegmentTask) markTaskAndJobFailed(reason string) {
	ctx := t.ctx
	updateErr := t.copyMeta.UpdateTask(ctx, t.GetTaskId(),
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed),
		UpdateCopyTaskReason(reason))
	if updateErr != nil {
		mlog.Warn(ctx, "failed to update copy segment task state to failed",
			WrapCopySegmentTaskLog(t, mlog.Err(updateErr))...)
		return
	}

	// Sync job state immediately (fail-fast)
	job := t.copyMeta.GetJob(ctx, t.GetJobId())
	if job != nil && job.GetState() != datapb.CopySegmentJobState_CopySegmentJobFailed {
		updateErr = t.copyMeta.UpdateJobStateAndReleaseRef(ctx, t.GetJobId(),
			UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
			UpdateCopyJobReason(reason))
		if updateErr != nil {
			mlog.Warn(ctx, "failed to update job state to Failed",
				mlog.FieldJobID(t.GetJobId()), mlog.Err(updateErr))
		}
	}
	mlog.Warn(ctx, "copy segment task failed",
		WrapCopySegmentTaskLog(t, mlog.String("reason", reason))...)
}

// replanUnderFreshIdentity rebuilds this task's remaining work as a NEW task:
// same job, same source segments, a fresh task ID and a fresh set of target
// segment IDs.
//
// Why a fresh identity rather than another attempt at the same one. An abandoned
// attempt is not necessarily a stopped attempt -- the worker may still be
// copying -- and every target object key is a pure function of the target
// segment ID. Re-dispatching under the same IDs therefore points two live
// attempts at the same keys, and nothing downstream can tell their output apart:
// not the DataNode task manager, which fences by task ID; not
// validateCopySegmentResults, which fences by target segment ID; not a cleanup
// pass, which would be deleting keys the survivor owns. Giving the retry its own
// IDs makes the collision impossible by construction, and whatever the old
// attempt still writes is unreferenced and reclaimed by GC. Index output already
// worked this way -- a fresh buildID per dispatch -- so this closes the half of
// the rule that binlog output was missing.
//
// Ordering. The replacement record and its new targets are published through
// one composite metadata operation, with the task record first in the ordered
// over-limit fallback. Memory sees them only after the whole operation
// succeeds. Only then are the old targets made reclaimable and the predecessor
// record retired. A crash anywhere in the handoff therefore leaves an exact
// durable owner whose predecessor_task_id lets recovery resume without overlap
// heuristics (see reconcileReplannedTasks).
func (t *copySegmentTask) replanUnderFreshIdentity(ctx context.Context) (CopySegmentTask, error) {
	old := t.GetTaskId()
	mappings := t.GetIdMappings()
	if len(mappings) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("copy segment task %d has no ID mappings to replan", old)
	}

	// Source-side successor uniqueness: if an earlier run of this very replan
	// already persisted a successor -- its composite write may have landed
	// while the response was lost, leaving this process's cache unaware --
	// adopt it instead of minting a second identity. Two successors would race
	// for the same work and force the reconciler into an election. The
	// publication below is idempotent, so it simply fills any targets the
	// adopted successor is still missing.
	//
	// The adopted path returns without retiring this record or dropping its
	// targets: reconcileReplannedTasks owns that pair cleanup and runs it on
	// the next inspection round.
	if existing := t.copyMeta.GetReplacementByPredecessor(ctx, old); existing != nil {
		if err := publishCopySegmentReplacement(ctx, t, existing, t.meta, t.copyMeta); err != nil {
			return nil, err
		}
		t.replanCommitted.Store(true)
		mlog.Info(ctx, "copy segment replan adopts its already-published successor",
			WrapCopySegmentTaskLog(t,
				mlog.Int64("existingTaskID", existing.GetTaskId()),
				mlog.Int64("attempt", existing.GetTaskVersion()))...)
		return existing, nil
	}

	newTaskID, err := t.alloc.AllocID(ctx)
	if err != nil {
		return nil, merr.Wrap(err, "failed to allocate a copy segment replan task ID")
	}
	segmentIDStart, _, err := t.alloc.AllocN(int64(len(mappings)))
	if err != nil {
		return nil, merr.Wrap(err, "failed to allocate copy segment replan target IDs")
	}

	// Validate the predecessor inventory and build the new identity first. The
	// task and actual clones are published together below.
	newMappings := make([]*datapb.CopySegmentIDMapping, 0, len(mappings))
	for i, mapping := range mappings {
		oldSegmentID := mapping.GetTargetSegmentId()
		if t.meta.GetSegment(ctx, oldSegmentID) == nil {
			return nil, merr.WrapErrSegmentNotFound(oldSegmentID, "copy segment replan target is gone")
		}
		newMappings = append(newMappings, &datapb.CopySegmentIDMapping{
			SourceSegmentId: mapping.GetSourceSegmentId(),
			TargetSegmentId: segmentIDStart + int64(i),
			PartitionId:     mapping.GetPartitionId(),
		})
	}

	replanned := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("copy segment task"),
		times: taskcommon.NewTimes(),
	}
	replanned.task.Store(&datapb.CopySegmentTask{
		TaskId:            newTaskID,
		JobId:             t.GetJobId(),
		CollectionId:      t.GetCollectionId(),
		NodeId:            NullNodeID,
		TaskVersion:       t.GetTaskVersion() + 1,
		TaskSlot:          t.GetTaskSlot(),
		State:             datapb.CopySegmentTaskState_CopySegmentTaskPending,
		IdMappings:        newMappings,
		CreatedTs:         uint64(time.Now().UnixNano()),
		PredecessorTaskId: old,
	})
	if err := publishCopySegmentReplacement(ctx, t, replanned, t.meta, t.copyMeta); err != nil {
		return nil, err
	}
	// The replacement owns the work from this point. Make the scheduler retire
	// the old attempt after QueryTaskOnWorker returns, even if one of the cleanup
	// writes below fails. GetState intentionally remains InProgress so the job
	// checker does not fail a job whose replacement is healthy.
	t.replanCommitted.Store(true)

	// The old record remains the durable cleanup inventory until every abandoned
	// target is Dropped. Failed writes leave the exact predecessor/replacement
	// pair in meta for the inspector to resume.
	// Clearing is_importing keeps the abandoned targets reclaimable: the
	// garbage collector skips is_importing segments to protect the in-flight
	// commit marker, so a target left is_importing after being dropped would
	// never be collected. Copy targets are never that marker -- it exists only
	// for import sort-skip origins.
	ops := make([]UpdateOperator, 0, len(mappings)*2)
	for _, mapping := range mappings {
		segmentID := mapping.GetTargetSegmentId()
		ops = append(ops,
			UpdateStatusOperator(segmentID, commonpb.SegmentState_Dropped),
			UpdateIsImporting(segmentID, false))
	}
	if err := t.meta.UpdateSegmentsInfo(ctx, ops...); err != nil {
		return nil, merr.Wrap(err, "failed to drop the abandoned copy segment targets")
	}

	if err := t.copyMeta.RemoveTask(ctx, old); err != nil {
		// The old record is still persisted AND still in the meta cache, sharing
		// the very object the caller is about to retire in memory. Reporting
		// success here would let that retirement be published as a task failure
		// the checker acts on -- failing a job whose replacement is healthy. Fail
		// the replan instead: both records now claim the work, and
		// reconcileReplannedTasks settles the pair by attempt number on the next
		// inspection round.
		return nil, merr.Wrapf(err, "failed to retire copy segment task %d after replanning it as %d", old, newTaskID)
	}

	mlog.Info(ctx, "copy segment task replanned under a fresh identity",
		WrapCopySegmentTaskLog(t,
			mlog.Int64("replanTaskID", newTaskID),
			mlog.Int64("attempt", replanned.GetTaskVersion()))...)
	return replanned, nil
}

// publishCopySegmentReplacement atomically publishes the replacement owner and
// its target inventory. It is intentionally idempotent so recovery can finish
// an ordered over-limit write that stopped after the owner record or retry an
// ambiguous catalog result without exposing an incomplete task to scheduling.
func publishCopySegmentReplacement(
	ctx context.Context,
	predecessor CopySegmentTask,
	replacement CopySegmentTask,
	meta *meta,
	copyMeta CopySegmentMeta,
) error {
	if replacement.GetPredecessorTaskId() != predecessor.GetTaskId() {
		return merr.WrapErrServiceInternalMsg(
			"copy segment replan %d names predecessor %d, expected %d",
			replacement.GetTaskId(), replacement.GetPredecessorTaskId(), predecessor.GetTaskId())
	}

	predecessorMappings := make(map[[2]int64]*datapb.CopySegmentIDMapping, len(predecessor.GetIdMappings()))
	for _, mapping := range predecessor.GetIdMappings() {
		key := [2]int64{mapping.GetSourceSegmentId(), mapping.GetPartitionId()}
		if _, duplicate := predecessorMappings[key]; duplicate {
			return merr.WrapErrServiceInternalMsg(
				"copy segment predecessor %d has duplicate source/partition mapping %v",
				predecessor.GetTaskId(), key)
		}
		predecessorMappings[key] = mapping
	}
	if len(predecessorMappings) != len(replacement.GetIdMappings()) {
		return merr.WrapErrServiceInternalMsg(
			"copy segment replan %d mapping count %d differs from predecessor %d count %d",
			replacement.GetTaskId(), len(replacement.GetIdMappings()),
			predecessor.GetTaskId(), len(predecessorMappings))
	}

	seenTargets := typeutil.NewUniqueSet()
	targets := make([]*SegmentInfo, 0, len(replacement.GetIdMappings()))
	for _, mapping := range replacement.GetIdMappings() {
		key := [2]int64{mapping.GetSourceSegmentId(), mapping.GetPartitionId()}
		oldMapping, ok := predecessorMappings[key]
		if !ok {
			return merr.WrapErrServiceInternalMsg(
				"copy segment replan %d contains source/partition mapping %v absent from predecessor %d",
				replacement.GetTaskId(), key, predecessor.GetTaskId())
		}
		newTargetID := mapping.GetTargetSegmentId()
		if seenTargets.Contain(newTargetID) {
			return merr.WrapErrServiceInternalMsg(
				"copy segment replan %d contains duplicate target segment %d",
				replacement.GetTaskId(), newTargetID)
		}
		seenTargets.Insert(newTargetID)
		oldTargetID := oldMapping.GetTargetSegmentId()
		oldTarget := meta.GetSegment(ctx, oldTargetID)
		if oldTarget == nil {
			return merr.WrapErrSegmentNotFound(oldTargetID, "copy segment predecessor target is gone during replan publication")
		}
		newTarget := copySegmentReplanTarget(oldTarget, newTargetID)
		targets = append(targets, newTarget)
	}

	if err := copyMeta.PublishReplan(ctx, replacement, targets); err != nil {
		return merr.Wrapf(err, "failed to publish copy segment replan %d", replacement.GetTaskId())
	}
	return nil
}

// copySegmentReplanTarget reconstructs the pre-execution target template under
// a fresh segment ID. The predecessor may already be Dropped when recovery
// resumes, or a failed metadata sync may have attached worker output before the
// attempt was abandoned. Neither state nor execution-produced artifacts belong
// to the replacement identity.
func copySegmentReplanTarget(predecessor *SegmentInfo, targetID int64) *SegmentInfo {
	info := proto.Clone(predecessor.SegmentInfo).(*datapb.SegmentInfo)
	info.ID = targetID
	info.State = commonpb.SegmentState_Importing
	info.DroppedAt = 0
	info.IsImporting = true
	info.Binlogs = nil
	info.Statslogs = nil
	info.Deltalogs = nil
	info.Bm25Statslogs = nil
	info.TextStatsLogs = nil
	info.JsonKeyStats = nil
	info.ManifestPath = ""
	info.ChildManifestPaths = nil
	info.Stats = nil
	return NewSegmentInfo(info)
}

// abandonAttempt gives this attempt's work to a replacement, or settles the job
// when the attempt cap is spent. The caller must treat the receiver as retired
// afterwards.
func (t *copySegmentTask) abandonAttempt(ctx context.Context, reason string) {
	// A dead or missing job cannot be replanned for. This poll can arrive
	// arbitrarily late -- it was in flight while the job failed, timed out, or
	// (with a short retention configured) was already GC'd -- and publishing a
	// fresh replacement then either churns (the failed-job checker re-fails it
	// next tick) or, with the job gone, creates an orphan record no job-driven
	// loop will ever inspect or GC. Retire the attempt in memory only; the
	// scheduler's terminal release still drops it on its worker.
	if job := t.copyMeta.GetJob(ctx, t.GetJobId()); job == nil ||
		job.GetState() == datapb.CopySegmentJobState_CopySegmentJobFailed ||
		job.GetState() == datapb.CopySegmentJobState_CopySegmentJobCompleted {
		mlog.Info(ctx, "not replanning a copy segment attempt for a terminal or missing job",
			WrapCopySegmentTaskLog(t, mlog.String("reason", reason))...)
		retired := t.Clone().(*copySegmentTask).task.Load()
		retired.State = datapb.CopySegmentTaskState_CopySegmentTaskFailed
		retired.Reason = reason
		t.task.Store(retired)
		return
	}

	maxAttempts := Params.DataCoordCfg.CopySegmentMaxAttempts.GetAsInt64()
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	if t.GetTaskVersion()+1 >= maxAttempts {
		t.markTaskAndJobFailed(fmt.Sprintf("%s, and the copy segment attempt cap (%d) is spent", reason, maxAttempts))
		return
	}
	if _, err := t.replanUnderFreshIdentity(ctx); err != nil {
		// The old record still owns the handoff. A replacement may already be
		// durable, but reconcileReplannedTasks recognizes the pair and finishes
		// cleanup instead of dispatching both attempts.
		mlog.Warn(ctx, "failed to replan the copy segment task, retrying on a later round",
			WrapCopySegmentTaskLog(t, mlog.Err(err))...)
		return
	}
	// In-memory only. After a fresh replan the old record is gone from the
	// cache, so the checker never sees this state and cannot mistake it for a
	// job failure; after adopting an already-published successor the old
	// record does remain in the cache, but reconcileReplannedTasks retires it
	// before it can be re-dispatched, so the checker never sees it either. The
	// state exists so the scheduler retires the task through its terminal
	// path, which sends the worker drop after releasing the per-task lock.
	retired := t.Clone().(*copySegmentTask).task.Load()
	retired.State = datapb.CopySegmentTaskState_CopySegmentTaskFailed
	retired.Reason = reason
	t.task.Store(retired)
}

// QueryTaskOnWorker polls the DataNode for task execution status.
//
// Process flow:
//  1. Send QueryCopySegmentRequest to assigned DataNode
//  2. Check response state:
//     - In progress or other non-terminal states: keep polling later
//     - Failed: Mark task/job as failed (fail-fast)
//     - Completed: Sync binlog and index metadata to segment
//  3. Update task state accordingly
//
// Failure handling:
//   - Any query RPC error ends the attempt: a round that produces no answer
//     tells us nothing about the worker, so the work is rebuilt as a new task
//     under a fresh task ID and fresh target segment IDs (see
//     replanUnderFreshIdentity). A worker still copying under the old IDs cannot
//     collide with the replacement, and what it writes is unreferenced.
//   - Worker failure responses trigger immediate failure
//   - Task failure immediately marks parent job as failed (fail-fast)
//   - Enables quick feedback to user without waiting for timeout
//
// Success handling:
// - Calls SyncCopySegmentTask to update segment metadata
// - Updates binlogs, indexes (vector/scalar/text/JSON)
// - Marks segments as Flushed for query availability
// - Records executing and total duration metrics
//
// Why fail-fast design:
// - User should know immediately if restore is failing
// - No point continuing if one task fails (data integrity)
// - Saves resources by stopping early
func (t *copySegmentTask) QueryTaskOnWorker(cluster session.Cluster) {
	nodeID := t.GetNodeId()
	if !isNodeAssigned(nodeID) {
		// The assignment was released underneath this poll -- the inspector
		// retried the drop for a task that went terminal after the scheduler
		// snapshotted it. There is no worker left to ask, and querying node -1
		// would come back as an unanswered round and trigger a pointless replan.
		mlog.Info(t.ctx, "copy segment task has no worker assignment, skip query",
			WrapCopySegmentTaskLog(t)...)
		return
	}
	req := &datapb.QueryCopySegmentRequest{
		TaskID: t.GetTaskId(),
	}
	resp, err := cluster.QueryCopySegment(nodeID, req)
	// Handle RPC error separately to avoid nil resp dereference.
	if err != nil {
		mlog.Info(t.ctx, "copy segment query left the attempt unanswered, replanning",
			WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
		// No worker drop is sent from here. This runs under the scheduler's
		// per-task lock, and the drop is an RPC bounded only by
		// dataCoord.requestTimeoutSeconds -- against the very node that just
		// failed to answer, it would hold the lock for a second full timeout.
		// abandonAttempt leaves the task in a terminal state instead, so the
		// scheduler sends the drop after releasing the lock.
		t.abandonAttempt(t.ctx, fmt.Sprintf("query on node %d returned no answer: %v", nodeID, err))
		return
	}

	// A transient fault on the worker (object storage throttling, a timeout) is
	// not a reason to fail the restore. Rebuild the work under a fresh task and
	// fresh target segment IDs, exactly as an unanswered round does -- the old
	// attempt's partial output stays unreferenced and the attempt cap bounds how
	// many times this can happen.
	if resp.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskRetry {
		mlog.Info(t.ctx, "copy segment task hit a retriable failure on its worker, replanning",
			WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID),
				mlog.String("reason", resp.GetReason()))...)
		t.abandonAttempt(t.ctx, fmt.Sprintf("retriable failure on node %d: %s", nodeID, resp.GetReason()))
		return
	}

	// Handle permanent task execution failure (resp is guaranteed non-nil here)
	if resp.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskFailed {
		t.markTaskAndJobFailed(resp.GetReason())
		return
	}

	if resp.GetState() != datapb.CopySegmentTaskState_CopySegmentTaskCompleted {
		return
	}

	// Sync task state and binlog info
	err = SyncCopySegmentTask(t.ctx, t, resp, t.copyMeta, t.meta)
	if err != nil {
		t.markTaskAndJobFailed(fmt.Sprintf("failed to sync segment metadata: %v", err))
		return
	}

	mlog.Info(t.ctx, "query copy segment task",
		WrapCopySegmentTaskLog(t, mlog.String("respState", resp.GetState().String()),
			mlog.String("reason", resp.GetReason()))...)
}

// ===========================================================================================
// Task Lifecycle: Cleanup on DataNode
// ===========================================================================================

// DropTaskOnWorker removes task resources from the DataNode.
//
// Process flow:
//  1. Send DropCopySegment RPC to assigned DataNode
//  2. DataNode cleans up task state and temporary files
//  3. Log success or failure
//
// When called:
// - After task completes successfully (cleanup)
// - After task fails and is marked for deletion (cleanup)
// - During garbage collection of old tasks
//
// Releasing the assignment is part of the drop, not an afterthought. The GC in
// copySegmentChecker.checkGC refuses to remove a task that still names a node,
// on the assumption that a worker may still be holding it. Nothing else ever
// clears node_id, so a drop that reports success but leaves the field set
// strands the task record -- and with it its job -- in meta for good. Persisting
// NullNodeID here is what closes that loop.
//
// A node that is no longer registered took its copy of the task with it, so
// ErrNodeNotFound releases the assignment exactly like a successful drop. Any
// other error keeps node_id, which is what lets the GC loop retry the drop on
// a later round (see copySegmentChecker.checkGC).
func (t *copySegmentTask) DropTaskOnWorker(cluster session.Cluster) {
	nodeID := t.GetNodeId()
	if !isNodeAssigned(nodeID) {
		return
	}
	err := cluster.DropCopySegment(nodeID, t.GetTaskId())
	if err != nil && !errors.Is(err, merr.ErrNodeNotFound) {
		// Rated: the GC loop retries this drop every tick until it lands, so
		// against a node that refuses fast this line otherwise repeats every
		// couple of seconds for the whole outage.
		mlog.RatedWarn(t.ctx, rate.Limit(1.0/60), "failed to drop copy segment task on datanode",
			WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
		return
	}
	// The record may already be gone -- a replan retires the predecessor before
	// the scheduler sends this drop -- in which case UpdateTask is a no-op and
	// there is no assignment left to release.
	if updateErr := t.copyMeta.UpdateTask(t.ctx, t.GetTaskId(),
		UpdateCopyTaskNodeID(NullNodeID)); updateErr != nil {
		mlog.Warn(t.ctx, "dropped copy segment task on datanode but failed to release the assignment",
			WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(updateErr))...)
		return
	}
	mlog.Info(t.ctx, "drop copy segment task on datanode done",
		WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID))...)
}

// ===========================================================================================
// Helper Functions
// ===========================================================================================

// WrapCopySegmentTaskLog creates structured log fields for copy segment tasks.
//
// Standard fields included:
//   - taskID: Unique task identifier
//   - jobID: Parent job identifier
//   - collectionID: Target collection
//   - state: Current task state
//
// Use case: Consistent logging format across all task operations
func WrapCopySegmentTaskLog(task CopySegmentTask, fields ...mlog.Field) []mlog.Field {
	res := []mlog.Field{
		mlog.FieldTaskID(task.GetTaskId()),
		mlog.FieldJobID(task.GetJobId()),
		mlog.FieldCollectionID(task.GetCollectionId()),
		mlog.String("state", task.GetState().String()),
	}
	res = append(res, fields...)
	return res
}

// ===========================================================================================
// Request Assembly: Build CopySegmentRequest from Snapshot Data
// ===========================================================================================

// AssembleCopySegmentRequest builds the request for DataNode copy segment operation.
//
// Process flow:
//  1. Read complete snapshot data from S3 (contains source segment descriptions)
//  2. Build source segment lookup map for efficient retrieval
//  3. For each ID mapping in the task:
//     a. Lookup source segment in snapshot data
//     b. Build CopySegmentSource with full binlog paths (insert/stats/delta/index)
//     c. Build CopySegmentTarget with only IDs (binlogs generated during copy)
//  4. Assemble CopySegmentRequest with sources, targets, and storage config
//
// Parameters:
//   - task: Copy segment task containing ID mappings
//   - job: Parent job containing snapshot name and options
//
// Returns:
//   - CopySegmentRequest ready to send to DataNode
//   - Error if snapshot data cannot be read
//
// Why read full snapshot:
// - Source segments contain complete binlog paths for all file types
// - Index files (vector/scalar/text/JSON) need to be copied with segment data
// - Snapshot is authoritative source for segment metadata
//
// Source vs Target:
// - Source: Full binlog paths from snapshot (what to copy)
// - Target: Only IDs (where to copy, paths generated on DataNode)
func AssembleCopySegmentRequest(task CopySegmentTask, job CopySegmentJob) (*datapb.CopySegmentRequest, error) {
	t := task.(*copySegmentTask)
	ctx := t.ctx
	if job == nil {
		return nil, merr.WrapErrServiceInternalMsg(
			"copy segment job %d not found while assembling task %d",
			t.GetJobId(),
			t.GetTaskId(),
		)
	}

	// Read complete snapshot data from S3 to retrieve source segment binlogs
	var (
		snapshotData *snapshotstorage.SnapshotData
		err          error
	)
	concreteJob, ok := job.(*copySegmentJob)
	if !ok || concreteJob.snapshotCache == nil {
		return nil, merr.WrapErrServiceInternalMsg(
			"copy segment job %d has no snapshot cache",
			job.GetJobId(),
		)
	}
	snapshotData, err = concreteJob.snapshotCache.load(func() (*snapshotstorage.SnapshotData, error) {
		var loaded *snapshotstorage.SnapshotData
		if job.GetExternal() {
			resolved, resolveErr := snapshotstorage.ResolveForeignStorage(
				ctx,
				snapshotstorage.InstanceConfigFromParamtable(Params),
				snapshotstorage.DirectionRestore,
				job.GetSnapshotS3Location(),
				job.GetExternalSpec(),
			)
			if resolveErr != nil {
				return nil, resolveErr
			}
			loaded, err = t.snapshotMeta.ReadExternalSnapshotDataWithChunkManager(
				ctx,
				resolved.ForeignCM,
				job.GetSnapshotS3Location(),
				true,
			)
		} else {
			loaded, err = t.snapshotMeta.ReadSnapshotData(ctx, job.GetSourceCollectionId(), job.GetSnapshotName(), true)
		}
		if err != nil {
			return nil, err
		}
		if expected := job.GetSnapshotFingerprint(); expected != "" {
			actual, fingerprintErr := snapshotstorage.SnapshotFingerprint(loaded)
			if fingerprintErr != nil {
				return nil, fingerprintErr
			}
			if actual != expected {
				return nil, merr.WrapErrDataIntegrityMsg("external snapshot changed after restore job creation")
			}
		}
		return loaded, nil
	})
	if err != nil {
		mlog.Error(ctx, "failed to read snapshot data for copy segment task",
			append(WrapCopySegmentTaskLog(task), mlog.Err(err))...)
		return nil, err
	}
	storageConfig := createStorageConfig()
	sourceRootPath := ""
	if job.GetExternal() {
		// DataNode uses SourceRootPath both to detect a foreign source bucket and
		// to rebase source object keys into the target storage root.
		sourceRootPath, err = deriveSnapshotSourceRootURI(job.GetSnapshotS3Location(), snapshotData.Layout)
		if err != nil {
			return nil, err
		}
	}

	// Build source segment map for quick lookup
	sourceSegmentMap := make(map[int64]*datapb.SegmentDescription)
	for _, segDesc := range snapshotData.Segments {
		sourceSegmentMap[segDesc.GetSegmentId()] = segDesc
	}

	// Dynamically build sources and targets from id_mappings
	idMappings := task.GetIdMappings()
	sources := make([]*datapb.CopySegmentSource, 0, len(idMappings))
	targets := make([]*datapb.CopySegmentTarget, 0, len(idMappings))
	var sourceSchema *schemapb.CollectionSchema
	if snapshotData.Collection != nil {
		sourceSchema = snapshotData.Collection.GetSchema()
	}
	isExternalCollection := typeutil.IsExternalCollection(sourceSchema)

	for _, mapping := range idMappings {
		sourceSegID := mapping.GetSourceSegmentId()
		targetSegID := mapping.GetTargetSegmentId()
		partitionID := mapping.GetPartitionId()

		// Get source segment description from snapshot
		sourceSegDesc, ok := sourceSegmentMap[sourceSegID]
		if !ok {
			return nil, merr.WrapErrServiceInternal(
				fmt.Sprintf("source segment %d not found in snapshot %s", sourceSegID, job.GetSnapshotName()))
		}

		// Build source with full binlog information
		source := &datapb.CopySegmentSource{
			CollectionId:         snapshotData.SnapshotInfo.GetCollectionId(),
			PartitionId:          sourceSegDesc.GetPartitionId(),
			SegmentId:            sourceSegDesc.GetSegmentId(),
			InsertBinlogs:        sourceSegDesc.GetBinlogs(),
			StatsBinlogs:         sourceSegDesc.GetStatslogs(),
			DeltaBinlogs:         sourceSegDesc.GetDeltalogs(),
			IndexFiles:           sourceSegDesc.GetIndexFiles(),        // vector/scalar index file info
			Bm25Binlogs:          sourceSegDesc.GetBm25Statslogs(),     // BM25 stats logs
			TextIndexFiles:       sourceSegDesc.GetTextIndexFiles(),    // Text index files
			JsonKeyIndexFiles:    sourceSegDesc.GetJsonKeyIndexFiles(), // JSON key index files
			ManifestPath:         sourceSegDesc.GetManifestPath(),      // manifest path for StorageV3+
			StorageVersion:       sourceSegDesc.GetStorageVersion(),    // storage version for binlog format decision
			IsExternalCollection: isExternalCollection,
			SourceRootPath:       sourceRootPath,
			NumOfRows:            sourceSegDesc.GetNumOfRows(),
		}
		sources = append(sources, source)

		// Collect all unique source build IDs from index files and allocate new ones
		// to avoid buildID reuse across copy segments, which would corrupt the
		// 1:1 segmentBuildInfo map in DataCoord indexMeta.
		newBuildIDs := make(map[int64]int64)
		allocNewBuildID := func(srcBuildID int64) error {
			if _, exists := newBuildIDs[srcBuildID]; !exists {
				newID, err := t.alloc.AllocID(ctx)
				if err != nil {
					return merr.Wrapf(err, "failed to allocate new buildID for source buildID %d", srcBuildID)
				}
				newBuildIDs[srcBuildID] = newID
			}
			return nil
		}
		for _, indexFile := range sourceSegDesc.GetIndexFiles() {
			if err := allocNewBuildID(indexFile.GetBuildID()); err != nil {
				return nil, err
			}
		}
		for _, textIndex := range sourceSegDesc.GetTextIndexFiles() {
			if textIndex.GetBuildID() != 0 {
				if err := allocNewBuildID(textIndex.GetBuildID()); err != nil {
					return nil, err
				}
			}
		}
		for _, jsonKeyIndex := range sourceSegDesc.GetJsonKeyIndexFiles() {
			if jsonKeyIndex.GetBuildID() != 0 {
				if err := allocNewBuildID(jsonKeyIndex.GetBuildID()); err != nil {
					return nil, err
				}
			}
		}
		// Build target with IDs and buildID mappings
		target := &datapb.CopySegmentTarget{
			CollectionId:   job.GetCollectionId(),
			PartitionId:    partitionID,
			SegmentId:      targetSegID,
			NewBuildIds:    newBuildIDs,
			TargetRootPath: storageConfig.GetRootPath(),
		}
		mlog.Info(ctx, "prepare copy segment source and target",
			WrapCopySegmentTaskLog(task,
				mlog.Int64("sourceCollectionID", source.GetCollectionId()),
				mlog.Int64("sourcePartitionID", source.GetPartitionId()),
				mlog.Int64("sourceSegmentID", source.GetSegmentId()),
				mlog.Int64("targetCollectionID", target.GetCollectionId()),
				mlog.Int64("targetPartitionID", target.GetPartitionId()),
				mlog.Int64("targetSegmentID", target.GetSegmentId()),
				mlog.Int("newBuildIDCount", len(newBuildIDs)),
				mlog.Bool("hasManifestPath", source.GetManifestPath() != ""),
				mlog.Int64("storageVersion", source.GetStorageVersion()))...)
		targets = append(targets, target)
	}

	return &datapb.CopySegmentRequest{
		ClusterID:     Params.CommonCfg.ClusterPrefix.GetValue(),
		JobID:         task.GetJobId(),
		TaskID:        task.GetTaskId(),
		Sources:       sources,
		Targets:       targets,
		StorageConfig: storageConfig,
		TaskSlot:      task.GetTaskSlot(),
		ExternalSpec:  job.GetExternalSpec(),
	}, nil
}

func deriveSnapshotSourceRootURI(snapshotS3Location string, layout datapb.SnapshotLayout) (string, error) {
	root, found := snapshotstorage.DeriveSnapshotRootPath(snapshotS3Location)
	if !found {
		return "", merr.WrapErrServiceInternalMsg("validated snapshot URI has no snapshot root")
	}
	objectKey := strings.TrimSuffix(root, "/")
	if layout == datapb.SnapshotLayout_SnapshotLayoutSelfContained {
		// Exported bundles store data under bundleRoot/files, while referenced
		// snapshots point directly at the original Milvus storage root.
		objectKey = path.Join(objectKey, snapshotstorage.ExportedSnapshotFilesPath)
	}
	parsed, err := url.Parse(snapshotS3Location)
	if err != nil {
		return "", merr.WrapErrServiceInternalErr(err, "failed to parse validated snapshot URI")
	}

	bucket, _, endpointHost, err := snapshotstorage.ParseForeignURI(snapshotS3Location)
	if err != nil {
		return "", merr.WrapErrServiceInternalErr(err, "failed to parse validated snapshot URI")
	}
	if endpointHost != "" {
		parsed.Path = "/" + path.Join(bucket, objectKey)
	} else {
		parsed.Path = "/" + objectKey
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return strings.TrimSuffix(parsed.String(), "/"), nil
}

// ===========================================================================================
// Result Synchronization: Update Segment Metadata from DataNode Response
// ===========================================================================================

// SyncCopySegmentTask synchronizes task results from DataNode to DataCoord metadata.
//
// Process flow (on successful completion):
//  1. For each segment result from DataNode:
//     a. Compress binlog paths and fill logID
//     b. Update segment binlogs (insert/stats/delta/BM25)
//     c. Mark segment as Flushed
//     d. Sync vector/scalar indexes to indexMeta
//     e. Sync text indexes to segment metadata
//     f. Sync JSON key indexes to segment metadata
//  2. Record task execution metrics (executing duration, total duration)
//  3. Mark task as completed with completion timestamp
//
// Process flow (on failure):
//  1. Mark task as failed with reason
//  2. Return error (job will be failed by caller)
//
// Parameters:
//   - task: Copy segment task being synced
//   - resp: QueryCopySegmentResponse from DataNode
//   - copyMeta: Metadata manager for updating task state
//   - meta: Segment metadata for updating binlogs and indexes
//
// Returns:
//   - nil on success
//   - error on failure (task and job will be marked as failed)
//
// Why sync multiple index types:
// - Vector/scalar indexes: Traditional dense/sparse vector and scalar indexes
// - Text indexes: Full-text search indexes for VARCHAR fields
// - JSON key indexes: Indexes on JSON field keys
// - All must be copied and registered for query functionality
//
// Error handling:
// - Any error during sync marks both task and job as failed
// - Ensures data integrity (no partial restore)
// - Provides clear error messages for troubleshooting
func validateCopySegmentResults(task CopySegmentTask, results []*datapb.CopySegmentResult) error {
	if len(task.GetIdMappings()) == 0 {
		return merr.WrapErrDataIntegrityMsg("copy segment task %d has no ID mappings", task.GetTaskId())
	}
	expected := make(map[int64]struct{}, len(task.GetIdMappings()))
	expectedOrder := make([]int64, 0, len(task.GetIdMappings()))
	for i, mapping := range task.GetIdMappings() {
		if mapping == nil {
			return merr.WrapErrDataIntegrityMsg("copy segment task %d has nil ID mapping at index %d", task.GetTaskId(), i)
		}
		targetID := mapping.GetTargetSegmentId()
		if _, duplicate := expected[targetID]; duplicate {
			return merr.WrapErrDataIntegrityMsg("copy segment task %d has duplicate target segment %d in ID mappings", task.GetTaskId(), targetID)
		}
		expected[targetID] = struct{}{}
		expectedOrder = append(expectedOrder, targetID)
	}

	seen := make(map[int64]struct{}, len(results))
	for i, result := range results {
		if result == nil {
			return merr.WrapErrDataIntegrityMsg("copy segment task %d returned nil segment result at index %d", task.GetTaskId(), i)
		}
		targetID := result.GetSegmentId()
		if _, ok := expected[targetID]; !ok {
			return merr.WrapErrDataIntegrityMsg("copy segment task %d returned foreign target segment %d", task.GetTaskId(), targetID)
		}
		if _, duplicate := seen[targetID]; duplicate {
			return merr.WrapErrDataIntegrityMsg("copy segment task %d returned duplicate result for target segment %d", task.GetTaskId(), targetID)
		}
		seen[targetID] = struct{}{}
	}

	for _, targetID := range expectedOrder {
		if _, ok := seen[targetID]; !ok {
			return merr.WrapErrDataIntegrityMsg("copy segment task %d is missing result for target segment %d", task.GetTaskId(), targetID)
		}
	}
	return nil
}

func markCopySegmentSyncFailed(ctx context.Context, task CopySegmentTask, copyMeta CopySegmentMeta, reason string) {
	if err := copyMeta.UpdateTask(ctx, task.GetTaskId(),
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed),
		UpdateCopyTaskReason(reason)); err != nil {
		mlog.Warn(ctx, "failed to update copy segment task state to Failed",
			mlog.FieldTaskID(task.GetTaskId()), mlog.Err(err))
	}

	job := copyMeta.GetJob(ctx, task.GetJobId())
	if job == nil || job.GetState() == datapb.CopySegmentJobState_CopySegmentJobFailed {
		return
	}
	if err := copyMeta.UpdateJobStateAndReleaseRef(ctx, task.GetJobId(),
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
		UpdateCopyJobReason(reason)); err != nil {
		mlog.Warn(ctx, "failed to update copy segment job state to Failed",
			mlog.FieldJobID(task.GetJobId()), mlog.Err(err))
	}
}

func SyncCopySegmentTask(ctx context.Context, task CopySegmentTask, resp *datapb.QueryCopySegmentResponse, copyMeta CopySegmentMeta, meta *meta) error {
	// Update task state based on response
	switch resp.GetState() {
	case datapb.CopySegmentTaskState_CopySegmentTaskCompleted:
		// A poll can outlive its task: the job checker fails every InProgress
		// task the moment its job fails, and the drop/GC path can retire the
		// record entirely, all while this response was in flight. Applying the
		// Completed sync then resurrects Dropped target segments to Flushed
		// under a job that reported Failed -- partially restored data becomes
		// visible, and the re-flipped task blocks the job's GC forever. Only a
		// record that is still InProgress may commit a completed result.
		if current := copyMeta.GetTask(ctx, task.GetTaskId()); current == nil ||
			current.GetState() != datapb.CopySegmentTaskState_CopySegmentTaskInProgress {
			mlog.Info(ctx, "discarding a completed copy segment result for a task no longer in progress",
				WrapCopySegmentTaskLog(task)...)
			return nil
		}
		// Validate the complete response before mutating any segment or index
		// metadata.  A partial, duplicate, or foreign result set must not make a
		// restore appear partially committed.
		if err := validateCopySegmentResults(task, resp.GetSegmentResults()); err != nil {
			markCopySegmentSyncFailed(ctx, task, copyMeta, err.Error())
			return err
		}

		// Update binlog information for all segments
		for _, result := range resp.GetSegmentResults() {
			// Update binlog info and segment state to Flushed
			// For StorageV3+ segments, also update manifest_path
			var err error
			op1 := UpdateBinlogsOperator(result.GetSegmentId(), result.GetBinlogs(),
				result.GetStatslogs(), result.GetDeltalogs(), result.GetBm25Logs())
			op2 := UpdateStatusOperator(result.GetSegmentId(), commonpb.SegmentState_Flushed)
			op3 := UpdateIsImporting(result.GetSegmentId(), false)
			operators := []UpdateOperator{op1, op2, op3}
			if manifestPath := result.GetManifestPath(); manifestPath != "" {
				operators = append(operators, UpdateManifest(result.GetSegmentId(), manifestPath))
			}
			err = meta.UpdateSegmentsInfo(ctx, operators...)
			if err != nil {
				markCopySegmentSyncFailed(ctx, task, copyMeta, err.Error())
				mlog.Warn(ctx, "update copy segment binlogs failed",
					WrapCopySegmentTaskLog(task, mlog.String("err", err.Error()))...)
				return err
			}

			// Sync vector/scalar indexes
			if err = syncVectorScalarIndexes(ctx, result, task, meta, copyMeta); err != nil {
				return err
			}

			// Sync text indexes
			if err = syncTextIndexes(ctx, result, task, meta, copyMeta); err != nil {
				return err
			}

			// Sync JSON key indexes
			if err = syncJSONKeyIndexes(ctx, result, task, meta, copyMeta); err != nil {
				return err
			}

			mlog.Info(ctx, "update copy segment info done",
				WrapCopySegmentTaskLog(task, mlog.Int64("segmentID", result.GetSegmentId()),
					mlog.Int64("importedRows", result.GetImportedRows()),
					mlog.Int("binlogFields", len(result.GetBinlogs())),
					mlog.Bool("hasManifestPath", result.GetManifestPath() != ""))...)
		}

		// Mark task as completed and record copying duration
		completeTs := uint64(time.Now().UnixNano())
		copyingDuration := task.GetTR().RecordSpan()
		metrics.CopySegmentTaskLatency.WithLabelValues(metrics.Executing).Observe(float64(copyingDuration.Milliseconds()))
		// Record total latency (from task creation to completion)
		totalDuration := task.GetTR().ElapseSpan()
		metrics.CopySegmentTaskLatency.WithLabelValues(metrics.Done).Observe(float64(totalDuration.Milliseconds()))
		mlog.Info(ctx, "copy segment task completed",
			WrapCopySegmentTaskLog(task,
				mlog.Duration("taskTimeCost/copying", copyingDuration),
				mlog.Duration("taskTimeCost/total", totalDuration))...)

		return copyMeta.UpdateTask(ctx, task.GetTaskId(),
			UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskCompleted),
			UpdateCopyTaskCompleteTs(completeTs))

	case datapb.CopySegmentTaskState_CopySegmentTaskFailed:
		// QueryTaskOnWorker handles a failed response before it gets here, so
		// this is only reachable through another caller of this exported
		// helper. Record it rather than let a task go failed unexplained.
		mlog.Warn(ctx, "syncing a failed copy segment result",
			WrapCopySegmentTaskLog(task, mlog.String("reason", resp.GetReason()))...)
		return copyMeta.UpdateTask(ctx, task.GetTaskId(),
			UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed),
			UpdateCopyTaskReason(resp.GetReason()))
	}
	return nil
}

// ===========================================================================================
// Index Synchronization: Vector and Scalar Indexes
// ===========================================================================================

// syncVectorScalarIndexes synchronizes vector and scalar index metadata to indexMeta.
//
// Process flow:
//  1. Find partition ID from task's ID mappings
//  2. For each index in segment result:
//     a. Build SegmentIndex model with index paths and metadata
//     b. Add to indexMeta (persistent storage)
//     c. Log success or failure
//
// Parameters:
//   - ctx: Context for cancellation
//   - result: Segment copy result from DataNode
//   - task: Copy segment task
//   - meta: Metadata manager containing indexMeta
//   - copyMeta: For marking task/job as failed on error
//
// Returns:
//   - nil on success
//   - error on failure (task and job will be marked as failed)
//
// Index types handled:
// - Vector indexes: Dense/sparse vector indexes (HNSW, IVF, etc.)
// - Scalar indexes: Inverted indexes on scalar fields
//
// Why separate from binlogs:
// - Indexes have separate lifecycle from binlogs
// - Index metadata stored in separate indexMeta structure
// - Enables independent index management and rebuilding
func syncVectorScalarIndexes(ctx context.Context, result *datapb.CopySegmentResult,
	task CopySegmentTask, meta *meta, copyMeta CopySegmentMeta,
) error {
	if len(result.GetIndexInfos()) == 0 {
		return nil
	}

	// Build indexName -> target indexID mapping from target collection's index definitions.
	// The source snapshot stores the source collection's indexID, but the target collection
	// has new indexIDs allocated during RestoreIndexes(). We must use the target indexID
	// so that segmentIndexes entries match the index definitions in indexes map.
	// Using indexName (instead of fieldID) as key because a single JSON field can have
	// multiple indexes on different paths, and indexName is preserved during RestoreIndexes.
	targetIndexes := meta.indexMeta.GetIndexesForCollection(task.GetCollectionId(), "")
	indexNameToTargetID := make(map[string]int64, len(targetIndexes))
	for _, index := range targetIndexes {
		indexNameToTargetID[index.IndexName] = index.IndexID
	}

	// Find partition ID from task's ID mappings
	var partitionID int64
	for _, mapping := range task.GetIdMappings() {
		if mapping.GetTargetSegmentId() == result.GetSegmentId() {
			partitionID = mapping.GetPartitionId()
			break
		}
	}
	numRows := result.GetImportedRows()
	if meta.segments != nil {
		// StorageV3 bundles intentionally omit legacy PB insert binlogs, so
		// DataNode cannot derive row count from EntriesNum. The target segment was
		// pre-registered from snapshot metadata and remains the authoritative value.
		if segment := meta.GetSegment(ctx, result.GetSegmentId()); segment != nil {
			numRows = segment.GetNumOfRows()
		}
	}

	// Sync each vector/scalar index
	for _, indexInfo := range result.GetIndexInfos() {
		// Resolve target indexID by indexName instead of fieldID.
		// This correctly handles JSON path indexes where one field has multiple indexes.
		targetIndexID, ok := indexNameToTargetID[indexInfo.GetIndexName()]
		if !ok {
			mlog.Warn(ctx, "no index definition found for index name in target collection, skip syncing",
				WrapCopySegmentTaskLog(task,
					mlog.String("indexName", indexInfo.GetIndexName()),
					mlog.FieldFieldID(indexInfo.GetFieldId()),
					mlog.Int64("sourceIndexID", indexInfo.GetIndexId()))...)
			continue
		}

		now := time.Now().Unix()
		segIndex := &model.SegmentIndex{
			SegmentID:                 result.GetSegmentId(),
			CollectionID:              task.GetCollectionId(),
			PartitionID:               partitionID,
			IndexID:                   targetIndexID,
			BuildID:                   indexInfo.GetBuildId(),
			IndexState:                commonpb.IndexState_Finished,
			IndexFileKeys:             indexInfo.GetIndexFilePaths(),
			IndexSerializedSize:       uint64(indexInfo.GetIndexSize()),
			IndexMemSize:              uint64(indexInfo.GetIndexSize()),
			IndexVersion:              indexInfo.GetVersion(),
			CurrentIndexVersion:       indexInfo.GetCurrentIndexVersion(),
			CurrentScalarIndexVersion: indexInfo.GetCurrentScalarIndexVersion(),
			CreatedUTCTime:            uint64(now),
			FinishedUTCTime:           uint64(now),
			NumRows:                   numRows,
			IndexStorePathVersion:     indexInfo.GetIndexStorePathVersion(),
		}

		err := meta.indexMeta.AddSegmentIndex(ctx, segIndex)
		if err != nil {
			mlog.Warn(ctx, "failed to add segment index",
				WrapCopySegmentTaskLog(task,
					mlog.FieldSegmentID(result.GetSegmentId()),
					mlog.String("indexName", indexInfo.GetIndexName()),
					mlog.FieldIndexID(targetIndexID),
					mlog.Err(err))...)

			markCopySegmentSyncFailed(ctx, task, copyMeta, err.Error())
			return err
		}

		mlog.Info(ctx, "synced vector/scalar index",
			WrapCopySegmentTaskLog(task,
				mlog.FieldSegmentID(result.GetSegmentId()),
				mlog.String("indexName", indexInfo.GetIndexName()),
				mlog.FieldFieldID(indexInfo.GetFieldId()),
				mlog.FieldIndexID(targetIndexID),
				mlog.Int64("sourceIndexID", indexInfo.GetIndexId()),
				mlog.FieldBuildID(indexInfo.GetBuildId()))...)
	}
	return nil
}

// ===========================================================================================
// Index Synchronization: Text Indexes
// ===========================================================================================

// syncTextIndexes synchronizes text index metadata to segment.
//
// Process flow:
//  1. Update segment with text index logs
//  2. Log success or mark task/job as failed
//
// Parameters:
//   - ctx: Context for cancellation
//   - result: Segment copy result from DataNode
//   - task: Copy segment task
//   - meta: Metadata manager for updating segment
//   - copyMeta: For marking task/job as failed on error
//
// Returns:
//   - nil on success
//   - error on failure (task and job will be marked as failed)
//
// Text indexes:
// - Full-text search indexes for VARCHAR fields
// - Stored inline with segment metadata (not in indexMeta)
// - Enables text search queries on restored collection
func syncTextIndexes(ctx context.Context, result *datapb.CopySegmentResult,
	task CopySegmentTask, meta *meta, copyMeta CopySegmentMeta,
) error {
	if len(result.GetTextIndexInfos()) == 0 {
		return nil
	}

	err := meta.UpdateSegment(result.GetSegmentId(),
		SetTextIndexLogs(result.GetTextIndexInfos()))
	if err != nil {
		mlog.Warn(ctx, "failed to update text index",
			WrapCopySegmentTaskLog(task,
				mlog.FieldSegmentID(result.GetSegmentId()),
				mlog.Err(err))...)

		markCopySegmentSyncFailed(ctx, task, copyMeta, err.Error())
		return err
	}

	mlog.Info(ctx, "synced text indexes",
		WrapCopySegmentTaskLog(task,
			mlog.FieldSegmentID(result.GetSegmentId()),
			mlog.Int("count", len(result.GetTextIndexInfos())))...)
	return nil
}

// ===========================================================================================
// Index Synchronization: JSON Key Indexes
// ===========================================================================================

// syncJSONKeyIndexes synchronizes JSON key index metadata to segment.
//
// Process flow:
//  1. Update segment with JSON key index logs
//  2. Log success or mark task/job as failed
//
// Parameters:
//   - ctx: Context for cancellation
//   - result: Segment copy result from DataNode
//   - task: Copy segment task
//   - meta: Metadata manager for updating segment
//   - copyMeta: For marking task/job as failed on error
//
// Returns:
//   - nil on success
//   - error on failure (task and job will be marked as failed)
//
// JSON key indexes:
// - Indexes on keys within JSON fields
// - Stored inline with segment metadata (not in indexMeta)
// - Enables efficient queries on JSON field contents
func syncJSONKeyIndexes(ctx context.Context, result *datapb.CopySegmentResult,
	task CopySegmentTask, meta *meta, copyMeta CopySegmentMeta,
) error {
	if len(result.GetJsonKeyIndexInfos()) == 0 {
		return nil
	}

	err := meta.UpdateSegment(result.GetSegmentId(),
		SetJSONKeyIndexLogs(result.GetJsonKeyIndexInfos()))
	if err != nil {
		mlog.Warn(ctx, "failed to update json key index",
			WrapCopySegmentTaskLog(task,
				mlog.FieldSegmentID(result.GetSegmentId()),
				mlog.Err(err))...)

		markCopySegmentSyncFailed(ctx, task, copyMeta, err.Error())
		return err
	}

	mlog.Info(ctx, "synced json key indexes",
		WrapCopySegmentTaskLog(task,
			mlog.FieldSegmentID(result.GetSegmentId()),
			mlog.Int("count", len(result.GetJsonKeyIndexInfos())))...)
	return nil
}
