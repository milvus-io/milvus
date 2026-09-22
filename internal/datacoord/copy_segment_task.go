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
	"io/fs"
	"maps"
	"net/url"
	"path"
	"slices"
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
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// copySegmentTask coordinates one snapshot-restore copy attempt. A transient
// failure is replaced with fresh task and target segment IDs; a permanent
// failure ends the job. Successful results publish copied binlogs and indexes
// before the target segments become Flushed.

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

// UpdateCopyTaskIndexWriteToManifest persists the index placement chosen
// before dispatch. It must not be recomputed while handling the worker result:
// the runtime switch may have changed while the task was running or while
// DataCoord was restarting.
func UpdateCopyTaskIndexWriteToManifest(enabled bool) UpdateCopySegmentTaskAction {
	return func(t CopySegmentTask) {
		t.(*copySegmentTask).task.Load().IndexWriteToManifest = proto.Bool(enabled)
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
	GetIndexWriteToManifest() (bool, bool)
	GetCleanupPrefixes() []string
	GetCleanupRequired() bool
	GetIdMappings() []*datapb.CopySegmentIDMapping // Lightweight ID mappings
	GetTR() *timerecord.TimeRecorder
	Clone() CopySegmentTask
}

// copySegmentTask implements CopySegmentTask with atomic state updates.
type copySegmentTask struct {
	task atomic.Pointer[datapb.CopySegmentTask] // Atomic pointer for concurrent access
	// Keep a failed task in the scheduler until completed-output cleanup is durable.
	cleanupAdmissionPending atomic.Bool

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

func (t *copySegmentTask) GetCleanupPrefixes() []string { return t.task.Load().GetCleanupPrefixes() }
func (t *copySegmentTask) GetCleanupRequired() bool     { return t.task.Load().GetCleanupRequired() }

func updateCopyTaskCleanup(required bool) UpdateCopySegmentTaskAction {
	return func(task CopySegmentTask) { task.(*copySegmentTask).task.Load().CleanupRequired = required }
}

// Keep every dispatch's allocated index directories: a retry allocates new
// build IDs, and an earlier attempt may have uploaded files before losing its reply.
func appendCopyTaskCleanupPrefixes(prefixes []string) UpdateCopySegmentTaskAction {
	return func(task CopySegmentTask) {
		record := task.(*copySegmentTask).task.Load()
		record.CleanupPrefixes = slices.Compact(slices.Sorted(slices.Values(append(record.CleanupPrefixes, prefixes...))))
	}
}

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

// GetIndexWriteToManifest returns the dispatch-time placement and whether it
// was explicitly persisted. Tasks created by older DataCoords have no value.
func (t *copySegmentTask) GetIndexWriteToManifest() (bool, bool) {
	task := t.task.Load()
	return task.GetIndexWriteToManifest(), task.IndexWriteToManifest != nil
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
	if t.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskFailed && t.cleanupAdmissionPending.Load() {
		return taskcommon.InProgress
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

// GetTaskVersion returns the persisted attempt counter. Each replacement
// increments it, and the attempt cap reads it.
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
//   - Transient assembly errors leave the task Pending for the copy inspector's
//     next interval
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
	if _, ok := t.GetIndexWriteToManifest(); !ok {
		if err := t.copyMeta.UpdateTask(ctx, t.GetTaskId(),
			UpdateCopyTaskIndexWriteToManifest(writeSegmentIndexToManifest())); err != nil {
			mlog.Warn(ctx, "failed to persist copy segment index placement before dispatch",
				WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
			return
		}
	}
	req, err := AssembleCopySegmentRequest(t, job)
	if err != nil {
		mlog.Warn(ctx, "failed to assemble copy segment request",
			WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
		if isPermanentSnapshotError(err) {
			t.markTaskAndJobFailed(merr.Wrap(err, "failed to assemble copy segment request").Error())
		}
		return
	}
	if err := t.prepareCopyCleanup(ctx, req); err != nil {
		mlog.Warn(ctx, "failed to persist copy cleanup plan", mlog.FieldTaskID(t.GetTaskId()), mlog.Err(err))
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
		t.abandonAttempt(ctx, cluster, fmt.Sprintf("create on node %d returned no success: %v", nodeID, err))
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
	if job != nil && (job.GetState() == datapb.CopySegmentJobState_CopySegmentJobPending ||
		job.GetState() == datapb.CopySegmentJobState_CopySegmentJobExecuting) {
		_, updateErr = t.copyMeta.UpdateJobStateAndReleasePin(ctx, t.GetJobId(), job.GetState(),
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

// isCopyTaskLostOnWorker reports whether a QueryCopySegment error means the
// worker-side task is confirmed lost, as opposed to a transient transport error.
//
// Confirmed-loss signals (audited against every construction site on the
// QueryCopySegment path):
//   - merr.ErrNodeNotFound: the node manager no longer knows the assigned
//     DataNode (its session was removed after a restart/replacement), so its
//     in-memory task manager — and the task with it — is gone.
//   - merr.ErrImportSysFailed: on this RPC the code is produced only by the
//     DataNode's task-not-found branch (importv2.WrapTaskNotFoundError when the
//     queried task is absent from its task manager), i.e. the DataNode is alive
//     but restarted and lost the task.
//
// Everything else (gRPC transport errors, node briefly not serving/not ready,
// response decode failures) may coexist with a still-running worker task and
// must be retried by polling, not by re-dispatching.
func isCopyTaskLostOnWorker(err error) bool {
	return errors.Is(err, merr.ErrNodeNotFound) || errors.Is(err, merr.ErrImportSysFailed)
}

// replanUnderFreshIdentity replaces the current attempt with a fresh task ID
// and fresh target segment IDs in one catalog update.
func (t *copySegmentTask) replanUnderFreshIdentity(ctx context.Context) (CopySegmentTask, error) {
	old := t.GetTaskId()
	mappings := t.GetIdMappings()
	if len(mappings) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("copy segment task %d has no ID mappings to replan", old)
	}
	if t.alloc == nil {
		return nil, merr.WrapErrServiceInternalMsg("copy segment task %d has no allocator to replan", old)
	}

	newTaskID, err := t.alloc.AllocID(ctx)
	if err != nil {
		return nil, merr.Wrap(err, "failed to allocate a copy segment replan task ID")
	}
	segmentIDStart, _, err := t.alloc.AllocN(int64(len(mappings)))
	if err != nil {
		return nil, merr.Wrap(err, "failed to allocate copy segment replan target IDs")
	}

	newMappings := make([]*datapb.CopySegmentIDMapping, 0, len(mappings))
	for i, mapping := range mappings {
		if mapping == nil {
			return nil, merr.WrapErrServiceInternalMsg("copy segment task %d has a nil ID mapping", old)
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
		TaskId:       newTaskID,
		JobId:        t.GetJobId(),
		CollectionId: t.GetCollectionId(),
		NodeId:       NullNodeID,
		TaskVersion:  t.GetTaskVersion() + 1,
		TaskSlot:     t.GetTaskSlot(),
		State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
		IdMappings:   newMappings,
		CreatedTs:    uint64(time.Now().UnixNano()),
		// Keep every earlier attempt's task-owned directories: an abandoned
		// attempt may have uploaded files before losing its reply, and the
		// prefixes are only deleted once no target segment references them.
		CleanupPrefixes: t.GetCleanupPrefixes(),
	})
	replaced, err := t.copyMeta.ReplaceRetryTask(ctx, old, replanned)
	if err != nil {
		return nil, err
	}
	if !replaced {
		return nil, nil
	}

	mlog.Info(ctx, "copy segment task replanned under a fresh identity",
		WrapCopySegmentTaskLog(t,
			mlog.Int64("replanTaskID", newTaskID),
			mlog.Int64("attempt", replanned.GetTaskVersion()))...)
	return replanned, nil
}

// copySegmentReplanTarget reconstructs the pre-execution target template under
// a fresh segment ID. Execution-produced artifacts never cross attempts.
func copySegmentReplanTarget(oldTarget *SegmentInfo, targetID int64) *SegmentInfo {
	info := proto.Clone(oldTarget.SegmentInfo).(*datapb.SegmentInfo)
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

// abandonAttempt records retry debt, or fails the job when the attempt cap is
// spent. It never constructs the next attempt: the copy inspector owns that
// work and its retry interval.
func (t *copySegmentTask) abandonAttempt(ctx context.Context, cluster session.Cluster, reason string) {
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
		_, _ = t.copyMeta.UpdateTaskInState(ctx, t.GetTaskId(),
			datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
			UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed),
			UpdateCopyTaskReason(reason))
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
	applied, err := t.copyMeta.UpdateTaskInState(ctx, t.GetTaskId(),
		datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskRetry),
		UpdateCopyTaskReason(reason))
	if err != nil {
		mlog.Warn(ctx, "failed to persist copy segment retry state",
			WrapCopySegmentTaskLog(t, mlog.Err(err))...)
		// Release this wrapper even when persistence is unavailable. The copy
		// inspector re-offers the authoritative InProgress record on its own
		// interval, so recovery does not fall back to the scheduler's 100ms loop.
		local := t.Clone().(*copySegmentTask).task.Load()
		local.State = datapb.CopySegmentTaskState_CopySegmentTaskRetry
		local.Reason = reason
		t.task.Store(local)
	} else if !applied {
		return
	}

	// Retry replacement uses a fresh task ID, so a delayed or failed Drop of
	// this predecessor cannot affect the next attempt. Keep cleanup best effort:
	// publishing Retry is the handoff, and worker availability must not block the
	// inspector from replacing it.
	t.DropTaskOnWorker(cluster)
}

// replaceRetryAttempt runs only from copySegmentInspector. The old Retry meta
// is removed in the same catalog update that publishes the fresh task and
// target segments; only after that succeeds may the replacement be enqueued.
func (t *copySegmentTask) replaceRetryAttempt(ctx context.Context) (CopySegmentTask, error) {
	if t.GetState() != datapb.CopySegmentTaskState_CopySegmentTaskRetry {
		return nil, nil
	}
	maxAttempts := Params.DataCoordCfg.CopySegmentMaxAttempts.GetAsInt64()
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	if t.GetTaskVersion()+1 >= maxAttempts {
		t.markTaskAndJobFailed(fmt.Sprintf("%s, and the copy segment attempt cap (%d) is spent", t.GetReason(), maxAttempts))
		return nil, nil
	}
	return t.replanUnderFreshIdentity(ctx)
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
//   - Retry responses replace the attempt; permanent failures fail the job
//
// Success handling:
// - Calls SyncCopySegmentTask to update segment metadata
// - Updates binlogs, indexes (vector/scalar/text/JSON)
// - Marks segments as Flushed for query availability
// - Records executing and total duration metrics
func (t *copySegmentTask) QueryTaskOnWorker(cluster session.Cluster) {
	nodeID := t.GetNodeId()
	req := &datapb.QueryCopySegmentRequest{
		TaskID: t.GetTaskId(),
	}
	resp, err := cluster.QueryCopySegment(nodeID, req)
	if t.copyMeta.GetTask(t.ctx, t.GetTaskId()) == nil {
		oldTaskState := t.Clone().(*copySegmentTask).task.Load()
		oldTaskState.State = datapb.CopySegmentTaskState_CopySegmentTaskFailed
		t.task.Store(oldTaskState)
		mlog.Info(t.ctx, "discarding copy segment result for a task no longer in metadata",
			WrapCopySegmentTaskLog(t)...)
		return
	}
	// Handle RPC error separately to avoid nil resp dereference.
	if err != nil {
		// An admitted completion may have published some targets before a crash.
		// Its cleanup plan must outlive this attempt, so worker loss cannot turn
		// it into a fresh attempt while the cleanup intent is armed: fail it and
		// retain the plan for the cleanup worker and ordinary GC instead.
		if t.GetCleanupRequired() || (t.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskFailed && len(t.GetCleanupPrefixes()) > 0) {
			if !isCopyTaskLostOnWorker(err) {
				// The worker may still hold the admitted result; keep polling.
				mlog.Warn(t.ctx, "transient error querying an admitted copy segment task on datanode, will retry",
					WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
				return
			}
			t.cleanupAdmissionPending.Store(true)
			if t.GetState() != datapb.CopySegmentTaskState_CopySegmentTaskFailed {
				t.markTaskAndJobFailed("copy worker lost after completed-result installation was admitted")
				if t.GetState() != datapb.CopySegmentTaskState_CopySegmentTaskFailed {
					return // The failed-state save must succeed before releasing the worker.
				}
			}
			if err := t.copyMeta.UpdateTask(t.ctx, t.GetTaskId(), updateCopyTaskCleanup(true), UpdateCopyTaskNodeID(NullNodeID)); err != nil {
				t.cleanupAdmissionPending.Store(true)
				mlog.Warn(t.ctx, "retry failed copy cleanup after worker loss", mlog.FieldTaskID(t.GetTaskId()), mlog.Err(err))
				return
			}
			t.cleanupAdmissionPending.Store(false)
			return
		}
		// Nothing of this attempt has been admitted, so an unanswered round
		// tells us nothing about the worker: rebuild the work under a fresh
		// task and fresh target segment IDs.
		mlog.Info(t.ctx, "copy segment query left the attempt unanswered, replanning",
			WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
		t.abandonAttempt(t.ctx, cluster, fmt.Sprintf("query on node %d returned no answer: %v", nodeID, err))
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
		t.abandonAttempt(t.ctx, cluster, fmt.Sprintf("retriable failure on node %d: %s", nodeID, resp.GetReason()))
		return
	}

	// Handle permanent task execution failure (resp is guaranteed non-nil here)
	if resp.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskFailed {
		t.cleanupAdmissionPending.Store(false)
		t.markTaskAndJobFailed(resp.GetReason())
		return
	}

	if resp.GetState() != datapb.CopySegmentTaskState_CopySegmentTaskCompleted {
		// A recovered failed task still owns cleanup tracking while its worker
		// is running. Keep polling until completion or confirmed worker loss.
		return
	}

	// Sync task state and binlog info
	t.cleanupAdmissionPending.Store(true)
	err = SyncCopySegmentTask(t.ctx, t, resp, t.copyMeta, t.meta)
	if err != nil {
		if !t.GetCleanupRequired() {
			// Admission failed before cleanup intent was durable. Retry the
			// completed result; marking Failed here would lose its cleanup plan.
			mlog.Warn(t.ctx, "retry completed copy after cleanup intent persistence failure", mlog.FieldTaskID(t.GetTaskId()), mlog.Err(err))
			return
		}
		t.cleanupAdmissionPending.Store(false)
		t.markTaskAndJobFailed(fmt.Sprintf("failed to sync segment metadata: %v", err))
		return
	}
	t.cleanupAdmissionPending.Store(false)

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
//  2. DataNode removes its in-memory task entry
//  3. Log success or failure
//
// When called:
// - After task completes successfully (cleanup)
// - After task fails and is marked for deletion (cleanup)
// - During garbage collection of old tasks
//
// Object files are reclaimed after DataCoord drops abandoned target segments;
// segment, orphan, and LOB GC perform the actual object cleanup. A replacement
// may already have removed the old task record, making the metadata update a
// no-op.
func (t *copySegmentTask) DropTaskOnWorker(cluster session.Cluster) {
	nodeID := t.GetNodeId()
	if !isNodeAssigned(nodeID) {
		return
	}
	err := cluster.DropCopySegment(nodeID, t.GetTaskId())
	if err != nil && !errors.Is(err, merr.ErrNodeNotFound) {
		mlog.RatedWarn(t.ctx, rate.Limit(1.0/60), "failed to drop copy segment task on datanode",
			WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
		return
	}
	// The record may already be gone -- retry replacement removes the old task
	// before the scheduler sends this drop -- in which case UpdateTask is a no-op
	// and there is no assignment left to release.
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

// prepareCopyCleanup records only coordinator-derived task-owned directories.
// No worker-supplied directory is ever promoted into a deletion prefix.
func (t *copySegmentTask) prepareCopyCleanup(ctx context.Context, req *datapb.CopySegmentRequest) error {
	var prefixes []string
	var operators []UpdateOperator
	for i, target := range req.GetTargets() {
		source := req.GetSources()[i]
		root := req.GetStorageConfig().GetRootPath()
		base := path.Join(root, common.SegmentInsertLogPath, fmt.Sprint(target.GetCollectionId()), fmt.Sprint(target.GetPartitionId()), fmt.Sprint(target.GetSegmentId()))
		for _, logType := range []string{common.SegmentInsertLogPath, common.SegmentStatslogPath, common.SegmentDeltaLogPath, common.SegmentBm25LogPath} {
			prefixes = append(prefixes, path.Join(root, logType, fmt.Sprint(target.GetCollectionId()), fmt.Sprint(target.GetPartitionId()), fmt.Sprint(target.GetSegmentId()))+"/")
		}
		// An Importing placeholder needs a base even if validation later refuses
		// the worker's first manifest. GC can then reclaim it after worker loss.
		if source.GetStorageVersion() >= storage.StorageV3 {
			segmentID := target.GetSegmentId()
			operators = append(operators, func(pack *updateSegmentPack) bool {
				segment := pack.Get(segmentID)
				if segment == nil || segment.GetState() != commonpb.SegmentState_Importing || segment.GetManifestPath() != "" {
					return false
				}
				segment.ManifestPath = packed.MarshalManifestPath(base, 0)
				return true
			})
		}
		for _, info := range source.GetIndexFiles() {
			buildID := target.GetNewBuildIds()[info.GetBuildID()]
			if buildID <= 0 {
				return merr.WrapErrServiceInternalMsg("missing allocated copy build ID for %d", info.GetBuildID())
			}
			builder := metautil.NewIndexPathBuilder(root, info.GetIndexStorePathVersion(), target.GetCollectionId(), target.GetPartitionId(), target.GetSegmentId(), buildID, info.GetIndexVersion())
			prefixes = append(prefixes, builder.BuildPrefix()+"/")
		}
	}
	if len(prefixes) == 0 {
		return nil
	}
	if err := t.copyMeta.UpdateTask(ctx, t.GetTaskId(), appendCopyTaskCleanupPrefixes(prefixes)); err != nil {
		return err
	}
	return t.meta.UpdateSegmentsInfo(ctx, operators...)
}

// copyResultLock serializes result installation with deletion of task-owned files.
// It is scoped to the task ID, so metadata clones share the same exclusion.
func (m *meta) copyResultLock() *lock.KeyLock[int64] {
	m.copyResultLocksOnce.Do(func() { m.copyResultLocks = lock.NewKeyLock[int64]() })
	return m.copyResultLocks
}

// Completion or confirmed worker loss admits cleanup; ordinary GC's retirement of
// every target makes deletion safe. Skip active publication and retry next pass.
func cleanupRejectedCopy(ctx context.Context, task CopySegmentTask, meta *meta, copyMeta CopySegmentMeta) error {
	locks := meta.copyResultLock()
	if !locks.TryLock(task.GetTaskId()) {
		return nil
	}
	defer locks.Unlock(task.GetTaskId())
	if task.GetState() != datapb.CopySegmentTaskState_CopySegmentTaskFailed || !task.GetCleanupRequired() {
		return nil
	}
	for _, mapping := range task.GetIdMappings() {
		segment := meta.GetSegment(ctx, mapping.GetTargetSegmentId())
		if segment != nil {
			// A result can have published some targets before a later save fails.
			// Ordinary segment GC owns snapshot, loaded-reader, pause and retention
			// checks. Its final metadata removal authorizes leftover-prefix cleanup.
			return nil
		}
	}
	if meta.chunkManager == nil {
		return merr.WrapErrServiceUnavailableMsg("copy cleanup storage is unavailable")
	}
	for _, prefix := range task.GetCleanupPrefixes() {
		if err := meta.chunkManager.RemoveWithPrefix(ctx, prefix); err != nil {
			// Local walks report ENOENT for unused stats/delta/BM25 directories.
			// Only absence of the prefix itself is a no-op; partial walk failures retry.
			if isAbsentLocalPrefix(meta.chunkManager, prefix, err) {
				continue
			}
			return merr.Wrap(err, "cleanup rejected copy")
		}
	}
	return copyMeta.UpdateTask(ctx, task.GetTaskId(), updateCopyTaskCleanup(false))
}

// Only absence of the walked root proves there is no cleanup work. A missing
// descendant can instead be a partial walk failure and must retain the retry.
func isAbsentLocalPrefix(cm storage.ChunkManager, prefix string, err error) bool {
	var pathErr *fs.PathError
	_, local := cm.(*storage.LocalChunkManager)
	return local && errors.Is(err, fs.ErrNotExist) && errors.As(err, &pathErr) && pathErr.Path == path.Clean(prefix)
}

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

	writeIndexesToManifest, _ := taskIndexWriteToManifest(task)
	// The restored collection is created from the snapshot schema, so its field
	// names come from the snapshot rather than a local collection cache.
	targetIndexes := buildCopySegmentTargetIndexes(t.meta, sourceSchema, job.GetCollectionId(), writeIndexesToManifest)

	// Resolve the immutable source descriptions first, then prefetch only the
	// manifest index sections the snapshot cannot answer on its own. The FFI
	// calls are independent across segments, but build-ID allocation and request
	// assembly below stay ordered so parallel I/O cannot change the request.
	preparedSources := make([]*datapb.CopySegmentSource, len(idMappings))
	manifestIndexesByMapping := make([][]packed.ManifestIndexInfo, len(idMappings))
	manifestReadMappings := make([]int, 0, len(idMappings))
	for i, mapping := range idMappings {
		sourceSegID := mapping.GetSourceSegmentId()
		sourceSegDesc, ok := sourceSegmentMap[sourceSegID]
		if !ok {
			return nil, merr.WrapErrServiceInternal(
				fmt.Sprintf("source segment %d not found in snapshot %s", sourceSegID, job.GetSnapshotName()))
		}
		source := &datapb.CopySegmentSource{
			CollectionId:         snapshotData.SnapshotInfo.GetCollectionId(),
			PartitionId:          sourceSegDesc.GetPartitionId(),
			SegmentId:            sourceSegDesc.GetSegmentId(),
			InsertBinlogs:        sourceSegDesc.GetBinlogs(),
			StatsBinlogs:         sourceSegDesc.GetStatslogs(),
			DeltaBinlogs:         sourceSegDesc.GetDeltalogs(),
			IndexFiles:           sourceSegDesc.GetIndexFiles(),
			Bm25Binlogs:          sourceSegDesc.GetBm25Statslogs(),
			TextIndexFiles:       sourceSegDesc.GetTextIndexFiles(),
			JsonKeyIndexFiles:    sourceSegDesc.GetJsonKeyIndexFiles(),
			ManifestPath:         sourceSegDesc.GetManifestPath(),
			StorageVersion:       sourceSegDesc.GetStorageVersion(),
			IsExternalCollection: isExternalCollection,
			SourceRootPath:       sourceRootPath,
			NumOfRows:            sourceSegDesc.GetNumOfRows(),
			ManifestHasIndex:     cloneOptionalBool(sourceSegDesc.ManifestHasIndex),
		}
		preparedSources[i] = source

		snapshotCarriesIndexFiles := len(source.GetIndexFiles()) > 0
		manifestKnownEmpty := source.ManifestHasIndex != nil && !source.GetManifestHasIndex()
		if !snapshotCarriesIndexFiles && !manifestKnownEmpty && !job.GetExternal() &&
			source.GetStorageVersion() >= storage.StorageV3 && source.GetManifestPath() != "" {
			manifestReadMappings = append(manifestReadMappings, i)
		}
	}

	snapshotIndexIDs := make(map[int64]struct{}, len(snapshotData.Indexes))
	for _, index := range snapshotData.Indexes {
		if index != nil {
			snapshotIndexIDs[index.GetIndexID()] = struct{}{}
		}
	}
	if len(manifestReadMappings) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		concurrency := min(len(manifestReadMappings), segmentIndexManifestReadConcurrency())
		pool := conc.NewPool[struct{}](concurrency)
		futures := make([]*conc.Future[struct{}], 0, len(manifestReadMappings))
		for _, mappingIndex := range manifestReadMappings {
			mappingIndex := mappingIndex
			futures = append(futures, pool.Submit(func() (struct{}, error) {
				if err := ctx.Err(); err != nil {
					return struct{}{}, err
				}
				source := preparedSources[mappingIndex]
				manifestIndexes, err := t.meta.readManifestIndexes(ctx, source.GetManifestPath(), storageConfig)
				if err != nil {
					return struct{}{}, merr.Wrapf(err,
						"failed to load source index metadata from manifest for segment %d", source.GetSegmentId())
				}
				manifestIndexesByMapping[mappingIndex] = manifestIndexes
				return struct{}{}, ctx.Err()
			}))
		}
		err := conc.BlockOnAll(futures...)
		pool.Release()
		if err != nil {
			return nil, err
		}
	}

	for i, mapping := range idMappings {
		sourceSegID := mapping.GetSourceSegmentId()
		targetSegID := mapping.GetTargetSegmentId()
		partitionID := mapping.GetPartitionId()

		sourceSegDesc := sourceSegmentMap[sourceSegID]
		source := preparedSources[i]
		// WHICH INDEX FILES TO COPY: the snapshot's own index metadata wins.
		// SegmentDescription.IndexFiles is the snapshot's capture of the etcd
		// SegmentIndex records, so a segment that has them - including every
		// segment predating manifest index publication - is served entirely from
		// there and the manifest is never consulted for this question.
		snapshotCarriesIndexFiles := len(source.GetIndexFiles()) > 0

		// If the snapshot predates manifest-only finished records, IndexFiles is
		// already complete and no manifest read is needed. Otherwise, supplement
		// it from the source manifest. After the copy the worker either uses the
		// captured marker as proof of an empty index section or enumerates and
		// retracts inherited entries itself; DataCoord does not need to ship a
		// second advisory list of the same IDs.
		if !snapshotCarriesIndexFiles {
			for _, manifestIndex := range manifestIndexesByMapping[i] {
				if _, exists := snapshotIndexIDs[manifestIndex.IndexID]; !exists {
					continue
				}
				info, ok := manifestIndexFilePathInfoForSegment(storageConfig.GetRootPath(), &datapb.SegmentInfo{
					ID: source.GetSegmentId(), CollectionID: source.GetCollectionId(), PartitionID: source.GetPartitionId(),
				}, manifestIndex)
				if !ok {
					return nil, merr.WrapErrServiceInternalMsg("invalid source index metadata in manifest for segment %d", source.GetSegmentId())
				}
				source.IndexFiles = append(source.IndexFiles, info)
			}
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
		for _, indexFile := range source.GetIndexFiles() {
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
		TargetIndexes: targetIndexes,
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

func validateCopySegmentResults(task CopySegmentTask, results []*datapb.CopySegmentResult) error {
	expected := make(map[int64]struct{}, len(task.GetIdMappings()))
	for i, mapping := range task.GetIdMappings() {
		if mapping == nil {
			return merr.WrapErrServiceInternalMsg(
				"copy segment task %d has nil ID mapping at index %d", task.GetTaskId(), i)
		}
		targetID := mapping.GetTargetSegmentId()
		if _, duplicated := expected[targetID]; duplicated {
			return merr.WrapErrServiceInternalMsg(
				"copy segment task %d has duplicate target segment %d", task.GetTaskId(), targetID)
		}
		expected[targetID] = struct{}{}
	}

	seen := make(map[int64]struct{}, len(results))
	for i, result := range results {
		if result == nil {
			return merr.WrapErrServiceInternalMsg(
				"copy segment task %d returned nil segment result at index %d", task.GetTaskId(), i)
		}
		segmentID := result.GetSegmentId()
		if _, ok := expected[segmentID]; !ok {
			return merr.WrapErrServiceInternalMsg(
				"copy segment task %d returned unexpected target segment %d", task.GetTaskId(), segmentID)
		}
		if _, duplicated := seen[segmentID]; duplicated {
			return merr.WrapErrServiceInternalMsg(
				"copy segment task %d returned duplicate target segment %d", task.GetTaskId(), segmentID)
		}
		seen[segmentID] = struct{}{}
	}

	if len(seen) != len(expected) {
		for _, mapping := range task.GetIdMappings() {
			targetID := mapping.GetTargetSegmentId()
			if _, ok := seen[targetID]; !ok {
				return merr.WrapErrServiceInternalMsg(
					"copy segment task %d result is missing target segment %d", task.GetTaskId(), targetID)
			}
		}
	}
	return nil
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
//     c. Sync vector/scalar indexes to indexMeta
//     d. Sync text indexes to segment metadata
//     e. Sync JSON key indexes to segment metadata
//  2. Record task execution metrics (executing duration, total duration)
//  3. Mark task as completed with completion timestamp
//
// Process flow (on failure):
//  1. Return the error
//  2. The caller marks the task and job failed once
//
// Parameters:
//   - task: Copy segment task being synced
//   - resp: QueryCopySegmentResponse from DataNode
//   - copyMeta: Metadata manager for updating task state
//   - meta: Segment metadata for updating binlogs and indexes
//
// Returns:
//   - nil on success
//   - error on failure (the caller marks the task and job failed)
//
// Why sync multiple index types:
// - Vector/scalar indexes: Traditional dense/sparse vector and scalar indexes
// - Text indexes: Full-text search indexes for VARCHAR fields
// - JSON key indexes: Indexes on JSON field keys
// - All must be copied and registered for query functionality
//
// Error handling:
//   - Any error is returned to QueryTaskOnWorker, which marks the task and job
//     failed once through markTaskAndJobFailed
//   - Ensures data integrity (no partial restore)
//   - Provides clear error messages for troubleshooting
func SyncCopySegmentTask(ctx context.Context, task CopySegmentTask, resp *datapb.QueryCopySegmentResponse, copyMeta CopySegmentMeta, meta *meta) error {
	// Update task state based on response
	switch resp.GetState() {
	case datapb.CopySegmentTaskState_CopySegmentTaskCompleted:
		locks := meta.copyResultLock()
		if !locks.TryLock(task.GetTaskId()) {
			// Another publication or cleanup owns this task. Let the scheduler
			// observe its outcome on the next poll without waiting on deletion.
			return nil
		}
		defer locks.Unlock(task.GetTaskId())
		// Even a late result rejected below owns files that need durable cleanup.
		// Re-arming an already cleaned task is safe because prefix deletion is idempotent.
		if err := copyMeta.UpdateTask(ctx, task.GetTaskId(), updateCopyTaskCleanup(true)); err != nil {
			return err
		}
		// A poll can outlive its task: the job checker fails every InProgress
		// task the moment its job fails, and the drop/GC path can retire the
		// record entirely, all while this response was in flight. Applying the
		// Completed sync then resurrects Dropped target segments under a job
		// that reported Failed, and the re-flipped task blocks the job's GC
		// forever. Only a record that is still InProgress may commit a completed
		// result; a late one keeps just the cleanup intent armed above.
		current := copyMeta.GetTask(ctx, task.GetTaskId())
		if current != nil && current.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskFailed && len(current.GetCleanupPrefixes()) > 0 {
			// A failed task with a cleanup plan may already have lost its files,
			// including after restart when CleanupRequired has been cleared.
			return merr.WrapErrServiceInternalMsg("cannot publish a failed copy task with planned cleanup")
		}
		if current == nil || current.GetState() != datapb.CopySegmentTaskState_CopySegmentTaskInProgress {
			mlog.Info(ctx, "discarding a completed copy segment result for a task no longer in progress",
				WrapCopySegmentTaskLog(task)...)
			return nil
		}
		if err := validateCopySegmentResults(task, resp.GetSegmentResults()); err != nil {
			mlog.Warn(ctx, "invalid completed copy segment response",
				WrapCopySegmentTaskLog(task, mlog.Err(err))...)
			return err
		}
		// The worker returns a complete manifest pointer - including the copied
		// index entries, which it re-derives rather than inheriting from the
		// source manifest. "Re-derives" is verified, not assumed: an old DataNode
		// silently skips the republication (the result carries no acknowledgement
		// and the cluster RPC has no version gate), so the pointer is read back
		// and rejected before anything is published if it still carries foreign
		// entries. Any error is returned to the caller, which marks the task and
		// job failed once.
		results := resp.GetSegmentResults()
		verified := make([]map[int64]int64, len(results))
		verificationErrors := make([]error, len(results))
		pool := conc.NewPool[struct{}](min(max(1, len(results)), segmentIndexManifestReadConcurrency()))
		futures := make([]*conc.Future[struct{}], 0, len(results))
		for i, result := range results {
			futures = append(futures, pool.Submit(func() (struct{}, error) {
				verified[i], verificationErrors[i] = verifyCopiedManifestIndexOwnership(ctx, result, task, meta)
				return struct{}{}, nil
			}))
		}
		_ = conc.BlockOnAll(futures...)
		pool.Release()
		for i, result := range results {
			if verificationErrors[i] != nil {
				mlog.Warn(ctx, "copied manifest index verification failed",
					WrapCopySegmentTaskLog(task, mlog.Int64("segmentID", result.GetSegmentId()), mlog.Err(verificationErrors[i]))...)
				return failCopySegmentSync(ctx, task, verificationErrors[i])
			}
			if err := validateCopiedManifestIndexPlacement(result, task, meta, verified[i]); err != nil {
				mlog.Warn(ctx, "copied manifest index placement mismatch",
					WrapCopySegmentTaskLog(task, mlog.Int64("segmentID", result.GetSegmentId()), mlog.Err(err))...)
				return failCopySegmentSync(ctx, task, err)
			}
		}
		// Update binlog information for all segments.
		for i, result := range results {
			// Keep the target hidden until CompleteJob publishes all mappings and
			// the terminal job state together. A copy target is freshly created and
			// exclusively owned, so its first manifest pointer can be set inline
			// via UpdateManifest; no CommitSegmentManifest serialization is needed
			// for a segment no other writer touches.
			publishedBuilds := verified[i]
			operators := []UpdateOperator{
				UpdateBinlogsOperator(result.GetSegmentId(), result.GetBinlogs(),
					result.GetStatslogs(), result.GetDeltalogs(), result.GetBm25Logs()),
			}
			if manifestPath := result.GetManifestPath(); manifestPath != "" {
				operators = append(operators, UpdateManifest(result.GetSegmentId(), manifestPath))
				if len(publishedBuilds) > 0 {
					// The worker-created manifest is adopted outside
					// CommitSegmentManifest, so publish its recovery
					// marker in the same segment transaction as the pointer.
					operators = append(operators, UpdateManifestHasIndex(result.GetSegmentId()))
				}
			}
			err := meta.UpdateSegmentsInfo(ctx, operators...)
			if err != nil {
				mlog.Warn(ctx, "update copy segment binlogs failed",
					WrapCopySegmentTaskLog(task, mlog.String("err", err.Error()))...)
				return err
			}

			// Sync vector/scalar indexes
			if err = syncVectorScalarIndexes(ctx, result, task, meta, publishedBuilds); err != nil {
				return err
			}

			// Sync text indexes
			if err = syncTextIndexes(ctx, result, task, meta); err != nil {
				return err
			}

			// Sync JSON key indexes
			if err = syncJSONKeyIndexes(ctx, result, task, meta); err != nil {
				return err
			}
			// The SegmentMeta mutation is committed; schedule an asynchronous
			// DataView snapshot reconciliation instead of a synchronous event
			// publish (materialized-view model).
			if meta.dataViewManager != nil {
				meta.recomputeDataView(context.TODO(), task.GetCollectionId())
			}

			mlog.Info(ctx, "update copy segment info done",
				WrapCopySegmentTaskLog(task, mlog.Int64("segmentID", result.GetSegmentId()),
					mlog.Int64("importedRows", result.GetImportedRows()),
					mlog.Int("binlogFields", len(result.GetBinlogs())),
					mlog.Bool("hasManifestPath", result.GetManifestPath() != ""))...)
		}

		// Commit the terminal transition before reporting completion. A concurrent
		// job-failure path may already have moved the task out of InProgress.
		completeTs := uint64(time.Now().UnixNano())
		applied, err := copyMeta.UpdateTaskInState(ctx, task.GetTaskId(),
			datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
			UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskCompleted),
			updateCopyTaskCleanup(false),
			UpdateCopyTaskCompleteTs(completeTs))
		if err != nil {
			return err
		}
		if !applied {
			mlog.Info(ctx, "discarding the final completed transition for a copy segment task no longer in progress",
				WrapCopySegmentTaskLog(task)...)
			return nil
		}

		copyingDuration := task.GetTR().RecordSpan()
		metrics.CopySegmentTaskLatency.WithLabelValues(metrics.Executing).Observe(float64(copyingDuration.Milliseconds()))
		// Record total latency (from task creation to completion)
		totalDuration := task.GetTR().ElapseSpan()
		metrics.CopySegmentTaskLatency.WithLabelValues(metrics.Done).Observe(float64(totalDuration.Milliseconds()))
		mlog.Info(ctx, "copy segment task completed",
			WrapCopySegmentTaskLog(task,
				mlog.Duration("taskTimeCost/copying", copyingDuration),
				mlog.Duration("taskTimeCost/total", totalDuration))...)
		return nil

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

// failCopySegmentSync settles a completed result whose manifest verification
// failed closed, before anything is published: the task and its job are marked
// failed here so the rejection is durable even when this helper is reached
// outside QueryTaskOnWorker; the error is still returned, and the caller's own
// marking is idempotent on an already failed task.
func failCopySegmentSync(ctx context.Context, task CopySegmentTask, err error) error {
	if concrete, ok := task.(*copySegmentTask); ok {
		concrete.markTaskAndJobFailed(err.Error())
	} else {
		mlog.Warn(ctx, "rejected copy segment result on a task without a failure path",
			WrapCopySegmentTaskLog(task, mlog.Err(err))...)
	}
	return err
}

// verifyCopiedManifestIndexOwnership binds each published build to its actual
// target index ID. Build-ID acknowledgements alone cannot prove which definition
// a worker used: that definition may have been dropped and recreated during the
// copy. Read non-empty manifests back, including current-worker results, and
// compare their identities and paths before the segment becomes visible.
func verifyCopiedManifestIndexOwnership(ctx context.Context, result *datapb.CopySegmentResult,
	task CopySegmentTask, meta *meta,
) (map[int64]int64, error) {
	manifestPath := result.GetManifestPath()
	if manifestPath == "" || meta == nil {
		return nil, nil
	}
	segment := meta.GetSegment(ctx, result.GetSegmentId())
	if segment == nil || segment.GetStorageVersion() < storage.StorageV3 {
		return nil, nil
	}
	if result.ManifestIndexRewritten != nil {
		if !result.GetManifestIndexRewritten() {
			return nil, merr.WrapErrServiceInternalMsg(
				"copied manifest index rewrite was not completed for segment %d", result.GetSegmentId())
		}
		if len(result.GetManifestIndexBuildIds()) == 0 {
			// Current workers explicitly acknowledge an empty index section.
			return nil, nil
		}
	}
	storageConfig := createStorageConfig()
	entries, err := meta.readManifestIndexes(ctx, manifestPath, storageConfig)
	if err != nil {
		return nil, merr.Wrapf(err, "failed to read back copied manifest indexes for segment %d", result.GetSegmentId())
	}
	rootPath := storageConfig.GetRootPath()
	if meta.chunkManager != nil {
		rootPath = meta.chunkManager.RootPath()
	}
	definitions := make(map[int64]*model.Index)
	if meta.indexMeta != nil {
		for _, index := range meta.indexMeta.GetIndexesForCollection(task.GetCollectionId(), "") {
			definitions[index.IndexID] = index
		}
	}
	copied := make(map[int64]*datapb.VectorScalarIndexInfo, len(result.GetIndexInfos()))
	for _, info := range result.GetIndexInfos() {
		copied[info.GetBuildId()] = info
	}
	publishedBuilds := make(map[int64]int64, len(entries))
	for _, entry := range entries {
		definition := definitions[entry.IndexID]
		if definition == nil || definition.IndexName != entry.IndexName || definition.FieldID != entry.FieldID {
			return nil, merr.WrapErrServiceInternalMsg(
				"copied manifest of segment %d carries index %d foreign to the current target definition; retry the restore",
				result.GetSegmentId(), entry.IndexID)
		}
		expectedParams := common.KeyValuePairs(definition.TypeParams).ToMap()
		for key, value := range common.KeyValuePairs(definition.IndexParams).ToMap() {
			expectedParams[key] = value
		}
		if entry.IndexType != GetIndexType(definition.IndexParams) || !maps.Equal(entry.Properties, expectedParams) {
			return nil, merr.WrapErrServiceInternalMsg(
				"copied manifest index %d has changed target parameters; retry the restore", entry.IndexID)
		}
		if _, ok := manifestIndexFilePathInfoForSegment(rootPath, segment.SegmentInfo, entry); !ok {
			return nil, merr.Wrapf(merr.ErrDataIntegrity,
				"copied manifest of segment %d has an invalid artifact path for build %d", result.GetSegmentId(), entry.BuildID)
		}
		info := copied[entry.BuildID]
		if info == nil || info.GetIndexName() != entry.IndexName || info.GetFieldId() != entry.FieldID ||
			info.GetVersion() != entry.IndexVersion || info.GetIndexStorePathVersion() != entry.IndexStorePathVersion ||
			!slices.Equal(slices.Sorted(slices.Values(info.GetIndexFilePaths())), slices.Sorted(slices.Values(entry.IndexFileKeys))) {
			return nil, merr.WrapErrServiceInternalMsg(
				"copied manifest of segment %d does not match copied artifact build %d", result.GetSegmentId(), entry.BuildID)
		}
		if _, exists := publishedBuilds[entry.BuildID]; exists {
			return nil, merr.Wrapf(merr.ErrDataIntegrity, "copied manifest repeats build %d", entry.BuildID)
		}
		publishedBuilds[entry.BuildID] = entry.IndexID
	}
	if result.ManifestIndexRewritten != nil {
		acknowledged := make(map[int64]struct{}, len(result.GetManifestIndexBuildIds()))
		for _, buildID := range result.GetManifestIndexBuildIds() {
			if _, ok := publishedBuilds[buildID]; !ok {
				return nil, merr.WrapErrServiceInternalMsg("copied manifest is missing acknowledged build %d", buildID)
			}
			acknowledged[buildID] = struct{}{}
		}
		if len(acknowledged) != len(publishedBuilds) {
			return nil, merr.WrapErrServiceInternalMsg("copied manifest contains unacknowledged indexes")
		}
	}
	return publishedBuilds, nil
}

// validateCopiedManifestIndexPlacement checks the worker result against the
// placement persisted before dispatch. It runs before the target segment is
// made Flushed, so a mismatched or old worker cannot publish a visible segment
// and only then fail index synchronization.
func validateCopiedManifestIndexPlacement(result *datapb.CopySegmentResult,
	task CopySegmentTask, meta *meta, publishedBuilds map[int64]int64,
) error {
	writeToManifest, known := task.GetIndexWriteToManifest()
	if !known {
		// Pre-upgrade tasks have no dispatch-time placement. The durable result
		// (acknowledgement or compatibility read-back) remains authoritative.
		return nil
	}
	if !writeToManifest && len(publishedBuilds) > 0 {
		return merr.WrapErrServiceInternalMsg(
			"copied manifest of segment %d contains index entries while manifest publication was disabled at dispatch",
			result.GetSegmentId())
	}
	if !writeToManifest || meta == nil || meta.indexMeta == nil {
		return nil
	}
	// Placement is recorded per task, which can also contain legacy segments.
	// Those segments have no manifest and always retain their etcd index records.
	segment := meta.GetSegment(context.TODO(), result.GetSegmentId())
	if segment == nil || segment.GetStorageVersion() < storage.StorageV3 {
		return nil
	}
	targetIndexNames := make(map[string]struct{})
	for _, index := range meta.indexMeta.GetIndexesForCollection(task.GetCollectionId(), "") {
		if index != nil && !index.IsDeleted {
			targetIndexNames[index.IndexName] = struct{}{}
		}
	}
	for _, indexInfo := range result.GetIndexInfos() {
		if len(indexInfo.GetIndexFilePaths()) == 0 {
			continue
		}
		if _, exists := targetIndexNames[indexInfo.GetIndexName()]; !exists {
			continue
		}
		if _, published := publishedBuilds[indexInfo.GetBuildId()]; !published {
			return merr.WrapErrServiceInternalMsg(
				"copied manifest of segment %d carries no entry for index %q (buildID %d) while manifest publication was enabled at dispatch; "+
					"the DataNode that ran the copy likely predates manifest index republication - upgrade it and retry the restore",
				result.GetSegmentId(), indexInfo.GetIndexName(), indexInfo.GetBuildId())
		}
	}
	return nil
}

// buildCopySegmentTargetIndexes projects the target collection's index
// definitions into the per-index metadata the worker needs to record a copied
// artifact in the target manifest.
//
// Index identity is remapped across the snapshot boundary: RestoreIndexes()
// allocates new index IDs, and indexName is the only stable key (one JSON field
// can carry several indexes on different paths, so fieldID cannot be used). The
// worker knows the name it copied and nothing else about the target, so the
// mapping is resolved here and shipped with the request.
func buildCopySegmentTargetIndexes(m *meta, schema *schemapb.CollectionSchema, collectionID int64, writeToManifest bool) []*datapb.CopySegmentTargetIndex {
	// Strict exclusivity with the etcd record: with manifest publication off,
	// the copied segment's index records go to etcd (syncVectorScalarIndexes)
	// and its manifest must gain no index entry. An empty map makes the worker
	// miss every definition lookup, so it retracts the inherited entries and
	// writes none of its own.
	if !writeToManifest {
		return nil
	}
	if m == nil || m.indexMeta == nil {
		return nil
	}
	indexes := m.indexMeta.GetIndexesForCollection(collectionID, "")
	if len(indexes) == 0 {
		return nil
	}
	targetIndexes := make([]*datapb.CopySegmentTargetIndex, 0, len(indexes))
	for _, index := range indexes {
		if index == nil || index.IsDeleted {
			continue
		}
		indexParams := m.indexMeta.GetIndexParams(collectionID, index.IndexID)
		properties := common.KeyValuePairs(m.indexMeta.GetTypeParams(collectionID, index.IndexID)).ToMap()
		for key, value := range common.KeyValuePairs(indexParams).ToMap() {
			properties[key] = value
		}
		targetIndexes = append(targetIndexes, &datapb.CopySegmentTargetIndex{
			IndexName:  index.IndexName,
			IndexId:    index.IndexID,
			FieldId:    index.FieldID,
			ColumnName: schemaFieldName(schema, index.FieldID),
			IndexType:  GetIndexType(indexParams),
			Properties: properties,
		})
	}
	return targetIndexes
}

// taskIndexWriteToManifest returns the placement captured for the task. The
// fallback keeps direct callers and persisted pre-upgrade tasks compatible;
// scheduler dispatch always persists the fallback before issuing the RPC.
func taskIndexWriteToManifest(task CopySegmentTask) (bool, bool) {
	if enabled, ok := task.GetIndexWriteToManifest(); ok {
		return enabled, true
	}
	return writeSegmentIndexToManifest(), false
}

func cloneOptionalBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	return proto.Bool(*value)
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
//
// Returns:
//   - nil on success
//   - error on failure
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
	task CopySegmentTask, meta *meta, publishedBuilds map[int64]int64,
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
	// A copy target records its indexes in its own manifest: the worker retracts
	// the entries inherited from the source and writes the target's own in the
	// manifest revision this result points at. publishedBuilds comes from the
	// verified manifest read-back, so publication is decided per record, not per manifest pointer.
	// A pointer alone proves nothing: a DataNode that predates republication
	// returns one whose index section is empty, and marking those records
	// published would create a record that cannot be recovered on restart.
	storageV3 := false
	if meta.segments != nil {
		// StorageV3 bundles intentionally omit legacy PB insert binlogs, so
		// DataNode cannot derive row count from EntriesNum. The target segment was
		// pre-registered from snapshot metadata and remains the authoritative value.
		if segment := meta.GetSegment(ctx, result.GetSegmentId()); segment != nil {
			numRows = segment.GetNumOfRows()
			storageV3 = segment.GetStorageVersion() >= storage.StorageV3
		}
	}
	// Sync each vector/scalar index
	for _, indexInfo := range result.GetIndexInfos() {
		// Manifest artifacts retain their verified index ID. Legacy records
		// resolve by name to support multiple JSON path indexes on one field.
		targetIndexID, published := publishedBuilds[indexInfo.GetBuildId()]
		if !published {
			targetIndexID = indexNameToTargetID[indexInfo.GetIndexName()]
		}
		// A manifest-resident result keeps the verified dispatch-time ID even
		// if a same-name definition is replaced after validation.
		if targetIndexID == 0 {
			mlog.Warn(ctx, "no index definition found for index name in target collection, skip syncing",
				WrapCopySegmentTaskLog(task,
					mlog.String("indexName", indexInfo.GetIndexName()),
					mlog.FieldFieldID(indexInfo.GetFieldId()),
					mlog.Int64("sourceIndexID", indexInfo.GetIndexId()))...)
			continue
		}

		hasArtifact := len(indexInfo.GetIndexFilePaths()) > 0
		manifestResident := storageV3 && published && hasArtifact

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

		var err error
		if manifestResident {
			err = meta.indexMeta.AddSegmentIndexFromManifest(ctx, segIndex)
		} else {
			err = meta.indexMeta.AddSegmentIndex(ctx, segIndex)
		}
		if err != nil {
			mlog.Warn(ctx, "failed to add segment index",
				WrapCopySegmentTaskLog(task,
					mlog.FieldSegmentID(result.GetSegmentId()),
					mlog.String("indexName", indexInfo.GetIndexName()),
					mlog.FieldIndexID(targetIndexID),
					mlog.Err(err))...)

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
//  2. Log success or return the update error
//
// Parameters:
//   - ctx: Context for cancellation
//   - result: Segment copy result from DataNode
//   - task: Copy segment task
//   - meta: Metadata manager for updating segment
//
// Returns:
//   - nil on success
//   - error on failure
//
// Text indexes:
// - Full-text search indexes for VARCHAR fields
// - Stored inline with segment metadata (not in indexMeta)
// - Enables text search queries on restored collection
func syncTextIndexes(ctx context.Context, result *datapb.CopySegmentResult,
	task CopySegmentTask, meta *meta,
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
//  2. Log success or return the update error
//
// Parameters:
//   - ctx: Context for cancellation
//   - result: Segment copy result from DataNode
//   - task: Copy segment task
//   - meta: Metadata manager for updating segment
//
// Returns:
//   - nil on success
//   - error on failure
//
// JSON key indexes:
// - Indexes on keys within JSON fields
// - Stored inline with segment metadata (not in indexMeta)
// - Enables efficient queries on JSON field contents
func syncJSONKeyIndexes(ctx context.Context, result *datapb.CopySegmentResult,
	task CopySegmentTask, meta *meta,
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

		return err
	}

	mlog.Info(ctx, "synced json key indexes",
		WrapCopySegmentTaskLog(task,
			mlog.FieldSegmentID(result.GetSegmentId()),
			mlog.Int("count", len(result.GetJsonKeyIndexInfos())))...)
	return nil
}
