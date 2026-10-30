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
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
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
		if enabled {
			t.(*copySegmentTask).task.Load().IndexStorePathVersion = indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED.Enum()
		}
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
	GetIndexStorePathVersion() (indexpb.IndexStorePathVersion, bool)
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

// Cached descriptions and the lookup table are immutable after publication.
// Job clones share this cache; terminal jobs release it and recovery reloads it.
type copySegmentSnapshotCache struct {
	mu           sync.Mutex
	data         *snapshotstorage.SnapshotData
	segmentsByID map[int64]*datapb.SegmentDescription
}

func (c *copySegmentSnapshotCache) load(
	loader func() (*snapshotstorage.SnapshotData, error),
) (*snapshotstorage.SnapshotData, map[int64]*datapb.SegmentDescription, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.data == nil {
		data, err := loader()
		if err != nil {
			return nil, nil, err
		}
		c.data = data
	}
	if c.segmentsByID == nil {
		c.segmentsByID = make(map[int64]*datapb.SegmentDescription, len(c.data.Segments))
		for _, segment := range c.data.Segments {
			c.segmentsByID[segment.GetSegmentId()] = segment
		}
	}
	return c.data, c.segmentsByID, nil
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

func (t *copySegmentTask) GetIndexStorePathVersion() (indexpb.IndexStorePathVersion, bool) {
	task := t.task.Load()
	return task.GetIndexStorePathVersion(), task.IndexStorePathVersion != nil
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
// would leave memory and etcd out of sync.
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

// GetTaskVersion returns the task version for optimistic concurrency control.
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
//  5. Send request to DataNode via cluster.CreateCopySegment
//  6. Update task state to InProgress with assigned node ID
//  7. Record pending duration metric
//
// Parameters:
//   - nodeID: ID of DataNode selected by scheduler
//   - cluster: Cluster session manager for RPC communication
//
// Error handling:
//   - Permanent snapshot assembly errors mark the task and job failed
//   - An external task rejected as unsupported marks the task and job failed
//   - Other transient assembly or DataNode RPC errors leave the task Pending
//     so the scheduler can retry it on a later cycle
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
	err = cluster.CreateCopySegment(nodeID, req, t.GetCollectionId(), job.GetExternal())
	if err != nil {
		mlog.Warn(ctx, "failed to create copy segment task on datanode",
			WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
		version, _ := t.GetIndexStorePathVersion()
		if (job.GetExternal() || metautil.IsSegmentRooted(version)) && errors.Is(err, merr.ErrServiceUnimplemented) {
			t.markTaskAndJobFailed(merr.Wrap(err,
				"datanode does not support the requested copy segment task").Error())
		}
		return
	}
	mlog.Info(ctx, "create copy segment task on datanode done",
		WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID))...)
	err = t.copyMeta.UpdateTask(ctx, t.GetTaskId(),
		UpdateCopyTaskNodeID(nodeID),
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskInProgress))
	if err != nil {
		mlog.Warn(ctx, "failed to update copy segment task state",
			WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
		return
	}
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
func (t *copySegmentTask) markTaskAndJobFailed(reason string) {
	updateErr := t.copyMeta.UpdateTask(context.TODO(), t.GetTaskId(),
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed),
		UpdateCopyTaskReason(reason))
	if updateErr != nil {
		mlog.Warn(context.TODO(), "failed to update copy segment task state to failed",
			WrapCopySegmentTaskLog(t, mlog.Err(updateErr))...)
		return
	}

	// Sync job state immediately (fail-fast)
	job := t.copyMeta.GetJob(context.TODO(), t.GetJobId())
	if job != nil && job.GetState() != datapb.CopySegmentJobState_CopySegmentJobFailed {
		updateErr = t.copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), t.GetJobId(),
			UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
			UpdateCopyJobReason(reason))
		if updateErr != nil {
			mlog.Warn(t.ctx, "failed to update job state to Failed",
				mlog.FieldJobID(t.GetJobId()), mlog.Err(updateErr))
		}
	}
	mlog.Warn(context.TODO(), "copy segment task failed",
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
//   - A query RPC error is either a transient transport failure or a confirmed loss of the
//     worker-side task; the two must be handled differently (see isCopyTaskLostOnWorker):
//     confirmed loss resets the task to Pending for re-dispatch, transient errors keep the
//     task InProgress so the next check round simply queries again
//   - Re-dispatch is the only way a node restart gets retried: the scheduler only
//     re-dispatches Pending(Init) tasks, never ones left InProgress
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
	req := &datapb.QueryCopySegmentRequest{
		TaskID: t.GetTaskId(),
	}
	resp, err := cluster.QueryCopySegment(nodeID, req)
	// Handle RPC error separately to avoid nil resp dereference.
	if err != nil {
		if !isCopyTaskLostOnWorker(err) {
			// Transient transport failure (network blip, RPC timeout, node briefly
			// not ready). The worker-side task may well still be running, so keep
			// the task InProgress and let the next check round query again.
			// Resetting here would re-dispatch a task that is possibly still
			// executing on a live node, starting a concurrent duplicate copy.
			mlog.Warn(context.TODO(), "transient error querying copy segment task on datanode, will retry",
				WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
			return
		}
		// An admitted completion may have published some targets before a crash.
		// Worker loss cannot turn it into a fresh attempt while its old cleanup
		// intent is armed. Fail it and retain the plan for ordinary GC instead.
		if t.GetCleanupRequired() || (t.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskFailed && len(t.GetCleanupPrefixes()) > 0) {
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
		// Confirmed loss: the worker-side task no longer exists (DataNode
		// restarted/replaced, or its in-memory task manager lost the task).
		// Leaving the task InProgress would make the scheduler poll a dead
		// node until the job-level timeout, since only Pending tasks are
		// re-dispatched. Reset to Pending with NullNodeID so the scheduler
		// re-dispatches it to a live node.
		// Re-dispatch is idempotent: target binlog paths are deterministic
		// transforms of the source paths (same content on overwrite), and each
		// dispatch allocates fresh buildIDs, so index files from a partial
		// earlier attempt are never referenced by meta and are removed by GC.
		if resetErr := t.copyMeta.UpdateTask(context.TODO(), t.GetTaskId(),
			UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskPending),
			UpdateCopyTaskNodeID(NullNodeID)); resetErr != nil {
			mlog.Warn(context.TODO(), "failed to reset copy segment task to pending after worker loss",
				WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(resetErr))...)
			return
		}
		mlog.Info(context.TODO(), "reset copy segment task to pending due to worker loss, will re-dispatch",
			WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
		return
	}

	// Handle task execution failure (resp is guaranteed non-nil here)
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
	err = SyncCopySegmentTask(t, resp, t.copyMeta, t.meta)
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

	mlog.Info(context.TODO(), "query copy segment task",
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
// Error handling:
// - Logs warning but does not retry (task will be GC'd eventually)
// - Non-critical operation (task already finished)
func (t *copySegmentTask) DropTaskOnWorker(cluster session.Cluster) {
	nodeID := t.GetNodeId()
	if nodeID == NullNodeID {
		return
	}
	err := cluster.DropCopySegment(nodeID, t.GetTaskId())
	if err != nil {
		mlog.Warn(context.TODO(), "failed to drop copy segment task on datanode",
			WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
		return
	}
	if err := t.copyMeta.UpdateTask(t.ctx, t.GetTaskId(), UpdateCopyTaskNodeID(NullNodeID)); err != nil {
		mlog.Warn(t.ctx, "failed to clear copy worker assignment", mlog.FieldTaskID(t.GetTaskId()), mlog.Err(err))
	}
	mlog.Info(context.TODO(), "drop copy segment task on datanode done",
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
			pathVersion := info.GetIndexStorePathVersion()
			if target.IndexStorePathVersion != nil {
				pathVersion = target.GetIndexStorePathVersion()
			}
			builder := metautil.NewIndexPathBuilder(root, pathVersion, target.GetCollectionId(), target.GetPartitionId(), target.GetSegmentId(), buildID, info.GetIndexVersion())
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if job == nil {
		return nil, merr.WrapErrServiceInternalMsg(
			"copy segment job %d not found while assembling task %d",
			t.GetJobId(),
			t.GetTaskId(),
		)
	}

	// Read complete snapshot data from S3 to retrieve source segment binlogs
	var (
		snapshotData     *snapshotstorage.SnapshotData
		sourceSegmentMap map[int64]*datapb.SegmentDescription
		err              error
	)
	concreteJob, ok := job.(*copySegmentJob)
	if !ok || concreteJob.snapshotCache == nil {
		return nil, merr.WrapErrServiceInternalMsg(
			"copy segment job %d has no snapshot cache",
			job.GetJobId(),
		)
	}
	snapshotData, sourceSegmentMap, err = concreteJob.snapshotCache.load(func() (*snapshotstorage.SnapshotData, error) {
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
		mlog.Error(context.TODO(), "failed to read snapshot data for copy segment task",
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
	targetIndexes := buildCopySegmentTargetIndexes(t.meta, job.GetCollectionId(), writeIndexesToManifest)

	// Reserve one million IDs in a task-local range rather than allocate per build.
	// Refill exhausted ranges with the same size. Allocate lazily so index-free
	// tasks need no IDs; unused IDs are discarded after assembly.
	const buildIDBatchSize int64 = 1_000_000
	var nextBuildID, endBuildID int64
	takeBuildID := func() (int64, error) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if nextBuildID == endBuildID {
			start, end, err := t.alloc.AllocN(buildIDBatchSize)
			if err != nil {
				return 0, err
			}
			// AllocN owns its bounded timeout rather than the task context.
			// Never use its range to dispatch a task canceled during allocation.
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			nextBuildID, endBuildID = start, end
		}
		id := nextBuildID
		nextBuildID++
		return id, nil
	}

	// Avro IndexFiles is the complete snapshot index inventory, including an
	// empty list. Never recover additional indexes from a native manifest.
	for _, mapping := range idMappings {
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
		targetSegID := mapping.GetTargetSegmentId()
		partitionID := mapping.GetPartitionId()
		sources = append(sources, source)

		// Collect all unique source build IDs from index files and allocate new ones
		// to avoid buildID reuse across copy segments, which would corrupt the
		// 1:1 segmentBuildInfo map in DataCoord indexMeta.
		newBuildIDs := make(map[int64]int64)
		allocNewBuildID := func(srcBuildID int64) error {
			if _, exists := newBuildIDs[srcBuildID]; !exists {
				newID, err := takeBuildID()
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
		if version, known := task.GetIndexStorePathVersion(); known && source.GetStorageVersion() == storage.StorageV3 {
			target.IndexStorePathVersion = version.Enum()
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
		CopyConcurrency: Params.DataCoordCfg.SnapshotCopyConcurrency.GetAsInt32(),
		TargetIndexes:   targetIndexes,
		ClusterID:       Params.CommonCfg.ClusterPrefix.GetValue(),
		JobID:           task.GetJobId(),
		TaskID:          task.GetTaskId(),
		Sources:         sources,
		Targets:         targets,
		StorageConfig:   storageConfig,
		TaskSlot:        task.GetTaskSlot(),
		ExternalSpec:    job.GetExternalSpec(),
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
func SyncCopySegmentTask(task CopySegmentTask, resp *datapb.QueryCopySegmentResponse, copyMeta CopySegmentMeta, meta *meta) error {
	ctx := context.TODO()

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
		// A failed task with a cleanup plan may already have lost its files,
		// including after restart when CleanupRequired has been cleared.
		if len(task.GetCleanupPrefixes()) > 0 && task.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskFailed {
			return merr.WrapErrServiceInternalMsg("cannot publish a failed copy task with planned cleanup")
		}
		results := resp.GetSegmentResults()
		lookup := buildCopySegmentIndexLookup(task, meta)
		verified := make([]map[int64]int64, len(results))
		io := packed.NewManifestIOContext(min(max(1, len(results)), segmentIndexManifestReadConcurrency()))
		err := runManifestBatch(ctx, len(results), func(ctx context.Context, i int, complete func(error)) error {
			return submitCopiedManifestIndexOwnership(ctx, io, results[i], task, meta, func(builds map[int64]int64, err error) {
				verified[i] = builds
				complete(err)
			})
		})
		io.Close()
		if err != nil {
			return failCopySegmentSync(ctx, task, copyMeta, err)
		}
		for i, result := range results {
			if err := validateCopiedManifestIndexPlacement(result, task, meta, verified[i], lookup); err != nil {
				return failCopySegmentSync(ctx, task, copyMeta, err)
			}
		}
		for i, result := range results {
			id := result.GetSegmentId()
			// Persist complete metadata before installing indexes or exposing data.
			operators := []UpdateOperator{
				UpdateBinlogsOperator(id, result.GetBinlogs(), result.GetStatslogs(), result.GetDeltalogs(), result.GetBm25Logs()),
				UpdateStatusOperator(id, commonpb.SegmentState_Flushed),
				UpdateIsImporting(id, true),
			}
			if manifest := result.GetManifestPath(); manifest != "" {
				operators = append(operators, UpdateManifest(id, manifest))
				if len(verified[i]) > 0 {
					// Keep the pointer and recovery marker in the same persisted record.
					operators = append(operators, UpdateManifestHasIndex(id))
				}
			}
			if len(result.GetTextIndexInfos()) > 0 || len(result.GetJsonKeyIndexInfos()) > 0 {
				operators = append(operators, UpdateTextAndJSONIndexLogs(id, result.GetTextIndexInfos(), result.GetJsonKeyIndexInfos()))
			}
			if err := meta.UpdateSegmentsInfo(ctx, operators...); err != nil {
				return failCopySegmentSync(ctx, task, copyMeta, err)
			}
			if err := syncVectorScalarIndexes(ctx, result, task, meta, verified[i], lookup); err != nil {
				return failCopySegmentSync(ctx, task, copyMeta, err)
			}
			if err := meta.UpdateSegmentsInfo(ctx, UpdateIsImporting(id, false)); err != nil {
				return failCopySegmentSync(ctx, task, copyMeta, err)
			}
			// Reconcile successful segments even if a later result fails.
			meta.recomputeDataView(ctx, task.GetCollectionId())
			// Enqueue the published segment to build indexes missing from the snapshot.
			notifySegmentIndexBuild(id)
			notifySegmentStatsBuild(id)
		}

		// Mark task as completed and record copying duration
		completeTs := uint64(time.Now().UnixNano())
		copyingDuration := task.GetTR().RecordSpan()
		metrics.CopySegmentTaskLatency.WithLabelValues(metrics.Executing).Observe(float64(copyingDuration.Milliseconds()))
		// Record total latency (from task creation to completion)
		totalDuration := task.GetTR().ElapseSpan()
		metrics.CopySegmentTaskLatency.WithLabelValues(metrics.Done).Observe(float64(totalDuration.Milliseconds()))
		mlog.Info(context.TODO(), "copy segment task completed",
			WrapCopySegmentTaskLog(task,
				mlog.Duration("taskTimeCost/copying", copyingDuration),
				mlog.Duration("taskTimeCost/total", totalDuration))...)

		return copyMeta.UpdateTask(ctx, task.GetTaskId(),
			UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskCompleted),
			updateCopyTaskCleanup(false),
			UpdateCopyTaskCompleteTs(completeTs))

	case datapb.CopySegmentTaskState_CopySegmentTaskFailed:
		return copyMeta.UpdateTask(ctx, task.GetTaskId(),
			UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed),
			UpdateCopyTaskReason(resp.GetReason()))
	}
	return nil
}

func failCopySegmentSync(ctx context.Context, task CopySegmentTask, copyMeta CopySegmentMeta, err error) error {
	// On error, mark task and job as failed
	updateErr := copyMeta.UpdateTask(ctx, task.GetTaskId(),
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed),
		UpdateCopyTaskReason(err.Error()))
	if updateErr != nil {
		mlog.Warn(ctx, "failed to update task state to Failed",
			mlog.FieldTaskID(task.GetTaskId()), mlog.Err(updateErr))
	}

	updateErr = copyMeta.UpdateJobStateAndReleaseRef(ctx, task.GetJobId(),
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
		UpdateCopyJobReason(err.Error()))
	if updateErr != nil {
		mlog.Warn(ctx, "failed to update job state to Failed",
			mlog.FieldJobID(task.GetJobId()), mlog.Err(updateErr))
	}

	mlog.Warn(ctx, "update copy segment binlogs failed",
		WrapCopySegmentTaskLog(task, mlog.String("err", err.Error()))...)
	return err
}

// submitCopiedManifestIndexOwnership binds each published build to its actual
// target index ID. Build-ID acknowledgements alone cannot prove which definition
// a worker used: that definition may have been dropped and recreated during the
// copy. Read non-empty manifests back, including current-worker results, and
// compare their identities and paths before the segment becomes visible.
func submitCopiedManifestIndexOwnership(ctx context.Context, io *packed.ManifestIOContext, result *datapb.CopySegmentResult,
	task CopySegmentTask, meta *meta, complete func(map[int64]int64, error),
) error {
	manifestPath := result.GetManifestPath()
	if manifestPath == "" || meta == nil {
		complete(nil, nil)
		return nil
	}
	segment := meta.GetSegment(ctx, result.GetSegmentId())
	if segment == nil || segment.GetStorageVersion() < storage.StorageV3 {
		complete(nil, nil)
		return nil
	}
	if result.ManifestIndexRewritten != nil {
		if !result.GetManifestIndexRewritten() {
			return merr.WrapErrServiceInternalMsg(
				"copied manifest index rewrite was not completed for segment %d", result.GetSegmentId())
		}
		if len(result.GetManifestIndexBuildIds()) == 0 {
			// Current workers explicitly acknowledge an empty index section.
			complete(nil, nil)
			return nil
		}
	}
	storageConfig := createStorageConfig()
	return meta.submitManifestIndexRead(ctx, io, manifestPath, storageConfig, func(entries []packed.ManifestIndexInfo, err error) {
		if err != nil {
			complete(nil, merr.Wrapf(err, "failed to read back copied manifest indexes for segment %d", result.GetSegmentId()))
			return
		}
		builds, err := verifyCopiedManifestIndexOwnership(result, task, meta, segment, storageConfig.GetRootPath(), entries)
		complete(builds, err)
	})
}

// Verify the loaded entries without further I/O before publishing any segment.
func verifyCopiedManifestIndexOwnership(result *datapb.CopySegmentResult, task CopySegmentTask, meta *meta,
	segment *SegmentInfo, rootPath string, entries []packed.ManifestIndexInfo,
) (map[int64]int64, error) {
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
	task CopySegmentTask, meta *meta, publishedBuilds map[int64]int64, lookup copySegmentIndexLookup,
) error {
	writeToManifest, known := task.GetIndexWriteToManifest()
	if !known {
		// Pre-upgrade tasks have no dispatch-time placement. The durable result
		// manifest verification remains authoritative.
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
	for _, indexInfo := range result.GetIndexInfos() {
		if len(indexInfo.GetIndexFilePaths()) == 0 {
			continue
		}
		if _, exists := lookup.indexIDByName[indexInfo.GetIndexName()]; !exists {
			continue
		}
		if expected, known := task.GetIndexStorePathVersion(); known && indexInfo.GetIndexStorePathVersion() != expected {
			return merr.WrapErrServiceInternalMsg("copied index build %d returned path version %s instead of %s; upgrade the DataNode and retry the restore",
				indexInfo.GetBuildId(), indexInfo.GetIndexStorePathVersion(), expected)
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
func buildCopySegmentTargetIndexes(m *meta, collectionID int64, writeToManifest bool) []*datapb.CopySegmentTargetIndex {
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
			ColumnName: collectionFieldName(m, collectionID, index.FieldID),
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

// copySegmentIndexLookup is task-local: concurrent target index DDL during
// Restore is outside this protocol's supported scope.
type copySegmentIndexLookup struct {
	partitionBySegmentID map[int64]int64
	indexIDByName        map[string]int64
}

func buildCopySegmentIndexLookup(task CopySegmentTask, meta *meta) copySegmentIndexLookup {
	lookup := copySegmentIndexLookup{
		partitionBySegmentID: make(map[int64]int64, len(task.GetIdMappings())),
		indexIDByName:        make(map[string]int64),
	}
	for _, mapping := range task.GetIdMappings() {
		id := mapping.GetTargetSegmentId()
		if _, exists := lookup.partitionBySegmentID[id]; !exists {
			lookup.partitionBySegmentID[id] = mapping.GetPartitionId()
		}
	}
	if meta.indexMeta != nil {
		for _, index := range meta.indexMeta.GetIndexesForCollection(task.GetCollectionId(), "") {
			lookup.indexIDByName[index.IndexName] = index.IndexID
		}
	}
	return lookup
}

// syncVectorScalarIndexes installs one segment's records using task-local
// lookups. The caller owns publication ordering and task/job failure handling.
func syncVectorScalarIndexes(ctx context.Context, result *datapb.CopySegmentResult,
	task CopySegmentTask, meta *meta, publishedBuilds map[int64]int64, lookup copySegmentIndexLookup,
) error {
	if len(result.GetIndexInfos()) == 0 {
		return nil
	}
	partitionID := lookup.partitionBySegmentID[result.GetSegmentId()]
	numRows := result.GetImportedRows()
	// A copy target records its indexes in its own manifest: the worker retracts
	// the entries inherited from the source and writes the target's own in the
	// manifest revision this result points at. publishedBuilds comes from the
	// verified manifest contents, so publication is decided per record, not per pointer.
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
			targetIndexID = lookup.indexIDByName[indexInfo.GetIndexName()]
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
