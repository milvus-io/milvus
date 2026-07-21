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
	GetIdMappings() []*datapb.CopySegmentIDMapping // Lightweight ID mappings
	GetTR() *timerecord.TimeRecorder
	Clone() CopySegmentTask
}

// copySegmentTask implements CopySegmentTask with atomic state updates.
type copySegmentTask struct {
	task atomic.Pointer[datapb.CopySegmentTask] // Atomic pointer for concurrent access

	copyMeta     CopySegmentMeta          // For accessing job metadata and updating task state
	meta         *meta                    // For accessing segment metadata and collection schema
	snapshotMeta *snapshotMeta            // For accessing snapshot data (source binlogs)
	alloc        allocator.Allocator      // For allocating new build IDs to avoid buildID reuse
	tr           *timerecord.TimeRecorder // For measuring task duration (pending, executing, total)
	times        *taskcommon.Times        // For tracking task lifecycle timestamps
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
// - Logs warnings but does not retry (scheduler will retry on next cycle)
// - Task remains in Pending state if dispatch fails
//
// Why read snapshot on every dispatch:
// - Snapshot data contains full binlog paths needed for copy
// - Reading from S3 is necessary to populate CopySegmentRequest
// - Cached in snapshotMeta to avoid redundant reads
func (t *copySegmentTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	mlog.Info(context.TODO(), "processing pending copy segment task...", WrapCopySegmentTaskLog(t)...)
	job := t.copyMeta.GetJob(context.TODO(), t.GetJobId())
	req, err := AssembleCopySegmentRequest(t, job)
	if err != nil {
		mlog.Warn(context.TODO(), "failed to assemble copy segment request",
			WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
		return
	}
	err = cluster.CreateCopySegment(nodeID, req, t.GetCollectionId())
	if err != nil {
		mlog.Warn(context.TODO(), "failed to create copy segment task on datanode",
			WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
		return
	}
	mlog.Info(context.TODO(), "create copy segment task on datanode done",
		WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID))...)
	err = t.copyMeta.UpdateTask(context.TODO(), t.GetTaskId(),
		UpdateCopyTaskNodeID(nodeID),
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskInProgress))
	if err != nil {
		mlog.Warn(context.TODO(), "failed to update copy segment task state",
			WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
		return
	}
	// Record pending duration
	pendingDuration := t.GetTR().RecordSpan()
	metrics.CopySegmentTaskLatency.WithLabelValues(metrics.Pending).Observe(float64(pendingDuration.Milliseconds()))
	mlog.Info(context.TODO(), "copy segment task start to execute",
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
			mlog.Warn(context.TODO(), "failed to update job state to Failed",
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
		//
		// The reset is state-guarded (task still InProgress, parent job still
		// active): this response can arrive long after the query was issued, and
		// in the meantime another task may have failed the parent job. Reviving
		// the task to Pending then would let the scheduler issue one more
		// dispatch for an already-dead job before checkFailedJob converges it
		// back to Failed.
		applied, resetErr := t.copyMeta.UpdateTaskInStateIfJobActive(context.TODO(), t.GetTaskId(),
			datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
			UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskPending),
			UpdateCopyTaskNodeID(NullNodeID))
		if resetErr != nil {
			mlog.Warn(context.TODO(), "failed to reset copy segment task to pending after worker loss",
				WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(resetErr))...)
			return
		}
		if !applied {
			mlog.Info(context.TODO(), "skip resetting copy segment task after worker loss: task left InProgress or job no longer active",
				WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
			return
		}
		mlog.Info(context.TODO(), "reset copy segment task to pending due to worker loss, will re-dispatch",
			WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
		return
	}

	// Handle task execution failure (resp is guaranteed non-nil here)
	if resp.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskFailed {
		t.markTaskAndJobFailed(resp.GetReason())
		return
	}

	if resp.GetState() != datapb.CopySegmentTaskState_CopySegmentTaskCompleted {
		return
	}

	// Sync task state and binlog info
	err = SyncCopySegmentTask(t, resp, t.copyMeta, t.meta)
	if err != nil {
		t.markTaskAndJobFailed(fmt.Sprintf("failed to sync segment metadata: %v", err))
		return
	}

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
	err := cluster.DropCopySegment(nodeID, t.GetTaskId())
	if err != nil {
		mlog.Warn(context.TODO(), "failed to drop copy segment task on datanode",
			WrapCopySegmentTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
		return
	}
	mlog.Info(context.TODO(), "drop copy segment task on datanode done",
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
	ctx := context.Background()

	// Read complete snapshot data from S3 to retrieve source segment binlogs
	snapshotData, err := t.snapshotMeta.ReadSnapshotData(ctx, job.GetSourceCollectionId(), job.GetSnapshotName(), true)
	if err != nil {
		mlog.Error(context.TODO(), "failed to read snapshot data for copy segment task",
			append(WrapCopySegmentTaskLog(task), mlog.Err(err))...)
		return nil, err
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
			CollectionId: job.GetCollectionId(),
			PartitionId:  partitionID,
			SegmentId:    targetSegID,
			NewBuildIds:  newBuildIDs,
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
		StorageConfig: createStorageConfig(),
		TaskSlot:      task.GetTaskSlot(),
	}, nil
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
				// On error, mark task and job as failed
				updateErr := copyMeta.UpdateTask(ctx, task.GetTaskId(),
					UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed),
					UpdateCopyTaskReason(err.Error()))
				if updateErr != nil {
					mlog.Warn(context.TODO(), "failed to update task state to Failed",
						mlog.FieldTaskID(task.GetTaskId()), mlog.Err(updateErr))
				}

				updateErr = copyMeta.UpdateJobStateAndReleaseRef(ctx, task.GetJobId(),
					UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
					UpdateCopyJobReason(err.Error()))
				if updateErr != nil {
					mlog.Warn(context.TODO(), "failed to update job state to Failed",
						mlog.FieldJobID(task.GetJobId()), mlog.Err(updateErr))
				}

				mlog.Warn(context.TODO(), "update copy segment binlogs failed",
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

			mlog.Info(context.TODO(), "update copy segment info done",
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
		mlog.Info(context.TODO(), "copy segment task completed",
			WrapCopySegmentTaskLog(task,
				mlog.Duration("taskTimeCost/copying", copyingDuration),
				mlog.Duration("taskTimeCost/total", totalDuration))...)

		return copyMeta.UpdateTask(ctx, task.GetTaskId(),
			UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskCompleted),
			UpdateCopyTaskCompleteTs(completeTs))

	case datapb.CopySegmentTaskState_CopySegmentTaskFailed:
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
			NumRows:                   result.GetImportedRows(),
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

			// Mark task and job as failed
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

		// Mark task and job as failed
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

		// Mark task and job as failed
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
		return err
	}

	mlog.Info(ctx, "synced json key indexes",
		WrapCopySegmentTaskLog(task,
			mlog.FieldSegmentID(result.GetSegmentId()),
			mlog.Int("count", len(result.GetJsonKeyIndexInfos())))...)
	return nil
}
