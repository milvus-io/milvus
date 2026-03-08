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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
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
func (t *copySegmentTask) Clone() CopySegmentTask {
	cloned := &copySegmentTask{
		copyMeta:     t.copyMeta,
		meta:         t.meta,
		snapshotMeta: t.snapshotMeta,
		tr:           t.tr,
		times:        t.times,
	}
	cloned.task.Store(t.task.Load())
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
	log.Info("processing pending copy segment task...", WrapCopySegmentTaskLog(t)...)
	job := t.copyMeta.GetJob(context.TODO(), t.GetJobId())
	req, err := AssembleCopySegmentRequest(t, job)
	if err != nil {
		log.Warn("failed to assemble copy segment request",
			WrapCopySegmentTaskLog(t, zap.Int64("nodeID", nodeID), zap.Error(err))...)
		return
	}
	err = cluster.CreateCopySegment(nodeID, req)
	if err != nil {
		log.Warn("failed to create copy segment task on datanode",
			WrapCopySegmentTaskLog(t, zap.Int64("nodeID", nodeID), zap.Error(err))...)
		return
	}
	log.Info("create copy segment task on datanode done",
		WrapCopySegmentTaskLog(t, zap.Int64("nodeID", nodeID))...)
	err = t.copyMeta.UpdateTask(context.TODO(), t.GetTaskId(),
		UpdateCopyTaskNodeID(nodeID),
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskInProgress))
	if err != nil {
		log.Warn("failed to update copy segment task state",
			WrapCopySegmentTaskLog(t, zap.Int64("nodeID", nodeID), zap.Error(err))...)
		return
	}
	// Record pending duration
	pendingDuration := t.GetTR().RecordSpan()
	metrics.CopySegmentTaskLatency.WithLabelValues(metrics.Pending).Observe(float64(pendingDuration.Milliseconds()))
	log.Info("copy segment task start to execute",
		WrapCopySegmentTaskLog(t, zap.Int64("scheduledNodeID", nodeID),
			zap.Duration("taskTimeCost/pending", pendingDuration))...)
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
		log.Warn("failed to update copy segment task state to failed",
			WrapCopySegmentTaskLog(t, zap.Error(updateErr))...)
		return
	}

	// Sync job state immediately (fail-fast)
	job := t.copyMeta.GetJob(context.TODO(), t.GetJobId())
	if job != nil && job.GetState() != datapb.CopySegmentJobState_CopySegmentJobFailed {
		updateErr = t.copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), t.GetJobId(),
			UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
			UpdateCopyJobReason(reason))
		if updateErr != nil {
			log.Warn("failed to update job state to Failed",
				zap.Int64("jobID", t.GetJobId()), zap.Error(updateErr))
		}
	}
	log.Warn("copy segment task failed",
		WrapCopySegmentTaskLog(t, zap.String("reason", reason))...)
}

// QueryTaskOnWorker polls the DataNode for task execution status.
//
// Process flow:
//  1. Send QueryCopySegmentRequest to assigned DataNode
//  2. Check response state:
//     - Not Completed: Mark task/job as failed (fail-fast)
//     - Completed: Sync binlog and index metadata to segment
//  3. Update task state accordingly
//
// Failure handling:
// - Any error or non-completed state triggers immediate failure
// - Task failure immediately marks parent job as failed (fail-fast)
// - Enables quick feedback to user without waiting for timeout
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
	// Handle RPC error separately to avoid nil resp dereference
	if err != nil {
		t.markTaskAndJobFailed(fmt.Sprintf("query copy segment RPC failed: %v", err))
		return
	}

	// Handle task execution failure (resp is guaranteed non-nil here)
	if resp.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskFailed {
		t.markTaskAndJobFailed(resp.GetReason())
		return
	}

	if resp.GetState() != datapb.CopySegmentTaskState_CopySegmentTaskCompleted {
		log.Info("copy segment task not completed",
			WrapCopySegmentTaskLog(t, zap.String("state", resp.GetState().String()))...)
		return
	}

	// Sync task state and binlog info
	err = SyncCopySegmentTask(t, resp, t.copyMeta, t.meta)
	if err != nil {
		t.markTaskAndJobFailed(fmt.Sprintf("failed to sync segment metadata: %v", err))
		return
	}

	log.Info("query copy segment task",
		WrapCopySegmentTaskLog(t, zap.String("respState", resp.GetState().String()),
			zap.String("reason", resp.GetReason()))...)
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
		log.Warn("failed to drop copy segment task on datanode",
			WrapCopySegmentTaskLog(t, zap.Int64("nodeID", nodeID), zap.Error(err))...)
		return
	}
	log.Info("drop copy segment task on datanode done",
		WrapCopySegmentTaskLog(t, zap.Int64("nodeID", nodeID))...)
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
func WrapCopySegmentTaskLog(task CopySegmentTask, fields ...zap.Field) []zap.Field {
	res := []zap.Field{
		zap.Int64("taskID", task.GetTaskId()),
		zap.Int64("jobID", task.GetJobId()),
		zap.Int64("collectionID", task.GetCollectionId()),
		zap.String("state", task.GetState().String()),
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
	snapshotData, err := t.snapshotMeta.ReadSnapshotData(ctx, job.GetSnapshotName(), true)
	if err != nil {
		log.Error("failed to read snapshot data for copy segment task",
			append(WrapCopySegmentTaskLog(task), zap.Error(err))...)
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

	for _, mapping := range idMappings {
		sourceSegID := mapping.GetSourceSegmentId()
		targetSegID := mapping.GetTargetSegmentId()
		partitionID := mapping.GetPartitionId()

		// Get source segment description from snapshot
		sourceSegDesc, ok := sourceSegmentMap[sourceSegID]
		if !ok {
			log.Warn("source segment not found in snapshot",
				zap.Int64("sourceSegmentID", sourceSegID),
				zap.String("snapshotName", job.GetSnapshotName()))
			continue
		}

		// Build source with full binlog information
		source := &datapb.CopySegmentSource{
			CollectionId:      snapshotData.SnapshotInfo.GetCollectionId(),
			PartitionId:       sourceSegDesc.GetPartitionId(),
			SegmentId:         sourceSegDesc.GetSegmentId(),
			InsertBinlogs:     sourceSegDesc.GetBinlogs(),
			StatsBinlogs:      sourceSegDesc.GetStatslogs(),
			DeltaBinlogs:      sourceSegDesc.GetDeltalogs(),
			IndexFiles:        sourceSegDesc.GetIndexFiles(),        // vector/scalar index file info
			Bm25Binlogs:       sourceSegDesc.GetBm25Statslogs(),     // BM25 stats logs
			TextIndexFiles:    sourceSegDesc.GetTextIndexFiles(),    // Text index files
			JsonKeyIndexFiles: sourceSegDesc.GetJsonKeyIndexFiles(), // JSON key index files
			ManifestPath:      sourceSegDesc.GetManifestPath(),      // manifest path for StorageV3+
			StorageVersion:    sourceSegDesc.GetStorageVersion(),    // storage version for binlog format decision
		}
		sources = append(sources, source)

		// Build target with only IDs (binlog paths will be generated during copy)
		target := &datapb.CopySegmentTarget{
			CollectionId: job.GetCollectionId(),
			PartitionId:  partitionID,
			SegmentId:    targetSegID,
		}
		log.Info("prepare copy segment source and target", zap.Any("source", sourceSegDesc), zap.Any("target", target))
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
			operators := []UpdateOperator{op1, op2}
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
					log.Warn("failed to update task state to Failed",
						zap.Int64("taskID", task.GetTaskId()), zap.Error(updateErr))
				}

				updateErr = copyMeta.UpdateJobStateAndReleaseRef(ctx, task.GetJobId(),
					UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
					UpdateCopyJobReason(err.Error()))
				if updateErr != nil {
					log.Warn("failed to update job state to Failed",
						zap.Int64("jobID", task.GetJobId()), zap.Error(updateErr))
				}

				log.Warn("update copy segment binlogs failed",
					WrapCopySegmentTaskLog(task, zap.String("err", err.Error()))...)
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
			if err = syncJsonKeyIndexes(ctx, result, task, meta, copyMeta); err != nil {
				return err
			}

			log.Info("update copy segment info done",
				WrapCopySegmentTaskLog(task, zap.Int64("segmentID", result.GetSegmentId()),
					zap.Any("segmentResult", result))...)
		}

		// Mark task as completed and record copying duration
		completeTs := uint64(time.Now().UnixNano())
		copyingDuration := task.GetTR().RecordSpan()
		metrics.CopySegmentTaskLatency.WithLabelValues(metrics.Executing).Observe(float64(copyingDuration.Milliseconds()))
		// Record total latency (from task creation to completion)
		totalDuration := task.GetTR().ElapseSpan()
		metrics.CopySegmentTaskLatency.WithLabelValues(metrics.Done).Observe(float64(totalDuration.Milliseconds()))
		log.Info("copy segment task completed",
			WrapCopySegmentTaskLog(task,
				zap.Duration("taskTimeCost/copying", copyingDuration),
				zap.Duration("taskTimeCost/total", totalDuration))...)

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

	// Find partition ID from task's ID mappings
	var partitionID int64
	for _, mapping := range task.GetIdMappings() {
		if mapping.GetTargetSegmentId() == result.GetSegmentId() {
			partitionID = mapping.GetPartitionId()
			break
		}
	}

	// Sync each vector/scalar index
	for fieldID, indexInfo := range result.GetIndexInfos() {
		segIndex := &model.SegmentIndex{
			SegmentID:                 result.GetSegmentId(),
			CollectionID:              task.GetCollectionId(),
			PartitionID:               partitionID,
			IndexID:                   indexInfo.GetIndexId(),
			BuildID:                   indexInfo.GetBuildId(),
			IndexState:                commonpb.IndexState_Finished,
			IndexFileKeys:             indexInfo.GetIndexFilePaths(),
			IndexSerializedSize:       uint64(indexInfo.GetIndexSize()),
			IndexMemSize:              uint64(indexInfo.GetIndexSize()),
			IndexVersion:              indexInfo.GetVersion(),
			CurrentIndexVersion:       indexInfo.GetCurrentIndexVersion(),
			CurrentScalarIndexVersion: indexInfo.GetCurrentScalarIndexVersion(),
			CreatedUTCTime:            uint64(time.Now().Unix()),
			FinishedUTCTime:           uint64(time.Now().Unix()),
			NumRows:                   result.GetImportedRows(),
		}

		err := meta.indexMeta.AddSegmentIndex(ctx, segIndex)
		if err != nil {
			log.Warn("failed to add segment index",
				WrapCopySegmentTaskLog(task,
					zap.Int64("segmentID", result.GetSegmentId()),
					zap.Int64("fieldID", fieldID),
					zap.Int64("indexID", indexInfo.GetIndexId()),
					zap.Error(err))...)

			// Mark task and job as failed
			updateErr := copyMeta.UpdateTask(ctx, task.GetTaskId(),
				UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed),
				UpdateCopyTaskReason(err.Error()))
			if updateErr != nil {
				log.Warn("failed to update task state to Failed",
					zap.Int64("taskID", task.GetTaskId()), zap.Error(updateErr))
			}

			updateErr = copyMeta.UpdateJobStateAndReleaseRef(ctx, task.GetJobId(),
				UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
				UpdateCopyJobReason(err.Error()))
			if updateErr != nil {
				log.Warn("failed to update job state to Failed",
					zap.Int64("jobID", task.GetJobId()), zap.Error(updateErr))
			}
			return err
		}

		log.Info("synced vector/scalar index",
			WrapCopySegmentTaskLog(task,
				zap.Int64("segmentID", result.GetSegmentId()),
				zap.Int64("fieldID", fieldID),
				zap.Int64("indexID", indexInfo.GetIndexId()),
				zap.Int64("buildID", indexInfo.GetBuildId()))...)
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
		log.Warn("failed to update text index",
			WrapCopySegmentTaskLog(task,
				zap.Int64("segmentID", result.GetSegmentId()),
				zap.Error(err))...)

		// Mark task and job as failed
		updateErr := copyMeta.UpdateTask(ctx, task.GetTaskId(),
			UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed),
			UpdateCopyTaskReason(err.Error()))
		if updateErr != nil {
			log.Warn("failed to update task state to Failed",
				zap.Int64("taskID", task.GetTaskId()), zap.Error(updateErr))
		}

		updateErr = copyMeta.UpdateJobStateAndReleaseRef(ctx, task.GetJobId(),
			UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
			UpdateCopyJobReason(err.Error()))
		if updateErr != nil {
			log.Warn("failed to update job state to Failed",
				zap.Int64("jobID", task.GetJobId()), zap.Error(updateErr))
		}
		return err
	}

	log.Info("synced text indexes",
		WrapCopySegmentTaskLog(task,
			zap.Int64("segmentID", result.GetSegmentId()),
			zap.Int("count", len(result.GetTextIndexInfos())))...)
	return nil
}

// ===========================================================================================
// Index Synchronization: JSON Key Indexes
// ===========================================================================================

// syncJsonKeyIndexes synchronizes JSON key index metadata to segment.
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
func syncJsonKeyIndexes(ctx context.Context, result *datapb.CopySegmentResult,
	task CopySegmentTask, meta *meta, copyMeta CopySegmentMeta,
) error {
	if len(result.GetJsonKeyIndexInfos()) == 0 {
		return nil
	}

	err := meta.UpdateSegment(result.GetSegmentId(),
		SetJsonKeyIndexLogs(result.GetJsonKeyIndexInfos()))
	if err != nil {
		log.Warn("failed to update json key index",
			WrapCopySegmentTaskLog(task,
				zap.Int64("segmentID", result.GetSegmentId()),
				zap.Error(err))...)

		// Mark task and job as failed
		updateErr := copyMeta.UpdateTask(ctx, task.GetTaskId(),
			UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed),
			UpdateCopyTaskReason(err.Error()))
		if updateErr != nil {
			log.Warn("failed to update task state to Failed",
				zap.Int64("taskID", task.GetTaskId()), zap.Error(updateErr))
		}

		updateErr = copyMeta.UpdateJobStateAndReleaseRef(ctx, task.GetJobId(),
			UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
			UpdateCopyJobReason(err.Error()))
		if updateErr != nil {
			log.Warn("failed to update job state to Failed",
				zap.Int64("jobID", task.GetJobId()), zap.Error(updateErr))
		}
		return err
	}

	log.Info("synced json key indexes",
		WrapCopySegmentTaskLog(task,
			zap.Int64("segmentID", result.GetSegmentId()),
			zap.Int("count", len(result.GetJsonKeyIndexInfos())))...)
	return nil
}
