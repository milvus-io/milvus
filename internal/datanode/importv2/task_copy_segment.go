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

package importv2

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// Copy Segment Task for Snapshot Restore
//
// This file implements the task layer for high-performance segment copying during
// snapshot restore operations. It manages the execution of segment copy operations
// across multiple source-target segment pairs in parallel.
//
// TASK LIFECYCLE:
// 1. Create: NewCopySegmentTask initializes task with source/target segment pairs
// 2. Execute: Parallel copy of all segment pairs using worker pool
// 3. Monitor: Task manager tracks progress and handles failures
// 4. Complete: Report results to DataCoord with binlog/index metadata
// 5. Cleanup: On failure, remove all copied files to prevent orphans
//
// PARALLEL EXECUTION:
// - Each source-target segment pair is copied independently in parallel
// - Uses shared execution pool (GetExecPool) for resource management
// - Fail-fast behavior: first failure marks entire task as failed
// - All copied files are tracked for potential cleanup on failure
//
// FAILURE HANDLING:
// - Track all successfully copied files during execution
// - On task failure, DropCopySegment triggers CleanupCopiedFiles
// - Cleanup removes all copied files to prevent orphan data
// - Thread-safe file tracking using mutex
//
// INTEGRATION:
// - DataCoord creates tasks via CopySegment RPC
// - DataNode executes tasks and reports results
// - Inspector monitors task progress and triggers cleanup on failure

// CopySegmentTask manages the copying of multiple segment pairs from source to target.
//
// This task is created by DataNode when it receives a CopySegment RPC from DataCoord.
// It coordinates parallel copying of segment files (binlogs and indexes) for one or
// more source-target segment pairs.
//
// Key responsibilities:
//   - Parallel execution of segment copy operations
//   - Progress tracking and result reporting to DataCoord
//   - Cleanup of copied files on task failure
//   - Resource management via slot allocation
type CopySegmentTask struct {
	ctx            context.Context                     // Context for cancellation and timeout
	cancel         context.CancelFunc                  // Cancel function for aborting task execution
	jobID          int64                               // Parent job ID for tracking related tasks
	taskID         int64                               // Unique task ID assigned by DataCoord
	collectionID   int64                               // Target collection ID
	partitionIDs   []int64                             // Target partition IDs (deduplicated from targets)
	state          datapb.ImportTaskStateV2            // Current task state (Pending/InProgress/Completed/Failed)
	reason         string                              // Failure reason if state is Failed
	slots          int64                               // Resource slots allocated for this task
	segmentResults map[int64]*datapb.CopySegmentResult // Results for each target segment
	req            *datapb.CopySegmentRequest          // Original request with source/target pairs
	manager        TaskManager                         // Task manager for state updates and coordination
	cm             storage.ChunkManager                // ChunkManager for file copy operations

	// Cleanup tracking: records all successfully copied files for cleanup on failure
	copiedFilesMu sync.Mutex // Protects copiedFiles for concurrent segment copies
	copiedFiles   []string   // List of all successfully copied file paths
}

// NewCopySegmentTask creates a new copy segment task from a DataCoord request.
//
// This is called by DataNode when it receives a CopySegment RPC. The task is initialized
// in Pending state and will be executed by the task scheduler when resources are available.
//
// Process flow:
//  1. Create cancellable context for task execution control
//  2. Initialize empty result structures for each target segment
//  3. Extract collection and partition IDs from targets (deduplicate partitions)
//  4. Create task with all necessary components (manager, chunkManager)
//
// The task supports copying multiple source-target segment pairs in a single task,
// enabling efficient parallel execution and result batching.
//
// Parameters:
//   - req: CopySegmentRequest containing source/target segment pairs and metadata
//   - manager: TaskManager for state updates and progress tracking
//   - cm: ChunkManager for file copy operations (S3, MinIO, local storage)
//
// Returns:
//   - Task: Initialized CopySegmentTask in Pending state, ready for execution
func NewCopySegmentTask(
	req *datapb.CopySegmentRequest,
	manager TaskManager,
	cm storage.ChunkManager,
) Task {
	ctx, cancel := context.WithCancel(context.Background())

	// Step 1: Initialize empty result structures for each target segment
	// These will be populated during execution with binlog/index metadata
	segmentResults := make(map[int64]*datapb.CopySegmentResult)
	for _, target := range req.GetTargets() {
		segmentResults[target.GetSegmentId()] = &datapb.CopySegmentResult{
			SegmentId:         target.GetSegmentId(),
			ImportedRows:      0,
			Binlogs:           []*datapb.FieldBinlog{},
			Statslogs:         []*datapb.FieldBinlog{},
			Deltalogs:         []*datapb.FieldBinlog{},
			Bm25Logs:          []*datapb.FieldBinlog{},
			IndexInfos:        make(map[int64]*datapb.VectorScalarIndexInfo),
			TextIndexInfos:    make(map[int64]*datapb.TextIndexStats),
			JsonKeyIndexInfos: make(map[int64]*datapb.JsonKeyStats),
		}
	}

	// Step 2: Extract collection and partition IDs from targets
	// Note: All targets should have the same collection ID (enforced by DataCoord)
	// Partition IDs are deduplicated in case multiple segments belong to same partition
	var collectionID int64
	var partitionIDs []int64
	if len(req.GetTargets()) > 0 {
		collectionID = req.GetTargets()[0].GetCollectionId()
		partitionIDSet := make(map[int64]struct{})
		for _, target := range req.GetTargets() {
			partitionIDSet[target.GetPartitionId()] = struct{}{}
		}
		for pid := range partitionIDSet {
			partitionIDs = append(partitionIDs, pid)
		}
	}

	// Step 3: Create task with all components
	task := &CopySegmentTask{
		ctx:            ctx,
		cancel:         cancel,
		jobID:          req.GetJobID(),
		taskID:         req.GetTaskID(),
		collectionID:   collectionID,
		partitionIDs:   partitionIDs,
		state:          datapb.ImportTaskStateV2_Pending,
		reason:         "",
		slots:          req.GetTaskSlot(),
		segmentResults: segmentResults,
		req:            req,
		manager:        manager,
		cm:             cm,
	}
	return task
}

// ============================================================================
// Task Interface Implementation
// ============================================================================
// The following methods implement the Task interface required by TaskManager.
// These provide metadata and control operations for task scheduling and monitoring.

func (t *CopySegmentTask) GetType() TaskType {
	return CopySegmentTaskType
}

func (t *CopySegmentTask) GetPartitionIDs() []int64 {
	return t.partitionIDs
}

func (t *CopySegmentTask) GetVchannels() []string {
	return nil // CopySegmentTask doesn't need vchannels (no streaming data)
}

func (t *CopySegmentTask) GetJobID() int64 {
	return t.jobID
}

func (t *CopySegmentTask) GetTaskID() int64 {
	return t.taskID
}

func (t *CopySegmentTask) GetCollectionID() int64 {
	return t.collectionID
}

func (t *CopySegmentTask) GetState() datapb.ImportTaskStateV2 {
	return t.state
}

func (t *CopySegmentTask) GetReason() string {
	return t.reason
}

func (t *CopySegmentTask) GetSchema() *schemapb.CollectionSchema {
	return nil // CopySegmentTask doesn't need schema (copies files directly)
}

func (t *CopySegmentTask) GetSlots() int64 {
	return t.slots
}

func (t *CopySegmentTask) GetBufferSize() int64 {
	return 0 // Copy task doesn't use memory buffer (direct file copy)
}

// Cancel aborts the task execution by cancelling the context.
// This will interrupt any ongoing file copy operations.
func (t *CopySegmentTask) Cancel() {
	t.cancel()
}

// Clone creates a copy of the task with deep-copied segmentResults.
// Note: This shares references to manager, cm, and other components.
// The segmentResults map is deep-copied to avoid concurrent map access.
func (t *CopySegmentTask) Clone() Task {
	// Deep copy segmentResults to avoid concurrent map access
	results := make(map[int64]*datapb.CopySegmentResult)
	for id, result := range t.segmentResults {
		results[id] = typeutil.Clone(result)
	}

	return &CopySegmentTask{
		ctx:            t.ctx,
		cancel:         t.cancel,
		jobID:          t.jobID,
		taskID:         t.taskID,
		collectionID:   t.collectionID,
		partitionIDs:   t.partitionIDs,
		state:          t.state,
		reason:         t.reason,
		slots:          t.slots,
		segmentResults: results,
		req:            t.req,
		manager:        t.manager,
		cm:             t.cm,
	}
}

// GetSegmentResults returns the copy results for all target segments.
// This is called by DataCoord to retrieve binlog/index metadata after task completion.
func (t *CopySegmentTask) GetSegmentResults() map[int64]*datapb.CopySegmentResult {
	// Return a copy to avoid concurrent map access during iteration
	results := make(map[int64]*datapb.CopySegmentResult)
	for id, result := range t.segmentResults {
		results[id] = result
	}
	return results
}

// ============================================================================
// Task Execution
// ============================================================================

// Execute starts parallel execution of all segment copy operations.
//
// This is the main entry point called by TaskManager when the task is scheduled.
// It validates the request, then submits all source-target segment pairs to the
// execution pool for parallel processing.
//
// Process flow:
//  1. Update task state to InProgress
//  2. Validate request (sources exist, counts match)
//  3. Submit each segment pair to execution pool as independent future
//  4. Return futures for TaskManager to monitor
//
// Parallel execution:
//   - Each source-target pair is processed independently
//   - Uses shared GetExecPool() for resource management
//   - Futures are monitored by TaskManager for completion/failure
//   - First failure marks entire task as failed (fail-fast)
//
// Parameters: None (uses task's internal request)
//
// Returns:
//   - []*conc.Future[any]: Futures for all segment copy operations (nil if validation fails)
func (t *CopySegmentTask) Execute() []*conc.Future[any] {
	log.Info("start copy segment task", WrapLogFields(t)...)

	// Step 1: Update task state to InProgress
	t.manager.Update(t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_InProgress))

	sources := t.req.GetSources()
	targets := t.req.GetTargets()

	// Step 2: Validate input
	if len(sources) == 0 {
		reason := "no source segments to copy"
		t.manager.Update(t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason(reason))
		return nil
	}
	if len(sources) != len(targets) {
		reason := fmt.Sprintf("source segments count (%d) does not match target segments count (%d)",
			len(sources), len(targets))
		t.manager.Update(t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason(reason))
		return nil
	}

	// Step 3: Submit all segment pairs to execution pool for parallel processing
	futures := make([]*conc.Future[any], 0, len(sources))
	for i := range sources {
		source := sources[i]
		target := targets[i]
		future := GetExecPool().Submit(func() (any, error) {
			return t.copySingleSegment(source, target)
		})
		futures = append(futures, future)
	}

	return futures
}

// copySingleSegment copies all files for a single source-target segment pair.
//
// This is executed in parallel for each segment pair by the execution pool.
// It performs the actual file copy operation and tracks copied files for cleanup.
//
// Process flow:
//  1. Validate source has required binlogs (insert or delta)
//  2. Copy all segment files (binlogs + indexes) via CopySegmentAndIndexFiles
//  3. Record copied files for potential cleanup on failure
//  4. Update task with segment result (binlog/index metadata)
//
// File tracking:
//   - Always record successfully copied files (even on partial failure)
//   - Enables cleanup if task fails later or during execution
//   - Thread-safe recording via recordCopiedFiles
//
// Error handling:
//   - Any copy failure marks entire task as Failed
//   - Partial copy results are recorded for cleanup
//   - Task manager is notified immediately on failure
//
// Parameters:
//   - source: Source segment metadata with binlog/index file paths
//   - target: Target segment IDs for path transformation
//
// Returns:
//   - any: Always nil (future compatibility)
//   - error: Error if validation fails or copy operation fails
func (t *CopySegmentTask) copySingleSegment(source *datapb.CopySegmentSource, target *datapb.CopySegmentTarget) (any, error) {
	logFields := WrapLogFields(t,
		zap.Int64("sourceCollectionID", source.GetCollectionId()),
		zap.Int64("sourcePartitionID", source.GetPartitionId()),
		zap.Int64("sourceSegmentID", source.GetSegmentId()),
		zap.Int64("targetCollectionID", target.GetCollectionId()),
		zap.Int64("targetPartitionID", target.GetPartitionId()),
		zap.Int64("targetSegmentID", target.GetSegmentId()),
		zap.Int("insertBinlogFields", len(source.GetInsertBinlogs())),
		zap.Int("statsBinlogFields", len(source.GetStatsBinlogs())),
		zap.Int("deltaBinlogFields", len(source.GetDeltaBinlogs())),
		zap.Int("bm25BinlogFields", len(source.GetBm25Binlogs())),
		zap.Int("vectorScalarIndexInfoCount", len(source.GetIndexFiles())),
		zap.Int("textIndexFieldCount", len(source.GetTextIndexFiles())),
		zap.Int("jsonKeyIndexFieldCount", len(source.GetJsonKeyIndexFiles())),
	)

	log.Info("start copying single segment", logFields...)

	// Step 1: Validate source has required binlogs
	if len(source.GetInsertBinlogs()) == 0 && len(source.GetDeltaBinlogs()) == 0 {
		reason := "no insert/delete binlogs for segment"
		log.Error(reason, logFields...)
		t.manager.Update(t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason(reason))
		return nil, errors.New(reason)
	}

	// Step 2: Copy all segment files (binlogs + indexes) together
	segmentResult, copiedFiles, err := CopySegmentAndIndexFiles(
		t.ctx,
		t.cm,
		source,
		target,
		logFields,
	)

	// Step 3: Always record copied files (even on failure, for potential cleanup)
	if len(copiedFiles) > 0 {
		t.recordCopiedFiles(copiedFiles)
	}

	if err != nil {
		reason := fmt.Sprintf("failed to copy segment files: %v", err)
		log.Error(reason, logFields...)
		t.manager.Update(t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason(reason))
		return nil, err
	}

	// Step 4: Update segment result in task with complete metadata (binlogs + indexes)
	t.manager.Update(t.GetTaskID(), UpdateSegmentResult(segmentResult))

	log.Info("successfully copied single segment",
		append(logFields, zap.Int("copiedFileCount", len(copiedFiles)))...)
	return nil, nil
}

// ============================================================================
// Cleanup on Failure
// ============================================================================

// recordCopiedFiles records the list of successfully copied files for cleanup on failure.
//
// This is called during segment copy execution to track all files that have been
// successfully copied. The tracking is thread-safe to support parallel segment copies.
//
// Why track copied files:
//   - On task failure, we need to cleanup all copied files to prevent orphan data
//   - Without cleanup, failed tasks leave garbage files in storage
//   - Tracking enables complete rollback of partial copy operations
//
// Thread safety:
//   - Uses mutex to protect copiedFiles list
//   - Safe for concurrent calls from parallel segment copy operations
//
// Parameters:
//   - files: List of successfully copied file paths to record
func (t *CopySegmentTask) recordCopiedFiles(files []string) {
	t.copiedFilesMu.Lock()
	defer t.copiedFilesMu.Unlock()
	t.copiedFiles = append(t.copiedFiles, files...)
}

// CleanupCopiedFiles removes all copied files for failed tasks.
//
// This is called by DropCopySegment RPC when DataCoord inspector detects a failed task.
// It removes all files that were successfully copied before the failure, preventing
// orphan data in storage that cannot be cleaned by garbage collection.
//
// Process flow:
//  1. Lock and copy the file list (to avoid holding lock during I/O)
//  2. Early return if no files to cleanup
//  3. Use ChunkManager.MultiRemove for batch deletion with timeout
//  4. Log success/failure (failure is logged but doesn't block task removal)
//
// Why cleanup is necessary:
//   - Failed copy tasks leave files in storage with no metadata references
//   - Regular GC cannot clean these orphan files (not in any segment metadata)
//   - Without cleanup, storage leaks accumulate over time
//
// Error handling:
//   - Cleanup failure is logged but doesn't prevent task removal
//   - Best-effort cleanup: some files may remain if deletion fails
//   - 30-second timeout prevents cleanup from blocking indefinitely
//
// Idempotency:
//   - Safe to call multiple times (operation is idempotent)
//   - Subsequent calls will attempt to delete same files again
func (t *CopySegmentTask) CleanupCopiedFiles() {
	// Step 1: Copy file list under lock (avoid holding lock during I/O)
	t.copiedFilesMu.Lock()
	files := make([]string, len(t.copiedFiles))
	copy(files, t.copiedFiles)
	t.copiedFilesMu.Unlock()

	// Step 2: Early return if no files to cleanup
	if len(files) == 0 {
		log.Info("no files to cleanup", zap.Int64("taskID", t.taskID))
		return
	}

	log.Info("cleaning up copied files for failed task",
		zap.Int64("taskID", t.taskID),
		zap.Int64("jobID", t.jobID),
		zap.Int("fileCount", len(files)))

	// Step 3: Delete all copied files with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := t.cm.MultiRemove(ctx, files); err != nil {
		// Cleanup failure is logged but doesn't block task removal
		log.Error("failed to cleanup copied files",
			zap.Int64("taskID", t.taskID),
			zap.Int64("jobID", t.jobID),
			zap.Int("fileCount", len(files)),
			zap.Error(err))
	} else {
		log.Info("successfully cleaned up copied files",
			zap.Int64("taskID", t.taskID),
			zap.Int64("jobID", t.jobID),
			zap.Int("fileCount", len(files)))
	}
}
