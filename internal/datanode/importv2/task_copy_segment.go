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
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// CopySegmentTask manages the copying of multiple segment pairs from source to target.
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

	sourceCM            storage.ChunkManager
	targetCM            storage.ChunkManager
	sourceStorageConfig *indexpb.StorageConfig
	copier              storage.CrossBucketCopier
	sourceBucket        string
	targetBucket        string

	// Target objects created by this task, managed through TaskManager updates.
	copiedFiles []string
}

func NewCopySegmentTask(
	parentCtx context.Context,
	req *datapb.CopySegmentRequest,
	manager TaskManager,
	sourceCM storage.ChunkManager,
	targetCM storage.ChunkManager,
	sourceStorageConfig *indexpb.StorageConfig,
	copier storage.CrossBucketCopier,
	sourceBucket string,
	targetBucket string,
) Task {
	ctx, cancel := context.WithCancel(parentCtx)

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

		sourceCM:            sourceCM,
		targetCM:            targetCM,
		sourceStorageConfig: sourceStorageConfig,
		copier:              copier,
		sourceBucket:        sourceBucket,
		targetBucket:        targetBucket,
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

// GetResourceRequirement prices a segment copy, which moves no segment bytes
// through this process: the object store does the copying.
func (t *CopySegmentTask) GetResourceRequirement() taskresource.Requirement {
	return taskresource.EstimateCopySegment(len(t.segmentResults))
}

func (t *CopySegmentTask) GetBufferSize() int64 {
	return 0 // Copy task doesn't use memory buffer (direct file copy)
}

// Cancel aborts the task execution by canceling the context.
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
	copiedFiles := append([]string(nil), t.copiedFiles...)

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

		sourceCM:            t.sourceCM,
		targetCM:            t.targetCM,
		sourceStorageConfig: t.sourceStorageConfig,
		copier:              t.copier,
		sourceBucket:        t.sourceBucket,
		targetBucket:        t.targetBucket,
		copiedFiles:         copiedFiles,
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
//  3. Submit each segment pair to the execution pool
//  4. Wait for every worker before publishing a terminal failure
//
// Parallel execution:
//   - Each source-target pair is processed independently
//   - Uses shared GetExecPool() for resource management
//   - The first failure cancels sibling workers while preserving its reason
//   - A task-level finalizer publishes failure after every worker has exited
//
// Parameters: None (uses task's internal request)
//
// Returns:
//   - []*conc.Future[any]: A task-level finalizer future (nil if validation fails)
func (t *CopySegmentTask) Execute() []*conc.Future[any] {
	mlog.Info(t.ctx, "start copy segment task", WrapLogFields(t)...)

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

	// Step 3: Submit all segment pairs to the execution pool. Workers publish
	// copied files before returning; only the finalizer publishes failure.
	workerFutures := make([]*conc.Future[any], 0, len(sources))
	var (
		firstErr     error
		firstErrOnce sync.Once
	)
	for i := range sources {
		source := sources[i]
		target := targets[i]
		future := GetExecPool().Submit(func() (any, error) {
			result, err := t.copySingleSegment(source, target)
			if err != nil {
				firstErrOnce.Do(func() {
					firstErr = err
					t.cancel()
				})
			}
			return result, err
		})
		workerFutures = append(workerFutures, future)
	}

	// Keep the waiter outside the bounded copy pool. Otherwise it could occupy a
	// slot needed by one of the workers it is waiting for.
	finalizer := conc.Go(func() (any, error) {
		_ = conc.BlockOnAll(workerFutures...)
		if firstErr == nil {
			return nil, nil
		}
		t.manager.Update(t.GetTaskID(),
			UpdateState(datapb.ImportTaskStateV2_Failed),
			UpdateReason(firstErr.Error()),
		)
		return nil, firstErr
	})

	return []*conc.Future[any]{finalizer}
}

// copySingleSegment copies all files for a single source-target segment pair.
//
// This is executed in parallel for each segment pair by the execution pool.
// It performs the actual file copy operation and tracks copied files for cleanup.
//
// Process flow:
//  1. Validate source has required binlogs (insert or delta)
//  2. Copy all segment files (binlogs + indexes) via CopySegmentAndIndexFiles
//  3. Publish copied files and the segment result through TaskManager
//
// File tracking:
//   - Always publish successfully copied files, including partial failures
//   - TaskManager serializes concurrent segment updates
//   - Clone preserves the files in each published task snapshot
//
// Error handling:
//   - Any copy failure is returned to the task-level finalizer
//   - Partial copy results are recorded for cleanup
//   - Failed is published only after all workers have exited
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
		mlog.Int64("sourceCollectionID", source.GetCollectionId()),
		mlog.Int64("sourcePartitionID", source.GetPartitionId()),
		mlog.Int64("sourceSegmentID", source.GetSegmentId()),
		mlog.Int64("targetCollectionID", target.GetCollectionId()),
		mlog.Int64("targetPartitionID", target.GetPartitionId()),
		mlog.Int64("targetSegmentID", target.GetSegmentId()),
		mlog.Int("insertBinlogFields", len(source.GetInsertBinlogs())),
		mlog.Int("statsBinlogFields", len(source.GetStatsBinlogs())),
		mlog.Int("deltaBinlogFields", len(source.GetDeltaBinlogs())),
		mlog.Int("bm25BinlogFields", len(source.GetBm25Binlogs())),
		mlog.Int("vectorScalarIndexInfoCount", len(source.GetIndexFiles())),
		mlog.Int("textIndexFieldCount", len(source.GetTextIndexFiles())),
		mlog.Int("jsonKeyIndexFieldCount", len(source.GetJsonKeyIndexFiles())),
	)

	mlog.Info(t.ctx, "start copying single segment", logFields...)

	// Step 1: Validate source has required binlogs or a StorageV3 manifest.
	hasManifestInsert := source.GetStorageVersion() >= storage.StorageV3 && source.GetManifestPath() != ""
	if len(source.GetInsertBinlogs()) == 0 && len(source.GetDeltaBinlogs()) == 0 && !hasManifestInsert {
		reason := "no insert/delete binlogs for segment"
		mlog.Error(t.ctx,
			reason, logFields...)
		return nil, merr.WrapErrParameterInvalidMsg(reason)
	}

	// Step 2: Copy all segment files (binlogs + indexes) together
	segmentResult, copiedFiles, err := CopySegmentAndIndexFiles(
		t.ctx,
		t.sourceCM,
		t.sourceStorageConfig,
		t.copier,
		t.sourceBucket,
		t.targetBucket,
		source,
		target,
		logFields,
	)
	if err != nil {
		copyErr := merr.Wrap(err, "failed to copy segment files")
		mlog.Error(t.ctx,
			copyErr.Error(), logFields...)
		t.manager.Update(t.GetTaskID(), UpdateCopiedFiles(copiedFiles))
		return nil, copyErr
	}

	// Step 3: Publish the copied files and complete segment metadata atomically.
	t.manager.Update(t.GetTaskID(),
		UpdateCopiedFiles(copiedFiles),
		UpdateSegmentResult(segmentResult),
	)

	mlog.Info(t.ctx, "successfully copied single segment",
		append(logFields, mlog.Int("copiedFileCount", len(copiedFiles)))...)
	return nil, nil
}

// ============================================================================
// Cleanup on Failure
// ============================================================================

// CleanupCopiedFiles removes all copied files for failed tasks.
//
// This is called by DropCopySegment RPC when DataCoord inspector detects a failed task.
// It removes all files that were successfully copied before the failure, preventing
// orphan data in storage that cannot be cleaned by garbage collection.
//
// Process flow:
//  1. Copy the immutable task snapshot's file list
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
	// Step 1: Copy the manager-owned task snapshot before performing I/O.
	files := append([]string(nil), t.copiedFiles...)

	// Step 2: Early return if no files to cleanup
	if len(files) == 0 {
		mlog.Info(t.ctx, "no files to cleanup", mlog.Int64("taskID", t.taskID))
		return
	}

	mlog.Info(t.ctx, "cleaning up copied files for failed task",
		mlog.Int64("taskID", t.taskID),
		mlog.Int64("jobID", t.jobID),
		mlog.Int("fileCount", len(files)))

	// Step 3: Delete all copied files with timeout
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.ctx), 30*time.Second)
	defer cancel()

	if err := t.targetCM.MultiRemove(ctx, files); err != nil {
		// Cleanup failure is logged but doesn't block task removal
		mlog.Error(t.ctx, "failed to cleanup copied files",
			mlog.Int64("taskID", t.taskID),
			mlog.Int64("jobID", t.jobID),
			mlog.Int("fileCount", len(files)),
			mlog.Err(err))
	} else {
		mlog.Info(t.ctx, "successfully cleaned up copied files",
			mlog.Int64("taskID", t.taskID),
			mlog.Int64("jobID", t.jobID),
			mlog.Int("fileCount", len(files)))
	}
}
