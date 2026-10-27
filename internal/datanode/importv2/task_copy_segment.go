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
	"path"
	"strings"
	"sync"
	"time"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
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
	copyPermits         chan struct{}

	// Target objects created by this task, managed through TaskManager updates.
	copiedFiles []string
}

func NewCopySegmentTask(
	parentCtx context.Context,
	copyPermits chan struct{},
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
		copyPermits:         copyPermits,
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
		copyPermits:         t.copyPermits,
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

// Execute runs each segment's inventory, copies and metadata finalization in
// a bounded worker. The orchestrator stays outside the import pool:
// waiting inside that pool could occupy a slot needed by a preparation worker.
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

	finalizer := conc.Go(func() (any, error) {
		err := t.copySegments()
		if err != nil {
			t.cancel()
			// Every stage has drained its workers and recorded confirmed writes.
			t.manager.Update(t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason(err.Error()))
		}
		return nil, err
	})
	return []*conc.Future[any]{finalizer}
}

// runCopySegmentStage uses the existing bounded import pool for segment work.
// Cancellation stops queued work; active copies and native calls are joined
// before results or failure cleanup may be published.
func runCopySegmentStage(ctx context.Context, count int, work func(context.Context, int) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var firstErr error
	var once sync.Once
	futures := make([]*conc.Future[any], 0, count)
	for i := 0; i < count && ctx.Err() == nil; i++ {
		futures = append(futures, GetExecPool().Submit(func() (any, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			err := work(ctx, i)
			if err != nil {
				once.Do(func() { firstErr = err; cancel() })
			}
			return nil, err
		}))
	}
	waitErr := conc.BlockOnAll(futures...)
	if firstErr != nil {
		return firstErr
	}
	if waitErr != nil {
		return waitErr
	}
	return ctx.Err()
}

func (t *CopySegmentTask) copySegments() error {
	sources := t.req.GetSources()
	targets := append([]*datapb.CopySegmentTarget(nil), t.req.GetTargets()...)
	results := make([]*datapb.CopySegmentResult, len(sources))
	createdFiles := make([][]string, len(sources))
	// Each bounded worker owns one segment from inventory through publication.
	// File copies use a separate task-local budget, so even a single import
	// worker can make progress without recursively submitting to its own pool.
	// Shared LOBs may be copied again: sources are immutable, destinations are
	// job-owned, and provider copies are idempotent. Cleanup must exclude LOBs.
	err := runCopySegmentStage(t.ctx, len(sources), func(ctx context.Context, i int) error {
		source := sources[i]
		if len(source.GetInsertBinlogs()) == 0 && len(source.GetDeltaBinlogs()) == 0 &&
			(source.GetStorageVersion() < storage.StorageV3 || source.GetManifestPath() == "") {
			return merr.WrapErrParameterInvalidMsg("no insert/delete binlogs for segment")
		}
		if len(t.req.GetTargetIndexes()) > 0 {
			targets[i] = proto.Clone(targets[i]).(*datapb.CopySegmentTarget)
			targets[i].TargetIndexes = t.req.GetTargetIndexes()
		}
		plan, err := prepareSegmentCopy(ctx, t.sourceCM, t.sourceStorageConfig, source, targets[i])
		if err != nil {
			return err
		}
		items, err := segmentCopyFiles(plan)
		if err != nil {
			return err
		}
		copied, err := copySegmentFiles(ctx, t.copyPermits, items, t.copier, t.sourceBucket, t.targetBucket, WrapLogFields(t))
		createdFiles[i] = copied // Retain confirmed writes, including cancellation races.
		if err != nil {
			return err
		}
		result, created, err := completeSegmentCopy(ctx, t.req.GetStorageConfig(), source, targets[i], plan)
		createdFiles[i] = append(createdFiles[i], created...)
		if err == nil {
			results[i] = result
		}
		return err
	})
	// Publish once after draining: TaskManager clones the whole task, including
	// its file list. Updating per segment would repeatedly clone the whole plan's
	// cleanup list. Failed completion still publishes every confirmed new manifest.
	actions := make([]UpdateAction, 0, len(sources)+1)
	var created []string
	for i, result := range results {
		created = append(created, createdFiles[i]...)
		if result != nil {
			actions = append(actions, UpdateSegmentResult(result))
		}
	}
	actions = append(actions, UpdateCopiedFiles(created))
	t.manager.Update(t.GetTaskID(), actions...)
	return err
}

// ============================================================================
// Cleanup on Failure
// ============================================================================

// CleanupCopiedFiles removes task-owned copied files for failed tasks.
//
// This is called by DropCopySegment RPC when DataCoord inspector detects a failed task.
// Partition-level LOBs are shared across tasks after TEXT compaction. Deleting one
// here could break a completed task, even if this task originally created the LOB.
// Leave them to reference-aware LOB GC; disabling that GC can retain orphan LOBs.
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
	// Match the actual target partition namespace, not any "/lobs/" component
	// in a storage root or a segment-owned path. Preserve literal object keys.
	sharedLOBPrefixes := typeutil.NewSet[string]()
	for _, target := range t.req.GetTargets() {
		sharedLOBPrefixes.Insert(path.Join(strings.TrimSpace(target.GetTargetRootPath()),
			BinlogTypeInsert, fmt.Sprint(target.GetCollectionId()), fmt.Sprint(target.GetPartitionId()), "lobs") + "/")
	}
	files := lo.Filter(lo.Uniq(t.copiedFiles), func(file string, _ int) bool {
		index := strings.LastIndex(file, "/lobs/")
		return index < 0 || !sharedLOBPrefixes.Contain(file[:index+len("/lobs/")])
	})

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
