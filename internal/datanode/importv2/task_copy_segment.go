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

	"go.uber.org/atomic"

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

	// Target objects created by this task, managed through TaskManager updates.
	copiedFiles []string

	runtime *copySegmentRuntime // Shared by every TaskManager clone
}

// copySegmentRuntime contains execution ownership that must survive metadata
// clones. Abort uses the same object as all submitted copy closures, so it can
// prevent new work, wait for every accepted closure, and then clean the final
// complete set of outputs.
type copySegmentRuntime struct {
	mu      sync.Mutex
	wg      sync.WaitGroup
	aborted bool

	// dispatchVersion is the epoch of the newest DataCoord dispatch this
	// worker-side task serves. It lives on the runtime rather than the task so
	// that TaskManager clones — and a re-dispatch that TaskManager folds into the
	// existing entry — observe the same value.
	dispatchVersion atomic.Int64
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
	runtime := &copySegmentRuntime{}
	runtime.dispatchVersion.Store(req.GetTaskVersion())
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
		runtime:             runtime,
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

// AdoptDispatchVersion binds this worker-side task to a newer DataCoord
// dispatch.
//
// TaskManager keeps the first entry when the same taskID is created again, so a
// re-dispatch is served by the task already running here. Raising the epoch
// hands ownership to the newest dispatch, which is what makes a drop queued for
// an earlier one recognizably stale — without it, that drop would match and
// abort the copy the new dispatch is relying on. Never lowers the epoch, so
// out-of-order dispatch RPCs cannot resurrect an older owner.
func (t *CopySegmentTask) AdoptDispatchVersion(version int64) {
	for {
		current := t.runtime.dispatchVersion.Load()
		if version <= current {
			return
		}
		if t.runtime.dispatchVersion.CompareAndSwap(current, version) {
			return
		}
	}
}

// AcceptsDrop reports whether a drop issued for the given dispatch epoch still
// applies to this task.
//
// Target object keys are a deterministic transform of the source keys and carry
// no dispatch identity, so every dispatch of a task writes the same objects. A
// drop from a superseded dispatch must therefore be ignored: honoring it would
// delete the output the current dispatch produced, which may already be
// published and referenced by live segment metadata.
//
// A zero epoch on either side means the peer predates the fence (rolling
// upgrade) and the drop is honored, preserving the previous behavior.
func (t *CopySegmentTask) AcceptsDrop(version int64) bool {
	current := t.runtime.dispatchVersion.Load()
	return version == 0 || current == 0 || version == current
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
		runtime:             t.runtime,
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

	// Step 3: Claim the whole batch against a concurrent Abort, then release the
	// lock before submitting anything.
	//
	// The aborted check and the wg.Add must be atomic: once Abort observes
	// aborted == false it is guaranteed to see the full counter in wg.Wait(),
	// and once it sets aborted == true no further batch is accepted.
	//
	// The lock must NOT span the submit loop. GetExecPool().Submit blocks when
	// the pool has no idle worker, so holding the mutex across the loop would
	// stall every other holder — including a concurrent Abort — behind a pool
	// that only frees up once the accepted closures finish.
	t.runtime.mu.Lock()
	if t.runtime.aborted {
		t.runtime.mu.Unlock()
		return nil
	}
	t.runtime.wg.Add(len(sources))
	t.runtime.mu.Unlock()

	// Workers publish copied files before returning; only the finalizer
	// publishes failure.
	workerFutures := make([]*conc.Future[any], 0, len(sources))
	var (
		firstErr     error
		firstErrOnce sync.Once
	)
	for i := range sources {
		// Stop admitting work once Abort has fenced the task. Every source
		// counted into wg above must still be released, or Abort would wait on
		// copies that will never run — so hand back the whole remaining tail in
		// one atomic decrement.
		if t.runtimeAborted() {
			t.runtime.wg.Add(i - len(sources))
			break
		}

		source := sources[i]
		target := targets[i]
		// Each source owns a release latch so its wg slot is given back exactly
		// once, whether the closure runs to completion or the pool rejects it
		// outright (in which case the closure never runs and cannot release it).
		released := atomic.NewBool(false)
		release := func() {
			if released.CompareAndSwap(false, true) {
				t.runtime.wg.Done()
			}
		}
		future := GetExecPool().Submit(func() (any, error) {
			defer release()
			result, err := t.copySingleSegment(source, target)
			if err != nil {
				firstErrOnce.Do(func() {
					firstErr = err
					t.cancel()
				})
			}
			return result, err
		})
		if isPoolRejection(future) {
			release()
		}
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

// runtimeAborted reports whether Abort has fenced this task.
func (t *CopySegmentTask) runtimeAborted() bool {
	t.runtime.mu.Lock()
	defer t.runtime.mu.Unlock()
	return t.runtime.aborted
}

// isPoolRejection reports whether the pool declined the closure instead of
// scheduling it. A rejected submission completes its future immediately with an
// error without ever entering the closure, so the caller owns the cleanup that
// the closure would otherwise have performed. A closure that ran and failed also
// completes with an error, which is why the release latch — not this check — is
// what keeps the accounting exactly-once.
func isPoolRejection(future *conc.Future[any]) bool {
	select {
	case <-future.Inner():
		return future.Err() != nil
	default:
		return false
	}
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
//  1. Read the freshest published file list from TaskManager
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
//   - Abort uses cleanupCopiedFiles and propagates deletion failure, so the
//     task remains registered and a later Drop RPC can retry.
//   - This compatibility wrapper logs the error for older direct callers.
//
// Idempotency:
//   - Safe to call multiple times (operation is idempotent)
//   - Subsequent calls will attempt to delete same files again
func (t *CopySegmentTask) CleanupCopiedFiles() {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.ctx), 30*time.Second)
	defer cancel()
	if err := t.cleanupCopiedFiles(ctx); err != nil {
		mlog.Error(t.ctx, "failed to cleanup copied files", mlog.Int64("taskID", t.taskID), mlog.Err(err))
	}
}

// currentCopiedFiles returns the freshest published set of target objects.
//
// Workers publish their output through TaskManager, which applies every update
// to a fresh task snapshot. The receiver here is whichever snapshot the caller
// happened to hold — for Abort, the one taken before the in-flight closures were
// joined — so it can be missing files that were published in the meantime.
// Deleting only that stale subset would leave the rest orphaned forever.
func (t *CopySegmentTask) currentCopiedFiles() []string {
	if t.manager != nil {
		if latest, ok := t.manager.Get(t.taskID).(*CopySegmentTask); ok {
			return append([]string(nil), latest.copiedFiles...)
		}
	}
	return append([]string(nil), t.copiedFiles...)
}

func (t *CopySegmentTask) cleanupCopiedFiles(ctx context.Context) error {
	// Step 1: Read the freshest published snapshot of the copied files.
	files := t.currentCopiedFiles()

	// Step 2: Early return if no files to cleanup
	if len(files) == 0 {
		mlog.Info(t.ctx, "no files to cleanup", mlog.Int64("taskID", t.taskID))
		return nil
	}

	mlog.Info(t.ctx, "cleaning up copied files for failed task",
		mlog.Int64("taskID", t.taskID),
		mlog.Int64("jobID", t.jobID),
		mlog.Int("fileCount", len(files)))

	// Step 3: Delete all copied files.
	if err := t.targetCM.MultiRemove(ctx, files); err != nil {
		mlog.Error(t.ctx, "failed to cleanup copied files",
			mlog.Int64("taskID", t.taskID),
			mlog.Int64("jobID", t.jobID),
			mlog.Int("fileCount", len(files)),
			mlog.Err(err))
		return err
	} else {
		mlog.Info(t.ctx, "successfully cleaned up copied files",
			mlog.Int64("taskID", t.taskID),
			mlog.Int64("jobID", t.jobID),
			mlog.Int("fileCount", len(files)))
	}
	return nil
}

// Abort prevents submission of new copy operations, cancels in-flight I/O,
// joins every accepted copy closure, and only then removes all recorded output.
// The task must remain in TaskManager when this returns an error so a later RPC
// can retry cleanup with the same shared runtime state.
func (t *CopySegmentTask) Abort(ctx context.Context) error {
	t.runtime.mu.Lock()
	t.runtime.aborted = true
	t.cancel()
	t.runtime.mu.Unlock()

	// Joining every accepted closure before cleanup is the whole point of the
	// shared runtime: removing output while a copy is still writing would leave
	// the objects it writes afterwards behind forever. But the join must not
	// outlive the caller's deadline — Abort runs synchronously inside the
	// DropCopySegment RPC handler, and Execute's submit loop can be parked in the
	// shared exec pool behind *other* tasks' work, which cancel() cannot release.
	//
	// On timeout, report the failure without touching storage. The task stays
	// registered, so DataCoord's drop retry converges the cleanup later with the
	// same runtime state, rather than this handler deleting files out from under
	// copies that are still running.
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		t.runtime.wg.Wait()
	}()
	select {
	case <-joined:
	case <-ctx.Done():
		return merr.Wrap(ctx.Err(), "timed out joining in-flight copy segment closures before cleanup")
	}
	return t.cleanupCopiedFiles(ctx)
}
