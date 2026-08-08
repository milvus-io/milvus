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

package writebuffer

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The write buffer's growing-segment flush path: source selection, per-segment
// progress, handoff to the write-buffer path, and retry state. Counterpart of
// write_buffer_sync_coordinator.go, which owns the path that flushes payload
// yielded out of the write buffer.
//
// The two differ in one way that explains most of the code below: a
// growing-source flush does NOT take ownership of the rows. They stay pinned in
// the segcore growing segment until CommitGrowingFlush, so a failed attempt
// costs a round trip and nothing else. The write-buffer path yields its payload and
// must therefore hold on to a failed task until it succeeds.
//
// State lives in writeBufferBase.growingSourceProgress (plus growingSettled) and
// is guarded by writeBufferBase.mut.
// It deliberately does not take a lock of its own: every decision here reads
// buffer state (buffers, metaCache, checkpoint) under that same lock, and a
// second lock would only add an ordering problem.

func (wb *writeBufferBase) AllowGrowingSourceFlush() bool {
	return wb.allowGrowingSourceFlush
}

// finishGrowingSourceSync owns the growing-source side of task completion, the
// counterpart of finishWriteBufferSync.
//
// Both ways a growing-source task can end — the sync manager's completion
// callback, and SyncData refusing the submission outright — go through here, so
// the failure bookkeeping (pendingCommitted capture, error classification,
// retry-vs-escalate) is stated exactly once.
//
// Returns the error the caller should propagate.
func (wb *writeBufferBase) finishGrowingSourceSync(ctx context.Context, task *syncmgr.GrowingSourceSyncTask, taskErr error) error {
	segmentID := task.SegmentID()
	var resyncSegmentID int64
	var fatalErr error

	if taskErr != nil {
		// Commit releases the source inline on its success paths. A task that
		// fails in Prepare, is rejected by the dispatcher, or is canceled while
		// queued never reaches it — release idempotently here for those.
		task.ReleaseSource()
	}

	wb.mut.Lock()
	// Task-derived settlement runs FIRST and unconditionally. Everything below it
	// is progress bookkeeping, which is legitimately skipped when the progress is
	// gone; giving back what this task reserved is not.
	if taskErr != nil {
		wb.settleFailedGrowingTaskLocked(task)
	}
	if progress, exists := wb.growingSourceProgress[segmentID]; exists {
		if taskErr != nil {
			if task.HasCommittedFlush() && task.CommittedManifestPath() != "" {
				progress.pendingCommitted = &growingSourcePendingCommittedFlush{
					checkpoint:       task.Checkpoint(),
					batchRows:        task.BatchRows(),
					flushedThroughTs: task.FlushThroughTs(),
					isFlush:          task.IsFlush(),
					isDrop:           task.IsDrop(),
					manifestPath:     task.CommittedManifestPath(),
					bm25Stats:        cloneBM25StatsMap(task.CommittedBM25Stats()),
					insertBinlogs:    task.CommittedInsertBinlogs(),
					pkStats:          task.CommittedPKStats(),
				}
			}
			wb.failGrowingSyncLocked(progress, taskErr)
			wb.observeGrowingSourceSyncFailureLocked(segmentID, progress)
			if syncmgr.ClassifySyncError(ctx, taskErr) == syncmgr.SyncTerminal {
				// markNonRetryableFailure permanently parks this segment:
				// growingSourceProgressSyncable refuses it forever, so its batches
				// are never trimmed and the channel checkpoint stays pinned at
				// firstUncommittedPosition. Left silent that is an unbounded,
				// alert-less stall — strictly worse than a crash, because nothing
				// ever reports it. Fail loudly instead: the rows are still
				// recoverable from the WAL, and a human has to look at this.
				progress.markNonRetryableFailure()
				mlog.Error(ctx, "growing-source sync hit a terminal failure, escalating",
					mlog.Int64("segmentID", segmentID),
					mlog.Uint64("lastFlushedTs", progress.lastFlushedTs),
					mlog.String("lastFailure", progress.lastFailure))
				fatalErr = errors.Wrapf(taskErr, "growing-source sync unrecoverable, segmentID=%d lastFlushedTs=%d",
					segmentID, progress.lastFlushedTs)
			} else {
				wb.scheduleGrowingSourceRetryLocked(segmentID)
			}
		} else {
			if task.IsFlush() {
				progress.owesFlush = false
			}
			wb.ackGrowingSyncLocked(progress, task.FlushThroughTs())
			wb.resetGrowingSourceSyncFailureMetric(segmentID)
			if progress.owesFlush && len(progress.batches) == 0 {
				if _, ok := wb.metaCache.GetSegmentByID(segmentID); !ok {
					delete(wb.growingSourceProgress, segmentID)
				} else {
					// No claim here: the resync below goes through getSyncTask,
					// which claims Sealed itself.
					resyncSegmentID = segmentID
				}
			} else if len(progress.batches) == 0 {
				segment, ok := wb.metaCache.GetSegmentByID(segmentID)
				if task.IsFlush() || task.IsDrop() || !ok ||
					segment.State() == commonpb.SegmentState_Flushed ||
					segment.State() == commonpb.SegmentState_Dropped {
					delete(wb.growingSourceProgress, segmentID)
				}
			}
		}
	}
	wb.mut.Unlock()

	// Deferred, not called inline: the fatal handler panics by default, and the
	// observer callback below must still run for this task first.
	if fatalErr != nil {
		defer wb.errHandler(fatalErr)
	}
	if resyncSegmentID != 0 {
		wb.syncSegments(wb.syncCtx, []int64{resyncSegmentID})
	}

	if taskErr != nil {
		if wb.taskObserverCallback != nil {
			wb.taskObserverCallback(task, taskErr)
		}
		return taskErr
	}

	if task.StartPosition() != nil {
		wb.syncCheckpoint.Remove(segmentID, task.StartPosition().GetTimestamp())
	}
	if task.IsFlush() {
		wb.metaCache.RemoveSegments(metacache.WithSegmentIDs(segmentID))
		mlog.Info(ctx, "flushed segment removed", mlog.FieldSegmentID(segmentID), mlog.String("channel", task.ChannelName()))
	}
	if wb.taskObserverCallback != nil {
		wb.taskObserverCallback(task, nil)
	}
	return nil
}

func (wb *writeBufferBase) GetGrowingFlushProgress(ctx context.Context, segmentIDs []int64, fenceTs uint64) ([]GrowingFlushSegmentProgress, error) {
	if err := wb.waitProcessed(ctx, fenceTs); err != nil {
		return nil, err
	}

	wb.mut.RLock()
	if len(segmentIDs) == 0 {
		segmentIDs = lo.Keys(wb.growingSourceProgress)
	} else {
		segmentIDs = lo.Uniq(append(segmentIDs, lo.Keys(wb.growingSourceProgress)...))
	}

	progresses := make([]GrowingFlushSegmentProgress, 0, len(segmentIDs))
	releaseSegments := make([]syncmgr.GrowingSourceReleaseHandoffSegment, 0, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		progress := GrowingFlushSegmentProgress{
			SegmentID:  segmentID,
			SourceMode: metacache.FlushSourceUnknown,
		}
		if segment, ok := wb.metaCache.GetSegmentByID(segmentID); ok {
			progress.SourceMode = segment.FlushSourceMode()
		}
		if growingProgress, ok := wb.growingSourceProgress[segmentID]; ok {
			progress.FlushThroughTs = growingProgress.handoffFenceTs()
			progress.NeedReleaseHandoff = wb.growingProgressRequiresHandoff(segmentID, growingProgress)
			progress.SourceMode = metacache.FlushSourceGrowing
		}
		if progress.NeedReleaseHandoff {
			releaseSegments = append(releaseSegments, syncmgr.GrowingSourceReleaseHandoffSegment{
				SegmentID:      segmentID,
				FlushThroughTs: progress.FlushThroughTs,
			})
		}
		progresses = append(progresses, progress)
	}
	wb.mut.RUnlock()

	if len(releaseSegments) > 0 {
		if err := syncmgr.DefaultGrowingSourceRegistry().PrepareGrowingSourceReleaseHandoff(ctx, wb.channelName, fenceTs, releaseSegments); err != nil {
			return nil, err
		}
	}
	return progresses, nil
}

func (wb *writeBufferBase) growingProgressRequiresHandoff(segmentID int64, progress *growingSourceProgress) bool {
	if progress == nil {
		return false
	}
	if len(progress.batches) > 0 {
		return true
	}
	segment, ok := wb.metaCache.GetSegmentByID(segmentID)
	if !ok {
		return false
	}
	return segment.FlushSourceMode() == metacache.FlushSourceGrowing &&
		segment.State() != commonpb.SegmentState_Flushed
}

func (wb *writeBufferBase) hasGrowingSourceProgress(segmentID int64) bool {
	_, ok := wb.growingSourceProgress[segmentID]
	return ok
}

func (wb *writeBufferBase) decideGrowingFlushSource(segmentID int64, endPos *msgpb.MsgPosition) metacache.FlushSourceMode {
	// 1. Honor the sticky decision recorded in metacache. Once the first
	//    insert for a segment commits a source choice, every subsequent call
	//    must return the same kind so that progress / payload tracking stays
	//    consistent for the segment's lifetime.
	if seg, ok := wb.metaCache.GetSegmentByID(segmentID); ok {
		if seg.GetStorageVersion() != storage.StorageV3 {
			return metacache.FlushSourceWriteBuffer
		}
		switch seg.FlushSourceMode() {
		case metacache.FlushSourceGrowing:
			return metacache.FlushSourceGrowing
		case metacache.FlushSourceWriteBuffer:
			return metacache.FlushSourceWriteBuffer
		}
	}

	// 2. Fallback for the brief window where in-memory bookkeeping has been
	//    populated but the metacache sticky bit hasn't been set yet (e.g. on
	//    re-entry after a partial state).
	if wb.hasGrowingSourceProgress(segmentID) {
		return metacache.FlushSourceGrowing
	}

	if wb.hasWriteBufferInsertPayload(segmentID) {
		return metacache.FlushSourceWriteBuffer
	}

	if state := wb.getGrowingSourceState(segmentID, endPos); state == syncmgr.GrowingSourceUsable || state == syncmgr.GrowingSourcePending {
		return metacache.FlushSourceGrowing
	}
	wb.warnGrowingSourceFallback(segmentID, endPos)
	return metacache.FlushSourceWriteBuffer
}

func (wb *writeBufferBase) getGrowingSource(segmentID int64, endPos *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
	if wb.growingSourceResolver == nil {
		return nil, syncmgr.GrowingSourceUnavailable
	}
	return wb.growingSourceResolver(segmentID, endPos)
}

func (wb *writeBufferBase) getGrowingSourceState(segmentID int64, endPos *msgpb.MsgPosition) syncmgr.GrowingSourceState {
	source, state := wb.getGrowingSource(segmentID, endPos)
	if source != nil {
		source.Release()
	}
	return state
}

func (wb *writeBufferBase) warnGrowingSourceFallback(segmentID int64, endPos *msgpb.MsgPosition) {
	if !wb.allowGrowingSourceFlush {
		return
	}
	wb.growingSourceRatedLogger.RatedWarn(context.TODO(), rate.Limit(1), "growing-source source is unavailable, fallback to WriteBuffer",
		mlog.Int64("segmentID", segmentID),
		mlog.Any("endPosition", endPos),
	)
}

// growingSourceProgressSyncable reports whether this progress can produce a task
// now, and whether a retry should be scheduled.
//
// Its only write is recording owesFlush for a segment that got sealed while a
// sync was in flight. It does NOT touch metacache segment state: claiming the
// flush belongs to getSyncTask, where the content is fixed, and a claim made
// here would be made before the answer is known.
func (wb *writeBufferBase) growingSourceProgressSyncable(segmentID int64, progress *growingSourceProgress) (bool, bool) {
	if progress.nonRetryableFailure {
		return false, false
	}
	if progress.syncing {
		if segment, ok := wb.metaCache.GetSegmentByID(segmentID); ok &&
			(segment.State() == commonpb.SegmentState_Sealed || segment.State() == commonpb.SegmentState_Flushing) {
			progress.owesFlush = true
		}
		return false, false
	}
	if progress.pendingCommitted != nil {
		return true, false
	}
	if len(progress.batches) == 0 && !progress.owesFlush {
		return false, false
	}
	if len(progress.batches) == 0 {
		segment, ok := wb.metaCache.GetSegmentByID(segmentID)
		if !ok || (segment.State() != commonpb.SegmentState_Sealed && segment.State() != commonpb.SegmentState_Flushing) {
			return false, false
		}
	}
	checkpoint := wb.checkpoint
	if len(progress.batches) > 0 {
		checkpoint = progress.batches[len(progress.batches)-1].endPosition
	}
	if checkpoint == nil {
		return false, false
	}
	state := wb.getGrowingSourceState(segmentID, checkpoint)
	if state == syncmgr.GrowingSourceUsable {
		return true, false
	}

	// No rollback: nothing was claimed. A segment already in Flushing stays
	// there, and GetSealedSegmentsPolicy selects Flushing too, so it is picked
	// up again once the source catches up.
	return false, true
}

// scheduleGrowingSourceRetryLocked arms one segment's clock. There is no timer:
// driveGrowingSourceRetries picks it up on the next timetick, the same signal
// the write-buffer queue rides.
func (wb *writeBufferBase) scheduleGrowingSourceRetryLocked(segmentID int64) {
	if wb.closed || wb.dropping || wb.flushRetryInterval < 0 {
		return
	}
	if progress, ok := wb.growingSourceProgress[segmentID]; ok {
		progress.intent.backoff(time.Now())
	}
}

// driveGrowingSourceRetries re-submits growing-source flushes that asked for
// another round, no more often than the configured interval.
func (wb *writeBufferBase) driveGrowingSourceRetries(ctx context.Context, now time.Time, interval time.Duration) {
	wb.mut.Lock()
	if wb.closed || wb.dropping || wb.checkpoint == nil || len(wb.growingSourceProgress) == 0 {
		wb.mut.Unlock()
		return
	}
	segmentIDs, stillOwed := wb.getGrowingSourceSegmentsToRetry(now, interval)
	for _, segmentID := range segmentIDs {
		wb.growingSourceProgress[segmentID].intent.clear()
	}
	for _, segmentID := range stillOwed {
		// attempted, not backoff: the probe just happened. backoff refuses to
		// move `since` while the debt is already outstanding, so it would leave
		// the FIRST failure's timestamp in place and make due() true on every
		// timetick from then on.
		wb.growingSourceProgress[segmentID].intent.attempted(now)
	}
	var syncTasks []syncmgr.Task
	if len(segmentIDs) > 0 {
		wb.logger.Info(ctx, "retry growing-source sync", mlog.Int64s("segmentIDs", segmentIDs))
		syncTasks = wb.getSyncTasksLocked(wb.syncCtx, segmentIDs)
	}
	wb.mut.Unlock()

	if len(syncTasks) > 0 {
		wb.submitSyncTasks(wb.syncCtx, syncTasks)
	}
}

// getGrowingSourceSegmentsToRetry returns the segments whose clock is due AND
// which can produce a task now, plus the ones that must stay armed for a later
// round. Per segment, like the write-buffer queues: one segment's failure no
// longer holds up another's retry.
func (wb *writeBufferBase) getGrowingSourceSegmentsToRetry(now time.Time, interval time.Duration) (due []int64, stillOwed []int64) {
	for segmentID, progress := range wb.growingSourceProgress {
		if !progress.intent.due(now, interval) {
			continue
		}
		syncable, retry := wb.growingSourceProgressSyncable(segmentID, progress)
		if syncable {
			due = append(due, segmentID)
		} else if retry {
			stillOwed = append(stillOwed, segmentID)
		}
	}
	return due, stillOwed
}

func (wb *writeBufferBase) recordGrowingSourceProgress(inData *InsertData, startPos, endPos *msgpb.MsgPosition, schemaVersion int32) error {
	err := wb.CreateNewGrowingSegment(CreateGrowingSegmentInfo{
		PartitionID:   inData.partitionID,
		SegmentID:     inData.segmentID,
		StartPos:      startPos,
		SchemaVersion: schemaVersion,
	})
	if err != nil {
		return err
	}
	segment, ok := wb.metaCache.GetSegmentByID(inData.segmentID)
	if !ok {
		return merr.WrapErrSegmentNotFound(inData.segmentID)
	}
	if segment.GetStorageVersion() != storage.StorageV3 {
		return merr.WrapErrServiceInternalMsg("growing-source flush requires StorageV3 segment, segmentID=%d storageVersion=%d",
			inData.segmentID, segment.GetStorageVersion())
	}
	progress, ok := wb.growingSourceProgress[inData.segmentID]
	if !ok {
		progress = &growingSourceProgress{
			segmentID: inData.segmentID,
			// Where this segment was last flushed to. On a fresh segment it is
			// zero; on one recovered mid-flush it comes from the position the
			// last successful flush persisted.
			lastFlushedTs: segment.LastFlushPosition().GetTimestamp(),
		}
		wb.growingSourceProgress[inData.segmentID] = progress
	}
	progress.batches = append(progress.batches, growingSourceProgressBatch{
		startPosition: startPos,
		endPosition:   endPos,
		rowNum:        inData.rowNum,
	})
	// SetFlushSourceMode is sticky: only the first call commits the choice,
	// so we can include it unconditionally here without overriding a prior
	// FlushSourceWriteBuffer decision.
	wb.metaCache.UpdateSegments(metacache.MergeSegmentAction(
		metacache.SetStartPositionIfNil(startPos),
		metacache.SetFlushSourceMode(metacache.FlushSourceGrowing),
		wb.updateGrowingSourceBufferedRows(progress),
	), metacache.WithSegmentIDs(inData.segmentID))
	wb.notifyFlushSourceMode(inData.segmentID)
	return nil
}

func (wb *writeBufferBase) updateGrowingSourceBufferedRows(progress *growingSourceProgress) metacache.SegmentAction {
	// pendingRows already excludes everything a flush has acknowledged, so
	// FlushedRows does not enter this — one less place where a row count
	// from this side has to line up with the growing segment's own. Both
	// terms belong to progress and move under wb.mut, so the difference
	// cannot go negative.
	return metacache.UpdateBufferedRows(progress.pendingRows() - progress.claimedRows)
}

func (wb *writeBufferBase) growingSourceProgressSelectedByPolicy(ts typeutil.Timestamp, segmentID int64, progress *growingSourceProgress) bool {
	if progress == nil {
		return false
	}
	if progress.nonRetryableFailure {
		return false
	}
	if progress.owesFlush {
		return true
	}
	segment, ok := wb.metaCache.GetSegmentByID(segmentID)
	if ok {
		switch segment.State() {
		case commonpb.SegmentState_Sealed, commonpb.SegmentState_Flushing, commonpb.SegmentState_Dropped:
			return true
		}
		if wb.growingSourceProgressFull(segment, progress) {
			return true
		}
	}
	startPos := progress.firstUncommittedPosition()
	if startPos == nil {
		return false
	}
	staleDuration := paramtable.Get().DataNodeCfg.SyncPeriod.GetAsDuration(time.Second)
	current := tsoutil.PhysicalTime(ts)
	start := tsoutil.PhysicalTime(startPos.GetTimestamp())
	return current.Sub(start) > staleDuration
}

func (wb *writeBufferBase) growingSourceProgressFull(segment *metacache.SegmentInfo, progress *growingSourceProgress) bool {
	if segment == nil || progress == nil {
		return false
	}
	rows := progress.pendingRows() - progress.claimedRows
	if rows <= 0 {
		return false
	}
	if wb.estSizePerRecord <= 0 {
		return false
	}
	thresholdRows := int64(wb.getEstBatchSize())
	if thresholdRows <= 0 {
		return true
	}
	return rows >= thresholdRows
}

// noteGrowingSourceCandidateFailed records the failed attempt of a candidate that
// could not produce its task: failure counters, metric, retry intent.
//
// Nothing is rolled back — getGrowingSourceSyncTask sets syncing only on its
// success paths, so there is no bookkeeping to reverse. The segment's own state
// is left alone too: a claimed flush stays claimed, and GetSealedSegmentsPolicy
// re-selects Flushing segments, so the retry resumes the SAME flush instead of
// re-deciding what to flush.
func (wb *writeBufferBase) noteGrowingSourceCandidateFailed(segmentID int64) {
	if progress, ok := wb.growingSourceProgress[segmentID]; ok {
		wb.failGrowingSyncLocked(progress, errGrowingSourceUnavailable)
		wb.observeGrowingSourceSyncFailureLocked(segmentID, progress)
		wb.scheduleGrowingSourceRetryLocked(segmentID)
	}
}

// settleFailedGrowingTaskLocked returns what a failed task RESERVED: the
// metacache syncing rows it claimed, and the checkpoint candidate it pinned.
//
// Derived entirely from the task, and therefore run unconditionally — never
// under a lookup of growingSourceProgress. A concurrent abortDrop clears that
// map while tasks are still in flight, and a callback landing afterwards used to
// skip this entirely: the segment kept inflated syncingRows forever and the
// channel checkpoint stayed pinned behind a candidate nobody would remove.
func (wb *writeBufferBase) settleFailedGrowingTaskLocked(task *syncmgr.GrowingSourceSyncTask) {
	if task.BatchRows() > 0 {
		wb.metaCache.UpdateSegments(metacache.AbortSyncing(task.BatchRows()), metacache.WithSegmentIDs(task.SegmentID()))
	}
	if task.StartPosition() != nil {
		wb.syncCheckpoint.Remove(task.SegmentID(), task.StartPosition().GetTimestamp())
	}
}

func (wb *writeBufferBase) observeGrowingSourceSyncFailureLocked(segmentID int64, progress *growingSourceProgress) {
	metrics.DataNodeGrowingSourceSyncFailureCount.WithLabelValues(
		paramtable.GetStringNodeID(),
		fmt.Sprint(wb.collectionID),
		wb.channelName,
	).Set(float64(progress.failureCount))

	if progress.failureCount < growingSourceSyncFailureWarnThreshold ||
		progress.failureCount%growingSourceSyncFailureWarnThreshold != 0 {
		return
	}

	wb.growingSourceRatedLogger.RatedWarn(context.TODO(), rate.Limit(1), "growing-source source sync keeps failing",
		mlog.Int64("segmentID", segmentID),
		mlog.Int64("failureCount", progress.failureCount),
		mlog.Uint64("lastFlushedTs", progress.lastFlushedTs),
		mlog.String("lastFailure", progress.lastFailure),
	)
}

func (wb *writeBufferBase) resetGrowingSourceSyncFailureMetric(segmentID int64) {
	metrics.DataNodeGrowingSourceSyncFailureCount.WithLabelValues(
		paramtable.GetStringNodeID(),
		fmt.Sprint(wb.collectionID),
		wb.channelName,
	).Set(0)
	if progress, ok := wb.growingSourceProgress[segmentID]; ok {
		progress.failureCount = 0
		progress.lastFailure = ""
	}
}

func (wb *writeBufferBase) getGrowingSourceSyncTask(ctx context.Context, segmentInfo *metacache.SegmentInfo, progress *growingSourceProgress) (syncmgr.Task, error) {
	if segmentInfo.GetStorageVersion() != storage.StorageV3 {
		return nil, merr.WrapErrServiceInternalMsg("growing-source sync requires StorageV3 segment, segmentID=%d storageVersion=%d",
			segmentInfo.SegmentID(), segmentInfo.GetStorageVersion())
	}
	pendingCommitted := progress.pendingCommitted
	startPos := progress.firstUncommittedPosition()

	// The flush target is a POSITION this side already holds — the newest pack
	// recorded for this segment — not a count derived from anything. It is
	// published unchanged as the checkpoint, so the range written and the
	// position published cannot drift apart.
	checkpoint := progress.flushTarget()
	if pendingCommitted != nil {
		// Replay of a flush whose data is already in storage: publish the exact
		// position that manifest covers, never a newer one.
		checkpoint = pendingCommitted.checkpoint
	}
	if checkpoint == nil {
		// No pack recorded: this is a metadata-only flush (a sealed segment that
		// owes a flush but holds no new rows). The fence must NOT advance here —
		// falling back to the channel's latest consumed position would move
		// lastFlushedTs over ground this segment never verified, and the next
		// flush would start above it. Re-publish the position already reached.
		checkpoint = segmentInfo.LastFlushPosition()
	}
	if checkpoint == nil {
		checkpoint = startPos
	}
	schemaTimestamp := uint64(0)
	if startPos != nil {
		schemaTimestamp = startPos.GetTimestamp()
	}
	var source syncmgr.GrowingFlushSource
	if pendingCommitted == nil {
		var state syncmgr.GrowingSourceState
		source, state = wb.getGrowingSource(progress.segmentID, checkpoint)
		if state != syncmgr.GrowingSourceUsable {
			if source != nil {
				source.Release()
			}
			return nil, errors.Wrapf(errGrowingSourceUnavailable, "segment %d state %d", progress.segmentID, state)
		}
	} else {
		var state syncmgr.GrowingSourceState
		source, state = wb.getGrowingSource(progress.segmentID, checkpoint)
		if state != syncmgr.GrowingSourceUsable {
			if source != nil {
				source.Release()
				source = nil
			}
			wb.logger.Warn(ctx, "growing source unavailable during committed flush ack retry; retrying SaveBinlogPaths without re-flush",
				mlog.Int64("segmentID", progress.segmentID),
				mlog.Uint64("flushThroughTs", checkpoint.GetTimestamp()),
				mlog.Int("state", int(state)))
		}
	}

	// This side's own tally of the rows in the range, used only to cross-check
	// what the source reports it wrote. claimedRows is zero here — a new task
	// is only built when none is in flight — so this is pendingRows() minus a
	// structural zero, kept as a subtraction for the day single-flight is
	// relaxed. A committed-flush replay carries the count frozen with its
	// manifest instead.
	batchSize := progress.pendingRows() - progress.claimedRows
	if pendingCommitted != nil {
		batchSize = pendingCommitted.batchRows
	}
	buildTask := func(batchRows int64) *syncmgr.GrowingSourceSyncTask {
		task := syncmgr.NewGrowingSourceSyncTask().
			WithCollectionID(wb.collectionID).
			WithPartitionID(segmentInfo.PartitionID()).
			WithSegmentID(progress.segmentID).
			WithChannelName(wb.channelName).
			WithStartPosition(startPos).
			WithCheckpoint(checkpoint).
			WithBatchRows(batchRows).
			WithFlushFromTs(progress.lastFlushedTs).
			WithLevel(segmentInfo.Level()).
			WithMetaCache(wb.metaCache).
			WithMetaWriter(wb.metaWriter).
			WithSchema(wb.metaCache.GetSchema(schemaTimestamp)).
			WithAllocator(wb.allocator).
			WithStorageConfig(packed.CreateStorageConfig()).
			// Non-fatal on purpose: the rows stay pinned in the growing segment
			// until CommitGrowingFlush, so a failed attempt costs nothing but a
			// round trip. A retryable failure only arms the segment's intent
			// (scheduleGrowingSourceRetryLocked); driveGrowingSourceRetries
			// builds the NEXT attempt. Escalation to the fatal handler happens
			// only where recovery is impossible — see the SyncTerminal branch in
			// finishGrowingSourceSync.
			WithFailureCallback(wb.growingSourceErrHandler)
		if source != nil {
			task.WithSource(source)
		}
		if pendingCommitted != nil {
			task.WithCommittedFlush(pendingCommitted.manifestPath, cloneBM25StatsMap(pendingCommitted.bm25Stats), pendingCommitted.insertBinlogs)
			task.WithCommittedPKStats(pendingCommitted.pkStats)
		}
		// The finalization flags come from the frozen attempt when replaying a
		// committed manifest: the manifest covers exactly the frozen range, so
		// only the attempt that wrote it knows whether it was final. Deriving
		// them from the CURRENT state would upgrade a periodic sync to the
		// final flush after a concurrent seal — publishing a manifest that does
		// not cover the sealed tail as the segment's last word.
		if pendingCommitted != nil {
			if pendingCommitted.isFlush {
				task.WithFlush()
			}
			// isFlush stays frozen — deriving it from current state could
			// upgrade a periodic manifest to the final flush after a
			// concurrent seal. Drop is different: it is a monotonic terminal
			// state that may have arrived AFTER the attempt was frozen, and a
			// replay that ignores it publishes Flushed over Dropped and then
			// removes the segment from the metacache — at which point no drop
			// task can ever be built and the Drop is lost. The CURRENT Dropped
			// state therefore supersedes the frozen isDrop.
			if pendingCommitted.isDrop || segmentInfo.State() == commonpb.SegmentState_Dropped {
				task.WithDrop()
			}
			return task
		}
		if segmentInfo.State() == commonpb.SegmentState_Flushing {
			task.WithFlush()
		}
		if segmentInfo.State() == commonpb.SegmentState_Dropped {
			task.WithDrop()
		}
		return task
	}

	if batchSize <= 0 {
		progress.syncing = true
		return buildTask(0), nil
	}

	if startPos != nil {
		wb.syncCheckpoint.Add(progress.segmentID, startPos, "growing source syncing task")
	}
	progress.syncing = true
	progress.claimedRows = batchSize
	wb.metaCache.UpdateSegments(metacache.StartSyncing(batchSize), metacache.WithSegmentIDs(progress.segmentID))

	return buildTask(batchSize), nil
}

// growingSourceProgress and friends: the per-segment state this path owns.
//
// A growing-source flush does not take the rows — they stay pinned in segcore
// until CommitGrowingFlush — so progress is expressed as offsets and batches
// rather than as a queue of tasks holding payload.

const growingSourceSyncFailureWarnThreshold = 600

var errGrowingSourceUnavailable = errors.New("growing source is unavailable")

type growingSourceProgress struct {
	segmentID int64
	// lastFlushedTs is the timestamp of the position this segment was last
	// flushed through — the lower fence of the next flush. It is the only
	// "how far along am I" state this side keeps, and it is a POSITION, not a
	// row count: row counts live in the growing segment's own coordinate
	// system, which a WAL replay resets.
	lastFlushedTs uint64
	syncing       bool
	// claimedRows is how many of pendingRows() the in-flight task took as its
	// batch. Owned by progress and updated under the same lock as batches, so
	// "buffered = pendingRows() - claimedRows" is a single-clock read — the
	// metacache SyncingRows counter tracks the same quantity but is advanced by
	// the task at different moments, and subtracting across the two clocks
	// produced transient negatives. Zero whenever no task is in flight.
	claimedRows int64
	// owesFlush is sticky: seal sets it, and only a successful flush task clears
	// it. NOT the same bit as intent — a segment can owe a flush while having no
	// outstanding attempt to make, and vice versa.
	owesFlush           bool
	pendingCommitted    *growingSourcePendingCommittedFlush
	nonRetryableFailure bool
	batches             []growingSourceProgressBatch
	failureCount        int64
	lastFailure         string
	// intent is this segment's flush debt, the same type the write-buffer queue
	// uses. Distinct from owesFlush: intent is "try again", owesFlush is "a
	// FLUSH is still owed".
	intent flushIntent
}

// growingSourcePendingCommittedFlush is a flush whose DATA reached storage but
// whose metadata commit did not. The retry must re-publish exactly what was
// written, so the position and the row count are FROZEN here alongside the
// manifest.
//
// Re-deriving them from the live progress would be silent data loss: by the time
// the retry runs, later packs may have been recorded, and the retry would then
// publish the newer position while reusing the old manifest — acking away rows
// that were never persisted.
type growingSourcePendingCommittedFlush struct {
	checkpoint       *msgpb.MsgPosition
	batchRows        int64
	flushedThroughTs uint64
	// isFlush/isDrop are frozen with the manifest, NOT re-derived from the
	// segment's state at retry time. A periodic sync to T1 whose ack failed can
	// be retried after the segment sealed with T2 data recorded; deriving the
	// flag then would replay the T1-only manifest as the FINAL flush — Commit
	// would mark the segment Flushed and remove it while T2 still pins the
	// checkpoint, with no way to ever build the task that covers it.
	isFlush       bool
	isDrop        bool
	manifestPath  string
	bm25Stats     map[int64]*storage.BM25Stats
	insertBinlogs map[int64]*datapb.FieldBinlog
	pkStats       *storage.PrimaryKeyStats
}

// growingSourceProgressBatch is one WAL message pack's contribution to a
// segment, recorded when the pack was consumed.
//
// endPosition is the flush fence AND the checkpoint: a pack's rows all carry a
// timestamp <= its end position's, and the next pack's rows carry a strictly
// greater one, so a fence set here can never split a pack. rowNum is this
// side's own tally, used only to cross-check what the source reports it wrote.
type growingSourceProgressBatch struct {
	startPosition *msgpb.MsgPosition
	endPosition   *msgpb.MsgPosition
	rowNum        int64
}

func (p *growingSourceProgress) firstUncommittedPosition() *msgpb.MsgPosition {
	if len(p.batches) == 0 {
		return nil
	}
	return p.batches[0].startPosition
}

// flushTarget is the position the next flush should run to: the newest pack
// recorded for this segment. Everything recorded is flushed in one go — there is
// no partial target to compute, because the fence is a position this side
// already holds rather than a count it has to derive.
func (p *growingSourceProgress) flushTarget() *msgpb.MsgPosition {
	if len(p.batches) == 0 {
		return nil
	}
	return p.batches[len(p.batches)-1].endPosition
}

// handoffFenceTs is how far this segment must be flushed before its growing
// source may be released, as a WAL timestamp.
//
// With packs outstanding it is the newest one's end position — releasing before
// that would drop rows still only in the segment. With none outstanding the
// segment still owes a metadata-only final flush, so the fence is where it was
// last flushed to; reporting zero there would skip retention entirely and let
// the source go away before that flush runs.
func (p *growingSourceProgress) handoffFenceTs() uint64 {
	if target := p.flushTarget(); target != nil {
		return target.GetTimestamp()
	}
	return p.lastFlushedTs
}

// pendingRows is this side's tally of the rows recorded but not yet flushed.
// Cross-checked against the source's report; never used to bound the flush.
func (p *growingSourceProgress) pendingRows() int64 {
	var rows int64
	for _, batch := range p.batches {
		rows += batch.rowNum
	}
	return rows
}

// ack records that everything through flushedThroughTs is now persisted.
func (p *growingSourceProgress) ack(flushedThroughTs uint64) {
	keepIdx := 0
	for keepIdx < len(p.batches) && p.batches[keepIdx].endPosition.GetTimestamp() <= flushedThroughTs {
		keepIdx++
	}
	p.batches = p.batches[keepIdx:]
	if flushedThroughTs > p.lastFlushedTs {
		p.lastFlushedTs = flushedThroughTs
	}
	if p.pendingCommitted != nil && flushedThroughTs >= p.pendingCommitted.flushedThroughTs {
		p.pendingCommitted = nil
	}
	p.syncing = false
	p.claimedRows = 0
	p.failureCount = 0
	p.lastFailure = ""
}

func (p *growingSourceProgress) failSync(err error) {
	p.syncing = false
	p.claimedRows = 0
	p.failureCount++
	if err != nil {
		p.lastFailure = err.Error()
	}
}

// ackGrowingSyncLocked and failGrowingSyncLocked are the ONLY ways a progress may
// leave the in-flight state. They exist so the wake-up cannot be forgotten:
// waitSyncsSettled blocks on growingSettled rather than polling, so a caller that
// cleared `syncing` without broadcasting would hang every shutdown until the
// grace expired.
func (wb *writeBufferBase) ackGrowingSyncLocked(progress *growingSourceProgress, flushedThroughTs uint64) {
	progress.ack(flushedThroughTs)
	wb.notifyGrowingSettledLocked()
}

func (wb *writeBufferBase) failGrowingSyncLocked(progress *growingSourceProgress, err error) {
	progress.failSync(err)
	wb.notifyGrowingSettledLocked()
}

func (wb *writeBufferBase) notifyGrowingSettledLocked() {
	close(wb.growingSettled)
	wb.growingSettled = make(chan struct{})
}

// anyGrowingSyncingLocked reports whether any growing-source flush is in flight.
func (wb *writeBufferBase) anyGrowingSyncingLocked() bool {
	for _, progress := range wb.growingSourceProgress {
		if progress.syncing {
			return true
		}
	}
	return false
}

func (p *growingSourceProgress) markNonRetryableFailure() {
	p.nonRetryableFailure = true
}

func cloneBM25StatsMap(stats map[int64]*storage.BM25Stats) map[int64]*storage.BM25Stats {
	if len(stats) == 0 {
		return nil
	}
	cloned := make(map[int64]*storage.BM25Stats, len(stats))
	for fieldID, stat := range stats {
		if stat != nil {
			cloned[fieldID] = stat.Clone()
		}
	}
	return cloned
}
