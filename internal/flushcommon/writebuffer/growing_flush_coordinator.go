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
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// growingFlushCoordinator is the write buffer's growing-segment flush path:
// source selection, per-segment progress, handoff to the ordinary path, and
// retry state. It is the counterpart of ordinarySyncCoordinator, which owns the
// path that flushes payload yielded out of the write buffer.
//
// The two differ in one way that explains most of the code below: a
// growing-source flush does NOT take ownership of the rows. They stay pinned in
// the segcore growing segment until CommitGrowingFlush, so a failed attempt
// costs a round trip and nothing else. The ordinary path yields its payload and
// must therefore hold on to a failed task until it succeeds.
//
// State lives in writeBufferBase.growing and is guarded by writeBufferBase.mut.
// It deliberately does not take a lock of its own: every decision here reads
// buffer state (buffers, metaCache, checkpoint) under that same lock, and a
// second lock would only add an ordering problem.

func (wb *writeBufferBase) AllowGrowingSourceFlush() bool {
	return wb.allowGrowingSourceFlush
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
			progress.TargetOffset = growingProgress.targetOffset
			progress.NeedReleaseHandoff = wb.growingProgressRequiresHandoff(segmentID, growingProgress)
			progress.SourceMode = metacache.FlushSourceGrowing
		}
		if progress.NeedReleaseHandoff {
			releaseSegments = append(releaseSegments, syncmgr.GrowingSourceReleaseHandoffSegment{
				SegmentID:    segmentID,
				TargetOffset: progress.TargetOffset,
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

func (wb *writeBufferBase) decideGrowingFlushSource(segmentID int64, targetOffset int64, endPos *msgpb.MsgPosition) growingFlushSourceDecision {
	// 1. Honor the sticky decision recorded in metacache. Once the first
	//    insert for a segment commits a source choice, every subsequent call
	//    must return the same kind so that progress / payload tracking stays
	//    consistent for the segment's lifetime.
	if seg, ok := wb.metaCache.GetSegmentByID(segmentID); ok {
		if seg.GetStorageVersion() != storage.StorageV3 {
			return growingFlushSourceDecision{sourceType: metacache.FlushSourceWriteBuffer}
		}
		switch seg.FlushSourceMode() {
		case metacache.FlushSourceGrowing:
			state := wb.getGrowingSourceState(segmentID, targetOffset, endPos)
			return growingFlushSourceDecision{
				sourceType:  metacache.FlushSourceGrowing,
				sourceState: state,
			}
		case metacache.FlushSourceWriteBuffer:
			return growingFlushSourceDecision{sourceType: metacache.FlushSourceWriteBuffer}
		}
	}

	// 2. Fallback for the brief window where in-memory bookkeeping has been
	//    populated but the metacache sticky bit hasn't been set yet (e.g. on
	//    re-entry after a partial state).
	if wb.hasGrowingSourceProgress(segmentID) {
		state := wb.getGrowingSourceState(segmentID, targetOffset, endPos)
		return growingFlushSourceDecision{
			sourceType:  metacache.FlushSourceGrowing,
			sourceState: state,
		}
	}

	if wb.hasWriteBufferInsertPayload(segmentID) {
		return growingFlushSourceDecision{sourceType: metacache.FlushSourceWriteBuffer}
	}

	state := wb.getGrowingSourceState(segmentID, targetOffset, endPos)
	if state == syncmgr.GrowingSourceUsable || state == syncmgr.GrowingSourcePending {
		return growingFlushSourceDecision{
			sourceType:  metacache.FlushSourceGrowing,
			sourceState: state,
		}
	}
	wb.warnGrowingSourceFallback(segmentID, targetOffset, endPos)
	return growingFlushSourceDecision{sourceType: metacache.FlushSourceWriteBuffer}
}

func (wb *writeBufferBase) getGrowingSource(segmentID int64, targetOffset int64, endPos *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
	if wb.growingSourceResolver == nil {
		return nil, syncmgr.GrowingSourceUnavailable
	}
	return wb.growingSourceResolver(segmentID, targetOffset, endPos)
}

func (wb *writeBufferBase) getGrowingSourceState(segmentID int64, targetOffset int64, endPos *msgpb.MsgPosition) syncmgr.GrowingSourceState {
	source, state := wb.getGrowingSource(segmentID, targetOffset, endPos)
	if source != nil {
		source.Release()
	}
	return state
}

func (wb *writeBufferBase) warnGrowingSourceFallback(segmentID int64, targetOffset int64, endPos *msgpb.MsgPosition) {
	if !wb.allowGrowingSourceFlush {
		return
	}
	wb.growingSourceRatedLogger.RatedWarn(context.TODO(), rate.Limit(1), "growing-source source is unavailable, fallback to WriteBuffer",
		mlog.Int64("segmentID", segmentID),
		mlog.Int64("targetOffset", targetOffset),
		mlog.Any("endPosition", endPos),
	)
}

func (wb *writeBufferBase) growingSourceProgressSyncable(segmentID int64, progress *growingSourceProgress, rollbackFlushing bool, markSealedFlushing bool) (bool, bool) {
	if progress.nonRetryableFailure {
		return false, false
	}
	if progress.syncing {
		if segment, ok := wb.metaCache.GetSegmentByID(segmentID); ok &&
			(segment.State() == commonpb.SegmentState_Sealed || segment.State() == commonpb.SegmentState_Flushing) {
			progress.pendingFlush = true
		}
		return false, false
	}
	if progress.pendingCommitted != nil {
		if markSealedFlushing {
			if segment, ok := wb.metaCache.GetSegmentByID(segmentID); ok && segment.State() == commonpb.SegmentState_Sealed {
				wb.metaCache.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Flushing), metacache.WithSegmentIDs(segmentID))
			}
		}
		return true, false
	}
	if len(progress.batches) == 0 && !progress.pendingFlush {
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
	state := wb.getGrowingSourceState(segmentID, progress.targetOffset, checkpoint)
	if state == syncmgr.GrowingSourceUsable {
		if markSealedFlushing {
			if segment, ok := wb.metaCache.GetSegmentByID(segmentID); ok && segment.State() == commonpb.SegmentState_Sealed {
				wb.metaCache.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Flushing), metacache.WithSegmentIDs(segmentID))
			}
		}
		return true, false
	}

	// GetSealedSegmentsPolicy moves Sealed -> Flushing before returning the
	// candidate. If the growing source is only pending, roll it back so the
	// sealed segment can be selected again when the source catches up.
	if rollbackFlushing {
		if segment, ok := wb.metaCache.GetSegmentByID(segmentID); ok && segment.State() == commonpb.SegmentState_Flushing {
			wb.metaCache.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Sealed), metacache.WithSegmentIDs(segmentID))
		}
	}
	return false, true
}

// scheduleGrowingSourceRetryLocked only records that a retry is wanted. There is
// no timer: driveGrowingSourceRetries picks it up on the next timetick, the same
// signal the ordinary queue rides.
func (wb *writeBufferBase) scheduleGrowingSourceRetryLocked() {
	if wb.closed || wb.dropping || wb.flushRetryInterval < 0 || len(wb.growingSourceProgress) == 0 {
		return
	}
	if !wb.growingSourceRetryScheduled {
		// Stamp the round's start so the FIRST retry honours the interval too;
		// a zero growingSourceLastRetry made the very next timetick fire it.
		wb.growingSourceLastRetry = time.Now()
	}
	wb.growingSourceRetryScheduled = true
}

// driveGrowingSourceRetries re-submits growing-source flushes that asked for
// another round, no more often than the configured interval.
func (wb *writeBufferBase) driveGrowingSourceRetries(ctx context.Context, now time.Time, interval time.Duration) {
	wb.mut.Lock()
	if wb.closed || wb.dropping || !wb.growingSourceRetryScheduled ||
		wb.checkpoint == nil || len(wb.growingSourceProgress) == 0 ||
		now.Sub(wb.growingSourceLastRetry) < interval {
		wb.mut.Unlock()
		return
	}
	wb.growingSourceRetryScheduled = false
	wb.growingSourceLastRetry = now

	segmentIDs, retryNeeded := wb.getGrowingSourceSegmentsToRetry()
	if retryNeeded {
		wb.growingSourceRetryScheduled = true
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

func (wb *writeBufferBase) getGrowingSourceSegmentsToRetry() ([]int64, bool) {
	segments := make([]int64, 0, len(wb.growingSourceProgress))
	retryNeeded := false
	for segmentID, progress := range wb.growingSourceProgress {
		syncable, retry := wb.growingSourceProgressSyncable(segmentID, progress, false, true)
		retryNeeded = retryNeeded || retry
		if syncable {
			segments = append(segments, segmentID)
		}
	}
	return segments, retryNeeded
}

func (wb *writeBufferBase) recordGrowingSourceProgress(inData *InsertData, startPos, endPos *msgpb.MsgPosition, schemaVersion int32, targetOffset int64) error {
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
			segmentID:    inData.segmentID,
			targetOffset: targetOffset - inData.rowNum,
		}
		wb.growingSourceProgress[inData.segmentID] = progress
	}
	progress.targetOffset += inData.rowNum
	progress.batches = append(progress.batches, growingSourceProgressBatch{
		startPosition: startPos,
		endPosition:   endPos,
		endOffset:     progress.targetOffset,
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

func (wb *writeBufferBase) growingSourceTargetOffset(segmentID int64, rows int64) int64 {
	return wb.growingSourceBaseOffset(segmentID) + rows
}

func (wb *writeBufferBase) growingSourceBaseOffset(segmentID int64) int64 {
	if progress, ok := wb.growingSourceProgress[segmentID]; ok {
		return progress.targetOffset
	}
	if segment, ok := wb.metaCache.GetSegmentByID(segmentID); ok {
		return segment.NumOfRows()
	}
	return 0
}

func (wb *writeBufferBase) updateGrowingSourceBufferedRows(progress *growingSourceProgress) metacache.SegmentAction {
	return func(info *metacache.SegmentInfo) {
		bufferedRows := progress.targetOffset - info.FlushedRows() - info.SyncingRows()
		if bufferedRows < 0 {
			bufferedRows = 0
		}
		metacache.UpdateBufferedRows(bufferedRows)(info)
	}
}

func (wb *writeBufferBase) growingSourceProgressSelectedByPolicy(ts typeutil.Timestamp, segmentID int64, progress *growingSourceProgress) bool {
	if progress == nil {
		return false
	}
	if progress.nonRetryableFailure {
		return false
	}
	if progress.pendingFlush {
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
	rows := progress.targetOffset - segment.FlushedRows() - segment.SyncingRows()
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

func (wb *writeBufferBase) rollbackGrowingSourceSyncCandidate(segmentID int64) {
	if progress, ok := wb.growingSourceProgress[segmentID]; ok {
		progress.failSync(errGrowingSourceUnavailable)
		wb.observeGrowingSourceSyncFailureLocked(segmentID, progress)
		wb.scheduleGrowingSourceRetryLocked()
	}
	if segment, ok := wb.metaCache.GetSegmentByID(segmentID); ok && segment.State() == commonpb.SegmentState_Flushing {
		wb.metaCache.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Sealed), metacache.WithSegmentIDs(segmentID))
	}
}

func (wb *writeBufferBase) rollbackGrowingSourceSyncTaskLocked(task *syncmgr.GrowingSourceSyncTask) {
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
		mlog.Int64("targetOffset", progress.targetOffset),
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
	targetOffset := progress.targetOffset
	pendingCommitted := progress.pendingCommitted
	if pendingCommitted != nil {
		targetOffset = pendingCommitted.targetOffset
	}
	checkpoint := progress.checkpointFor(targetOffset)
	startPos := progress.firstUncommittedPosition()
	if checkpoint == nil {
		checkpoint = startPos
	}
	if checkpoint == nil {
		checkpoint = wb.checkpoint
	}
	schemaTimestamp := uint64(0)
	if startPos != nil {
		schemaTimestamp = startPos.GetTimestamp()
	}
	var source syncmgr.GrowingFlushSource
	if pendingCommitted == nil {
		var state syncmgr.GrowingSourceState
		source, state = wb.getGrowingSource(progress.segmentID, targetOffset, checkpoint)
		if state != syncmgr.GrowingSourceUsable {
			if source != nil {
				source.Release()
			}
			return nil, errors.Wrapf(errGrowingSourceUnavailable, "segment %d state %d", progress.segmentID, state)
		}
	} else {
		var state syncmgr.GrowingSourceState
		source, state = wb.getGrowingSource(progress.segmentID, targetOffset, checkpoint)
		if state != syncmgr.GrowingSourceUsable {
			if source != nil {
				source.Release()
				source = nil
			}
			wb.logger.Warn(ctx, "growing source unavailable during committed flush ack retry; retrying SaveBinlogPaths without re-flush",
				mlog.Int64("segmentID", progress.segmentID),
				mlog.Int64("targetOffset", targetOffset),
				mlog.Int("state", int(state)))
		}
	}

	batchSize := targetOffset - segmentInfo.FlushedRows() - segmentInfo.SyncingRows()
	buildTask := func(batchRows int64) *syncmgr.GrowingSourceSyncTask {
		task := syncmgr.NewGrowingSourceSyncTask().
			WithCollectionID(wb.collectionID).
			WithPartitionID(segmentInfo.PartitionID()).
			WithSegmentID(progress.segmentID).
			WithChannelName(wb.channelName).
			WithStartPosition(startPos).
			WithCheckpoint(checkpoint).
			WithBatchRows(batchRows).
			WithTargetOffset(targetOffset).
			WithLevel(segmentInfo.Level()).
			WithMetaCache(wb.metaCache).
			WithMetaWriter(wb.metaWriter).
			WithSchema(wb.metaCache.GetSchema(schemaTimestamp)).
			WithAllocator(wb.allocator).
			WithStorageConfig(packed.CreateStorageConfig()).
			// Non-fatal on purpose: this task is re-submitted by
			// scheduleGrowingSourceRetryLocked, and the rows it flushes stay
			// pinned in the growing segment until CommitGrowingFlush, so a
			// failed attempt costs nothing but a round trip. Escalation to the
			// fatal handler happens only where recovery is impossible — see the
			// terminal branch in submitSyncTasks.
			WithFailureCallback(wb.growingSourceErrHandler)
		if source != nil {
			task.WithSource(source)
		}
		if pendingCommitted != nil {
			task.WithCommittedFlush(pendingCommitted.manifestPath, cloneBM25StatsMap(pendingCommitted.bm25Stats), pendingCommitted.insertBinlogs)
			task.WithCommittedPKStats(pendingCommitted.pkStats)
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
		progress.syncingOffset = targetOffset
		return buildTask(0), nil
	}

	if startPos != nil {
		wb.syncCheckpoint.Add(progress.segmentID, startPos, "growing source syncing task")
	}
	progress.syncing = true
	progress.syncingOffset = targetOffset
	wb.metaCache.UpdateSegments(metacache.StartSyncing(batchSize), metacache.WithSegmentIDs(progress.segmentID))

	return buildTask(batchSize), nil
}

func (wb *writeBufferBase) waitGrowingSourceSyncs(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	// Deadline for the post-cancellation wait. An in-flight
	// C.FlushGrowingSegmentData takes no cancellation token, so once it has
	// started nothing on the Go side can preempt it — without this the wait
	// is unbounded and no configured timeout is a real upper bound.
	var graceDeadline time.Time
	for {
		wb.mut.RLock()
		inFlight := false
		for _, progress := range wb.growingSourceProgress {
			if progress.syncing {
				inFlight = true
				break
			}
		}
		wb.mut.RUnlock()
		if !inFlight {
			if err := ctx.Err(); err != nil {
				return err
			}
			return nil
		}
		if !graceDeadline.IsZero() {
			if time.Now().After(graceDeadline) {
				// Give up the wait, not the work: the task holds a segment pin,
				// so the native flush finishes safely in the background and its
				// callback rolls back on an orphaned progress entry. Returning
				// keeps the caller's timeout an actual bound — a Drop turns this
				// into its loud abort path, and a channel release stops hanging
				// shutdown behind a stuck write.
				wb.logger.Warn(wb.syncCtx, "giving up the wait for an in-flight "+
					"growing-source native flush; it cannot be preempted and is "+
					"left to finish in the background", mlog.Err(ctx.Err()))
				return ctx.Err()
			}
			<-ticker.C
			continue
		}
		select {
		case <-ctx.Done():
			// Pre-existing attempts were submitted with wb.syncCtx before Drop
			// took ownership. Cancel both generations, then give whatever IS
			// cancellable a bounded grace period to unwind.
			wb.syncCancel()
			graceDeadline = time.Now().Add(growingFlushCancelGrace)
		case <-ticker.C:
		}
	}
}
