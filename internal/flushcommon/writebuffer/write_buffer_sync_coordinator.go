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
	"time"

	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// writeBufferSyncEntry is one flush task and the write buffer's ownership of it.
// The entry — and the payload its task holds — survives a failed attempt: the
// task stays at its place in the queue and is submitted again by the retry
// drive. Only success or a terminal failure removes it.
type writeBufferSyncEntry struct {
	task            *syncmgr.SyncTask
	payloadReleased chan struct{}
	done            chan struct{}
	terminalErr     error
	submitted       bool
	failed          bool
	// everFailed stays set across re-drives (failed flips back on submit).
	// It marks the segment as flush-impaired: backpressure then waits on task
	// COMPLETION, not just payload release — a Commit stuck in retry holds no
	// payload, and without this its buffered tail grows with no bound at all.
	everFailed bool
}

type writeBufferSyncQueue struct {
	entries []*writeBufferSyncEntry
	// intent is this segment's flush debt — refused admission, or a failed
	// attempt waiting out its interval. One per queue, not per entry: a retry
	// replays the segment from its oldest task, so the queue advances as a unit.
	intent flushIntent
}

// needsRetryLocked reports whether this segment has a failed attempt that is due
// for another one.
//
// NOTHING may be in flight. A re-drive replays the whole queue from its oldest
// task, so a task the dispatcher still holds would be submitted a second time —
// and a second submission against an aborted key finishes SYNCHRONOUSLY, so the
// terminal branch would Abandon a task whose first Prepare is still writing:
// its payload is pulled out from under the writer, and the preparedV3 handle it
// assigns afterwards has no owner left to Destroy it.
//
// This is reachable: the dispatcher aborts a whole key on one failure but
// drainAborted stops at the first entry that has not finished Prepare, so a
// failed head can coexist with a successor still inside a native write.
func (q *writeBufferSyncQueue) needsRetryLocked(now time.Time, interval time.Duration) bool {
	if len(q.entries) == 0 || !q.entries[0].failed || !q.intent.due(now, interval) {
		return false
	}
	for _, entry := range q.entries {
		if entry.submitted {
			return false
		}
	}
	return true
}

// driveRetries re-submits segments whose last flush attempt failed. It is
// called from the flowgraph on every timetick, which is why the sync path needs
// no timer of its own: timeticks arrive continuously while a channel is alive,
// and the only place that survives without them — Drop — drives this itself.
//
// Retry always restarts from the OLDEST pending task of the segment. The tasks
// behind it were built against state that task's commit was supposed to
// publish, so they can only be replayed in the same order.
func (wb *writeBufferBase) driveRetries(ctx context.Context) {
	interval := wb.retryInterval()
	if interval < 0 {
		return
	}
	now := time.Now()

	wb.mut.RLock()
	stopped := wb.closed || wb.dropping
	wb.mut.RUnlock()
	if stopped {
		return
	}
	wb.driveDueWriteBufferRetries(ctx, now, interval)
	wb.driveGrowingSourceRetries(ctx, now, interval)
}

// driveDueWriteBufferRetries re-submits every write-buffer entry due for another
// attempt. Deliberately NOT guarded on closed/dropping: whether a retry is still
// wanted depends on who is driving. The timetick drive stops during Drop because
// the drop wait takes over; the drop wait itself must keep retrying, or the very
// flush it is blocked on never runs again.
func (wb *writeBufferBase) driveDueWriteBufferRetries(ctx context.Context, now time.Time, interval time.Duration) {
	wb.mut.Lock()
	due := wb.dueWriteBufferRetriesLocked(now, interval)
	wb.mut.Unlock()
	for _, entry := range due {
		wb.resubmitWriteBufferSync(ctx, entry)
	}
}

func (wb *writeBufferBase) dueWriteBufferRetriesLocked(now time.Time, interval time.Duration) []*writeBufferSyncEntry {
	var due []*writeBufferSyncEntry
	for _, queue := range wb.writeBufferSyncQueues {
		if !queue.needsRetryLocked(now, interval) {
			continue
		}
		// Restart the rate limit, but keep the debt: only a completed task settles
		// it, and clearing here would lose a pending request for a NEW task that
		// this replay cannot serve.
		queue.intent.attempted(now)
		for _, entry := range queue.entries {
			entry.failed = false
			entry.submitted = false
			due = append(due, entry)
		}
	}
	return due
}

// rotateL0SegmentLocked retires the partition -> L0 segment mapping so the next
// delete opens a fresh segment.
//
// The anchor is task CONSTRUCTION — the moment this segment stops accepting new
// data — not flush completion. Both later anchors are wrong in their own way:
//
//   - Clearing only in BufferData's triggerSync loop (what this used to do) missed
//     every other route to a flush. A segment flushed by the memory watchdog's
//     EvictBuffer left the mapping behind, so the next delete recreated a buffer
//     for an ID whose segment was about to leave the metacache; every sync attempt
//     then died on segment-not-found and that buffer, plus the channel checkpoint
//     it pins, never moved again.
//   - Clearing on RemoveSegments (where this hung a moment ago) still leaves a
//     window: deletes arriving between task construction and flush completion join
//     a segment whose buffer has already been yielded, and are orphaned the instant
//     it is removed.
//
// Rotating at construction closes both: an L0 task always carries WithFlush, so
// building one is exactly the point after which nothing may be added.
func (wb *writeBufferBase) rotateL0SegmentLocked(segmentID int64) {
	if partition, ok := wb.l0partition[segmentID]; ok {
		delete(wb.l0partition, segmentID)
		delete(wb.l0Segments, partition)
	}
}

func (wb *writeBufferBase) registerWriteBufferSyncLocked(entry *writeBufferSyncEntry) {
	segmentID := entry.task.SegmentID()
	queue := wb.writeBufferSyncQueues[segmentID]
	if queue == nil {
		queue = &writeBufferSyncQueue{}
		wb.writeBufferSyncQueues[segmentID] = queue
	}
	queue.entries = append(queue.entries, entry)
}

func (wb *writeBufferBase) writeBufferSyncEntryLocked(task *syncmgr.SyncTask) *writeBufferSyncEntry {
	queue := wb.writeBufferSyncQueues[task.SegmentID()]
	if queue == nil {
		return nil
	}
	for _, entry := range queue.entries {
		if entry.task == task {
			return entry
		}
	}
	return nil
}

func (wb *writeBufferBase) removeWriteBufferSyncLocked(entry *writeBufferSyncEntry) {
	segmentID := entry.task.SegmentID()
	queue := wb.writeBufferSyncQueues[segmentID]
	if queue == nil {
		return
	}
	for i, candidate := range queue.entries {
		if candidate == entry {
			queue.entries = append(queue.entries[:i], queue.entries[i+1:]...)
			break
		}
	}
	if len(queue.entries) == 0 {
		delete(wb.writeBufferSyncQueues, segmentID)
	}
}

// segmentReorderWindow is how many tasks of one segment may be outstanding at
// once. They prepare in parallel and commit in order; the window is what turns
// that parallelism into a bounded amount of resident payload. It lives here
// because the write buffer is the only owner of the queue — the sync manager
// just executes what it is given.
const segmentReorderWindow = 5

// writeBufferSyncBlockedLocked reports whether segmentID may not produce a new sync
// task right now. It is read-only; callers that intend to build a task use
// deferWriteBufferSyncLocked instead.
//
// A segment is blocked when it still holds a task the dispatcher has not
// accepted yet (its payload accounting is not settled), when it has filled the
// reorder window, or when it is sealed/flushing/dropped — the final task of a
// segment must be built from committed counters, not from a snapshot that an
// uncommitted predecessor is about to change.
func (wb *writeBufferBase) writeBufferSyncBlockedLocked(segmentID int64) bool {
	queue := wb.writeBufferSyncQueues[segmentID]
	if queue == nil || len(queue.entries) == 0 {
		return false
	}
	if len(queue.entries) >= segmentReorderWindow {
		return true
	}
	for _, entry := range queue.entries {
		if !entry.submitted {
			return true
		}
	}
	if segment, exists := wb.metaCache.GetSegmentByID(segmentID); exists {
		switch segment.State() {
		case commonpb.SegmentState_Sealed, commonpb.SegmentState_Flushing, commonpb.SegmentState_Dropped:
			return true
		}
	}
	return false
}

// deferWriteBufferSyncLocked is writeBufferSyncBlockedLocked plus the note that this
// segment wants a task as soon as its queue drains.
func (wb *writeBufferBase) deferWriteBufferSyncLocked(segmentID int64) bool {
	if !wb.writeBufferSyncBlockedLocked(segmentID) {
		return false
	}
	wb.writeBufferSyncQueues[segmentID].intent.want()
	return true
}

func (wb *writeBufferBase) allWriteBufferSyncEntriesLocked() []*writeBufferSyncEntry {
	var entries []*writeBufferSyncEntry
	for _, queue := range wb.writeBufferSyncQueues {
		entries = append(entries, queue.entries...)
	}
	return entries
}

// takeParkedWriteBufferSyncsLocked removes and returns every entry that is parked
// waiting for a retry re-drive (failed, not in flight). Once nothing will ever
// drive retries again — Close, or an aborted Drop — these entries would hold
// their payload forever; the caller must hand them to
// abandonParkedWriteBufferSyncs outside the lock. In-flight entries are left
// alone: their own completion callback settles them.
func (wb *writeBufferBase) takeParkedWriteBufferSyncsLocked() []*writeBufferSyncEntry {
	var parked []*writeBufferSyncEntry
	for _, queue := range wb.writeBufferSyncQueues {
		for _, entry := range queue.entries {
			if entry.failed && !entry.submitted {
				parked = append(parked, entry)
			}
		}
	}
	for _, entry := range parked {
		wb.removeWriteBufferSyncLocked(entry)
	}
	return parked
}

// abandonParkedWriteBufferSyncs settles entries that were parked for a retry
// nothing will ever drive. Runs WITHOUT wb.mut held: Abandon releases the
// payload, whose accounting callback takes locks of its own.
func (wb *writeBufferBase) abandonParkedWriteBufferSyncs(parked []*writeBufferSyncEntry, err error) {
	for _, entry := range parked {
		wb.releaseTerminalSync(entry.task)
		entry.complete(err)
	}
}

func (entry *writeBufferSyncEntry) complete(err error) {
	entry.terminalErr = err
	close(entry.done)
}

// finishWriteBufferSync owns the write-buffer side of task completion.
//
// The dispatcher does NOT retry whole tasks — each phase only spends its own
// small inner IO budget. So a retryable error keeps this task queued for
// driveRetries to re-submit; only a terminal failure (or Close) ends the segment
// stream, leaving its checkpoint pinned for WAL replay.
func (wb *writeBufferBase) finishWriteBufferSync(ctx context.Context, entry *writeBufferSyncEntry, task *syncmgr.SyncTask, taskErr error) error {
	segmentID := task.SegmentID()
	var resyncTasks []syncmgr.Task
	var fatalErr error

	wb.mut.Lock()
	if wb.writeBufferSyncEntryLocked(task) != entry {
		wb.mut.Unlock()
		return taskErr
	}

	if taskErr != nil {
		// Retryable failures stay queued during Drop too: Drop is a synchronous
		// wait, and waitSyncsSettled drives these retries itself (timeticks
		// stop once DropChannel is delivered). Only Close makes retrying
		// pointless — nothing will ever drive it again.
		decision := syncmgr.ClassifySyncError(ctx, taskErr)
		if decision == syncmgr.SyncRetry && !wb.closed {
			// Keep everything: the payload, the metacache syncing counters and
			// the queue position. driveRetries submits this exact task object
			// again, and its Prepare will not rewrite what it already wrote.
			// HandleError is deliberately NOT called — it destroys the prepared
			// storage handle this retry is going to reuse.
			entry.failed = true
			entry.everFailed = true
			entry.submitted = false
			// Rate-limit from the FIRST failure, not from the next timetick.
			if queue := wb.writeBufferSyncQueues[segmentID]; queue != nil {
				queue.intent.backoff(time.Now())
			}
			wb.mut.Unlock()
			wb.retryRatedLogger.RatedWarn(ctx, rate.Limit(1), "flush attempt failed, will retry",
				mlog.Int64("segmentID", segmentID), mlog.Err(taskErr))
			return taskErr
		}
		// Terminal: this task will never run again.
		wb.releaseTerminalSync(task)
		wb.removeWriteBufferSyncLocked(entry)
		// During Drop the terminal error is not swallowed — it surfaces
		// through entry.terminalErr to waitSyncsSettled, whose caller (Close)
		// already escalates a failed Drop. Feeding it to the fatal errHandler
		// as well would just crash the process twice for one failure.
		//
		// SyncCanceled is excluded for the same reason it exists: the caller
		// went away, which is not a failure of the task. It reaches here with
		// the buffer still open on a graceful stop, because DataNode.Stop closes
		// the sync manager BEFORE the flowgraphs — the dispatcher aborts every
		// in-flight task with context.Canceled while wb.closed is still false.
		// Escalating that would turn every drain-with-traffic into a panic.
		if !wb.closed && !wb.dropping && decision != syncmgr.SyncCanceled {
			fatalErr = taskErr
		}
	} else {
		queue := wb.writeBufferSyncQueues[segmentID]
		resync := queue != nil && queue.intent.due(time.Now(), wb.retryInterval()) && wb.buffers[segmentID] != nil
		if queue != nil {
			queue.intent.clear()
		}
		if segment, exists := wb.metaCache.GetSegmentByID(segmentID); exists {
			switch segment.State() {
			case commonpb.SegmentState_Sealed, commonpb.SegmentState_Flushing:
				// The segment still owes a flush. getSyncTask claims Sealed on
				// the way in, so the resync task carries WithFlush and this
				// terminates after one round — before the claim moved there,
				// a Sealed segment could not produce a flush task at all and
				// this re-triggered forever on empty buffers.
				resync = resync || !task.IsFlush()
			case commonpb.SegmentState_Dropped:
				resync = resync || !task.IsDrop()
			}
		}
		if task.StartPosition() != nil {
			wb.syncCheckpoint.Remove(segmentID, task.StartPosition().GetTimestamp())
		}
		if task.IsFlush() {
			wb.metaCache.RemoveSegments(metacache.WithSegmentIDs(segmentID))
		}
		wb.removeWriteBufferSyncLocked(entry)
		if resync && !wb.closed && !wb.dropping {
			resyncTasks = wb.getSyncTasksLocked(wb.syncCtx, []int64{segmentID})
		}
	}
	wb.mut.Unlock()

	if len(resyncTasks) > 0 {
		go wb.submitSyncTasks(wb.syncCtx, resyncTasks)
	}

	defer entry.complete(taskErr)
	var firstPanic any
	run := func(callback func()) {
		defer func() {
			if panicValue := recover(); panicValue != nil && firstPanic == nil {
				firstPanic = panicValue
			}
		}()
		callback()
	}
	if wb.taskObserverCallback != nil {
		run(func() { wb.taskObserverCallback(task, taskErr) })
	}
	if fatalErr != nil {
		run(func() { wb.errHandler(fatalErr) })
	}
	if firstPanic != nil {
		panic(firstPanic)
	}
	return taskErr
}

// resubmitWriteBufferSync hands a retained task back to the sync manager. It is
// the same task object, so a Prepare that already succeeded is not redone.
func (wb *writeBufferBase) resubmitWriteBufferSync(ctx context.Context, entry *writeBufferSyncEntry) {
	wb.submitSyncTasks(ctx, []syncmgr.Task{entry.task})
}

// getWriteBufferSyncTask builds the task for the write-buffer path: it yields the
// segment's buffered payload and hands ownership of it to the task. Counterpart
// of getGrowingSourceSyncTask, which builds for the path whose rows stay pinned
// in segcore.
//
// Caller must hold wb.mut and must have gone through getSyncTask, which decides
// which of the two paths owns this flush and claims it.
func (wb *writeBufferBase) getWriteBufferSyncTask(ctx context.Context, segmentInfo *metacache.SegmentInfo) (syncmgr.Task, error) {
	segmentID := segmentInfo.SegmentID()
	var batchSize int64
	var insertMemSize int64
	var deleteMemSize int64
	var tsFrom, tsTo uint64

	if buffer := wb.buffers[segmentID]; buffer != nil {
		insertMemSize = buffer.insertBuffer.size
		deleteMemSize = buffer.deltaBuffer.size
	}
	if segmentInfo.Level() == datapb.SegmentLevel_L0 {
		// Retire the mapping BEFORE taking the buffer: from here on this segment
		// is being flushed and must not receive another delete.
		wb.rotateL0SegmentLocked(segmentID)
	}
	insert, bm25, delta, schema, timeRange, startPos := wb.yieldBuffer(segmentID)
	if timeRange != nil {
		tsFrom, tsTo = timeRange.timestampMin, timeRange.timestampMax
	}

	if startPos != nil {
		wb.syncCheckpoint.Add(segmentID, startPos, "syncing task")
	}

	for _, chunk := range insert {
		batchSize += int64(chunk.GetRowNum())
	}

	pack := &syncmgr.SyncPack{}
	pack.WithInsertData(insert).
		WithDeleteData(delta).
		WithCollectionID(wb.collectionID).
		WithPartitionID(segmentInfo.PartitionID()).
		WithChannelName(wb.channelName).
		WithSegmentID(segmentID).
		WithStartPosition(startPos).
		WithTimeRange(tsFrom, tsTo).
		WithLevel(segmentInfo.Level()).
		WithDataSource(metrics.StreamingDataSourceLabel).
		WithCheckpoint(wb.checkpoint).
		WithBatchRows(batchSize).
		// SyncManager only reports task-local errors (HandleError); each phase
		// has its own small inner IO retry budget. Whole-task retry and terminal
		// segment-stream cleanup belong to the write-buffer callback.
		WithErrorHandler(nil)

	if len(bm25) != 0 {
		pack.WithBM25Stats(bm25)
	}

	if segmentInfo.State() == commonpb.SegmentState_Flushing ||
		segmentInfo.Level() == datapb.SegmentLevel_L0 { // Level zero segment will always be sync as flushed
		pack.WithFlush()
	}

	if segmentInfo.State() == commonpb.SegmentState_Dropped {
		pack.WithDrop()
	}

	payloadReleased := make(chan struct{})
	task := syncmgr.NewSyncTask().
		WithAllocator(wb.allocator).
		WithMetaWriter(wb.metaWriter).
		WithMetaCache(wb.metaCache).
		WithSchema(schema).
		WithSyncPack(pack).
		WithStorageConfig(packed.CreateStorageConfig()).
		WithPayloadAccounting(insertMemSize, deleteMemSize, func(released int64) {
			// addBufferMetric, not a bare Sub: a task that outlives the bounded
			// shutdown wait lands here after the series was already deleted, and
			// settleBufferMetric has made this a no-op by then. Closing the
			// channel is NOT conditional — backpressure waits on it either way.
			wb.addBufferMetric(-released)
			close(payloadReleased)
		})

	// SetCurrentSplitIfNil carries its own guard: it is a no-op both when the
	// segment already has a split and when columnGroups is nil.
	columnGroups := task.ResolveColumnGroups(segmentInfo)
	wb.metaCache.UpdateSegments(metacache.MergeSegmentAction(
		metacache.StartSyncing(batchSize),
		metacache.SetCurrentSplitIfNil(columnGroups),
	), metacache.WithSegmentIDs(segmentID))
	task.WithFrozenColumnGroups(columnGroups)
	wb.registerWriteBufferSyncLocked(&writeBufferSyncEntry{
		task:            task,
		payloadReleased: payloadReleased,
		done:            make(chan struct{}),
	})
	return task, nil
}
