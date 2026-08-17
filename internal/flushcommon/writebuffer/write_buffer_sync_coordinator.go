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
	"math"
	"time"

	"github.com/cockroachdb/errors"

	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// writeBufferSyncEntry is one flush task and the write buffer's ownership of it.
//
// For an owned-payload task (*syncmgr.SyncTask) the entry — and the payload its
// task holds — survives a failed attempt: the task stays at its place in the
// queue and is submitted again by the retry drive. Only success or a terminal
// failure removes it.
//
// For a ref-payload task (*syncmgr.GrowingSourceSyncTask) the entry lives only
// while the attempt is in flight: a failed attempt costs nothing (the rows stay
// pinned in segcore), so the entry leaves the queue on ANY completion and a
// retry builds a fresh task with a fresh source lease.
type writeBufferSyncEntry struct {
	task            syncmgr.Task
	admission       syncmgr.SyncTaskAdmission
	payloadReleased chan struct{}
	done            chan struct{}
	terminalErr     error
	submitted       bool
	failed          bool
	// payload/snapshotID tie this entry to the snapshot it carries, so
	// settlement reaches exactly its own floor: success → CommitFlush (the only
	// settlement), terminal failure → AbortFlush (floor stays pinned for WAL
	// replay). payload is nil for a task built without a segment buffer.
	payload    segmentPayload
	snapshotID int64
	// everFailed stays set across re-drives (failed flips back on submit).
	// It marks the segment as flush-impaired: backpressure then waits on task
	// COMPLETION, not just payload release — a Commit stuck in retry holds no
	// payload, and without this its buffered tail grows with no bound at all.
	everFailed bool
}

type writeBufferSyncQueue struct {
	entries []*writeBufferSyncEntry
	// intent is this segment's flush debt — a new task deferred behind an owner
	// or reorder window, or a failed attempt waiting out its interval. One per
	// queue, not per entry, for BOTH payload modes: an owned retry replays the
	// segment from its oldest task, so the queue advances as a unit; a ref
	// (growing) retry rebuilds a fresh task, so its debt is carried by a queue
	// with no entries — the queue outlives its entries while intent.owes.
	intent flushIntent
}

func (wb *writeBufferBase) getOrCreateSyncQueueLocked(segmentID int64) *writeBufferSyncQueue {
	queue := wb.writeBufferSyncQueues[segmentID]
	if queue == nil {
		queue = &writeBufferSyncQueue{}
		wb.writeBufferSyncQueues[segmentID] = queue
	}
	return queue
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
		// Every entry must be PARKED (failed). A non-failed entry is either in
		// flight (submitted) or freshly registered and still owned by the
		// builder that has not called submit yet — the build happens under
		// wb.mut but the submission happens after it is released, so a retry
		// drive landing in that gap would submit the fresh entry a second time
		// (see the double-submission hazard above). "Not submitted" is NOT the
		// same as "nobody owns the submission".
		if !entry.failed {
			return false
		}
	}
	return true
}

// driveRetries re-submits segments whose last flush attempt failed. Retries
// are dual-driven: the flowgraph calls this on every timetick (the fast path),
// and the bufferManager retry ticker calls it via driveSyncRetries as the
// backstop for whenever the flowgraph goroutine is parked or idle. The one
// wait that still drives retries itself is waitSyncsSettled — a dropping
// buffer has already left the manager map, out of the ticker's reach.
//
// ONE drive over ONE structure — the per-segment sync queues — for both
// payload modes. A queue with entries is the owned shape: retry re-submits the
// SAME parked task objects, restarting from the OLDEST pending one (the tasks
// behind it were built against state its commit was supposed to publish, so
// they can only be replayed in the same order). A queue whose entries are gone
// but whose intent still owes is the ref (growing) shape: a failed attempt
// cost nothing, so retry builds a FRESH task with a fresh source lease.
func (wb *writeBufferBase) driveRetries(ctx context.Context) {
	interval := wb.retryInterval()
	if interval < 0 {
		return
	}
	now := time.Now()

	wb.mut.Lock()
	if wb.closed || wb.dropping {
		wb.mut.Unlock()
		return
	}
	due := wb.dueWriteBufferRetriesLocked(now, interval)
	rebuild := wb.dueRefRebuildsLocked(now, interval)
	wb.mut.Unlock()

	for _, entry := range due {
		wb.resubmitWriteBufferSync(ctx, entry)
	}
	if len(rebuild) > 0 {
		wb.logger.Info(ctx, "retry growing-source sync", mlog.Int64s("segmentIDs", rebuild))
		wb.syncSegments(wb.syncCtx, rebuild)
	}
}

// dueRefRebuildsLocked returns the segments whose entry-less queue debt is due
// and whose ledger can produce a task now. Ineligible segments keep their
// existing debt; source-unavailable failures are re-armed by task
// construction. The attempted stamp is set here so an unavailable source is
// re-armed from this round rather than from the first failure forever.
func (wb *writeBufferBase) dueRefRebuildsLocked(now time.Time, interval time.Duration) (due []int64) {
	if wb.checkpoint == nil {
		// Nothing was ever buffered; a ref rebuild has no fence to run to.
		return nil
	}
	for segmentID, queue := range wb.writeBufferSyncQueues {
		if len(queue.entries) > 0 || !queue.intent.due(now, interval) {
			continue
		}
		payload, ok := wb.refPayloadLocked(segmentID)
		if !ok {
			// An owned queue never parks debt without entries (its debt dies
			// with its terminal task); an entry-less queue whose ref buffer is
			// gone has nothing left to rebuild from. Self-heal either way.
			delete(wb.writeBufferSyncQueues, segmentID)
			continue
		}
		if wb.refPayloadSyncableLocked(segmentID, payload) {
			queue.intent.attempted(now)
			due = append(due, segmentID)
		}
	}
	return due
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
			// Claim submission ownership before dropping wb.mut. Close must see
			// this as in-flight and wait for the pre-accept rejection callback;
			// otherwise the entry is neither parked nor accepted in the handoff gap.
			entry.submitted = true
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
	queue := wb.getOrCreateSyncQueueLocked(entry.task.SegmentID())
	queue.entries = append(queue.entries, entry)
}

func (wb *writeBufferBase) writeBufferSyncEntryLocked(task syncmgr.Task) *writeBufferSyncEntry {
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
	// The queue outlives its entries while it still carries flush debt: a ref
	// (growing) retry has no parked task to keep the queue alive, so the debt
	// itself does.
	if len(queue.entries) == 0 && !queue.intent.owes {
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
	if segment, exists := wb.metaCache.GetSegmentByID(segmentID); exists &&
		terminalSegmentState(segment.State()) {
		return true
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
		// Abandoning a parked task is terminal: AbortFlush keeps its snapshot's
		// floor pinned (WAL replay is the only copy left). Done here, under
		// wb.mut, because abandonParkedWriteBufferSyncs runs without the lock.
		if entry.payload != nil {
			entry.payload.AbortFlush(entry.snapshotID)
		}
		wb.removeWriteBufferSyncLocked(entry)
	}
	return parked
}

// abandonParkedWriteBufferSyncs settles entries that were parked for a retry
// nothing will ever drive. Runs WITHOUT wb.mut held: Abandon releases the
// payload, whose accounting callback takes locks of its own.
func (wb *writeBufferBase) abandonParkedWriteBufferSyncs(parked []*writeBufferSyncEntry, err error) {
	for _, entry := range parked {
		// Only owned entries can be parked (growing entries leave the queue on
		// any completion), so the assertion documents rather than guards.
		if task, ok := entry.task.(*syncmgr.SyncTask); ok {
			wb.releaseTerminalSync(task)
		}
		entry.releaseAdmission()
		entry.complete(err)
	}
}

func (entry *writeBufferSyncEntry) releaseAdmission() {
	if entry.admission != nil {
		entry.admission.Release()
		entry.admission = nil
	}
}

func (entry *writeBufferSyncEntry) complete(err error) {
	entry.terminalErr = err
	close(entry.done)
}

// finishWriteBufferSync owns task completion for BOTH payload modes — the two
// finishXxx twins died here. One skeleton: entry identity, ClassifySyncError,
// payload settlement (success → CommitFlush, the only settlement; failure →
// AbortFlush for anything that will not be resubmitted), admission handling,
// resync drive, observer/fatal callbacks, entry completion.
//
// The modes differ in what a failed attempt costs, and that difference decides
// the queue behavior:
//
//   - owned: the payload was yielded out of the buffer, so a retryable failure
//     keeps the SAME task parked in the queue for driveRetries to re-submit;
//     only a terminal failure (or Close) ends the stream, leaving the
//     checkpoint pinned for WAL replay.
//   - ref (growing): the rows stay pinned in segcore, so a failed attempt costs
//     a round trip and nothing else. The entry leaves the queue on ANY
//     completion; a retry builds a FRESH task with a fresh source lease
//     (armRefRetryLocked arms the debt, driveRetries rebuilds).
func (wb *writeBufferBase) finishWriteBufferSync(ctx context.Context, entry *writeBufferSyncEntry, task syncmgr.Task, taskErr error) error {
	segmentID := task.SegmentID()
	growingTask, isGrowing := task.(*syncmgr.GrowingSourceSyncTask)
	var resyncSegmentID int64
	var fatalErr error
	var admission syncmgr.SyncTaskAdmission
	completionErr := taskErr
	removeFlushedSegment := false

	decision := syncmgr.SyncRetry
	if taskErr != nil {
		decision = syncmgr.ClassifySyncError(ctx, taskErr)
		if isGrowing {
			// Commit releases the source inline on its success paths. A task
			// that fails in Prepare, is rejected by the dispatcher, or is
			// canceled while queued never reaches it — release idempotently
			// here for those.
			growingTask.ReleaseSource()
		}
	}

	wb.mut.Lock()
	if wb.writeBufferSyncEntryLocked(task) != entry {
		wb.mut.Unlock()
		return taskErr
	}

	if isGrowing {
		segment, segmentExists := wb.metaCache.GetSegmentByID(segmentID)
		segmentDropped := segmentExists && segment.State() == commonpb.SegmentState_Dropped
		// Drop is a monotonic debt independent of the flags frozen on the task:
		// a final-flush replay built before Drop must not remove the segment
		// before a real drop task is committed, so Dropped wins over IsFlush.
		removeFlushedSegment = taskErr == nil && task.IsFlush() && !segmentDropped
		// The ledger is the entry's own payload; "live" means it is still the
		// buffer's payload, so streak/debt bookkeeping should still happen. A
		// callback landing after the buffer left the map settles ONLY its own
		// snapshot record, so the source lease and in-flight state cannot leak.
		payload, _ := entry.payload.(*refPayload)
		live := false
		if buffer, ok := wb.buffers[segmentID]; ok && payload != nil && buffer.payload == entry.payload {
			live = true
		}
		if taskErr != nil {
			// Task-derived settlement runs FIRST and unconditionally — giving
			// back what this task reserved must not depend on the buffer map,
			// which a concurrent teardown clears while tasks are in flight.
			wb.settleFailedGrowingTaskLocked(growingTask)
			// AbortFlush, NOT CommitFlush: the ledger keeps its batches (the
			// checkpoint keeps pinning) and a source lease the task never took
			// is released. Failure counters below are coordinator bookkeeping,
			// not settlement.
			if entry.payload != nil {
				entry.payload.AbortFlush(entry.snapshotID)
			}
			if decision != syncmgr.SyncTerminal {
				// Settlement signaling only: a retryable/canceled growing
				// failure must not abort a Close/Drop drain — the rows are
				// still in segcore and the retry machinery re-flushes them.
				completionErr = nil
			}
			if live {
				if growingTask.HasCommittedFlush() && growingTask.CommittedManifestPath() != "" {
					// One assignment of the whole frozen attempt, so the stored
					// replay record cannot drift from what the task committed.
					record := growingTask.CommittedFlushRecord()
					payload.pendingCommitted = &record
				}
				switch decision {
				case syncmgr.SyncCanceled:
					// Cancellation is lifecycle, not a failed storage attempt.
					// AbortFlush already cleared the in-flight state; keep any
					// previous streak intact and do not arm a retry that
					// shutdown will never drive.
				case syncmgr.SyncTerminal:
					payload.recordFailure(taskErr)
					wb.observeGrowingSourceSyncFailureLocked(segmentID, payload)
					// markNonRetryableFailure permanently parks this segment:
					// refPayloadSyncableLocked refuses it forever, so its
					// batches are never trimmed and the channel checkpoint stays
					// pinned. Left silent that is an unbounded, alert-less stall
					// — strictly worse than a crash, because nothing ever
					// reports it. Fail loudly instead: the rows are still
					// recoverable from the WAL, and a human has to look at this.
					payload.markNonRetryableFailure()
					mlog.Error(ctx, "growing-source sync hit a terminal failure, escalating",
						mlog.Int64("segmentID", segmentID),
						mlog.Uint64("lastFlushedTs", payload.lastFlushedTs),
						mlog.String("lastFailure", payload.lastFailure))
					fatalErr = errors.Wrapf(taskErr, "growing-source sync unrecoverable, segmentID=%d lastFlushedTs=%d",
						segmentID, payload.lastFlushedTs)
				case syncmgr.SyncRetry:
					payload.recordFailure(taskErr)
					wb.observeGrowingSourceSyncFailureLocked(segmentID, payload)
					wb.armRefRetryLocked(segmentID)
				}
			}
		} else {
			// Success. The terminal debts are READ off the metacache, not off
			// stored bits: a Sealed/Flushing segment still owes its flush, a
			// Dropped one its drop, until the task that carries that flag
			// commits.
			if payload != nil && growingTask.SourceFinalized() {
				payload.sourceFinalized = true
				if growingTask.FlushThroughTs() > payload.sourceFinalizedThroughTs {
					payload.sourceFinalizedThroughTs = growingTask.FlushThroughTs()
				}
			}
			// Success is the ONLY settlement: the ledger advances to the frozen
			// fence and trims the acked batches.
			if entry.payload != nil {
				entry.payload.CommitFlush(entry.snapshotID)
			}
			if live {
				wb.resetGrowingSourceSyncFailureMetricLocked(payload)
				switch {
				case payload.owesDropAfterLocked(task):
					// A frozen replay only settles the range its manifest
					// covers. Drop remains owed and is built from the live tail
					// after this callback.
					if !wb.closed && !wb.dropping {
						resyncSegmentID = segmentID
					}
				case payload.owesFlushAfterLocked(task) && len(payload.batches) == 0:
					// Metadata-only final flush still owed. No claim here: the
					// resync goes through getSyncTask, which claims Sealed
					// itself.
					resyncSegmentID = segmentID
				case len(payload.batches) == 0:
					if task.IsFlush() || task.IsDrop() || !segmentExists ||
						segment.State() == commonpb.SegmentState_Flushed {
						delete(wb.buffers, segmentID)
					}
				}
			}
		}
		wb.removeWriteBufferSyncLocked(entry)
		admission = entry.admission
		entry.admission = nil
		if removeFlushedSegment {
			// A final flush must have covered the WHOLE ledger before the
			// segment may leave: batches above the flushed fence would vanish
			// with the buffer — silent data loss. Today this cannot happen
			// (batches are recorded and wb.checkpoint advanced under one
			// wb.mut hold, so every batch end <= checkpoint at Snapshot time),
			// but the invariant lives in BufferData, a different file; keep the
			// debt instead of the loss if that ever changes.
			if payload, ok := entry.payload.(*refPayload); ok && len(payload.batches) > 0 {
				mlog.Error(ctx, "final flush left ledger batches above its fence; keeping segment debt instead of dropping rows",
					mlog.FieldSegmentID(segmentID),
					mlog.Int("remainingBatches", len(payload.batches)))
				resyncSegmentID = segmentID
			} else {
				// Keep the decision and removal in the same write-buffer critical
				// section as DropPartitions. Otherwise Drop can mark the segment
				// after the check above but before RemoveSegments and lose its
				// terminal debt.
				wb.metaCache.RemoveSegments(metacache.WithSegmentIDs(segmentID))
				delete(wb.buffers, segmentID)
			}
		}
	} else {
		ownedTask := task.(*syncmgr.SyncTask)
		if taskErr != nil {
			// Retryable failures stay queued during Drop too: Drop is a synchronous
			// wait, and waitSyncsSettled drives these retries itself (timeticks
			// stop once DropChannel is delivered). Only Close makes retrying
			// pointless — nothing will ever drive it again.
			if decision == syncmgr.SyncRetry && !wb.closed {
				// Keep everything: the payload, the metacache syncing counters and
				// the queue position. driveRetries submits this exact task object
				// again, and its Prepare will not rewrite what it already wrote.
				// SyncManager's HandleError hook only reports this failed attempt; it
				// deliberately does not release the prepared storage handle that this
				// retry is going to reuse.
				entry.failed = true
				entry.everFailed = true
				entry.submitted = false
				// Rate-limit from this failure, not from the next timetick: the
				// failed attempt was an attempt, so it restarts the clock.
				if queue := wb.writeBufferSyncQueues[segmentID]; queue != nil {
					queue.intent.want()
					queue.intent.attempted(time.Now())
				}
				wb.mut.Unlock()
				wb.logger.RatedWarn(ctx, rate.Limit(1), "flush attempt failed, will retry",
					mlog.Int64("segmentID", segmentID), mlog.Err(taskErr))
				return taskErr
			}
			// Terminal: this task will never run again.
			wb.releaseTerminalSync(ownedTask)
			// AbortFlush, NOT CommitFlush: the snapshot is unsettled and its floor
			// stays, so the channel checkpoint keeps pinning the replay origin of
			// rows whose only remaining copy is the WAL.
			if entry.payload != nil {
				entry.payload.AbortFlush(entry.snapshotID)
			}
			wb.removeWriteBufferSyncLocked(entry)
			admission = entry.admission
			entry.admission = nil
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
			// Success is the ONLY settlement of the snapshot this entry carried:
			// its floor is dropped and the checkpoint may advance past it.
			if entry.payload != nil {
				entry.payload.CommitFlush(entry.snapshotID)
			}
			queue := wb.writeBufferSyncQueues[segmentID]
			resync := queue != nil && queue.intent.due(time.Now(), wb.retryInterval()) &&
				wb.buffers[segmentID] != nil && !wb.buffers[segmentID].IsEmpty()
			if queue != nil {
				queue.intent.clear()
			}
			if segment, exists := wb.metaCache.GetSegmentByID(segmentID); exists {
				if pendingFlushState(segment.State()) {
					// The segment still owes a flush. getSyncTask claims Sealed on
					// the way in, so the resync task carries WithFlush and this
					// terminates after one round — before the claim moved there,
					// a Sealed segment could not produce a flush task at all and
					// this re-triggered forever on empty buffers.
					resync = resync || !task.IsFlush()
				} else if segment.State() == commonpb.SegmentState_Dropped {
					resync = resync || !task.IsDrop()
				}
			}
			if task.IsFlush() {
				wb.metaCache.RemoveSegments(metacache.WithSegmentIDs(segmentID))
			}
			// The segment's final task committed: its persistent buffer (kept in
			// the map so in-flight floors stayed reachable) may leave now.
			if task.IsFlush() || task.IsDrop() {
				delete(wb.buffers, segmentID)
			}
			wb.removeWriteBufferSyncLocked(entry)
			admission = entry.admission
			entry.admission = nil
			if resync && !wb.closed && !wb.dropping {
				resyncSegmentID = segmentID
			}
		}
	}
	wb.mut.Unlock()
	if admission != nil {
		admission.Release()
	}

	if resyncSegmentID != 0 {
		// Do not reserve or build the follow-up inside this completion callback.
		// The dispatcher keeps the old key position until the callback returns;
		// a detached drive lets that position advance even if admission blocks.
		go wb.syncSegments(wb.syncCtx, []int64{resyncSegmentID})
	}

	if isGrowing && taskErr == nil && removeFlushedSegment {
		mlog.Info(ctx, "flushed segment removed", mlog.FieldSegmentID(segmentID), mlog.String("channel", task.ChannelName()))
	}

	defer entry.complete(completionErr)
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
	wb.submitSyncTaskSubmissions(ctx, []syncTaskSubmission{{
		task:      entry.task,
		admission: entry.admission,
	}})
}

// getWriteBufferSyncTask builds the task for the owned-payload path from the
// snapshot getSyncTask already took: the buffered payload was yielded and its
// ownership moves to the task. Counterpart of getGrowingSourceSyncTask, which
// builds for the path whose rows stay pinned in segcore.
//
// Caller must hold wb.mut and must have gone through getSyncTask, which took
// the snapshot (input, nil for a segment without a buffer) and claims the
// flush.
func (wb *writeBufferBase) getWriteBufferSyncTask(ctx context.Context, segmentInfo *metacache.SegmentInfo, buffer *segmentBuffer, input *flushInput) (syncmgr.Task, error) {
	segmentID := segmentInfo.SegmentID()
	var batchSize int64
	var insertMemSize int64
	var deleteMemSize int64
	var tsFrom, tsTo uint64

	if segmentInfo.Level() == datapb.SegmentLevel_L0 {
		// Retire the mapping BEFORE taking the buffer: from here on this segment
		// is being flushed and must not receive another delete.
		wb.rotateL0SegmentLocked(segmentID)
	}

	// The buffer stays in wb.buffers — the payload's floor (appended by
	// Snapshot) is what keeps the channel checkpoint pinned while this task is
	// in flight, and GetCheckpoint reads it through the buffers map.
	var payload segmentPayload
	var snapshotID int64
	var insert []*storage.InsertData
	var bm25 map[int64]*storage.BM25Stats
	var delta *storage.DeleteData
	var schema *schemapb.CollectionSchema
	var startPos *msgpb.MsgPosition
	if buffer != nil && input != nil {
		payload = buffer.payload
		insertMemSize = input.bytes
		deleteMemSize = buffer.deltaBuffer.size
		snapshotID = input.snapshotID
		insert = input.insert
		bm25 = input.bm25
		schema = input.schema
		deltaData, deltaTimeRange, deltaStart := buffer.yieldDelta()
		delta = deltaData
		timeRange := &TimeRange{timestampMin: math.MaxUint64, timestampMax: 0}
		if input.timeRange != nil {
			timeRange.Merge(input.timeRange)
		}
		if deltaTimeRange != nil {
			timeRange.Merge(deltaTimeRange)
		}
		tsFrom, tsTo = timeRange.timestampMin, timeRange.timestampMax
		startPos = getEarliestCheckpoint(input.startPos, deltaStart)
		// The floor Snapshot recorded covers the insert side only; widen it to
		// the combined task start so the delta rows this task carries stay
		// pinned too (delta-only L0 tasks get their floor created here).
		if owned, ok := payload.(*ownedPayload); ok {
			owned.widenFloor(snapshotID, deltaStart)
		}
	}

	// Checkpoint pinning is fully derived — no per-entry registration:
	//
	//   - A task with payload pins its COMBINED start position via the payload's
	//     floor list (Snapshot + widenFloor above), released only by CommitFlush.
	//   - A metadata-only FLUSH task is covered by the seal fence pin the seal
	//     installed (metacache pendingFlushCheckpoint, see sealActionLocked),
	//     which is both earlier and longer lived than any entry-level pin.
	//   - A metadata-only DROP task (Dropped segment via DropPartitions /
	//     GetDroppedSegmentPolicy, or the final per-segment task on the
	//     drop-channel path) pins NOTHING, deliberately. Drop authority is
	//     coordinator meta, which converges with zero DataNode drop-ack even if
	//     this task never lands (crash after the checkpoint passed the drop
	//     message): a DropPartition broadcast is FastAck'd and RootCoord's
	//     ack-callback retries NotifyDropPartition forever until DataCoord's
	//     meta.DropSegmentsOfPartition (internal/datacoord/meta.go
	//     DropSegmentsOfPartition) marks every segment of the partition Dropped;
	//     a channel drop is driven both by RootCoord's DropVirtualChannel
	//     ack-callback and by the streamingnode recovery storage's
	//     dropAllVirtualChannel (internal/streamingnode/server/wal/recovery/
	//     recovery_background_task.go), landing in DataCoord's
	//     meta.UpdateDropChannelSegmentInfo which drops every segment of the
	//     channel regardless of the request payload. The residual cost of a
	//     crashed drop task is minutes of Dropped-state latency, not
	//     correctness.

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
		payload:         payload,
		snapshotID:      snapshotID,
	})
	return task, nil
}

// getGrowingSourceSyncTask builds the task for the ref-payload path from the
// snapshot getSyncTask already took: the fence, source lease, replay manifest
// and row tally are all frozen in input.growing, so this only assembles the
// task and registers its queue entry — same entry lifecycle as the owned path.
//
// Caller must hold wb.mut and must have gone through getSyncTask.
func (wb *writeBufferBase) getGrowingSourceSyncTask(ctx context.Context, segmentInfo *metacache.SegmentInfo, buffer *segmentBuffer, input *flushInput) (syncmgr.Task, error) {
	growing := input.growing
	payload := buffer.payload.(*refPayload)
	startPos := growing.startPos
	schemaTimestamp := uint64(0)
	if startPos != nil {
		schemaTimestamp = startPos.GetTimestamp()
	}
	pendingCommitted := growing.pendingCommitted

	task := syncmgr.NewGrowingSourceSyncTask().
		WithCollectionID(wb.collectionID).
		WithPartitionID(segmentInfo.PartitionID()).
		WithSegmentID(segmentInfo.SegmentID()).
		WithChannelName(wb.channelName).
		WithStartPosition(startPos).
		WithCheckpoint(growing.checkpoint).
		WithBatchRows(input.rows).
		WithFlushFromTs(growing.flushFromTs).
		WithLevel(segmentInfo.Level()).
		WithMetaCache(wb.metaCache).
		WithMetaWriter(wb.metaWriter).
		WithSchema(wb.metaCache.GetSchema(schemaTimestamp)).
		WithAllocator(wb.allocator).
		WithStorageConfig(packed.CreateStorageConfig()).
		// Non-fatal on purpose: the rows stay pinned in the growing segment
		// until CommitGrowingFlush, so a failed attempt costs nothing but a
		// round trip. A retryable failure only arms the segment's intent
		// (armRefRetryLocked); driveRetries builds the NEXT
		// attempt. Escalation to the fatal handler happens only where recovery
		// is impossible — see the SyncTerminal branch in finishWriteBufferSync.
		WithFailureCallback(wb.growingSourceErrHandler)
	// Ownership of the source lease moves from the snapshot record to the
	// task: from here the task releases it (Commit success or ReleaseSource).
	if source := payload.takeSource(input.snapshotID); source != nil {
		task.WithSource(source)
	}
	if pendingCommitted != nil {
		task.WithCommittedFlush(pendingCommitted.ManifestPath, storage.CloneBM25StatsMap(pendingCommitted.BM25Stats), pendingCommitted.InsertBinlogs)
		task.WithCommittedPKStats(pendingCommitted.PKStats)
		// The finalization flags come from the frozen attempt when replaying a
		// committed manifest: the manifest covers exactly the frozen range, so
		// only the attempt that wrote it knows whether it was final. Deriving
		// them from the CURRENT state would upgrade a periodic sync to the
		// final flush after a concurrent seal — publishing a manifest that does
		// not cover the sealed tail as the segment's last word.
		if pendingCommitted.IsFlush {
			task.WithFlush()
		}
		if pendingCommitted.IsDrop {
			task.WithDrop()
		}
	} else {
		if segmentInfo.State() == commonpb.SegmentState_Flushing {
			task.WithFlush()
		}
		// The metacache Dropped state IS the drop debt (the old owesDrop bit).
		if segmentInfo.State() == commonpb.SegmentState_Dropped {
			task.WithDrop()
		}
	}

	wb.registerWriteBufferSyncLocked(&writeBufferSyncEntry{
		task:       task,
		done:       make(chan struct{}),
		payload:    buffer.payload,
		snapshotID: input.snapshotID,
	})
	return task, nil
}
