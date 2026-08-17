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

	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// What BOTH payload modes use, so neither has to squat in the other's file: the
// flush debt a segment carries, the retry cadence, the submission dispatch, the
// release every terminal path performs, and the one wait that covers them
// together.
//
// The mode-specific pieces are payload_owned.go (payload yielded out of the
// write buffer, which the task then owns) and payload_ref.go (rows still pinned
// in a segcore growing segment, which the task only reads); the shared queue
// and completion machinery is write_buffer_sync_coordinator.go.
// write_buffer.go keeps the buffer itself and the channel lifecycle.

// flushIntent is one segment's outstanding flush debt: it owes another attempt,
// and the earliest that attempt may be made.
//
// Both flush paths embed one. Two triggers read the same debt: a task completing
// (whatever blocked the segment cleared) and a timetick (enough time passed).
// Both go through due(), so a completion cannot jump a retry interval that a
// failure has imposed.
type flushIntent struct {
	owes bool
	// since is when the debt started being rate-limited; zero means it is not.
	// The interval itself is applied at DRIVE time, not stored here, so a
	// changed flushRetryInterval takes effect on debts already outstanding.
	since time.Time
}

// want records the debt without ADDING a delay: it does not stamp `since`, so a
// fresh debt is due immediately and one already rate-limited by an earlier
// attempted() stays rate-limited unchanged. Used when an existing per-segment
// owner/window defers a new task — completion, not time, is what may unblock it.
// A failure path pairs it with attempted(now): the failed attempt IS an attempt,
// so the rate limit starts from it.
func (i *flushIntent) want() {
	i.owes = true
}

// attempted records that an attempt was just made for this debt. The debt STAYS
// — only a completed task settles it — but the rate limit starts over.
//
// Every re-drive must call this. Without it `since` keeps the timestamp of the
// FIRST failure and due() is true on every timetick from then on: a retry storm
// instead of one attempt per interval. Restamping unconditionally means
// cascading failure callbacks inside one round each move `since` by the
// milliseconds between them — at most one extra interval on one retry, which is
// an accepted cost for having only two verbs.
func (i *flushIntent) attempted(now time.Time) {
	i.since = now
}

func (i *flushIntent) due(now time.Time, interval time.Duration) bool {
	if !i.owes {
		return false
	}
	return i.since.IsZero() || now.Sub(i.since) >= interval
}

func (i *flushIntent) clear() {
	i.owes = false
	i.since = time.Time{}
}

// releaseTerminalSync gives up everything a task will never use again: the
// metacache syncing counters it reserved, and the payload plus any prepared
// storage handle it still holds.
//
// The single definition of "done for good": both the failure branch of
// finishWriteBufferSync and the parked sweep go through it, so a resource the
// task grows later is released in one place.
//
// It deliberately does NOT complete the entry or touch the queue — those differ
// by caller. The failure branch completes on its way out, after its observer
// callbacks; the parked sweep was already dequeued under the lock.
func (wb *writeBufferBase) releaseTerminalSync(task *syncmgr.SyncTask) {
	wb.metaCache.UpdateSegments(
		metacache.DiscardSyncing(task.BatchRows()),
		metacache.WithSegmentIDs(task.SegmentID()),
	)
	task.Abandon()
}

// waitSyncsSettled waits for this channel's in-flight flushes to settle — the
// given write-buffer entries AND every growing-source sync — and returns the
// first error any of them reported.
//
// One function for both because every caller wants both, and because the bound
// must be shared: two waits applied in sequence would each start their own
// growingFlushCancelGrace, making the real upper bound twice the stated one.
//
// After cancellation the wait is BOUNDED. Canceling unwinds whatever is
// cancellable, but an already-started native write takes no cancellation token,
// so waiting past the grace only turns the caller's timeout into a hang — and a
// DropChannel that never returns is strictly worse than one that reports failure
// and lets WAL replay redo the work. Giving up the wait is not giving up the
// work: the task keeps its segment pin and finishes in the background.
func (wb *writeBufferBase) waitSyncsSettled(ctx context.Context, cancel context.CancelFunc, waiters []*writeBufferSyncEntry) error {
	results := make(chan error, len(waiters))
	for _, entry := range waiters {
		go func(entry *writeBufferSyncEntry) {
			<-entry.done
			results <- entry.terminalErr
		}(entry)
	}

	var firstErr error
	ctxDone := ctx.Done()
	// A ceiling that exists from the START, not one that only appears once
	// something goes wrong.
	//
	// What is being waited on is an object-storage write plus a SaveBinlogPaths
	// RPC, and the meta writer's retry loop around that RPC is driven by
	// wb.syncCtx — which only syncCancel below cuts short. syncCancel lives in
	// abort(), and abort() only runs on ctx-done or a task error. So a caller
	// with a non-cancellable ctx, against a coordinator that is unresponsive
	// rather than failing, would wait forever: the retry never errors precisely
	// because nothing canceled it.
	deadline := time.NewTimer(paramtable.Get().DataNodeCfg.GracefulStopTimeout.GetAsDuration(time.Second))
	defer deadline.Stop()
	var grace <-chan time.Time
	var graceTimer *time.Timer
	defer func() {
		if graceTimer != nil {
			graceTimer.Stop()
		}
	}()
	abort := func(err error) {
		firstErr = err
		if cancel != nil {
			cancel()
		}
		wb.syncCancel()
		ctxDone = nil
		graceTimer = time.NewTimer(growingFlushCancelGrace)
		grace = graceTimer.C
	}
	// This wait runs during Drop, after the flowgraph stopped delivering
	// timeticks, so driveRetries can no longer re-drive a failed attempt. The
	// bufferManager retry ticker cannot reach this buffer either:
	// DropChannel/RemoveChannel do GetAndRemove BEFORE Close, so a dropping
	// buffer has already left the manager map — this arm is the only driver
	// left. Retryable failures keep their entry pending (finishWriteBufferSync
	// leaves them queued while dropping) and this loop re-submits them on the
	// same flushRetryInterval cadence — until the caller's context cancels,
	// which turns every further attempt terminal.
	retryEvery, retryTick, stopRetry := wb.retryTicker()
	defer stopRetry()
	completed := 0
	for {
		// Both conditions, one loop: entries all reported, and no growing-source
		// flush still in flight.
		wb.mut.RLock()
		growingInFlight := wb.anyGrowingSyncingLocked()
		growingSettled := wb.growingSettled
		wb.mut.RUnlock()
		if completed >= len(waiters) && !growingInFlight {
			if firstErr != nil {
				return firstErr
			}
			return ctx.Err()
		}

		select {
		case err := <-results:
			completed++
			if err != nil && firstErr == nil {
				abort(err)
			}
		case <-growingSettled:
			// A growing-source sync left the in-flight set; re-evaluate.
		case <-ctxDone:
			if firstErr == nil {
				abort(ctx.Err())
			}
		case <-deadline.C:
			// Same outcome as the grace expiry below, but reachable without any
			// cancellation having happened first.
			wb.logger.Warn(wb.syncCtx, "flush drain exceeded its hard bound; "+
				"canceling and leaving in-flight tasks to finish in the background")
			wb.syncCancel()
			if firstErr != nil {
				return firstErr
			}
			// Never ctx.Err() here: with a non-cancellable caller context it is
			// nil, and a nil return says "everything settled" about a drain that
			// just gave up — the caller would report a successful drop for data
			// still in flight.
			return merr.WrapErrServiceInternalMsg(
				"flush drain exceeded its hard bound (%s) with %d/%d write-buffer tasks settled",
				paramtable.Get().DataNodeCfg.GracefulStopTimeout.GetAsDuration(time.Second), completed, len(waiters))
		case <-grace:
			wb.logger.Warn(wb.syncCtx, "giving up the wait for canceled sync tasks; "+
				"they cannot be preempted and are left to finish in the background",
				mlog.Err(firstErr))
			if firstErr != nil {
				return firstErr
			}
			return ctx.Err()
		case now := <-retryTick:
			if firstErr != nil {
				continue
			}
			// Write-buffer queues only: growing-source progress during Drop is
			// re-driven by syncDropSegment's own errGrowingSourceUnavailable
			// loop, which is the path that knows how to wait for a source.
			wb.driveDueWriteBufferRetries(ctx, now, retryEvery)
		}
	}
}

// ---- shared submission and retry cadence ----

// retryInterval is the one cadence for every flush retry on this channel:
// write-buffer queues and growing-source progress alike. The option field is a
// test-only override; a negative value disables retry entirely.
func (wb *writeBufferBase) retryInterval() time.Duration {
	if wb.flushRetryInterval != 0 {
		return wb.flushRetryInterval
	}
	return paramtable.Get().DataNodeCfg.FlushRetryInterval.GetAsDuration(time.Millisecond)
}

// driveSyncRetries is the bufferManager retry ticker's per-buffer entry point:
// it drives this buffer's due flush retries (write-buffer queues and
// growing-source progress alike) under the buffer's own locking, bound to
// wb.syncCtx — the buffer lifetime — never to any caller's wait. Closed or
// dropping buffers are guarded inside driveRetries.
func (wb *writeBufferBase) driveSyncRetries() {
	wb.driveRetries(wb.syncCtx)
}

// retryPeriodFloor keeps a zero/tiny configured retry interval from turning a
// retry sweep ticker into a spin.
const retryPeriodFloor = 100 * time.Millisecond

// clampRetryPeriod bounds a retry sweep period to [retryPeriodFloor, ceiling];
// a non-positive ceiling means uncapped. The per-segment retry interval itself
// is applied inside the drive functions — the clamped period only paces how
// often a sweep looks.
func clampRetryPeriod(period, ceiling time.Duration) time.Duration {
	if period < retryPeriodFloor {
		period = retryPeriodFloor
	}
	if ceiling > 0 && period > ceiling {
		period = ceiling
	}
	return period
}

// retryTicker builds the ticker the drop wait uses to re-drive failed flushes
// while it holds the goroutine that would otherwise do the driving. Returns a
// nil channel when retry is disabled, so the caller's select arm simply never
// fires.
func (wb *writeBufferBase) retryTicker() (interval time.Duration, tick <-chan time.Time, stop func()) {
	interval = wb.retryInterval()
	if interval < 0 {
		return interval, nil, func() {}
	}
	ticker := time.NewTicker(clampRetryPeriod(interval, 0))
	return interval, ticker.C, ticker.Stop
}

type syncTaskSubmission struct {
	task      syncmgr.Task
	admission syncmgr.SyncTaskAdmission
}

// reserveSyncTask acquires one node-wide payload slot, bounded by the
// graceful-stop timeout. timedOut=true (with nil error) tells the caller to
// keep its payload buffered for a later policy round.
//
// A plain blocking call on purpose. This often runs on the flowgraph
// goroutine, but retries parked behind the slot it waits for are re-driven by
// the bufferManager retry ticker, so waiting passively cannot self-deadlock.
// And a timeout cannot leak the slot: the sync manager's acquire releases the
// semaphore itself when a successful acquire races ctx cancellation
// (reorderDispatcher.acquireAdmission), so an error return never holds a slot.
func (wb *writeBufferBase) reserveSyncTask(ctx context.Context) (syncmgr.SyncTaskAdmission, bool, error) {
	reservable, ok := wb.syncMgr.(syncmgr.SyncTaskAdmissionReservable)
	if !ok {
		return nil, false, nil
	}

	boundCtx, cancel := context.WithTimeout(ctx,
		paramtable.Get().DataNodeCfg.GracefulStopTimeout.GetAsDuration(time.Second))
	defer cancel()
	admission, err := reservable.ReserveSyncTask(boundCtx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
		if boundCtx.Err() != nil {
			return nil, true, nil
		}
		return nil, false, err
	}
	return admission, false, nil
}

func completedSyncFuture(err error) *conc.Future[struct{}] {
	return conc.Go(func() (struct{}, error) {
		return struct{}{}, err
	})
}

func (wb *writeBufferBase) submitSyncTaskSubmissions(ctx context.Context, submissions []syncTaskSubmission) []*conc.Future[struct{}] {
	result := make([]*conc.Future[struct{}], 0, len(submissions))
	for _, submission := range submissions {
		syncTask := submission.task
		wb.mut.Lock()
		writeBufferEntry := wb.writeBufferSyncEntryLocked(syncTask)
		if writeBufferEntry != nil {
			// Marked BEFORE the task is handed over: a fast failure can run
			// the completion callback before SyncData returns, and that
			// callback's failed=true/submitted=false must not be overwritten
			// afterwards — it is what keeps the submission gate closed and
			// stops the next retry round from double-submitting the suffix.
			writeBufferEntry.submitted = true
		}
		wb.mut.Unlock()
		if writeBufferEntry == nil {
			if submission.admission != nil {
				submission.admission.Release()
			}
			continue
		}

		// One dispatch, used by both the completion callback and the
		// submit-error path below, so the two can never disagree about how a
		// task is settled — finishWriteBufferSync branches on the payload mode
		// itself.
		finish := func(err error) error {
			return wb.finishWriteBufferSync(ctx, writeBufferEntry, syncTask, err)
		}

		var future *conc.Future[struct{}]
		var err error
		if submission.admission != nil {
			future, err = submission.admission.Submit(ctx, syncTask, finish)
		} else {
			future, err = wb.syncMgr.SyncData(ctx, syncTask, finish)
		}
		if err != nil {
			settled := finish(err)
			result = append(result, completedSyncFuture(settled))
			continue
		}
		result = append(result, future)
	}
	return result
}
