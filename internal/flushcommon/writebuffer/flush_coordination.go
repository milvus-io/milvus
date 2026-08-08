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

// What BOTH flush paths use, so neither has to squat in the other's file: the
// flush debt a segment carries, the retry cadence, the submission dispatch, the
// release every terminal path performs, and the one wait that covers them
// together.
//
// The paths themselves are write_buffer_sync_coordinator.go (payload yielded out
// of the write buffer, which the task then owns) and growing_flush_coordinator.go
// (rows still pinned in a segcore growing segment, which the task only reads).
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
// debt already rate-limited by an earlier backoff stays rate-limited. Used when
// admission refused the task — what unblocks that is an event, not time.
func (i *flushIntent) want() {
	i.owes = true
}

// backoff records the same debt, rate-limited: the attempt could not be made or
// it failed, so time has to pass first. Only the transition INTO a rate-limited
// debt stamps it — repeated failures inside one round must not keep pushing the
// next attempt out.
func (i *flushIntent) backoff(now time.Time) {
	if !i.owes || i.since.IsZero() {
		i.since = now
	}
	i.owes = true
}

// attempted records that an attempt was just made for this debt. The debt STAYS
// — only a completed task settles it — but the rate limit starts over.
//
// Every re-drive must call this. Without it `since` keeps the timestamp of the
// FIRST failure, backoff refuses to move it (the debt is already outstanding),
// and due() is true on every timetick from then on: a retry storm instead of one
// attempt per interval.
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
	// timeticks, so driveRetries can no longer re-drive a failed attempt.
	// Retryable failures keep their entry pending (finishWriteBufferSync leaves
	// them queued while dropping) and this loop re-submits them on the same
	// flushRetryInterval cadence — until the caller's context cancels, which
	// turns every further attempt terminal.
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

// retryTicker builds the ticker every wait loop uses to re-drive failed flushes
// while it holds the goroutine that would otherwise do the driving. Returns a
// nil channel when retry is disabled, so the caller's select arm simply never
// fires. The 100ms floor keeps a zero/tiny configured interval from turning the
// wait into a spin.
func (wb *writeBufferBase) retryTicker() (interval time.Duration, tick <-chan time.Time, stop func()) {
	interval = wb.retryInterval()
	if interval < 0 {
		return interval, nil, func() {}
	}
	period := interval
	if period < 100*time.Millisecond {
		period = 100 * time.Millisecond
	}
	ticker := time.NewTicker(period)
	return interval, ticker.C, ticker.Stop
}

func (wb *writeBufferBase) submitSyncTasks(ctx context.Context, syncTasks []syncmgr.Task) []*conc.Future[struct{}] {
	result := make([]*conc.Future[struct{}], 0, len(syncTasks))
	for _, syncTask := range syncTasks {
		var writeBufferEntry *writeBufferSyncEntry
		if writeBufferTask, ok := syncTask.(*syncmgr.SyncTask); ok {
			wb.mut.Lock()
			writeBufferEntry = wb.writeBufferSyncEntryLocked(writeBufferTask)
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
				continue
			}
		}

		// One dispatch, used by both the completion callback and the
		// submit-error path below, so the two can never disagree about how a
		// task of a given kind is settled.
		finish := func(err error) error {
			switch typedTask := syncTask.(type) {
			case *syncmgr.SyncTask:
				return wb.finishWriteBufferSync(ctx, writeBufferEntry, typedTask, err)
			case *syncmgr.GrowingSourceSyncTask:
				return wb.finishGrowingSourceSync(ctx, typedTask, err)
			default:
				mlog.Fatal(ctx, "unsupported sync task", mlog.Int64("segmentID", syncTask.SegmentID()))
				return err
			}
		}

		future, err := wb.syncMgr.SyncData(ctx, syncTask, finish)
		if err != nil {
			settled := finish(err)
			result = append(result, conc.Go(func() (struct{}, error) {
				return struct{}{}, settled
			}))
			continue
		}
		result = append(result, future)
	}
	return result
}
