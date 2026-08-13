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

package syncmgr

import (
	"context"
	"strings"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// SyncErrorDecision is what the sync path does about a failed phase. It is the
// ONLY error classification in this package: the write buffer owns whole-task
// retries, so every layer below it — pack writers, meta writer, growing flush —
// returns its error unchanged and lets this decide.
type SyncErrorDecision int

const (
	// SyncRetry: the same phase, run again, can succeed. Object storage
	// throttling, a coordinator that is not ready, an ID allocation round trip.
	// This is the default, because the alternative is expensive: giving up on a
	// segment stream pins its checkpoint and forces WAL replay.
	SyncRetry SyncErrorDecision = iota
	// SyncTerminal: no attempt can change the outcome. The phase re-derives its
	// inputs from the same segment state every time, so a retry reproduces the
	// failure exactly and would spin forever with nothing to show for it.
	SyncTerminal
	// SyncCanceled: the caller went away — close, drop, or an aborted segment
	// stream. Not a failure of the task, and never reported as one.
	SyncCanceled
)

// ClassifySyncError decides what to do about err. ctx is the caller's, and is
// what distinguishes "the caller went away" from an inner timeout.
//
// Storage-layer permanence is deliberately NOT consulted. Today every loon FFI
// failure is wrapped as packed.ErrLoonTransient regardless of its real cause,
// so asking would classify a dead bucket as retryable and a throttle as
// terminal with equal confidence — worse than not asking. Retrying is the safe
// side of that ignorance: rows stay pinned in their segment and the checkpoint
// stays put. When storage-layer classification lands, add it here — and only
// here.
func ClassifySyncError(ctx context.Context, err error) SyncErrorDecision {
	switch {
	case err == nil:
		return SyncRetry
	case errors.Is(err, context.Canceled):
		// Always a deliberate shutdown signal of this package: the dispatcher
		// cancels queued tasks with context.Canceled when closing or abandoning
		// a key, and the sync manager returns a bare context.Canceled when a
		// submission arrives after it started closing. Nothing manufactures one
		// from an unrelated context.
		return SyncCanceled
	case errors.Is(err, context.DeadlineExceeded):
		// A deadline, unlike a cancel, can be someone else's. The ID allocator
		// wraps its OWN 5s timeout around every AllocOne, so a briefly slow
		// coordinator surfaces as DeadlineExceeded from deep inside a task whose
		// caller is perfectly healthy. Classifying that as canceled dropped the
		// payload, removed the entry, scheduled no retry and raised no fatal — a
		// silent stall until restart. Only the caller's own expiry counts;
		// anything else is transient and falls through to SyncRetry.
		if ctx.Err() != nil {
			return SyncCanceled
		}
		return SyncRetry
	case isLayoutMismatch(err):
		// The flush layout disagrees with what the segment materialized.
		// Re-reading the same rows reproduces it exactly.
		return SyncTerminal
	case errors.Is(err, merr.ErrDataIntegrity):
		// The task refused its own inputs: empty manifest, row-count mismatch,
		// missing insert summary, a target offset behind already-flushed rows.
		// Every one is re-derived from the same segment state and the same
		// offset range.
		//
		// ErrServiceInternal is deliberately NOT here: on the growing-source
		// path it means "the source is not ready yet" (nil source, source
		// behind the target offset), which the next round genuinely resolves.
		return SyncTerminal
	case errors.Is(err, merr.ErrSegmentNotFound), errors.Is(err, merr.ErrChannelNotFound):
		// DataCoord says the target is gone. The meta writer already stopped its
		// own retry loop for exactly these two (see BrokerMetaWriter), and no
		// number of further attempts can bring the segment or channel back — but
		// the default is SyncRetry, so without this the task would be re-driven on
		// every timetick forever, pinning the channel checkpoint behind a segment
		// that no longer exists. A loud terminal failure beats a silent stall.
		return SyncTerminal
	case merr.GetErrorType(err) == merr.InputError:
		// The request content itself forces this branch; re-running the same
		// task re-derives the same input. Without this case the RetryErr
		// predicate in ioRetryOptions would override the retry framework's
		// own InputError short-circuit and spin forever.
		return SyncTerminal
	case !retry.IsRecoverable(err):
		// A layer below marked the error unrecoverable on purpose — the loon
		// classifier does this for anything that is not ErrLoonTransient.
		// Honoring that marker is the whole point of it existing.
		return SyncTerminal
	case merr.IsNonRetryableErr(err):
		// Milvus-side terminal error: the request is malformed or the target is
		// gone. A restart can at least pick up corrected configuration;
		// retrying in-process cannot.
		return SyncTerminal
	default:
		return SyncRetry
	}
}

// isLayoutMismatch matches on message text because the column-group check lives
// below the cgo boundary and has no error code to carry.
func isLayoutMismatch(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "Column count mismatch") ||
		strings.Contains(msg, "Column group size mismatch")
}

// DefaultIORetryAttempts is how many times the object-storage writer and the
// meta writer retry one operation before handing the error back.
//
// How long this inner retry should be depends entirely on how expensive the
// NEXT layer's retry is:
//
//	flush   cheap. The write buffer keeps the task, its payload and its queue
//	        slot, and re-drives it on the next timetick. So this stays short —
//	        an unbounded retry here would hide an outage from the layer that
//	        owns ordering and memory backpressure.
//	import  expensive. Nothing re-submits the task; the failure fails the whole
//	        ImportTask, and DataCoord re-reads and re-parses the entire file.
//	        So import raises this budget (dataNode.import.maxWriteRetryAttempts,
//	        0 = unlimited) rather than pay that price for one throttled PUT.
//
// Both callers classify with ClassifySyncError; only the budget and the backoff
// schedule differ, and both travel together in ioRetryPolicy.
const (
	DefaultIORetryAttempts uint = 3
	// defaultIORetryInitialSleep/defaultIORetryMaxSleep keep the flush path's
	// inner retry short for the same reason its budget is small: the write buffer
	// re-drives the whole task, and a long inner backoff would just hold its queue
	// slot and its payload. Import overrides both — see NewSyncTask there.
	defaultIORetryInitialSleep = 200 * time.Millisecond
	defaultIORetryMaxSleep     = time.Second
)

// ioRetryPolicy is one caller's budget and backoff for the retries inside the
// object storage writer and the meta writer. attempts of 0 means unlimited,
// matching retry.Attempts.
type ioRetryPolicy struct {
	attempts        uint
	initialInterval time.Duration
	maxInterval     time.Duration
}

// defaultIORetryPolicy is what the flush path uses.
func defaultIORetryPolicy() ioRetryPolicy {
	return ioRetryPolicy{
		attempts:        DefaultIORetryAttempts,
		initialInterval: defaultIORetryInitialSleep,
		maxInterval:     defaultIORetryMaxSleep,
	}
}

// normalized rejects a non-positive interval before it reaches retry.Do. With
// attempts=0 (unlimited, which import ships by default) a zero initial sleep
// degenerates into a zero-delay spin against the very object store that is
// already throttling us.
func (p ioRetryPolicy) normalized() ioRetryPolicy {
	if p.initialInterval <= 0 {
		p.initialInterval = defaultIORetryInitialSleep
	}
	if p.maxInterval <= 0 {
		p.maxInterval = defaultIORetryMaxSleep
	}
	return p
}

// ioRetryOptions is the single policy for those inner retries.
//
// The options are order-sensitive: retry.Sleep raises maxSleepTime to
// 2*initial, so MaxSleepTime must be applied after it to be the real cap.
func ioRetryOptions(ctx context.Context, policy ioRetryPolicy) []retry.Option {
	policy = policy.normalized()
	return []retry.Option{
		retry.Attempts(policy.attempts),
		retry.Sleep(policy.initialInterval),
		retry.MaxSleepTime(policy.maxInterval),
		retry.RetryErr(func(err error) bool {
			return ClassifySyncError(ctx, err) == SyncRetry
		}),
	}
}
