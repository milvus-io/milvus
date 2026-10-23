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

package pkindex

import (
	"context"
	"slices"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// warnFailures is the number of failed handovers in a row after which a failed
// handover logs at warn level.
const warnFailures = 5

// handoverSchedule is what the driver keeps about the handovers of one Index:
// whether one runs, whether they stopped, whether one is requested, and when
// the next one may start. The driver step reads and changes it only while
// running is false, and the handover task only while running is true.
// running is set before the submit and cleared as the last action of the
// task, so the two never overlap. The atomics are the exception: the observe
// raises requestedTT and reads the others from its own goroutine.
type handoverSchedule struct {
	// running is true from the submit of a handover until its end.
	running atomic.Bool
	// stopped is set after a fatal handover error. The Index keeps serving
	// writes, but it does not hand over again.
	stopped atomic.Bool
	// requestedTT is the highest time tick an explicit flush asked the Index
	// to cover, see RequestHandover. It is never reset: a request is served
	// once LastCommittedTimeTick reaches it.
	requestedTT atomic.Uint64
	// retryAt is the instant of the next attempt after a failure, in unix
	// nanoseconds, 0 when the Index is not in backoff. The task sets it from
	// backoff, so that the observe can read it without the backoff.
	retryAt atomic.Int64
	// backoff and failures are reset on a success with the current params.
	backoff  *typeutil.BackoffWithInstant
	failures int
	handle   nodescheduler.TaskHandle
}

// nextAttempt returns the instant of the next attempt after a failure, the
// zero time when the Index is not in backoff.
func (s *handoverSchedule) nextAttempt() time.Time {
	at := s.retryAt.Load()
	if at == 0 {
		return time.Time{}
	}
	return time.Unix(0, at)
}

// startDriver starts the driver goroutine. It sleeps until ObserveTimeTick
// signals a trigger or the backoff of a failed handover ends. There is no
// ticker: the persisted TimeTicks of the WAL are the clock of the triggers,
// and the WAL appends one at least every forced persist interval.
func (c *PChannelIndexManager) startDriver() {
	c.driverDone = make(chan struct{})
	go func() {
		defer close(c.driverDone)
		timer := time.NewTimer(time.Hour)
		if !timer.Stop() {
			<-timer.C
		}
		defer timer.Stop()
		for {
			select {
			case <-c.ctx.Done():
				return
			case <-c.wake:
			case <-timer.C:
			}
			if next := c.step(time.Now()); !next.IsZero() {
				timer.Reset(time.Until(next))
			}
		}
	}()
}

// step picks the indexes that hand over now and submits their handovers. It
// returns when the driver must wake again for a handover in backoff, zero
// when none is.
func (c *PChannelIndexManager) step(now time.Time) time.Time {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return time.Time{}
	}
	defer c.lifetime.Done()
	c.steps.Add(1)
	// Every index, also an idle one, sheds the in-flight entries with an
	// unknown outcome that the observe passed. The observe does not fan a
	// TimeTick out to every vchannel; the driver does it when it wakes.
	lo := c.lastObserved.Load()
	for _, idx := range c.list() {
		idx.Authority().ObserveTimeTick(lo)
	}
	picks, next, stalled := c.pickHandovers(now)
	for _, idx := range picks {
		c.submitHandover(idx)
	}
	if stalled {
		// the next stall pick comes a stall timeout later, not at the next
		// TimeTick while this handover runs
		c.stallSince.Store(0)
	}
	return next
}

// pickHandovers returns the indexes that hand over now, the earliest instant
// at which a dirty Index in backoff may retry (zero when none is), and
// whether the stall trigger picked. Only the driver step calls it: it reaps
// the finished tasks first.
func (c *PChannelIndexManager) pickHandovers(now time.Time) ([]*index.Index, time.Time, bool) {
	for _, idx := range c.dirtyIndexes() {
		c.reapHandover(c.scheduleOf(idx))
	}
	return c.evaluate(now)
}

// stalledFor returns how long, by observed time, the index has held the
// checkpoint back: the physical time between stallSince and lastObserved.
func (c *PChannelIndexManager) stalledFor() time.Duration {
	since := c.stallSince.Load()
	if since == 0 {
		return 0
	}
	return tsoutil.PhysicalTime(c.lastObserved.Load()).Sub(tsoutil.PhysicalTime(since))
}

// wantsHandover is the check of the observe: it reports whether the driver
// would hand an Index over now. It evaluates the same conditions as the
// driver, so an Index in backoff does not wake the driver: the backoff timer
// of the driver does. It only reads.
func (c *PChannelIndexManager) wantsHandover() bool {
	picks, _, _ := c.evaluate(time.Now())
	return len(picks) > 0
}

// scheduleOf returns the handover schedule of idx. Add creates the schedule with the
// Index, so that the first instant of its backoff is the open of the Index,
// and detachLocked deletes it. A detached Index gets a schedule that no step sees.
func (c *PChannelIndexManager) scheduleOf(idx *index.Index) *handoverSchedule {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s, ok := c.schedules[idx]; ok {
		return s
	}
	return &handoverSchedule{backoff: c.newBackoff()}
}

// submitHandover submits one handover of idx to the node scheduler. Only the
// driver step and the tests call it.
func (c *PChannelIndexManager) submitHandover(idx *index.Index) {
	s := c.scheduleOf(idx)
	s.running.Store(true)
	s.handle = c.m.cfg.Sched.Submit(&handoverTask{c: c, idx: idx, schedule: s})
}

// reapHandover clears running when the scheduler finished the task without
// running it, for example after the scheduler closed. Only the driver step
// calls it.
func (c *PChannelIndexManager) reapHandover(s *handoverSchedule) {
	if s.handle == nil || !s.running.Load() {
		return
	}
	done, cancel := context.WithCancel(context.Background())
	cancel()
	// Wait returns nil only when the task is finished. A finished task that
	// ran already cleared running, so this store changes nothing for it.
	if s.handle.Wait(done) == nil {
		s.handle = nil
		s.running.Store(false)
	}
}

// handoverTask runs one handover of an Index on the node scheduler.
type handoverTask struct {
	c        *PChannelIndexManager
	idx      *index.Index
	schedule *handoverSchedule
}

// Execute runs the handover. It always returns nil: the driver retries a
// failed handover with backoff.
func (t *handoverTask) Execute(ctx context.Context) error {
	t.c.runHandover(ctx, t.idx, t.schedule)
	return nil
}

// runHandover runs one handover step of idx and records its outcome in the
// schedule: a success resets the backoff, a retryable failure advances it, a
// fatal error stops the handovers of the Index and marks the WAL unavailable.
func (c *PChannelIndexManager) runHandover(ctx context.Context, idx *index.Index, s *handoverSchedule) {
	retry := false
	defer func() {
		s.running.Store(false)
		if retry {
			// the driver sets its timer from the backoff at its next step;
			// it must see running false, so signal after the store
			c.signal()
		}
	}()
	committed, err := idx.Handover(ctx)
	switch {
	case err == nil:
		if committed {
			if s.failures > 0 {
				s.backoff, s.failures = c.newBackoff(), 0
				s.retryAt.Store(0)
			}
			metrics.StreamingNodePKIndexHandoverTotal.WithLabelValues(paramtable.GetStringNodeID()).Inc()
		}
	case errors.Is(err, context.Canceled):
		// the Index or the scheduler is closing, this is not a failure
	case index.IsFatal(err):
		s.stopped.Store(true)
		idx.Logger().Error(ctx, "primary key index handover failed, the WAL becomes unavailable", mlog.Err(err))
		c.fatal(err)
	default:
		s.failures++
		s.backoff.UpdateInstantWithNextBackOff()
		s.retryAt.Store(s.backoff.NextInstant().UnixNano())
		retry = true
		metrics.StreamingNodePKIndexHandoverFailuresTotal.WithLabelValues(paramtable.GetStringNodeID()).Inc()
		fields := []mlog.Field{mlog.Int("failures", s.failures), mlog.Bool("pending", idx.HasPending()), mlog.Err(err)}
		if s.failures > warnFailures {
			idx.Logger().Warn(ctx, "primary key index handover failed, retry later", fields...)
		} else {
			idx.Logger().Info(ctx, "primary key index handover failed, retry later", fields...)
		}
	}
}

// evaluate evaluates the handover triggers at now. It returns the indexes to
// hand over, the earliest instant at which a dirty Index in backoff may
// retry (zero when none is), and whether the stall trigger picked.
//
// The candidates come from the LastAcked heap, which holds every dirty Index
// of a node with a Committer: an Index with nothing to publish is never a
// candidate, and a pending request counts as dirty. So an evaluation costs
// the number of dirty Indexes. An Index that runs a handover, stopped, is
// not ready, or is in backoff can not run now.
//
// Triggers 2 to 4 hand over the Index with the oldest commit: the one that
// holds the WAL checkpoint back. When that Index can not run now, nothing
// runs for them: a younger Index would not move LastAcked. The oldest is
// found over every dirty Index, also the ones that can not run.
//
// The observe calls it from its goroutine and the driver step from its own,
// so it only reads: the dirty set, the stats of the engines, and atomics.
func (c *PChannelIndexManager) evaluate(now time.Time) (out []*index.Index, next time.Time, stalled bool) {
	params := c.m.cfg.Params()
	tail := *c.tail.Load()
	lo := c.lastObserved.Load()
	var oldest *index.Index
	oldestRunnable := false
	for _, idx := range c.dirtyIndexes() {
		s := c.scheduleOf(idx)
		runnable := !s.running.Load() && !s.stopped.Load() && idx.Ready()
		if at := s.nextAttempt(); runnable && now.Before(at) {
			if next.IsZero() || at.Before(next) {
				next = at
			}
			runnable = false
		}
		dirty := idx.Dirty()
		if dirty && (oldest == nil || idx.LastCommittedTimeTick() < oldest.LastCommittedTimeTick()) {
			oldest, oldestRunnable = idx, runnable
		}
		if !runnable {
			continue
		}
		// a pending request is retried first, regardless of size or tail
		if idx.HasPending() {
			out = append(out, idx)
			continue
		}
		// trigger 5: an explicit flush asked the Index to cover want, and
		// lastObserved reached want, so the handover covers it
		if want := s.requestedTT.Load(); dirty && idx.LastCommittedTimeTick() < want && lo >= want {
			out = append(out, idx)
			continue
		}
		// trigger 1: the active generation is large
		if dirty && idx.ActiveBytes() >= params.ActiveBytes {
			out = append(out, idx)
		}
	}
	// Triggers 2 to 4 need the index to hold the WAL checkpoint back. Else
	// another component holds the tail, and a handover only makes a small
	// table.
	if oldest == nil || !oldestRunnable || slices.Contains(out, oldest) || !c.holdsCheckpointBack(tail) {
		return out, next, false
	}
	// trigger 2: the WAL tail reaches the early threshold of the index
	// trigger 3: the WAL is under soft pressure, a fallback for trigger 2
	if tail.Bytes >= params.TailBytes || tail.Bytes >= params.SoftWatermark {
		return append(out, oldest), next, false
	}
	// trigger 4: the index held the checkpoint back for the stall timeout,
	// by observed time
	if params.StallTimeout > 0 && c.stalledFor() >= params.StallTimeout {
		return append(out, oldest), next, true
	}
	return out, next, false
}

// newBackoff returns the handover backoff of an Index with the current
// params.
func (c *PChannelIndexManager) newBackoff() *typeutil.BackoffWithInstant {
	params := c.m.cfg.Params()
	return typeutil.NewBackoffWithInstant(typeutil.BackoffTimerConfig{
		Backoff: typeutil.BackoffConfig{
			InitialInterval: params.BackoffBase,
			Multiplier:      2,
			MaxInterval:     params.MaxBackoff,
		},
	})
}
