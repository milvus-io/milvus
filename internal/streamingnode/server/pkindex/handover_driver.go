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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// warnFailures is the number of failed handovers in a row after which a failed
// handover logs at warn level.
const warnFailures = 5

// handoverSlot is what the driver keeps about the handovers of one Index.
// The driver step reads and changes it only while running is false, and the
// handover task only while running is true. running is set before the submit
// and cleared as the last action of the task, so the two never overlap.
type handoverSlot struct {
	// running is true from the submit of a handover until its end.
	running atomic.Bool
	// stopped is set after a fatal handover error. The Index keeps serving
	// writes, but it does not hand over again.
	stopped atomic.Bool
	// backoff and failures are reset on a success with the current params.
	backoff  *typeutil.BackoffWithInstant
	failures int
	handle   nodescheduler.TaskHandle
}

// driveInterval is the time between two steps of the handover driver.
const driveInterval = time.Second

// startDriver starts the goroutine that runs step every driveInterval.
func (c *PChannelIndexManager) startDriver() {
	c.driverDone = make(chan struct{})
	go func() {
		defer close(c.driverDone)
		ticker := time.NewTicker(driveInterval)
		defer ticker.Stop()
		for {
			select {
			case <-c.ctx.Done():
				return
			case now := <-ticker.C:
				c.step(now)
			}
		}
	}()
}

// step picks the indexes that hand over now and submits their handovers.
func (c *PChannelIndexManager) step(now time.Time) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return
	}
	defer c.lifetime.Done()
	for _, idx := range c.pickHandovers(now) {
		c.submitHandover(idx)
	}
}

// slotOf returns the handover slot of idx. Add creates the slot with the
// Index, so that the first instant of its backoff is the open of the Index,
// and detachLocked deletes it. A detached Index gets a slot that no step sees.
func (c *PChannelIndexManager) slotOf(idx *index.Index) *handoverSlot {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s, ok := c.slots[idx]; ok {
		return s
	}
	return &handoverSlot{backoff: c.newBackoff()}
}

// submitHandover submits one handover of idx to the node scheduler. Only the
// driver step and the tests call it.
func (c *PChannelIndexManager) submitHandover(idx *index.Index) {
	s := c.slotOf(idx)
	s.running.Store(true)
	s.handle = c.m.cfg.Sched.Submit(&handoverTask{c: c, idx: idx, slot: s})
}

// reapHandover clears running when the scheduler finished the task without
// running it, for example after the scheduler closed. Only the driver step
// calls it.
func (c *PChannelIndexManager) reapHandover(s *handoverSlot) {
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
	c    *PChannelIndexManager
	idx  *index.Index
	slot *handoverSlot
}

// Execute runs the handover. It always returns nil: the driver retries a
// failed handover with backoff.
func (t *handoverTask) Execute(ctx context.Context) error {
	t.c.runHandover(ctx, t.idx, t.slot)
	return nil
}

// runHandover runs one handover step of idx and records its outcome in the
// slot: a success resets the backoff, a retryable failure advances it, a
// fatal error stops the handovers of the Index and marks the WAL unavailable.
func (c *PChannelIndexManager) runHandover(ctx context.Context, idx *index.Index, s *handoverSlot) {
	defer s.running.Store(false)
	committed, err := idx.Handover(ctx)
	switch {
	case err == nil:
		if committed {
			if s.failures > 0 {
				s.backoff, s.failures = c.newBackoff(), 0
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
		metrics.StreamingNodePKIndexHandoverFailuresTotal.WithLabelValues(paramtable.GetStringNodeID()).Inc()
		fields := []mlog.Field{mlog.Int("failures", s.failures), mlog.Bool("pending", idx.HasPending()), mlog.Err(err)}
		if s.failures > warnFailures {
			idx.Logger().Warn(ctx, "primary key index handover failed, retry later", fields...)
		} else {
			idx.Logger().Info(ctx, "primary key index handover failed, retry later", fields...)
		}
	}
}

// pickHandovers returns the indexes that hand over now.
//
// Only the driver step calls it. It reads and changes the slot of an Index
// only when no handover of that Index runs, see handoverSlot.
//
// The candidates come from the LastAcked heap, which holds every dirty and
// fresh Index: an Index with nothing to publish is never a candidate, and a
// pending request counts as dirty. So a step costs the number of dirty
// Indexes.
func (c *PChannelIndexManager) pickHandovers(now time.Time) []*index.Index {
	params := c.m.cfg.Params()
	tail := c.tail()
	var out []*index.Index
	var oldest *index.Index
	for _, idx := range c.dirtyIndexes() {
		s := c.slotOf(idx)
		c.reapHandover(s)
		if s.running.Load() || s.stopped.Load() || !idx.Ready() || !idx.Fresh() {
			continue
		}
		if now.Before(s.backoff.NextInstant()) {
			continue
		}
		// a pending request is retried first, regardless of size or tail
		if idx.HasPending() {
			out = append(out, idx)
			continue
		}
		dirty := idx.Dirty()
		// trigger 1: the active generation is large
		if dirty && idx.ActiveBytes() >= params.ActiveBytes {
			out = append(out, idx)
			continue
		}
		if dirty && (oldest == nil || idx.LastCommittedTimeTick() < oldest.LastCommittedTimeTick()) {
			oldest = idx
		}
	}
	// trigger 2: the WAL tail reaches the early threshold of the index
	// trigger 3: the WAL is under soft pressure, a fallback for trigger 2
	// Both need the index to hold the WAL truncation back. Else another
	// component holds the tail, and a handover only makes a small table.
	// The check uses the value without the clamp to ackedMax: the clamp can
	// hide an Index that holds the truncation back.
	//
	// TODO: after recovery adds LastAcked to the checkpoint of the WAL
	// recovery, RecoveryTimeTick is never above LastAcked, and these
	// triggers never fire. Then compare with the checkpoint of the other
	// components, without the index.
	if oldest != nil && (tail.Bytes >= params.TailBytes || tail.Bytes >= params.SoftWatermark) &&
		c.lastAckedUnclamped() < tail.RecoveryTimeTick {
		out = append(out, oldest)
	}
	return out
}
