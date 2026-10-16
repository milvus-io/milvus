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
	"time"

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// driveInterval is the time between two steps of the handover driver.
const driveInterval = time.Second

// startDriver starts the goroutine that runs step every driveInterval.
func (c *Channel) startDriver() {
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
func (c *Channel) step(now time.Time) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return
	}
	defer c.lifetime.Done()
	for _, idx := range c.pickHandovers(now) {
		idx.submitHandover()
	}
}

// pickHandovers returns the indexes that hand over now.
//
// Only the driver step calls it. It reads and changes the backoff of an
// Index only when no handover of that Index runs. The running flag, set
// before the submit and cleared as the last action of the handover, keeps
// the driver and the handover task apart.
//
// The candidates come from the LastAcked heap, which holds every dirty and
// fresh Index: an Index with nothing to publish is never a candidate, and a
// pending request counts as dirty. So a step costs the number of dirty
// Indexes.
func (c *Channel) pickHandovers(now time.Time) []*Index {
	params := c.m.cfg.Params()
	tail := c.tail()
	var out []*Index
	var oldest *Index
	for _, idx := range c.dirtyIndexes() {
		idx.reapHandover()
		if idx.running.Load() || idx.handoverStopped.Load() || !idx.readyDone() || !idx.fresh() {
			continue
		}
		if now.Before(idx.backoff.NextInstant()) {
			continue
		}
		// a pending request is retried first, regardless of size or tail
		if idx.pending.Load() != nil {
			out = append(out, idx)
			continue
		}
		dirty := idx.dirty()
		// trigger 1: the active generation is large
		if dirty && idx.eng.Stats().ActiveBytes >= params.ActiveBytes {
			out = append(out, idx)
			continue
		}
		if dirty && (oldest == nil || idx.lastCommittedTimeTick() < oldest.lastCommittedTimeTick()) {
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
