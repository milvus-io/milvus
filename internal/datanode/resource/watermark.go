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

package resource

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// watermarkSampleInterval is also the unit of dataNode.resource.slowGrowPeriods:
// the non-task reservation can only relax after that many of these samples.
const watermarkSampleInterval = 3 * time.Second

// startWatermarkLoop samples measured memory in the background until ctx ends.
//
// Nothing here participates in admission arithmetic. The loop may only take
// budget away -- by freezing, or by growing the non-task reservation -- and
// giving budget back is deliberately slow. A measured signal that could widen
// the budget would reintroduce "decide from current state", which fails
// precisely when admitted tasks have not yet reached their peak.
func (g *guard) startWatermarkLoop(ctx context.Context) {
	go g.watermarkLoop(ctx, watermarkSampleInterval)
}

// watermarkLoop is the body of the loop, with the period passed in so tests can
// run it without waiting out the production interval.
func (g *guard) watermarkLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.sampleOnce()
		}
	}
}

// sampleOnce takes one measurement and folds it into the two one-way signals.
// It is separate from the loop so tests can step the state machine without a
// timer.
func (g *guard) sampleOnce() {
	used := hardware.GetUsedMemoryCount()
	total := hardware.GetMemoryCount()

	g.mu.Lock()
	g.updateFrozenLocked(used, total)
	g.updateNonTaskLocked(used)
	frozen := g.frozen
	g.mu.Unlock()

	if !frozen {
		// Either signal may have relaxed, and neither goes through Release. A
		// waiter already parked in its select re-checks the ledger only when its
		// channel fires, so without this wake it would sleep through the wider
		// budget until its own deadline.
		g.wakeWaiters()
	}
}

func (g *guard) updateFrozenLocked(used, total uint64) {
	if total == 0 {
		// The reading failed; hardware.GetMemoryCount logs and returns 0. A ratio
		// against it is +Inf or NaN, either of which would move the freeze on no
		// evidence at all. Keeping the previous state is the only safe answer.
		return
	}
	cfg := &paramtable.Get().DataNodeCfg
	ratio := float64(used) / float64(total)

	switch {
	case ratio >= cfg.ResourceHighWatermark.GetAsFloat():
		if !g.frozen {
			mlog.Warn(context.TODO(), "memory high watermark reached, admission frozen",
				mlog.Float64("ratio", ratio))
		}
		g.frozen = true
	case ratio < cfg.ResourceLowWatermark.GetAsFloat():
		if g.frozen {
			mlog.Info(context.TODO(), "memory back below low watermark, admission resumed",
				mlog.Float64("ratio", ratio))
		}
		g.frozen = false
	}
	// Between the marks the previous state is kept: the hysteresis band is what
	// stops the node oscillating between admitting and refusing.
}

// updateNonTaskLocked tracks memory that the ledger does not know about --
// flowgraph buffers, the write path, and in standalone the other roles sharing
// this process. It needs no per-task attribution, only a total.
func (g *guard) updateNonTaskLocked(used uint64) {
	cfg := &paramtable.Get().DataNodeCfg

	sample := int64(used) - g.reserved.Memory
	if sample < 0 {
		// The ledger has committed more than the process has actually touched,
		// which is the normal state for freshly admitted tasks. That headroom is
		// already spoken for, so none of it may be reported as non-task memory.
		// A negative reservation would be *added* to the budget by budgetLocked,
		// so this clamp comes before the floor rather than relying on the floor
		// being configured non-negative.
		sample = 0
	}
	if floor := cfg.ResourceNonTaskMemoryFloor.GetAsInt64(); sample < floor {
		sample = floor
	}

	if sample >= g.nonTaskPeak {
		// Tightening takes effect at once.
		g.nonTaskPeak = sample
		g.lowSampleCount = 0
	} else {
		// Relaxing requires sustained evidence: slowGrowPeriods consecutive lower
		// samples. One low reading usually means the admitted tasks have not
		// grown into their estimates yet.
		g.lowSampleCount++
		if g.lowSampleCount >= cfg.ResourceSlowGrowPeriods.GetAsInt() {
			g.nonTaskPeak = sample
			g.lowSampleCount = 0
		}
	}
	g.nonTask = g.nonTaskPeak
}
