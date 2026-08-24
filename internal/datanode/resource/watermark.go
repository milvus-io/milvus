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

// watermarkSampleInterval is how often measured memory is read.
const watermarkSampleInterval = 3 * time.Second

// The safety valve, and the ONLY place measured memory enters this package.
//
// It exists because the ledger cannot see everything. Flowgraph buffers, the
// write path, and in a standalone deployment the other roles sharing this
// process all consume memory no task requirement accounts for. If that pushes
// the machine towards its limit, something has to stop taking work, and only
// the machine itself can tell.
//
// It is a valve and not a budget. An earlier version fed the gap between
// measured and committed memory back in as a "non-task reservation" that shrank
// the task budget. That charged freed-but-not-yet-returned RSS as though it
// belonged to someone else -- Go's scavenger returns a large heap over minutes,
// so every completed large task immediately crushed the budget for the next one
// -- and in standalone it charged the QueryNode's segments the same way, with
// nothing to distinguish the two. Deciding placement from observation is the
// failure this design exists to avoid; the valve says only "stop", never "you
// have less than you think".

// startWatermarkLoop samples measured memory in the background until ctx ends.
// done, when non-nil, is closed once the loop has returned, so a caller that
// cancels ctx can wait for the last sample rather than racing it.
func (g *guard) startWatermarkLoop(ctx context.Context, done chan struct{}) {
	go func() {
		if done != nil {
			defer close(done)
		}
		g.watermarkLoop(ctx, watermarkSampleInterval)
	}()
}

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

// sampleOnce takes one measurement and moves the valve. It is separate from the
// loop so tests can step the state machine without a timer.
func (g *guard) sampleOnce() {
	used := hardware.GetUsedMemoryCount()
	total := hardware.GetMemoryCount()

	g.mu.Lock()
	g.updateFrozenLocked(used, total)
	// One consistent set of gauges per sample; see publishLocked.
	g.publishLocked(int64(used))
	g.mu.Unlock()
}

func (g *guard) updateFrozenLocked(used, total uint64) {
	if total == 0 {
		// The reading failed; hardware.GetMemoryCount logs and returns 0. A
		// ratio against it is +Inf or NaN, either of which would move the valve
		// on no evidence at all. Keeping the previous state is the only safe
		// answer.
		return
	}
	cfg := &paramtable.Get().DataNodeCfg
	ratio := float64(used) / float64(total)

	switch {
	case ratio >= cfg.ResourceHighWatermark.GetAsFloat():
		if !g.frozen {
			mlog.Warn(context.TODO(), "memory high watermark reached, node has stopped taking new tasks",
				mlog.Float64("ratio", ratio))
		}
		g.frozen = true
	case ratio < cfg.ResourceLowWatermark.GetAsFloat():
		if g.frozen {
			mlog.Info(context.TODO(), "memory back below low watermark, node is taking tasks again",
				mlog.Float64("ratio", ratio))
			g.thawLocked()
		}
		g.frozen = false
	}
	// Between the marks the previous state is kept: the hysteresis band is what
	// stops the node oscillating between taking work and refusing it.
}

// thawLocked wakes everything parked in Accept and arms a fresh channel for the
// next freeze. Closing is what makes the wake broadcast rather than a handoff
// to one waiter.
func (g *guard) thawLocked() {
	close(g.thaw)
	g.thaw = make(chan struct{})
}
