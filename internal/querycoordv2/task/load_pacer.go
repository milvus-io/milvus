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

package task

// Load pacer: admission control on the bytes of segment data in flight to
// a QueryNode.
//
// The executor admits up to CPU x queryNodeTaskParallelismFactor tasks per
// node (320 on a 16-core node) while a QueryNode loads at a bandwidth that
// is shared by whatever is in flight: 20 ten-gigabyte segments loading at
// once take 140 s each on a node that loads one in 20 s, and a burst of
// loads (rollout, scale-out, user reload) times out and is redone. Loads
// of small segments, by contrast, are bounded by the node's load pool, not
// its bandwidth.
//
// The pacer keeps the bytes in flight to a node at what the node drains
// within a quarter of the task timeout at its believed bandwidth, and the
// loads in flight to its CPU count. The bandwidth starts at
// loadBandwidthPerCoreMBps per core (32 MiB/s: a conservative figure for a
// node with local NVMe) and adapts (AIMD): a load that took more than
// slowFraction (half) of its timeout while the node was saturated halves
// it, at most once per window (one budget's worth of bytes), so one slow
// burst does not collapse it; a window of fast loads on a saturated node
// adds one core's worth. It settles where queueing keeps loads at about
// half the timeout: the node stays busy and no load comes near its
// deadline. An idle node always takes one load, however large. Loads above
// the budget wait in QueryCoord, where no deadline is armed, and are
// dispatched as soon as a slot frees. On by default;
// queryCoord.loadPacingEnabled turns it off.

import (
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	pacerDrainFraction = 0.25 // of the timeout: what the in-flight bytes must drain within
	pacerFloor         = 1.0 / 8
	pacerCeiling       = 8.0
	pacerDefaultCPU    = 4
)

type loadPacerConfig struct {
	enabled      bool
	waitForTick  bool    // test knob: do not wake the dispatcher when a slot frees
	perCore      float64 // starting bandwidth per core, bytes/s
	slowFraction float64 // a load slower than this x its timeout shrinks the bandwidth
}

func getLoadPacerConfig() loadPacerConfig {
	cfg := &paramtable.Get().QueryCoordCfg
	return loadPacerConfig{
		enabled:      cfg.LoadPacingEnabled.GetAsBool(),
		perCore:      cfg.LoadBandwidthPerCoreMBps.GetAsFloat() * (1 << 20),
		slowFraction: cfg.LoadPacingSlowFraction.GetAsFloat(),
		waitForTick:  cfg.LoadPacingWaitForTick.GetAsBool(),
	}
}

type loadPacer struct {
	mu               sync.Mutex
	initial          float64 // bytes/s at the start: cores x perCore
	bandwidth        float64 // bytes/s the node is believed to load at
	cpu              int
	inflight         int
	inflightBytes    float64
	sinceShrinkBytes float64 // bytes completed since the bandwidth last shrank
	held             bool    // a load was refused since the last wake
}

func (p *loadPacer) init(cfg loadPacerConfig, cpu int) {
	if cpu <= 0 {
		cpu = pacerDefaultCPU
	}
	p.cpu = cpu
	if p.initial == 0 {
		p.initial = float64(cpu) * cfg.perCore
		p.bandwidth = p.initial
	}
}

// budget is the bytes the node drains within pacerDrainFraction of timeout.
func (p *loadPacer) budget(timeout time.Duration) float64 {
	return p.bandwidth * timeout.Seconds() * pacerDrainFraction
}

// tryAcquire admits a load of bytes if the node is idle, or has a free
// load slot and the budget for it.
func (p *loadPacer) tryAcquire(cfg loadPacerConfig, cpu int, bytes float64, timeout time.Duration) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.init(cfg, cpu)
	if p.inflight > 0 && (p.inflight >= p.cpu || p.inflightBytes+bytes > p.budget(timeout)) {
		p.held = true
		return false
	}
	p.inflight++
	p.inflightBytes += bytes
	return true
}

// release records a finished load of bytes and adapts the bandwidth.
func (p *loadPacer) release(cfg loadPacerConfig, bytes float64, elapsed, timeout time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	budget := p.budget(timeout)
	// Saturated: a load was held back, or the node was at its budget or its
	// load slots, while this one ran.
	saturated := p.held || p.inflight >= p.cpu || p.inflightBytes >= budget-bytes
	p.inflight--
	p.inflightBytes = max(0, p.inflightBytes-bytes)
	if timeout <= 0 {
		return
	}
	p.sinceShrinkBytes += bytes
	slow := elapsed.Seconds() > cfg.slowFraction*timeout.Seconds()
	switch {
	case slow && saturated && p.sinceShrinkBytes >= budget:
		// Only queueing on a saturated node is congestion; a load that is
		// slow on its own (a large or jittery one) is not. At most once
		// per window of bytes, so one slow burst does not collapse it.
		p.bandwidth = max(p.initial*pacerFloor, p.bandwidth/2)
		p.sinceShrinkBytes = 0
	case !slow && saturated:
		// One core's worth per window of fast loads.
		p.bandwidth = min(p.initial*pacerCeiling, p.bandwidth+cfg.perCore*bytes/max(budget, 1))
	}
}

// takeHeld reports whether a load was refused since the last call: a freed
// slot then has a load waiting for it.
func (p *loadPacer) takeHeld() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	held := p.held
	p.held = false
	return held
}

// Budget is the bytes the pacer lets the node have in flight under timeout,
// and the loads: one per core. Placement and balance plan against it.
func (p *loadPacer) Budget(cfg loadPacerConfig, cpu int, timeout time.Duration) (bytes float64, loads int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.init(cfg, cpu)
	return p.budget(timeout), p.cpu
}

// Bandwidth is the bytes/s the node is believed to load at (0 before the
// first load).
func (p *loadPacer) Bandwidth() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.bandwidth
}

// InflightBytes is the bytes of segment data on their way to the node.
func (p *loadPacer) InflightBytes() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.inflightBytes
}
