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

// Per-node load pacing.
//
// The executor admits up to CPU x queryNodeTaskParallelismFactor tasks per
// node (320 on a 16-core node) while a QueryNode loads only a few segments at
// a time; the rest queue inside the QueryNode, where the wait counts against
// the task timeout, so a burst (rollout, scale-out, user reload) times out
// and is redone. The pacer caps segment loads in flight per node and adapts
// the cap from how long loads take relative to their timeout (AIMD): a load
// that took more than slowFraction (a quarter) of its timeout on a saturated
// node halves the cap, at most once per window of loads; a window of loads
// all under that on a saturated node grows it by one. The cap settles where
// queueing keeps loads at about a quarter of the timeout: the node stays
// busy and no load comes near its deadline. With size-aware timeouts
// (minLoadBandwidthMBps) a large segment's longer load is measured against
// its own longer timeout. Loads above the cap wait in QueryCoord, where no
// deadline is armed. On by default; queryCoord.loadPacingEnabled turns it
// off.

import (
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type loadPacerConfig struct {
	enabled      bool
	initial, min float64
	max          float64
	slowFraction float64 // a load slower than this x its timeout shrinks the cap
}

func getLoadPacerConfig() loadPacerConfig {
	cfg := &paramtable.Get().QueryCoordCfg
	return loadPacerConfig{
		enabled:      cfg.LoadPacingEnabled.GetAsBool(),
		initial:      cfg.LoadPacingInitial.GetAsFloat(),
		min:          cfg.LoadPacingMin.GetAsFloat(),
		max:          cfg.LoadPacingMax.GetAsFloat(),
		slowFraction: cfg.LoadPacingSlowFraction.GetAsFloat(),
	}
}

type loadPacer struct {
	mu          sync.Mutex
	cap         float64
	inflight    int
	sinceShrink int // loads finished since the cap last shrank
}

// tryAcquire admits one load if the node is under its cap.
func (p *loadPacer) tryAcquire(cfg loadPacerConfig) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cap == 0 {
		p.cap = cfg.initial
	}
	if float64(p.inflight) >= p.cap {
		return false
	}
	p.inflight++
	return true
}

// release records a finished load and adapts the cap.
func (p *loadPacer) release(cfg loadPacerConfig, elapsed, timeout time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	saturated := float64(p.inflight) >= p.cap-1
	p.inflight--
	if timeout <= 0 {
		return
	}
	p.sinceShrink++
	slow := elapsed.Seconds() > cfg.slowFraction*timeout.Seconds()
	switch {
	case slow && saturated && float64(p.sinceShrink) >= p.cap:
		// Only queueing on a saturated node is congestion; a load that is
		// slow on its own (a large or jittery one) is not. At most once
		// per window of loads, so one slow burst does not collapse the cap.
		p.cap = max(cfg.min, p.cap/2)
		p.sinceShrink = 0
	case !slow && saturated:
		p.cap = min(cfg.max, p.cap+1/p.cap)
	}
}

// Cap reports the current cap (0 before the first load).
func (p *loadPacer) Cap() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cap
}
