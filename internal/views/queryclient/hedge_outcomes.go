// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package queryclient

import "time"

const (
	// hedgeOutcomesCap is how many of the latest hedge outcomes are kept.
	hedgeOutcomesCap = 128
	// hedgeWinRateMinSamples is the number of outcomes within hedgeWindow
	// below which the win rate is not acted upon.
	hedgeWinRateMinSamples = 10
	// defaultHedgeMinWinRate suppresses speculative hedges when recent win rate
	// falls below this threshold, retaining hedgeProbeRate as recovery probes.
	defaultHedgeMinWinRate = 0.3
	// hedgeProbeRate is the probability a hedge is still sent with when the
	// win rate is below the minimum: the probes let the win rate recover.
	hedgeProbeRate = 0.1
)

type hedgeOutcome struct {
	at  time.Time
	won bool
}

// hedgeOutcomes holds the latest hedgeOutcomesCap hedge outcomes of a
// statistics key. It is not safe for concurrent use.
type hedgeOutcomes struct {
	ring  [hedgeOutcomesCap]hedgeOutcome
	count int // outcomes recorded, up to len(ring)
	next  int
}

func (h *hedgeOutcomes) add(now time.Time, won bool) {
	h.ring[h.next] = hedgeOutcome{at: now, won: won}
	h.next = (h.next + 1) % len(h.ring)
	if h.count < len(h.ring) {
		h.count++
	}
}

// winRate returns the share of won hedges among the kept outcomes that are
// within hedgeWindow, and false when these are fewer than
// hedgeWinRateMinSamples: too few, or too old, to tell.
func (h *hedgeOutcomes) winRate(now time.Time) (float64, bool) {
	n, won := 0, 0
	for i := 0; i < h.count; i++ {
		o := h.ring[i]
		if now.Sub(o.at) > hedgeWindow {
			continue
		}
		n++
		if o.won {
			won++
		}
	}
	if n < hedgeWinRateMinSamples {
		return 0, false
	}
	return float64(won) / float64(n), true
}

func (h *hedgeOutcomes) lastUsed() time.Time {
	if h.count == 0 {
		return time.Time{}
	}
	return h.ring[(h.next-1+len(h.ring))%len(h.ring)].at
}
