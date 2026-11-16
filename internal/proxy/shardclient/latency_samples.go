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
package shardclient

import (
	"math"
	"sort"
	"time"
)

const (
	// latencySamplesCap is how many of the latest samples are kept.
	latencySamplesCap = 1024
	// hedgeWindow is the window the hedge quantile is taken over, and
	// hedgeRecentSamples how many of the latest samples are used instead
	// when the window holds fewer.
	hedgeWindow        = 10 * time.Second
	hedgeRecentSamples = 100
)

type latencySample struct {
	at      time.Time
	latency time.Duration
}

// latencySamples keeps the latest request latencies of one statistics key
// (channel and request class), all replicas together: one sample per
// successful channel request, the time until its first result. It answers
// "how long does this kind of request on this channel take", which the hedge
// delay is derived from.
//
// It is not safe for concurrent use.
type latencySamples struct {
	ring []latencySample
	next int // where the next sample goes once the ring is full
}

func (l *latencySamples) add(now time.Time, latency time.Duration) {
	if len(l.ring) < latencySamplesCap {
		l.ring = append(l.ring, latencySample{at: now, latency: latency})
		return
	}
	l.ring[l.next] = latencySample{at: now, latency: latency}
	l.next = (l.next + 1) % latencySamplesCap
}

// newest returns the i-th newest sample (0 is the latest).
func (l *latencySamples) newest(i int) latencySample {
	last := len(l.ring) - 1
	if len(l.ring) == latencySamplesCap {
		last = (l.next - 1 + latencySamplesCap) % latencySamplesCap
	}
	return l.ring[(last-i+len(l.ring))%len(l.ring)]
}

func (l *latencySamples) lastUsed() time.Time {
	if len(l.ring) == 0 {
		return time.Time{}
	}
	return l.newest(0).at
}

// quantile returns the q-quantile (0 < q <= 1) of the samples within
// hedgeWindow, or, when those are fewer than hedgeRecentSamples, of the latest
// hedgeRecentSamples samples however old (all samples while fewer were seen).
// It returns false without any sample.
func (l *latencySamples) quantile(now time.Time, q float64) (time.Duration, bool) {
	if len(l.ring) == 0 {
		return 0, false
	}
	picked := make([]time.Duration, 0, len(l.ring))
	for i := 0; i < len(l.ring); i++ {
		s := l.newest(i)
		if len(picked) >= hedgeRecentSamples && now.Sub(s.at) > hedgeWindow {
			break
		}
		picked = append(picked, s.latency)
	}
	sort.Slice(picked, func(i, j int) bool { return picked[i] < picked[j] })
	rank := int(math.Ceil(q * float64(len(picked))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(picked) {
		rank = len(picked)
	}
	return picked[rank-1], true
}
