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
	"time"
)

const (
	// replicaStaleDecays is after how many decay time constants without a
	// sample the success latency of a replica counts as unknown again.
	replicaStaleDecays = 6
	// failuresBeforePenalty is how many attempts in a row must fail on a
	// replica before failures count against it.
	failuresBeforePenalty = 3
)

// decayFactor returns e^(-elapsed/decay).
func decayFactor(elapsed, decay time.Duration) float64 {
	if elapsed <= 0 || decay <= 0 {
		return 1
	}
	return math.Exp(-float64(elapsed) / float64(decay))
}

// replicaStats is what is known about the requests of one statistics key
// (channel and request class) on one replica:
//   - the latency of its successful attempts as a time-decayed mean: a sample
//     weighs e^(-age/decay), so the mean follows the replica within a few
//     decay times;
//   - a penalty, set when attempts fail in a row, which then decays with the
//     same time constant. Failures are kept out of
//     the mean, where a few of them among many successes would hardly show;
//   - the number of in-flight attempts the balancer selected. How long they
//     have been running does not count: a heavy request running long does not
//     make its replica look bad.
//
// It is not safe for concurrent use.
type replicaStats struct {
	// decayed sum of the latencies (seconds) and decayed count of the
	// samples, both as of sumAt
	sum   float64
	count float64
	sumAt time.Time

	penalty   time.Duration
	penaltyAt time.Time
	// failures counts the attempts that failed in a row
	failures int

	inflight int
	lastUsed time.Time
}

// succeed records a successful attempt that took latency.
func (s *replicaStats) succeed(now time.Time, latency, decay time.Duration) {
	if now.Before(s.sumAt) {
		// reported out of order: count it as of the latest sample
		now = s.sumAt
	}
	f := decayFactor(now.Sub(s.sumAt), decay)
	s.sum = s.sum*f + latency.Seconds()
	s.count = s.count*f + 1
	s.sumAt = now
	s.failures = 0
}

// fail records a failed attempt: an error, a timeout of its request, or being
// overtaken by a hedge. Like consecutive-failure outlier detection
// (Envoy consecutive_5xx, Finagle failure accrual), failures only count
// against the replica once failuresBeforePenalty attempts in a row failed,
// with no success in between: a single error during a segment handoff, or a
// heavy request timing out on a replica that keeps answering other requests,
// does not move the traffic. It returns whether the penalty applied.
func (s *replicaStats) fail(now time.Time, penalty, decay time.Duration) bool {
	s.failures++
	if s.failures < failuresBeforePenalty {
		return false
	}
	// a smaller penalty does not lower the current one
	if current := s.currentPenalty(now, decay); current > penalty {
		penalty = current
	}
	s.penalty, s.penaltyAt = penalty, now
	return true
}

// mean returns the decayed mean latency of successful attempts, and false
// without a sample within replicaStaleDecays decay times.
func (s *replicaStats) mean(now time.Time, decay time.Duration) (time.Duration, bool) {
	if s.count == 0 || now.Sub(s.sumAt) > replicaStaleDecays*decay {
		return 0, false
	}
	return time.Duration(s.sum / s.count * float64(time.Second)), true
}

// currentPenalty returns the penalty decayed to now.
func (s *replicaStats) currentPenalty(now time.Time, decay time.Duration) time.Duration {
	if s.penalty <= 0 {
		return 0
	}
	return time.Duration(float64(s.penalty) * decayFactor(now.Sub(s.penaltyAt), decay))
}

// latency returns the latency the replica is weighed with: the larger of the
// success mean and the penalty. The boolean reports whether a recent successful sample exists.
func (s *replicaStats) latency(now time.Time, decay time.Duration) (time.Duration, bool) {
	latency, known := s.mean(now, decay)
	if p := s.currentPenalty(now, decay); p > 0 {
		if p > latency {
			latency = p
		}
	}
	return latency, known
}

func (s *replicaStats) begin() { s.inflight++ }

func (s *replicaStats) end() {
	if s.inflight > 0 {
		s.inflight--
	}
}
