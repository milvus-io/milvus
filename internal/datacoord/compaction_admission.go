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

package datacoord

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

// Admission smoothing for single (delete/expiry-accumulation-triggered) compaction.
//
// Segments created in the same batch accumulate deltalogs at nearly the same
// rate, so they cross the hard trigger thresholds nearly simultaneously and
// produce a rewrite avalanche (see issue #51094). Two mechanisms de-synchronize
// and bound this:
//
//  1. Per-segment threshold jitter: each segment gets a deterministic
//     multiplier in [1, 1+J] applied to the accumulation thresholds, spreading
//     a cohort's crossing times over J x (accumulation period).
//  2. A token bucket at candidate admission: no matter how many delete-triggered
//     segments become eligible in one trigger round (e.g. after a threshold
//     config change), at most `rateLimitTokens` are admitted per
//     `rateLimitInterval`. A segment whose delete pressure has reached
//     `deferralHardCap` times a trigger threshold (on any delete axis) is always
//     admitted so deferral stays bounded; that escape is itself jittered so a
//     deferred cohort does not re-synchronize and avalanche through it at once.
//
// Only delete-accumulation candidates are routed through the admitter; both the
// dirtiest-first ordering and the bounded-deferral escape use a normalized delete
// pressure across all three delete axes (deltalog count, deleted-rows ratio,
// delete-log size), so a segment triggered by any single axis is ordered and
// bounded on that axis. Expiry accumulation, index rebuild and strict age-based
// TTL are retention / correctness obligations that must not be paced behind a
// delete backlog, so their callers bypass admission entirely and are only
// de-synchronized by jitter (see compaction_trigger.go) — the token-bucket rate
// ceiling applies to delete accumulation only.
//
// Both knobs are refreshable; jitter=0 and tokens=0 restore legacy behavior.

// deferralHardCap bounds how far the admission limiter may defer a segment:
// once its normalized delete pressure reaches deferralHardCap (4x a trigger
// threshold on some delete axis, scaled by the segment's jitter multiplier) it
// is admitted regardless of remaining tokens.
const deferralHardCap = 4.0

// consecutiveThrottledRoundsToWarn controls how many consecutive throttled
// admission rounds are tolerated before emitting a warning; sustained
// throttling means the bucket is sized below steady-state demand.
const consecutiveThrottledRoundsToWarn = 30

// reliefTokenFraction is the post-refill token level (as a fraction of the
// budget) at which the bucket is considered genuinely recovered. A deferral-free
// call only resets the sustained-throttle counter when the bucket has refilled
// to at least this level, so a lightly loaded caller cannot mask a warning while
// another caller is being throttled every round against a still-drained bucket.
const reliefTokenFraction = 0.5

// singleCompactionThresholdMultiplier returns the deterministic per-segment
// jitter multiplier in [1, 1+J]. It is a pure function of the segment ID:
// stable across restarts and nodes, re-drawn naturally when a compaction
// produces a segment with a new ID.
func singleCompactionThresholdMultiplier(segmentID int64) float64 {
	jitter := Params.DataCoordCfg.SingleCompactionThresholdJitter.GetAsFloat()
	if jitter <= 0 {
		return 1.0
	}
	// splitmix64 finalizer as a cheap uniform hash.
	x := uint64(segmentID)
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	hash01 := float64(x>>11) / float64(uint64(1)<<53)
	return 1.0 + jitter*hash01
}

// segmentDeleteCompactionPressure returns a segment's normalized dirtiness across
// all three delete trigger axes: the max of deltalog-count / maxnum,
// deleted-rows-ratio / ratioThreshold, and delete-log-size / sizeThreshold. A
// value of 1.0 means the segment just crossed a 1x trigger threshold on some
// axis; deferralHardCap (4.0) means it has reached the bounded-deferral escape.
// Using the normalized max means a segment triggered by any single axis (e.g.
// large varchar-PK deletes concentrated in a few files, with a low row ratio) is
// both ordered dirtiest-first and bounded on that axis, not only on the
// file-count axis. Axes whose threshold is non-positive are skipped, so a
// misconfigured (<= 0) threshold cannot spuriously drive pressure to the escape.
func segmentDeleteCompactionPressure(segment *SegmentInfo) float64 {
	var deltalogCount int
	var deletedRows, deleteLogSize int64
	for _, deltaLogs := range segment.GetDeltalogs() {
		for _, l := range deltaLogs.GetBinlogs() {
			deletedRows += l.GetEntriesNum()
			deleteLogSize += l.GetMemorySize()
		}
		deltalogCount += len(deltaLogs.GetBinlogs())
	}
	pressure := 0.0
	if maxNum := Params.DataCoordCfg.SingleCompactionDeltalogMaxNum.GetAsFloat(); maxNum > 0 {
		pressure = math.Max(pressure, float64(deltalogCount)/maxNum)
	}
	if rows := segment.GetNumOfRows(); rows > 0 {
		if ratioThreshold := Params.DataCoordCfg.SingleCompactionRatioThreshold.GetAsFloat(); ratioThreshold > 0 {
			pressure = math.Max(pressure, (float64(deletedRows)/float64(rows))/ratioThreshold)
		}
	}
	if sizeThreshold := float64(Params.DataCoordCfg.SingleCompactionDeltaLogMaxSize.GetAsInt64()); sizeThreshold > 0 {
		pressure = math.Max(pressure, float64(deleteLogSize)/sizeThreshold)
	}
	return pressure
}

// singleCompactionAdmitter is a token bucket shared by every single-compaction
// candidate producer (the legacy trigger and the single compaction policy), so
// the whole DataCoord observes one admission budget. The shared budget is
// intentional: the constraint being protected is the aggregate rewrite load
// against a rate-limited object store, which does not care whether a rewrite
// originated from the L1 trigger or the L2 policy. Cross-caller fairness within
// a single round is first-come, but the bucket refills every round, so no caller
// is starved across rounds.
type singleCompactionAdmitter struct {
	mu              sync.Mutex
	tokens          float64
	lastRefill      time.Time
	throttledRounds int
	nowFn           func() time.Time // injectable for tests
}

var (
	globalSingleCompactionAdmitter     *singleCompactionAdmitter
	globalSingleCompactionAdmitterOnce sync.Once
)

func getSingleCompactionAdmitter() *singleCompactionAdmitter {
	globalSingleCompactionAdmitterOnce.Do(func() {
		globalSingleCompactionAdmitter = &singleCompactionAdmitter{nowFn: time.Now}
	})
	return globalSingleCompactionAdmitter
}

// admit selects which eligible segments may be submitted this round.
// Ordering: segments past the deferral hard cap are always admitted; the rest
// are admitted dirtiest-first while tokens last. A non-positive token config
// disables limiting entirely (legacy behavior). The returned gatedIDs are the
// admitted segments that consumed a token (i.e. excluding the hard-cap bypass),
// so the caller can refund tokens for plans it ends up dropping.
func (a *singleCompactionAdmitter) admit(eligible []*SegmentInfo) (admitted []*SegmentInfo, gatedIDs map[int64]struct{}, deferred int) {
	if len(eligible) == 0 {
		return nil, nil, 0
	}
	budget := Params.DataCoordCfg.SingleCompactionRateLimitTokens.GetAsFloat()
	if budget <= 0 {
		return eligible, nil, 0
	}
	// A sub-one positive budget can never let tokens reach 1, which would defer
	// every non-hard-cap candidate forever. Treat any positive budget as at
	// least one admission per interval.
	if budget < 1 {
		budget = 1
	}
	interval := Params.DataCoordCfg.SingleCompactionRateLimitInterval.GetAsDuration(time.Second)
	if interval <= 0 {
		return eligible, nil, 0
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	now := a.nowFn()
	if a.lastRefill.IsZero() {
		a.tokens = budget
	} else {
		a.tokens += budget * now.Sub(a.lastRefill).Seconds() / interval.Seconds()
		if a.tokens > budget {
			a.tokens = budget
		}
	}
	a.lastRefill = now
	tokensAfterRefill := a.tokens

	// Precompute each segment's normalized delete pressure once; it drives both
	// the jittered hard-cap escape and the dirtiest-first ordering.
	type pressured struct {
		seg *SegmentInfo
		p   float64
	}
	rest := make([]pressured, 0, len(eligible))
	for _, segment := range eligible {
		p := segmentDeleteCompactionPressure(segment)
		// Jittered bounded-deferral escape: a segment whose pressure has reached
		// deferralHardCap x its per-segment jitter multiplier on some delete axis
		// is always admitted without consuming a token, so deferral stays bounded
		// on every axis (count, ratio, size). Jittering the escape (like the 1x
		// trigger) de-synchronizes a cohort that would otherwise all reach the 4x
		// threshold at the same instant and avalanche through the bypass together;
		// it also means a config decrease raises pressure only into a per-segment
		// spread of escape thresholds rather than releasing the cohort at once.
		if p >= deferralHardCap*singleCompactionThresholdMultiplier(segment.GetID()) {
			admitted = append(admitted, segment)
			continue
		}
		rest = append(rest, pressured{seg: segment, p: p})
	}
	sort.Slice(rest, func(i, j int) bool {
		return rest[i].p > rest[j].p
	})
	for _, r := range rest {
		if a.tokens < 1 {
			deferred++
			continue
		}
		a.tokens--
		if gatedIDs == nil {
			gatedIDs = make(map[int64]struct{})
		}
		gatedIDs[r.seg.GetID()] = struct{}{}
		admitted = append(admitted, r.seg)
	}

	if deferred > 0 {
		a.throttledRounds++
		if a.throttledRounds >= consecutiveThrottledRoundsToWarn {
			log.Ctx(context.TODO()).RatedWarn(60, "single compaction admission throttled for many consecutive rounds; "+
				"the rate limit may be below steady-state demand and deltalog backlog is growing",
				zap.Int("deferred", deferred),
				zap.Int("consecutiveThrottledRounds", a.throttledRounds),
				zap.Float64("rateLimitTokens", budget))
		} else {
			log.Ctx(context.TODO()).RatedInfo(10, "single compaction admission throttled",
				zap.Int("admitted", len(admitted)),
				zap.Int("deferred", deferred))
		}
	} else if tokensAfterRefill >= budget*reliefTokenFraction {
		// Only a genuinely recovered bucket clears the sustained-throttle
		// counter. If the bucket is still drained (another caller is exhausting
		// it every round) a lightly loaded, deferral-free caller leaves the
		// counter untouched instead of masking the warning.
		a.throttledRounds = 0
	}
	return admitted, gatedIDs, deferred
}

// refund returns n unused tokens to the bucket, capped at the current budget.
// The trigger consumes a token per admitted segment at selection time, but may
// drop already-built plans when the compaction inspector fills mid-cycle; those
// tokens are refunded so the effective admission rate is not silently collapsed
// below the configured rate by backpressure.
func (a *singleCompactionAdmitter) refund(n int) {
	if n <= 0 {
		return
	}
	budget := Params.DataCoordCfg.SingleCompactionRateLimitTokens.GetAsFloat()
	if budget <= 0 {
		return
	}
	if budget < 1 {
		budget = 1
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.tokens += float64(n)
	if a.tokens > budget {
		a.tokens = budget
	}
}
