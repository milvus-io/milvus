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
	"sort"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// Admission smoothing for single (delete/expiry-triggered) compaction.
//
// Segments created in the same batch accumulate deltalogs at nearly the same
// rate, so they cross the hard trigger thresholds nearly simultaneously and
// produce a rewrite avalanche (see issue #51094). Two mechanisms de-synchronize
// and bound this:
//
//  1. Per-segment threshold jitter: each segment gets a deterministic
//     multiplier in [1, 1+J] applied to the accumulation thresholds, spreading
//     a cohort's crossing times over J x (accumulation period).
//  2. A token bucket at candidate admission: no matter how many segments become
//     eligible in one trigger round (e.g. after a threshold config change), at
//     most `rateLimitTokens` are admitted per `rateLimitInterval`. Segments
//     whose deltalog file count exceeded the configured threshold by
//     `deferralHardCap` times are always admitted so deferral stays bounded.
//
// Both knobs are refreshable; jitter=0 and tokens=0 restore legacy behavior.

// deferralHardCap bounds how far the admission limiter may defer a segment:
// once its deltalog file count exceeds hardCap x SingleCompactionDeltalogMaxNum
// it is admitted regardless of remaining tokens.
const deferralHardCap = 4.0

// consecutiveThrottledRoundsToWarn controls how many consecutive throttled
// admission rounds are tolerated before emitting a warning; sustained
// throttling means the bucket is sized below steady-state demand.
const consecutiveThrottledRoundsToWarn = 30

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

// segmentDeletedRowsRatio estimates the deleted-rows proportion of a segment,
// used to admit the dirtiest (highest reclaim value) segments first.
func segmentDeletedRowsRatio(segment *SegmentInfo) float64 {
	if segment.GetNumOfRows() <= 0 {
		return 0
	}
	var deleted int64
	for _, deltaLogs := range segment.GetDeltalogs() {
		for _, l := range deltaLogs.GetBinlogs() {
			deleted += l.GetEntriesNum()
		}
	}
	return float64(deleted) / float64(segment.GetNumOfRows())
}

func segmentDeltalogCount(segment *SegmentInfo) int {
	count := 0
	for _, deltaLogs := range segment.GetDeltalogs() {
		count += len(deltaLogs.GetBinlogs())
	}
	return count
}

// singleCompactionAdmitter is a token bucket shared by every single-compaction
// candidate producer (the legacy trigger and the single compaction policy),
// so the whole DataCoord observes one admission budget.
type singleCompactionAdmitter struct {
	mu              sync.Mutex
	tokens          float64
	lastRefill      time.Time
	throttledRounds int
	// maxDeltalogMaxNum snapshots the largest SingleCompactionDeltalogMaxNum
	// ever observed. The hard cap is derived from it monotonically so that
	// lowering the (refreshable) config cannot retroactively drop the hard cap
	// and release a whole cohort of already-accumulated segments in one round,
	// reproducing the very avalanche the bucket exists to pace.
	maxDeltalogMaxNum float64
	nowFn             func() time.Time // injectable for tests
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
//
// Candidates are split by reason into two classes:
//   - accumulation: delete / expired-entity accumulation, the avalanche-shaped
//     case. Paced dirtiest-first; segments past the deferral hard cap are always
//     admitted.
//   - retention: strict age-TTL, TTL-field expiry, and index rebuild. These are
//     not delete-driven, so under the accumulation ordering they would always
//     sort last and never reach the hard cap — i.e. be starved whenever
//     delete-driven demand saturates the bucket. They are paced too, but the two
//     classes are interleaved so a reserved share of every round goes to
//     retention and it can never be indefinitely starved.
//
// A non-positive token config disables limiting entirely (legacy behavior).
func (a *singleCompactionAdmitter) admit(ctx context.Context, accumulation, retention []*SegmentInfo) (admitted []*SegmentInfo, deferred int) {
	if len(accumulation)+len(retention) == 0 {
		return nil, 0
	}
	budget := Params.DataCoordCfg.SingleCompactionRateLimitTokens.GetAsFloat()
	if budget <= 0 {
		return append(accumulation, retention...), 0
	}
	interval := Params.DataCoordCfg.SingleCompactionRateLimitInterval.GetAsDuration(time.Second)
	if interval <= 0 {
		return append(accumulation, retention...), 0
	}
	// A sub-one positive token budget is a misconfiguration: the bucket caps at
	// `budget`, so tokens could never reach 1 and every non-hard-cap candidate
	// would be deferred forever (a soft deadlock). A slow rate is expressed via
	// a longer interval, not a fractional token count; clamp up to 1 and warn.
	if budget < 1 {
		mlog.RatedWarn(ctx, rate.Limit(60), "single compaction rateLimitTokens is a positive value below 1; "+
			"clamping to 1 to avoid a soft deadlock — use a longer rateLimitInterval to express a slower rate",
			mlog.Float64("configuredTokens", budget))
		budget = 1
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

	// Derive the hard cap from a monotonically non-decreasing snapshot of the
	// threshold, so a config decrease cannot drop the cap and release a cohort
	// of already-accumulated segments at once.
	if cur := Params.DataCoordCfg.SingleCompactionDeltalogMaxNum.GetAsFloat(); cur > a.maxDeltalogMaxNum {
		a.maxDeltalogMaxNum = cur
	}
	hardCapCount := int(deferralHardCap * a.maxDeltalogMaxNum)

	// Bounded deferral: accumulation segments past the hard cap are always
	// admitted and do not consume tokens. The rest are paced dirtiest-first.
	pacedAccum := make([]*SegmentInfo, 0, len(accumulation))
	for _, segment := range accumulation {
		if hardCapCount > 0 && segmentDeltalogCount(segment) >= hardCapCount {
			admitted = append(admitted, segment)
			continue
		}
		pacedAccum = append(pacedAccum, segment)
	}
	sort.Slice(pacedAccum, func(i, j int) bool {
		return segmentDeletedRowsRatio(pacedAccum[i]) > segmentDeletedRowsRatio(pacedAccum[j])
	})
	// Deterministic order for the retention class (stable across rounds).
	sort.Slice(retention, func(i, j int) bool {
		return retention[i].GetID() < retention[j].GetID()
	})

	// Interleave the two classes so retention gets a reserved share of tokens
	// and is never starved by a saturating accumulation stream. When one class
	// drains, the other consumes the remaining tokens.
	ai, ri := 0, 0
	takeRetention := false
	for ai < len(pacedAccum) || ri < len(retention) {
		if a.tokens < 1 {
			deferred += (len(pacedAccum) - ai) + (len(retention) - ri)
			break
		}
		if (takeRetention || ai >= len(pacedAccum)) && ri < len(retention) {
			admitted = append(admitted, retention[ri])
			ri++
		} else {
			admitted = append(admitted, pacedAccum[ai])
			ai++
		}
		a.tokens--
		takeRetention = !takeRetention
	}

	if deferred > 0 {
		a.throttledRounds++
		if a.throttledRounds >= consecutiveThrottledRoundsToWarn {
			mlog.RatedWarn(ctx, rate.Limit(60), "single compaction admission throttled for many consecutive rounds; "+
				"the rate limit may be below steady-state demand and deltalog backlog is growing",
				mlog.Int("deferred", deferred),
				mlog.Int("consecutiveThrottledRounds", a.throttledRounds),
				mlog.Float64("rateLimitTokens", budget))
		} else {
			mlog.RatedInfo(ctx, rate.Limit(10), "single compaction admission throttled",
				mlog.Int("admitted", len(admitted)),
				mlog.Int("deferred", deferred))
		}
	} else if a.tokens >= 1 {
		// Only clear the throttle counter when this round genuinely had spare
		// capacity. A lightly-loaded caller that happened to defer nothing must
		// not mask sustained throttling seen by another caller sharing the
		// bucket (which would have already drained the tokens).
		a.throttledRounds = 0
	}
	return admitted, deferred
}
