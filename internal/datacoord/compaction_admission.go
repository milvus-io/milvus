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
// disables limiting entirely (legacy behavior).
func (a *singleCompactionAdmitter) admit(eligible []*SegmentInfo) (admitted []*SegmentInfo, deferred int) {
	if len(eligible) == 0 {
		return nil, 0
	}
	budget := Params.DataCoordCfg.SingleCompactionRateLimitTokens.GetAsFloat()
	if budget <= 0 {
		return eligible, 0
	}
	interval := Params.DataCoordCfg.SingleCompactionRateLimitInterval.GetAsDuration(time.Second)
	if interval <= 0 {
		return eligible, 0
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

	hardCapCount := int(deferralHardCap * Params.DataCoordCfg.SingleCompactionDeltalogMaxNum.GetAsFloat())
	rest := make([]*SegmentInfo, 0, len(eligible))
	for _, segment := range eligible {
		if segmentDeltalogCount(segment) >= hardCapCount {
			// bounded deferral: always admitted, does not consume tokens
			admitted = append(admitted, segment)
			continue
		}
		rest = append(rest, segment)
	}
	sort.Slice(rest, func(i, j int) bool {
		return segmentDeletedRowsRatio(rest[i]) > segmentDeletedRowsRatio(rest[j])
	})
	for _, segment := range rest {
		if a.tokens < 1 {
			deferred++
			continue
		}
		a.tokens--
		admitted = append(admitted, segment)
	}

	if deferred > 0 {
		a.throttledRounds++
		if a.throttledRounds >= consecutiveThrottledRoundsToWarn {
			mlog.RatedWarn(context.TODO(), rate.Limit(60), "single compaction admission throttled for many consecutive rounds; "+
				"the rate limit may be below steady-state demand and deltalog backlog is growing",
				mlog.Int("deferred", deferred),
				mlog.Int("consecutiveThrottledRounds", a.throttledRounds),
				mlog.Float64("rateLimitTokens", budget))
		} else {
			mlog.RatedInfo(context.TODO(), rate.Limit(10), "single compaction admission throttled",
				mlog.Int("admitted", len(admitted)),
				mlog.Int("deferred", deferred))
		}
	} else {
		a.throttledRounds = 0
	}
	return admitted, deferred
}
