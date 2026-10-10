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
	"fmt"
	"sort"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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
//     most `rateLimitTokens` are admitted per `rateLimitInterval`.
//
// Admission runs once per trigger round over the candidates of every
// collection, so the budget is shared fairly: picks alternate between the two
// candidate classes and rotate across collections, and the cursors persist
// between rounds, so neither a class nor a collection can be starved however
// small the budget is. Candidates whose deltalog count has grown past
// `deferralHardCap` times the configured maximum are served first, within a
// share of the round, so the deferral of any single segment stays bounded
// without a bypass that a configuration change could turn into a flood.
//
// Two producers share the bucket: the legacy trigger (global rounds only;
// collection-scoped flush signals leave single compaction to the next global
// round so a busy collection cannot drain the budget) and the single
// compaction policy (L2 segments). Each keeps its own fairness cursors.
//
// Both knobs are refreshable; jitter=0 and tokens=0 restore legacy behavior.

// deferralHardCap bounds how far the admission limiter may defer a segment:
// once its deltalog file count exceeds hardCap x SingleCompactionDeltalogMaxNum
// it moves to the head of the line and takes the next tokens before any other
// accumulation candidate.
const deferralHardCap = 4.0

// consecutiveThrottledRoundsToWarn controls how many consecutive throttled
// admission rounds are tolerated before emitting a warning; sustained
// throttling means the bucket is sized below steady-state demand.
const consecutiveThrottledRoundsToWarn = 30

// singleCompactionReason classifies why a segment is eligible for single
// compaction, so the admission limiter can pace the two shapes fairly.
type singleCompactionReason int

const (
	// singleReasonNone: segment is not eligible for single compaction.
	singleReasonNone singleCompactionReason = iota
	// singleReasonAccumulation: delete / expired-entity accumulation. This is
	// the avalanche-shaped case the admission bucket exists for; candidates are
	// paced dirtiest-first.
	singleReasonAccumulation
	// singleReasonRetention: TTL expiry (strict age, expired-entities ratio or
	// size, TTL field) and index rebuild. These are not delete-driven (deltalog
	// count / deleted-rows ratio ~= 0), so under the accumulation ordering they
	// would always sort last; they get every other pick instead.
	singleReasonRetention
)

// singleCandidate is a segment eligible for single compaction together with
// the statistics admission orders by, computed once when the candidate is
// built rather than on every comparison.
type singleCandidate struct {
	segment       *SegmentInfo
	reason        singleCompactionReason
	collectionID  int64
	deltalogCount int
	deletedRatio  float64
}

func newSingleCandidate(segment *SegmentInfo, reason singleCompactionReason) *singleCandidate {
	stats := segment.EnsureStats()
	c := &singleCandidate{
		segment:       segment,
		reason:        reason,
		collectionID:  segment.GetCollectionID(),
		deltalogCount: int(stats.GetDeltaBinlogCount()),
	}
	if rows := segment.GetNumOfRows(); rows > 0 {
		c.deletedRatio = float64(stats.GetDeleteNumRows()) / float64(rows)
	}
	return c
}

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

// singleCompactionAdmitter is a token bucket shared by every single-compaction
// candidate producer (the legacy trigger and the single compaction policy),
// so the whole DataCoord observes one admission budget.
type singleCompactionAdmitter struct {
	mu              sync.Mutex
	tokens          float64
	lastRefill      time.Time
	throttledRounds int

	// Fairness cursors, per producer and per class. They persist across
	// rounds so that a budget of a few tokens per round still rotates over
	// every class and every collection instead of restarting from the same
	// ones each time, and one producer's or class's progress does not move
	// another's cursor.
	retentionTurn  map[string]bool              // per producer: the next interleaved pick goes to retention
	lastCollection map[admissionCursorKey]int64 // per producer and class: the round-robin resumes after this collection

	nowFn func() time.Time // injectable for tests
}

type admissionCursorKey struct {
	producer string
	reason   singleCompactionReason
}

// Producers of single compaction candidates, used as metric label and cursor key.
const (
	admissionSourceTrigger = "trigger"
	admissionSourcePolicy  = "policy"
)

var (
	globalSingleCompactionAdmitter     *singleCompactionAdmitter
	globalSingleCompactionAdmitterOnce sync.Once
)

func getSingleCompactionAdmitter() *singleCompactionAdmitter {
	globalSingleCompactionAdmitterOnce.Do(func() {
		globalSingleCompactionAdmitter = newSingleCompactionAdmitter(time.Now)
	})
	return globalSingleCompactionAdmitter
}

func newSingleCompactionAdmitter(nowFn func() time.Time) *singleCompactionAdmitter {
	return &singleCompactionAdmitter{
		nowFn:          nowFn,
		retentionTurn:  make(map[string]bool),
		lastCollection: make(map[admissionCursorKey]int64),
	}
}

// admissionBudget returns the configured bucket size and refill interval, or
// ok=false when limiting is disabled.
func admissionBudget(ctx context.Context) (budget float64, interval time.Duration, ok bool) {
	budget = Params.DataCoordCfg.SingleCompactionRateLimitTokens.GetAsFloat()
	if budget <= 0 {
		return 0, 0, false
	}
	interval = Params.DataCoordCfg.SingleCompactionRateLimitInterval.GetAsDuration(time.Second)
	if interval <= 0 {
		return 0, 0, false
	}
	// A sub-one positive token budget is a misconfiguration: the bucket caps at
	// `budget`, so tokens could never reach 1 and every candidate would be
	// deferred forever. A slow rate is expressed via a longer interval, not a
	// fractional token count; clamp up to 1 and warn.
	if budget < 1 {
		mlog.RatedWarn(ctx, rate.Limit(60), "single compaction rateLimitTokens is a positive value below 1; "+
			"clamping to 1 to avoid a soft deadlock, use a longer rateLimitInterval to express a slower rate",
			mlog.Float64("configuredTokens", budget))
		budget = 1
	}
	return budget, interval, true
}

// admit selects which eligible segments may be submitted this round.
//
// producer names the caller (admissionSourceTrigger or admissionSourcePolicy)
// for its cursors and metrics. candidates are the single-compaction candidates
// of every collection the round looked at, so one call decides the whole
// round. capacity is how many tasks the caller can still enqueue; a negative
// value means unbounded. Admission never hands out more than that, so a token
// is only spent on a candidate that has a place to go; a candidate the caller
// still fails to enqueue is given back with refund.
//
// Order of service:
//  1. accumulation candidates past the deferral hard cap, most deltalogs
//     first, within half of the round so they cannot crowd out the rest;
//  2. the two classes alternately, each served round-robin across collections,
//     accumulation dirtiest-first within a collection and retention by segment
//     id. The class and collection cursors persist between rounds;
//  3. the remaining over-cap candidates.
//
// A non-positive token config disables limiting entirely (legacy behavior).
func (a *singleCompactionAdmitter) admit(ctx context.Context, producer string, candidates []*singleCandidate, capacity int) (admitted []*singleCandidate, deferred int) {
	budget, interval, limited := admissionBudget(ctx)
	if !limited {
		admitted = candidates
		if capacity >= 0 && len(candidates) > capacity {
			admitted, deferred = candidates[:capacity], len(candidates)-capacity
		}
		a.mu.Lock()
		defer a.mu.Unlock()
		a.observe(ctx, producer, 0, 0, len(admitted), deferred)
		return admitted, deferred
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if len(candidates) == 0 {
		a.observe(ctx, producer, budget, interval, 0, 0)
		return nil, 0
	}

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

	available := int(a.tokens)
	if capacity >= 0 && capacity < available {
		available = capacity
	}

	queue, urgent := a.order(producer, candidates, available)
	if available > len(queue) {
		available = len(queue)
	}
	admitted = queue[:available]
	deferred = len(queue) - available
	a.tokens -= float64(available)
	// Advance the cursors only by what was admitted: the class turn flips once
	// per interleaved pick, and each class's round-robin resumes after the
	// collection of the last candidate admitted from it.
	if interleaved := available - min(available, urgent); interleaved%2 == 1 {
		a.retentionTurn[producer] = !a.retentionTurn[producer]
	}
	for _, c := range admitted[min(available, urgent):] {
		a.lastCollection[admissionCursorKey{producer, c.reason}] = c.collectionID
	}

	a.observe(ctx, producer, budget, interval, len(admitted), deferred)
	return admitted, deferred
}

// refund returns the tokens of admitted candidates that were not submitted
// after all, so the budget only counts work that actually reached the queue.
func (a *singleCompactionAdmitter) refund(n int) {
	if n <= 0 {
		return
	}
	budget := Params.DataCoordCfg.SingleCompactionRateLimitTokens.GetAsFloat()
	if budget <= 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.tokens += float64(n)
	if a.tokens > budget {
		a.tokens = budget
	}
}

// order lays the candidates out in service order and reports how many
// over-cap candidates lead the list. Over-cap candidates take at most half of
// the available picks ahead of the interleaved classes (all of them when
// nothing else is waiting), the rest queue behind. It reads the fairness
// cursors but leaves advancing them to admit, which knows what was admitted.
func (a *singleCompactionAdmitter) order(producer string, candidates []*singleCandidate, available int) (ordered []*singleCandidate, urgentCount int) {
	hardCapCount := int(deferralHardCap * Params.DataCoordCfg.SingleCompactionDeltalogMaxNum.GetAsFloat())

	var urgent []*singleCandidate
	accumulation := make(map[int64][]*singleCandidate)
	retention := make(map[int64][]*singleCandidate)
	for _, c := range candidates {
		switch {
		case c.reason == singleReasonAccumulation && hardCapCount > 0 && c.deltalogCount >= hardCapCount:
			urgent = append(urgent, c)
		case c.reason == singleReasonAccumulation:
			accumulation[c.collectionID] = append(accumulation[c.collectionID], c)
		default:
			retention[c.collectionID] = append(retention[c.collectionID], c)
		}
	}
	sort.Slice(urgent, func(i, j int) bool {
		if urgent[i].deltalogCount != urgent[j].deltalogCount {
			return urgent[i].deltalogCount > urgent[j].deltalogCount
		}
		return urgent[i].segment.GetID() < urgent[j].segment.GetID()
	})
	for _, list := range accumulation {
		sort.Slice(list, func(i, j int) bool {
			if list[i].deletedRatio != list[j].deletedRatio {
				return list[i].deletedRatio > list[j].deletedRatio
			}
			if list[i].deltalogCount != list[j].deltalogCount {
				return list[i].deltalogCount > list[j].deltalogCount
			}
			return list[i].segment.GetID() < list[j].segment.GetID()
		})
	}
	for _, list := range retention {
		sort.Slice(list, func(i, j int) bool { return list[i].segment.GetID() < list[j].segment.GetID() })
	}

	accCursor, hasAcc := a.lastCollection[admissionCursorKey{producer, singleReasonAccumulation}]
	retCursor, hasRet := a.lastCollection[admissionCursorKey{producer, singleReasonRetention}]
	accumulationRR := newCollectionRoundRobin(accumulation, accCursor, hasAcc)
	retentionRR := newCollectionRoundRobin(retention, retCursor, hasRet)

	urgentCount = len(urgent)
	if interleavedTotal := accumulationRR.remaining() + retentionRR.remaining(); interleavedTotal > 0 {
		urgentCount = min(len(urgent), max(1, available/2))
	}
	ordered = make([]*singleCandidate, 0, len(candidates))
	ordered = append(ordered, urgent[:urgentCount]...)
	retentionTurn := a.retentionTurn[producer]
	for accumulationRR.remaining()+retentionRR.remaining() > 0 {
		var c *singleCandidate
		if (retentionTurn || accumulationRR.remaining() == 0) && retentionRR.remaining() > 0 {
			c = retentionRR.next()
		} else {
			c = accumulationRR.next()
		}
		ordered = append(ordered, c)
		retentionTurn = !retentionTurn
	}
	ordered = append(ordered, urgent[urgentCount:]...)
	return ordered, urgentCount
}

// collectionRoundRobin hands out candidates one collection at a time, in
// ascending collection id, resuming after the collection the previous round
// ended on so that a small budget still reaches every collection eventually.
type collectionRoundRobin struct {
	ids   []int64
	lists map[int64][]*singleCandidate
	pos   int
	left  int
}

func newCollectionRoundRobin(lists map[int64][]*singleCandidate, lastCollection int64, hasLast bool) *collectionRoundRobin {
	rr := &collectionRoundRobin{lists: lists}
	for id, list := range lists {
		rr.ids = append(rr.ids, id)
		rr.left += len(list)
	}
	sort.Slice(rr.ids, func(i, j int) bool { return rr.ids[i] < rr.ids[j] })
	if hasLast {
		for i, id := range rr.ids {
			if id > lastCollection {
				rr.pos = i
				break
			}
		}
	}
	return rr
}

func (rr *collectionRoundRobin) remaining() int { return rr.left }

func (rr *collectionRoundRobin) next() *singleCandidate {
	for {
		id := rr.ids[rr.pos%len(rr.ids)]
		rr.pos++
		list := rr.lists[id]
		if len(list) == 0 {
			continue
		}
		rr.lists[id] = list[1:]
		rr.left--
		return list[0]
	}
}

// observe publishes the round's outcome for one producer: the admitted and
// deferred counts as gauges, the time the deferred backlog needs to drain at
// the configured rate (zero when limiting is off, budget <= 0), and a warning
// once rounds have been throttled for long enough that the bucket is evidently
// below steady-state demand. It is called on every path, including rounds
// with nothing eligible, so the gauges never keep a stale backlog.
func (a *singleCompactionAdmitter) observe(ctx context.Context, producer string, budget float64, interval time.Duration, admitted, deferred int) {
	nodeID := fmt.Sprint(paramtable.GetNodeID())
	metrics.DataCoordSingleCompactionAdmissionNum.WithLabelValues(nodeID, producer, metrics.SingleCompactionAdmitted).Set(float64(admitted))
	metrics.DataCoordSingleCompactionAdmissionNum.WithLabelValues(nodeID, producer, metrics.SingleCompactionDeferred).Set(float64(deferred))
	drainSeconds := 0.0
	if budget > 0 {
		drainSeconds = float64(deferred) / budget * interval.Seconds()
	}
	metrics.DataCoordSingleCompactionAdmissionDrainSeconds.WithLabelValues(nodeID, producer).Set(drainSeconds)

	if deferred == 0 {
		// Only clear the counter when the round had spare capacity: a round
		// that deferred nothing because nothing was eligible says nothing
		// about the bucket size.
		if a.tokens >= 1 {
			a.throttledRounds = 0
		}
		return
	}
	a.throttledRounds++
	if a.throttledRounds >= consecutiveThrottledRoundsToWarn {
		mlog.RatedWarn(ctx, rate.Limit(60), "single compaction admission throttled for many consecutive rounds; "+
			"the rate limit is below steady-state demand and the deltalog backlog is growing",
			mlog.Int("admitted", admitted),
			mlog.Int("deferred", deferred),
			mlog.Int("consecutiveThrottledRounds", a.throttledRounds),
			mlog.Float64("rateLimitTokens", budget),
			mlog.Duration("rateLimitInterval", interval),
			mlog.Float64("estimatedDrainSeconds", drainSeconds))
		return
	}
	mlog.RatedInfo(ctx, rate.Limit(10), "single compaction admission throttled",
		mlog.Int("admitted", admitted),
		mlog.Int("deferred", deferred),
		mlog.Float64("estimatedDrainSeconds", drainSeconds))
}
