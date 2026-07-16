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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func makeAdmissionTestSegment(id int64, numRows int64, deletedRows int64, deltalogCount int) *SegmentInfo {
	binlogs := make([]*datapb.Binlog, 0, deltalogCount)
	perLog := int64(0)
	if deltalogCount > 0 {
		perLog = deletedRows / int64(deltalogCount)
	}
	for i := 0; i < deltalogCount; i++ {
		binlogs = append(binlogs, &datapb.Binlog{EntriesNum: perLog, MemorySize: 1024})
	}
	return &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:        id,
			NumOfRows: numRows,
			Deltalogs: []*datapb.FieldBinlog{{Binlogs: binlogs}},
		},
	}
}

func TestSingleCompactionThresholdMultiplier(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()

	t.Run("zero jitter restores legacy behavior", func(t *testing.T) {
		pt.Save(pt.DataCoordCfg.SingleCompactionThresholdJitter.Key, "0")
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionThresholdJitter.Key)
		assert.Equal(t, 1.0, singleCompactionThresholdMultiplier(12345))
	})

	t.Run("deterministic and within range", func(t *testing.T) {
		pt.Save(pt.DataCoordCfg.SingleCompactionThresholdJitter.Key, "0.25")
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionThresholdJitter.Key)

		seen := make(map[float64]int)
		for _, id := range []int64{1, 2, 3, 1e12, 466295849486964560} {
			m1 := singleCompactionThresholdMultiplier(id)
			m2 := singleCompactionThresholdMultiplier(id)
			assert.Equal(t, m1, m2, "multiplier must be deterministic for id %d", id)
			assert.GreaterOrEqual(t, m1, 1.0)
			assert.Less(t, m1, 1.25)
			seen[m1]++
		}
		assert.Greater(t, len(seen), 1, "different ids should spread across the jitter band")
	})
}

func TestSingleCompactionReasonAdmissionGated(t *testing.T) {
	// Only delete accumulation is paced: it has both an ordering signal and a
	// bounded-deferral hard cap measured from delete deltalogs. Expiry
	// accumulation, strict age-TTL and index-rebuild bypass admission so they
	// can never be starved behind a delete backlog.
	assert.False(t, reasonNone.admissionGated())
	assert.False(t, reasonExpiryStrict.admissionGated())
	assert.False(t, reasonExpiryAccumulation.admissionGated())
	assert.True(t, reasonTooManyDeletions.admissionGated())
	assert.False(t, reasonRebuildIndex.admissionGated())
}

func TestSingleCompactionAdmitter(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	now := time.Now()
	newAdmitter := func() *singleCompactionAdmitter {
		return &singleCompactionAdmitter{nowFn: func() time.Time { return now }}
	}
	// Pin jitter off so the hard-cap escape threshold is exactly deferralHardCap
	// (jitter itself is covered by TestSingleCompactionThresholdMultiplier).
	pt.Save(pt.DataCoordCfg.SingleCompactionThresholdJitter.Key, "0")
	defer pt.Reset(pt.DataCoordCfg.SingleCompactionThresholdJitter.Key)

	t.Run("disabled bucket admits everything", func(t *testing.T) {
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key, "0")
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key)

		a := newAdmitter()
		eligible := []*SegmentInfo{
			makeAdmissionTestSegment(1, 1000, 100, 10),
			makeAdmissionTestSegment(2, 1000, 100, 10),
		}
		admitted, gated, deferred := a.admit(eligible)
		assert.Len(t, admitted, 2)
		assert.Empty(t, gated)
		assert.Zero(t, deferred)
	})

	t.Run("limits admissions and prefers the dirtiest segments", func(t *testing.T) {
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key, "2")
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key, "60")
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key)
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key)

		a := newAdmitter()
		eligible := []*SegmentInfo{
			makeAdmissionTestSegment(1, 1000, 100, 10), // 10% deleted
			makeAdmissionTestSegment(2, 1000, 500, 10), // 50% deleted -> dirtiest
			makeAdmissionTestSegment(3, 1000, 300, 10), // 30% deleted
		}
		admitted, gated, deferred := a.admit(eligible)
		assert.Len(t, admitted, 2)
		assert.Len(t, gated, 2)
		assert.Equal(t, 1, deferred)
		assert.Equal(t, int64(2), admitted[0].GetID())
		assert.Equal(t, int64(3), admitted[1].GetID())
	})

	t.Run("hard-cap segments bypass the bucket", func(t *testing.T) {
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key, "1")
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key, "60")
		pt.Save(pt.DataCoordCfg.SingleCompactionDeltalogMaxNum.Key, "200")
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key)
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key)
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionDeltalogMaxNum.Key)

		a := newAdmitter()
		overCap := makeAdmissionTestSegment(10, 1000, 10, 900) // >= 4x200 deltalogs
		eligible := []*SegmentInfo{
			makeAdmissionTestSegment(1, 1000, 100, 10),
			makeAdmissionTestSegment(2, 1000, 500, 10),
			overCap,
		}
		admitted, gated, deferred := a.admit(eligible)
		assert.Len(t, admitted, 2) // hard-cap one + one token
		assert.Len(t, gated, 1)    // only the token-consuming one is gated
		assert.Equal(t, 1, deferred)
		ids := []int64{admitted[0].GetID(), admitted[1].GetID()}
		assert.Contains(t, ids, int64(10), "hard-cap segment must always be admitted")
		assert.NotContains(t, gated, int64(10), "hard-cap bypass must not consume a token")
	})

	t.Run("tokens refill over time", func(t *testing.T) {
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key, "1")
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key, "60")
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key)
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key)

		current := now
		a := &singleCompactionAdmitter{nowFn: func() time.Time { return current }}
		seg := func(id int64) []*SegmentInfo { return []*SegmentInfo{makeAdmissionTestSegment(id, 1000, 100, 10)} }

		admitted, _, _ := a.admit(seg(1))
		assert.Len(t, admitted, 1)
		// bucket drained: an immediate retry is deferred
		admitted, _, deferred := a.admit(seg(2))
		assert.Len(t, admitted, 0)
		assert.Equal(t, 1, deferred)
		// after one full interval the bucket refills
		current = current.Add(61 * time.Second)
		admitted, _, deferred = a.admit(seg(3))
		assert.Len(t, admitted, 1)
		assert.Zero(t, deferred)
	})

	// Fix (review): a sub-one positive token budget must not soft-deadlock.
	t.Run("sub-one positive budget still admits one per interval", func(t *testing.T) {
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key, "0.5")
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key, "60")
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key)
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key)

		a := newAdmitter()
		eligible := []*SegmentInfo{
			makeAdmissionTestSegment(1, 1000, 100, 10),
			makeAdmissionTestSegment(2, 1000, 200, 10),
			makeAdmissionTestSegment(3, 1000, 300, 10),
		}
		admitted, _, deferred := a.admit(eligible)
		assert.Len(t, admitted, 1, "a fractional budget is clamped to at least one admission")
		assert.Equal(t, 2, deferred)
	})

	// Fix (review): a segment triggered only by delete-log size (few files, low
	// row ratio, large delete bytes) must reach the bounded-deferral escape too,
	// not just file-count-triggered segments.
	t.Run("delete-size axis reaches the bounded-deferral escape", func(t *testing.T) {
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key, "1")
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key, "60")
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key)
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key)

		sizeThreshold := pt.DataCoordCfg.SingleCompactionDeltaLogMaxSize.GetAsInt64()
		// One file, ~zero row ratio, but delete bytes >> 4x the size threshold.
		sizeSeg := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:        20,
				NumOfRows: 1_000_000,
				Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{
					{EntriesNum: 10, MemorySize: 5 * sizeThreshold},
				}}},
			},
		}
		// A ratio-dirtier but small segment competes for the single token.
		ratioSeg := makeAdmissionTestSegment(21, 1000, 500, 5)

		a := newAdmitter()
		admitted, gated, _ := a.admit([]*SegmentInfo{ratioSeg, sizeSeg})
		ids := []int64{}
		for _, s := range admitted {
			ids = append(ids, s.GetID())
		}
		// sizeSeg bypasses via the size axis (no token) even though its row ratio
		// and file count would sort it last / never hit a count-only hard cap;
		// ratioSeg takes the single token.
		assert.Contains(t, ids, int64(20), "delete-size-triggered segment must reach the escape")
		assert.NotContains(t, gated, int64(20), "escape must not consume a token")
		assert.Contains(t, ids, int64(21))
		assert.Contains(t, gated, int64(21))
	})

	// Fix (review): refund returns tokens for dropped plans so backpressure does
	// not silently collapse the admission rate.
	t.Run("refund returns unused tokens", func(t *testing.T) {
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key, "2")
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key, "60")
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key)
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key)

		a := newAdmitter()
		admitted, gated, _ := a.admit([]*SegmentInfo{
			makeAdmissionTestSegment(1, 1000, 100, 10),
			makeAdmissionTestSegment(2, 1000, 100, 10),
		})
		assert.Len(t, admitted, 2)
		assert.Len(t, gated, 2)
		// bucket drained now; refund one token
		a.refund(1)
		admitted, _, deferred := a.admit([]*SegmentInfo{makeAdmissionTestSegment(3, 1000, 100, 10)})
		assert.Len(t, admitted, 1, "refunded token must be reusable")
		assert.Zero(t, deferred)
		// refund never exceeds the configured budget
		a.refund(100)
		admitted, _, _ = a.admit([]*SegmentInfo{
			makeAdmissionTestSegment(4, 1000, 100, 10),
			makeAdmissionTestSegment(5, 1000, 100, 10),
			makeAdmissionTestSegment(6, 1000, 100, 10),
		})
		assert.LessOrEqual(t, len(admitted), 2, "refund must be capped at the budget")
	})

	// Fix (review): a non-positive deltalog-max config must not turn the count
	// axis into a free bypass for every segment (which would silently disable
	// pacing). A high deltalog count that would have tripped a count-based hard
	// cap must still be routed through the bucket when maxnum <= 0.
	t.Run("non-positive deltalog-max does not disable pacing", func(t *testing.T) {
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key, "1")
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key, "60")
		pt.Save(pt.DataCoordCfg.SingleCompactionDeltalogMaxNum.Key, "0")
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key)
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key)
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionDeltalogMaxNum.Key)

		a := newAdmitter()
		// 500 files each; with maxnum<=0 the count axis is skipped, so pressure
		// stays well below the escape and the bucket still limits admissions.
		admitted, _, deferred := a.admit([]*SegmentInfo{
			makeAdmissionTestSegment(1, 1000, 100, 500),
			makeAdmissionTestSegment(2, 1000, 100, 500),
		})
		assert.Len(t, admitted, 1)
		assert.Equal(t, 1, deferred)
	})

	// Fix (review): a lightly loaded deferral-free caller must not reset the
	// sustained-throttle counter while the shared bucket is still drained.
	t.Run("throttle counter survives a deferral-free call on a drained bucket", func(t *testing.T) {
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key, "1")
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key, "60")
		pt.Save(pt.DataCoordCfg.SingleCompactionDeltalogMaxNum.Key, "200")
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key)
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key)
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionDeltalogMaxNum.Key)

		current := now
		a := &singleCompactionAdmitter{nowFn: func() time.Time { return current }}

		// Round 1 throttles and drains the bucket.
		_, _, deferred := a.admit([]*SegmentInfo{
			makeAdmissionTestSegment(1, 1000, 100, 10),
			makeAdmissionTestSegment(2, 1000, 100, 10),
		})
		assert.Equal(t, 1, deferred)
		assert.Equal(t, 1, a.throttledRounds)

		// Round 2: a hard-cap-only round defers nothing, but the bucket is still
		// drained, so the throttle counter must be preserved (not masked).
		_, _, deferred = a.admit([]*SegmentInfo{makeAdmissionTestSegment(3, 1000, 10, 900)})
		assert.Zero(t, deferred)
		assert.Equal(t, 1, a.throttledRounds, "drained bucket must not clear the throttle counter")

		// Round 3: after a full refill the bucket has genuinely recovered, so a
		// deferral-free call clears the counter.
		current = current.Add(61 * time.Second)
		_, _, deferred = a.admit([]*SegmentInfo{makeAdmissionTestSegment(4, 1000, 100, 10)})
		assert.Zero(t, deferred)
		assert.Zero(t, a.throttledRounds, "recovered bucket clears the throttle counter")
	})
}
