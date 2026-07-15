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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

func TestSingleCompactionAdmitter(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	now := time.Now()
	newAdmitter := func() *singleCompactionAdmitter {
		return &singleCompactionAdmitter{nowFn: func() time.Time { return now }}
	}

	t.Run("disabled bucket admits everything", func(t *testing.T) {
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key, "0")
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key)

		a := newAdmitter()
		eligible := []*SegmentInfo{
			makeAdmissionTestSegment(1, 1000, 100, 10),
			makeAdmissionTestSegment(2, 1000, 100, 10),
		}
		admitted, deferred := a.admit(context.Background(), eligible, nil)
		assert.Len(t, admitted, 2)
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
		admitted, deferred := a.admit(context.Background(), eligible, nil)
		assert.Len(t, admitted, 2)
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
		admitted, deferred := a.admit(context.Background(), eligible, nil)
		assert.Len(t, admitted, 2) // hard-cap one + one token
		assert.Equal(t, 1, deferred)
		ids := []int64{admitted[0].GetID(), admitted[1].GetID()}
		assert.Contains(t, ids, int64(10), "hard-cap segment must always be admitted")
	})

	t.Run("tokens refill over time", func(t *testing.T) {
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key, "1")
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key, "60")
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key)
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key)

		current := now
		a := &singleCompactionAdmitter{nowFn: func() time.Time { return current }}
		seg := func(id int64) []*SegmentInfo { return []*SegmentInfo{makeAdmissionTestSegment(id, 1000, 100, 10)} }

		admitted, _ := a.admit(context.Background(), seg(1), nil)
		assert.Len(t, admitted, 1)
		// bucket drained: an immediate retry is deferred
		admitted, deferred := a.admit(context.Background(), seg(2), nil)
		assert.Len(t, admitted, 0)
		assert.Equal(t, 1, deferred)
		// after one full interval the bucket refills
		current = current.Add(61 * time.Second)
		admitted, deferred = a.admit(context.Background(), seg(3), nil)
		assert.Len(t, admitted, 1)
		assert.Zero(t, deferred)
	})
}

func TestSingleCompactionAdmitterFixes(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	now := time.Now()
	newAdmitter := func() *singleCompactionAdmitter {
		return &singleCompactionAdmitter{nowFn: func() time.Time { return now }}
	}

	// retention/index candidates (deltalogCount == 0, deletedRows == 0) must not
	// be starved by a saturating stream of delete-driven accumulation candidates.
	t.Run("retention class is not starved by an accumulation flood", func(t *testing.T) {
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key, "4")
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key, "60")
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key)
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key)

		a := newAdmitter()
		accumulation := make([]*SegmentInfo, 0, 100)
		for i := int64(1); i <= 100; i++ {
			accumulation = append(accumulation, makeAdmissionTestSegment(i, 1000, 500, 10)) // all 50% dirty
		}
		retention := []*SegmentInfo{
			makeAdmissionTestSegment(5001, 1000, 0, 0), // index rebuild / age-TTL: no deletes
			makeAdmissionTestSegment(5002, 1000, 0, 0),
		}
		admitted, deferred := a.admit(context.Background(), accumulation, retention)
		assert.Len(t, admitted, 4) // budget of 4 shared across the two classes
		assert.Equal(t, 98, deferred)
		admittedIDs := make(map[int64]bool)
		for _, s := range admitted {
			admittedIDs[s.GetID()] = true
		}
		assert.True(t, admittedIDs[5001], "retention candidate must get a reserved share")
		assert.True(t, admittedIDs[5002], "retention candidate must get a reserved share")
	})

	// A positive sub-one token budget must clamp to 1 rather than deadlocking.
	t.Run("sub-one token budget clamps to 1 instead of deadlocking", func(t *testing.T) {
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key, "0.5")
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key, "60")
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key)
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key)

		a := newAdmitter()
		eligible := []*SegmentInfo{makeAdmissionTestSegment(1, 1000, 100, 10)}
		admitted, deferred := a.admit(context.Background(), eligible, nil)
		assert.Len(t, admitted, 1, "clamped budget of 1 must admit one segment, not deadlock")
		assert.Zero(t, deferred)
	})

	// Lowering the refreshable deltalog threshold must not drop the hard cap and
	// release an already-accumulated cohort in a single round.
	t.Run("hard cap does not drop when the threshold config is lowered", func(t *testing.T) {
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key, "1")
		pt.Save(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key, "60")
		pt.Save(pt.DataCoordCfg.SingleCompactionDeltalogMaxNum.Key, "200")
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitTokens.Key)
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionRateLimitInterval.Key)
		defer pt.Reset(pt.DataCoordCfg.SingleCompactionDeltalogMaxNum.Key)

		a := newAdmitter()
		// Round 1 snapshots maxDeltalogMaxNum = 200 (hard cap = 800) and drains
		// the single token on an ordinary segment.
		primed, _ := a.admit(context.Background(), []*SegmentInfo{makeAdmissionTestSegment(1, 1000, 100, 10)}, nil)
		assert.Len(t, primed, 1)

		// Operator lowers the threshold 200 -> 50. A naive hard cap would drop to
		// 4*50 = 200, so a 300-deltalog cohort segment would bypass the drained
		// bucket. With the monotonic snapshot the cap stays 800, so the segment
		// remains token-gated and is deferred (bucket is empty this round).
		pt.Save(pt.DataCoordCfg.SingleCompactionDeltalogMaxNum.Key, "50")
		cohort := makeAdmissionTestSegment(2, 1000, 100, 300) // 4*50 <= 300 < 4*200
		admitted, deferred := a.admit(context.Background(), []*SegmentInfo{cohort}, nil)
		assert.Empty(t, admitted, "lowering the config must not let the cohort bypass the drained bucket")
		assert.Equal(t, 1, deferred)
	})
}
