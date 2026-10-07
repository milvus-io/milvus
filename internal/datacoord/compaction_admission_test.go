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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func withAdmissionParams(t *testing.T, jitter, tokens, interval, deltalogMaxNum string) {
	items := map[*paramtable.ParamItem]string{
		&Params.DataCoordCfg.SingleCompactionThresholdJitter:   jitter,
		&Params.DataCoordCfg.SingleCompactionRateLimitTokens:   tokens,
		&Params.DataCoordCfg.SingleCompactionRateLimitInterval: interval,
		&Params.DataCoordCfg.SingleCompactionDeltalogMaxNum:    deltalogMaxNum,
	}
	saved := make(map[string]string, len(items))
	for item, v := range items {
		saved[item.Key] = item.GetValue()
		Params.Save(item.Key, v)
	}
	t.Cleanup(func() {
		for k, v := range saved {
			Params.Save(k, v)
		}
	})
}

// admissionSegment builds a candidate with the given deltalog count and
// deleted-rows ratio; the stats are what admission orders by.
func admissionSegment(id, collectionID int64, deltalogs int, deletedRatio float64, reason singleCompactionReason) *singleCandidate {
	const rows = 10000
	binlogs := make([]*datapb.Binlog, 0, deltalogs)
	perLog := int64(deletedRatio * rows / float64(max(deltalogs, 1)))
	for i := 0; i < deltalogs; i++ {
		binlogs = append(binlogs, &datapb.Binlog{EntriesNum: perLog, LogSize: 10})
	}
	segment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:           id,
		CollectionID: collectionID,
		NumOfRows:    rows,
		Deltalogs:    []*datapb.FieldBinlog{{Binlogs: binlogs}},
	}}
	return newSingleCandidate(segment, reason)
}

func ids(cands []*singleCandidate) []int64 {
	out := make([]int64, 0, len(cands))
	for _, c := range cands {
		out = append(out, c.segment.GetID())
	}
	return out
}

func TestSingleCompactionThresholdMultiplier(t *testing.T) {
	paramtable.Init()
	withAdmissionParams(t, "0.25", "0", "60", "200")

	for id := int64(1); id < 2000; id++ {
		m := singleCompactionThresholdMultiplier(id)
		assert.GreaterOrEqual(t, m, 1.0)
		assert.LessOrEqual(t, m, 1.25)
		assert.Equal(t, m, singleCompactionThresholdMultiplier(id), "deterministic per segment")
	}
	assert.NotEqual(t, singleCompactionThresholdMultiplier(1), singleCompactionThresholdMultiplier(2))

	Params.Save(Params.DataCoordCfg.SingleCompactionThresholdJitter.Key, "0")
	assert.Equal(t, 1.0, singleCompactionThresholdMultiplier(42))
}

func TestSingleCandidateStatsComputedOnce(t *testing.T) {
	paramtable.Init()
	c := admissionSegment(1, 1, 20, 0.3, singleReasonAccumulation)
	assert.Equal(t, 20, c.deltalogCount)
	assert.InDelta(t, 0.3, c.deletedRatio, 0.01)
	assert.Equal(t, int64(1), c.collectionID)
}

func TestAdmissionDisabledAdmitsEverything(t *testing.T) {
	paramtable.Init()
	withAdmissionParams(t, "0", "0", "60", "200")
	a := newSingleCompactionAdmitter(time.Now)

	cands := []*singleCandidate{
		admissionSegment(1, 1, 1, 0.1, singleReasonAccumulation),
		admissionSegment(2, 1, 1, 0.1, singleReasonRetention),
	}
	admitted, deferred := a.admit(context.Background(), cands, -1)
	assert.Len(t, admitted, 2)
	assert.Zero(t, deferred)

	// Still bounded by the room left in the inspector.
	admitted, deferred = a.admit(context.Background(), cands, 1)
	assert.Len(t, admitted, 1)
	assert.Equal(t, 1, deferred)
}

func TestAdmissionDirtiestFirstAndTokenRefill(t *testing.T) {
	paramtable.Init()
	withAdmissionParams(t, "0", "2", "60", "200")
	now := time.Unix(1000, 0)
	a := newSingleCompactionAdmitter(func() time.Time { return now })

	cands := []*singleCandidate{
		admissionSegment(1, 1, 5, 0.1, singleReasonAccumulation),
		admissionSegment(2, 1, 5, 0.5, singleReasonAccumulation),
		admissionSegment(3, 1, 5, 0.3, singleReasonAccumulation),
	}
	admitted, deferred := a.admit(context.Background(), cands, -1)
	assert.Equal(t, []int64{2, 3}, ids(admitted))
	assert.Equal(t, 1, deferred)

	// Nothing refilled yet.
	admitted, deferred = a.admit(context.Background(), cands[:1], -1)
	assert.Empty(t, admitted)
	assert.Equal(t, 1, deferred)

	// Half an interval later one token is back.
	now = now.Add(30 * time.Second)
	admitted, _ = a.admit(context.Background(), cands[:1], -1)
	assert.Equal(t, []int64{1}, ids(admitted))
}

// Segments past the deferral hard cap are served before any other
// accumulation candidate, most deltalogs first, but still through the bucket:
// a configuration change cannot release a whole cohort in one round.
func TestAdmissionHardCapGoesFirstButStaysPaced(t *testing.T) {
	paramtable.Init()
	withAdmissionParams(t, "0", "2", "60", "10")
	a := newSingleCompactionAdmitter(time.Now)

	cands := []*singleCandidate{
		admissionSegment(1, 1, 5, 0.9, singleReasonAccumulation),  // dirtiest, under the cap
		admissionSegment(2, 1, 40, 0.0, singleReasonAccumulation), // 4x the maximum
		admissionSegment(3, 1, 60, 0.0, singleReasonAccumulation), // 6x the maximum
		admissionSegment(4, 1, 45, 0.0, singleReasonAccumulation), // 4.5x the maximum
	}
	admitted, deferred := a.admit(context.Background(), cands, -1)
	assert.Equal(t, []int64{3, 4}, ids(admitted))
	assert.Equal(t, 2, deferred, "the third over-cap segment waits for the next tokens like everyone else")
}

// A delete-heavy workload cannot starve retention (TTL / index rebuild)
// candidates: picks alternate between the classes, and the class cursor
// persists across rounds so a single token per round still reaches both.
func TestAdmissionRetentionNotStarved(t *testing.T) {
	paramtable.Init()
	withAdmissionParams(t, "0", "1", "60", "200")
	now := time.Unix(1000, 0)
	a := newSingleCompactionAdmitter(func() time.Time { return now })

	var cands []*singleCandidate
	for i := int64(1); i <= 20; i++ {
		cands = append(cands, admissionSegment(i, 1, 5, 0.5, singleReasonAccumulation))
	}
	retention := admissionSegment(100, 1, 0, 0, singleReasonRetention)
	cands = append(cands, retention)

	var served []int64
	for round := 0; round < 4; round++ {
		admitted, _ := a.admit(context.Background(), cands, -1)
		require.Len(t, admitted, 1)
		served = append(served, admitted[0].segment.GetID())
		now = now.Add(60 * time.Second)
	}
	assert.Contains(t, served, int64(100), "retention got a token within two rounds: %v", served)
	assert.Equal(t, int64(100), served[1])
}

// The budget rotates across collections and the collection cursor persists
// across rounds, so the first collection in the walk cannot drain it.
func TestAdmissionRoundRobinAcrossCollections(t *testing.T) {
	paramtable.Init()
	withAdmissionParams(t, "0", "2", "60", "200")
	now := time.Unix(1000, 0)
	a := newSingleCompactionAdmitter(func() time.Time { return now })

	var cands []*singleCandidate
	for coll := int64(1); coll <= 3; coll++ {
		for i := int64(0); i < 3; i++ {
			cands = append(cands, admissionSegment(coll*10+i, coll, 5, 0.5-float64(i)*0.1, singleReasonAccumulation))
		}
	}

	collectionsOf := func(cs []*singleCandidate) []int64 {
		out := make([]int64, 0, len(cs))
		for _, c := range cs {
			out = append(out, c.collectionID)
		}
		return out
	}
	admitted, deferred := a.admit(context.Background(), cands, -1)
	assert.Equal(t, []int64{1, 2}, collectionsOf(admitted))
	assert.Equal(t, 7, deferred)

	now = now.Add(60 * time.Second)
	admitted, _ = a.admit(context.Background(), cands, -1)
	assert.Equal(t, []int64{3, 1}, collectionsOf(admitted), "resumes after the collection the last round ended on")

	// Within a collection the dirtiest segment goes first.
	assert.Equal(t, int64(30), admitted[0].segment.GetID())
}

// Admission never hands out more than the inspector can take, and tokens of
// admitted segments that never reached the queue are given back.
func TestAdmissionBoundedByCapacityAndRefund(t *testing.T) {
	paramtable.Init()
	withAdmissionParams(t, "0", "10", "60", "200")
	a := newSingleCompactionAdmitter(time.Now)

	var cands []*singleCandidate
	for i := int64(1); i <= 10; i++ {
		cands = append(cands, admissionSegment(i, 1, 5, 0.5, singleReasonAccumulation))
	}
	admitted, deferred := a.admit(context.Background(), cands, 3)
	assert.Len(t, admitted, 3)
	assert.Equal(t, 7, deferred)
	assert.Equal(t, 7.0, a.tokens)

	round := newSingleAdmission(admitted, deferred)
	round.enqueued(admitted[0].segment.GetID(), 999) // one reached the queue, with an unrelated segment
	round.settle(a)
	assert.Equal(t, 9.0, a.tokens, "two admitted segments did not reach the queue and were refunded")

	// A nil admission (force signal) allows everything and settles to nothing.
	var none *singleAdmission
	assert.True(t, none.allows(1))
	none.settle(a)
	assert.Equal(t, 9.0, a.tokens)
}

func TestAdmissionMetricsAndThrottleWarning(t *testing.T) {
	paramtable.Init()
	withAdmissionParams(t, "0", "2", "60", "200")
	now := time.Unix(1000, 0)
	a := newSingleCompactionAdmitter(func() time.Time { return now })

	var cands []*singleCandidate
	for i := int64(1); i <= 5; i++ {
		cands = append(cands, admissionSegment(i, 1, 5, 0.5, singleReasonAccumulation))
	}
	nodeID := fmt.Sprint(paramtable.GetNodeID())
	for round := 0; round < consecutiveThrottledRoundsToWarn+1; round++ {
		a.admit(context.Background(), cands, -1)
		now = now.Add(60 * time.Second)
	}
	assert.Equal(t, 2.0, testutil.ToFloat64(metrics.DataCoordSingleCompactionAdmissionNum.WithLabelValues(nodeID, metrics.SingleCompactionAdmitted)))
	assert.Equal(t, 3.0, testutil.ToFloat64(metrics.DataCoordSingleCompactionAdmissionNum.WithLabelValues(nodeID, metrics.SingleCompactionDeferred)))
	// 3 deferred at 2 tokens per 60 seconds.
	assert.Equal(t, 90.0, testutil.ToFloat64(metrics.DataCoordSingleCompactionAdmissionDrainSeconds.WithLabelValues(nodeID)))
	// Counted per round, not per collection: one call per round above.
	assert.Equal(t, consecutiveThrottledRoundsToWarn+1, a.throttledRounds)

	// A round with spare capacity clears the counter.
	a.admit(context.Background(), cands[:1], -1)
	assert.Equal(t, 0, a.throttledRounds)
}

func TestAdmissionFractionalBudgetClampsToOne(t *testing.T) {
	paramtable.Init()
	withAdmissionParams(t, "0", "0.5", "60", "200")
	a := newSingleCompactionAdmitter(time.Now)
	admitted, deferred := a.admit(context.Background(), []*singleCandidate{
		admissionSegment(1, 1, 5, 0.5, singleReasonAccumulation),
		admissionSegment(2, 1, 5, 0.5, singleReasonAccumulation),
	}, -1)
	assert.Len(t, admitted, 1)
	assert.Equal(t, 1, deferred)
}
