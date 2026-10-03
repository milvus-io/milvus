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

package compactor

import (
	"context"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func hashSplitPlan(targets []*datapb.SplitShardTaskTarget, inputs int, idRange *datapb.IDRange) *datapb.CompactionPlan {
	binlogs := make([]*datapb.CompactionSegmentBinlogs, 0, inputs)
	for i := range inputs {
		binlogs = append(binlogs, &datapb.CompactionSegmentBinlogs{
			SegmentID:    int64(1000 + i),
			CollectionID: 1,
			PartitionID:  2,
		})
	}
	return &datapb.CompactionPlan{
		PlanID:           42,
		Type:             datapb.CompactionType_HashSplitCompaction,
		Channel:          "by-dev-rootcoord-dml_0_1v0",
		SegmentBinlogs:   binlogs,
		HashSplitTargets: targets,
		// The residues above are taken against this; a plan without it cannot be
		// partitioned at all.
		HashSplitModulus:       uint64(len(targets)),
		PreAllocatedSegmentIDs: idRange,
		PreAllocatedLogIDs:     &datapb.IDRange{Begin: 20000, End: 30000},
		TotalRows:              100,
	}
}

func doublingPlanTargets() []*datapb.SplitShardTaskTarget {
	return []*datapb.SplitShardTaskTarget{
		{Vchannel: "by-dev-rootcoord-dml_1_1v0", Buckets: []uint64{0}},
		{Vchannel: "by-dev-rootcoord-dml_2_1v0", Buckets: []uint64{1}},
	}
}

func TestHashSplitCompactorRejectsMalformedPlans(t *testing.T) {
	idRange := &datapb.IDRange{Begin: 10000, End: 10100}
	cases := []struct {
		name   string
		plan   *datapb.CompactionPlan
		errStr string
	}{
		{
			name:   "more than one input segment",
			plan:   hashSplitPlan(doublingPlanTargets(), 2, idRange),
			errStr: "exactly one input segment",
		},
		{
			name:   "no input segment",
			plan:   hashSplitPlan(doublingPlanTargets(), 0, idRange),
			errStr: "got 0 among 0 plan segments",
		},
		{
			name:   "fewer than two targets",
			plan:   hashSplitPlan(doublingPlanTargets()[:1], 1, idRange),
			errStr: "exactly two targets",
		},
		{
			// A shard split has exactly two targets; there is no rehash.
			name: "more than two targets",
			plan: hashSplitPlan(append(doublingPlanTargets(),
				&datapb.SplitShardTaskTarget{Vchannel: "by-dev-rootcoord-dml_3_1v0", Buckets: []uint64{2}}), 1, idRange),
			errStr: "exactly two targets",
		},
		{
			name:   "no pre-allocated ids",
			plan:   hashSplitPlan(doublingPlanTargets(), 1, nil),
			errStr: "pre-allocated segment id range",
		},
		{
			name:   "zero pre-allocated begin",
			plan:   hashSplitPlan(doublingPlanTargets(), 1, &datapb.IDRange{Begin: 0, End: 100}),
			errStr: "pre-allocated segment id range",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := NewHashSplitCompactionTask(context.Background(), nil, tc.plan, compaction.GenParams())
			err := task.preCompact()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errStr)
		})
	}
}

// withSourceLevelZero appends n L0 delete-source segments to a plan, the way
// datacoord attaches the source channel's pending deletes to a rewrite.
func withSourceLevelZero(plan *datapb.CompactionPlan, n int) *datapb.CompactionPlan {
	for i := range n {
		plan.SegmentBinlogs = append(plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			SegmentID:    int64(2000 + i),
			CollectionID: 1,
			PartitionID:  2,
			Level:        datapb.SegmentLevel_L0,
			Deltalogs:    []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: "delta/l0"}}}},
		})
	}
	return plan
}

func TestHashSplitCompactorSeparatesTheInputFromItsDeleteSources(t *testing.T) {
	idRange := &datapb.IDRange{Begin: 10000, End: 10100}

	t.Run("one input and its L0 delete sources", func(t *testing.T) {
		plan := withSourceLevelZero(hashSplitPlan(doublingPlanTargets(), 1, idRange), 2)
		task := NewHashSplitCompactionTask(context.Background(), nil, plan, compaction.GenParams())
		require.NoError(t, task.preCompact())
		require.NotNil(t, task.input)
		assert.Equal(t, int64(1000), task.input.GetSegmentID())
		assert.Equal(t, []int64{2000, 2001}, lo.Map(task.deleteSources,
			func(seg *datapb.CompactionSegmentBinlogs, _ int) int64 { return seg.GetSegmentID() }))
		// The L0s must not be mistaken for data: the scope still comes from the
		// one segment that is actually rewritten.
		assert.Equal(t, int64(2), task.partitionID)
	})

	t.Run("the L0s do not hide a second input", func(t *testing.T) {
		plan := withSourceLevelZero(hashSplitPlan(doublingPlanTargets(), 2, idRange), 1)
		task := NewHashSplitCompactionTask(context.Background(), nil, plan, compaction.GenParams())
		err := task.preCompact()
		require.Error(t, err)
		// The count is over the whole plan, not "everything seen so far that was
		// not an L0": the L0 entries follow the inputs.
		assert.Contains(t, err.Error(), "got 2 among 3 plan segments")
	})

	t.Run("delete sources alone are not a rewrite", func(t *testing.T) {
		plan := withSourceLevelZero(hashSplitPlan(doublingPlanTargets(), 0, idRange), 2)
		task := NewHashSplitCompactionTask(context.Background(), nil, plan, compaction.GenParams())
		err := task.preCompact()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "got 0 among 2 plan segments")
	})

	t.Run("a second preCompact does not accumulate delete sources", func(t *testing.T) {
		plan := withSourceLevelZero(hashSplitPlan(doublingPlanTargets(), 1, idRange), 2)
		task := NewHashSplitCompactionTask(context.Background(), nil, plan, compaction.GenParams())
		require.NoError(t, task.preCompact())
		require.NoError(t, task.preCompact())
		assert.Len(t, task.deleteSources, 2)
	})
}

func TestHashSplitCompactorPreCompactCachesScope(t *testing.T) {
	plan := hashSplitPlan(doublingPlanTargets(), 1, &datapb.IDRange{Begin: 10000, End: 10100})
	task := NewHashSplitCompactionTask(context.Background(), nil, plan, compaction.GenParams())
	require.NoError(t, task.preCompact())
	assert.Equal(t, int64(1), task.collectionID)
	assert.Equal(t, int64(2), task.partitionID)
	assert.Equal(t, int64(100), task.maxRows)
}

func TestHashSplitCompactorReportsItsIdentity(t *testing.T) {
	plan := hashSplitPlan(doublingPlanTargets(), 1, &datapb.IDRange{Begin: 10000, End: 10100})
	task := NewHashSplitCompactionTask(context.Background(), nil, plan, compaction.GenParams())
	assert.Equal(t, typeutil.UniqueID(42), task.GetPlanID())
	assert.Equal(t, datapb.CompactionType_HashSplitCompaction, task.GetCompactionType())
	// The plan runs on the SOURCE channel: that is where its input segment
	// lives. Its outputs are attributed to the targets by their writers.
	assert.Equal(t, "by-dev-rootcoord-dml_0_1v0", task.GetChannelName())
	assert.Equal(t, typeutil.UniqueID(1), task.GetCollection())
}

func TestHashSplitCompactorSplitsIDRangePerTarget(t *testing.T) {
	// Each target writer must draw output segment ids from its own sub-range,
	// or the two writers could mint the same segment id.
	plan := hashSplitPlan(doublingPlanTargets(), 1, &datapb.IDRange{Begin: 10000, End: 10100})
	task := NewHashSplitCompactionTask(context.Background(), nil, plan, compaction.GenParams())
	require.NoError(t, task.preCompact())

	partitioner, err := newHashSplitPartitioner(plan.GetHashSplitModulus(), plan.GetHashSplitTargets())
	require.NoError(t, err)

	writers, err := task.newTargetWriters(context.Background(), partitioner)
	require.NoError(t, err)
	defer func() {
		for _, w := range writers {
			w.Close()
		}
	}()
	require.Len(t, writers, 2)

	// Each writer is bound to its own target vchannel, which is what puts its
	// output segments on the right shard.
	assert.Equal(t, "by-dev-rootcoord-dml_1_1v0", writers[0].channel)
	assert.Equal(t, "by-dev-rootcoord-dml_2_1v0", writers[1].channel)
}

func TestHashSplitCompactorRejectsTooSmallIDRange(t *testing.T) {
	// One id for two targets cannot be divided; failing here beats minting
	// colliding segment ids.
	plan := hashSplitPlan(doublingPlanTargets(), 1, &datapb.IDRange{Begin: 10000, End: 10001})
	task := NewHashSplitCompactionTask(context.Background(), nil, plan, compaction.GenParams())
	require.NoError(t, task.preCompact())

	partitioner, err := newHashSplitPartitioner(plan.GetHashSplitModulus(), plan.GetHashSplitTargets())
	require.NoError(t, err)

	_, err = task.newTargetWriters(context.Background(), partitioner)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too small")
}
