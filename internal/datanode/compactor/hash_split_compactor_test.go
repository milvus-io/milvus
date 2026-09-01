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
			name:   "fewer than two targets",
			plan:   hashSplitPlan(doublingPlanTargets()[:1], 1, idRange),
			errStr: "at least two targets",
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

func TestHashSplitCompactorAcceptsARehashPlan(t *testing.T) {
	// A rehash to M shards hands the compactor M targets, not two. preCompact
	// rejected anything but two, so every rewrite plan of a real rehash failed
	// on the datanode — which unit tests missed because they build the
	// partitioner directly and never run preCompact.
	targets := []*datapb.SplitShardTaskTarget{
		{Vchannel: "t0", Buckets: []uint64{0}},
		{Vchannel: "t1", Buckets: []uint64{1}},
		{Vchannel: "t2", Buckets: []uint64{2}},
	}
	plan := hashSplitPlan(targets, 1, &datapb.IDRange{Begin: 10000, End: 10100})
	task := NewHashSplitCompactionTask(context.Background(), nil, plan, compaction.GenParams())
	assert.NoError(t, task.preCompact(), "an M-target rehash plan must be accepted")
}
