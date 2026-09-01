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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	hsSource = "by-dev-rootcoord-dml_0_77v0"
	hsTgt0   = "by-dev-rootcoord-dml_1_77v1"
	hsTgt1   = "by-dev-rootcoord-dml_2_77v2"
)

// newHashSplitMutationMeta builds a meta holding one flushed source segment on
// hsSource, the input of a rewrite.
func newHashSplitMutationMeta(t *testing.T) *meta {
	m := &meta{
		ctx:         context.Background(),
		catalog:     &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		channelCPs:  newChannelCps(),
	}
	m.segments.SetSegment(500, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:            500,
		CollectionID:  77,
		PartitionID:   7,
		InsertChannel: hsSource,
		State:         commonpb.SegmentState_Flushed,
		NumOfRows:     100,
	}})
	return m
}

func hashSplitRewriteTask() *datapb.CompactionTask {
	return &datapb.CompactionTask{
		PlanID:        900,
		CollectionID:  77,
		Type:          datapb.CompactionType_HashSplitCompaction,
		Channel:       hsSource,
		InputSegments: []int64{500},
		StartTime:     1,
		Schema:        &schemapb.CollectionSchema{Name: "c", Version: 3},
		HashSplitTargets: []*datapb.SplitShardTaskTarget{
			{Vchannel: hsTgt0, Buckets: []uint64{0}},
			{Vchannel: hsTgt1, Buckets: []uint64{1}},
		},
	}
}

func TestHashSplitMutationPutsOutputsOnTheirTargetChannels(t *testing.T) {
	// The whole point of the rewrite: each output belongs to the target its
	// writer was bound to. The mix mutation stamps the PLAN's channel, which
	// here is the source — using it would put every rewritten row back on the
	// shard the split is moving it off.
	m := newHashSplitMutationMeta(t)
	task := hashSplitRewriteTask()

	outputs, mutation, err := m.CompleteCompactionMutation(context.Background(), task,
		&datapb.CompactionPlanResult{
			PlanID:  900,
			Channel: hsSource, // the plan-level channel is still the source
			Segments: []*datapb.CompactionSegment{
				{SegmentID: 601, NumOfRows: 60, Channel: hsTgt0},
				{SegmentID: 602, NumOfRows: 40, Channel: hsTgt1},
			},
		})
	require.NoError(t, err)
	require.NotNil(t, mutation)
	require.Len(t, outputs, 2)

	byID := map[int64]*SegmentInfo{}
	for _, o := range outputs {
		byID[o.GetID()] = o
	}
	assert.Equal(t, hsTgt0, byID[601].GetInsertChannel())
	assert.Equal(t, hsTgt1, byID[602].GetInsertChannel())

	// And they are readable back from meta on their own channels, which is how
	// the child delegators find them.
	assert.Len(t, m.GetSegmentsByChannel(hsTgt0), 1)
	assert.Len(t, m.GetSegmentsByChannel(hsTgt1), 1)
}

func TestHashSplitMutationKeepsTheSourceSegment(t *testing.T) {
	// Mix retires its inputs as soon as the outputs land, because the outputs
	// replace them. Here the source segment is what the source delegator is
	// still serving through the fronting window — retiring it now would empty
	// the shard that is still answering every read.
	m := newHashSplitMutationMeta(t)

	_, _, err := m.CompleteCompactionMutation(context.Background(), hashSplitRewriteTask(),
		&datapb.CompactionPlanResult{
			PlanID:   900,
			Segments: []*datapb.CompactionSegment{{SegmentID: 601, NumOfRows: 100, Channel: hsTgt0}},
		})
	require.NoError(t, err)

	source := m.GetSegment(context.Background(), 500)
	require.NotNil(t, source, "the source segment must still exist")
	assert.Equal(t, commonpb.SegmentState_Flushed, source.GetState(),
		"the source must stay serving until adoption drops it")
	assert.False(t, source.GetCompacted(), "the source is not replaced by the rewrite")
	assert.Len(t, m.GetSegmentsByChannel(hsSource), 1)
}

func TestHashSplitMutationRecordsLineageForCompletion(t *testing.T) {
	// The split task judges a source rewritten by finding it in an output's
	// CompactionFrom on a target channel. Losing the lineage would make the
	// task retry the same segment forever.
	m := newHashSplitMutationMeta(t)

	outputs, _, err := m.CompleteCompactionMutation(context.Background(), hashSplitRewriteTask(),
		&datapb.CompactionPlanResult{
			PlanID:   900,
			Segments: []*datapb.CompactionSegment{{SegmentID: 601, NumOfRows: 100, Channel: hsTgt0}},
		})
	require.NoError(t, err)
	require.Len(t, outputs, 1)
	assert.Equal(t, []int64{500}, outputs[0].GetCompactionFrom())
}

func TestHashSplitMutationRejectsAnOutputOffTarget(t *testing.T) {
	// A result naming a channel the plan never targeted would put rows on a
	// shard that does not own their keys — worse than failing the plan.
	m := newHashSplitMutationMeta(t)

	_, _, err := m.CompleteCompactionMutation(context.Background(), hashSplitRewriteTask(),
		&datapb.CompactionPlanResult{
			PlanID:   900,
			Segments: []*datapb.CompactionSegment{{SegmentID: 601, NumOfRows: 100, Channel: "somewhere-else"}},
		})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not one of the plan's targets")
}

func TestHashSplitMutationRejectsAnOutputWithNoChannel(t *testing.T) {
	m := newHashSplitMutationMeta(t)

	_, _, err := m.CompleteCompactionMutation(context.Background(), hashSplitRewriteTask(),
		&datapb.CompactionPlanResult{
			PlanID:   900,
			Segments: []*datapb.CompactionSegment{{SegmentID: 601, NumOfRows: 100}},
		})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "carries no channel")
}

func TestHashSplitMutationPublishesAnEmptyHalf(t *testing.T) {
	// A source whose keys all fall on one side produces nothing for the other.
	// That output must still be published — the split counts committed outputs,
	// so dropping it silently would stall the task.
	m := newHashSplitMutationMeta(t)

	outputs, _, err := m.CompleteCompactionMutation(context.Background(), hashSplitRewriteTask(),
		&datapb.CompactionPlanResult{
			PlanID: 900,
			Segments: []*datapb.CompactionSegment{
				{SegmentID: 601, NumOfRows: 100, Channel: hsTgt0},
				{SegmentID: 602, NumOfRows: 0, Channel: hsTgt1},
			},
		})
	require.NoError(t, err)
	require.Len(t, outputs, 2)

	byID := map[int64]*SegmentInfo{}
	for _, o := range outputs {
		byID[o.GetID()] = o
	}
	assert.Equal(t, commonpb.SegmentState_Flushed, byID[601].GetState())
	assert.Equal(t, commonpb.SegmentState_Dropped, byID[602].GetState(),
		"an empty half is published as Dropped, not omitted")
	assert.Equal(t, []int64{500}, byID[602].GetCompactionFrom())
}

func TestHashSplitMutationIsReachableFromTheDispatchSwitch(t *testing.T) {
	// Before this existed, CompleteCompactionMutation had no HashSplitCompaction
	// case and fell through to "illegal compaction type" — so every finished
	// rewrite was refused at the meta write and the task stalled in Rewriting
	// forever. This pins the dispatch, not just the function.
	m := newHashSplitMutationMeta(t)
	outputs, _, err := m.CompleteCompactionMutation(context.Background(), hashSplitRewriteTask(),
		&datapb.CompactionPlanResult{
			PlanID:   900,
			Segments: []*datapb.CompactionSegment{{SegmentID: 601, NumOfRows: 100, Channel: hsTgt0}},
		})
	require.NoError(t, err, "the dispatch must reach the hash-split mutation")
	require.Len(t, outputs, 1)

	// An unknown type still falls through, so the test above is not vacuous.
	bogus := hashSplitRewriteTask()
	bogus.Type = datapb.CompactionType_UndefinedCompaction
	_, _, err = m.CompleteCompactionMutation(context.Background(), bogus,
		&datapb.CompactionPlanResult{PlanID: 901})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "illegal compaction type")
}

func TestRetireSplitSourceSegments(t *testing.T) {
	// Adoption's last step. The rewrite deliberately keeps its inputs alive so
	// the source delegator can serve the whole key space through the fronting
	// window; once the targets own the routing those rows exist twice, and the
	// source's copy has to go or it is an unreclaimable duplicate of the whole
	// collection.
	m := newHashSplitMutationMeta(t)
	m.segments.SetSegment(501, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 501, CollectionID: 77, PartitionID: 7, InsertChannel: hsSource,
		State: commonpb.SegmentState_Flushed, NumOfRows: 40,
	}})
	// an output of the rewrite, on a target channel — must survive.
	m.segments.SetSegment(601, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 601, CollectionID: 77, PartitionID: 7, InsertChannel: hsTgt0,
		State: commonpb.SegmentState_Flushed, NumOfRows: 60,
	}})

	retired, err := m.RetireSplitSourceSegments(context.Background(), []string{hsSource})
	require.NoError(t, err)
	assert.Equal(t, 2, retired)

	assert.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(context.Background(), 500).GetState())
	assert.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(context.Background(), 501).GetState())
	assert.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(context.Background(), 601).GetState(),
		"a rewrite output must not be retired with its source")
	assert.Empty(t, m.GetSegmentsByChannel(hsSource))
	assert.Len(t, m.GetSegmentsByChannel(hsTgt0), 1)
}

func TestRetireSplitSourceSegmentsIsIdempotent(t *testing.T) {
	// The routing commit and this call are two writes; a crash between them and
	// the retry that follows must converge rather than double-count.
	m := newHashSplitMutationMeta(t)

	first, err := m.RetireSplitSourceSegments(context.Background(), []string{hsSource})
	require.NoError(t, err)
	assert.Equal(t, 1, first)

	second, err := m.RetireSplitSourceSegments(context.Background(), []string{hsSource})
	require.NoError(t, err)
	assert.Equal(t, 0, second, "an already-retired source changes nothing")
}

func TestRetireSplitSourceSegmentsWithNoChannels(t *testing.T) {
	m := newHashSplitMutationMeta(t)
	retired, err := m.RetireSplitSourceSegments(context.Background(), nil)
	require.NoError(t, err)
	assert.Zero(t, retired)
	assert.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(context.Background(), 500).GetState())
}
