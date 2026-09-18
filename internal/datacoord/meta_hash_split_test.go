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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

func TestHashSplitMutationDropsItsInputInTheSameCommit(t *testing.T) {
	// The commit is the rewrite's frontier: the outputs replace the input the
	// way a mix compaction's do. Publishing the outputs and dropping the input
	// in ONE catalog write means every recovery view sees either the input or
	// the outputs, never both and never neither -- and it is what lets the
	// drain's "no live segment on the source" conjunct pass at all.
	m := newHashSplitMutationMeta(t)
	catalog := mocks.NewDataCoordCatalog(t)
	var got []metastore.UpdateAction
	catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, actions ...metastore.UpdateAction) error {
			got = actions
			return nil
		}).Once()
	m.catalog = catalog

	outputs, mutation, err := m.CompleteCompactionMutation(context.Background(), hashSplitRewriteTask(),
		&datapb.CompactionPlanResult{
			PlanID: 900,
			Segments: []*datapb.CompactionSegment{
				{SegmentID: 601, NumOfRows: 60, Channel: hsTgt0},
				{SegmentID: 602, NumOfRows: 40, Channel: hsTgt1},
			},
		})
	require.NoError(t, err)
	require.Len(t, outputs, 2)

	// One write carries both: the outputs added first, then the input retired,
	// so the ordered fallback path never retires an input before its outputs.
	require.Len(t, got, 3)
	added := make([]int64, 0, 2)
	for _, action := range got[:2] {
		entry, ok := action.Entry.(metastore.SegmentEntry)
		require.True(t, ok)
		assert.Equal(t, metastore.ActionAdd, action.Type)
		added = append(added, entry.Segment.GetID())
	}
	assert.ElementsMatch(t, []int64{601, 602}, added)
	retired, ok := got[2].Entry.(metastore.SegmentEntry)
	require.True(t, ok)
	assert.Equal(t, metastore.ActionUpdate, got[2].Type)
	assert.True(t, retired.AlterEncoding, "retired with the same encoding mix uses")
	assert.EqualValues(t, 500, retired.Segment.GetID())
	assert.Equal(t, commonpb.SegmentState_Dropped, retired.Segment.GetState())
	assert.True(t, retired.Segment.GetCompacted())
	assert.NotZero(t, retired.Segment.GetDroppedAt())

	source := m.GetSegment(context.Background(), 500)
	require.NotNil(t, source)
	assert.Equal(t, commonpb.SegmentState_Dropped, source.GetState())
	assert.True(t, source.GetCompacted())
	assert.NotZero(t, source.GetDroppedAt())
	assert.Empty(t, m.GetSegmentsByChannel(hsSource), "the source holds nothing live once its only segment is rewritten")

	// The metrics move with the state: 100 rows leave with the input and 100
	// arrive with the outputs.
	require.NotNil(t, mutation)
	assert.Zero(t, mutation.rowCountChange)
	assert.EqualValues(t, 100, mutation.rowCountAccChange)
}

func TestHashSplitMutationCatalogFailureChangesNothing(t *testing.T) {
	// A failed write must leave memory exactly as it was: neither the outputs
	// published nor the input dropped, so the retried plan converges.
	m := newHashSplitMutationMeta(t)
	before := m.GetSegment(context.Background(), 500)
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("etcd down")).Once()
	m.catalog = catalog

	outputs, mutation, err := m.CompleteCompactionMutation(context.Background(), hashSplitRewriteTask(),
		&datapb.CompactionPlanResult{
			PlanID:   900,
			Segments: []*datapb.CompactionSegment{{SegmentID: 601, NumOfRows: 100, Channel: hsTgt0}},
		})
	require.Error(t, err)
	assert.Nil(t, outputs)
	assert.Nil(t, mutation)

	source := m.GetSegment(context.Background(), 500)
	assert.Same(t, before, source, "the in-memory input is not replaced")
	assert.Equal(t, commonpb.SegmentState_Flushed, source.GetState())
	assert.False(t, source.GetCompacted())
	assert.Zero(t, source.GetDroppedAt())
	assert.Nil(t, m.GetSegment(context.Background(), 601), "no output is published")
	assert.Len(t, m.GetSegmentsByChannel(hsSource), 1)
}

func TestHashSplitMutationWithNoOutputStillDropsItsInput(t *testing.T) {
	// Zero-output plans: every row of the input was deleted or expired, so the
	// datanode produces nothing. The commit must still drop the input, in its
	// own write, because no output lineage will ever name it.
	m := newHashSplitMutationMeta(t)
	catalog := mocks.NewDataCoordCatalog(t)
	var got []metastore.UpdateAction
	catalog.EXPECT().Update(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, actions ...metastore.UpdateAction) error {
			got = actions
			return nil
		}).Once()
	m.catalog = catalog

	outputs, mutation, err := m.CompleteCompactionMutation(context.Background(), hashSplitRewriteTask(),
		&datapb.CompactionPlanResult{PlanID: 900})
	require.NoError(t, err)
	assert.Empty(t, outputs)

	require.Len(t, got, 1)
	retired, ok := got[0].Entry.(metastore.SegmentEntry)
	require.True(t, ok)
	assert.Equal(t, metastore.ActionUpdate, got[0].Type)
	assert.EqualValues(t, 500, retired.Segment.GetID())
	assert.Equal(t, commonpb.SegmentState_Dropped, retired.Segment.GetState())

	assert.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(context.Background(), 500).GetState())
	assert.Empty(t, m.GetSegmentsByChannel(hsSource))
	require.NotNil(t, mutation)
	assert.EqualValues(t, -100, mutation.rowCountChange, "the input's rows leave and nothing replaces them")
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
	assert.Contains(t, err.Error(), "not one of the plan's targets")
	assert.Nil(t, m.GetSegment(context.Background(), 601))
}

func TestHashSplitMutationPublishesAnEmptyHalf(t *testing.T) {
	// A source whose keys all fall on one side produces nothing for the other.
	// That output is still published, Dropped, as mix publishes an empty
	// output, to keep the segment ledger complete.
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
	// rewrite was refused at the meta write and the task stalled in its
	// redistribution forever. This pins the dispatch, not just the function.
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

func TestHashSplitMutationRefusesAPlanItCannotCommit(t *testing.T) {
	result := &datapb.CompactionPlanResult{
		PlanID:   900,
		Segments: []*datapb.CompactionSegment{{SegmentID: 601, NumOfRows: 100, Channel: hsTgt0}},
	}
	cases := []struct {
		name   string
		mutate func(m *meta, task *datapb.CompactionTask)
		errIs  error
	}{
		{
			name:   "an input absent from meta",
			mutate: func(_ *meta, task *datapb.CompactionTask) { task.InputSegments = []int64{404} },
			errIs:  merr.ErrSegmentNotFound,
		},
		{
			// A second plan for an input an earlier plan already rewrote.
			name: "an input an earlier commit dropped",
			mutate: func(m *meta, _ *datapb.CompactionTask) {
				dropped := m.segments.GetSegment(500).Clone()
				dropped.State = commonpb.SegmentState_Dropped
				m.segments.SetSegment(500, dropped)
			},
			errIs: merr.ErrSegmentNotFound,
		},
		{
			name:   "no input at all",
			mutate: func(_ *meta, task *datapb.CompactionTask) { task.InputSegments = nil },
			errIs:  merr.ErrIllegalCompactionPlan,
		},
		{
			name:   "no schema",
			mutate: func(_ *meta, task *datapb.CompactionTask) { task.Schema = nil },
			errIs:  merr.ErrIllegalCompactionPlan,
		},
		{
			name:   "no target",
			mutate: func(_ *meta, task *datapb.CompactionTask) { task.HashSplitTargets = nil },
			errIs:  merr.ErrIllegalCompactionPlan,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newHashSplitMutationMeta(t)
			task := hashSplitRewriteTask()
			tc.mutate(m, task)
			outputs, mutation, err := m.CompleteCompactionMutation(context.Background(), task, result)
			assert.ErrorIs(t, err, tc.errIs)
			assert.Nil(t, outputs)
			assert.Nil(t, mutation)
			assert.Nil(t, m.GetSegment(context.Background(), 601), "no output is published")
		})
	}
}
