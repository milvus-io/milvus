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

package split

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func rewriteFixture() *datapb.SplitShardTask {
	return &datapb.SplitShardTask{
		TaskId: 10, CollectionId: 100, SourceVchannel: testSource, SwitchTimeTick: 100, RoutingModulus: 2, State: datapb.SplitShardTaskState_SplitShardTaskResidualFlushed,
		Targets: []*datapb.SplitShardTaskTarget{{Vchannel: testTarget1, GenesisTimeTick: 101, Residues: []uint64{0}}, {Vchannel: testTarget2, GenesisTimeTick: 102, Residues: []uint64{1}}},
	}
}

func rewriteSegment(id int64, partition int64, level datapb.SegmentLevel, label string) SourceSegment {
	return SourceSegment{Info: &datapb.SegmentInfo{ID: id, CollectionID: 100, PartitionID: partition, InsertChannel: testSource, State: commonpb.SegmentState_Flushed, Level: level, SplitTarget: label}, Size: 10}
}

func TestRewriteWaitsForResidualAndSourceL0(t *testing.T) {
	task := rewriteFixture()
	task.State = datapb.SplitShardTaskState_SplitShardTaskWindowOpen
	step, err := PlanRewrite(task, nil, 20)
	require.NoError(t, err)
	require.False(t, step.Ended)
	task.State = datapb.SplitShardTaskState_SplitShardTaskResidualFlushed
	segments := []SourceSegment{rewriteSegment(1, 1, datapb.SegmentLevel_L1, ""), rewriteSegment(2, 1, datapb.SegmentLevel_L0, ""), rewriteSegment(3, 2, datapb.SegmentLevel_L1, "")}
	step, err = PlanRewrite(task, segments, 20)
	require.NoError(t, err)
	require.True(t, step.FoldL0)
	require.Len(t, step.Batches, 1)
	require.Equal(t, []int64{3}, step.Batches[0].SegmentIDs)
	segments = segments[:1]
	segments[0].Compacting = true
	step, err = PlanRewrite(task, segments, 20)
	require.NoError(t, err)
	require.Empty(t, step.Batches)
	require.False(t, step.Ended)
	segments[0].Compacting = false
	segments[0].Info.SplitTarget = testTarget1
	step, err = PlanRewrite(task, segments, 20)
	require.NoError(t, err)
	require.True(t, step.Ended)
	segments[0].Info.SplitTarget = "wrong"
	_, err = PlanRewrite(task, segments, 20)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
}

func TestPlanAdoptionRechecksEverySourceSegment(t *testing.T) {
	task := rewriteFixture()
	task.State = datapb.SplitShardTaskState_SplitShardTaskRewriteDone
	segment := rewriteSegment(1, 1, datapb.SegmentLevel_L1, testTarget1)
	moves, err := PlanAdoption(task, []SourceSegment{segment})
	require.NoError(t, err)
	require.EqualValues(t, 100, moves[0].TransformStart)
	for _, mutate := range []func(*SourceSegment){func(s *SourceSegment) { s.Compacting = true }, func(s *SourceSegment) { s.Info.SplitTarget = "" }, func(s *SourceSegment) { s.Info.State = commonpb.SegmentState_Flushing }, func(s *SourceSegment) { s.Info.Level = datapb.SegmentLevel_L0 }} {
		segment = rewriteSegment(1, 1, datapb.SegmentLevel_L1, testTarget1)
		mutate(&segment)
		_, err = PlanAdoption(task, []SourceSegment{segment})
		require.ErrorIs(t, err, merr.ErrServiceNotReady)
	}
}

func TestAdoptionKeepsL0AndPinsUntilExplicitLifecycleSteps(t *testing.T) {
	ctx := context.Background()
	catalog := newFakeCatalog()
	store, err := NewStore(ctx, catalog)
	require.NoError(t, err)
	task := rewriteFixture()
	task.State = datapb.SplitShardTaskState_SplitShardTaskRewriteDone
	require.NoError(t, store.saveLocked(ctx, task))
	require.NoError(t, store.Advance(ctx, 10, datapb.SplitShardTaskState_SplitShardTaskAdopted))
	require.True(t, store.IsL0CompactionSuspended(testTarget1))
	require.NotEmpty(t, store.TransformPins(100))
	require.Error(t, store.ReleasePins(ctx, 10))
	require.NoError(t, store.ResumeL0(ctx, 10))
	require.False(t, store.IsL0CompactionSuspended(testTarget1))
	require.NotEmpty(t, store.TransformPins(100))
	recovered, err := NewStore(ctx, catalog)
	require.NoError(t, err)
	require.False(t, recovered.IsL0CompactionSuspended(testTarget1))
	require.NotEmpty(t, recovered.TransformPins(100))
	require.NoError(t, recovered.ReleasePins(ctx, 10))
	require.Empty(t, recovered.TransformPins(100))
}

func TestSplitRewritePlanCarriesFixedTTLCutoff(t *testing.T) {
	task := rewriteFixture()
	require.Equal(t, task.GetSwitchTimeTick(), RewritePlan(task).GetTtlCutoffTimetick())
}
