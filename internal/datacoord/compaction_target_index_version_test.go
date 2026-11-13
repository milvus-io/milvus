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

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/targets"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestStandingIndexVersionCompactionTargetCondition(t *testing.T) {
	ctx := t.Context()
	enableTestCompactionTargets(t)
	cfg := &Params.DataCoordCfg
	for key, value := range map[string]string{
		cfg.EnableAutoCompaction.Key:           "true",
		cfg.AutoUpgradeSegmentIndex.Key:        "false",
		cfg.ForceRebuildSegmentIndex.Key:       "false",
		cfg.ForceRebuildScalarSegmentIndex.Key: "false",
		cfg.TargetVecIndexVersion.Key:          "0",
		cfg.TargetScalarIndexVersion.Key:       "3",
	} {
		require.NoError(t, Params.Save(key, value))
		t.Cleanup(func() { Params.Reset(key) })
	}
	catalog, _ := targets.NewCompactionTargetTestCatalog(t)
	s, _ := newTargetRPCServer(t, catalog, meta.NewMockAllocator(t))
	s.indexMeta = index.NewSegmentIndexMeta(catalog)
	versions := index.NewIndexEngineVersionManager()
	var id int64
	check := func(enabled bool, parameters map[string]string) {
		t.Helper()
		reconciler := targets.NewCompactionTargetReconciler(s.meta, s.indexMeta, s.compactionTargetMeta, s.handler, s.allocator, s.compactionTargetCollectionAvailable)
		reconciler.SetIndexVersionManager(versions)
		_, err := reconciler.Reconcile(ctx)
		require.NoError(t, err)
		list, err := s.ListCompactionTargets(ctx, &datapb.ListCompactionTargetsRequest{IncludeSystem: true})
		require.NoError(t, merr.CheckRPCCall(list, err))
		var found []*datapb.CompactionTargetReadModel
		for _, target := range list.GetTargets() {
			if target.GetIntent() == datapb.TargetIntent_INTENT_INDEX_VERSION {
				found = append(found, target)
			}
		}
		require.Len(t, found, 1)
		listed := found[0]
		if id == 0 {
			id = listed.GetTargetId()
			require.Positive(t, id)
		}
		require.Equal(t, id, listed.GetTargetId())
		require.Zero(t, listed.GetResource().GetCollectionId())
		require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, listed.GetState())
		require.NotNil(t, listed.Enabled)
		require.Equal(t, enabled, listed.GetEnabled())
		require.Equal(t, parameters, listed.GetParameters())
		require.Nil(t, listed.RemainingRows)

		resp, err := s.DescribeCompactionTarget(ctx, &datapb.DescribeCompactionTargetRequest{
			TargetId: id, Collection: &datapb.CompactionTargetResource{},
		})
		require.NoError(t, merr.CheckRPCCall(resp, err))
		described := resp.GetTarget()
		require.Equal(t, parameters, described.GetParameters())
		require.Equal(t, enabled, described.GetEnabled())
		if enabled {
			require.NotNil(t, described.RemainingRows)
			require.Zero(t, described.GetRemainingRows())
			require.NotZero(t, described.GetCheckedAt())
		} else {
			require.Nil(t, described.RemainingRows)
			require.Zero(t, described.GetCheckedAt())
		}
	}

	check(false, map[string]string{"vector_mode": "ABSENT", "scalar_mode": "ABSENT"})
	require.NoError(t, Params.Save(cfg.ForceRebuildSegmentIndex.Key, "true"))
	check(true, map[string]string{"vector_mode": "EXACT", "vector_version": "0", "scalar_mode": "ABSENT"})

	require.NoError(t, Params.Save(cfg.AutoUpgradeSegmentIndex.Key, "true"))
	check(true, map[string]string{
		"vector_mode": "EXACT", "vector_version": "0", "scalar_mode": "AT_LEAST", "scalar_version": "3",
	})
	require.NoError(t, Params.Save(cfg.TargetVecIndexVersion.Key, "-1"))
	bothAuto := map[string]string{
		"vector_mode": "AT_LEAST", "vector_version": "0", "scalar_mode": "AT_LEAST", "scalar_version": "3",
	}
	check(true, bothAuto)

	var err error
	s.compactionTargetMeta, err = targets.NewCompactionTargetMeta(ctx, catalog)
	require.NoError(t, err)
	require.NoError(t, Params.Save(cfg.EnableAutoCompaction.Key, "false"))
	check(false, bothAuto)
	require.NoError(t, Params.Save(cfg.EnableAutoCompaction.Key, "true"))
	check(true, bothAuto)
}
func TestStandingIndexVersionCompactionTargetUsesPublishedArtifacts(t *testing.T) {
	ctx := t.Context()
	enableTestCompactionTargets(t)
	cfg := &Params.DataCoordCfg
	for key, value := range map[string]string{
		cfg.EnableAutoCompaction.Key:           "true",
		cfg.EnableSortCompaction.Key:           "false",
		cfg.AutoUpgradeSegmentIndex.Key:        "true",
		cfg.ForceRebuildSegmentIndex.Key:       "true",
		cfg.ForceRebuildScalarSegmentIndex.Key: "false",
		cfg.TargetVecIndexVersion.Key:          "2",
		cfg.TargetScalarIndexVersion.Key:       "3",
		cfg.IndexBasedCompaction.Key:           "true",
		cfg.DVForceAllIndexReady.Key:           "false",
	} {
		require.NoError(t, Params.Save(key, value))
		t.Cleanup(func() { Params.Reset(key) })
	}
	catalog, _ := targets.NewCompactionTargetTestCatalog(t)
	s, _ := newTargetRPCServer(t, catalog, meta.NewMockAllocator(t))
	mt, indexMetas, err := index.NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	s.meta, s.indexMeta = mt, indexMetas.IndexMeta
	s.handler = meta.NewMockHandlerWithMeta(mt)
	const (
		vectorIndex  int64 = 10
		scalarIndex  int64 = 20
		retiredIndex int64 = 30
	)
	addCollection := func(id int64, properties map[string]string, externalField string) {
		t.Helper()
		mt.AddCollection(&meta.CollectionInfo{ID: id, Properties: properties,
			Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 100, DataType: schemapb.DataType_FloatVector, ExternalField: externalField},
				{FieldID: 101, DataType: schemapb.DataType_Int64},
				{FieldID: 102, DataType: schemapb.DataType_Int64},
			}},
		})
		for _, definition := range []*model.Index{
			{CollectionID: id, FieldID: 100, IndexID: vectorIndex, IndexName: "v", IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}}},
			{CollectionID: id, FieldID: 101, IndexID: scalarIndex, IndexName: "s", IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "INVERTED"}}},
			{CollectionID: id, FieldID: 102, IndexID: retiredIndex, IndexName: "retired", IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "INVERTED"}}},
		} {
			require.NoError(t, s.indexMeta.CreateIndex(ctx, definition))
		}
	}
	addCollection(1, nil, "")
	addCollection(2, map[string]string{common.CollectionAutoCompactionKey: "false"}, "")
	addCollection(3, nil, "external-vector")
	addSegment := func(id, collectionID, rows int64, change func(*datapb.SegmentInfo)) {
		t.Helper()
		segment := &datapb.SegmentInfo{
			ID: id, CollectionID: collectionID, PartitionID: 1, InsertChannel: "ch",
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1, NumOfRows: rows,
			IsSorted: true,
		}
		if change != nil {
			change(segment)
		}
		require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(segment)))
	}
	build := func(collectionID, segmentID, indexID int64, state commonpb.IndexState, vector, scalar int32, keys ...string) int64 {
		t.Helper()
		buildID := segmentID*100 + indexID
		require.NoError(t, s.indexMeta.AddSegmentIndex(ctx, &model.SegmentIndex{
			CollectionID: collectionID, PartitionID: 1, SegmentID: segmentID, IndexID: indexID, BuildID: buildID,
		}))
		require.NoError(t, s.indexMeta.FinishTask(&workerpb.IndexTaskInfo{
			BuildID: buildID, State: state, IndexFileKeys: keys,
			CurrentIndexVersion: vector, CurrentScalarIndexVersion: scalar,
		}))
		return buildID
	}

	// Each segment has a different row count so Describe detects omitted and extra matches.
	addSegment(101, 1, 11, nil)
	build(1, 101, vectorIndex, commonpb.IndexState_Finished, 4, 0, "old-vector")

	addSegment(102, 1, 13, nil)
	build(1, 102, vectorIndex, commonpb.IndexState_Finished, 2, 0, "vector")
	build(1, 102, scalarIndex, commonpb.IndexState_Failed, 99, 2, "stale-scalar")

	addSegment(103, 1, 17, nil)
	build(1, 103, vectorIndex, commonpb.IndexState_Failed, 1, 0, "stale-vector")

	addSegment(104, 1, 19, func(seg *datapb.SegmentInfo) { seg.IsImporting = true })
	build(1, 104, vectorIndex, commonpb.IndexState_Finished, 1, 0, "imported-vector")

	addSegment(105, 1, 23, func(seg *datapb.SegmentInfo) { seg.Level = datapb.SegmentLevel_L2 })
	build(1, 105, vectorIndex, commonpb.IndexState_Finished, 1, 0, "l2-vector")

	addSegment(106, 1, 29, nil)
	build(1, 106, vectorIndex, commonpb.IndexState_Failed, 1, 0)

	addSegment(107, 1, 31, nil)
	deleted := build(1, 107, vectorIndex, commonpb.IndexState_Finished, 1, 0, "deleted-artifact")
	require.NoError(t, s.indexMeta.DeleteTask(deleted))
	build(1, 107, retiredIndex, commonpb.IndexState_Finished, 0, 1, "retired-definition")
	require.NoError(t, s.indexMeta.MarkIndexAsDeleted(ctx, 1, []int64{retiredIndex}))

	addSegment(108, 1, 37, nil)
	build(1, 108, vectorIndex, commonpb.IndexState_Finished, 2, 0, "vector")
	build(1, 108, scalarIndex, commonpb.IndexState_Finished, 0, 4, "new-scalar")

	addSegment(109, 2, 41, nil)
	build(2, 109, vectorIndex, commonpb.IndexState_Finished, 1, 0, "auto-disabled")

	addSegment(110, 3, 43, nil)
	build(3, 110, vectorIndex, commonpb.IndexState_Finished, 1, 0, "external")

	reconciler := targets.NewCompactionTargetReconciler(mt, s.indexMeta, s.compactionTargetMeta, s.handler, s.allocator, s.compactionTargetCollectionAvailable)
	reconciler.SetIndexVersionManager(index.NewIndexEngineVersionManager())
	runRound := func(want ...int64) []*datapb.CompactionTask {
		t.Helper()
		events, err := reconciler.Reconcile(ctx)
		require.NoError(t, err)
		var plans []*datapb.CompactionTask
		var inputIDs []int64
		for _, pool := range events[policy.TriggerTypeSingle] {
			tasks, err := pool.Plan()
			require.NoError(t, err)
			for _, task := range tasks {
				require.Equal(t, datapb.CompactionType_MixCompaction, task.GetType())
				inputIDs = append(inputIDs, task.GetInputSegments()...)
			}
			plans = append(plans, tasks...)
		}
		require.ElementsMatch(t, want, inputIDs)
		return plans
	}

	runRound(101, 102)
	list, err := s.ListCompactionTargets(ctx, &datapb.ListCompactionTargetsRequest{IncludeSystem: true})
	require.NoError(t, merr.CheckRPCCall(list, err))
	var targetID int64
	for _, target := range list.GetTargets() {
		if target.GetIntent() == datapb.TargetIntent_INTENT_INDEX_VERSION {
			targetID = target.GetTargetId()
		}
	}
	require.Positive(t, targetID)
	checkRows := func(want int64) {
		t.Helper()
		resp, err := s.DescribeCompactionTarget(ctx, &datapb.DescribeCompactionTargetRequest{
			TargetId: targetID, Collection: &datapb.CompactionTargetResource{},
		})
		require.NoError(t, merr.CheckRPCCall(resp, err))
		require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, resp.GetTarget().GetState())
		require.NotNil(t, resp.GetTarget().RemainingRows)
		require.Equal(t, want, resp.GetTarget().GetRemainingRows())
	}
	checkRows(101)

	// Requiring scalar readiness delays its unfinished artifact without hiding its rows.
	require.NoError(t, Params.Save(cfg.DVForceAllIndexReady.Key, "true"))
	build(1, 101, scalarIndex, commonpb.IndexState_Finished, 0, 3, "scalar")
	plans := runRound(101)
	checkRows(101)
	require.Len(t, plans, 1)
	mt.SetSegmentCompacting(101, true)
	runRound()
	checkRows(101)

	const replacement = 201
	output, _, err := mt.CompleteCompactionMutation(ctx, plans[0], &datapb.CompactionPlanResult{
		Segments: []*datapb.CompactionSegment{{
			SegmentID: replacement, IsSorted: true, NumOfRows: 11,
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				102: {Files: []string{"text-stats"}, CurrentScalarIndexVersion: 1},
			},
		}},
	})
	require.NoError(t, err)
	require.Len(t, output, 1)
	require.Equal(t, datapb.SegmentLevel_L1, output[0].GetLevel())
	require.NotEmpty(t, output[0].GetTextStatsLogs())
	runRound()
	checkRows(90)

	// The worker-reported version may be lower than the requested version.
	vectorBuild := build(1, replacement, vectorIndex, commonpb.IndexState_Finished, 1, 0, "rebuilt-old")
	build(1, replacement, scalarIndex, commonpb.IndexState_Finished, 0, 3, "rebuilt-scalar")
	runRound(replacement)
	checkRows(101)
	require.NoError(t, s.indexMeta.FinishTask(&workerpb.IndexTaskInfo{
		BuildID: vectorBuild, State: commonpb.IndexState_Finished, CurrentIndexVersion: 2, IndexFileKeys: []string{"rebuilt-current"},
	}))
	runRound()
	checkRows(90)
}
