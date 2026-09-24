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
	"sort"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func newIndexRebuildFixture(t *testing.T) *JSONPathIndexMigrationPolicySuite {
	s := &JSONPathIndexMigrationPolicySuite{}
	s.SetT(t)
	s.SetupTest()
	s.saveParam(&Params.DataCoordCfg.EnableAutoCompaction, "true")
	s.setQueryNodeVersions(5, 10)
	s.gate.AddDataNode(newJSONPathMigrationQueryNode(2, 5, 10))
	return s
}

func TestMigrationCompactionIndexRebuild(t *testing.T) {
	for _, tc := range []struct {
		name         string
		config       map[string]string
		scalar       int32
		vector       int32
		want         bool
		scalarTarget int32
		vectorTarget int32
		boundWorker  bool
	}{
		{name: "disabled", scalar: 1, vector: 8},
		{name: "scalar upgrade", config: map[string]string{"autoUpgradeSegmentIndex": "true"}, scalar: 1, vector: 10, want: true},
		{name: "vector upgrade", config: map[string]string{"autoUpgradeSegmentIndex": "true"}, scalar: 5, vector: 8, want: true},
		{name: "both upgrades share one rewrite", config: map[string]string{"autoUpgradeSegmentIndex": "true"}, scalar: 1, vector: 8, want: true},
		{name: "already current", config: map[string]string{"autoUpgradeSegmentIndex": "true"}, scalar: 5, vector: 10},
		{name: "protect newer scalar", config: map[string]string{"autoUpgradeSegmentIndex": "true"}, scalar: 6, vector: 8},
		{name: "protect newer vector", config: map[string]string{"autoUpgradeSegmentIndex": "true"}, scalar: 1, vector: 11},
		{name: "scalar force upgrade", config: map[string]string{"forceRebuildScalarSegmentIndex": "true", "targetScalarIndexVersion": "3"}, scalar: 1, vector: 10, want: true, scalarTarget: 3},
		{name: "scalar force downgrade", config: map[string]string{"forceRebuildScalarSegmentIndex": "true", "targetScalarIndexVersion": "3"}, scalar: 5, vector: 10, want: true, scalarTarget: 3},
		{name: "vector force downgrade", config: map[string]string{"forceRebuildSegmentIndex": "true", "targetVecIndexVersion": "8"}, scalar: 5, vector: 10, want: true, vectorTarget: 8},
		{name: "scalar force protects vector", config: map[string]string{"forceRebuildScalarSegmentIndex": "true", "targetScalarIndexVersion": "3"}, scalar: 5, vector: 11},
		{name: "vector force protects scalar", config: map[string]string{"forceRebuildSegmentIndex": "true", "targetVecIndexVersion": "8"}, scalar: 6, vector: 10},
		{name: "scalar clamp converges", config: map[string]string{"forceRebuildScalarSegmentIndex": "true", "targetScalarIndexVersion": "99"}, scalar: 1, vector: 10, want: true},
		{name: "vector clamp converges", config: map[string]string{"forceRebuildSegmentIndex": "true", "targetVecIndexVersion": "99"}, scalar: 5, vector: 8, want: true},
		{name: "scalar force without target", config: map[string]string{"forceRebuildScalarSegmentIndex": "true"}, scalar: 1, vector: 10},
		{name: "vector force without target", config: map[string]string{"forceRebuildSegmentIndex": "true"}, scalar: 5, vector: 8},
		{name: "target alone does not rebuild", config: map[string]string{"targetScalarIndexVersion": "3"}, scalar: 1, vector: 10},
		{name: "auto compaction disabled", config: map[string]string{"autoUpgradeSegmentIndex": "true", "compaction.enableAutoCompaction": "false"}, scalar: 1, vector: 8},
		{name: "bound scalar upgrade", boundWorker: true, config: map[string]string{"autoUpgradeSegmentIndex": "true"}, scalar: 1, vector: 10, want: true},
		{name: "bound vector upgrade", boundWorker: true, config: map[string]string{"autoUpgradeSegmentIndex": "true"}, scalar: 5, vector: 8, want: true},
		{name: "bound scalar force downgrade", boundWorker: true, config: map[string]string{"forceRebuildScalarSegmentIndex": "true", "targetScalarIndexVersion": "3"}, scalar: 5, vector: 10, want: true, scalarTarget: 3},
		{name: "bound vector force downgrade", boundWorker: true, config: map[string]string{"forceRebuildSegmentIndex": "true", "targetVecIndexVersion": "8"}, scalar: 5, vector: 10, want: true, vectorTarget: 8},
		{name: "bound protects newer scalar", boundWorker: true, config: map[string]string{"autoUpgradeSegmentIndex": "true"}, scalar: 6, vector: 8},
		{name: "bound protects newer vector", boundWorker: true, config: map[string]string{"autoUpgradeSegmentIndex": "true"}, scalar: 1, vector: 11},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newIndexRebuildFixture(t)
			if tc.boundWorker {
				s.saveParam(&Params.DataCoordCfg.BindIndexNodeMode, "true")
				s.gate.StartupDataNodes(nil)
			}
			for key, value := range tc.config {
				key := "dataCoord." + key
				require.NoError(t, Params.Save(key, value))
				t.Cleanup(func() { Params.Reset(key) })
			}
			// No JSON fields: generic rebuilds must work before the JSON gates open.
			schema := newJSONPathMigrationSchema()
			schema.Fields = []*schemapb.FieldSchema{schema.Fields[0], schema.Fields[2], schema.Fields[3]}
			collection := &collectionInfo{ID: 100, Schema: schema}
			segment := newJSONPathMigrationSegment(100, 201)
			scalar := newJSONPathMigrationSegmentIndex(100, 201, 1000, tc.scalar)
			scalar.CurrentIndexVersion = 99 // must not be interpreted as a vector artifact
			vector := newJSONPathMigrationSegmentIndex(100, 201, 1001, 99)
			vector.CurrentIndexVersion = tc.vector
			s.setPolicyMeta(collection, map[UniqueID]*SegmentInfo{201: segment}, []*model.Index{
				newJSONPathMigrationIndex(100, 200, 1000, ""),
				newJSONPathMigrationVectorIndex(100, 300, 1001, "HNSW"),
			}, []*model.SegmentIndex{scalar, vector})
			s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(collection, nil).Maybe()
			if tc.want {
				s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(10000), nil).Once()
			}
			events, err := s.policy.Trigger(context.Background())
			require.NoError(t, err)
			if !tc.want {
				require.Empty(t, events[TriggerTypeSingle])
				return
			}
			require.True(t, s.policy.Enable())
			require.Len(t, events[TriggerTypeSingle], 1)
			require.Len(t, events[TriggerTypeSingle][0].GetSegmentsView(), 1)
			require.Equal(t, []int64{201}, viewSegmentIDs(events[TriggerTypeSingle]))

			// Simulate the replacement's published artifact versions. Use the
			// same resolver as build requests, not a copy of the selection logic.
			scalar.CurrentScalarIndexVersion = s.version.ResolveScalarIndexVersion()
			vector.CurrentIndexVersion = s.version.ResolveVecIndexVersion()
			if tc.scalarTarget == 0 {
				tc.scalarTarget = 5
			}
			if tc.vectorTarget == 0 {
				tc.vectorTarget = 10
			}
			require.Equal(t, tc.scalarTarget, scalar.CurrentScalarIndexVersion)
			require.Equal(t, tc.vectorTarget, vector.CurrentIndexVersion)
			events, err = s.policy.Trigger(context.Background())
			require.NoError(t, err)
			require.Empty(t, events[TriggerTypeSingle], "rebuild must converge at the version actually requested from the writer")
		})
	}
}

func TestMigrationCompactionIndexRebuildSingleSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version int32
		state   commonpb.IndexState
		want    bool
	}{
		{"safe", 5, commonpb.IndexState_Finished, true},
		{"newer artifact", 6, commonpb.IndexState_Finished, false},
		{"pending build", 0, commonpb.IndexState_InProgress, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newIndexRebuildFixture(t)
			collection := &collectionInfo{ID: 100, Schema: newJSONPathMigrationSchema()}
			oldIndex := newJSONPathMigrationSegmentIndex(100, 201, 1000, 1)
			otherIndex := newJSONPathMigrationSegmentIndex(100, 201, 1001, tc.version)
			otherIndex.IndexState = tc.state
			s.setPolicyMeta(collection, nil, []*model.Index{
				newJSONPathMigrationIndex(100, 200, 1000, ""),
				newJSONPathMigrationIndex(100, 100, 1001, "DOUBLE"),
			}, []*model.SegmentIndex{oldIndex, otherIndex})
			infos := s.policy.meta.indexMeta.getSegmentIndexRebuildInfo(201)
			sort.Slice(infos, func(i, j int) bool { return infos[i].IndexID < infos[j].IndexID })
			reads := 0
			snapshot := mockey.Mock((*indexMeta).getSegmentIndexRebuildInfo).To(func(*indexMeta, int64) []segmentIndexRebuildInfo {
				reads++
				// Put the rebuild reason first to verify that later artifacts
				// are still checked for safety before accepting the segment.
				return infos
			}).Build()
			t.Cleanup(func() { snapshot.UnPatch() })
			canRebuild, needsRebuild := checkSegmentIndexRebuild(
				s.policy.meta.indexMeta, 201,
				getActiveIndexIDs(s.policy.meta.indexMeta, collection.ID),
				nil, nil, nil,
				safeSegmentRebuildTargets{scalarVersion: 5, vectorVersion: 10},
				indexRebuildOptions{autoUpgrade: true})
			require.Equal(t, 1, reads, "safety and rebuild reasons must share one snapshot")
			require.Equal(t, tc.want, canRebuild)
			require.Equal(t, tc.want, needsRebuild)
		})
	}
}

func TestMigrationCompactionBoundWorkerJSONGates(t *testing.T) {
	for _, tc := range []struct {
		name         string
		scalarTarget string
		statsFormat  string
		want         bool
	}{
		{"gates closed", "-1", "3", false},
		{"path gate open", "6", "3", true},
		{"stats gate open", "-1", "4", true},
		{"both gates open", "6", "4", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newIndexRebuildFixture(t)
			s.saveParam(&Params.DataCoordCfg.BindIndexNodeMode, "true")
			s.saveParam(&Params.DataCoordCfg.TargetScalarIndexVersion, tc.scalarTarget)
			s.saveParam(&Params.DataCoordCfg.JSONStatsFormatVersion, tc.statsFormat)
			s.saveParam(&Params.CommonCfg.EnabledJSONKeyStats, "true")
			s.saveParam(&Params.DataCoordCfg.JSONStatsTriggerCount, "10")
			s.gate.StartupDataNodes(nil)
			// The upgraded QN advertises current V5 / maximum V6. Only the
			// configured gate targets enable automatic JSON migrations.
			reader := newJSONPathMigrationQueryNode(1, 6, 10)
			reader.ScalarIndexEngineVersion.CurrentIndexVersion = 5
			s.version.Update(reader)
			collection := &collectionInfo{ID: 100, Schema: newJSONPathMigrationSchema()}
			segment := newJSONPathMigrationSegment(100, 201)
			segment.JsonKeyStats = map[int64]*datapb.JsonKeyStats{
				100: {JsonKeyStatsDataFormat: common.JSONStatsDataFormatV3},
			}
			s.setPolicyMeta(collection, map[UniqueID]*SegmentInfo{201: segment},
				[]*model.Index{newJSONPathMigrationIndex(100, 100, 1000, "DOUBLE")},
				[]*model.SegmentIndex{newJSONPathMigrationSegmentIndex(100, 201, 1000, 5)})
			s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(collection, nil).Maybe()
			if tc.want {
				s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(10000), nil).Once()
			}
			require.Equal(t, tc.want, s.policy.Enable())
			events, err := s.policy.Trigger(context.Background())
			require.NoError(t, err)
			if tc.want {
				require.Equal(t, []int64{201}, viewSegmentIDs(events[TriggerTypeSingle]))
			} else {
				require.Empty(t, events[TriggerTypeSingle])
			}
		})
	}
}

func TestMigrationCompactionIndexRebuildEligibility(t *testing.T) {
	s := newIndexRebuildFixture(t)
	s.saveParam(&Params.DataCoordCfg.AutoUpgradeSegmentIndex, "true")
	collection := &collectionInfo{ID: 100, Schema: newJSONPathMigrationSchema()}
	segments := make(map[UniqueID]*SegmentInfo)
	var indexes []*model.SegmentIndex
	for id := int64(201); id <= 207; id++ {
		segments[id] = newJSONPathMigrationSegment(100, id)
		indexes = append(indexes, newJSONPathMigrationSegmentIndex(100, id, 1000, 1))
	}
	indexes[1].IndexFileKeys = nil // fake-finished: do not repeatedly rewrite small segments
	indexes[2].IndexState = commonpb.IndexState_Failed
	indexes[3].IndexState = commonpb.IndexState_InProgress
	segments[205].isCompacting = true
	segments[206].IsImporting = true
	// 201 and 207 must remain separate even in the same partition/channel.
	s.setPolicyMeta(collection, segments,
		[]*model.Index{newJSONPathMigrationIndex(100, 200, 1000, "")}, indexes)
	s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(collection, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(10000), nil).Once()
	events, err := s.policy.Trigger(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{201, 207}, viewSegmentIDs(events[TriggerTypeSingle]))
	for _, view := range events[TriggerTypeSingle] {
		require.Len(t, view.GetSegmentsView(), 1)
	}

	collection.Properties = map[string]string{common.CollectionAutoCompactionKey: "false"}
	events, err = s.policy.Trigger(context.Background())
	require.NoError(t, err)
	require.Empty(t, events[TriggerTypeSingle])
	collection.Properties = nil
	s.saveParam(&Params.DataCoordCfg.StorageVersionCompactionRateLimitTokens, "0")
	events, err = s.policy.Trigger(context.Background())
	require.NoError(t, err)
	require.Empty(t, events[TriggerTypeSingle])
}

func TestMigrationCompactionForceRebuildTextSafety(t *testing.T) {
	for _, forceScalar := range []bool{false, true} {
		t.Run(map[bool]string{false: "vector force protects text", true: "scalar force aligns text"}[forceScalar], func(t *testing.T) {
			s := newIndexRebuildFixture(t)
			if forceScalar {
				s.saveParam(&Params.DataCoordCfg.ForceRebuildScalarSegmentIndex, "true")
				s.saveParam(&Params.DataCoordCfg.TargetScalarIndexVersion, "3")
			} else {
				s.saveParam(&Params.DataCoordCfg.ForceRebuildSegmentIndex, "true")
				s.saveParam(&Params.DataCoordCfg.TargetVecIndexVersion, "8")
			}
			collection := &collectionInfo{ID: 100, Schema: newJSONPathMigrationSchema()}
			segment := newJSONPathMigrationSegment(100, 201)
			segment.TextStatsLogs = map[int64]*datapb.TextIndexStats{200: {CurrentScalarIndexVersion: 6}}
			vector := newJSONPathMigrationSegmentIndex(100, 201, 1001, 0)
			vector.CurrentIndexVersion = 10
			s.setPolicyMeta(collection, map[UniqueID]*SegmentInfo{201: segment}, []*model.Index{
				newJSONPathMigrationIndex(100, 200, 1000, ""),
				newJSONPathMigrationVectorIndex(100, 300, 1001, "HNSW"),
			}, []*model.SegmentIndex{newJSONPathMigrationSegmentIndex(100, 201, 1000, 5), vector})
			s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(collection, nil)
			if forceScalar {
				s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(10000), nil).Once()
			}
			events, err := s.policy.Trigger(context.Background())
			require.NoError(t, err)
			require.Equal(t, forceScalar, len(events[TriggerTypeSingle]) == 1)
		})
	}
}

func TestMigrationCompactionIndexRebuildWaitsForWriters(t *testing.T) {
	s := newIndexRebuildFixture(t)
	s.saveParam(&Params.DataCoordCfg.ForceRebuildSegmentIndex, "true")
	s.saveParam(&Params.DataCoordCfg.TargetVecIndexVersion, "10")
	// QNs resolve target 10, but the DN can only reproduce up to 8.
	s.gate.UpdateDataNode(newJSONPathMigrationQueryNode(2, 5, 8))
	collection := &collectionInfo{ID: 100, Schema: newJSONPathMigrationSchema()}
	segment := newJSONPathMigrationSegment(100, 201)
	index := newJSONPathMigrationSegmentIndex(100, 201, 1000, 0)
	index.CurrentIndexVersion = 7
	s.setPolicyMeta(collection, map[UniqueID]*SegmentInfo{201: segment},
		[]*model.Index{newJSONPathMigrationVectorIndex(100, 300, 1000, "HNSW")}, []*model.SegmentIndex{index})
	s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(collection, nil)
	events, err := s.policy.Trigger(context.Background())
	require.NoError(t, err)
	require.Empty(t, events[TriggerTypeSingle])

	// A later session update enables the same registered checker without restart.
	s.gate.UpdateDataNode(newJSONPathMigrationQueryNode(2, 5, 10))
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(10000), nil).Once()
	events, err = s.policy.Trigger(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int64{201}, viewSegmentIDs(events[TriggerTypeSingle]))
	index.CurrentIndexVersion = s.version.ResolveVecIndexVersion()
	events, err = s.policy.Trigger(context.Background())
	require.NoError(t, err)
	require.Empty(t, events[TriggerTypeSingle])
}

func TestMigrationCompactionCombinesIndexAndJSONReasons(t *testing.T) {
	for _, autoCompaction := range []string{"true", "false"} {
		t.Run("autoCompaction="+autoCompaction, func(t *testing.T) {
			s := newIndexRebuildFixture(t)
			s.setQueryNodeVersions(6, 10)
			s.gate.UpdateDataNode(newJSONPathMigrationQueryNode(2, 6, 10))
			s.saveParam(&Params.DataCoordCfg.AutoUpgradeSegmentIndex, "true")
			s.saveParam(&Params.DataCoordCfg.EnableAutoCompaction, autoCompaction)
			s.saveParam(&Params.DataCoordCfg.JSONStatsFormatVersion, "4")
			s.saveParam(&Params.CommonCfg.EnabledJSONKeyStats, "true")
			s.saveParam(&Params.DataCoordCfg.JSONStatsTriggerCount, "10")
			collection := &collectionInfo{ID: 100, Schema: newJSONPathMigrationSchema()}
			segment := newJSONPathMigrationSegment(100, 201)
			segment.JsonKeyStats = map[int64]*datapb.JsonKeyStats{
				100: {JsonKeyStatsDataFormat: common.JSONStatsDataFormatV3},
			}
			vector := newJSONPathMigrationSegmentIndex(100, 201, 1001, 0)
			vector.CurrentIndexVersion = 8
			s.setPolicyMeta(collection, map[UniqueID]*SegmentInfo{201: segment}, []*model.Index{
				newJSONPathMigrationIndex(100, 100, 1000, "DOUBLE"),
				newJSONPathMigrationVectorIndex(100, 300, 1001, "HNSW"),
			}, []*model.SegmentIndex{newJSONPathMigrationSegmentIndex(100, 201, 1000, 5), vector})
			s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(collection, nil)
			s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(10000), nil).Once()
			events, err := s.policy.Trigger(context.Background())
			require.NoError(t, err)
			require.Len(t, events[TriggerTypeSingle], 1)
			require.Equal(t, []int64{201}, viewSegmentIDs(events[TriggerTypeSingle]))
		})
	}
}

func TestMigrationCompactionIndexRebuildDispatch(t *testing.T) {
	s := newIndexRebuildFixture(t)
	s.saveParam(&Params.DataCoordCfg.AutoUpgradeSegmentIndex, "true")
	collection := &collectionInfo{ID: 100, Schema: newJSONPathMigrationSchema()}
	segment := newJSONPathMigrationSegment(100, 201)
	s.setPolicyMeta(collection, map[UniqueID]*SegmentInfo{201: segment},
		[]*model.Index{newJSONPathMigrationIndex(100, 200, 1000, "")},
		[]*model.SegmentIndex{newJSONPathMigrationSegmentIndex(100, 201, 1000, 1)})
	s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(collection, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(10000), nil).Once()
	s.mockAlloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		return 20000, 20000 + n, nil
	}).Once()
	inspector := NewMockCompactionInspector(t)
	inspector.EXPECT().isFull().Return(false)
	inspector.EXPECT().enqueueCompaction(mock.Anything).RunAndReturn(func(task *datapb.CompactionTask) error {
		require.Equal(t, datapb.CompactionType_MixCompaction, task.GetType())
		require.Equal(t, []int64{201}, task.GetInputSegments())
		require.Equal(t, int64(10000), task.GetTriggerID())
		require.Equal(t, collection.Schema, task.GetSchema())
		// Real admission marks the input compacting before returning.
		segment.isCompacting = true
		return nil
	}).Once()
	manager := NewCompactionTriggerManager(s.mockAlloc, s.handler, inspector, s.policy.meta, s.version)
	manager.handleTicker(context.Background(), MigrationCompactionTicker)
	manager.handleTicker(context.Background(), MigrationCompactionTicker)
}
