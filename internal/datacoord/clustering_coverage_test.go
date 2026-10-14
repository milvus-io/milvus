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
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func coverageSnapshot() *storage.PartitionStatsSnapshot {
	stats := &storage.PartitionStatsSnapshot{Version: 7, SegmentStats: map[int64]storage.SegmentStats{}, ClusteringStats: &clusteringpb.PartitionClusteringStats{
		FormatVersion: storage.PartitionClusteringStatsFormatVersion, FieldId: 100, DataType: schemapb.DataType_FloatVector, Dim: 2,
		NumClusters: 4, ClustersFile: "part_stats/1/2/ch/7.clusters",
		Groups:          []*clusteringpb.ClusteringGroup{{SegmentIds: []int64{10, 20}}, {SegmentIds: []int64{30, 40}}},
		ClusteringIndex: &clusteringpb.ClusteringIndexInfo{IndexParams: []*commonpb.KeyValuePair{{Key: "index_type", Value: "FLAT"}, {Key: "metric_type", Value: "L2"}}, IndexFilePaths: []string{"part_stats/1/2/ch/7.clustering_index/data"}, IndexEngineVersion: 10, IndexSize: 1024},
	}}
	for i, id := range []int64{10, 20, 30, 40} {
		stats.SegmentStats[id] = storage.SegmentStats{NumRows: 2, ClusteringRanges: []*clusteringpb.ClusteringRange{{ClusterOffset: uint32(i), RowCount: 2}}}
	}
	return stats
}

func TestClusteringInputClosure(t *testing.T) {
	base := coverageSnapshot()
	require.NoError(t, base.ValidateClusteringStats())
	for _, tc := range []struct{ input, expected []int64 }{
		{[]int64{10, 50}, []int64{10, 20, 50}}, // A/B have disjoint clusters, but are one group.
		{[]int64{30}, []int64{30, 40}},
		{[]int64{10, 40}, []int64{10, 20, 30, 40}},
		{[]int64{50}, []int64{50}},
		{nil, []int64{}},
	} {
		got, err := clusteringInputClosure(base, tc.input)
		require.NoError(t, err)
		require.ElementsMatch(t, tc.expected, got)
		require.NoError(t, validateClusteringInputGroups(base, got))
	}
	require.ErrorIs(t, validateClusteringInputGroups(base, []int64{10, 50}), merr.ErrCompactionPlanConflict)
	require.Equal(t, []int64{10, 20}, base.ClusteringStats.Groups[0].SegmentIds)
	// Same cluster may span both members, and remains in one group.
	base.SegmentStats[20] = storage.SegmentStats{NumRows: 2, ClusteringRanges: []*clusteringpb.ClusteringRange{{ClusterOffset: 0, RowCount: 1}, {ClusterOffset: 1, RowOffset: 1, RowCount: 1}}}
	got, err := clusteringInputClosure(base, []int64{20})
	require.NoError(t, err)
	require.Equal(t, []int64{10, 20}, got)
	for _, mutate := range []func(*storage.PartitionStatsSnapshot){
		func(s *storage.PartitionStatsSnapshot) {
			s.ClusteringStats.Groups[1].SegmentIds = append(s.ClusteringStats.Groups[1].SegmentIds, 10)
		},
		func(s *storage.PartitionStatsSnapshot) { s.SegmentStats[30].ClusteringRanges[0].ClusterOffset = 0 },
		func(s *storage.PartitionStatsSnapshot) { s.SegmentStats[10].ClusteringRanges[0].RowOffset = 1 },
	} {
		bad := coverageSnapshot()
		mutate(bad)
		_, err := clusteringInputClosure(bad, []int64{10})
		require.Error(t, err)
	}
	_, err = clusteringInputClosure(base, []int64{10, 10})
	require.Error(t, err)
	legacy := coverageSnapshot()
	legacy.ClusteringStats = nil
	for id := range legacy.SegmentStats {
		legacy.SegmentStats[id] = storage.SegmentStats{NumRows: 2}
	}
	got, err = clusteringInputClosure(legacy, []int64{50})
	require.NoError(t, err)
	require.Equal(t, []int64{50}, got)
}

func coverageMeta(t *testing.T) (*meta, *ClusteringSegmentsView, string) {
	t.Helper()
	base := coverageSnapshot()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	key := path.Join(cm.RootPath(), "part_stats/1/2/ch/7")
	blob, err := storage.SerializePartitionStatsSnapshot(base)
	require.NoError(t, err)
	require.NoError(t, cm.Write(context.Background(), key, blob))
	m := &meta{chunkManager: cm, segments: NewSegmentsInfo(), partitionStatsMeta: &partitionStatsMeta{partitionStatsInfos: map[string]map[int64]*partitionStatsInfo{
		"ch": {2: {currentVersion: 7, infos: map[int64]*datapb.PartitionStatsInfo{7: {CollectionID: 1, PartitionID: 2, VChannel: "ch", Version: 7, SegmentIDs: []int64{10, 20, 30, 40}}}}},
	}}}
	for _, id := range []int64{10, 20, 30, 40, 50} {
		m.segments.SetSegment(id, NewSegmentInfo(&datapb.SegmentInfo{ID: id, CollectionID: 1, PartitionID: 2, InsertChannel: "ch", State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L2, NumOfRows: 2}))
	}
	field := &schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}}
	view := &ClusteringSegmentsView{label: &CompactionGroupLabel{CollectionID: 1, PartitionID: 2, Channel: "ch"}, clusteringKeyField: field, segments: GetViewsByInfo(m.segments.GetSegment(10), m.segments.GetSegment(50))}
	return m, view, key
}

func TestClusteringCoverageSelectionAndBasePin(t *testing.T) {
	ctx := context.Background()
	m, view, key := coverageMeta(t)
	selected, err := prepareClusteringView(ctx, m, view, false)
	require.NoError(t, err)
	require.Equal(t, int64(7), selected.basePartitionStatsVersion)
	var ids []int64
	for _, s := range selected.segments {
		ids = append(ids, s.ID)
	}
	require.Equal(t, []int64{10, 20, 50}, ids)
	require.Len(t, view.segments, 2, "selection must not mutate original view")
	task := &clusteringCompactionTask{meta: m}
	task.SetTask(&datapb.CompactionTask{CollectionID: 1, PartitionID: 2, Channel: "ch", BasePartitionStatsVersion: 7, ClusteringKeyField: view.clusteringKeyField, InputSegments: ids})
	base, err := task.validateClusteringBase(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(7), base.Version)
	// The legacy publisher must not adopt a partial replacement. This fails
	// before touching the (deliberately absent) allocator or worker session.
	require.ErrorIs(t, task.doCompact(1, nil), merr.ErrServiceUnimplemented)
	require.Nil(t, task.plan)
	require.Equal(t, int64(7), m.GetPartitionStatsMeta().GetCurrentPartitionStatsVersion(1, 2, "ch"))
	// A retained group's busy member must not block selection of g0.
	m.SetSegmentCompacting(30, true)
	_, err = prepareClusteringView(ctx, m, view, false)
	require.NoError(t, err)
	m.SetSegmentCompacting(20, true)
	_, err = prepareClusteringView(ctx, m, view, false)
	require.ErrorIs(t, err, merr.ErrCompactionPlanConflict)
	m.SetSegmentCompacting(20, false)
	m.segments.GetSegment(20).InsertChannel = "other"
	_, err = prepareClusteringView(ctx, m, view, false)
	require.Error(t, err)
	m.segments.GetSegment(20).InsertChannel = "ch"
	m.partitionStatsMeta.partitionStatsInfos["ch"][2].currentVersion = 8
	_, err = task.validateClusteringBase(ctx)
	require.ErrorIs(t, err, merr.ErrCompactionPlanConflict)
	m.partitionStatsMeta.partitionStatsInfos["ch"][2].currentVersion = 7
	require.NoError(t, m.chunkManager.Remove(ctx, key))
	_, err = task.validateClusteringBase(ctx)
	require.Error(t, err, "missing nonzero base must not become an initial build")
}

func TestClusteringCoverageRejectsCorruptOrWrongBase(t *testing.T) {
	for _, tc := range []string{"file_version", "field", "scope", "catalog", "corrupt"} {
		t.Run(tc, func(t *testing.T) {
			m, view, key := coverageMeta(t)
			ctx := context.Background()
			switch tc {
			case "file_version":
				b := coverageSnapshot()
				b.Version = 8
				blob, err := storage.SerializePartitionStatsSnapshot(b)
				require.NoError(t, err)
				require.NoError(t, m.chunkManager.Write(ctx, key, blob))
			case "field":
				view.clusteringKeyField.FieldID++
			case "scope":
				m.partitionStatsMeta.partitionStatsInfos["ch"][2].infos[7].CollectionID = 99
			case "catalog":
				m.partitionStatsMeta.partitionStatsInfos["ch"][2].infos[7].SegmentIDs = []int64{10}
			case "corrupt":
				require.NoError(t, m.chunkManager.Write(ctx, key, []byte("bad json")))
			}
			_, err := prepareClusteringView(ctx, m, view, false)
			require.Error(t, err)
		})
	}
}

func TestClusteringCoverageLegacyRebuildUsesLiveSegments(t *testing.T) {
	m, view, key := coverageMeta(t)
	base := coverageSnapshot()
	base.ClusteringStats = nil
	for id := range base.SegmentStats {
		base.SegmentStats[id] = storage.SegmentStats{NumRows: 2}
	}
	blob, err := storage.SerializePartitionStatsSnapshot(base)
	require.NoError(t, err)
	require.NoError(t, m.chunkManager.Write(context.Background(), key, blob))
	// Old segment A has already been replaced outside clustering. Its old stats
	// must not make a full rebuild impossible or cause it to be resurrected.
	m.segments.GetSegment(10).State = commonpb.SegmentState_Dropped
	view.segments = GetViewsByInfo(m.segments.GetSegment(50))
	next, err := prepareClusteringView(context.Background(), m, view, false)
	require.NoError(t, err)
	var ids []int64
	for _, segment := range next.segments {
		ids = append(ids, segment.ID)
	}
	require.Equal(t, []int64{20, 30, 40, 50}, ids)
	m.SetSegmentCompacting(30, true)
	_, err = prepareClusteringView(context.Background(), m, view, false)
	require.ErrorIs(t, err, merr.ErrCompactionPlanConflict)
}
