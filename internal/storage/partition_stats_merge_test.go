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

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func mergeCentroids(values ...float32) *clusteringpb.ClusteringCentroidsStats {
	result := &clusteringpb.ClusteringCentroidsStats{}
	for _, value := range values {
		result.Centroids = append(result.Centroids, &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{value, value}}}})
	}
	return result
}

func TestClusteringHalfSnapshotMerge(t *testing.T) {
	for _, dtype := range []schemapb.DataType{schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector} {
		t.Run(dtype.String(), func(t *testing.T) {
			base := clusteringSnapshotFixture()
			base.Version = 7
			base.ClusteringStats.DataType = dtype
			base.ClusteringStats.NumClusters = 2
			base.ClusteringStats.Groups = []*clusteringpb.ClusteringGroup{{SegmentIds: []int64{10}}, {SegmentIds: []int64{20}}}
			base.SegmentStats = map[int64]SegmentStats{
				10: {NumRows: 2, ClusteringRanges: []*clusteringpb.ClusteringRange{{ClusterOffset: 0, RowCount: 2}}},
				20: {NumRows: 2, ClusteringRanges: []*clusteringpb.ClusteringRange{{ClusterOffset: 1, RowCount: 2}}},
			}
			field := &schemapb.FieldSchema{FieldID: 100, DataType: dtype, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}}
			old, next := mergeCentroids(10, 20), mergeCentroids(30)
			for _, table := range []*clusteringpb.ClusteringCentroidsStats{old, next} {
				for _, c := range table.Centroids {
					b, err := typeutil.ConvertFloat32ToFP16BF16Bytes(c.GetFloatVector().Data, dtype)
					require.NoError(t, err)
					if dtype == schemapb.DataType_Float16Vector {
						c.Data = &schemapb.VectorField_Float16Vector{Float16Vector: b}
					} else {
						c.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: b}
					}
				}
			}
			draft, err := MergeClusteringSnapshot(base, old, []int64{10}, map[int64]SegmentStats{30: {NumRows: 2, ClusteringRanges: []*clusteringpb.ClusteringRange{{ClusterOffset: 0, RowCount: 2}}}}, []*clusteringpb.ClusteringGroup{{SegmentIds: []int64{30}}}, next, field, 8)
			require.NoError(t, err)
			require.True(t, proto.Equal(old.Centroids[1], draft.Centroids.Centroids[0]))
			require.True(t, proto.Equal(next.Centroids[0], draft.Centroids.Centroids[1]))
			require.NotSame(t, old.Centroids[1], draft.Centroids.Centroids[0])
			final, err := draft.Finalize("8.clusters", base.ClusteringStats.ClusteringIndex)
			require.NoError(t, err)
			blob, err := SerializePartitionStatsSnapshot(final)
			require.NoError(t, err)
			restored, err := DeserializePartitionsStatsSnapshot(blob)
			require.NoError(t, err)
			require.True(t, proto.Equal(final.ClusteringStats, restored.ClusteringStats))
			require.Equal(t, uint32(0), restored.SegmentStats[20].ClusteringRanges[0].ClusterOffset)
			require.Equal(t, uint32(1), restored.SegmentStats[30].ClusteringRanges[0].ClusterOffset)
		})
	}
}

func TestClusteringSnapshotMergeWholeGroups(t *testing.T) {
	base := clusteringSnapshotFixture()
	base.Version = 7
	base.ClusteringStats.NumClusters = 4
	base.ClusteringStats.Groups = []*clusteringpb.ClusteringGroup{{SegmentIds: []int64{10, 20}}, {SegmentIds: []int64{30, 40}}}
	base.SegmentStats = map[int64]SegmentStats{}
	for i, id := range []int64{10, 20, 30, 40} {
		base.SegmentStats[id] = SegmentStats{NumRows: 2, ClusteringRanges: []*clusteringpb.ClusteringRange{{ClusterOffset: uint32(i), RowCount: 2}}}
	}
	field := &schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}}
	oldCentroids, newCentroids := mergeCentroids(10, 20, 30, 40), mergeCentroids(50, 60, 70)
	outputs := map[int64]SegmentStats{
		60: {NumRows: 2, ClusteringRanges: []*clusteringpb.ClusteringRange{{ClusterOffset: 0, RowCount: 2}}},
		70: {NumRows: 3, ClusteringRanges: []*clusteringpb.ClusteringRange{{ClusterOffset: 2, RowCount: 3}}},
	}
	groups := []*clusteringpb.ClusteringGroup{{SegmentIds: []int64{60, 70}}}
	draft, err := MergeClusteringSnapshot(base, oldCentroids, []int64{10, 20, 50}, outputs, groups, newCentroids, field, 8)
	require.NoError(t, err)
	require.Len(t, draft.Centroids.Centroids, 4, "empty Analyze cluster is removed")
	require.Equal(t, []float32{30, 30}, draft.Centroids.Centroids[0].GetFloatVector().Data)
	require.Equal(t, []float32{70, 70}, draft.Centroids.Centroids[3].GetFloatVector().Data)
	require.Equal(t, []int64{30, 40}, draft.Snapshot.ClusteringStats.Groups[0].SegmentIds)
	require.Equal(t, uint32(0), draft.Snapshot.SegmentStats[30].ClusteringRanges[0].ClusterOffset)
	require.Equal(t, uint32(3), draft.Snapshot.SegmentStats[70].ClusteringRanges[0].ClusterOffset)
	_, err = draft.Finalize("8.clusters", nil)
	require.Error(t, err, "nonempty snapshot requires completed index")
	index := &clusteringpb.ClusteringIndexInfo{IndexParams: []*commonpb.KeyValuePair{{Key: "index_type", Value: "FLAT"}, {Key: "metric_type", Value: "L2"}}, IndexEngineVersion: 10, IndexSize: 32, IndexFilePaths: []string{"8.clustering_index/data"}}
	final, err := draft.Finalize("8.clusters", index)
	require.NoError(t, err)
	_, err = SerializePartitionStatsSnapshot(final)
	require.NoError(t, err)
	final.SegmentStats[30].ClusteringRanges[0].RowCount = 99
	final.ClusteringStats.Groups[0].SegmentIds[0] = 999
	draft.Centroids.Centroids[0].GetFloatVector().Data[0] = -1
	require.Equal(t, int64(2), base.SegmentStats[30].ClusteringRanges[0].RowCount)
	require.Equal(t, int64(30), base.ClusteringStats.Groups[1].SegmentIds[0])
	require.Equal(t, float32(30), oldCentroids.Centroids[2].GetFloatVector().Data[0])
	_, err = MergeClusteringSnapshot(base, oldCentroids, []int64{10, 50}, outputs, groups, newCentroids, field, 8)
	require.ErrorIs(t, err, merr.ErrCompactionPlanConflict, "new snapshot validation cannot detect a split old group")
	retained, err := MergeClusteringSnapshot(base, oldCentroids, []int64{10, 20}, nil, nil, newCentroids, field, 8)
	require.NoError(t, err)
	require.Len(t, retained.Snapshot.SegmentStats, 2, "all delta rows deleted does not erase retained groups")
	require.Len(t, retained.Centroids.Centroids, 2)
	empty, err := MergeClusteringSnapshot(base, oldCentroids, []int64{10, 20, 30, 40}, nil, nil, newCentroids, field, 8)
	require.NoError(t, err)
	_, err = empty.Finalize("", nil)
	require.NoError(t, err)
	require.Empty(t, empty.Snapshot.ClusteringStats.Groups)
	require.Empty(t, empty.Centroids.Centroids)
}
