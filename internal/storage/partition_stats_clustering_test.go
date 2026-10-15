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
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func clusteringSnapshotFixture() *PartitionStatsSnapshot {
	return &PartitionStatsSnapshot{
		Version: 9007199254740993,
		ClusteringStats: &clusteringpb.PartitionClusteringStats{
			FormatVersion: PartitionClusteringStatsFormatVersion, FieldId: 100,
			DataType: schemapb.DataType_FloatVector, Dim: 2,
			NumClusters: 2, ClustersFile: "files/part_stats/1/2/channel/9007199254740993.clusters",
			Groups: []*clusteringpb.ClusteringGroup{{SegmentIds: []int64{10, 20}}},
			ClusteringIndex: &clusteringpb.ClusteringIndexInfo{
				IndexParams: []*commonpb.KeyValuePair{
					{Key: "index_type", Value: "HNSW"},
					{Key: "metric_type", Value: "L2"},
					{Key: "M", Value: "16"},
				},
				IndexFilePaths:     []string{"files/part_stats/1/2/channel/9007199254740993.clustering_index/HNSW"},
				IndexEngineVersion: 9,
				IndexSize:          4096,
			},
		},
		SegmentStats: map[int64]SegmentStats{
			10: {NumRows: 5, ClusteringRanges: []*clusteringpb.ClusteringRange{
				{ClusterOffset: 0, RowOffset: 0, RowCount: 2},
				{ClusterOffset: 1, RowOffset: 2, RowCount: 3},
			}},
			20: {NumRows: 1, ClusteringRanges: []*clusteringpb.ClusteringRange{
				{ClusterOffset: 1, RowOffset: 0, RowCount: 1},
			}},
		},
	}
}

func TestClusteringSnapshotRoundTripAndRemap(t *testing.T) {
	original := clusteringSnapshotFixture()
	data, err := SerializePartitionStatsSnapshot(original)
	require.NoError(t, err)
	decoded, err := DeserializePartitionsStatsSnapshot(data)
	require.NoError(t, err)
	require.Equal(t, original.Version, decoded.Version)
	require.True(t, proto.Equal(original.ClusteringStats, decoded.ClusteringStats))
	for id, stats := range original.SegmentStats {
		require.Equal(t, stats.NumRows, decoded.SegmentStats[id].NumRows)
		for i, entry := range stats.ClusteringRanges {
			require.True(t, proto.Equal(entry, decoded.SegmentStats[id].ClusteringRanges[i]))
		}
	}

	// A new complete snapshot may renumber retained clusters. Physical ranges
	// keep their row order; numerical cluster offsets need not be sorted.
	decoded.Version++
	// The merger writes the reordered vectors to a new immutable file. This
	// metadata test checks reference ownership; it does not perform file IO.
	decoded.ClusteringStats.ClustersFile = "files/part_stats/1/2/channel/9007199254740994.clusters"
	decoded.ClusteringStats.ClusteringIndex.IndexFilePaths[0] = "files/part_stats/1/2/channel/9007199254740994.clustering_index/HNSW"
	// Segment membership is independent of the cluster offset order. Mutating
	// the decoded member slice must not change the published snapshot's group.
	decoded.ClusteringStats.Groups[0].SegmentIds[0] = 20
	decoded.ClusteringStats.Groups[0].SegmentIds[1] = 10
	for _, stats := range decoded.SegmentStats {
		for _, entry := range stats.ClusteringRanges {
			entry.ClusterOffset = 1 - entry.ClusterOffset
		}
	}
	require.NoError(t, decoded.ValidateClusteringStats())
	require.Equal(t, []int64{10, 20}, original.ClusteringStats.Groups[0].SegmentIds)
	require.Equal(t, uint32(0), original.SegmentStats[10].ClusteringRanges[0].ClusterOffset)
	require.Equal(t, "files/part_stats/1/2/channel/9007199254740993.clusters", original.ClusteringStats.ClustersFile)
	require.Equal(t, "files/part_stats/1/2/channel/9007199254740993.clustering_index/HNSW", original.ClusteringStats.ClusteringIndex.IndexFilePaths[0])
	unchanged, err := SerializePartitionStatsSnapshot(original)
	require.NoError(t, err)
	require.JSONEq(t, string(data), string(unchanged))
}

func TestClusteringSnapshotLegacyJSON(t *testing.T) {
	for _, document := range []string{
		`{"segmentStats":null}`,
		`{"segmentStats":{},"Version":123}`,
		`{"segmentStats":{"10":{"NumRows":5,"fieldStats":[{"fieldID":100,"type":5,"min":1,"max":5}]}},"Version":123}`,
		`{"segmentStats":{"10":{"NumRows":5,"fieldStats":[{"fieldID":101,"type":101,"centroids":[[1,2]]}]}},"Version":123}`,
	} {
		decoded, err := DeserializePartitionsStatsSnapshot([]byte(document))
		require.NoError(t, err, document)
		require.Nil(t, decoded.ClusteringStats)
		encoded, err := SerializePartitionStatsSnapshot(decoded)
		require.NoError(t, err)
		require.NotContains(t, string(encoded), "clusteringStats")
		require.NotContains(t, string(encoded), "clusteringRanges")
		again, err := DeserializePartitionsStatsSnapshot(encoded)
		require.NoError(t, err)
		require.Equal(t, decoded, again)
	}
}

func TestClusteringSnapshotEmptyAndLargeRanges(t *testing.T) {
	snapshot := clusteringSnapshotFixture()
	snapshot.ClusteringStats.NumClusters = 0
	snapshot.ClusteringStats.ClustersFile = ""
	snapshot.ClusteringStats.ClusteringIndex = nil
	snapshot.ClusteringStats.Groups = nil
	snapshot.SegmentStats = map[int64]SegmentStats{}
	encoded, err := SerializePartitionStatsSnapshot(snapshot)
	require.NoError(t, err)
	decoded, err := DeserializePartitionsStatsSnapshot(encoded)
	require.NoError(t, err)
	require.NotNil(t, decoded.ClusteringStats)
	require.Empty(t, decoded.SegmentStats)
	require.Nil(t, decoded.ClusteringStats.ClusteringIndex)
	require.Empty(t, decoded.ClusteringStats.Groups)

	snapshot = clusteringSnapshotFixture()
	snapshot.ClusteringStats.NumClusters = 1
	snapshot.ClusteringStats.ClusteringIndex.IndexSize = 9007199254740993
	const largeSegmentID int64 = 9007199254740993
	snapshot.ClusteringStats.Groups = []*clusteringpb.ClusteringGroup{{SegmentIds: []int64{largeSegmentID}}}
	snapshot.SegmentStats = map[int64]SegmentStats{largeSegmentID: {
		NumRows:          math.MaxInt64,
		ClusteringRanges: []*clusteringpb.ClusteringRange{{RowCount: math.MaxInt64}},
	}}
	encoded, err = SerializePartitionStatsSnapshot(snapshot)
	require.NoError(t, err)
	decoded, err = DeserializePartitionsStatsSnapshot(encoded)
	require.NoError(t, err)
	require.Equal(t, int64(math.MaxInt64), decoded.SegmentStats[largeSegmentID].ClusteringRanges[0].RowCount)
	require.Equal(t, []int64{largeSegmentID}, decoded.ClusteringStats.Groups[0].SegmentIds)
	require.Equal(t, int64(9007199254740993), decoded.ClusteringStats.ClusteringIndex.IndexSize)
}

func TestClusteringSnapshotRejectsInvalidIndex(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*clusteringpb.PartitionClusteringStats)
	}{
		{"missing index", func(s *clusteringpb.PartitionClusteringStats) { s.ClusteringIndex = nil }},
		{"zero engine version", func(s *clusteringpb.PartitionClusteringStats) { s.ClusteringIndex.IndexEngineVersion = 0 }},
		{"negative engine version", func(s *clusteringpb.PartitionClusteringStats) { s.ClusteringIndex.IndexEngineVersion = -1 }},
		{"zero size", func(s *clusteringpb.PartitionClusteringStats) { s.ClusteringIndex.IndexSize = 0 }},
		{"negative size", func(s *clusteringpb.PartitionClusteringStats) { s.ClusteringIndex.IndexSize = -1 }},
		{"missing files", func(s *clusteringpb.PartitionClusteringStats) { s.ClusteringIndex.IndexFilePaths = nil }},
		{"empty file", func(s *clusteringpb.PartitionClusteringStats) { s.ClusteringIndex.IndexFilePaths[0] = "" }},
		{"raw cluster description file", func(s *clusteringpb.PartitionClusteringStats) { s.ClusteringIndex.IndexFilePaths[0] = s.ClustersFile }},
		{"duplicate file", func(s *clusteringpb.PartitionClusteringStats) {
			s.ClusteringIndex.IndexFilePaths = append(s.ClusteringIndex.IndexFilePaths, s.ClusteringIndex.IndexFilePaths[0])
		}},
		{"missing parameters", func(s *clusteringpb.PartitionClusteringStats) { s.ClusteringIndex.IndexParams = nil }},
		{"missing index type", func(s *clusteringpb.PartitionClusteringStats) {
			s.ClusteringIndex.IndexParams = s.ClusteringIndex.IndexParams[1:]
		}},
		{"blank metric", func(s *clusteringpb.PartitionClusteringStats) { s.ClusteringIndex.IndexParams[1].Value = " " }},
		{"nil parameter", func(s *clusteringpb.PartitionClusteringStats) { s.ClusteringIndex.IndexParams[0] = nil }},
		{"empty parameter key", func(s *clusteringpb.PartitionClusteringStats) { s.ClusteringIndex.IndexParams[0].Key = "" }},
		{"duplicate parameter", func(s *clusteringpb.PartitionClusteringStats) {
			s.ClusteringIndex.IndexParams = append(s.ClusteringIndex.IndexParams, &commonpb.KeyValuePair{Key: "metric_type", Value: "IP"})
		}},
		{"index on empty table", func(s *clusteringpb.PartitionClusteringStats) {
			s.NumClusters = 0
			s.ClustersFile = ""
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := clusteringSnapshotFixture()
			tc.mutate(snapshot.ClusteringStats)
			if snapshot.ClusteringStats.NumClusters == 0 {
				snapshot.SegmentStats = map[int64]SegmentStats{}
				snapshot.ClusteringStats.Groups = nil
			}
			_, err := SerializePartitionStatsSnapshot(snapshot)
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
			// Corrupt persisted metadata must also fail on the read boundary.
			encoded, err := json.Marshal(snapshot)
			require.NoError(t, err)
			_, err = DeserializePartitionsStatsSnapshot(encoded)
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
		})
	}
}

func TestClusteringSnapshotGroupMembership(t *testing.T) {
	// Clusters 0/1 share the first group, including cluster 1 spanning two
	// segments. Cluster 2 belongs to a second, disjoint group.
	snapshot := clusteringSnapshotFixture()
	snapshot.ClusteringStats.NumClusters = 3
	snapshot.SegmentStats[30] = SegmentStats{NumRows: 4, ClusteringRanges: []*clusteringpb.ClusteringRange{
		{ClusterOffset: 2, RowCount: 4},
	}}
	snapshot.ClusteringStats.Groups = append(snapshot.ClusteringStats.Groups,
		&clusteringpb.ClusteringGroup{SegmentIds: []int64{30}})
	encoded, err := SerializePartitionStatsSnapshot(snapshot)
	require.NoError(t, err)
	decoded, err := DeserializePartitionsStatsSnapshot(encoded)
	require.NoError(t, err)
	require.True(t, proto.Equal(snapshot.ClusteringStats, decoded.ClusteringStats))

	// Groups may also contain clusters with no shared segment. Membership is
	// defined by the segment directory, not by physical connectivity.
	snapshot.SegmentStats[10] = SegmentStats{NumRows: 5, ClusteringRanges: []*clusteringpb.ClusteringRange{
		{RowCount: 5},
	}}
	require.NoError(t, snapshot.ValidateClusteringStats())

	// Offsets belong to each snapshot: swapping the group order and cluster
	// labels preserves all physical ranges and unique derived memberships.
	snapshot.ClusteringStats.Groups[0], snapshot.ClusteringStats.Groups[1] =
		snapshot.ClusteringStats.Groups[1], snapshot.ClusteringStats.Groups[0]
	for _, stats := range snapshot.SegmentStats {
		for _, entry := range stats.ClusteringRanges {
			entry.ClusterOffset = 2 - entry.ClusterOffset
		}
	}
	require.NoError(t, snapshot.ValidateClusteringStats())
}

func TestClusteringSnapshotRejectsInvalidGroups(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*PartitionStatsSnapshot)
	}{
		{"missing groups", func(s *PartitionStatsSnapshot) { s.ClusteringStats.Groups = nil }},
		{"nil group", func(s *PartitionStatsSnapshot) { s.ClusteringStats.Groups[0] = nil }},
		{"empty group", func(s *PartitionStatsSnapshot) { s.ClusteringStats.Groups[0].SegmentIds = nil }},
		{"unreferenced segment", func(s *PartitionStatsSnapshot) { s.ClusteringStats.Groups[0].SegmentIds = []int64{10} }},
		{"unknown segment", func(s *PartitionStatsSnapshot) { s.ClusteringStats.Groups[0].SegmentIds = []int64{10, 30} }},
		{"zero segment", func(s *PartitionStatsSnapshot) { s.ClusteringStats.Groups[0].SegmentIds = []int64{10, 0} }},
		{"negative segment", func(s *PartitionStatsSnapshot) { s.ClusteringStats.Groups[0].SegmentIds = []int64{10, -20} }},
		{"duplicate member", func(s *PartitionStatsSnapshot) { s.ClusteringStats.Groups[0].SegmentIds = []int64{10, 20, 10} }},
		{"segment in two groups", func(s *PartitionStatsSnapshot) {
			s.ClusteringStats.Groups = append(s.ClusteringStats.Groups, &clusteringpb.ClusteringGroup{SegmentIds: []int64{20}})
		}},
		{"cluster spans groups", func(s *PartitionStatsSnapshot) {
			s.ClusteringStats.Groups = []*clusteringpb.ClusteringGroup{{SegmentIds: []int64{10}}, {SegmentIds: []int64{20}}}
		}},
		{"cluster zero spans groups", func(s *PartitionStatsSnapshot) {
			s.ClusteringStats.Groups = []*clusteringpb.ClusteringGroup{{SegmentIds: []int64{10}}, {SegmentIds: []int64{20}}}
			s.SegmentStats[20].ClusteringRanges[0].ClusterOffset = 0
		}},
		{"groups on empty snapshot", func(s *PartitionStatsSnapshot) {
			s.ClusteringStats.NumClusters = 0
			s.ClusteringStats.ClustersFile = ""
			s.ClusteringStats.ClusteringIndex = nil
			s.SegmentStats = nil
		}},
		{"empty group on empty snapshot", func(s *PartitionStatsSnapshot) {
			s.ClusteringStats.NumClusters = 0
			s.ClusteringStats.ClustersFile = ""
			s.ClusteringStats.ClusteringIndex = nil
			s.ClusteringStats.Groups = []*clusteringpb.ClusteringGroup{{}}
			s.SegmentStats = nil
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := clusteringSnapshotFixture()
			tc.mutate(snapshot)
			_, err := SerializePartitionStatsSnapshot(snapshot)
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
			// Bypass the write guard to verify corrupt persisted groups fail
			// through the real JSON decode and validation boundary as well.
			encoded, err := json.Marshal(snapshot)
			require.NoError(t, err)
			_, err = DeserializePartitionsStatsSnapshot(encoded)
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
		})
	}
}

func TestClusteringSnapshotRejectsInvalidLayout(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*PartitionStatsSnapshot)
	}{
		{"missing table", func(s *PartitionStatsSnapshot) { s.ClusteringStats = nil }},
		{"zero version", func(s *PartitionStatsSnapshot) { s.Version = 0 }},
		{"invalid field", func(s *PartitionStatsSnapshot) { s.ClusteringStats.FieldId = 0 }},
		{"invalid dimension", func(s *PartitionStatsSnapshot) { s.ClusteringStats.Dim = 0 }},
		{"missing cluster description file", func(s *PartitionStatsSnapshot) { s.ClusteringStats.ClustersFile = "" }},
		{"zero cluster count with file", func(s *PartitionStatsSnapshot) { s.ClusteringStats.NumClusters = 0 }},
		{"out of bounds maximum offset", func(s *PartitionStatsSnapshot) { s.SegmentStats[10].ClusteringRanges[0].ClusterOffset = math.MaxUint32 }},
		{"large untrusted count", func(s *PartitionStatsSnapshot) { s.ClusteringStats.NumClusters = math.MaxUint32 }},
		{"invalid segment", func(s *PartitionStatsSnapshot) {
			s.SegmentStats[0] = s.SegmentStats[10]
			s.ClusteringStats.Groups[0].SegmentIds = append(s.ClusteringStats.Groups[0].SegmentIds, 0)
		}},
		{"empty segment", func(s *PartitionStatsSnapshot) {
			s.SegmentStats[30] = SegmentStats{}
			s.ClusteringStats.Groups[0].SegmentIds = append(s.ClusteringStats.Groups[0].SegmentIds, 30)
		}},
		{"nil range", func(s *PartitionStatsSnapshot) { s.SegmentStats[10].ClusteringRanges[0] = nil }},
		{"out of bounds cluster", func(s *PartitionStatsSnapshot) { s.SegmentStats[10].ClusteringRanges[0].ClusterOffset = 2 }},
		{"duplicate cluster", func(s *PartitionStatsSnapshot) { s.SegmentStats[10].ClusteringRanges[1].ClusterOffset = 0 }},
		{"negative start", func(s *PartitionStatsSnapshot) { s.SegmentStats[10].ClusteringRanges[0].RowOffset = -1 }},
		{"empty range", func(s *PartitionStatsSnapshot) { s.SegmentStats[10].ClusteringRanges[0].RowCount = 0 }},
		{"negative count", func(s *PartitionStatsSnapshot) { s.SegmentStats[10].ClusteringRanges[0].RowCount = -1 }},
		{"gap", func(s *PartitionStatsSnapshot) { s.SegmentStats[10].ClusteringRanges[1].RowOffset = 3 }},
		{"overlap", func(s *PartitionStatsSnapshot) { s.SegmentStats[10].ClusteringRanges[1].RowOffset = 1 }},
		{"overflow", func(s *PartitionStatsSnapshot) { s.SegmentStats[10].ClusteringRanges[1].RowCount = math.MaxInt64 }},
		{"incomplete coverage", func(s *PartitionStatsSnapshot) { s.SegmentStats[10].ClusteringRanges[1].RowCount = 2 }},
		{"unreferenced cluster", func(s *PartitionStatsSnapshot) {
			s.ClusteringStats.NumClusters++
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := clusteringSnapshotFixture()
			tc.mutate(snapshot)
			_, err := SerializePartitionStatsSnapshot(snapshot)
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
			// Bypass the serializer's guard to exercise rejection at read time.
			encoded, err := json.Marshal(snapshot)
			if tc.name == "nil range" {
				require.ErrorIs(t, err, merr.ErrDataIntegrity)
				return
			}
			require.NoError(t, err)
			_, err = DeserializePartitionsStatsSnapshot(encoded)
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
		})
	}
}

func TestClusteringSnapshotDecodeErrors(t *testing.T) {
	for _, document := range []string{
		`{`, `null`, `{}`, `{"segmentStats":[]}`,
		`{"segmentStats":{"10":{"clusteringRanges":[null]}}}`,
		`{"segmentStats":{},"clusteringStats":{"numClusters":-1}}`,
		`{"segmentStats":{},"clusteringStats":{"numClusters":4294967296}}`,
		`{"segmentStats":{},"clusteringStats":{"groups":[{"segmentIds":["9223372036854775808"]}]}}`,
		`{"segmentStats":{"10":{"clusteringRanges":[{"rowCount":"9223372036854775808"}]}}}`,
	} {
		_, err := DeserializePartitionsStatsSnapshot([]byte(document))
		require.ErrorIs(t, err, merr.ErrDataIntegrity, document)
	}
	// The field decoder already assigns a code; the new envelope preserves it.
	_, err := DeserializePartitionsStatsSnapshot([]byte(`{"segmentStats":{"10":{"fieldStats":[{}]}}}`))
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Equal(t, merr.Code(merr.ErrServiceInternal), merr.Code(err))

	for _, mutate := range []func(*PartitionStatsSnapshot){
		func(s *PartitionStatsSnapshot) { s.ClusteringStats.FormatVersion = 99 },
		func(s *PartitionStatsSnapshot) { s.ClusteringStats.DataType = schemapb.DataType_Float16Vector },
		func(s *PartitionStatsSnapshot) { s.ClusteringStats.DataType = schemapb.DataType_BFloat16Vector },
		// Scalar metadata has no vector dimension or metric. It must fail as an
		// unsupported capability until typed mapping/query support is available,
		// rather than being mistaken for malformed vector metadata or admitted.
		func(s *PartitionStatsSnapshot) {
			s.ClusteringStats.DataType = schemapb.DataType_Int64
			s.ClusteringStats.Dim = 0
			s.ClusteringStats.ClusteringIndex.IndexParams = []*commonpb.KeyValuePair{{Key: "index_type", Value: "STL_SORT"}}
		},
	} {
		snapshot := clusteringSnapshotFixture()
		mutate(snapshot)
		_, err := SerializePartitionStatsSnapshot(snapshot)
		require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
		encoded, err := json.Marshal(snapshot)
		require.NoError(t, err)
		_, err = DeserializePartitionsStatsSnapshot(encoded)
		require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
	}
}
