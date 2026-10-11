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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func clusteringSnapshotFixture() *PartitionStatsSnapshot {
	vector := func(values ...float32) *schemapb.VectorField {
		return &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{
			FloatVector: &schemapb.FloatArray{Data: values},
		}}
	}
	return &PartitionStatsSnapshot{
		Version: 9007199254740993,
		ClusteringStats: &clusteringpb.PartitionClusteringStats{
			FormatVersion: PartitionClusteringStatsFormatVersion, FieldId: 100,
			DataType: schemapb.DataType_FloatVector, Dim: 2,
			Centroids: []*schemapb.VectorField{vector(1, 2), vector(3, 4)},
		},
		SegmentStats: map[int64]SegmentStats{
			10: {NumRows: 5, CentroidRanges: []*clusteringpb.ClusteringCentroidRange{
				{CentroidOffset: 0, RowOffset: 0, RowCount: 2},
				{CentroidOffset: 1, RowOffset: 2, RowCount: 3},
			}},
			20: {NumRows: 1, CentroidRanges: []*clusteringpb.ClusteringCentroidRange{
				{CentroidOffset: 1, RowOffset: 0, RowCount: 1},
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
		for i, entry := range stats.CentroidRanges {
			require.True(t, proto.Equal(entry, decoded.SegmentStats[id].CentroidRanges[i]))
		}
	}

	// A new complete snapshot may renumber retained centroids. Physical ranges
	// keep their row order; numerical centroid offsets need not be sorted.
	decoded.Version++
	centroids := decoded.ClusteringStats.Centroids
	centroids[0], centroids[1] = centroids[1], centroids[0]
	for _, stats := range decoded.SegmentStats {
		for _, entry := range stats.CentroidRanges {
			entry.CentroidOffset = 1 - entry.CentroidOffset
		}
	}
	require.NoError(t, decoded.ValidateClusteringStats())
	require.Equal(t, uint32(0), original.SegmentStats[10].CentroidRanges[0].CentroidOffset)
	require.Equal(t, float32(1), original.ClusteringStats.Centroids[0].GetFloatVector().Data[0])
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
		require.NotContains(t, string(encoded), "centroidRanges")
		again, err := DeserializePartitionsStatsSnapshot(encoded)
		require.NoError(t, err)
		require.Equal(t, decoded, again)
	}
}

func TestClusteringSnapshotEmptyAndLargeRanges(t *testing.T) {
	snapshot := clusteringSnapshotFixture()
	snapshot.ClusteringStats.Centroids = nil
	snapshot.SegmentStats = map[int64]SegmentStats{}
	encoded, err := SerializePartitionStatsSnapshot(snapshot)
	require.NoError(t, err)
	decoded, err := DeserializePartitionsStatsSnapshot(encoded)
	require.NoError(t, err)
	require.NotNil(t, decoded.ClusteringStats)
	require.Empty(t, decoded.SegmentStats)

	snapshot = clusteringSnapshotFixture()
	snapshot.ClusteringStats.Centroids = snapshot.ClusteringStats.Centroids[:1]
	snapshot.SegmentStats = map[int64]SegmentStats{10: {
		NumRows:        math.MaxInt64,
		CentroidRanges: []*clusteringpb.ClusteringCentroidRange{{RowCount: math.MaxInt64}},
	}}
	encoded, err = SerializePartitionStatsSnapshot(snapshot)
	require.NoError(t, err)
	decoded, err = DeserializePartitionsStatsSnapshot(encoded)
	require.NoError(t, err)
	require.Equal(t, int64(math.MaxInt64), decoded.SegmentStats[10].CentroidRanges[0].RowCount)
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
		{"nil centroid", func(s *PartitionStatsSnapshot) { s.ClusteringStats.Centroids[0] = nil }},
		{"wrong centroid dimension", func(s *PartitionStatsSnapshot) { s.ClusteringStats.Centroids[0].Dim = 3 }},
		{"wrong vector width", func(s *PartitionStatsSnapshot) { s.ClusteringStats.Centroids[0].GetFloatVector().Data = []float32{1} }},
		{"missing vector", func(s *PartitionStatsSnapshot) { s.ClusteringStats.Centroids[0].Data = nil }},
		{"nan", func(s *PartitionStatsSnapshot) {
			s.ClusteringStats.Centroids[0].GetFloatVector().Data[0] = float32(math.NaN())
		}},
		{"infinity", func(s *PartitionStatsSnapshot) {
			s.ClusteringStats.Centroids[0].GetFloatVector().Data[0] = float32(math.Inf(1))
		}},
		{"invalid segment", func(s *PartitionStatsSnapshot) { s.SegmentStats[0] = s.SegmentStats[10] }},
		{"empty segment", func(s *PartitionStatsSnapshot) { s.SegmentStats[30] = SegmentStats{} }},
		{"nil range", func(s *PartitionStatsSnapshot) { s.SegmentStats[10].CentroidRanges[0] = nil }},
		{"out of bounds centroid", func(s *PartitionStatsSnapshot) { s.SegmentStats[10].CentroidRanges[0].CentroidOffset = 2 }},
		{"duplicate centroid", func(s *PartitionStatsSnapshot) { s.SegmentStats[10].CentroidRanges[1].CentroidOffset = 0 }},
		{"negative start", func(s *PartitionStatsSnapshot) { s.SegmentStats[10].CentroidRanges[0].RowOffset = -1 }},
		{"empty range", func(s *PartitionStatsSnapshot) { s.SegmentStats[10].CentroidRanges[0].RowCount = 0 }},
		{"negative count", func(s *PartitionStatsSnapshot) { s.SegmentStats[10].CentroidRanges[0].RowCount = -1 }},
		{"gap", func(s *PartitionStatsSnapshot) { s.SegmentStats[10].CentroidRanges[1].RowOffset = 3 }},
		{"overlap", func(s *PartitionStatsSnapshot) { s.SegmentStats[10].CentroidRanges[1].RowOffset = 1 }},
		{"overflow", func(s *PartitionStatsSnapshot) { s.SegmentStats[10].CentroidRanges[1].RowCount = math.MaxInt64 }},
		{"incomplete coverage", func(s *PartitionStatsSnapshot) { s.SegmentStats[10].CentroidRanges[1].RowCount = 2 }},
		{"unreferenced centroid", func(s *PartitionStatsSnapshot) {
			s.ClusteringStats.Centroids = append(s.ClusteringStats.Centroids, proto.Clone(s.ClusteringStats.Centroids[0]).(*schemapb.VectorField))
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
		`{"segmentStats":{"10":{"centroidRanges":[null]}}}`,
		`{"segmentStats":{},"clusteringStats":{"centroids":[{"dim":"2","floatVector":{"data":[1,2]},"binaryVector":"AA=="}]}}`,
		`{"segmentStats":{"10":{"centroidRanges":[{"rowCount":"9223372036854775808"}]}}}`,
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
