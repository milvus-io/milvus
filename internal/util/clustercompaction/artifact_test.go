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

package clustercompaction

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
)

func TestBuildAnalyzeArtifactPaths(t *testing.T) {
	paths := BuildAnalyzeArtifactPaths("root", 11, 2, 21, 22, 23)

	require.Equal(t, "root/analyze_stats/11/2", paths.Root)
	require.Equal(t, "root/analyze_stats/11/2/21/22/23", paths.FieldRoot)
	require.Equal(t, "root/analyze_stats/11/2/21/22/23/centroids", paths.Centroids)
	require.Equal(t, "root/analyze_stats/11/2/21/22/23/31/offset_mapping", paths.SegmentOffsetMapping(31))
}

func TestValidateCentroidMappingStats(t *testing.T) {
	valid := func() *clusteringpb.ClusteringCentroidIdMappingStats {
		return &clusteringpb.ClusteringCentroidIdMappingStats{
			CentroidIdMapping:  []uint32{0, 1, 0},
			NumInCentroid:      []int64{2, 1, 0},
			DistanceToCentroid: []float32{0.1, 0.2, 0.3},
		}
	}

	require.NoError(t, ValidateCentroidMappingStats(valid(), 3, 3))

	tests := []struct {
		name   string
		mutate func(*clusteringpb.ClusteringCentroidIdMappingStats)
	}{
		{"mapping length", func(stats *clusteringpb.ClusteringCentroidIdMappingStats) {
			stats.CentroidIdMapping = stats.CentroidIdMapping[:2]
		}},
		{"distance length", func(stats *clusteringpb.ClusteringCentroidIdMappingStats) {
			stats.DistanceToCentroid = stats.DistanceToCentroid[:2]
		}},
		{"non-finite distance", func(stats *clusteringpb.ClusteringCentroidIdMappingStats) {
			stats.DistanceToCentroid[1] = float32(math.NaN())
		}},
		{"counts length", func(stats *clusteringpb.ClusteringCentroidIdMappingStats) {
			stats.NumInCentroid = stats.NumInCentroid[:2]
		}},
		{"count sum", func(stats *clusteringpb.ClusteringCentroidIdMappingStats) {
			stats.NumInCentroid[0] = 1
		}},
		{"centroid id range", func(stats *clusteringpb.ClusteringCentroidIdMappingStats) {
			stats.CentroidIdMapping[0] = 3
		}},
		{"count mismatch", func(stats *clusteringpb.ClusteringCentroidIdMappingStats) {
			stats.NumInCentroid[0], stats.NumInCentroid[1] = 1, 2
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stats := valid()
			test.mutate(stats)
			require.Error(t, ValidateCentroidMappingStats(stats, 3, 3))
		})
	}
}
