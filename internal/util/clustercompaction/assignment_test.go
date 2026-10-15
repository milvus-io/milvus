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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestValidateAssignment(t *testing.T) {
	valid := &clusteringpb.ClusteringCentroidIdMappingStats{CentroidIdMapping: []uint32{1, 0, 1}, DistanceToCentroid: []float32{2, 0, 3}, NumInCentroid: []int64{1, 2}}
	require.NoError(t, ValidateAssignment(valid, 3, 2))
	require.NoError(t, ValidateAssignment(&clusteringpb.ClusteringCentroidIdMappingStats{NumInCentroid: []int64{0, 0}}, 0, 2))
	require.ErrorIs(t, ValidateAssignment(nil, 0, 2), merr.ErrDataIntegrity)
	for name, change := range map[string]func(*clusteringpb.ClusteringCentroidIdMappingStats){
		"short_ids": func(s *clusteringpb.ClusteringCentroidIdMappingStats) { s.CentroidIdMapping = s.CentroidIdMapping[:2] },
		"short_distances": func(s *clusteringpb.ClusteringCentroidIdMappingStats) {
			s.DistanceToCentroid = s.DistanceToCentroid[:2]
		},
		"extra_counts":             func(s *clusteringpb.ClusteringCentroidIdMappingStats) { s.NumInCentroid = append(s.NumInCentroid, 0) },
		"invalid_offset":           func(s *clusteringpb.ClusteringCentroidIdMappingStats) { s.CentroidIdMapping[0] = 2 },
		"nan":                      func(s *clusteringpb.ClusteringCentroidIdMappingStats) { s.DistanceToCentroid[0] = float32(math.NaN()) },
		"infinite":                 func(s *clusteringpb.ClusteringCentroidIdMappingStats) { s.DistanceToCentroid[0] = float32(math.Inf(1)) },
		"negative_distance":        func(s *clusteringpb.ClusteringCentroidIdMappingStats) { s.DistanceToCentroid[0] = -1 },
		"wrong_histogram_same_sum": func(s *clusteringpb.ClusteringCentroidIdMappingStats) { s.NumInCentroid = []int64{2, 1} },
		"negative_count":           func(s *clusteringpb.ClusteringCentroidIdMappingStats) { s.NumInCentroid[0] = -1 },
	} {
		t.Run(name, func(t *testing.T) {
			s := proto.Clone(valid).(*clusteringpb.ClusteringCentroidIdMappingStats)
			change(s)
			require.ErrorIs(t, ValidateAssignment(s, 3, 2), merr.ErrDataIntegrity)
		})
	}
}
