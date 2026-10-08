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

package analyzecgowrapper

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/clustercompaction"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
)

func TestBuildCompactionPlan(t *testing.T) {
	centroids := &clusteringpb.ClusteringCentroidsStats{
		Centroids: []*schemapb.VectorField{
			newFloatCentroid(0, 0),
			newFloatCentroid(1, 1),
			newFloatCentroid(2, 2),
			newFloatCentroid(3, 3),
			newFloatCentroid(4, 4),
		},
	}

	plan, err := BuildCompactionPlan(
		centroids,
		schemapb.DataType_FloatVector,
		[]int64{40, 0, 70, 20, 150},
		"KMEANS",
		map[string]string{"planner": "ivf", "compaction_max_rows": "100"},
	)
	require.NoError(t, err)
	require.Equal(t, clustercompaction.LayoutPlanFormat, plan.Format)
	require.EqualValues(t, 280, plan.RowCount)
	require.EqualValues(t, 5, plan.CentroidCount)
	require.Equal(t, []int64{40, 0, 70, 20, 150}, plan.CentroidCounts)
	require.Equal(t, []clustercompaction.CentroidGroup{
		{CentroidGroupID: 0, Rows: 40, Centroids: []uint32{0}},
		{CentroidGroupID: 1, Rows: 90, Centroids: []uint32{2, 3}},
		{CentroidGroupID: 2, Rows: 150, Centroids: []uint32{4}},
	}, plan.CentroidGroups)
}

func TestBuildCompactionPlanLowPrecisionCentroids(t *testing.T) {
	for _, fieldType := range []schemapb.DataType{
		schemapb.DataType_Float16Vector,
		schemapb.DataType_BFloat16Vector,
	} {
		t.Run(fieldType.String(), func(t *testing.T) {
			centroids := &clusteringpb.ClusteringCentroidsStats{
				Centroids: []*schemapb.VectorField{
					newHalfCentroid(fieldType, 2),
					newHalfCentroid(fieldType, 2),
				},
			}
			plan, err := BuildCompactionPlan(
				centroids,
				fieldType,
				[]int64{40, 60},
				"KMEANS",
				map[string]string{"planner": "ivf", "compaction_max_rows": "100"},
			)
			require.NoError(t, err)
			require.EqualValues(t, 100, plan.RowCount)
			require.EqualValues(t, 2, plan.CentroidCount)
		})
	}
}

func TestBuildCompactionPlanRejectsInvalidInput(t *testing.T) {
	centroids := &clusteringpb.ClusteringCentroidsStats{
		Centroids: []*schemapb.VectorField{newFloatCentroid(0, 0)},
	}

	_, err := BuildCompactionPlan(
		centroids,
		schemapb.DataType_FloatVector,
		[]int64{-1},
		"KMEANS",
		map[string]string{"planner": "ivf", "compaction_max_rows": "100"},
	)
	require.ErrorContains(t, err, "must be non-negative")

	_, err = BuildCompactionPlan(
		centroids,
		schemapb.DataType_FloatVector,
		[]int64{1},
		"KMEANS",
		map[string]string{"planner": "metis", "compaction_max_rows": "100"},
	)
	require.Error(t, err)

	_, err = BuildCompactionPlan(
		centroids,
		schemapb.DataType_Float16Vector,
		[]int64{1},
		"KMEANS",
		map[string]string{"planner": "ivf", "compaction_max_rows": "100"},
	)
	require.ErrorContains(t, err, "differs from Float16Vector")
}

func newFloatCentroid(values ...float32) *schemapb.VectorField {
	return &schemapb.VectorField{
		Dim: int64(len(values)),
		Data: &schemapb.VectorField_FloatVector{
			FloatVector: &schemapb.FloatArray{Data: values},
		},
	}
}

func newHalfCentroid(fieldType schemapb.DataType, dim int) *schemapb.VectorField {
	data := make([]byte, dim*2)
	centroid := &schemapb.VectorField{Dim: int64(dim)}
	if fieldType == schemapb.DataType_Float16Vector {
		centroid.Data = &schemapb.VectorField_Float16Vector{Float16Vector: data}
	} else {
		centroid.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: data}
	}
	return centroid
}
