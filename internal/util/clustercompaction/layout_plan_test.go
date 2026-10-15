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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestLayoutPlanCoverage(t *testing.T) {
	tests := []struct {
		name  string
		plan  *LayoutPlan
		valid bool
	}{
		{"complete", &LayoutPlan{Counts: []int64{3, 0, 8}, Groups: []ClusterGroup{{Offsets: []uint32{2, 0}, Rows: 11}}}, true},
		{"oversized_indivisible", &LayoutPlan{Counts: []int64{math.MaxInt64}, Groups: []ClusterGroup{{Offsets: []uint32{0}, Rows: math.MaxInt64}}}, true},
		{"all_empty", &LayoutPlan{Counts: []int64{0, 0}}, true},
		{"missing", &LayoutPlan{Counts: []int64{1}}, false},
		{"duplicate", &LayoutPlan{Counts: []int64{1}, Groups: []ClusterGroup{{Offsets: []uint32{0}, Rows: 1}, {Offsets: []uint32{0}, Rows: 1}}}, false},
		{"out_of_range", &LayoutPlan{Counts: []int64{1}, Groups: []ClusterGroup{{Offsets: []uint32{1}, Rows: 1}}}, false},
		{"zero_member", &LayoutPlan{Counts: []int64{0}, Groups: []ClusterGroup{{Offsets: []uint32{0}, Rows: 0}}}, false},
		{"empty_group", &LayoutPlan{Counts: []int64{0}, Groups: []ClusterGroup{{}}}, false},
		{"bad_rows", &LayoutPlan{Counts: []int64{2}, Groups: []ClusterGroup{{Offsets: []uint32{0}, Rows: 1}}}, false},
		{"overflow", &LayoutPlan{Counts: []int64{math.MaxInt64, 1}}, false},
		{"negative", &LayoutPlan{Counts: []int64{-1}}, false},
		{"nil", nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.plan.Validate()
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestLayoutPlanCentroidValidation(t *testing.T) {
	stats := &clusteringpb.ClusteringCentroidsStats{Centroids: []*schemapb.VectorField{{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}}}}}}
	require.NoError(t, ValidateCentroids(stats, schemapb.DataType_FloatVector, 2))
	require.Error(t, ValidateCentroids(stats, schemapb.DataType_FloatVector, 3))
	require.Error(t, ValidateCentroids(stats, schemapb.DataType_Float16Vector, 2))
	stats.Centroids[0].GetFloatVector().Data[0] = float32(math.NaN())
	require.Error(t, ValidateCentroids(stats, schemapb.DataType_FloatVector, 2))
}

func TestHalfCentroidValidation(t *testing.T) {
	for _, dtype := range []schemapb.DataType{schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector} {
		t.Run(dtype.String(), func(t *testing.T) {
			set := func(raw []byte) *clusteringpb.ClusteringCentroidsStats {
				c := &schemapb.VectorField{Dim: 3}
				if dtype == schemapb.DataType_Float16Vector {
					c.Data = &schemapb.VectorField_Float16Vector{Float16Vector: raw}
				} else {
					c.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: raw}
				}
				return &clusteringpb.ClusteringCentroidsStats{Centroids: []*schemapb.VectorField{c}}
			}
			good, err := typeutil.ConvertFloat32ToFP16BF16Bytes([]float32{0, 1, -65504}, dtype)
			require.NoError(t, err)
			require.NoError(t, ValidateCentroids(set(good), dtype, 3))
			for _, bad := range [][]byte{nil, good[:5], append(append([]byte(nil), good...), 0, 0)} {
				require.Error(t, ValidateCentroids(set(bad), dtype, 3))
			}
			require.Error(t, ValidateCentroids(set(good), schemapb.DataType_FloatVector, 3))
			for _, value := range []float32{float32(math.NaN()), float32(math.Inf(1)), float32(math.Inf(-1))} {
				var raw []byte
				if dtype == schemapb.DataType_Float16Vector {
					raw = typeutil.Float32ArrayToFloat16Bytes([]float32{value, 1, 2})
				} else {
					raw = typeutil.Float32ArrayToBFloat16Bytes([]float32{value, 1, 2})
				}
				require.Error(t, ValidateCentroids(set(raw), dtype, 3))
			}
		})
	}
}
