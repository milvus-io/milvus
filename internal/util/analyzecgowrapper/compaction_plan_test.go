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
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
)

func plannerCentroids(k int) *clusteringpb.ClusteringCentroidsStats {
	stats := &clusteringpb.ClusteringCentroidsStats{}
	for i := 0; i < k; i++ {
		// Two well-separated clouds, interleaved in offset order.
		values := []float32{float32((i%2)*10000 + i/2), float32((i / 2) % 37), float32(i % 11), 1}
		stats.Centroids = append(stats.Centroids, &schemapb.VectorField{Dim: 4, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: values}}})
	}
	return stats
}

func TestBuildCompactionPlan(t *testing.T) {
	// Current Cardinal requires >= 2048 centroids; use the SIFT workaround.
	stats := plannerCentroids(4096)
	counts := make([]int64, 4096)
	for i := range counts {
		counts[i] = 1
	}
	counts[0] = 600
	counts[1] = 0
	options := map[string]string{"random_state": "42", "search_quant_type": "NONE", "refine_quant_type": "NONE", "compaction_max_rows": "1"}
	plan, err := BuildCompactionPlan(context.Background(), stats, schemapb.DataType_FloatVector, counts, "CARDINAL_KMEANS", options, 128)
	require.NoError(t, err)
	require.NoError(t, plan.Validate())
	require.Equal(t, counts, plan.Counts)
	require.Greater(t, len(plan.Groups), 1)
	require.Equal(t, "1", options["compaction_max_rows"])
	require.Less(t, len(plan.Groups), len(counts)/2, "coordinator row target must override provider parameter")
	// The returned buffers remain valid after the native owner was destroyed.
	counts[2] = 0
	stats.Centroids[2].GetFloatVector().Data[0] = float32(math.NaN())
	require.Equal(t, int64(1), plan.Counts[2])
	require.NoError(t, plan.Validate())
}

func TestBuildCompactionPlanFailures(t *testing.T) {
	stats := plannerCentroids(2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := BuildCompactionPlan(ctx, stats, schemapb.DataType_FloatVector, []int64{1, 1}, "CARDINAL_KMEANS", nil, 10)
	require.ErrorIs(t, err, context.Canceled)
	for _, counts := range [][]int64{{1}, {-1, 1}, {math.MaxInt64, 1}} {
		_, err := BuildCompactionPlan(context.Background(), stats, schemapb.DataType_FloatVector, counts, "CARDINAL_KMEANS", nil, 10)
		require.Error(t, err)
	}
	for _, provider := range []string{"KMEANS", "not-a-provider"} {
		_, err := BuildCompactionPlan(context.Background(), stats, schemapb.DataType_FloatVector, []int64{1, 1}, provider, nil, 10)
		require.Error(t, err, "unsupported providers must not fall back to round-robin")
	}
	_, err = BuildCompactionPlan(context.Background(), stats, schemapb.DataType_FloatVector, []int64{1, 1}, "CARDINAL_KMEANS", map[string]string{"compaction_neighbor_k": "0"}, 10)
	require.Error(t, err)
}
