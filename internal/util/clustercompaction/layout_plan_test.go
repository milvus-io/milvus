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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func validLayoutPlan() *LayoutPlan {
	return &LayoutPlan{
		Format:         LayoutPlanFormat,
		RowCount:       1_000_000,
		CentroidCount:  3,
		CentroidCounts: []int64{400_000, 350_000, 250_000},
		CentroidGroups: []CentroidGroup{
			{CentroidGroupID: 0, Rows: 750_000, Centroids: []uint32{0, 1}},
			{CentroidGroupID: 1, Rows: 250_000, Centroids: []uint32{2}},
		},
		PlannerStats:        json.RawMessage(`{"layout_policy":"metis","cut":12}`),
		DataClusteringStats: json.RawMessage(`{"source":"analyze"}`),
	}
}

func TestParseLayoutPlan(t *testing.T) {
	raw := []byte(`{
        "format":"cluster_compaction_layout_plan",
        "row_count":1000000,
        "centroid_count":3,
        "centroid_counts":[400000,350000,250000],
        "centroid_groups":[
          {"centroid_group_id":0,"rows":750000,"centroids":[0,1]},
          {"centroid_group_id":1,"rows":250000,"centroids":[2]}
        ],
        "planner_stats":{"layout_policy":"metis","future_field":true},
        "data_clustering_stats":{"future_field":"preserved"}
      }`)

	plan, err := ParseLayoutPlan(raw)
	require.NoError(t, err)
	require.Equal(t, LayoutPlanFormat, plan.Format)
	require.JSONEq(t, `{"layout_policy":"metis","future_field":true}`, string(plan.PlannerStats))
	require.JSONEq(t, `{"future_field":"preserved"}`, string(plan.DataClusteringStats))
}

func TestLayoutPlanValidate(t *testing.T) {
	require.NoError(t, validLayoutPlan().Validate())

	t.Run("zero-row centroid may be omitted", func(t *testing.T) {
		plan := validLayoutPlan()
		plan.CentroidCount = 4
		plan.CentroidCounts = append(plan.CentroidCounts, 0)
		require.NoError(t, plan.Validate())
	})

	tests := []struct {
		name   string
		mutate func(*LayoutPlan)
	}{
		{"format", func(plan *LayoutPlan) { plan.Format = "global_ivf_compaction_plan_v1" }},
		{"negative row count", func(plan *LayoutPlan) { plan.RowCount = -1 }},
		{"centroid count length", func(plan *LayoutPlan) { plan.CentroidCount = 2 }},
		{"negative centroid rows", func(plan *LayoutPlan) { plan.CentroidCounts[0] = -1 }},
		{"row count sum", func(plan *LayoutPlan) { plan.RowCount++ }},
		{"duplicate group id", func(plan *LayoutPlan) { plan.CentroidGroups[1].CentroidGroupID = 0 }},
		{"negative group id", func(plan *LayoutPlan) { plan.CentroidGroups[0].CentroidGroupID = -1 }},
		{"empty group", func(plan *LayoutPlan) { plan.CentroidGroups[0].Centroids = nil; plan.CentroidGroups[0].Rows = 0 }},
		{"invalid centroid id", func(plan *LayoutPlan) { plan.CentroidGroups[0].Centroids[0] = 3 }},
		{"duplicate centroid", func(plan *LayoutPlan) { plan.CentroidGroups[1].Centroids[0] = 1 }},
		{"group rows", func(plan *LayoutPlan) { plan.CentroidGroups[0].Rows++ }},
		{"missing non-empty centroid", func(plan *LayoutPlan) {
			plan.CentroidGroups = plan.CentroidGroups[:1]
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := validLayoutPlan()
			test.mutate(plan)
			require.Error(t, plan.Validate())
		})
	}
}

func TestParseLayoutPlanRejectsMalformedJSON(t *testing.T) {
	_, err := ParseLayoutPlan([]byte(`{"format":`))
	require.Error(t, err)
}
