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
	"math"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const LayoutPlanFormat = "cluster_compaction_layout_plan"

type CentroidGroup struct {
	CentroidGroupID int64    `json:"centroid_group_id"`
	Rows            int64    `json:"rows"`
	Centroids       []uint32 `json:"centroids"`
}

type LayoutPlan struct {
	Format              string          `json:"format"`
	RowCount            int64           `json:"row_count"`
	CentroidCount       int64           `json:"centroid_count"`
	CentroidCounts      []int64         `json:"centroid_counts"`
	CentroidGroups      []CentroidGroup `json:"centroid_groups"`
	PlannerStats        json.RawMessage `json:"planner_stats,omitempty"`
	DataClusteringStats json.RawMessage `json:"data_clustering_stats,omitempty"`
}

func ParseLayoutPlan(data []byte) (*LayoutPlan, error) {
	plan := &LayoutPlan{}
	if err := json.Unmarshal(data, plan); err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "failed to decode cluster compaction layout plan")
	}
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	return plan, nil
}

func (p *LayoutPlan) Validate() error {
	if p == nil {
		return merr.WrapErrServiceInternalMsg("cluster compaction layout plan is nil")
	}
	if p.Format != LayoutPlanFormat {
		return merr.WrapErrServiceInternalMsg("unexpected cluster compaction layout plan format %q", p.Format)
	}
	if p.RowCount < 0 {
		return merr.WrapErrServiceInternalMsg("cluster compaction layout plan row count must be non-negative, got %d", p.RowCount)
	}
	if p.CentroidCount < 0 {
		return merr.WrapErrServiceInternalMsg("cluster compaction layout plan centroid count must be non-negative, got %d", p.CentroidCount)
	}
	if int64(len(p.CentroidCounts)) != p.CentroidCount {
		return merr.WrapErrServiceInternalMsg(
			"cluster compaction layout plan centroid counts length mismatch, got %d, expected %d",
			len(p.CentroidCounts), p.CentroidCount,
		)
	}

	var countSum int64
	for centroidID, count := range p.CentroidCounts {
		if count < 0 {
			return merr.WrapErrServiceInternalMsg("cluster compaction layout plan centroid %d has negative row count %d", centroidID, count)
		}
		if countSum > math.MaxInt64-count {
			return merr.WrapErrServiceInternalMsg("cluster compaction layout plan centroid row count sum overflows int64")
		}
		countSum += count
	}
	if countSum != p.RowCount {
		return merr.WrapErrServiceInternalMsg(
			"cluster compaction layout plan row count mismatch, centroid counts sum to %d, expected %d",
			countSum, p.RowCount,
		)
	}

	groupIDs := make(map[int64]struct{}, len(p.CentroidGroups))
	seenCentroids := make([]bool, len(p.CentroidCounts))
	for groupOffset, group := range p.CentroidGroups {
		if group.CentroidGroupID < 0 {
			return merr.WrapErrServiceInternalMsg("cluster compaction layout plan group id must be non-negative, got %d", group.CentroidGroupID)
		}
		if group.CentroidGroupID != int64(groupOffset) {
			return merr.WrapErrServiceInternalMsg(
				"cluster compaction layout plan group id must match its offset, got %d at offset %d",
				group.CentroidGroupID, groupOffset,
			)
		}
		if _, ok := groupIDs[group.CentroidGroupID]; ok {
			return merr.WrapErrServiceInternalMsg("cluster compaction layout plan has duplicate group id %d", group.CentroidGroupID)
		}
		groupIDs[group.CentroidGroupID] = struct{}{}
		if len(group.Centroids) == 0 {
			return merr.WrapErrServiceInternalMsg("cluster compaction layout plan group %d is empty", group.CentroidGroupID)
		}

		var groupRows int64
		for _, centroidID := range group.Centroids {
			if int64(centroidID) >= p.CentroidCount {
				return merr.WrapErrServiceInternalMsg(
					"cluster compaction layout plan centroid id %d in group %d is out of range [0,%d)",
					centroidID, group.CentroidGroupID, p.CentroidCount,
				)
			}
			if seenCentroids[centroidID] {
				return merr.WrapErrServiceInternalMsg("cluster compaction layout plan centroid %d appears in more than one group", centroidID)
			}
			seenCentroids[centroidID] = true
			count := p.CentroidCounts[centroidID]
			if groupRows > math.MaxInt64-count {
				return merr.WrapErrServiceInternalMsg("cluster compaction layout plan group %d row count sum overflows int64", group.CentroidGroupID)
			}
			groupRows += count
		}
		if groupRows != group.Rows {
			return merr.WrapErrServiceInternalMsg(
				"cluster compaction layout plan group %d row count mismatch, centroids sum to %d, group reports %d",
				group.CentroidGroupID, groupRows, group.Rows,
			)
		}
	}

	for centroidID, count := range p.CentroidCounts {
		if count > 0 && !seenCentroids[centroidID] {
			return merr.WrapErrServiceInternalMsg("cluster compaction layout plan omits non-empty centroid %d", centroidID)
		}
	}
	return nil
}
