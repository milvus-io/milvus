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

	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ValidateAssignment checks the persisted row-aligned Analyze artifact before
// a planner or mapper uses its offsets or counts. Rows include deleted/TTL rows;
// filtering happens later during mapping and must not shift these positions.
func ValidateAssignment(stats *clusteringpb.ClusteringCentroidIdMappingStats, rows, clusters int64) error {
	if stats == nil || rows < 0 || clusters <= 0 || clusters > math.MaxUint32 ||
		int64(len(stats.GetCentroidIdMapping())) != rows || int64(len(stats.GetDistanceToCentroid())) != rows || int64(len(stats.GetNumInCentroid())) != clusters {
		return merr.WrapErrDataIntegrityMsg("analyze assignment shape does not match input rows/centroids")
	}
	counts := make([]int64, len(stats.GetNumInCentroid()))
	for row, offset := range stats.GetCentroidIdMapping() {
		if int64(offset) >= clusters {
			return merr.WrapErrDataIntegrityMsg("analyze centroid offset %d out of range at row %d", offset, row)
		}
		distance := float64(stats.GetDistanceToCentroid()[row])
		if math.IsNaN(distance) || math.IsInf(distance, 0) || distance < 0 {
			return merr.WrapErrDataIntegrityMsg("invalid analyze distance at row %d", row)
		}
		counts[offset]++
	}
	for offset, count := range stats.GetNumInCentroid() {
		if count != counts[offset] {
			return merr.WrapErrDataIntegrityMsg("analyze count for centroid %d does not match row assignments", offset)
		}
	}
	return nil
}
