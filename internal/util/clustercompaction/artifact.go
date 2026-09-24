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
	"path"
	"strconv"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type AnalyzeArtifactPaths struct {
	Root      string
	FieldRoot string
	Centroids string
}

func BuildAnalyzeArtifactPaths(rootPath string, taskID, version, collectionID, partitionID, fieldID int64) AnalyzeArtifactPaths {
	root := path.Join(rootPath, common.AnalyzeStatsPath, strconv.FormatInt(taskID, 10), strconv.FormatInt(version, 10))
	fieldRoot := path.Join(
		root,
		strconv.FormatInt(collectionID, 10),
		strconv.FormatInt(partitionID, 10),
		strconv.FormatInt(fieldID, 10),
	)
	return AnalyzeArtifactPaths{
		Root:      root,
		FieldRoot: fieldRoot,
		Centroids: path.Join(fieldRoot, common.Centroids),
	}
}

func (p AnalyzeArtifactPaths) SegmentOffsetMapping(segmentID int64) string {
	return path.Join(p.FieldRoot, strconv.FormatInt(segmentID, 10), common.OffsetMapping)
}

// ValidateCentroidMappingStats verifies an Analyze offset_mapping artifact.
// Violations are internal protocol errors: the artifact is produced by an
// Analyze worker, not by a user request.
func ValidateCentroidMappingStats(stats *clusteringpb.ClusteringCentroidIdMappingStats, numRows, centroidCount int64) error {
	if stats == nil {
		return merr.WrapErrServiceInternalMsg("clustering centroid mapping stats are nil")
	}
	if numRows < 0 {
		return merr.WrapErrServiceInternalMsg("clustering centroid mapping row count must be non-negative, got %d", numRows)
	}
	if centroidCount < 0 {
		return merr.WrapErrServiceInternalMsg("clustering centroid count must be non-negative, got %d", centroidCount)
	}
	if int64(len(stats.GetCentroidIdMapping())) != numRows {
		return merr.WrapErrServiceInternalMsg(
			"clustering centroid mapping length mismatch, got %d, expected %d",
			len(stats.GetCentroidIdMapping()), numRows,
		)
	}
	if int64(len(stats.GetDistanceToCentroid())) != numRows {
		return merr.WrapErrServiceInternalMsg(
			"clustering centroid distance length mismatch, got %d, expected %d",
			len(stats.GetDistanceToCentroid()), numRows,
		)
	}
	if int64(len(stats.GetNumInCentroid())) != centroidCount {
		return merr.WrapErrServiceInternalMsg(
			"clustering centroid counts length mismatch, got %d, expected %d",
			len(stats.GetNumInCentroid()), centroidCount,
		)
	}

	var countSum int64
	for centroidID, count := range stats.GetNumInCentroid() {
		if count < 0 {
			return merr.WrapErrServiceInternalMsg("clustering centroid %d has negative row count %d", centroidID, count)
		}
		if countSum > math.MaxInt64-count {
			return merr.WrapErrServiceInternalMsg("clustering centroid row count sum overflows int64")
		}
		countSum += count
	}
	if countSum != numRows {
		return merr.WrapErrServiceInternalMsg("clustering centroid row count mismatch, got %d, expected %d", countSum, numRows)
	}

	actualCounts := make([]int64, len(stats.GetNumInCentroid()))
	for row, centroidID := range stats.GetCentroidIdMapping() {
		distance := stats.GetDistanceToCentroid()[row]
		if math.IsNaN(float64(distance)) || math.IsInf(float64(distance), 0) {
			return merr.WrapErrServiceInternalMsg(
				"clustering centroid distance must be finite at row %d, got %v",
				row, distance,
			)
		}
		if int64(centroidID) >= centroidCount {
			return merr.WrapErrServiceInternalMsg(
				"clustering centroid id out of range at row %d, got %d, centroid count %d",
				row, centroidID, centroidCount,
			)
		}
		actualCounts[centroidID]++
	}
	for centroidID, expected := range stats.GetNumInCentroid() {
		if actualCounts[centroidID] != expected {
			return merr.WrapErrServiceInternalMsg(
				"clustering centroid %d row count mismatch, mapping has %d, stats has %d",
				centroidID, actualCounts[centroidID], expected,
			)
		}
	}
	return nil
}
