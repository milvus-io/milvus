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

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include "clustering/analyze_c.h"
*/
import "C"

import (
	"encoding/json"
	"math"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/clustercompaction"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func BuildCompactionPlan(
	centroids *clusteringpb.ClusteringCentroidsStats,
	fieldType schemapb.DataType,
	centroidCounts []int64,
	clusterType string,
	params map[string]string,
) (*clustercompaction.LayoutPlan, error) {
	if err := validateCompactionCentroids(centroids, fieldType); err != nil {
		return nil, err
	}
	centroidBlob, err := proto.Marshal(centroids)
	if err != nil {
		return nil, err
	}
	paramsBlob, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	if len(centroidBlob) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("clustering centroids are empty")
	}

	counts := make([]C.uint64_t, len(centroidCounts))
	for i, count := range centroidCounts {
		if count < 0 {
			return nil, merr.WrapErrParameterInvalidMsg("clustering centroid count must be non-negative")
		}
		counts[i] = C.uint64_t(count)
	}

	cClusterType := C.CString(clusterType)
	defer C.free(unsafe.Pointer(cClusterType))
	cParams := C.CString(string(paramsBlob))
	defer C.free(unsafe.Pointer(cParams))

	var countsPtr *C.uint64_t
	if len(counts) > 0 {
		countsPtr = &counts[0]
	}
	var handle C.CClusteringCompactionPlan
	status := C.BuildClusteringCompactionPlan(
		&handle,
		(*C.uint8_t)(unsafe.Pointer(&centroidBlob[0])),
		C.uint64_t(len(centroidBlob)),
		C.int32_t(fieldType),
		countsPtr,
		C.uint64_t(len(counts)),
		cClusterType,
		cParams,
	)
	if err := HandleCStatus(&status, "failed to build clustering compaction plan"); err != nil {
		return nil, err
	}
	defer func() {
		deleteStatus := C.DeleteClusteringCompactionPlan(handle)
		_ = HandleCStatus(&deleteStatus, "failed to delete clustering compaction plan")
	}()

	var rowCount C.uint64_t
	var centroidCount C.uint32_t
	var cCentroidCounts *C.uint64_t
	var groupCount C.uint64_t
	status = C.GetClusteringCompactionPlanMeta(
		handle, &rowCount, &centroidCount, &cCentroidCounts, &groupCount)
	if err := HandleCStatus(&status, "failed to get clustering compaction plan metadata"); err != nil {
		return nil, err
	}
	if uint64(rowCount) > math.MaxInt64 || uint64(groupCount) > uint64(math.MaxInt) {
		return nil, merr.WrapErrParameterInvalidMsg("clustering compaction plan exceeds Go integer limits")
	}

	plan := &clustercompaction.LayoutPlan{
		Format:         clustercompaction.LayoutPlanFormat,
		RowCount:       int64(rowCount),
		CentroidCount:  int64(centroidCount),
		CentroidCounts: make([]int64, int(centroidCount)),
		CentroidGroups: make([]clustercompaction.CentroidGroup, int(groupCount)),
	}
	if centroidCount > 0 && cCentroidCounts == nil {
		return nil, merr.WrapErrParameterInvalidMsg("clustering compaction plan has null centroid counts")
	}
	for i, value := range unsafe.Slice(cCentroidCounts, int(centroidCount)) {
		if uint64(value) > math.MaxInt64 {
			return nil, merr.WrapErrParameterInvalidMsg("clustering centroid count exceeds int64")
		}
		plan.CentroidCounts[i] = int64(value)
	}

	for offset := range plan.CentroidGroups {
		var group C.CClusteringCentroidGroup
		status = C.GetClusteringCompactionPlanGroup(handle, C.uint64_t(offset), &group)
		if err := HandleCStatus(&status, "failed to get clustering compaction plan group"); err != nil {
			return nil, err
		}
		if uint64(group.rows) > math.MaxInt64 || uint64(group.centroid_count) > uint64(math.MaxInt) {
			return nil, merr.WrapErrParameterInvalidMsg("clustering compaction group exceeds Go integer limits")
		}
		if group.centroid_count > 0 && group.centroids == nil {
			return nil, merr.WrapErrParameterInvalidMsg("clustering compaction group has null centroids")
		}
		centroidIDs := make([]uint32, int(group.centroid_count))
		for i, centroidID := range unsafe.Slice(group.centroids, int(group.centroid_count)) {
			centroidIDs[i] = uint32(centroidID)
		}
		plan.CentroidGroups[offset] = clustercompaction.CentroidGroup{
			CentroidGroupID: int64(group.centroid_group_id),
			Rows:            int64(group.rows),
			Centroids:       centroidIDs,
		}
	}
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	return plan, nil
}

func validateCompactionCentroids(centroids *clusteringpb.ClusteringCentroidsStats, fieldType schemapb.DataType) error {
	if centroids == nil || len(centroids.GetCentroids()) == 0 {
		return merr.WrapErrParameterInvalidMsg("clustering centroids are empty")
	}
	for _, centroid := range centroids.GetCentroids() {
		if centroid == nil || centroid.GetDim() <= 0 {
			return merr.WrapErrParameterInvalidMsg("clustering centroid dimension must be positive")
		}
		dim := centroid.GetDim()
		switch fieldType {
		case schemapb.DataType_FloatVector:
			value, ok := centroid.GetData().(*schemapb.VectorField_FloatVector)
			if !ok || value.FloatVector == nil || int64(len(value.FloatVector.GetData())) != dim {
				return merr.WrapErrParameterInvalidMsg("clustering centroid type or length differs from FloatVector field")
			}
		case schemapb.DataType_Float16Vector:
			value, ok := centroid.GetData().(*schemapb.VectorField_Float16Vector)
			if !ok || int64(len(value.Float16Vector)) != dim*2 {
				return merr.WrapErrParameterInvalidMsg("clustering centroid type or length differs from Float16Vector field")
			}
		case schemapb.DataType_BFloat16Vector:
			value, ok := centroid.GetData().(*schemapb.VectorField_Bfloat16Vector)
			if !ok || int64(len(value.Bfloat16Vector)) != dim*2 {
				return merr.WrapErrParameterInvalidMsg("clustering centroid type or length differs from BFloat16Vector field")
			}
		default:
			return merr.WrapErrParameterInvalidMsg("unsupported clustering centroid field type %s", fieldType.String())
		}
	}
	return nil
}
