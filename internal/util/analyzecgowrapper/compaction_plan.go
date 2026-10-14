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
	"context"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/clustercompaction"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// BuildCompactionPlan copies the native result before releasing its owner.
// The provider call is synchronous; cancellation is checked before and after
// it, and never frees inputs or handles while native code is still using them.
func BuildCompactionPlan(ctx context.Context, centroids *clusteringpb.ClusteringCentroidsStats,
	fieldType schemapb.DataType, counts []int64, clusterType string, params map[string]string,
	maxRows int64,
) (*clustercompaction.LayoutPlan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var dim int64
	if len(centroids.GetCentroids()) > 0 {
		dim = centroids.GetCentroids()[0].GetDim()
	}
	if err := clustercompaction.ValidateCentroids(centroids, fieldType, dim); err != nil {
		return nil, err
	}
	if len(counts) != len(centroids.GetCentroids()) || strings.TrimSpace(clusterType) == "" || strings.ContainsRune(clusterType, 0) || maxRows <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("invalid clustering planner shape, provider or row target")
	}
	var total int64
	nativeCounts := make([]C.uint64_t, len(counts))
	for i, count := range counts {
		if count < 0 || count > math.MaxInt64-total {
			return nil, merr.WrapErrDataIntegrityMsg("invalid clustering count sum")
		}
		total += count
		nativeCounts[i] = C.uint64_t(count)
	}
	options := maps.Clone(params)
	if options == nil {
		options = make(map[string]string)
	}
	// Shape and group row target belong to the task, not provider overrides.
	for _, key := range []string{"num_clusters", "dim", "num_rows", "train_size", "data_type", "metric_type"} {
		delete(options, key)
	}
	options["compaction_max_rows"] = strconv.FormatInt(maxRows, 10)
	paramsBlob, err := json.Marshal(options)
	if err != nil {
		return nil, merr.WrapErrSerializationFailed(err, "encode clustering planner parameters")
	}
	centroidBlob, err := proto.Marshal(centroids)
	if err != nil {
		return nil, merr.WrapErrSerializationFailed(err, "encode clustering centroids")
	}
	cType, cParams := C.CString(clusterType), C.CString(string(paramsBlob))
	defer C.free(unsafe.Pointer(cType))
	defer C.free(unsafe.Pointer(cParams))
	var handle C.CClusteringCompactionPlan
	status := C.BuildClusteringCompactionPlan(&handle,
		(*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(centroidBlob))), C.uint64_t(len(centroidBlob)),
		C.int32_t(fieldType), unsafe.SliceData(nativeCounts), C.uint64_t(len(nativeCounts)), cType, cParams)
	if err := HandleCStatus(&status, "build clustering compaction plan"); err != nil {
		return nil, err
	}
	defer func() {
		status := C.DeleteClusteringCompactionPlan(handle)
		_ = HandleCStatus(&status, "delete clustering compaction plan")
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var rowCount, groupCount C.uint64_t
	var count C.uint32_t
	var nativeResultCounts *C.uint64_t
	status = C.GetClusteringCompactionPlanMeta(handle, &rowCount, &count, &nativeResultCounts, &groupCount)
	if err := HandleCStatus(&status, "read clustering plan metadata"); err != nil {
		return nil, err
	}
	if uint64(rowCount) != uint64(total) || uint64(count) != uint64(len(counts)) || uint64(groupCount) > uint64(len(counts)) || nativeResultCounts == nil {
		return nil, merr.WrapErrDataIntegrityMsg("invalid native clustering plan metadata")
	}
	plan := &clustercompaction.LayoutPlan{Counts: make([]int64, len(counts)), Groups: make([]clustercompaction.ClusterGroup, int(groupCount))}
	for i, count := range unsafe.Slice(nativeResultCounts, len(counts)) {
		if uint64(count) > math.MaxInt64 {
			return nil, merr.WrapErrDataIntegrityMsg("native clustering count overflows")
		}
		plan.Counts[i] = int64(count)
	}
	if !slices.Equal(plan.Counts, counts) {
		return nil, merr.WrapErrDataIntegrityMsg("native planner changed counts")
	}
	for i := range plan.Groups {
		var group C.CClusteringGroup
		status = C.GetClusteringCompactionPlanGroup(handle, C.uint64_t(i), &group)
		if err := HandleCStatus(&status, "read clustering plan group"); err != nil {
			return nil, err
		}
		if uint64(group.count) == 0 || uint64(group.count) > uint64(len(counts)) || group.offsets == nil || uint64(group.rows) > math.MaxInt64 {
			return nil, merr.WrapErrDataIntegrityMsg("invalid native clustering group")
		}
		offsets := make([]uint32, int(group.count))
		for j, offset := range unsafe.Slice(group.offsets, int(group.count)) {
			offsets[j] = uint32(offset)
		}
		plan.Groups[i] = clustercompaction.ClusterGroup{Offsets: offsets, Rows: int64(group.rows)}
	}
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	return plan, nil
}
