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

package clusteringindex

/*
#cgo pkg-config: milvus_core
#include <stdlib.h>
#include "clustering/snapshot_index_c.h"
*/
import "C"

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/clustercompaction"
	"github.com/milvus-io/milvus/internal/util/segcore/loadresource"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func consume(status C.CStatus) error {
	if status.error_code == 0 {
		return nil
	}
	defer C.free(unsafe.Pointer(status.error_msg))
	return merr.SegcoreError(int32(status.error_code), C.GoString(status.error_msg))
}
func bytesPtr(b []byte) *C.uint8_t { return (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(b))) }

// Build uploads an index over the final full cluster table. Returning a receipt
// never publishes metadata; the caller writes the snapshot only after success.
func Build(ctx context.Context, build *indexcgopb.BuildIndexInfo, centroids *clusteringpb.ClusteringCentroidsStats, prefix string) (*clusteringpb.ClusteringIndexInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if build == nil || prefix == "" || build.GetSegmentID() != 0 || build.GetBuildID() != 0 || build.GetIndexVersion() <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("invalid snapshot index build ownership")
	}
	if err := clustercompaction.ValidateCentroids(centroids, build.GetFieldSchema().GetDataType(), build.GetDim()); err != nil {
		return nil, err
	}
	if build.GetNumRows() != int64(len(centroids.GetCentroids())) {
		return nil, merr.WrapErrDataIntegrityMsg("snapshot index row count mismatch")
	}
	info, err := proto.Marshal(build)
	if err != nil {
		return nil, err
	}
	blob, err := proto.Marshal(centroids)
	if err != nil {
		return nil, err
	}
	cprefix := C.CString(prefix)
	defer C.free(unsafe.Pointer(cprefix))
	result := C.CreateProtoLayout()
	defer C.ReleaseProtoLayout(result)
	if err := consume(C.BuildSnapshotIndex(result, bytesPtr(info), C.uint64_t(len(info)), bytesPtr(blob), C.uint64_t(len(blob)), cprefix)); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var output clusteringpb.ClusteringIndexInfo
	if err := proto.Unmarshal(unsafe.Slice((*byte)(result.blob), int(result.size)), &output); err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "decode snapshot index receipt")
	}
	return &output, nil
}

// Index owns a shared LoadIndexData cache slot. A query keeps the owning snapshot
// reachable; Close must only be called after its last owner releases it.
type Index struct {
	mu        sync.RWMutex
	ptr       C.CLoadIndexInfo
	rows, dim int64
	fieldType schemapb.DataType
	params    map[string]string
}

func Load(ctx context.Context, collection, partition int64, channel string, version int64,
	field *schemapb.FieldSchema, metadata *clusteringpb.ClusteringIndexInfo, rows int64,
) (*Index, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	info, err := loadresource.BuildSnapshotLoadIndexInfo(collection, partition, channel, version, field, metadata, rows)
	if err != nil {
		return nil, err
	}
	blob, err := proto.Marshal(info)
	if err != nil {
		return nil, err
	}
	idx := &Index{rows: rows, fieldType: field.GetDataType(), params: info.IndexParams}
	for _, kv := range field.GetTypeParams() {
		if kv.GetKey() == "dim" {
			_, err = fmt.Sscan(kv.GetValue(), &idx.dim)
			if err != nil {
				return nil, err
			}
		}
	}
	if idx.dim <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("invalid snapshot index dimension")
	}
	if err := consume(C.NewLoadIndexInfo(&idx.ptr)); err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			idx.Close()
		}
	}()
	if err := consume(C.FinishLoadIndexInfo(idx.ptr, bytesPtr(blob), C.uint64_t(len(blob)))); err != nil {
		return nil, err
	}
	// The shared translator supplies priority, admission, cache ownership and reload.
	if err := consume(C.AppendIndexV2(C.CTraceContext{}, idx.ptr)); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	success = true
	runtime.SetFinalizer(idx, (*Index).Close)
	return idx, nil
}

func (idx *Index) Close() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.ptr != nil {
		C.DeleteLoadIndexInfo(idx.ptr)
		idx.ptr = nil
	}
	runtime.SetFinalizer(idx, nil)
}

func (idx *Index) Search(ctx context.Context, vectors []byte, nq, topk int64) ([]int64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if idx.ptr == nil || nq <= 0 || topk <= 0 || topk > idx.rows || nq > math.MaxInt/topk || idx.dim <= 0 || nq > math.MaxInt/idx.dim/4 || len(vectors) != int(nq*idx.dim*4) {
		return nil, merr.WrapErrParameterInvalidMsg("invalid snapshot index query")
	}
	params := make(map[string]any, len(idx.params))
	for key, value := range idx.params {
		var parsed any
		if json.Unmarshal([]byte(value), &parsed) == nil {
			params[key] = parsed
		} else {
			params[key] = value
		}
	}
	blob, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	cparams := C.CString(string(blob))
	defer C.free(unsafe.Pointer(cparams))
	labels := make([]int64, nq*topk)
	if err := consume(C.SearchSnapshotIndex(idx.ptr, bytesPtr(vectors), C.uint64_t(len(vectors)), C.int64_t(nq), C.int64_t(topk), cparams, (*C.int64_t)(unsafe.Pointer(unsafe.SliceData(labels))))); err != nil {
		return nil, err
	}
	runtime.KeepAlive(idx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, label := range labels {
		if label < -1 || label >= idx.rows {
			return nil, merr.WrapErrDataIntegrityMsg("snapshot index label out of range")
		}
	}
	return labels, nil
}
