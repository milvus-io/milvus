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

package segcore

/*
#cgo pkg-config: milvus_core
#include "segcore/segment_c.h"
*/
import "C"

import (
	"context"
	"runtime"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/cgo"
	"github.com/milvus-io/milvus/internal/util/segcore/loadresource"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// SegmentLoadPlan owns native planning metadata. The caller must keep the
// segment pinned through Execute and Close; these methods are not concurrent.
type SegmentLoadPlan struct {
	handle  C.CSegmentLoadPlan
	Loading loadresource.SegmentResourceUsage
	Loaded  loadresource.SegmentResourceUsage
}

// PrepareSegmentLoadPlan performs cancellable metadata I/O before admission.
// initial selects initial Load rather than Reopen; schemaVersion is the native
// collection schema version associated with schema.
func PrepareSegmentLoadPlan(ctx context.Context, segment CSegment, schema *schemapb.CollectionSchema, schemaVersion uint64, info *querypb.SegmentLoadInfo, initial bool) (*SegmentLoadPlan, error) {
	var loadBlob, schemaBlob []byte
	var loadPtr *C.uint8_t
	var schemaPtr unsafe.Pointer
	if !initial {
		if info != nil {
			info.EstimatedBytesPerRow = loadresource.EstimateBytesPerRow(schema, info)
		}
		converted, err := convertToSegcoreSegmentLoadInfo(info, packed.ManifestReadReopen)
		if err != nil {
			return nil, err
		}
		loadBlob, err = proto.Marshal(converted)
		if err != nil {
			return nil, err
		}
		schemaBlob, err = externalspec.MarshalCollectionSchema(schema)
		if err != nil {
			return nil, err
		}
		if len(loadBlob) == 0 || len(schemaBlob) == 0 {
			return nil, merr.WrapErrServiceInternalMsg("empty load plan metadata")
		}
		loadPtr = (*C.uint8_t)(unsafe.Pointer(&loadBlob[0]))
		schemaPtr = unsafe.Pointer(&schemaBlob[0])
	}
	defer runtime.KeepAlive(loadBlob)
	defer runtime.KeepAlive(schemaBlob)
	future := cgo.Async(ctx, func() cgo.CFuturePtr {
		return cgo.CFuturePtr(C.AsyncPrepareSegmentLoadPlan(C.CSegmentInterface(segment.RawPointer()), loadPtr, C.int64_t(len(loadBlob)), schemaPtr, C.int64_t(len(schemaBlob)), C.uint64_t(schemaVersion)))
	}, cgo.WithName("prepare-segment-load-plan"))
	defer future.Release()
	ptr, err := future.BlockAndLeakyGet()
	if err != nil {
		return nil, err
	}
	plan := &SegmentLoadPlan{handle: C.CSegmentLoadPlan(ptr)}
	options := loadresource.DefaultSegmentLoadingEstimateOptions()
	request, err := loadresource.MarshalResourceEstimateRequest(ctx, []*schemapb.CollectionSchema{schema}, []*querypb.SegmentLoadInfo{info}, options.MmapVectorField, options.MmapScalarField, options.PreferFieldDataWhenIndexHasRawData, false)
	if err != nil {
		plan.Close()
		return nil, err
	}
	var estimate C.CSegmentLoadEstimate
	status := C.EstimatePreparedSegmentLoad(plan.handle, (*C.uint8_t)(unsafe.Pointer(&request[0])), C.uint64_t(len(request)), &estimate)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		plan.Close()
		return nil, err
	}
	plan.Loading = loadresource.PreparedLoadingResource(schema, info,
		uint64(estimate.loading_memory_bytes), uint64(estimate.loading_disk_bytes),
		uint64(estimate.timestamp_memory_bytes), uint64(estimate.interim_index_bytes), int(estimate.mmap_field_count))
	plan.Loaded = loadresource.SegmentResourceUsage{MemoryBytes: uint64(estimate.loaded_memory_bytes), DiskBytes: uint64(estimate.loaded_disk_bytes)}
	return plan, nil
}

// Execute returns a retriable error if another update invalidated this plan.
// Waiting for the future joins native work before the caller releases its reservation.
func (p *SegmentLoadPlan) Execute(ctx context.Context) error {
	if p.handle == nil {
		return merr.WrapErrServiceInternalMsg("load plan is closed")
	}
	future := cgo.Async(ctx, func() cgo.CFuturePtr {
		return cgo.CFuturePtr(C.AsyncExecuteSegmentLoadPlan(p.handle))
	}, cgo.WithName("execute-segment-load-plan"))
	defer future.Release()
	result, err := future.BlockAndLeakyGet()
	if err != nil {
		return err
	}
	defer C.ReleaseSegmentLoadPlanResult(result)
	if !bool(*(*C.bool)(result)) {
		return merr.WrapErrServiceNotReadyMsg("segment load plan became stale; prepare and reserve again")
	}
	return nil
}

// Close releases metadata only, after execution has stopped. It never drops
// resources already published by the segment.
func (p *SegmentLoadPlan) Close() {
	if p.handle != nil {
		C.ReleaseSegmentLoadPlan(p.handle)
		p.handle = nil
	}
}
