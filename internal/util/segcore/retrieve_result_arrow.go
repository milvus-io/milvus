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

#include <stdlib.h>
#include <stdint.h>
#include "common/arrow_c_data_c.h"
#include "common/type_c.h"
#include "segcore/segment_c.h"
#include "segcore/plan_c.h"

CStatus
FillRetrieveFieldsOrdered(CSegmentInterface* segments,
                          int64_t num_segments,
                          CRetrievePlan c_plan,
                          const int32_t* seg_indices,
                          const int64_t* seg_offsets,
                          int64_t total_rows,
                          struct ArrowSchema* out_schema,
                          struct ArrowArray* out_array,
                          void* cancellation_source);
*/
import "C"

import (
	"context"
	"runtime"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/cdata"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// FillRetrieveFieldsOrdered reads output fields from multiple segments
// and returns one Arrow RecordBatch in the specified row order.
// The caller is responsible for releasing the returned record.
func FillRetrieveFieldsOrdered(
	ctx context.Context,
	segments []CSegment,
	plan *RetrievePlan,
	segIndices []int32,
	segOffsets []int64,
) (arrow.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if plan == nil || plan.cRetrievePlan == nil {
		return nil, merr.WrapErrParameterInvalidMsg("nil retrieve plan")
	}
	if len(segments) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("empty segments")
	}
	if len(segIndices) != len(segOffsets) {
		return nil, merr.WrapErrParameterInvalidMsg(
			"unaligned segment indices (%d) and offsets (%d)",
			len(segIndices), len(segOffsets))
	}

	cSegments := make([]C.CSegmentInterface, len(segments))
	for i, seg := range segments {
		if seg == nil {
			return nil, merr.WrapErrParameterInvalidMsg("nil segment at index %d", i)
		}
		cSegments[i] = C.CSegmentInterface(seg.RawPointer())
	}

	var segIndicesPtr *C.int32_t
	var segOffsetsPtr *C.int64_t
	if len(segIndices) > 0 {
		segIndicesPtr = (*C.int32_t)(unsafe.Pointer(&segIndices[0]))
		segOffsetsPtr = (*C.int64_t)(unsafe.Pointer(&segOffsets[0]))
	}

	var cSchema C.struct_ArrowSchema
	var cArray C.struct_ArrowArray
	guard := NewCancellationGuard(ctx)
	defer guard.Close()
	status := C.FillRetrieveFieldsOrdered(
		&cSegments[0],
		C.int64_t(len(cSegments)),
		plan.cRetrievePlan,
		segIndicesPtr,
		segOffsetsPtr,
		C.int64_t(len(segIndices)),
		&cSchema,
		&cArray,
		guard.Source(),
	)
	runtime.KeepAlive(segIndices)
	runtime.KeepAlive(segOffsets)
	runtime.KeepAlive(cSegments)
	runtime.KeepAlive(segments)
	runtime.KeepAlive(plan)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		C.MilvusGoArrowSchemaRelease(&cSchema)
		C.MilvusGoArrowArrayRelease(&cArray)
		return nil, err
	}

	schema, err := cdata.ImportCArrowSchema(
		(*cdata.CArrowSchema)(unsafe.Pointer(&cSchema)))
	C.MilvusGoArrowSchemaRelease(&cSchema)
	if err != nil {
		C.MilvusGoArrowArrayRelease(&cArray)
		return nil, merr.WrapErrServiceInternal(
			"failed to import Arrow schema", err.Error())
	}
	record, err := cdata.ImportCRecordBatchWithSchema(
		(*cdata.CArrowArray)(unsafe.Pointer(&cArray)), schema)
	if err != nil {
		C.MilvusGoArrowArrayRelease(&cArray)
		return nil, merr.WrapErrServiceInternal(
			"failed to import Arrow RecordBatch", err.Error())
	}
	return record, nil
}
