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
ExportSearchResultAsArrowRecordBatch(CSearchResult c_search_result,
                                     CSearchPlan c_plan,
                                     const int64_t* extra_field_ids,
                                     int64_t num_extra_fields,
                                     struct ArrowSchema* out_schema,
                                     struct ArrowArray* out_array,
                                     int64_t** out_chunk_sizes,
                                     int64_t* out_num_chunks,
                                     void* cancellation_source);

CStatus
FillOutputFieldsOrdered(CSearchResult* search_results,
                        int64_t num_search_results,
                        CSearchPlan c_plan,
                        const int32_t* result_seg_indices,
                        const int64_t* result_seg_offsets,
                        int64_t total_rows,
                        CProto* out_result,
                        void* cancellation_source);

void
GetSearchResultMetadata(CSearchResult c_search_result,
                        bool* has_group_by,
                        int64_t* group_size,
                        int64_t* scanned_remote_bytes,
                        int64_t* scanned_total_bytes);
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

// ExportSearchResultAsArrowRecordBatch exports a per-segment C++ SearchResult as
// one full Arrow RecordBatch and returns row counts for each logical NQ chunk.
// The caller is responsible for releasing the returned record.
func ExportSearchResultAsArrowRecordBatch(ctx context.Context, result *SearchResult, plan *SearchPlan, extraFieldIDs []int64) (arrow.Record, []int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if result == nil {
		return nil, nil, merr.WrapErrParameterInvalidMsg("nil search result")
	}
	if plan == nil || plan.cSearchPlan == nil {
		return nil, nil, merr.WrapErrParameterInvalidMsg("nil search plan")
	}

	var extraPtr *C.int64_t
	if len(extraFieldIDs) > 0 {
		extraPtr = (*C.int64_t)(unsafe.Pointer(&extraFieldIDs[0]))
	}

	var cSchema C.struct_ArrowSchema
	var cArray C.struct_ArrowArray
	var chunkSizesPtr *C.int64_t
	var numChunks C.int64_t
	guard := NewCancellationGuard(ctx)
	defer guard.Close()
	status := C.ExportSearchResultAsArrowRecordBatch(
		result.cSearchResult,
		plan.cSearchPlan,
		extraPtr,
		C.int64_t(len(extraFieldIDs)),
		&cSchema,
		&cArray,
		&chunkSizesPtr,
		&numChunks,
		guard.Source(),
	)
	runtime.KeepAlive(extraFieldIDs)
	runtime.KeepAlive(result)
	runtime.KeepAlive(plan)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		if chunkSizesPtr != nil {
			C.free(unsafe.Pointer(chunkSizesPtr))
		}
		C.MilvusGoArrowSchemaRelease(&cSchema)
		C.MilvusGoArrowArrayRelease(&cArray)
		return nil, nil, err
	}
	if chunkSizesPtr == nil || numChunks <= 0 {
		C.MilvusGoArrowSchemaRelease(&cSchema)
		C.MilvusGoArrowArrayRelease(&cArray)
		return nil, nil, merr.WrapErrServiceInternal("missing Arrow RecordBatch chunk sizes")
	}
	cChunkSizes := unsafe.Slice((*int64)(unsafe.Pointer(chunkSizesPtr)), int(numChunks))
	chunkSizes := append([]int64(nil), cChunkSizes...)
	C.free(unsafe.Pointer(chunkSizesPtr))

	schema, err := cdata.ImportCArrowSchema((*cdata.CArrowSchema)(unsafe.Pointer(&cSchema)))
	C.MilvusGoArrowSchemaRelease(&cSchema)
	if err != nil {
		C.MilvusGoArrowArrayRelease(&cArray)
		return nil, nil, merr.WrapErrServiceInternal("failed to import Arrow schema", err.Error())
	}

	rec, err := cdata.ImportCRecordBatchWithSchema((*cdata.CArrowArray)(unsafe.Pointer(&cArray)), schema)
	if err != nil {
		C.MilvusGoArrowArrayRelease(&cArray)
		return nil, nil, merr.WrapErrServiceInternal("failed to import Arrow RecordBatch", err.Error())
	}
	return rec, chunkSizes, nil
}

// FillOutputFieldsOrdered reads output fields from multiple segments in a single CGO call,
// producing results in the specified output order.
// Storage cost is accumulated in the original SearchResult objects.
//
// segIndices[i] specifies which results[] element the i-th output row came from.
// segOffsets[i] specifies the segment-internal offset for that row.
// Returns serialized schemapb.SearchResultData proto with only FieldsData populated.
func FillOutputFieldsOrdered(
	ctx context.Context,
	results []*SearchResult,
	plan *SearchPlan,
	segIndices []int32,
	segOffsets []int64,
) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if plan == nil || plan.cSearchPlan == nil {
		return nil, merr.WrapErrParameterInvalidMsg("nil search plan")
	}
	if len(results) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("empty search results")
	}
	if len(segIndices) != len(segOffsets) {
		return nil, merr.WrapErrParameterInvalidMsg("unaligned segment indices (%d) and offsets (%d)",
			len(segIndices), len(segOffsets))
	}

	cResults := make([]C.CSearchResult, len(results))
	for i, r := range results {
		if r == nil {
			return nil, merr.WrapErrParameterInvalidMsg("nil search result at index %d", i)
		}
		cResults[i] = r.cSearchResult
	}

	var segIndicesPtr *C.int32_t
	var segOffsetsPtr *C.int64_t
	if len(segIndices) > 0 {
		segIndicesPtr = (*C.int32_t)(unsafe.Pointer(&segIndices[0]))
		segOffsetsPtr = (*C.int64_t)(unsafe.Pointer(&segOffsets[0]))
	}

	guard := NewCancellationGuard(ctx)
	defer guard.Close()

	var cProto C.CProto
	status := C.FillOutputFieldsOrdered(
		&cResults[0],
		C.int64_t(len(results)),
		plan.cSearchPlan,
		segIndicesPtr,
		segOffsetsPtr,
		C.int64_t(len(segIndices)),
		&cProto,
		guard.Source(),
	)
	runtime.KeepAlive(segIndices)
	runtime.KeepAlive(segOffsets)
	runtime.KeepAlive(cResults)
	runtime.KeepAlive(results)
	runtime.KeepAlive(plan)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}

	if cProto.proto_size == 0 {
		return nil, nil
	}
	// Copy to Go heap and free the C malloc'd buffer immediately.
	// Do NOT use getCProtoBlob here — it calls cgoconverter.Extract which
	// removes the pointer from the lease registry without calling C.free,
	// leaking the buffer.
	goBytes := C.GoBytes(cProto.proto_blob, C.int(cProto.proto_size))
	C.free(cProto.proto_blob)
	return goBytes, nil
}
