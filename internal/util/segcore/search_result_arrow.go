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
#include "common/type_c.h"
#include "segcore/segment_c.h"
#include "segcore/plan_c.h"

// Arrow C Data Interface structs (standard ABI, layout matches Go cdata package)
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

struct ArrowSchema {
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;
    void (*release)(struct ArrowSchema*);
    void* private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;
    void (*release)(struct ArrowArray*);
    void* private_data;
};

#endif  // ARROW_C_DATA_INTERFACE

#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE

struct ArrowArrayStream {
    int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema*);
    int (*get_next)(struct ArrowArrayStream*, struct ArrowArray*);
    const char* (*get_last_error)(struct ArrowArrayStream*);
    void (*release)(struct ArrowArrayStream*);
    void* private_data;
};

#endif  // ARROW_C_STREAM_INTERFACE

static inline void
MilvusGoArrowSchemaRelease(struct ArrowSchema* schema) {
    if (schema != NULL && schema->release != NULL) {
        schema->release(schema);
    }
}

static inline int
MilvusGoArrowArrayIsReleased(const struct ArrowArray* array) {
    return array == NULL || array->release == NULL;
}

static inline void
MilvusGoArrowArrayRelease(struct ArrowArray* array) {
    if (!MilvusGoArrowArrayIsReleased(array)) {
        array->release(array);
    }
}

static inline int
MilvusGoArrowArrayStreamIsReleased(const struct ArrowArrayStream* stream) {
    return stream == NULL || stream->release == NULL;
}

static inline int
MilvusGoArrowArrayStreamGetSchema(struct ArrowArrayStream* stream, struct ArrowSchema* out) {
    return stream->get_schema(stream, out);
}

static inline int
MilvusGoArrowArrayStreamGetNext(struct ArrowArrayStream* stream, struct ArrowArray* out) {
    return stream->get_next(stream, out);
}

static inline const char*
MilvusGoArrowArrayStreamGetLastError(struct ArrowArrayStream* stream) {
    return stream->get_last_error(stream);
}

static inline void
MilvusGoArrowArrayStreamRelease(struct ArrowArrayStream* stream) {
    if (!MilvusGoArrowArrayStreamIsReleased(stream)) {
        stream->release(stream);
    }
}

CStatus
ExportSearchResultAsArrowStream(CSearchResult c_search_result,
                                CSearchPlan c_plan,
                                const int64_t* extra_field_ids,
                                int64_t num_extra_fields,
                                struct ArrowArrayStream* out_stream);

CStatus
FillOutputFieldsOrdered(CSearchResult* search_results,
                        int64_t num_search_results,
                        CSearchPlan c_plan,
                        const int32_t* result_seg_indices,
                        const int64_t* result_seg_offsets,
                        int64_t total_rows,
                        CProto* out_result);

void
GetSearchResultMetadata(CSearchResult c_search_result,
                        bool* has_group_by,
                        int64_t* group_size,
                        int64_t* scanned_remote_bytes,
                        int64_t* scanned_total_bytes);
*/
import "C"

import (
	"fmt"
	"io"
	"runtime"
	"syscall"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/arrio"
	"github.com/apache/arrow/go/v17/arrow/cdata"
	"github.com/cockroachdb/errors"
)

// ExportSearchResultAsArrowStream exports a per-segment C++ SearchResult as a stream
// of Arrow RecordBatches, one per NQ query.
// Each RecordBatch has columns: $id (int64/string), $score (float32),
// $seg_offset (int64), plus optional $group_by when group-by is enabled,
// optional $element_indices for element-level search, and any extra fields
// specified by extraFieldIDs (e.g., fields needed by L0 rerank).
// The $group_by and extra-field Arrow fields carry Milvus field_id/data_type
// metadata and preserve field nullability.
// The caller should iterate with reader.Read() until io.EOF.
func ExportSearchResultAsArrowStream(result *SearchResult, plan *SearchPlan, extraFieldIDs []int64) (arrio.Reader, error) {
	if result == nil {
		return nil, errors.New("nil search result")
	}
	if plan == nil || plan.cSearchPlan == nil {
		return nil, errors.New("nil search plan")
	}

	var extraPtr *C.int64_t
	if len(extraFieldIDs) > 0 {
		extraPtr = (*C.int64_t)(unsafe.Pointer(&extraFieldIDs[0]))
	}

	cStream := (*C.struct_ArrowArrayStream)(C.calloc(1, C.size_t(unsafe.Sizeof(C.struct_ArrowArrayStream{}))))
	if cStream == nil {
		return nil, errors.New("failed to allocate ArrowArrayStream")
	}
	status := C.ExportSearchResultAsArrowStream(
		result.cSearchResult,
		plan.cSearchPlan,
		extraPtr,
		C.int64_t(len(extraFieldIDs)),
		cStream,
	)
	runtime.KeepAlive(extraFieldIDs)
	runtime.KeepAlive(result)
	runtime.KeepAlive(plan)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		C.MilvusGoArrowArrayStreamRelease(cStream)
		C.free(unsafe.Pointer(cStream))
		return nil, errors.Wrap(err, "ExportSearchResultAsArrowStream failed")
	}

	reader, err := newCRecordBatchReader(cStream)
	if err != nil {
		return nil, errors.Wrap(err, "failed to import Arrow RecordBatchReader")
	}

	return reader, nil
}

type cRecordBatchReader struct {
	stream *C.struct_ArrowArrayStream
	arr    *C.struct_ArrowArray
	schema *arrow.Schema
	cur    arrow.Record
	err    error
}

func newCRecordBatchReader(stream *C.struct_ArrowArrayStream) (*cRecordBatchReader, error) {
	reader := &cRecordBatchReader{
		stream: stream,
		arr:    (*C.struct_ArrowArray)(C.calloc(1, C.size_t(unsafe.Sizeof(C.struct_ArrowArray{})))),
	}
	if reader.arr == nil {
		C.MilvusGoArrowArrayStreamRelease(stream)
		C.free(unsafe.Pointer(stream))
		return nil, errors.New("failed to allocate ArrowArray")
	}
	runtime.SetFinalizer(reader, (*cRecordBatchReader).Release)

	if err := reader.importSchema(); err != nil {
		reader.Release()
		return nil, err
	}

	return reader, nil
}

func (r *cRecordBatchReader) importSchema() error {
	var sc C.struct_ArrowSchema
	errno := C.MilvusGoArrowArrayStreamGetSchema(r.stream, &sc)
	if errno != 0 {
		return r.getError(int(errno))
	}
	defer C.MilvusGoArrowSchemaRelease(&sc)

	schema, err := cdata.ImportCArrowSchema((*cdata.CArrowSchema)(unsafe.Pointer(&sc)))
	if err != nil {
		return err
	}
	r.schema = schema
	return nil
}

func (r *cRecordBatchReader) Read() (arrow.Record, error) {
	if r.stream == nil {
		return nil, io.EOF
	}
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}

	errno := C.MilvusGoArrowArrayStreamGetNext(r.stream, r.arr)
	if errno != 0 {
		r.err = r.getError(int(errno))
		return nil, r.err
	}
	if C.MilvusGoArrowArrayIsReleased(r.arr) == 1 {
		r.err = io.EOF
		return nil, io.EOF
	}

	rec, err := cdata.ImportCRecordBatchWithSchema((*cdata.CArrowArray)(unsafe.Pointer(r.arr)), r.schema)
	if err != nil {
		C.MilvusGoArrowArrayRelease(r.arr)
		r.err = err
		return nil, err
	}
	r.cur = rec
	return rec, nil
}

func (r *cRecordBatchReader) Release() {
	if r == nil {
		return
	}
	runtime.SetFinalizer(r, nil)
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	if r.stream != nil {
		C.MilvusGoArrowArrayStreamRelease(r.stream)
		C.free(unsafe.Pointer(r.stream))
		r.stream = nil
	}
	if r.arr != nil {
		C.MilvusGoArrowArrayRelease(r.arr)
		C.free(unsafe.Pointer(r.arr))
		r.arr = nil
	}
}

func (r *cRecordBatchReader) Err() error {
	if r.err == io.EOF {
		return nil
	}
	return r.err
}

func (r *cRecordBatchReader) Record() arrow.Record {
	return r.cur
}

func (r *cRecordBatchReader) Schema() *arrow.Schema {
	return r.schema
}

func (r *cRecordBatchReader) getError(errno int) error {
	msg := C.MilvusGoArrowArrayStreamGetLastError(r.stream)
	if msg == nil {
		return syscall.Errno(errno)
	}
	return fmt.Errorf("%w: %s", syscall.Errno(errno), C.GoString(msg))
}

// FillOutputFieldsOrdered reads output fields from multiple segments in a single CGO call,
// producing results in the specified output order.
// Storage cost is accumulated in the original SearchResult objects.
//
// segIndices[i] specifies which results[] element the i-th output row came from.
// segOffsets[i] specifies the segment-internal offset for that row.
// Returns serialized schemapb.SearchResultData proto with only FieldsData populated.
func FillOutputFieldsOrdered(
	results []*SearchResult,
	plan *SearchPlan,
	segIndices []int32,
	segOffsets []int64,
) ([]byte, error) {
	if plan == nil || plan.cSearchPlan == nil {
		return nil, errors.New("nil search plan")
	}
	if len(results) == 0 {
		return nil, errors.New("empty search results")
	}
	if len(segIndices) != len(segOffsets) {
		return nil, errors.Newf("unaligned segment indices (%d) and offsets (%d)",
			len(segIndices), len(segOffsets))
	}

	cResults := make([]C.CSearchResult, len(results))
	for i, r := range results {
		if r == nil {
			return nil, errors.Newf("nil search result at index %d", i)
		}
		cResults[i] = r.cSearchResult
	}

	var segIndicesPtr *C.int32_t
	var segOffsetsPtr *C.int64_t
	if len(segIndices) > 0 {
		segIndicesPtr = (*C.int32_t)(unsafe.Pointer(&segIndices[0]))
		segOffsetsPtr = (*C.int64_t)(unsafe.Pointer(&segOffsets[0]))
	}

	var cProto C.CProto
	status := C.FillOutputFieldsOrdered(
		&cResults[0],
		C.int64_t(len(results)),
		plan.cSearchPlan,
		segIndicesPtr,
		segOffsetsPtr,
		C.int64_t(len(segIndices)),
		&cProto,
	)
	runtime.KeepAlive(segIndices)
	runtime.KeepAlive(segOffsets)
	runtime.KeepAlive(cResults)
	runtime.KeepAlive(results)
	runtime.KeepAlive(plan)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, errors.Wrap(err, "FillOutputFieldsOrdered failed")
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
