// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package segcore

/*
#cgo pkg-config: milvus_core
#include "common/arrow_c_data_c.h"
#include "grep/segment_grep_c.h"
*/
import "C"

import (
	"context"
	"math"
	"runtime"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/cdata"

	"github.com/milvus-io/milvus/internal/util/function/grep/native"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// NewGrepSegment borrows a segcore segment without taking ownership. The query
// task must keep it pinned until ScanGrep returns. No matcher survives a call.
func NewGrepSegment(segment CSegment) native.Segment {
	return &grepSegment{segment: segment}
}

type grepSegment struct{ segment CSegment }

func (s *grepSegment) ScanGrep(ctx context.Context, offsets *array.Int64, req native.ScanRequest) (*native.ScanResult, error) {
	if ctx == nil || s == nil || s.segment == nil || s.segment.RawPointer() == nil || offsets == nil {
		return nil, merr.WrapErrServiceInternalMsg("grep requires context, segment and offsets")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c, e := req.CompileLimits, req.ExecutionLimits
	if req.Limit <= 0 || c.MaxRegexBytes == 0 || c.MaxDFACacheBytes == 0 ||
		e.BatchSize <= 0 || e.MaxResultBytes == 0 || offsets.NullN() != 0 {
		return nil, merr.WrapErrServiceInternalMsg("invalid prepared grep budgets, limit or offsets")
	}
	if len(req.Spec.Patterns) == 0 || uint64(len(req.Spec.Patterns)) > math.MaxUint32 {
		return nil, merr.WrapErrServiceInternalMsg("invalid prepared grep pattern count")
	}
	// Keep the C count and concatenated Go buffer representable; these are
	// transport bounds, not additional pattern policy limits.
	var total uint64
	for _, pattern := range req.Spec.Patterns {
		if uint64(len(pattern)) > uint64(math.MaxInt)-total {
			return nil, merr.WrapErrServiceInternalMsg("grep patterns exceed addressable memory")
		}
		total += uint64(len(pattern))
	}
	blob := make([]byte, 0, int(total))
	lengths := make([]C.uint64_t, len(req.Spec.Patterns))
	for i, pattern := range req.Spec.Patterns {
		blob = append(blob, pattern...)
		lengths[i] = C.uint64_t(len(pattern))
	}
	options := C.CGrepScanOptions{
		field_id: C.int64_t(req.FieldID), limit: C.int64_t(req.Limit), batch_size: C.int64_t(e.BatchSize),
		pattern_kind: C.uint32_t(req.Spec.PatternKind), case_mode: C.uint32_t(req.Spec.CaseMode), boundary: C.uint32_t(req.Spec.Boundary),
		max_regex_bytes: C.uint64_t(c.MaxRegexBytes), max_dfa_cache_bytes: C.uint64_t(c.MaxDFACacheBytes),
		max_result_bytes: C.uint64_t(e.MaxResultBytes),
	}
	var patterns *C.uint8_t
	if len(blob) > 0 {
		patterns = (*C.uint8_t)(unsafe.Pointer(&blob[0]))
	}
	values := offsets.Int64Values()
	var positions *C.int64_t
	if len(values) > 0 {
		positions = (*C.int64_t)(unsafe.Pointer(&values[0]))
	}
	var schema C.struct_ArrowSchema
	var data C.struct_ArrowArray
	defer C.MilvusGoArrowSchemaRelease(&schema)
	defer C.MilvusGoArrowArrayRelease(&data)
	guard := NewCancellationGuard(ctx)
	defer guard.Close()
	status := C.ScanGrepByOffsets(unsafe.Pointer(s.segment.RawPointer()), positions, C.int64_t(len(values)), &options,
		patterns, C.uint64_t(total), &lengths[0], C.uint32_t(len(lengths)), &schema, &data, guard.Source())
	runtime.KeepAlive(s.segment)
	runtime.KeepAlive(offsets)
	runtime.KeepAlive(blob)
	runtime.KeepAlive(lengths)
	err := ConsumeCStatusIntoError(&status)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, merr.Wrap(err, "scan grep segment")
	}
	importedSchema, err := cdata.ImportCArrowSchema((*cdata.CArrowSchema)(unsafe.Pointer(&schema)))
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "import grep result schema")
	}
	record, err := cdata.ImportCRecordBatchWithSchema((*cdata.CArrowArray)(unsafe.Pointer(&data)), importedSchema)
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "import grep result")
	}
	defer record.Release()
	if record.NumCols() != 2 || record.NumRows() > req.Limit || !arrow.TypeEqual(record.Column(0).DataType(), arrow.PrimitiveTypes.Int64) ||
		!arrow.TypeEqual(record.Column(1).DataType(), native.GrepDetailsType()) || record.Column(0).NullN() != 0 {
		return nil, merr.WrapErrServiceInternalMsg("invalid native grep result schema")
	}
	selected := append([]int64(nil), record.Column(0).(*array.Int64).Int64Values()...)
	details := record.Column(1)
	details.Retain()
	return &native.ScanResult{Positions: selected, Details: details}, nil
}
