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
		e.BatchSize <= 0 || e.MaxResultBytes == 0 {
		return nil, merr.WrapErrServiceInternalMsg("invalid prepared grep budgets, limit or offsets")
	}
	if offsets.NullN() != 0 {
		return nil, merr.WrapErrServiceInternalMsg("grep offsets must be non-null Int64")
	}
	patternCount := req.Spec.PatternCount()
	if patternCount == 0 || uint64(patternCount) > math.MaxUint32 {
		return nil, merr.WrapErrServiceInternalMsg("invalid prepared grep pattern count")
	}
	var blob []byte
	lengths := make([]C.uint64_t, patternCount)
	for i := 0; i < patternCount; i++ {
		pattern := req.Spec.Pattern(i)
		blob = append(blob, pattern...)
		lengths[i] = C.uint64_t(len(pattern))
	}
	options := C.CGrepScanOptions{
		field_id: C.int64_t(req.FieldID), limit: C.int64_t(req.Limit), batch_size: C.int64_t(e.BatchSize),
		pattern_kind: C.uint32_t(req.Spec.PatternKind()), case_mode: C.uint32_t(req.Spec.CaseMode()), boundary: C.uint32_t(req.Spec.Boundary()),
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
		patterns, C.uint64_t(len(blob)), &lengths[0], C.uint32_t(patternCount), &schema, &data, guard.Source())
	runtime.KeepAlive(s.segment)
	runtime.KeepAlive(offsets)
	runtime.KeepAlive(blob)
	runtime.KeepAlive(lengths)
	err := ConsumeCStatusIntoError(&status)
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
	result, err := newGrepScanResult(record, offsets.Len(), req.Limit)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// newGrepScanResult validates the native boundary once. Callers may release
// record after return: positions are copied and details retain their own reference.
func newGrepScanResult(record arrow.Record, inputRows int, limit int64) (*native.ScanResult, error) {
	if record.NumCols() != 2 || record.NumRows() > limit {
		return nil, merr.WrapErrServiceInternalMsg("invalid native grep result schema or row count")
	}
	positions, positionsOK := record.Column(0).(*array.Int64)
	details, detailsOK := record.Column(1).(*array.List)
	if !positionsOK || !detailsOK || !arrow.TypeEqual(details.DataType(), native.GrepDetailsType()) {
		return nil, merr.WrapErrServiceInternalMsg("invalid native grep result column types")
	}
	if positions.NullN() != 0 || details.NullN() != 0 ||
		int64(positions.Len()) != record.NumRows() || details.Len() != positions.Len() {
		return nil, merr.WrapErrServiceInternalMsg("native grep result columns have inconsistent rows or nulls")
	}
	previous := int64(-1)
	for i, position := range positions.Int64Values() {
		if position <= previous || position >= int64(inputRows) {
			return nil, merr.WrapErrServiceInternalMsg("grep runner positions must be increasing input positions")
		}
		start, end := details.ValueOffsets(i)
		if start == end {
			return nil, merr.WrapErrServiceInternalMsg("grep runner selected a row without matching lines")
		}
		previous = position
	}
	selected := append([]int64(nil), positions.Int64Values()...)
	details.Retain()
	return &native.ScanResult{Positions: selected, Details: details}, nil
}
