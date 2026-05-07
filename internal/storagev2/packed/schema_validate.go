// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package packed

/*
#include <stdlib.h>
#include "arrow/c/abi.h"
*/
import "C"

import (
	"fmt"
	"strings"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"

	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// ValidateSchemaConsistency checks that the actual Arrow schema returned from C++
// is type-compatible with the expected schema. This prevents SIGSEGV when the
// underlying data type (e.g. JSON/UTF8) doesn't match the expected type (e.g. Int64)
// due to field ID reuse across backup/restore with different schemas.
//
// Fields are matched by name (in binlog context, names are field ID strings).
// Only Arrow type IDs are compared (coarse-grained check).
func ValidateSchemaConsistency(expected, actual *arrow.Schema) error {
	if expected == nil || actual == nil {
		return nil
	}

	actualFieldMap := make(map[string]arrow.Field, len(actual.Fields()))
	for _, f := range actual.Fields() {
		actualFieldMap[f.Name] = f
	}

	var mismatches []string
	for _, ef := range expected.Fields() {
		af, ok := actualFieldMap[ef.Name]
		if !ok {
			continue // field not present in actual schema, skip (projection may have removed it)
		}
		if ef.Type.ID() != af.Type.ID() {
			mismatches = append(mismatches, fmt.Sprintf(
				"field %q: expected type %s (ID=%d) but got %s (ID=%d)",
				ef.Name, ef.Type, ef.Type.ID(), af.Type, af.Type.ID(),
			))
		}
	}

	if len(mismatches) > 0 {
		return merr.WrapErrImportFailed(
			fmt.Sprintf("schema type mismatch between expected and actual data: %s", strings.Join(mismatches, "; ")),
		)
	}
	return nil
}

// validateCArrayBufferLayout inspects the raw C ArrowArray structure to verify
// that each child's buffer count matches what the schema type expects.
//
// This is the critical defense against SIGSEGV: C++ PackedRecordBatchReader may
// produce a RecordBatch where the schema says type A (from the expected/target schema)
// but the actual data buffers are laid out for type B (from the source parquet files).
// The exported CArrowSchema lies (uses expected types), but the CArrowArray children's
// n_buffers reflects the true buffer layout. Comparing these catches the mismatch
// before ImportCRecordBatch tries to interpret incompatible buffers.
//
// Example: schema says Int64 (2 buffers: validity+values) but data is actually
// String/JSON (3 buffers: validity+offsets+data). Reading 800 bytes of "values"
// from a 400-byte offsets buffer → SIGSEGV.
func validateCArrayBufferLayout(cArr unsafe.Pointer, schema *arrow.Schema) error {
	if cArr == nil || schema == nil {
		return nil
	}

	arr := (*C.struct_ArrowArray)(cArr)
	nChildren := int(arr.n_children)
	nFields := len(schema.Fields())

	if nChildren != nFields {
		return merr.WrapErrImportFailed(
			fmt.Sprintf("field count mismatch: schema has %d fields but data has %d children", nFields, nChildren),
		)
	}

	if nChildren == 0 {
		return nil
	}

	children := unsafe.Slice(arr.children, nChildren)

	var mismatches []string
	for i, field := range schema.Fields() {
		child := children[i]
		if child == nil {
			continue
		}
		expected := expectedBufferCount(field.Type)
		if expected < 0 {
			continue // unknown type, skip validation
		}
		actual := int(child.n_buffers)
		if expected != actual {
			mismatches = append(mismatches, fmt.Sprintf(
				"field %q (type %s): expected %d buffers but data has %d (possible type mismatch in source data)",
				field.Name, field.Type, expected, actual,
			))
		}
	}

	if len(mismatches) > 0 {
		return merr.WrapErrImportFailed(
			fmt.Sprintf("data buffer layout mismatch, source data types may differ from target schema: %s",
				strings.Join(mismatches, "; ")),
		)
	}
	return nil
}

// newFakeCArrowArray creates a minimal C ArrowArray with the specified children n_buffers.
// Used for testing validateCArrayBufferLayout without real Arrow data.
func newFakeCArrowArray(childBufferCounts []int) (*C.struct_ArrowArray, func()) {
	nChildren := len(childBufferCounts)

	parent := (*C.struct_ArrowArray)(C.malloc(C.size_t(unsafe.Sizeof(C.struct_ArrowArray{}))))
	parent.length = 100
	parent.null_count = 0
	parent.offset = 0
	parent.n_buffers = 1
	parent.n_children = C.int64_t(nChildren)
	parent.release = nil
	parent.dictionary = nil
	parent.private_data = nil
	parent.buffers = nil

	if nChildren > 0 {
		childrenPtr := (**C.struct_ArrowArray)(C.malloc(C.size_t(uintptr(nChildren) * unsafe.Sizeof((*C.struct_ArrowArray)(nil)))))
		children := unsafe.Slice(childrenPtr, nChildren)
		for i, nBufs := range childBufferCounts {
			child := (*C.struct_ArrowArray)(C.malloc(C.size_t(unsafe.Sizeof(C.struct_ArrowArray{}))))
			child.length = 100
			child.null_count = 0
			child.offset = 0
			child.n_buffers = C.int64_t(nBufs)
			child.n_children = 0
			child.children = nil
			child.release = nil
			child.dictionary = nil
			child.private_data = nil
			child.buffers = nil
			children[i] = child
		}
		parent.children = childrenPtr
	} else {
		parent.children = nil
	}

	cleanup := func() {
		if nChildren > 0 {
			children := unsafe.Slice(parent.children, nChildren)
			for _, child := range children {
				C.free(unsafe.Pointer(child))
			}
			C.free(unsafe.Pointer(parent.children))
		}
		C.free(unsafe.Pointer(parent))
	}

	return parent, cleanup
}

// expectedBufferCount returns the number of buffers expected for an Arrow type
// according to the Arrow columnar format specification.
// Returns -1 for unknown types (validation will be skipped).
func expectedBufferCount(dt arrow.DataType) int {
	switch dt.ID() {
	case arrow.NULL:
		return 0
	case arrow.BOOL,
		arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64,
		arrow.DATE32, arrow.DATE64,
		arrow.TIME32, arrow.TIME64,
		arrow.TIMESTAMP, arrow.DURATION,
		arrow.INTERVAL_MONTHS, arrow.INTERVAL_DAY_TIME, arrow.INTERVAL_MONTH_DAY_NANO,
		arrow.DECIMAL128, arrow.DECIMAL256,
		arrow.FIXED_SIZE_BINARY:
		return 2 // validity bitmap + values
	case arrow.STRING, arrow.BINARY:
		return 3 // validity bitmap + offsets + data
	case arrow.LARGE_STRING, arrow.LARGE_BINARY:
		return 3 // validity bitmap + offsets + data
	case arrow.LIST, arrow.MAP:
		return 2 // validity bitmap + offsets
	case arrow.LARGE_LIST:
		return 2 // validity bitmap + offsets
	case arrow.FIXED_SIZE_LIST, arrow.STRUCT:
		return 1 // validity bitmap only
	case arrow.DENSE_UNION:
		return 2 // type_ids + offsets
	case arrow.SPARSE_UNION:
		return 1 // type_ids only
	default:
		return -1 // unknown, skip validation
	}
}
