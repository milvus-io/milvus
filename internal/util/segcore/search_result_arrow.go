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
#include "segcore/search_result_export_c.h"
*/
import "C"

import (
	"context"
	"runtime"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/cdata"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/pkg/v3/proto/cgopb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// MarshalFunctionChainInputPlan encodes the inputs for segcore. The returned
// bytes are immutable while in use and can be shared by per-segment exports.
func MarshalFunctionChainInputPlan(inputPlan *chain.DataFrameInputPlan) ([]byte, error) {
	if inputPlan == nil || len(inputPlan.Inputs) == 0 {
		return nil, nil
	}
	plan := &cgopb.FunctionChainInputPlan{
		Inputs: make([]*cgopb.FunctionChainInput, 0, len(inputPlan.Inputs)),
	}
	for index, input := range inputPlan.Inputs {
		if input.LogicalName == "" {
			return nil, merr.WrapErrServiceInternalMsg("function chain input projection %d has empty logical name", index)
		}
		targetType := input.DataType
		isJSONPath := input.DataType == schemapb.DataType_JSON
		if isJSONPath {
			if len(input.NestedPath) == 0 {
				return nil, merr.WrapErrServiceInternalMsg("function chain input projection %q has empty JSON path", input.LogicalName)
			}
			targetType = input.DataTypeHint
		} else if len(input.NestedPath) != 0 {
			return nil, merr.WrapErrServiceInternalMsg("scalar function chain input projection %q has a JSON path", input.LogicalName)
		}
		plan.Inputs = append(plan.Inputs, &cgopb.FunctionChainInput{
			SourceFieldId:  input.SourceFieldID,
			TargetDataType: targetType,
			LogicalName:    input.LogicalName,
			NestedPath:     input.NestedPath,
			IsJsonPath:     isJSONPath,
		})
	}
	blob, err := proto.Marshal(plan)
	if err != nil {
		return nil, merr.WrapErrSerializationFailed(err, "marshal function chain input plan")
	}
	return blob, nil
}

func classifyFunctionChainProjectionError(err error) error {
	if merr.IsSegcoreDataFormatBroken(err) {
		return merr.WrapErrDataIntegrity(err, "failed to project persisted function chain JSON data")
	}
	return err
}

// consumeArrowRecordBatch takes ownership of both C Data structs, including on
// export/import failure. A successful import moves array ownership to the record.
func consumeArrowRecordBatch(cSchema *C.struct_ArrowSchema, cArray *C.struct_ArrowArray, exportErr error) (arrow.Record, error) {
	defer C.MilvusGoArrowSchemaRelease(cSchema)
	defer C.MilvusGoArrowArrayRelease(cArray)
	if exportErr != nil {
		return nil, exportErr
	}
	schema, err := cdata.ImportCArrowSchema((*cdata.CArrowSchema)(unsafe.Pointer(cSchema)))
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "failed to import Arrow schema")
	}
	record, err := cdata.ImportCRecordBatchWithSchema((*cdata.CArrowArray)(unsafe.Pointer(cArray)), schema)
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "failed to import Arrow RecordBatch")
	}
	return record, nil
}

// consumeSearchResultArrowRecordBatch also copies and frees the C chunk sizes.
func consumeSearchResultArrowRecordBatch(
	cSchema *C.struct_ArrowSchema,
	cArray *C.struct_ArrowArray,
	chunkSizesPtr *C.int64_t,
	numChunks C.int64_t,
	exportErr error,
) (arrow.Record, []int64, error) {
	defer C.free(unsafe.Pointer(chunkSizesPtr))
	if exportErr == nil && (chunkSizesPtr == nil || numChunks <= 0) {
		exportErr = merr.WrapErrServiceInternal("missing Arrow RecordBatch chunk sizes")
	}
	record, err := consumeArrowRecordBatch(cSchema, cArray, exportErr)
	if err != nil {
		return nil, nil, err
	}
	chunkSizes := append([]int64(nil), unsafe.Slice((*int64)(unsafe.Pointer(chunkSizesPtr)), int(numChunks))...)
	return record, chunkSizes, nil
}

// ExportSearchResultAsArrowRecordBatchWithInputPlan exports system columns and
// the logical scalar/JSON-path columns declared by the serialized input plan.
// An empty input plan exports only system columns. The returned chunk sizes are
// the row counts per NQ; the caller must release the returned record.
// C++ parses the bytes synchronously and does not retain pointers into Go memory.
func ExportSearchResultAsArrowRecordBatchWithInputPlan(
	ctx context.Context,
	result *SearchResult,
	plan *SearchPlan,
	inputPlanBlob []byte,
) (arrow.Record, []int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if result == nil {
		return nil, nil, merr.WrapErrParameterInvalidMsg("nil search result")
	}
	if plan == nil || plan.cSearchPlan == nil {
		return nil, nil, merr.WrapErrParameterInvalidMsg("nil search plan")
	}

	var inputPlanPtr unsafe.Pointer
	if len(inputPlanBlob) > 0 {
		inputPlanPtr = unsafe.Pointer(&inputPlanBlob[0])
	}

	var cSchema C.struct_ArrowSchema
	var cArray C.struct_ArrowArray
	var chunkSizesPtr *C.int64_t
	var numChunks C.int64_t
	guard := NewCancellationGuard(ctx)
	defer guard.Close()
	status := C.ExportSearchResultAsArrowRecordBatchWithInputPlan(
		result.cSearchResult,
		plan.cSearchPlan,
		inputPlanPtr,
		C.int64_t(len(inputPlanBlob)),
		&cSchema,
		&cArray,
		&chunkSizesPtr,
		&numChunks,
		guard.Source(),
	)
	runtime.KeepAlive(inputPlanBlob)
	runtime.KeepAlive(result)
	runtime.KeepAlive(plan)
	exportErr := ConsumeCStatusIntoError(&status)
	if len(inputPlanBlob) > 0 {
		exportErr = classifyFunctionChainProjectionError(exportErr)
	}
	return consumeSearchResultArrowRecordBatch(&cSchema, &cArray, chunkSizesPtr, numChunks, exportErr)
}

// FillFieldsOrderedAsArrowRecordBatchWithInputPlan materializes logical
// scalar/JSON-path columns from a serialized input plan in the requested row order.
// C++ parses the bytes synchronously and does not retain pointers into Go memory.
func FillFieldsOrderedAsArrowRecordBatchWithInputPlan(
	ctx context.Context,
	results []*SearchResult,
	plan *SearchPlan,
	inputPlanBlob []byte,
	segIndices []int32,
	segOffsets []int64,
) (arrow.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if plan == nil || plan.cSearchPlan == nil {
		return nil, merr.WrapErrParameterInvalidMsg("nil search plan")
	}
	if len(results) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("empty search results")
	}
	if len(inputPlanBlob) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("empty function chain input plan")
	}
	if len(segIndices) != len(segOffsets) {
		return nil, merr.WrapErrParameterInvalidMsg("unaligned segment indices (%d) and offsets (%d)",
			len(segIndices), len(segOffsets))
	}

	cResults := make([]C.CSearchResult, len(results))
	for index, result := range results {
		if result == nil {
			return nil, merr.WrapErrParameterInvalidMsg("nil search result at index %d", index)
		}
		cResults[index] = result.cSearchResult
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
	status := C.FillFieldsOrderedAsArrowRecordBatchWithInputPlan(
		&cResults[0],
		C.int64_t(len(cResults)),
		plan.cSearchPlan,
		unsafe.Pointer(&inputPlanBlob[0]),
		C.int64_t(len(inputPlanBlob)),
		segIndicesPtr,
		segOffsetsPtr,
		C.int64_t(len(segIndices)),
		&cSchema,
		&cArray,
		guard.Source(),
	)
	runtime.KeepAlive(inputPlanBlob)
	runtime.KeepAlive(segIndices)
	runtime.KeepAlive(segOffsets)
	runtime.KeepAlive(cResults)
	runtime.KeepAlive(results)
	runtime.KeepAlive(plan)
	return consumeArrowRecordBatch(&cSchema, &cArray, classifyFunctionChainProjectionError(ConsumeCStatusIntoError(&status)))
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
