// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build cgo

package pyudf

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include "common/arrow_c_data_c.h"
#include "pyudf/pyudf_c.h"
*/
import "C"

import (
	"math"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/cdata"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type pyUDFCArrayImporter func(*cdata.CArrowArray, *cdata.CArrowSchema) (arrow.Field, arrow.Array, error)

// runPyUDFIdentity performs a synchronous Arrow C Data round trip through the
// native identity fake. Input buffers must be allocated outside the Go heap so
// native descriptor slots can retain them across cgo calls.
func runPyUDFIdentity(inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	return runPyUDFIdentityWithImporter(inputs, cdata.ImportCArray)
}

func runPyUDFIdentityWithImporter(inputs []*arrow.Chunked, importer pyUDFCArrayImporter) ([]*arrow.Chunked, error) {
	return runPyUDFArrowRoundTrip(inputs, importer, func(invocation C.CPyUDFInvocation, result *C.CPyUDFResult) C.CStatus {
		return C.RunPyUDFIdentity(invocation, result)
	})
}

func (resource *nativeResource) run(inputs []*arrow.Chunked, serializedParams []byte) ([]*arrow.Chunked, error) {
	return resource.runWithImporter(inputs, serializedParams, cdata.ImportCArray)
}

func (resource *nativeResource) runWithImporter(inputs []*arrow.Chunked, serializedParams []byte, importer pyUDFCArrayImporter) ([]*arrow.Chunked, error) {
	if resource == nil || resource.handle == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: native resource is closed")
	}
	var paramsPtr *C.uint8_t
	if len(serializedParams) > 0 {
		paramsPtr = (*C.uint8_t)(unsafe.Pointer(&serializedParams[0]))
	}
	return runPyUDFArrowRoundTrip(inputs, importer, func(invocation C.CPyUDFInvocation, result *C.CPyUDFResult) C.CStatus {
		return C.RunPyUDFResource(resource.handle, invocation, paramsPtr, C.uint64_t(len(serializedParams)), result)
	})
}

type pyUDFNativeRunner func(C.CPyUDFInvocation, *C.CPyUDFResult) C.CStatus

func runPyUDFArrowRoundTrip(inputs []*arrow.Chunked, importer pyUDFCArrayImporter, runner pyUDFNativeRunner) ([]*arrow.Chunked, error) {
	if importer == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: Arrow C Data importer is nil")
	}
	if runner == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: native runner is nil")
	}
	chunkSizes, err := validatePyUDFInputs(inputs)
	if err != nil {
		return nil, err
	}

	invocation, err := newPyUDFInvocation(len(inputs), chunkSizes)
	if err != nil {
		return nil, err
	}
	defer invocation.close()

	for inputIdx, input := range inputs {
		for chunkIdx, chunk := range input.Chunks() {
			arraySlot := C.PyUDFInvocationInputArray(invocation.handle, C.int32_t(inputIdx), C.int32_t(chunkIdx))
			schemaSlot := C.PyUDFInvocationInputSchema(invocation.handle, C.int32_t(inputIdx), C.int32_t(chunkIdx))
			if arraySlot == nil || schemaSlot == nil {
				return nil, merr.WrapErrServiceInternalMsg(
					"py_udf: native invocation returned nil descriptor slot for input %d chunk %d",
					inputIdx,
					chunkIdx,
				)
			}
			cdata.ExportArrowArray(
				chunk,
				(*cdata.CArrowArray)(unsafe.Pointer(arraySlot)),
				(*cdata.CArrowSchema)(unsafe.Pointer(schemaSlot)),
			)
		}
	}

	var resultHandle C.CPyUDFResult
	status := runner(invocation.handle, &resultHandle)
	if err := consumePyUDFCStatus(&status); err != nil {
		if resultHandle != nil {
			C.DeletePyUDFResult(resultHandle)
		}
		return nil, err
	}
	if resultHandle == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: native runner returned nil result")
	}
	result := pyUDFResult{handle: resultHandle}
	defer result.close()

	return importPyUDFResult(&result, chunkSizes, importer)
}

func validatePyUDFInputs(inputs []*arrow.Chunked) ([]int64, error) {
	if len(inputs) > math.MaxInt32 {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: input count %d exceeds native limit", len(inputs))
	}
	if len(inputs) == 0 {
		return nil, nil
	}
	if inputs[0] == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: input column 0 is nil")
	}

	numChunks := len(inputs[0].Chunks())
	if numChunks > math.MaxInt32 {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: chunk count %d exceeds native limit", numChunks)
	}
	chunkSizes := make([]int64, numChunks)
	for chunkIdx, chunk := range inputs[0].Chunks() {
		if chunk == nil {
			return nil, merr.WrapErrServiceInternalMsg("py_udf: input column 0 chunk %d is nil", chunkIdx)
		}
		chunkSizes[chunkIdx] = int64(chunk.Len())
	}

	for inputIdx, input := range inputs {
		if input == nil {
			return nil, merr.WrapErrServiceInternalMsg("py_udf: input column %d is nil", inputIdx)
		}
		if len(input.Chunks()) != numChunks {
			return nil, merr.WrapErrServiceInternalMsg(
				"py_udf: input column %d has %d chunks, expected %d",
				inputIdx,
				len(input.Chunks()),
				numChunks,
			)
		}
		for chunkIdx, chunk := range input.Chunks() {
			if chunk == nil {
				return nil, merr.WrapErrServiceInternalMsg("py_udf: input column %d chunk %d is nil", inputIdx, chunkIdx)
			}
			if !arrow.TypeEqual(chunk.DataType(), input.DataType()) {
				return nil, merr.WrapErrServiceInternalMsg(
					"py_udf: input column %d chunk %d has type %s, expected %s",
					inputIdx,
					chunkIdx,
					chunk.DataType(),
					input.DataType(),
				)
			}
			if int64(chunk.Len()) != chunkSizes[chunkIdx] {
				return nil, merr.WrapErrServiceInternalMsg(
					"py_udf: input column %d chunk %d has %d rows, expected %d",
					inputIdx,
					chunkIdx,
					chunk.Len(),
					chunkSizes[chunkIdx],
				)
			}
		}
	}
	return chunkSizes, nil
}

type pyUDFInvocation struct {
	handle C.CPyUDFInvocation
}

func newPyUDFInvocation(numInputs int, chunkSizes []int64) (*pyUDFInvocation, error) {
	var chunkSizesPtr *C.int64_t
	if len(chunkSizes) > 0 {
		chunkSizesPtr = (*C.int64_t)(unsafe.Pointer(&chunkSizes[0]))
	}
	var handle C.CPyUDFInvocation
	status := C.NewPyUDFInvocation(
		C.int32_t(numInputs),
		C.int32_t(len(chunkSizes)),
		chunkSizesPtr,
		&handle,
	)
	if err := consumePyUDFCStatus(&status); err != nil {
		return nil, err
	}
	if handle == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: native invocation constructor returned nil handle")
	}
	return &pyUDFInvocation{handle: handle}, nil
}

func (invocation *pyUDFInvocation) close() {
	if invocation == nil || invocation.handle == nil {
		return
	}
	C.DeletePyUDFInvocation(invocation.handle)
	invocation.handle = nil
}

type pyUDFResult struct {
	handle C.CPyUDFResult
}

func (result *pyUDFResult) close() {
	if result == nil || result.handle == nil {
		return
	}
	C.DeletePyUDFResult(result.handle)
	result.handle = nil
}

func importPyUDFResult(result *pyUDFResult, chunkSizes []int64, importer pyUDFCArrayImporter) ([]*arrow.Chunked, error) {
	numOutputs := int(C.PyUDFResultNumOutputs(result.handle))
	if numOutputs < 0 {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: native result returned invalid output count %d", numOutputs)
	}

	outputs := make([]*arrow.Chunked, 0, numOutputs)
	for outputIdx := 0; outputIdx < numOutputs; outputIdx++ {
		numChunks := int(C.PyUDFResultNumChunks(result.handle, C.int32_t(outputIdx)))
		if numChunks != len(chunkSizes) {
			releasePyUDFChunked(outputs)
			return nil, merr.WrapErrServiceInternalMsg(
				"py_udf: native output %d has %d chunks, expected %d",
				outputIdx,
				numChunks,
				len(chunkSizes),
			)
		}
		if numChunks == 0 {
			releasePyUDFChunked(outputs)
			return nil, merr.WrapErrServiceInternalMsg(
				"py_udf: native output %d has no chunks or type metadata",
				outputIdx,
			)
		}

		chunks := make([]arrow.Array, 0, numChunks)
		var dataType arrow.DataType
		for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
			arraySlot := C.PyUDFResultArray(result.handle, C.int32_t(outputIdx), C.int32_t(chunkIdx))
			schemaSlot := C.PyUDFResultSchema(result.handle, C.int32_t(outputIdx), C.int32_t(chunkIdx))
			if arraySlot == nil || schemaSlot == nil {
				releasePyUDFArrays(chunks)
				releasePyUDFChunked(outputs)
				return nil, merr.WrapErrServiceInternalMsg(
					"py_udf: native result returned nil descriptor slot for output %d chunk %d",
					outputIdx,
					chunkIdx,
				)
			}
			field, chunk, err := importer(
				(*cdata.CArrowArray)(unsafe.Pointer(arraySlot)),
				(*cdata.CArrowSchema)(unsafe.Pointer(schemaSlot)),
			)
			if err != nil {
				C.MilvusGoArrowArrayRelease(arraySlot)
				releasePyUDFArrays(chunks)
				releasePyUDFChunked(outputs)
				return nil, merr.WrapErrServiceInternalErr(err, "py_udf: import output %d chunk %d", outputIdx, chunkIdx)
			}
			if chunk == nil {
				releasePyUDFArrays(chunks)
				releasePyUDFChunked(outputs)
				return nil, merr.WrapErrServiceInternalMsg("py_udf: importer returned nil output %d chunk %d", outputIdx, chunkIdx)
			}
			if !arrow.TypeEqual(field.Type, chunk.DataType()) {
				chunk.Release()
				releasePyUDFArrays(chunks)
				releasePyUDFChunked(outputs)
				return nil, merr.WrapErrServiceInternalMsg(
					"py_udf: output %d chunk %d imported type %s but schema reports %s",
					outputIdx,
					chunkIdx,
					chunk.DataType(),
					field.Type,
				)
			}
			if dataType == nil {
				dataType = field.Type
			} else if !arrow.TypeEqual(dataType, field.Type) {
				chunk.Release()
				releasePyUDFArrays(chunks)
				releasePyUDFChunked(outputs)
				return nil, merr.WrapErrServiceInternalMsg(
					"py_udf: output %d chunk %d has type %s, expected %s",
					outputIdx,
					chunkIdx,
					field.Type,
					dataType,
				)
			}
			if int64(chunk.Len()) != chunkSizes[chunkIdx] {
				chunk.Release()
				releasePyUDFArrays(chunks)
				releasePyUDFChunked(outputs)
				return nil, merr.WrapErrServiceInternalMsg(
					"py_udf: output %d chunk %d has %d rows, expected %d",
					outputIdx,
					chunkIdx,
					chunk.Len(),
					chunkSizes[chunkIdx],
				)
			}
			chunks = append(chunks, chunk)
		}

		output := arrow.NewChunked(dataType, chunks)
		releasePyUDFArrays(chunks)
		outputs = append(outputs, output)
	}
	return outputs, nil
}

func releasePyUDFArrays(arrays []arrow.Array) {
	for _, array := range arrays {
		array.Release()
	}
}

func releasePyUDFChunked(chunked []*arrow.Chunked) {
	for _, column := range chunked {
		column.Release()
	}
}

func consumePyUDFCStatus(status *C.CStatus) error {
	if status.error_code == 0 {
		return nil
	}
	errorCode := int32(status.error_code)
	errorMsg := C.GoString(status.error_msg)
	C.free(unsafe.Pointer(status.error_msg))
	if errorCode == int32(C.PyUDFErrorCodeFunctionFailed) {
		return merr.WrapErrFunctionFailedMsg("%s", errorMsg)
	}
	return merr.SegcoreError(errorCode, errorMsg)
}
