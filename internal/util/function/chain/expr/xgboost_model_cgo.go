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

//go:build cgo

package expr

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include "common/arrow_c_data_c.h"
#include "rescores/xgboost_model_c.h"
*/
import "C"

import (
	"runtime"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/cdata"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func loadXGBoostModel(resource *fileresource.ResolvedFileResource) (*modelHandle, error) {
	if resource == nil {
		return nil, merr.WrapErrParameterInvalidMsg("xgboost: file resource is nil")
	}
	if resource.LocalPath == "" {
		return nil, merr.WrapErrServiceInternalMsg("xgboost: local path is empty for resource %q", resource.Name)
	}

	path := C.CString(resource.LocalPath)
	defer C.free(unsafe.Pointer(path))

	result := C.LoadXGBoostUBJModel(path)
	if err := consumeXGBoostCStatus(&result.status); err != nil {
		if result.model != nil {
			status := C.DeleteXGBoostModel(result.model)
			_ = consumeXGBoostCStatus(&status)
		}
		return nil, err
	}
	if result.model == nil {
		return nil, merr.WrapErrServiceInternalMsg("xgboost: loader returned nil model for resource %q", resource.Name)
	}
	return &modelHandle{
		h:           unsafe.Pointer(result.model),
		numFeatures: int(result.num_features),
	}, nil
}

func predictXGBoostArrowChunks(model *modelHandle, inputs []*arrow.Chunked, outputDefault bool, allocator memory.Allocator) (*arrow.Chunked, error) {
	if model == nil || model.h == nil {
		return nil, merr.WrapErrServiceInternalMsg("xgboost: model handle is nil")
	}
	if allocator == nil {
		allocator = memory.DefaultAllocator
	}
	if len(inputs) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("xgboost: expected at least one input column")
	}

	chunks := make([]arrow.Array, len(inputs[0].Chunks()))
	for chunkIdx := range chunks {
		chunk, err := predictXGBoostArrowChunk(model, inputs, chunkIdx, outputDefault, allocator)
		if err != nil {
			for i := 0; i < chunkIdx; i++ {
				chunks[i].Release()
			}
			return nil, err
		}
		chunks[chunkIdx] = chunk
	}

	result := arrow.NewChunked(arrow.PrimitiveTypes.Float32, chunks)
	for _, chunk := range chunks {
		chunk.Release()
	}
	return result, nil
}

func predictXGBoostArrowChunk(model *modelHandle, inputs []*arrow.Chunked, chunkIdx int, outputDefault bool, allocator memory.Allocator) (arrow.Array, error) {
	rows := inputs[0].Chunk(chunkIdx).Len()
	featureArrays := make([]C.struct_ArrowArray, len(inputs))
	featureSchemas := make([]C.struct_ArrowSchema, len(inputs))
	defer func() {
		for idx := range featureArrays {
			C.MilvusGoArrowArrayRelease(&featureArrays[idx])
			C.MilvusGoArrowSchemaRelease(&featureSchemas[idx])
		}
	}()

	for colIdx, input := range inputs {
		chunk := input.Chunk(chunkIdx)
		if _, ok := newNumericReader(chunk); !ok {
			return nil, merr.WrapErrParameterInvalidMsg("xgboost: column %d: unsupported input column type %T, expected numeric type", colIdx, chunk)
		}
		cdata.ExportArrowArray(
			chunk,
			(*cdata.CArrowArray)(unsafe.Pointer(&featureArrays[colIdx])),
			(*cdata.CArrowSchema)(unsafe.Pointer(&featureSchemas[colIdx])),
		)
	}

	output := make([]float32, rows)
	var outputPtr *C.float
	var outputPin runtime.Pinner
	if len(output) > 0 {
		outputPin.Pin(&output[0])
		defer outputPin.Unpin()
		outputPtr = (*C.float)(unsafe.Pointer(&output[0]))
	}

	var featureArrayPtr *C.struct_ArrowArray
	var featureSchemaPtr *C.struct_ArrowSchema
	if len(featureArrays) > 0 {
		featureArrayPtr = &featureArrays[0]
		featureSchemaPtr = &featureSchemas[0]
	}
	status := C.PredictXGBoost(C.CXGBoostPredictRequest{
		model:           C.CXGBoostModel(model.h),
		feature_arrays:  featureArrayPtr,
		feature_schemas: featureSchemaPtr,
		num_features:    C.int32_t(len(inputs)),
		output_default:  C.bool(outputDefault),
		output:          outputPtr,
	})
	if err := consumeXGBoostCStatus(&status); err != nil {
		return nil, err
	}

	builder := array.NewFloat32Builder(allocator)
	defer builder.Release()
	builder.AppendValues(output, nil)
	return builder.NewArray(), nil
}

func closeXGBoostModel(model *modelHandle) error {
	if model == nil || model.h == nil {
		return nil
	}
	status := C.DeleteXGBoostModel(C.CXGBoostModel(model.h))
	model.h = nil
	return consumeXGBoostCStatus(&status)
}

func consumeXGBoostCStatus(status *C.CStatus) error {
	if status.error_code == 0 {
		return nil
	}
	errorCode := int32(status.error_code)
	errorMsg := C.GoString(status.error_msg)
	C.free(unsafe.Pointer(status.error_msg))
	return merr.SegcoreError(errorCode, errorMsg)
}
