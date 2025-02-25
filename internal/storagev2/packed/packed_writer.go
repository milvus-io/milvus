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
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include "segcore/packed_writer_c.h"
#include "segcore/column_groups_c.h"
#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/cdata"
)

func NewPackedWriter(filePaths []string, schema *arrow.Schema, bufferSize int64, multiPartUploadSize int64, columnGroups [][]int) (*PackedWriter, error) {
	cFilePaths := make([]*C.char, len(filePaths))
	for i, path := range filePaths {
		cFilePaths[i] = C.CString(path)
		defer C.free(unsafe.Pointer(cFilePaths[i]))
	}
	cFilePathsArray := (**C.char)(unsafe.Pointer(&cFilePaths[0]))
	cNumPaths := C.int64_t(len(filePaths))

	var cas cdata.CArrowSchema
	cdata.ExportArrowSchema(schema, &cas)
	cSchema := (*C.struct_ArrowSchema)(unsafe.Pointer(&cas))

	cBufferSize := C.int64_t(bufferSize)

	cMultiPartUploadSize := C.int64_t(multiPartUploadSize)

	cColumnGroups := C.NewCColumnGroups()
	for _, group := range columnGroups {
		cGroup := C.malloc(C.size_t(len(group)) * C.size_t(unsafe.Sizeof(C.int(0))))
		if cGroup == nil {
			return nil, fmt.Errorf("failed to allocate memory for column groups")
		}
		cGroupSlice := (*[1 << 30]C.int)(cGroup)[:len(group):len(group)]
		for i, val := range group {
			cGroupSlice[i] = C.int(val)
		}
		C.AddCColumnGroup(cColumnGroups, (*C.int)(cGroup), C.int(len(group)))
		C.free(cGroup)
	}

	var cPackedWriter C.CPackedWriter
	status := C.NewPackedWriter(cSchema, cBufferSize, cFilePathsArray, cNumPaths, cMultiPartUploadSize, cColumnGroups, &cPackedWriter)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}
	return &PackedWriter{cPackedWriter: cPackedWriter}, nil
}

func (pw *PackedWriter) WriteRecordBatch(recordBatch arrow.Record) error {
	var caa cdata.CArrowArray
	var cas cdata.CArrowSchema

	cdata.ExportArrowRecordBatch(recordBatch, &caa, &cas)

	cArr := (*C.struct_ArrowArray)(unsafe.Pointer(&caa))
	cSchema := (*C.struct_ArrowSchema)(unsafe.Pointer(&cas))

	status := C.WriteRecordBatch(pw.cPackedWriter, cArr, cSchema)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return err
	}

	return nil
}

func (pw *PackedWriter) Close() error {
	status := C.CloseWriter(pw.cPackedWriter)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return err
	}
	return nil
}
