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

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/cdata"
	"github.com/cockroachdb/errors"
)

func NewPackedWriter(filePaths []string, multiPartUploadSize int64, schema *arrow.Schema, bufferSize int, columnGroups [][]int) (*PackedWriter, error) {
	var cas cdata.CArrowSchema
	cdata.ExportArrowSchema(schema, &cas)
	cSchema := (*C.struct_ArrowSchema)(unsafe.Pointer(&cas))

	cBufferSize := C.int64_t(bufferSize)

	cFilePaths := make([]*C.char, len(filePaths))
	for i, path := range filePaths {
		cFilePaths[i] = C.CString(path)             // Convert Go string to C string
		defer C.free(unsafe.Pointer(cFilePaths[i])) // Ensure memory is freed after use
	}

	// Create a pointer to the array of C strings (char**)
	cFilePathsArray := (**C.char)(unsafe.Pointer(&cFilePaths[0]))
	cNumPaths := C.int64_t(len(filePaths))

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

	cMultiPartUploadSize := C.int64_t(multiPartUploadSize)

	var cPackedWriter C.CPackedWriter
	status := C.NewPackedWriter(cSchema, cBufferSize, cFilePathsArray, cNumPaths, cMultiPartUploadSize, cColumnGroups, &cPackedWriter)
	if status != 0 {
		return nil, fmt.Errorf("failed to new packed writer")
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
	if status != 0 {
		return errors.New("PackedWriter: failed to write record batch")
	}

	return nil
}

func (pw *PackedWriter) Close() error {
	status := C.CloseWriter(pw.cPackedWriter)
	if status != 0 {
		return errors.New("PackedWriter: failed to close file")
	}
	return nil
}
