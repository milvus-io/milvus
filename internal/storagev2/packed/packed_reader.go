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
#include "segcore/packed_reader_c.h"
#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
*/
import "C"

import (
	"fmt"
	"io"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/cdata"
)

func NewPackedReader(filePaths []string, schema *arrow.Schema, bufferSize int64) (*PackedReader, error) {
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
	defer cdata.ReleaseCArrowSchema(&cas)

	cBufferSize := C.int64_t(bufferSize)

	var cPackedReader C.CPackedReader
	status := C.NewPackedReader(cFilePathsArray, cNumPaths, cSchema, cBufferSize, &cPackedReader)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}
	return &PackedReader{cPackedReader: cPackedReader, schema: schema}, nil
}

func (pr *PackedReader) ReadNext() (arrow.Record, error) {
	var cArr C.CArrowArray
	var cSchema C.CArrowSchema
	status := C.ReadNext(pr.cPackedReader, &cArr, &cSchema)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}

	if cArr == nil {
		return nil, io.EOF // end of stream, no more records to read
	}

	// Convert ArrowArray to Go RecordBatch using cdata
	goCArr := (*cdata.CArrowArray)(unsafe.Pointer(cArr))
	goCSchema := (*cdata.CArrowSchema)(unsafe.Pointer(cSchema))
	defer func() {
		cdata.ReleaseCArrowArray(goCArr)
		cdata.ReleaseCArrowSchema(goCSchema)
	}()
	recordBatch, err := cdata.ImportCRecordBatch(goCArr, goCSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to convert ArrowArray to Record: %w", err)
	}

	// Return the RecordBatch as an arrow.Record
	return recordBatch, nil
}

func (pr *PackedReader) Close() error {
	if pr.cPackedReader == nil {
		return nil
	}
	status := C.CloseReader(pr.cPackedReader)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return err
	}
	pr.cPackedReader = nil
	return nil
}
