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

func NewPackedWriter(path string, schema *arrow.Schema, bufferSize int) (*PackedWriter, error) {
	var cas cdata.CArrowSchema
	cdata.ExportArrowSchema(schema, &cas)
	cSchema := (*C.struct_ArrowSchema)(unsafe.Pointer(&cas))

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	cBufferSize := C.int64_t(bufferSize)

	var cPackedWriter C.CPackedWriter
	status := C.NewPackedWriter(cPath, cSchema, cBufferSize, &cPackedWriter)
	if status != 0 {
		return nil, fmt.Errorf("failed to new packed writer: %s, status: %d", path, status)
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
