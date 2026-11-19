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
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/cdata"

	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func NewPackedWriter(filePaths []string, schema *arrow.Schema, bufferSize int64, multiPartUploadSize int64, columnGroups []storagecommon.ColumnGroup, storageConfig *indexpb.StorageConfig, storagePluginContext *indexcgopb.StoragePluginContext) (*PackedWriter, error) {
	return newPackedWriter(filePaths, schema, bufferSize, multiPartUploadSize, columnGroups, storageConfig, storagePluginContext, false)
}

// NewImportPackedWriter uses the target keyset from the import task for this file.
func NewImportPackedWriter(filePaths []string, schema *arrow.Schema, bufferSize int64, multiPartUploadSize int64, columnGroups []storagecommon.ColumnGroup, storageConfig *indexpb.StorageConfig, target *indexcgopb.StoragePluginContext) (*PackedWriter, error) {
	return newPackedWriter(filePaths, schema, bufferSize, multiPartUploadSize, columnGroups, storageConfig, target, true)
}

func newPackedWriter(filePaths []string, schema *arrow.Schema, bufferSize int64, multiPartUploadSize int64, columnGroups []storagecommon.ColumnGroup, storageConfig *indexpb.StorageConfig, storagePluginContext *indexcgopb.StoragePluginContext, forImport bool) (*PackedWriter, error) {
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

	cMultiPartUploadSize := C.int64_t(multiPartUploadSize)

	cColumnSplits := C.NewCColumnSplits()
	if cColumnSplits == nil {
		return nil, merr.WrapErrServiceInternalMsg("failed to allocate column splits")
	}
	defer C.FreeCColumnSplits(cColumnSplits)
	for _, group := range columnGroups {
		cGroup := C.malloc(C.size_t(len(group.Columns)) * C.size_t(unsafe.Sizeof(C.int(0))))
		if cGroup == nil {
			return nil, merr.WrapErrServiceInternalMsg("failed to allocate memory for column groups")
		}
		cGroupSlice := (*[1 << 30]C.int)(cGroup)[:len(group.Columns):len(group.Columns)]
		for i, val := range group.Columns {
			cGroupSlice[i] = C.int(val)
		}
		ok := C.AddCColumnSplit(cColumnSplits, (*C.int)(cGroup), C.int(len(group.Columns)))
		C.free(cGroup)
		if !bool(ok) {
			return nil, merr.WrapErrServiceInternalMsg("failed to record column split group")
		}
	}

	var cPackedWriter C.CPackedWriter
	var status C.CStatus

	pluginContextPtr, release := newCPluginContext(storagePluginContext)
	defer release()
	var config *C.CStorageConfig
	if storageConfig != nil {
		cConfig, release := newCStorageConfig(storageConfig)
		defer release()
		config = &cConfig
	}
	if forImport {
		status = C.NewImportPackedWriter(cSchema, cBufferSize, cFilePathsArray, cNumPaths, cMultiPartUploadSize, cColumnSplits, config, &cPackedWriter, pluginContextPtr)
	} else if config != nil {
		status = C.NewPackedWriterWithStorageConfig(cSchema, cBufferSize, cFilePathsArray, cNumPaths, cMultiPartUploadSize, cColumnSplits, *config, &cPackedWriter, pluginContextPtr)
	} else {
		status = C.NewPackedWriter(cSchema, cBufferSize, cFilePathsArray, cNumPaths, cMultiPartUploadSize, cColumnSplits, &cPackedWriter, pluginContextPtr)
	}
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}
	return &PackedWriter{cPackedWriter: cPackedWriter}, nil
}

func (pw *PackedWriter) WriteRecordBatch(recordBatch arrow.Record) error {
	if recordBatch == nil || recordBatch.NumCols() == 0 {
		return nil
	}
	if pw.cPackedWriter == nil {
		return merr.WrapErrStorageMsg("packed writer is closed")
	}

	cArrays := make([]CArrowArray, recordBatch.NumCols())
	cSchemas := make([]CArrowSchema, recordBatch.NumCols())

	for i := range recordBatch.NumCols() {
		var caa cdata.CArrowArray
		var cas cdata.CArrowSchema
		cdata.ExportArrowArray(recordBatch.Column(int(i)), &caa, &cas)
		cArrays[i] = *(*CArrowArray)(unsafe.Pointer(&caa))
		cSchemas[i] = *(*CArrowSchema)(unsafe.Pointer(&cas))
	}

	var cas cdata.CArrowSchema
	cdata.ExportArrowSchema(recordBatch.Schema(), &cas)
	cSchema := (*C.struct_ArrowSchema)(unsafe.Pointer(&cas))

	status := C.WriteRecordBatch(pw.cPackedWriter, &cArrays[0], &cSchemas[0], cSchema)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return err
	}

	return nil
}

func (pw *PackedWriter) Close() error {
	if pw.cPackedWriter == nil {
		return nil
	}
	cPackedWriter := pw.cPackedWriter
	pw.cPackedWriter = nil
	status := C.CloseWriter(cPackedWriter)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return err
	}
	return nil
}

func (pw *PackedWriter) CloseAndTell(numGroups int) ([]int64, error) {
	if numGroups <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("numGroups must be greater than 0")
	}
	if pw.cPackedWriter == nil {
		if len(pw.closedSizes) == numGroups {
			return append([]int64(nil), pw.closedSizes...), nil
		}
		return nil, merr.WrapErrStorageMsg("packed writer is closed")
	}

	sizes := make([]int64, numGroups)
	cPackedWriter := pw.cPackedWriter
	pw.cPackedWriter = nil
	status := C.CloseAndTell(cPackedWriter, (*C.int64_t)(unsafe.Pointer(&sizes[0])), C.size_t(numGroups))
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}
	pw.closedSizes = append([]int64(nil), sizes...)
	return sizes, nil
}
