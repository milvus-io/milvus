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
	"errors"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/cdata"

	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
)

type WriterMode uint8

const (
	OrdinaryWrite WriterMode = iota
	BackupImportWrite
	// FileImportWrite also accepts an older native cipher for the dispatched target.
	FileImportWrite
)

func NewPackedWriter(filePaths []string, schema *arrow.Schema, bufferSize int64, multiPartUploadSize int64, columnGroups []storagecommon.ColumnGroup, storageConfig *indexpb.StorageConfig, storagePluginContext *indexcgopb.StoragePluginContext) (*PackedWriter, error) {
	return NewPackedWriterWithMode(filePaths, schema, bufferSize, multiPartUploadSize, columnGroups, storageConfig, storagePluginContext, OrdinaryWrite)
}

func NewPackedWriterWithMode(filePaths []string, schema *arrow.Schema, bufferSize int64, multiPartUploadSize int64, columnGroups []storagecommon.ColumnGroup, storageConfig *indexpb.StorageConfig, storagePluginContext *indexcgopb.StoragePluginContext, mode WriterMode) (*PackedWriter, error) {
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

	cColumnGroups := C.NewCColumnGroups()
	defer C.FreeCColumnGroups(cColumnGroups)
	for _, group := range columnGroups {
		cGroup := C.malloc(C.size_t(len(group.Columns)) * C.size_t(unsafe.Sizeof(C.int(0))))
		if cGroup == nil {
			return nil, merr.WrapErrServiceInternalMsg("failed to allocate memory for column groups")
		}
		cGroupSlice := (*[1 << 30]C.int)(cGroup)[:len(group.Columns):len(group.Columns)]
		for i, val := range group.Columns {
			cGroupSlice[i] = C.int(val)
		}
		C.AddCColumnGroup(cColumnGroups, (*C.int)(cGroup), C.int(len(group.Columns)))
		C.free(cGroup)
	}

	var cPackedWriter C.CPackedWriter
	var status C.CStatus

	var pluginContextPtr *C.CPluginContext
	if storagePluginContext != nil {
		ckey := C.CString(storagePluginContext.EncryptionKey)
		defer C.free(unsafe.Pointer(ckey))

		var pluginContext C.CPluginContext
		pluginContext.ez_id = C.int64_t(storagePluginContext.EncryptionZoneId)
		pluginContext.collection_id = C.int64_t(storagePluginContext.CollectionId)
		pluginContext.key = ckey
		pluginContextPtr = &pluginContext
	}

	var config *C.CStorageConfig
	if storageConfig != nil {
		cConfig := GetCStorageConfig(storageConfig)
		defer DeleteCStorageConfig(cConfig)
		config = &cConfig
	}
	switch {
	case mode != OrdinaryWrite:
		status = C.NewImportPackedWriter(cSchema, cBufferSize, cFilePathsArray, cNumPaths, cMultiPartUploadSize, cColumnGroups, config, &cPackedWriter, pluginContextPtr, C.bool(mode == FileImportWrite))
	case config != nil:
		status = C.NewPackedWriterWithStorageConfig(cSchema, cBufferSize, cFilePathsArray, cNumPaths, cMultiPartUploadSize, cColumnGroups, *config, &cPackedWriter, pluginContextPtr)
	default:
		status = C.NewPackedWriter(cSchema, cBufferSize, cFilePathsArray, cNumPaths, cMultiPartUploadSize, cColumnGroups, &cPackedWriter, pluginContextPtr)
	}
	if err := ConsumeCStatusIntoError(&status); err != nil {
		if mode != OrdinaryWrite && (errors.Is(err, merr.ErrSegcoreUnsupported) || merr.GetErrorType(err) == merr.InputError) {
			return nil, retry.Unrecoverable(err)
		}
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
