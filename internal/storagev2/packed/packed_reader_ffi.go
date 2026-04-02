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
#cgo pkg-config: milvus_core milvus-storage

#include <stdlib.h>
#include "milvus-storage/ffi_c.h"
#include "storage/loon_ffi/ffi_writer_c.h"
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
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

func NewFFIPackedReader(manifestPath string, schema *arrow.Schema, neededColumns []string, bufferSize int64, storageConfig *indexpb.StorageConfig, storagePluginContext *indexcgopb.StoragePluginContext) (*FFIPackedReader, error) {
	if storageConfig == nil {
		return nil, fmt.Errorf("storageConfig is required")
	}

	cLoonManifest, err := GetManifestHandle(manifestPath, storageConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get manifest")
	}
	defer C.loon_manifest_destroy(cLoonManifest)

	var cas cdata.CArrowSchema
	cdata.ExportArrowSchema(schema, &cas)
	cSchema := (*C.struct_ArrowSchema)(unsafe.Pointer(&cas))
	defer cdata.ReleaseCArrowSchema(&cas)

	var cNeededColumnPtr **C.char
	cNumColumns := C.size_t(len(neededColumns))
	if len(neededColumns) > 0 {
		cNeededColumn := make([]*C.char, len(neededColumns))
		for i, columnName := range neededColumns {
			cNeededColumn[i] = C.CString(columnName)
			defer C.free(unsafe.Pointer(cNeededColumn[i]))
		}
		cNeededColumnPtr = (**C.char)(unsafe.Pointer(&cNeededColumn[0]))
	}

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create properties")
	}
	defer C.loon_properties_free(cProperties)

	var readerHandle C.LoonReaderHandle
	result := C.loon_reader_new(&cLoonManifest.column_groups, cSchema, cNeededColumnPtr, cNumColumns, cProperties, &readerHandle)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, errors.Wrap(err, "failed to create reader")
	}

	// Configure CMEK decryption if plugin context is provided
	if storagePluginContext != nil {
		ckey := C.CString(storagePluginContext.EncryptionKey)
		defer C.free(unsafe.Pointer(ckey))
		var pluginContext C.CPluginContext
		pluginContext.ez_id = C.int64_t(storagePluginContext.EncryptionZoneId)
		pluginContext.collection_id = C.int64_t(storagePluginContext.CollectionId)
		pluginContext.key = ckey

		status := C.SetupReaderKeyRetriever(readerHandle, &pluginContext)
		if err := ConsumeCStatusIntoError(&status); err != nil {
			C.loon_reader_destroy(readerHandle)
			return nil, errors.Wrap(err, "failed to setup CMEK key retriever")
		}
	}

	// Get the ArrowArrayStream
	var cStream cdata.CArrowArrayStream
	result = C.loon_get_record_batch_reader(readerHandle, nil, (*C.struct_ArrowArrayStream)(unsafe.Pointer(&cStream)))
	if err := HandleLoonFFIResult(result); err != nil {
		C.loon_reader_destroy(readerHandle)
		return nil, fmt.Errorf("failed to get reader stream: %w", err)
	}

	// Import the stream as a RecordReader
	recordReader, err := cdata.ImportCRecordReader(&cStream, schema)
	if err != nil {
		C.loon_reader_destroy(readerHandle)
		return nil, fmt.Errorf("failed to import record reader: %w", err)
	}

	return &FFIPackedReader{
		cReaderHandle: readerHandle,
		recordReader:  recordReader,
		schema:        schema,
	}, nil
}

// ReadNext reads the next record batch from the reader
func (r *FFIPackedReader) ReadNext() (arrow.Record, error) {
	if r.recordReader == nil {
		return nil, io.EOF
	}

	// no need to manual release
	// stream reader will release previous one

	// Read next record from the stream
	rec, err := r.recordReader.Read()
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("failed to read next record: %w", err)
	}

	return rec, nil
}

// Close closes the FFI reader
func (r *FFIPackedReader) Close() error {
	if r.cReaderHandle == 0 {
		return nil
	}

	// no need to manual release current batch
	// stream reader handles it

	if r.recordReader != nil {
		r.recordReader = nil
	}

	C.loon_reader_destroy(r.cReaderHandle)
	r.cReaderHandle = 0
	return nil
}

// Schema returns the schema of the reader
func (r *FFIPackedReader) Schema() *arrow.Schema {
	return r.schema
}

// Retain increases the reference count
func (r *FFIPackedReader) Retain() {
}

// Release decreases the reference count
func (r *FFIPackedReader) Release() {
	r.Close()
}

func GetManifestHandle(manifestPath string, storageConfig *indexpb.StorageConfig) (loonManifestHandle *C.LoonManifest, err error) {
	var cManifestHandle *C.LoonManifest
	basePath, version, err := UnmarshalManifestPath(manifestPath)
	if err != nil {
		return cManifestHandle, err
	}
	log.Info("GetManifest", zap.String("manifestPath", manifestPath), zap.String("basePath", basePath), zap.Int64("version", version))

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return cManifestHandle, err
	}
	defer C.loon_properties_free(cProperties)
	cBasePath := C.CString(basePath)
	defer C.free(unsafe.Pointer(cBasePath))

	var cTransactionHandle C.LoonTransactionHandle
	result := C.loon_transaction_begin(cBasePath, cProperties, C.int64_t(version), C.int32_t(0) /* resolve_id */, C.uint32_t(1) /* retry_limit */, &cTransactionHandle)
	err = HandleLoonFFIResult(result)
	if err != nil {
		return cManifestHandle, err
	}
	defer C.loon_transaction_destroy(cTransactionHandle)

	result = C.loon_transaction_get_manifest(cTransactionHandle, &cManifestHandle)
	err = HandleLoonFFIResult(result)
	if err != nil {
		return cManifestHandle, err
	}

	return cManifestHandle, nil
}

// Ensure FFIPackedReader implements array.RecordReader interface
// var _ array.RecordReader = (*FFIPackedReader)(nil)
