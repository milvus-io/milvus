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
#include "storage/loon_ffi/ffi_reader_c.h"
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
	cColumnGroups, err := GetColumnGroups(manifestPath, storageConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get manifest")
	}

	var cas cdata.CArrowSchema
	cdata.ExportArrowSchema(schema, &cas)
	cSchema := (*C.struct_ArrowSchema)(unsafe.Pointer(&cas))
	defer cdata.ReleaseCArrowSchema(&cas)

	var cPackedReader C.CFFIPackedReader
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

	if storageConfig != nil {
		cStorageConfig := C.CStorageConfig{
			address:                C.CString(storageConfig.GetAddress()),
			bucket_name:            C.CString(storageConfig.GetBucketName()),
			access_key_id:          C.CString(storageConfig.GetAccessKeyID()),
			access_key_value:       C.CString(storageConfig.GetSecretAccessKey()),
			root_path:              C.CString(storageConfig.GetRootPath()),
			storage_type:           C.CString(storageConfig.GetStorageType()),
			cloud_provider:         C.CString(storageConfig.GetCloudProvider()),
			iam_endpoint:           C.CString(storageConfig.GetIAMEndpoint()),
			log_level:              C.CString("Warn"), // TODO use config after storage support lower case configuration
			useSSL:                 C.bool(storageConfig.GetUseSSL()),
			sslCACert:              C.CString(storageConfig.GetSslCACert()),
			useIAM:                 C.bool(storageConfig.GetUseIAM()),
			region:                 C.CString(storageConfig.GetRegion()),
			useVirtualHost:         C.bool(storageConfig.GetUseVirtualHost()),
			requestTimeoutMs:       C.int64_t(storageConfig.GetRequestTimeoutMs()),
			gcp_credential_json:    C.CString(storageConfig.GetGcpCredentialJSON()),
			use_custom_part_upload: true,
			max_connections:        C.uint32_t(storageConfig.GetMaxConnections()),
		}
		defer C.free(unsafe.Pointer(cStorageConfig.address))
		defer C.free(unsafe.Pointer(cStorageConfig.bucket_name))
		defer C.free(unsafe.Pointer(cStorageConfig.access_key_id))
		defer C.free(unsafe.Pointer(cStorageConfig.access_key_value))
		defer C.free(unsafe.Pointer(cStorageConfig.root_path))
		defer C.free(unsafe.Pointer(cStorageConfig.storage_type))
		defer C.free(unsafe.Pointer(cStorageConfig.cloud_provider))
		defer C.free(unsafe.Pointer(cStorageConfig.iam_endpoint))
		defer C.free(unsafe.Pointer(cStorageConfig.log_level))
		defer C.free(unsafe.Pointer(cStorageConfig.sslCACert))
		defer C.free(unsafe.Pointer(cStorageConfig.region))
		defer C.free(unsafe.Pointer(cStorageConfig.gcp_credential_json))

		cNeededColumn := make([]*C.char, len(neededColumns))
		for i, columnName := range neededColumns {
			cNeededColumn[i] = C.CString(columnName)
			defer C.free(unsafe.Pointer(cNeededColumn[i]))
		}
		cNeededColumnArray := (**C.char)(unsafe.Pointer(&cNeededColumn[0]))
		cNumColumns := C.int64_t(len(neededColumns))

		status = C.NewPackedFFIReaderWithManifest(cColumnGroups, cSchema, cNeededColumnArray, cNumColumns, &cPackedReader, cStorageConfig, pluginContextPtr)
	} else {
		return nil, fmt.Errorf("storageConfig is required")
	}
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}

	// Get the ArrowArrayStream
	var cStream cdata.CArrowArrayStream
	status = C.GetFFIReaderStream(cPackedReader, C.int64_t(8196), (*C.struct_ArrowArrayStream)(unsafe.Pointer(&cStream)))
	if err := ConsumeCStatusIntoError(&status); err != nil {
		C.CloseFFIReader(cPackedReader)
		return nil, fmt.Errorf("failed to get reader stream: %w", err)
	}

	// Import the stream as a RecordReader
	recordReader, err := cdata.ImportCRecordReader(&cStream, schema)
	if err != nil {
		C.CloseFFIReader(cPackedReader)
		return nil, fmt.Errorf("failed to import record reader: %w", err)
	}

	return &FFIPackedReader{
		cPackedReader: cPackedReader,
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
	// no need to manual release current batch
	// stream reader handles it

	if r.recordReader != nil {
		r.recordReader = nil
	}

	status := C.CloseFFIReader(r.cPackedReader)
	return ConsumeCStatusIntoError(&status)
}

// Schema returns the schema of the reader
func (r *FFIPackedReader) Schema() *arrow.Schema {
	return r.schema
}

// Retain increases the reference count
func (r *FFIPackedReader) Retain() {
	// if r.recordReader != nil {
	// r.recordReader.Retain()
	// }
}

// Release decreases the reference count
func (r *FFIPackedReader) Release() {
	r.Close()
}

func GetColumnGroups(manifestPath string, storageConfig *indexpb.StorageConfig) (columnGroups C.ColumnGroupsHandle, err error) {
	var cColumnGroups C.ColumnGroupsHandle
	basePath, version, err := UnmarshalManfestPath(manifestPath)
	if err != nil {
		return cColumnGroups, err
	}
	log.Info("GetManifest", zap.String("manifestPath", manifestPath), zap.String("basePath", basePath), zap.Int64("version", version))

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return cColumnGroups, err
	}
	cBasePath := C.CString(basePath)
	defer C.free(unsafe.Pointer(cBasePath))

	var cVersion C.int64_t
	result := C.get_latest_column_groups(cBasePath, cProperties, &cColumnGroups, &cVersion)
	err = HandleFFIResult(result)
	if err != nil {
		return cColumnGroups, err
	}

	return cColumnGroups, nil
}

// Ensure FFIPackedReader implements array.RecordReader interface
// var _ array.RecordReader = (*FFIPackedReader)(nil)
