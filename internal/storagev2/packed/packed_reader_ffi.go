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
#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
*/
import "C"

import (
	"fmt"
	"io"
	"strconv"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/cdata"

	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

// Property keys - matching milvus-storage/properties.h
const (
	PROPERTY_FS_ADDRESS                = "fs.address"
	PROPERTY_FS_BUCKET_NAME            = "fs.bucket_name"
	PROPERTY_FS_ACCESS_KEY_ID          = "fs.access_key_id"
	PROPERTY_FS_ACCESS_KEY_VALUE       = "fs.access_key_value"
	PROPERTY_FS_ROOT_PATH              = "fs.root_path"
	PROPERTY_FS_STORAGE_TYPE           = "fs.storage_type"
	PROPERTY_FS_CLOUD_PROVIDER         = "fs.cloud_provider"
	PROPERTY_FS_IAM_ENDPOINT           = "fs.iam_endpoint"
	PROPERTY_FS_LOG_LEVEL              = "fs.log_level"
	PROPERTY_FS_REGION                 = "fs.region"
	PROPERTY_FS_USE_SSL                = "fs.use_ssl"
	PROPERTY_FS_SSL_CA_CERT            = "fs.ssl_ca_cert"
	PROPERTY_FS_USE_IAM                = "fs.use_iam"
	PROPERTY_FS_USE_VIRTUAL_HOST       = "fs.use_virtual_host"
	PROPERTY_FS_REQUEST_TIMEOUT_MS     = "fs.request_timeout_ms"
	PROPERTY_FS_GCP_CREDENTIAL_JSON    = "fs.gcp_credential_json"
	PROPERTY_FS_USE_CUSTOM_PART_UPLOAD = "fs.use_custom_part_upload"
)

// MakePropertiesFromStorageConfig creates a Properties object from StorageConfig
// This function converts a StorageConfig structure into a Properties object by
// calling the FFI properties_create function. All configuration fields from
// StorageConfig are mapped to corresponding key-value pairs in Properties.
func MakePropertiesFromStorageConfig(storageConfig *indexpb.StorageConfig) (*C.Properties, error) {
	if storageConfig == nil {
		return nil, fmt.Errorf("storageConfig is required")
	}

	// Prepare key-value pairs from StorageConfig
	var keys []string
	var values []string

	// Add non-empty string fields
	if storageConfig.GetAddress() != "" {
		keys = append(keys, PROPERTY_FS_ADDRESS)
		values = append(values, storageConfig.GetAddress())
	}
	if storageConfig.GetBucketName() != "" {
		keys = append(keys, PROPERTY_FS_BUCKET_NAME)
		values = append(values, storageConfig.GetBucketName())
	}
	if storageConfig.GetAccessKeyID() != "" {
		keys = append(keys, PROPERTY_FS_ACCESS_KEY_ID)
		values = append(values, storageConfig.GetAccessKeyID())
	}
	if storageConfig.GetSecretAccessKey() != "" {
		keys = append(keys, PROPERTY_FS_ACCESS_KEY_VALUE)
		values = append(values, storageConfig.GetSecretAccessKey())
	}
	if storageConfig.GetRootPath() != "" {
		keys = append(keys, PROPERTY_FS_ROOT_PATH)
		values = append(values, storageConfig.GetRootPath())
	}
	if storageConfig.GetStorageType() != "" {
		keys = append(keys, PROPERTY_FS_STORAGE_TYPE)
		values = append(values, storageConfig.GetStorageType())
	}
	if storageConfig.GetCloudProvider() != "" {
		keys = append(keys, PROPERTY_FS_CLOUD_PROVIDER)
		values = append(values, storageConfig.GetCloudProvider())
	}
	if storageConfig.GetIAMEndpoint() != "" {
		keys = append(keys, PROPERTY_FS_IAM_ENDPOINT)
		values = append(values, storageConfig.GetIAMEndpoint())
	}
	// Always add log level if any string field is set (matching C++ behavior)
	keys = append(keys, PROPERTY_FS_LOG_LEVEL)
	values = append(values, "Warn")

	if storageConfig.GetRegion() != "" {
		keys = append(keys, PROPERTY_FS_REGION)
		values = append(values, storageConfig.GetRegion())
	}
	if storageConfig.GetSslCACert() != "" {
		keys = append(keys, PROPERTY_FS_SSL_CA_CERT)
		values = append(values, storageConfig.GetSslCACert())
	}
	if storageConfig.GetGcpCredentialJSON() != "" {
		keys = append(keys, PROPERTY_FS_GCP_CREDENTIAL_JSON)
		values = append(values, storageConfig.GetGcpCredentialJSON())
	}

	// Add boolean fields
	keys = append(keys, PROPERTY_FS_USE_SSL)
	if storageConfig.GetUseSSL() {
		values = append(values, "true")
	} else {
		values = append(values, "false")
	}

	keys = append(keys, PROPERTY_FS_USE_IAM)
	if storageConfig.GetUseIAM() {
		values = append(values, "true")
	} else {
		values = append(values, "false")
	}

	keys = append(keys, PROPERTY_FS_USE_VIRTUAL_HOST)
	if storageConfig.GetUseVirtualHost() {
		values = append(values, "true")
	} else {
		values = append(values, "false")
	}

	keys = append(keys, PROPERTY_FS_USE_CUSTOM_PART_UPLOAD)
	values = append(values, "true") // hardcoded to true as in the original code

	// Add integer field
	keys = append(keys, PROPERTY_FS_REQUEST_TIMEOUT_MS)
	values = append(values, strconv.FormatInt(storageConfig.GetRequestTimeoutMs(), 10))

	// Convert to C arrays
	cKeys := make([]*C.char, len(keys))
	cValues := make([]*C.char, len(values))
	for i := range keys {
		cKeys[i] = C.CString(keys[i])
		cValues[i] = C.CString(values[i])
	}
	// Defer cleanup of all C strings
	defer func() {
		for i := range cKeys {
			C.free(unsafe.Pointer(cKeys[i]))
			C.free(unsafe.Pointer(cValues[i]))
		}
	}()

	// Create Properties using FFI
	properties := &C.Properties{}
	var cKeysPtr **C.char
	var cValuesPtr **C.char
	if len(cKeys) > 0 {
		cKeysPtr = &cKeys[0]
		cValuesPtr = &cValues[0]
	}

	result := C.properties_create(
		(**C.char)(unsafe.Pointer(cKeysPtr)),
		(**C.char)(unsafe.Pointer(cValuesPtr)),
		C.size_t(len(keys)),
		properties,
	)

	err := HandleFFIResult(result)
	if err != nil {
		return nil, err
	}
	return properties, nil
}

func HandleFFIResult(ffiResult C.FFIResult) error {
	defer C.FreeFFIResult(&ffiResult)
	if C.IsSuccess(&ffiResult) == 0 {
		errMsg := C.GetErrorMessage(&ffiResult)
		errStr := "Unknown error"
		if errMsg != nil {
			errStr = C.GoString(errMsg)
		}

		return fmt.Errorf("failed to create properties: %s", errStr)
	}
	return nil
}

func NewFFIPackedReader(manifest string, schema *arrow.Schema, neededColumns []string, bufferSize int64, storageConfig *indexpb.StorageConfig, storagePluginContext *indexcgopb.StoragePluginContext) (reader *FFIPackedReader, err error) {
	if storageConfig == nil {
		return nil, fmt.Errorf("storageConfig is required")
	}

	cManifest := C.CString(manifest)
	defer C.free(unsafe.Pointer(cManifest))

	var cas cdata.CArrowSchema
	cdata.ExportArrowSchema(schema, &cas)
	cSchema := (*C.struct_ArrowSchema)(unsafe.Pointer(&cas))
	defer cdata.ReleaseCArrowSchema(&cas)

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig)
	if err != nil {
		return nil, err
	}
	defer C.properties_free(cProperties)

	cNeededColumn := make([]*C.char, len(neededColumns))
	for i, columnName := range neededColumns {
		cNeededColumn[i] = C.CString(columnName)
		defer C.free(unsafe.Pointer(cNeededColumn[i]))
	}
	cNeededColumnArray := (**C.char)(unsafe.Pointer(&cNeededColumn[0]))
	cNumColumns := C.size_t(len(neededColumns))

	var readerHandler C.ReaderHandle

	result := C.reader_new(cManifest, cSchema, cNeededColumnArray, cNumColumns, cProperties, &readerHandler)

	err = HandleFFIResult(result)
	if err != nil {
		return nil, err
	}

	// handle initialization cleanup
	defer func() {
		if err != nil {
			C.reader_destroy(readerHandler)
		}
	}()

	// Get the ArrowArrayStream
	var cStream cdata.CArrowArrayStream

	result = C.get_record_batch_reader(readerHandler, nil, (*C.struct_ArrowArrayStream)(unsafe.Pointer(&cStream)))
	err = HandleFFIResult(result)
	if err != nil {
		return nil, err
	}

	// Import the stream as a RecordReader
	recordReader, err := cdata.ImportCRecordReader(&cStream, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to import record reader: %w", err)
	}

	return &FFIPackedReader{
		ffiReaderHandle: readerHandler,
		recordReader:    recordReader,
		schema:          schema,
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

	if r.ffiReaderHandle != 0 {
		C.reader_destroy(r.ffiReaderHandle)
	}

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

// Ensure FFIPackedReader implements array.RecordReader interface
// var _ array.RecordReader = (*FFIPackedReader)(nil)
