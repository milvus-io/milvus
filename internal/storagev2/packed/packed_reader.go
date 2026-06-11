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
#include "milvus-storage/ffi_c.h"
#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"

CStatus NewPackedReaderWithProperties(char** paths,
                                      int64_t num_paths,
                                      struct ArrowSchema* schema,
                                      const int64_t buffer_size,
                                      const LoonProperties* c_properties,
                                      const char* filesystem_path,
                                      CPackedReader* c_packed_reader,
                                      CPluginContext* c_plugin_context);
*/
import "C"

import (
	"fmt"
	"io"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/cdata"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func NewPackedReader(filePaths []string, schema *arrow.Schema, bufferSize int64, storageConfig *indexpb.StorageConfig, storagePluginContext *indexcgopb.StoragePluginContext) (*PackedReader, error) {
	return NewPackedReaderWithExtfs(filePaths, schema, bufferSize, storageConfig, storagePluginContext, ExternalReaderContext{})
}

// NewPackedReaderWithExtfs opens packed files and optionally resolves them
// through an external filesystem alias described by extfs.
func NewPackedReaderWithExtfs(
	filePaths []string,
	schema *arrow.Schema,
	bufferSize int64,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
	extfs ExternalReaderContext,
) (*PackedReader, error) {
	var cProperties *C.LoonProperties
	var cFilesystemPath *C.char
	if extfs.Source != "" {
		if storageConfig == nil {
			return nil, fmt.Errorf("storageConfig is required for external packed reader")
		}
		properties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create properties: %w", err)
		}
		cProperties = properties
		defer C.loon_properties_free(cProperties)
		if err := injectExternalSpecProperties(cProperties, extfs.CollectionID, extfs.Source, extfs.Spec); err != nil {
			return nil, fmt.Errorf("inject extfs: %w", err)
		}
		var filesystemPath string
		normalizedPaths := make([]string, 0, len(filePaths))
		for _, filePath := range filePaths {
			currentFilesystemPath, normalizedPath, err := normalizeExternalPathForFilesystem(filePath, cProperties, extfs)
			if err != nil {
				return nil, fmt.Errorf("normalize external packed file path %s: %w", filePath, err)
			}
			if filesystemPath == "" {
				filesystemPath = currentFilesystemPath
			} else if currentFilesystemPath != filesystemPath {
				return nil, fmt.Errorf("external packed reader requires paths from one filesystem, got %s and %s", filesystemPath, currentFilesystemPath)
			}
			normalizedPaths = append(normalizedPaths, normalizedPath)
		}
		filePaths = normalizedPaths
		cFilesystemPath = C.CString(filesystemPath)
		defer C.free(unsafe.Pointer(cFilesystemPath))
	}

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

	if cProperties != nil {
		status = C.NewPackedReaderWithProperties(cFilePathsArray, cNumPaths, cSchema, cBufferSize, cProperties, cFilesystemPath, &cPackedReader, pluginContextPtr)
	} else if storageConfig != nil {
		cStorageConfig := C.CStorageConfig{
			address:                C.CString(storageConfig.GetAddress()),
			bucket_name:            C.CString(storageConfig.GetBucketName()),
			access_key_id:          C.CString(storageConfig.GetAccessKeyID()),
			access_key_value:       C.CString(storageConfig.GetSecretAccessKey()),
			root_path:              C.CString(storageConfig.GetRootPath()),
			storage_type:           C.CString(storageConfig.GetStorageType()),
			cloud_provider:         C.CString(storageConfig.GetCloudProvider()),
			iam_endpoint:           C.CString(storageConfig.GetIAMEndpoint()),
			log_level:              C.CString("warn"),
			useSSL:                 C.bool(storageConfig.GetUseSSL()),
			sslCACert:              C.CString(storageConfig.GetSslCACert()),
			useIAM:                 C.bool(storageConfig.GetUseIAM()),
			region:                 C.CString(storageConfig.GetRegion()),
			useVirtualHost:         C.bool(storageConfig.GetUseVirtualHost()),
			requestTimeoutMs:       C.int64_t(storageConfig.GetRequestTimeoutMs()),
			gcp_credential_json:    C.CString(storageConfig.GetGcpCredentialJSON()),
			use_custom_part_upload: true,
			max_connections:        C.uint32_t(storageConfig.GetMaxConnections()),
			tls_min_version:        C.CString(tlsMinVersionForStorage(storageConfig.GetSslTlsMinVersion())),
			use_crc32c_checksum:    C.bool(storageConfig.GetUseCrc32CChecksum()),
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
		defer C.free(unsafe.Pointer(cStorageConfig.tls_min_version))

		status = C.NewPackedReaderWithStorageConfig(cFilePathsArray, cNumPaths, cSchema, cBufferSize, cStorageConfig, &cPackedReader, pluginContextPtr)
	} else {
		status = C.NewPackedReader(cFilePathsArray, cNumPaths, cSchema, cBufferSize, &cPackedReader, pluginContextPtr)
	}
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}
	return &PackedReader{cPackedReader: cPackedReader, schema: schema}, nil
}

func (pr *PackedReader) ReadNext() (arrow.Record, error) {
	// return EOF if reader is closed
	if pr.cPackedReader == nil {
		return nil, io.EOF
	}

	if pr.currentBatch != nil {
		pr.currentBatch.Release()
		pr.currentBatch = nil
	}
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
		return nil, merr.WrapErrStorage(err, "failed to convert ArrowArray to Record")
	}
	pr.currentBatch = recordBatch

	// Return the RecordBatch as an arrow.Record
	return recordBatch, nil
}

func (pr *PackedReader) Close() error {
	if pr.cPackedReader == nil {
		return nil
	}
	if pr.currentBatch != nil {
		pr.currentBatch.Release()
	}
	status := C.CloseReader(pr.cPackedReader)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return err
	}
	pr.cPackedReader = nil
	return nil
}
