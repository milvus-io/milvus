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
#include "segcore/packed_writer_c.h"
#include "segcore/column_groups_c.h"
#include "storage/loon_ffi/ffi_writer_c.h"
#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
*/
import "C"

import (
	"strings"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/cdata"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func createStorageConfig() *indexpb.StorageConfig {
	var storageConfig *indexpb.StorageConfig

	if paramtable.Get().CommonCfg.StorageType.GetValue() == "local" {
		storageConfig = &indexpb.StorageConfig{
			RootPath:    paramtable.Get().LocalStorageCfg.Path.GetValue(),
			StorageType: paramtable.Get().CommonCfg.StorageType.GetValue(),
		}
	} else {
		storageConfig = &indexpb.StorageConfig{
			Address:           paramtable.Get().MinioCfg.Address.GetValue(),
			AccessKeyID:       paramtable.Get().MinioCfg.AccessKeyID.GetValue(),
			SecretAccessKey:   paramtable.Get().MinioCfg.SecretAccessKey.GetValue(),
			UseSSL:            paramtable.Get().MinioCfg.UseSSL.GetAsBool(),
			SslCACert:         paramtable.Get().MinioCfg.SslCACert.GetValue(),
			BucketName:        paramtable.Get().MinioCfg.BucketName.GetValue(),
			RootPath:          paramtable.Get().MinioCfg.RootPath.GetValue(),
			UseIAM:            paramtable.Get().MinioCfg.UseIAM.GetAsBool(),
			IAMEndpoint:       paramtable.Get().MinioCfg.IAMEndpoint.GetValue(),
			StorageType:       paramtable.Get().CommonCfg.StorageType.GetValue(),
			Region:            paramtable.Get().MinioCfg.Region.GetValue(),
			UseVirtualHost:    paramtable.Get().MinioCfg.UseVirtualHost.GetAsBool(),
			CloudProvider:     paramtable.Get().MinioCfg.CloudProvider.GetValue(),
			RequestTimeoutMs:  paramtable.Get().MinioCfg.RequestTimeoutMs.GetAsInt64(),
			GcpCredentialJSON: paramtable.Get().MinioCfg.GcpCredentialJSON.GetValue(),
		}
	}

	return storageConfig
}

func NewFFIPackedWriter(basePath string, schema *arrow.Schema, columnGroups []storagecommon.ColumnGroup, storageConfig *indexpb.StorageConfig, storagePluginContext *indexcgopb.StoragePluginContext) (*FFIPackedWriter, error) {
	cBasePath := C.CString(basePath)
	defer C.free(unsafe.Pointer(cBasePath))

	var cas cdata.CArrowSchema
	cdata.ExportArrowSchema(schema, &cas)
	cSchema := (*C.struct_ArrowSchema)(unsafe.Pointer(&cas))
	defer cdata.ReleaseCArrowSchema(&cas)

	if storageConfig == nil {
		storageConfig = createStorageConfig()
	}

	pattern := strings.Join(lo.Map(columnGroups, func(columnGroup storagecommon.ColumnGroup, _ int) string {
		return strings.Join(lo.Map(columnGroup.Columns, func(index int, _ int) string {
			return schema.Field(index).Name
		}), "|")
	}), ",")

	extra := map[string]string{
		PropertyWriterPolicy:             "schema_based",
		PropertyWriterSchemaBasedPattern: pattern,
	}

	// Configure CMEK encryption if plugin context is provided
	if storagePluginContext != nil {
		var cKey *C.char
		var cMeta *C.char

		encKey := C.CString(storagePluginContext.EncryptionKey)
		defer C.free(unsafe.Pointer(encKey))

		// Prepare plugin context for FFI call to retrieve encryption parameters
		var pluginContext C.CPluginContext
		pluginContext.ez_id = C.int64_t(storagePluginContext.EncryptionZoneId)
		pluginContext.collection_id = C.int64_t(storagePluginContext.CollectionId)
		pluginContext.key = encKey

		// Get encryption key and metadata from cipher plugin via FFI
		status := C.GetEncParams(&pluginContext, &cKey, &cMeta)
		if err := ConsumeCStatusIntoError(&status); err != nil {
			return nil, err
		}

		// Set encryption properties for the writer
		extra[PropertyWriterEncEnable] = "true"
		extra[PropertyWriterEncKey] = C.GoString(cKey)
		C.free(unsafe.Pointer(cKey))
		extra[PropertyWriterEncMeta] = C.GoString(cMeta)
		C.free(unsafe.Pointer(cMeta))
		extra[PropertyWriterEncAlgo] = "AES_GCM_V1"
	}

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, extra)
	if err != nil {
		return nil, err
	}

	var writerHandle C.WriterHandle

	result := C.writer_new(cBasePath, cSchema, cProperties, &writerHandle)

	err = HandleFFIResult(result)
	if err != nil {
		return nil, err
	}

	return &FFIPackedWriter{
		basePath:      basePath,
		cWriterHandle: writerHandle,
		cProperties:   cProperties,
	}, nil
}

func (pw *FFIPackedWriter) WriteRecordBatch(recordBatch arrow.Record) error {
	var caa cdata.CArrowArray
	var cas cdata.CArrowSchema

	// Export the record batch to C Arrow format
	cdata.ExportArrowRecordBatch(recordBatch, &caa, &cas)
	defer cdata.ReleaseCArrowArray(&caa)
	defer cdata.ReleaseCArrowSchema(&cas)

	// Convert to C struct
	cArray := (*C.struct_ArrowArray)(unsafe.Pointer(&caa))

	result := C.writer_write(pw.cWriterHandle, cArray)
	return HandleFFIResult(result)
}

func (pw *FFIPackedWriter) Close() (string, error) {
	var cColumnGroups C.ColumnGroupsHandle

	result := C.writer_close(pw.cWriterHandle, nil, nil, 0, &cColumnGroups)
	if err := HandleFFIResult(result); err != nil {
		return "", err
	}

	cBasePath := C.CString(pw.basePath)
	defer C.free(unsafe.Pointer(cBasePath))
	var transationHandle C.TransactionHandle

	// TODO pass version
	// use -1 as latest
	result = C.transaction_begin(cBasePath, pw.cProperties, &transationHandle, C.int64_t(-1))
	if err := HandleFFIResult(result); err != nil {
		return "", err
	}
	defer C.transaction_destroy(transationHandle)

	// #define LOON_TRANSACTION_UPDATE_ADDFILES 0
	// #define LOON_TRANSACTION_UPDATE_ADDFEILD 1
	// #define LOON_TRANSACTION_UPDATE_MAX 2

	// #define LOON_TRANSACTION_RESOLVE_FAIL 0
	// #define LOON_TRANSACTION_RESOLVE_MERGE 1
	// #define LOON_TRANSACTION_RESOLVE_MAX 2

	var commitResult C.TransactionCommitResult
	result = C.transaction_commit(transationHandle, C.int16_t(0), C.int16_t(0), cColumnGroups, &commitResult)
	if err := HandleFFIResult(result); err != nil {
		return "", err
	}

	log.Info("FFI writer closed", zap.Int64("version", int64(commitResult.committed_version)))

	defer C.properties_free(pw.cProperties)
	return MarshalManifestPath(pw.basePath, int64(commitResult.committed_version)), nil
}
