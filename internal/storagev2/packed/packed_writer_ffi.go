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
	"fmt"
	"strings"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/cdata"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func CreateStorageConfig() *indexpb.StorageConfig {
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
			SslTlsMinVersion:  paramtable.Get().MinioCfg.SslTLSMinVersion.GetValue(),
			UseCrc32CChecksum: paramtable.Get().MinioCfg.UseCRC32C.GetAsBool(),
		}
	}

	return storageConfig
}

// NewFFIPackedWriter creates a writer that produces parquet files under
// basePath. The writer knows nothing about manifests or versions — its
// only job is to write data files. Close returns the resulting column
// groups, which the caller passes to packed.CommitManifestUpdates to
// register them with a manifest version.
func NewFFIPackedWriter(basePath string, schema *arrow.Schema, columnGroups []storagecommon.ColumnGroup, storageConfig *indexpb.StorageConfig, storagePluginContext *indexcgopb.StoragePluginContext) (*FFIPackedWriter, error) {
	cBasePath := C.CString(basePath)
	defer C.free(unsafe.Pointer(cBasePath))

	var cas cdata.CArrowSchema
	cdata.ExportArrowSchema(schema, &cas)
	cSchema := (*C.struct_ArrowSchema)(unsafe.Pointer(&cas))
	defer cdata.ReleaseCArrowSchema(&cas)

	if storageConfig == nil {
		storageConfig = CreateStorageConfig()
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

	var writerHandle C.LoonWriterHandle

	result := C.loon_writer_new(cBasePath, cSchema, cProperties, &writerHandle)

	err = HandleLoonFFIResult(result)
	if err != nil {
		return nil, err
	}

	return &FFIPackedWriter{
		basePath:      basePath,
		cWriterHandle: writerHandle,
		cProperties:   cProperties,
	}, nil
}

// AsNewColumnGroups marks this writer so that the column groups returned
// by Close should be staged via loon_transaction_add_column_group instead
// of loon_transaction_append_files when later passed to
// CommitManifestUpdates. Use true when adding columns that do not yet
// exist in the manifest (e.g. function-field backfill).
func (pw *FFIPackedWriter) AsNewColumnGroups() *FFIPackedWriter {
	pw.addNewColumnGroups = true
	return pw
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

	result := C.loon_writer_write(pw.cWriterHandle, cArray)
	return HandleLoonFFIResult(result)
}

// ColumnGroups is the data carrier returned by FFIPackedWriter.Close. It
// holds the column-groups payload produced by the C writer and owns C
// memory; the caller MUST call Destroy after passing the handle to
// CommitManifestUpdates (success or failure). Destroy is idempotent;
// a nil cColumnGroups indicates the handle has already been released.
type ColumnGroups struct {
	cColumnGroups      *C.LoonColumnGroups
	addNewColumnGroups bool
}

// Destroy releases C memory. Safe to call multiple times.
func (f *ColumnGroups) Destroy() {
	if f == nil || f.cColumnGroups == nil {
		return
	}
	C.loon_column_groups_destroy(f.cColumnGroups)
	f.cColumnGroups = nil
}

// Close closes the underlying loon writer and returns the column-groups
// payload. The writer never touches the manifest — the caller is
// responsible for passing the returned handle to CommitManifestUpdates
// and calling Destroy when done.
//
// After Close, the writer is exhausted; further Close or Write calls fail.
func (pw *FFIPackedWriter) Close() (*ColumnGroups, error) {
	if pw.closed {
		return nil, fmt.Errorf("FFIPackedWriter already closed")
	}
	var cColumnGroups *C.LoonColumnGroups
	result := C.loon_writer_close(pw.cWriterHandle, nil, nil, 0, &cColumnGroups)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, err
	}
	pw.closed = true
	// The writer's cProperties belong to the FFI writer; they are no
	// longer needed once the C writer is closed. Release here to keep
	// the writer's Destroy responsibilities minimal.
	if pw.cProperties != nil {
		C.loon_properties_free(pw.cProperties)
		pw.cProperties = nil
	}
	return &ColumnGroups{
		cColumnGroups:      cColumnGroups,
		addNewColumnGroups: pw.addNewColumnGroups,
	}, nil
}
