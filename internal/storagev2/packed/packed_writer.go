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
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

func NewPackedWriter(filePaths []string, schema *arrow.Schema, bufferSize int64, multiPartUploadSize int64, columnGroups []storagecommon.ColumnGroup, storageConfig *indexpb.StorageConfig, storagePluginContext *indexcgopb.StoragePluginContext) (*PackedWriter, error) {
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
	for _, group := range columnGroups {
		cGroup := C.malloc(C.size_t(len(group.Columns)) * C.size_t(unsafe.Sizeof(C.int(0))))
		if cGroup == nil {
			return nil, errors.New("failed to allocate memory for column groups")
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
		status = C.NewPackedWriterWithStorageConfig(cSchema, cBufferSize, cFilePathsArray, cNumPaths, cMultiPartUploadSize, cColumnGroups, cStorageConfig, &cPackedWriter, pluginContextPtr)
	} else {
		status = C.NewPackedWriter(cSchema, cBufferSize, cFilePathsArray, cNumPaths, cMultiPartUploadSize, cColumnGroups, &cPackedWriter, pluginContextPtr)
	}
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}
	return &PackedWriter{cPackedWriter: cPackedWriter}, nil
}

func (pw *PackedWriter) WriteRecordBatch(recordBatch arrow.Record) error {
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
	status := C.CloseWriter(pw.cPackedWriter)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return err
	}
	return nil
}
