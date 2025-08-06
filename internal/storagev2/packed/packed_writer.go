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
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/cdata"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

func NewPackedWriter(filePaths []string, schema *arrow.Schema, bufferSize int64, multiPartUploadSize int64, columnGroups []storagecommon.ColumnGroup, storageConfig *indexpb.StorageConfig) (*PackedWriter, error) {
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
		status = C.NewPackedWriterWithStorageConfig(cSchema, cBufferSize, cFilePathsArray, cNumPaths, cMultiPartUploadSize, cColumnGroups, cStorageConfig, &cPackedWriter)
	} else {
		status = C.NewPackedWriter(cSchema, cBufferSize, cFilePathsArray, cNumPaths, cMultiPartUploadSize, cColumnGroups, &cPackedWriter)
	}
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}
	return &PackedWriter{cPackedWriter: cPackedWriter, columnGroups: columnGroups, cColumnGroups: cColumnGroups}, nil
}

func (pw *PackedWriter) WriteRecordBatch(recordBatch arrow.Record) error {
	numColumnGroups := len(pw.columnGroups)

	// Create arrays to hold the C ArrowArray and ArrowSchema objects for sub batches
	cSubArrays := make([]CArrowArray, numColumnGroups)
	cSubSchemas := make([]CArrowSchema, numColumnGroups)

	// Create sub record batches based on column groups
	for i, columnGroup := range pw.columnGroups {
		// Collect arrays and fields for this column group
		var groupArrays []arrow.Array
		var groupFields []arrow.Field

		for _, colIdx := range columnGroup.Columns {
			groupArrays = append(groupArrays, recordBatch.Column(colIdx))
			groupFields = append(groupFields, recordBatch.Schema().Field(colIdx))
		}

		// Create a sub record batch for this column group
		subSchema := arrow.NewSchema(groupFields, nil)
		subRecordBatch := array.NewRecord(subSchema, groupArrays, recordBatch.NumRows())

		// Export to C ArrowArray and ArrowSchema
		var caa cdata.CArrowArray
		var cas cdata.CArrowSchema
		cdata.ExportArrowRecordBatch(subRecordBatch, &caa, &cas)

		// Copy the data to our arrays
		cSubArrays[i] = *(*CArrowArray)(unsafe.Pointer(&caa))
		cSubSchemas[i] = *(*CArrowSchema)(unsafe.Pointer(&cas))
	}

	// Export the original schema
	var originalCas cdata.CArrowSchema
	cdata.ExportArrowSchema(recordBatch.Schema(), &originalCas)
	originalCSchema := (*CArrowSchema)(unsafe.Pointer(&originalCas))

	// Call the C function with sub arrays, sub schemas, column groups, and original schema
	status := C.WriteRecordBatch(pw.cPackedWriter, &cSubArrays[0], &cSubSchemas[0], C.int64_t(numColumnGroups), pw.cColumnGroups, originalCSchema)
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
