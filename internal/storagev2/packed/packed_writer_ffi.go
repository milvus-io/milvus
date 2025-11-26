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

	"github.com/milvus-io/milvus/internal/storagecommon"
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
	cPattern := C.CString(pattern)
	defer C.free(unsafe.Pointer(cPattern))

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

	var writerHandler C.LoonWriterHandler

	var status C.CStatus
	status = C.NewPackedLoonWriter(cBasePath, cSchema, cStorageConfig, cPattern, &writerHandler)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}

	return &FFIPackedWriter{
		basePath:      basePath,
		loonWriter:    writerHandler,
		storageConfig: storageConfig,
	}, nil
}

func (pw *FFIPackedWriter) WriteRecordBatch(recordBatch arrow.Record) error {
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

	status := C.PackedLoonWrite(pw.loonWriter, &cArrays[0], &cSchemas[0], cSchema)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return err
	}

	return nil
}

func (pw *FFIPackedWriter) Close() (string, error) {
	var outVersion C.int64_t
	cBasePath := C.CString(pw.basePath)
	defer C.free(unsafe.Pointer(cBasePath))

	storageConfig := pw.storageConfig
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

	status := C.ClosePackedLoonWriter(pw.loonWriter, cBasePath, cStorageConfig, &outVersion)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return "", err
	}

	return MarshalManifestPath(pw.basePath, int64(outVersion)), nil
}
