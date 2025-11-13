// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
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

#include "common/type_c.h"
#include "common/protobuf_utils_c.h"
#include "segcore/segment_c.h"
#include "segcore/packed_writer_c.h"
#include "storage/storage_c.h"
*/
import "C"

import (
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func ConsumeCStatusIntoError(status *C.CStatus) error {
	if status == nil || status.error_code == 0 {
		return nil
	}
	errorCode := status.error_code
	errorMsg := C.GoString(status.error_msg)
	C.free(unsafe.Pointer(status.error_msg))
	return merr.SegcoreError(int32(errorCode), errorMsg)
}

func GetFileSize(path string, storageConfig *indexpb.StorageConfig) (int64, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	var fileSize int64
	if storageConfig == nil {
		status := C.GetFileSize(cPath, (*C.int64_t)(unsafe.Pointer(&fileSize)))
		return fileSize, ConsumeCStatusIntoError(&status)
	} else {
		cStorageConfig := GetCStorageConfig(storageConfig)
		defer DeleteCStorageConfig(cStorageConfig)
		status := C.GetFileSizeWithStorageConfig(cPath, (*C.int64_t)(unsafe.Pointer(&fileSize)), cStorageConfig)
		return fileSize, ConsumeCStatusIntoError(&status)
	}
}

func GetCStorageConfig(storageConfig *indexpb.StorageConfig) C.CStorageConfig {
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
	return cStorageConfig
}

func DeleteCStorageConfig(cStorageConfig C.CStorageConfig) {
	C.free(unsafe.Pointer(cStorageConfig.address))
	C.free(unsafe.Pointer(cStorageConfig.bucket_name))
	C.free(unsafe.Pointer(cStorageConfig.access_key_id))
	C.free(unsafe.Pointer(cStorageConfig.access_key_value))
	C.free(unsafe.Pointer(cStorageConfig.root_path))
	C.free(unsafe.Pointer(cStorageConfig.storage_type))
	C.free(unsafe.Pointer(cStorageConfig.cloud_provider))
	C.free(unsafe.Pointer(cStorageConfig.iam_endpoint))
	C.free(unsafe.Pointer(cStorageConfig.log_level))
	C.free(unsafe.Pointer(cStorageConfig.sslCACert))
	C.free(unsafe.Pointer(cStorageConfig.region))
	C.free(unsafe.Pointer(cStorageConfig.gcp_credential_json))
}
