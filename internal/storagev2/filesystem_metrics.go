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

package storagev2

/*
#cgo pkg-config: milvus_core milvus-storage

#include <stdlib.h>
#include "milvus-storage/ffi_c.h"
#include "milvus-storage/ffi_filesystem_c.h"
#include "milvus-storage/ffi_filesystem_metrics_c.h"
*/
import "C"

import (
	"strconv"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// FilesystemMetrics holds a filesystem metrics snapshot.
type FilesystemMetrics struct {
	ReadCount               int64
	WriteCount              int64
	ReadBytes               int64
	WriteBytes              int64
	GetFileInfoCount        int64
	FailedCount             int64
	MultiPartUploadCreated  int64
	MultiPartUploadFinished int64
}

// FilesystemMetricsEntry identifies one cached filesystem and its metrics.
type FilesystemMetricsEntry struct {
	DisplayKey string
	FilesystemMetrics
}

// getMetricsFromHandle retrieves metrics from a filesystem handle
func getMetricsFromHandle(cFilesystem C.FileSystemHandle) (*FilesystemMetrics, error) {
	var cMetrics C.LoonFilesystemMetricsSnapshot
	metricsResult := C.loon_filesystem_get_metrics(cFilesystem, &cMetrics)
	if err := HandleLoonFFIResult(metricsResult); err != nil {
		C.loon_filesystem_destroy(cFilesystem)
		return nil, merr.Wrap(err, "failed to get filesystem metrics")
	}

	fsMetrics := &FilesystemMetrics{
		ReadCount:               int64(cMetrics.read_count),
		WriteCount:              int64(cMetrics.write_count),
		ReadBytes:               int64(cMetrics.read_bytes),
		WriteBytes:              int64(cMetrics.write_bytes),
		GetFileInfoCount:        int64(cMetrics.get_file_info_count),
		FailedCount:             int64(cMetrics.failed_count),
		MultiPartUploadCreated:  int64(cMetrics.multi_part_upload_created),
		MultiPartUploadFinished: int64(cMetrics.multi_part_upload_finished),
	}

	C.loon_filesystem_destroy(cFilesystem)
	return fsMetrics, nil
}

// ListFilesystemMetrics returns metrics for every filesystem currently held by the cache.
func ListFilesystemMetrics() ([]FilesystemMetricsEntry, error) {
	var cMetricsList C.LoonFilesystemMetricsList
	result := C.loon_filesystem_list_metrics(&cMetricsList)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, merr.Wrap(err, "failed to list filesystem metrics")
	}
	defer C.loon_filesystem_free_metrics_list(&cMetricsList)

	entries := unsafe.Slice(cMetricsList.entries, int(cMetricsList.count))
	metricsList := make([]FilesystemMetricsEntry, 0, len(entries))
	for _, entry := range entries {
		metricsList = append(metricsList, FilesystemMetricsEntry{
			DisplayKey: C.GoString(entry.display_key),
			FilesystemMetrics: FilesystemMetrics{
				ReadCount:               int64(entry.metrics.read_count),
				WriteCount:              int64(entry.metrics.write_count),
				ReadBytes:               int64(entry.metrics.read_bytes),
				WriteBytes:              int64(entry.metrics.write_bytes),
				GetFileInfoCount:        int64(entry.metrics.get_file_info_count),
				FailedCount:             int64(entry.metrics.failed_count),
				MultiPartUploadCreated:  int64(entry.metrics.multi_part_upload_created),
				MultiPartUploadFinished: int64(entry.metrics.multi_part_upload_finished),
			},
		})
	}
	return metricsList, nil
}

// Property keys exported by milvus-storage/ffi_c.h.
var (
	propAddress             = C.GoString(C.loon_properties_fs_address)
	propBucketName          = C.GoString(C.loon_properties_fs_bucket_name)
	propAccessKeyID         = C.GoString(C.loon_properties_fs_access_key_id)
	propAccessKeyValue      = C.GoString(C.loon_properties_fs_access_key_value)
	propRootPath            = C.GoString(C.loon_properties_fs_root_path)
	propStorageType         = C.GoString(C.loon_properties_fs_storage_type)
	propCloudProvider       = C.GoString(C.loon_properties_fs_cloud_provider)
	propIAMEndpoint         = C.GoString(C.loon_properties_fs_iam_endpoint)
	propLogLevel            = C.GoString(C.loon_properties_fs_log_level)
	propRegion              = C.GoString(C.loon_properties_fs_region)
	propSSLCACert           = C.GoString(C.loon_properties_fs_ssl_ca_cert)
	propGCPCredentialJSON   = C.GoString(C.loon_properties_fs_gcp_credential_json)
	propUseSSL              = C.GoString(C.loon_properties_fs_use_ssl)
	propUseIAM              = C.GoString(C.loon_properties_fs_use_iam)
	propUseVirtualHost      = C.GoString(C.loon_properties_fs_use_virtual_host)
	propUseCustomPartUpload = C.GoString(C.loon_properties_fs_use_custom_part_upload)
	propRequestTimeoutMS    = C.GoString(C.loon_properties_fs_request_timeout_ms)
	propMaxConnections      = C.GoString(C.loon_properties_fs_max_connections)
	propTLSMinVersion       = C.GoString(C.loon_properties_fs_tls_min_version)
	propUseCRC32CChecksum   = C.GoString(C.loon_properties_fs_use_crc32c_checksum)
)

// makePropertiesFromConfig builds C.LoonProperties from a StorageConfig.
// Mirrors packed.MakePropertiesFromStorageConfig (cgo types not shareable across packages).
func makePropertiesFromConfig(storageConfig *indexpb.StorageConfig) (C.LoonProperties, error) {
	var keys []string
	var values []string

	if addr := storageConfig.GetAddress(); addr != "" {
		keys = append(keys, propAddress)
		values = append(values, addr)
	}
	if v := storageConfig.GetBucketName(); v != "" {
		keys = append(keys, propBucketName)
		values = append(values, v)
	}
	if v := storageConfig.GetAccessKeyID(); v != "" {
		keys = append(keys, propAccessKeyID)
		values = append(values, v)
	}
	if v := storageConfig.GetSecretAccessKey(); v != "" {
		keys = append(keys, propAccessKeyValue)
		values = append(values, v)
	}
	if v := storageConfig.GetRootPath(); v != "" {
		keys = append(keys, propRootPath)
		values = append(values, v)
	}
	if v := storageConfig.GetStorageType(); v != "" {
		keys = append(keys, propStorageType)
		values = append(values, v)
	}
	if v := storageConfig.GetCloudProvider(); v != "" {
		keys = append(keys, propCloudProvider)
		values = append(values, v)
	}
	if v := storageConfig.GetIAMEndpoint(); v != "" {
		keys = append(keys, propIAMEndpoint)
		values = append(values, v)
	}
	keys = append(keys, propLogLevel)
	values = append(values, "warn")
	if v := storageConfig.GetRegion(); v != "" {
		keys = append(keys, propRegion)
		values = append(values, v)
	}
	if v := storageConfig.GetSslCACert(); v != "" {
		keys = append(keys, propSSLCACert)
		values = append(values, v)
	}
	if v := storageConfig.GetGcpCredentialJSON(); v != "" {
		keys = append(keys, propGCPCredentialJSON)
		values = append(values, v)
	}

	keys = append(keys, propUseSSL)
	values = append(values, strconv.FormatBool(storageConfig.GetUseSSL()))
	keys = append(keys, propUseIAM)
	values = append(values, strconv.FormatBool(storageConfig.GetUseIAM()))
	keys = append(keys, propUseVirtualHost)
	values = append(values, strconv.FormatBool(storageConfig.GetUseVirtualHost()))
	keys = append(keys, propUseCustomPartUpload)
	values = append(values, "true")

	keys = append(keys, propRequestTimeoutMS)
	values = append(values, strconv.FormatInt(storageConfig.GetRequestTimeoutMs(), 10))
	// Absent when unset, so milvus-storage's registered default applies. See
	// the same guard in packed.MakePropertiesFromStorageConfig for why an
	// explicit "0" is not equivalent.
	if maxConns := storageConfig.GetMaxConnections(); maxConns > 0 {
		keys = append(keys, propMaxConnections)
		values = append(values, strconv.FormatUint(uint64(maxConns), 10))
	}

	if v := storageConfig.GetSslTlsMinVersion(); v != "" && v != "default" {
		keys = append(keys, propTLSMinVersion)
		values = append(values, v)
	}
	keys = append(keys, propUseCRC32CChecksum)
	values = append(values, strconv.FormatBool(storageConfig.GetUseCrc32CChecksum()))

	count := len(keys)
	if count == 0 {
		return C.LoonProperties{}, nil
	}

	cKeys := make([]*C.char, count)
	cValues := make([]*C.char, count)
	for i := 0; i < count; i++ {
		cKeys[i] = C.CString(keys[i])
		cValues[i] = C.CString(values[i])
	}
	defer func() {
		for i := 0; i < count; i++ {
			C.free(unsafe.Pointer(cKeys[i]))
			C.free(unsafe.Pointer(cValues[i]))
		}
	}()

	var props C.LoonProperties
	result := C.loon_properties_create(
		(**C.char)(unsafe.Pointer(&cKeys[0])),
		(**C.char)(unsafe.Pointer(&cValues[0])),
		C.size_t(count),
		&props,
	)

	if err := HandleLoonFFIResult(result); err != nil {
		return C.LoonProperties{}, merr.Wrap(err, "failed to create properties")
	}

	return props, nil
}

// GetFilesystemMetricsWithConfig retrieves metrics from a cached filesystem
// using full storage config properties for proper cache resolution.
func GetFilesystemMetricsWithConfig(storageConfig *indexpb.StorageConfig) (*FilesystemMetrics, error) {
	if storageConfig == nil {
		return nil, merr.WrapErrStorageMsg("storageConfig is required")
	}

	props, err := makePropertiesFromConfig(storageConfig)
	if err != nil {
		return nil, err
	}
	defer C.loon_properties_free(&props)

	var cFilesystem C.FileSystemHandle
	result := C.loon_filesystem_get(&props, nil, 0, &cFilesystem)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, merr.Wrap(err, "failed to get cached filesystem")
	}

	return getMetricsFromHandle(cFilesystem)
}

// HandleLoonFFIResult handles the result from loon FFI calls
func HandleLoonFFIResult(ffiResult C.LoonFFIResult) error {
	defer C.loon_ffi_free_result(&ffiResult)
	if C.loon_ffi_is_success(&ffiResult) == 0 {
		errCode := int(ffiResult.err_code)
		errMsg := C.loon_ffi_get_errmsg(&ffiResult)
		errStr := "Unknown error"
		if errMsg != nil {
			errStr = C.GoString(errMsg)
		}
		return merr.WrapErrStorageMsg("loon FFI error (code %d): %s", errCode, errStr)
	}
	return nil
}
