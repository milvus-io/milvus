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
	"fmt"
	"strconv"
	"strings"
	"unsafe"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// FilesystemMetrics holds the 8 filesystem metrics retrieved from the default filesystem
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

// getMetricsFromHandle retrieves metrics from a filesystem handle
func getMetricsFromHandle(cFilesystem C.FileSystemHandle) (*FilesystemMetrics, error) {
	var cMetrics C.LoonFilesystemMetricsSnapshot
	metricsResult := C.loon_filesystem_get_metrics(cFilesystem, &cMetrics)
	if err := HandleLoonFFIResult(metricsResult); err != nil {
		C.loon_filesystem_destroy(cFilesystem)
		return nil, fmt.Errorf("failed to get filesystem metrics: %w", err)
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

// Property keys matching milvus-storage/properties.h.
// Duplicated from packed.PropertyFS* because cgo types are package-scoped.
const (
	propAddress             = "fs.address"
	propBucketName          = "fs.bucket_name"
	propAccessKeyID         = "fs.access_key_id"
	propAccessKeyValue      = "fs.access_key_value"
	propRootPath            = "fs.root_path"
	propStorageType         = "fs.storage_type"
	propCloudProvider       = "fs.cloud_provider"
	propIAMEndpoint         = "fs.iam_endpoint"
	propLogLevel            = "fs.log_level"
	propRegion              = "fs.region"
	propSSLCACert           = "fs.ssl_ca_cert"
	propGCPCredentialJSON   = "fs.gcp_credential_json"
	propUseSSL              = "fs.use_ssl"
	propUseIAM              = "fs.use_iam"
	propUseVirtualHost      = "fs.use_virtual_host"
	propUseCustomPartUpload = "fs.use_custom_part_upload"
	propRequestTimeoutMS    = "fs.request_timeout_ms"
	propTLSMinVersion       = "fs.tls_min_version"
	propUseCRC32CChecksum   = "fs.use_crc32c_checksum"
)

// makePropertiesFromConfig builds C.LoonProperties from a StorageConfig.
// Mirrors packed.MakePropertiesFromStorageConfig (cgo types not shareable across packages).
func makePropertiesFromConfig(storageConfig *indexpb.StorageConfig) (C.LoonProperties, error) {
	var keys []string
	var values []string

	if addr := storageConfig.GetAddress(); addr != "" {
		if !storageConfig.GetUseSSL() && !strings.Contains(addr, "://") {
			addr = "http://" + addr
		}
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
		return C.LoonProperties{}, fmt.Errorf("failed to create properties: %w", err)
	}

	return props, nil
}

// GetFilesystemMetricsWithConfig retrieves metrics from a cached filesystem
// using full storage config properties for proper cache resolution.
func GetFilesystemMetricsWithConfig(storageConfig *indexpb.StorageConfig) (*FilesystemMetrics, error) {
	if storageConfig == nil {
		return nil, fmt.Errorf("storageConfig is required")
	}

	props, err := makePropertiesFromConfig(storageConfig)
	if err != nil {
		return nil, err
	}
	defer C.loon_properties_free(&props)

	var cFilesystem C.FileSystemHandle
	result := C.loon_filesystem_get(&props, nil, 0, &cFilesystem)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, fmt.Errorf("failed to get cached filesystem: %w", err)
	}

	return getMetricsFromHandle(cFilesystem)
}

// HandleLoonFFIResult handles the result from loon FFI calls
func HandleLoonFFIResult(ffiResult C.LoonFFIResult) error {
	defer C.loon_ffi_free_result(&ffiResult)
	if C.loon_ffi_is_success(&ffiResult) == 0 {
		errMsg := C.loon_ffi_get_errmsg(&ffiResult)
		errStr := "Unknown error"
		if errMsg != nil {
			errStr = C.GoString(errMsg)
		}
		return fmt.Errorf("loon FFI error: %s", errStr)
	}
	return nil
}

// GetFilesystemKeyFromStorageConfig extracts filesystem cache key from StorageConfig.
func GetFilesystemKeyFromStorageConfig(storageConfig *indexpb.StorageConfig) string {
	if storageConfig == nil {
		return ""
	}

	storageType := storageConfig.GetStorageType()
	if storageType == "local" {
		return storageConfig.GetRootPath()
	}

	address := storageConfig.GetAddress()
	bucketName := storageConfig.GetBucketName()
	if address == "" || bucketName == "" {
		return ""
	}
	return address + "/" + bucketName
}

// PublishDefaultFilesystemMetrics retrieves and publishes metrics from the default filesystem.
func PublishDefaultFilesystemMetrics() (*FilesystemMetrics, error) {
	params := paramtable.Get()
	var storageConfig *indexpb.StorageConfig

	if params.CommonCfg.StorageType.GetValue() == "local" {
		storageConfig = &indexpb.StorageConfig{
			RootPath:    params.LocalStorageCfg.Path.GetValue(),
			StorageType: params.CommonCfg.StorageType.GetValue(),
		}
	} else {
		storageConfig = &indexpb.StorageConfig{
			Address:           params.MinioCfg.Address.GetValue(),
			AccessKeyID:       params.MinioCfg.AccessKeyID.GetValue(),
			SecretAccessKey:   params.MinioCfg.SecretAccessKey.GetValue(),
			UseSSL:            params.MinioCfg.UseSSL.GetAsBool(),
			SslCACert:         params.MinioCfg.SslCACert.GetValue(),
			BucketName:        params.MinioCfg.BucketName.GetValue(),
			RootPath:          params.MinioCfg.RootPath.GetValue(),
			UseIAM:            params.MinioCfg.UseIAM.GetAsBool(),
			IAMEndpoint:       params.MinioCfg.IAMEndpoint.GetValue(),
			StorageType:       params.CommonCfg.StorageType.GetValue(),
			Region:            params.MinioCfg.Region.GetValue(),
			UseVirtualHost:    params.MinioCfg.UseVirtualHost.GetAsBool(),
			CloudProvider:     params.MinioCfg.CloudProvider.GetValue(),
			RequestTimeoutMs:  params.MinioCfg.RequestTimeoutMs.GetAsInt64(),
			GcpCredentialJSON: params.MinioCfg.GcpCredentialJSON.GetValue(),
			SslTlsMinVersion:  params.MinioCfg.SslTLSMinVersion.GetValue(),
			UseCrc32CChecksum: params.MinioCfg.UseCRC32C.GetAsBool(),
		}
	}
	return PublishFilesystemMetricsWithConfig(storageConfig)
}

// PublishFilesystemMetricsWithConfig retrieves and publishes filesystem metrics using storage config.
func PublishFilesystemMetricsWithConfig(storageConfig *indexpb.StorageConfig) (*FilesystemMetrics, error) {
	metricSnapshot, err := GetFilesystemMetricsWithConfig(storageConfig)
	if err != nil {
		log.Warn("failed to get filesystem metrics with config", zap.Error(err))
		return nil, err
	}

	fsKey := GetFilesystemKeyFromStorageConfig(storageConfig)
	if fsKey == "" {
		fsKey = "default"
	}
	metrics.PublishFilesystemMetrics(
		fsKey,
		metricSnapshot.ReadCount,
		metricSnapshot.WriteCount,
		metricSnapshot.ReadBytes,
		metricSnapshot.WriteBytes,
		metricSnapshot.GetFileInfoCount,
		metricSnapshot.FailedCount,
		metricSnapshot.MultiPartUploadCreated,
		metricSnapshot.MultiPartUploadFinished,
	)
	return metricSnapshot, nil
}
