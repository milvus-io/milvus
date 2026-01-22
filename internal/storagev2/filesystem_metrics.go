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
	"unsafe"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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

// GetFilesystemMetricsWithConfig retrieves metrics from a filesystem using storage config properties.
// This is the preferred method when you have a StorageConfig, as it will find the exact
// filesystem instance in the cache (with proper metrics tracking).
func GetFilesystemMetricsWithConfig(storageConfig *indexpb.StorageConfig) (*FilesystemMetrics, error) {
	if storageConfig == nil {
		return GetFilesystemMetrics("")
	}

	// Debug logging for cache key verification
	fsKey := GetFilesystemKeyFromStorageConfig(storageConfig)
	log.Info("GetFilesystemMetricsWithConfig: looking up filesystem",
		zap.String("fsKey", fsKey),
		zap.String("address", storageConfig.GetAddress()),
		zap.String("bucketName", storageConfig.GetBucketName()),
		zap.String("rootPath", storageConfig.GetRootPath()),
		zap.String("storageType", storageConfig.GetStorageType()))

	// Build properties from storage config
	keys := []string{
		C.GoString(C.loon_properties_fs_address),
		C.GoString(C.loon_properties_fs_bucket_name),
		C.GoString(C.loon_properties_fs_access_key_id),
		C.GoString(C.loon_properties_fs_access_key_value),
		C.GoString(C.loon_properties_fs_root_path),
		C.GoString(C.loon_properties_fs_storage_type),
		C.GoString(C.loon_properties_fs_cloud_provider),
		C.GoString(C.loon_properties_fs_iam_endpoint),
		C.GoString(C.loon_properties_fs_log_level),
		C.GoString(C.loon_properties_fs_region),
		C.GoString(C.loon_properties_fs_use_ssl),
		C.GoString(C.loon_properties_fs_ssl_ca_cert),
		C.GoString(C.loon_properties_fs_use_iam),
		C.GoString(C.loon_properties_fs_use_virtual_host),
		C.GoString(C.loon_properties_fs_request_timeout_ms),
		C.GoString(C.loon_properties_fs_gcp_credential_json),
		C.GoString(C.loon_properties_fs_use_custom_part_upload),
		C.GoString(C.loon_properties_fs_max_connections),
	}
	values := []string{
		storageConfig.GetAddress(),
		storageConfig.GetBucketName(),
		storageConfig.GetAccessKeyID(),
		storageConfig.GetSecretAccessKey(),
		storageConfig.GetRootPath(),
		storageConfig.GetStorageType(),
		storageConfig.GetCloudProvider(),
		storageConfig.GetIAMEndpoint(),
		"warn",
		storageConfig.GetRegion(),
		strconv.FormatBool(storageConfig.GetUseSSL()),
		storageConfig.GetSslCACert(),
		strconv.FormatBool(storageConfig.GetUseIAM()),
		strconv.FormatBool(storageConfig.GetUseVirtualHost()),
		strconv.FormatInt(storageConfig.GetRequestTimeoutMs(), 10),
		storageConfig.GetGcpCredentialJSON(),
		"true",
		strconv.FormatUint(uint64(storageConfig.GetMaxConnections()), 10),
	}

	// Convert to C arrays
	cKeys := make([]*C.char, len(keys))
	cValues := make([]*C.char, len(values))
	for i := range keys {
		cKeys[i] = C.CString(keys[i])
		cValues[i] = C.CString(values[i])
	}
	defer func() {
		for i := range cKeys {
			C.free(unsafe.Pointer(cKeys[i]))
			C.free(unsafe.Pointer(cValues[i]))
		}
	}()

	// Create LoonProperties
	var properties C.LoonProperties
	result := C.loon_properties_create(&cKeys[0], &cValues[0], C.size_t(len(keys)), &properties)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, fmt.Errorf("failed to create properties: %w", err)
	}
	defer C.loon_properties_free(&properties)

	// Get filesystem using properties
	var cFilesystem C.FileSystemHandle
	result = C.loon_filesystem_get(&properties, nil, 0, &cFilesystem)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, fmt.Errorf("failed to get filesystem: %w", err)
	}

	return getMetricsFromHandle(cFilesystem)
}

// GetFilesystemMetrics retrieves metrics from a filesystem
// If path is empty, returns metrics from the singleton filesystem (initialized by InitLocalArrowFileSystem or InitRemoteArrowFileSystem)
// If path is provided (format: "address/bucketName" or root_path for local), returns metrics from that specific filesystem in the cache
// Note: When using StorageConfig, prefer GetFilesystemMetricsWithConfig for accurate cache lookup.
func GetFilesystemMetrics(path string) (*FilesystemMetrics, error) {
	var cFilesystem C.FileSystemHandle

	// If path is empty, try to use singleton filesystem first, otherwise get filesystem from cache by path
	if path == "" {
		result := C.loon_get_filesystem_singleton_handle(&cFilesystem)
		if err := HandleLoonFFIResult(result); err != nil {
			return nil, fmt.Errorf("failed to get filesystem singleton: %w", err)
		}
	} else {
		cPath := C.CString(path)
		defer C.free(unsafe.Pointer(cPath))
		pathLen := C.uint32_t(len(path))

		// Create an empty properties struct (required by loon_filesystem_get)
		var properties C.LoonProperties
		properties.properties = nil
		properties.count = 0

		result := C.loon_filesystem_get(&properties, cPath, pathLen, &cFilesystem)
		if err := HandleLoonFFIResult(result); err != nil {
			return nil, fmt.Errorf("failed to get filesystem for path %q: %w", path, err)
		}
	}

	return getMetricsFromHandle(cFilesystem)
}

// getMetricsFromHandle retrieves metrics from a filesystem handle
func getMetricsFromHandle(cFilesystem C.FileSystemHandle) (*FilesystemMetrics, error) {
	// Get metrics from the filesystem
	var cMetrics C.LoonFilesystemMetricsSnapshot
	metricsResult := C.loon_filesystem_get_metrics(cFilesystem, &cMetrics)
	if err := HandleLoonFFIResult(metricsResult); err != nil {
		// Clean up filesystem handle
		C.loon_filesystem_destroy(cFilesystem)
		return nil, fmt.Errorf("failed to get filesystem metrics: %w", err)
	}

	// Extract the 8 metrics from C struct
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

	// Clean up C resources
	C.loon_filesystem_destroy(cFilesystem)

	return fsMetrics, nil
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

// GetFilesystemKeyFromStorageConfig extracts filesystem key from StorageConfig
// Format: "address/bucketName" (e.g., "localhost:9000/a-bucket")
// Returns empty string if address/bucketName not available
func GetFilesystemKeyFromStorageConfig(storageConfig *indexpb.StorageConfig) string {
	if storageConfig == nil {
		return ""
	}

	address := storageConfig.GetAddress()
	bucketName := storageConfig.GetBucketName()

	if address == "" || bucketName == "" {
		return ""
	}

	return fmt.Sprintf("%s/%s", address, bucketName)
}

// PublishDefaultFilesystemMetrics retrieves and publishes metrics from the default filesystem.
// It builds a StorageConfig using the same logic as compaction.CreateStorageConfig() to ensure
// the cache key matches the filesystem used by compaction and other components.
func PublishDefaultFilesystemMetrics() (*FilesystemMetrics, error) {
	// Build storage config matching compaction.CreateStorageConfig() logic
	params := paramtable.Get()
	var storageConfig *indexpb.StorageConfig

	if params.CommonCfg.StorageType.GetValue() == "local" {
		// For local storage, use LocalStorageCfg.Path as RootPath (same as compaction.CreateStorageConfig)
		storageConfig = &indexpb.StorageConfig{
			RootPath:    params.LocalStorageCfg.Path.GetValue(),
			StorageType: params.CommonCfg.StorageType.GetValue(),
		}
	} else {
		// For remote storage, use full MinioCfg (same as compaction.CreateStorageConfig)
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
		}
	}
	return PublishFilesystemMetricsWithConfig(storageConfig)
}

// PublishFilesystemMetricsWithConfig retrieves and publishes filesystem metrics using storage config.
// This is the preferred method when you have a StorageConfig.
func PublishFilesystemMetricsWithConfig(storageConfig *indexpb.StorageConfig) (*FilesystemMetrics, error) {
	metricSnapshot, err := GetFilesystemMetricsWithConfig(storageConfig)
	if err != nil {
		log.Warn("failed to get filesystem metrics", zap.Error(err))
		return nil, err
	}
	fsKey := GetFilesystemKeyFromStorageConfig(storageConfig)
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

// PublishFilesystemMetrics retrieves default filesystem metrics and filesystem key
// If path is provided (format: "address/bucketName"), uses that filesystem; otherwise uses default
// Returns metrics, filesystem key, and error
// The caller should use the metrics package to publish these metrics with the fs key
func PublishFilesystemMetrics(path string) (*FilesystemMetrics, error) {
	metricSnapshot, err := GetFilesystemMetrics(path)
	if err != nil {
		log.Warn("failed to get filesystem metrics", zap.Error(err))
		return nil, err
	}

	fsKey := path
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
