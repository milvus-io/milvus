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

// GetCachedFilesystemMetrics retrieves metrics from a cached filesystem.
// If key is empty, returns metrics from the singleton filesystem.
// If key is provided, looks up the filesystem in the cache by that key.
// The key format must match what's used by the C++ cache (from ArrowFileSystemConfig::GetCacheKey):
// - Local storage (type="local"): root_path
// - Object storage (type="minio", "s3", "aws", "gcp", etc.): address/bucket_name
func GetCachedFilesystemMetrics(key string) (*FilesystemMetrics, error) {
	var cFilesystem C.FileSystemHandle

	if key == "" {
		result := C.loon_get_filesystem_singleton_handle(&cFilesystem)
		if err := HandleLoonFFIResult(result); err != nil {
			return nil, fmt.Errorf("failed to get filesystem singleton: %w", err)
		}
	} else {
		cKey := C.CString(key)
		defer C.free(unsafe.Pointer(cKey))
		keyLen := C.uint32_t(len(key))

		// Create an empty properties struct - this is a cache lookup only
		var properties C.LoonProperties
		properties.properties = nil
		properties.count = 0

		result := C.loon_filesystem_get(&properties, cKey, keyLen, &cFilesystem)
		if err := HandleLoonFFIResult(result); err != nil {
			return nil, fmt.Errorf("failed to get cached filesystem for key %q: %w", key, err)
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

// GetFilesystemKeyFromStorageConfig extracts filesystem cache key from StorageConfig.
// Must match the key format used by C++ ArrowFileSystemConfig::GetCacheKey():
// - Local storage (type="local"): root_path
// - Object storage (type="minio", "s3", "aws", "gcp", etc.): address/bucket_name
func GetFilesystemKeyFromStorageConfig(storageConfig *indexpb.StorageConfig) string {
	if storageConfig == nil {
		return ""
	}

	storageType := storageConfig.GetStorageType()
	if storageType == "local" {
		return storageConfig.GetRootPath()
	}

	// Object storage (minio, s3, aws, gcp, etc.)
	address := storageConfig.GetAddress()
	bucketName := storageConfig.GetBucketName()
	if address == "" || bucketName == "" {
		return ""
	}
	return address + "/" + bucketName
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
func PublishFilesystemMetricsWithConfig(storageConfig *indexpb.StorageConfig) (*FilesystemMetrics, error) {
	key := GetFilesystemKeyFromStorageConfig(storageConfig)
	return PublishCachedFilesystemMetrics(key)
}

// PublishCachedFilesystemMetrics retrieves and publishes metrics from a cached filesystem.
func PublishCachedFilesystemMetrics(key string) (*FilesystemMetrics, error) {
	metricSnapshot, err := GetCachedFilesystemMetrics(key)
	if err != nil {
		log.Warn("failed to get cached filesystem metrics", zap.String("key", key), zap.Error(err))
		return nil, err
	}

	fsKey := key
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
