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

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestGetFilesystemKeyFromStorageConfig(t *testing.T) {
	tests := []struct {
		name          string
		storageConfig *indexpb.StorageConfig
		expected      string
	}{
		{
			name:          "nil storage config",
			storageConfig: nil,
			expected:      "",
		},
		{
			name: "local storage with root path",
			storageConfig: &indexpb.StorageConfig{
				StorageType: "local",
				RootPath:    "/var/lib/milvus/data",
			},
			expected: "/var/lib/milvus/data",
		},
		{
			name: "local storage with empty root path",
			storageConfig: &indexpb.StorageConfig{
				StorageType: "local",
				RootPath:    "",
			},
			expected: "",
		},
		{
			name: "minio storage valid address and bucket",
			storageConfig: &indexpb.StorageConfig{
				StorageType: "minio",
				Address:     "localhost:9000",
				BucketName:  "test-bucket",
			},
			expected: "localhost:9000/test-bucket",
		},
		{
			name: "minio storage empty address",
			storageConfig: &indexpb.StorageConfig{
				StorageType: "minio",
				Address:     "",
				BucketName:  "test-bucket",
			},
			expected: "",
		},
		{
			name: "minio storage empty bucket name",
			storageConfig: &indexpb.StorageConfig{
				StorageType: "minio",
				Address:     "localhost:9000",
				BucketName:  "",
			},
			expected: "",
		},
		{
			name: "minio storage both empty",
			storageConfig: &indexpb.StorageConfig{
				StorageType: "minio",
				Address:     "",
				BucketName:  "",
			},
			expected: "",
		},
		{
			name: "S3 endpoint format",
			storageConfig: &indexpb.StorageConfig{
				StorageType: "s3",
				Address:     "s3.amazonaws.com",
				BucketName:  "my-bucket",
			},
			expected: "s3.amazonaws.com/my-bucket",
		},
		{
			name: "minio with IP address",
			storageConfig: &indexpb.StorageConfig{
				StorageType: "minio",
				Address:     "192.168.1.100:9000",
				BucketName:  "data-bucket",
			},
			expected: "192.168.1.100:9000/data-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetFilesystemKeyFromStorageConfig(tt.storageConfig)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetFilesystemKeyFromStorageConfigWithOtherFields(t *testing.T) {
	// Test that other fields in StorageConfig don't affect the result for object storage
	storageConfig := &indexpb.StorageConfig{
		Address:           "localhost:9000",
		BucketName:        "test-bucket",
		AccessKeyID:       "access-key",
		SecretAccessKey:   "secret-key",
		UseSSL:            true,
		RootPath:          "/root/path",
		UseIAM:            false,
		IAMEndpoint:       "iam-endpoint",
		StorageType:       "s3",
		UseVirtualHost:    true,
		Region:            "us-east-1",
		CloudProvider:     "aws",
		RequestTimeoutMs:  30000,
		SslCACert:         "cert",
		GcpCredentialJSON: "json",
		MaxConnections:    10,
	}

	result := GetFilesystemKeyFromStorageConfig(storageConfig)
	assert.Equal(t, "localhost:9000/test-bucket", result)

	// Test that other fields don't affect the result for local storage
	localConfig := &indexpb.StorageConfig{
		Address:           "localhost:9000", // Should be ignored for local
		BucketName:        "test-bucket",    // Should be ignored for local
		AccessKeyID:       "access-key",
		SecretAccessKey:   "secret-key",
		UseSSL:            true,
		RootPath:          "/var/lib/milvus/data",
		UseIAM:            false,
		IAMEndpoint:       "iam-endpoint",
		StorageType:       "local",
		UseVirtualHost:    true,
		Region:            "us-east-1",
		CloudProvider:     "aws",
		RequestTimeoutMs:  30000,
		SslCACert:         "cert",
		GcpCredentialJSON: "json",
		MaxConnections:    10,
	}

	result = GetFilesystemKeyFromStorageConfig(localConfig)
	assert.Equal(t, "/var/lib/milvus/data", result)
}

// TestGetCachedFilesystemMetricsDefault tests GetCachedFilesystemMetrics with empty key (default filesystem)
func TestGetCachedFilesystemMetricsDefault(t *testing.T) {
	// Set up local storage for testing
	dir := t.TempDir()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	pt.Save(pt.LocalStorageCfg.Path.Key, dir)

	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.LocalStorageCfg.Path.Key)
	})

	// Initialize local arrow filesystem
	err := initcore.InitLocalArrowFileSystem(dir)
	require.NoError(t, err, "Failed to initialize local arrow filesystem")

	// Test getting metrics from default filesystem (empty key)
	metrics, err := GetCachedFilesystemMetrics("")
	if err != nil {
		// If CGO libraries aren't available, skip the test
		t.Skipf("Skipping CGO test: %v", err)
		return
	}

	// Verify metrics struct is not nil
	require.NotNil(t, metrics, "Metrics should not be nil")

	// Verify all 8 metrics fields are present (values may be 0 or positive)
	assert.GreaterOrEqual(t, metrics.ReadCount, int64(0))
	assert.GreaterOrEqual(t, metrics.WriteCount, int64(0))
	assert.GreaterOrEqual(t, metrics.ReadBytes, int64(0))
	assert.GreaterOrEqual(t, metrics.WriteBytes, int64(0))
	assert.GreaterOrEqual(t, metrics.GetFileInfoCount, int64(0))
	assert.GreaterOrEqual(t, metrics.FailedCount, int64(0))
	assert.GreaterOrEqual(t, metrics.MultiPartUploadCreated, int64(0))
	assert.GreaterOrEqual(t, metrics.MultiPartUploadFinished, int64(0))
}

// TestPublishCachedFilesystemMetrics tests PublishCachedFilesystemMetrics function
func TestPublishCachedFilesystemMetrics(t *testing.T) {
	// Set up local storage for testing
	dir := t.TempDir()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	pt.Save(pt.LocalStorageCfg.Path.Key, dir)

	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.LocalStorageCfg.Path.Key)
	})

	// Initialize local arrow filesystem
	err := initcore.InitLocalArrowFileSystem(dir)
	require.NoError(t, err, "Failed to initialize local arrow filesystem")

	// Test with empty key (default filesystem)
	metrics, err := PublishCachedFilesystemMetrics("")
	if err != nil {
		// If CGO libraries aren't available, skip the test
		t.Skipf("Skipping CGO test: %v", err)
		return
	}

	// Verify metrics struct is not nil
	require.NotNil(t, metrics, "Metrics should not be nil")

	// Verify all 8 metrics fields are present
	assert.GreaterOrEqual(t, metrics.ReadCount, int64(0))
	assert.GreaterOrEqual(t, metrics.WriteCount, int64(0))
	assert.GreaterOrEqual(t, metrics.ReadBytes, int64(0))
	assert.GreaterOrEqual(t, metrics.WriteBytes, int64(0))
	assert.GreaterOrEqual(t, metrics.GetFileInfoCount, int64(0))
	assert.GreaterOrEqual(t, metrics.FailedCount, int64(0))
	assert.GreaterOrEqual(t, metrics.MultiPartUploadCreated, int64(0))
	assert.GreaterOrEqual(t, metrics.MultiPartUploadFinished, int64(0))
}

// TestPublishFilesystemMetricsWithConfig tests PublishFilesystemMetricsWithConfig function
func TestPublishFilesystemMetricsWithConfig(t *testing.T) {
	// Set up local storage for testing
	dir := t.TempDir()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	pt.Save(pt.LocalStorageCfg.Path.Key, dir)

	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.LocalStorageCfg.Path.Key)
	})

	// Initialize local arrow filesystem
	err := initcore.InitLocalArrowFileSystem(dir)
	require.NoError(t, err, "Failed to initialize local arrow filesystem")

	// Test with local storage config
	localConfig := &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    dir,
	}

	metrics, err := PublishFilesystemMetricsWithConfig(localConfig)
	assert.NoError(t, err)

	// Verify metrics struct is not nil
	require.NotNil(t, metrics, "Metrics should not be nil")

	// Verify all 8 metrics fields are present
	assert.GreaterOrEqual(t, metrics.ReadCount, int64(0))
	assert.GreaterOrEqual(t, metrics.WriteCount, int64(0))
	assert.GreaterOrEqual(t, metrics.ReadBytes, int64(0))
	assert.GreaterOrEqual(t, metrics.WriteBytes, int64(0))
	assert.GreaterOrEqual(t, metrics.GetFileInfoCount, int64(0))
	assert.GreaterOrEqual(t, metrics.FailedCount, int64(0))
	assert.GreaterOrEqual(t, metrics.MultiPartUploadCreated, int64(0))
	assert.GreaterOrEqual(t, metrics.MultiPartUploadFinished, int64(0))
}

// TestPublishFilesystemMetricsWithNilConfig tests PublishFilesystemMetricsWithConfig with nil config
func TestPublishFilesystemMetricsWithNilConfig(t *testing.T) {
	// Set up local storage for testing
	dir := t.TempDir()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	pt.Save(pt.LocalStorageCfg.Path.Key, dir)

	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.LocalStorageCfg.Path.Key)
	})

	// Initialize local arrow filesystem
	err := initcore.InitLocalArrowFileSystem(dir)
	require.NoError(t, err, "Failed to initialize local arrow filesystem")

	// Test with nil config - should use empty key (default filesystem)
	metrics, err := PublishFilesystemMetricsWithConfig(nil)
	assert.NoError(t, err)
	assert.NotNil(t, metrics, "Metrics should not be nil")
}

// TestPublishDefaultFilesystemMetrics tests PublishDefaultFilesystemMetrics function
func TestPublishDefaultFilesystemMetrics(t *testing.T) {
	// Set up local storage for testing
	dir := t.TempDir()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	pt.Save(pt.LocalStorageCfg.Path.Key, dir)

	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.LocalStorageCfg.Path.Key)
	})

	// Initialize local arrow filesystem
	err := initcore.InitLocalArrowFileSystem(dir)
	require.NoError(t, err, "Failed to initialize local arrow filesystem")

	// Test PublishDefaultFilesystemMetrics
	metrics, err := PublishDefaultFilesystemMetrics()
	assert.NoError(t, err)
	assert.NotNil(t, metrics, "Metrics should not be nil")
}

// TestGetCachedFilesystemMetricsWithInvalidKey tests error handling for non-existent cache key
func TestInvalidKey(t *testing.T) {
	// Set up local storage for testing
	dir := t.TempDir()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	pt.Save(pt.LocalStorageCfg.Path.Key, dir)

	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.LocalStorageCfg.Path.Key)
	})

	// Initialize local arrow filesystem
	err := initcore.InitLocalArrowFileSystem(dir)
	require.NoError(t, err, "Failed to initialize local arrow filesystem")

	// Test with a key that doesn't exist in cache
	// FIXME: FilesystemCache::get from milvus-storage will return default filesystem if the key is not in cache and path is schemeless.
	_, err = GetCachedFilesystemMetrics("s3://non-existent-key")
	assert.Error(t, err)

	_, err = PublishCachedFilesystemMetrics("s3://non-existent-key")
	assert.Error(t, err)
}

// TestGetFilesystemKeyFromStorageConfigEdgeCases tests edge cases for key generation
func TestGetFilesystemKeyFromStorageConfigEdgeCases(t *testing.T) {
	tests := []struct {
		name          string
		storageConfig *indexpb.StorageConfig
		expected      string
	}{
		{
			name: "empty storage type defaults to object storage logic",
			storageConfig: &indexpb.StorageConfig{
				StorageType: "",
				Address:     "localhost:9000",
				BucketName:  "bucket",
			},
			expected: "localhost:9000/bucket",
		},
		{
			name: "unknown storage type uses object storage logic",
			storageConfig: &indexpb.StorageConfig{
				StorageType: "unknown",
				Address:     "localhost:9000",
				BucketName:  "bucket",
			},
			expected: "localhost:9000/bucket",
		},
		{
			name: "azure storage type",
			storageConfig: &indexpb.StorageConfig{
				StorageType: "azure",
				Address:     "account.blob.core.windows.net",
				BucketName:  "container",
			},
			expected: "account.blob.core.windows.net/container",
		},
		{
			name: "gcp storage type",
			storageConfig: &indexpb.StorageConfig{
				StorageType: "gcp",
				Address:     "storage.googleapis.com",
				BucketName:  "my-gcp-bucket",
			},
			expected: "storage.googleapis.com/my-gcp-bucket",
		},
		{
			name: "address with special characters",
			storageConfig: &indexpb.StorageConfig{
				StorageType: "minio",
				Address:     "minio-service.namespace.svc.cluster.local:9000",
				BucketName:  "bucket-name-with-dashes",
			},
			expected: "minio-service.namespace.svc.cluster.local:9000/bucket-name-with-dashes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetFilesystemKeyFromStorageConfig(tt.storageConfig)
			assert.Equal(t, tt.expected, result)
		})
	}
}
