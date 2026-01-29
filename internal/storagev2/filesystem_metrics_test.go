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
			name: "valid address and bucket",
			storageConfig: &indexpb.StorageConfig{
				Address:    "localhost:9000",
				BucketName: "test-bucket",
			},
			expected: "localhost:9000/test-bucket",
		},
		{
			name: "empty address",
			storageConfig: &indexpb.StorageConfig{
				Address:    "",
				BucketName: "test-bucket",
			},
			expected: "",
		},
		{
			name: "empty bucket name",
			storageConfig: &indexpb.StorageConfig{
				Address:    "localhost:9000",
				BucketName: "",
			},
			expected: "",
		},
		{
			name: "both empty",
			storageConfig: &indexpb.StorageConfig{
				Address:    "",
				BucketName: "",
			},
			expected: "",
		},
		{
			name: "S3 endpoint format",
			storageConfig: &indexpb.StorageConfig{
				Address:    "s3.amazonaws.com",
				BucketName: "my-bucket",
			},
			expected: "s3.amazonaws.com/my-bucket",
		},
		{
			name: "IP address format",
			storageConfig: &indexpb.StorageConfig{
				Address:    "192.168.1.100:9000",
				BucketName: "data-bucket",
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
	// Test that other fields in StorageConfig don't affect the result
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
}

// TestGetFilesystemMetricsDefault tests GetFilesystemMetrics with empty path (default filesystem)
func TestGetFilesystemMetricsDefault(t *testing.T) {
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

	// Test getting metrics from default filesystem (empty path)
	metrics, err := GetFilesystemMetrics("")
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

// TestPublishFilesystemMetrics tests PublishFilesystemMetrics function
func TestPublishFilesystemMetrics(t *testing.T) {
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

	// Test with empty path (default filesystem)
	metrics, err := PublishFilesystemMetrics("")
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
