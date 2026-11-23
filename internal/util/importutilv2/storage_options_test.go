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

package importutilv2

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func externalStorageOptions() []*commonpb.KeyValuePair {
	return []*commonpb.KeyValuePair{
		{Key: StorageType, Value: "remote"},
		{Key: StorageEndpoint, Value: "s3.us-east-1.amazonaws.com"},
		{Key: StorageBucket, Value: "external-bucket"},
		{Key: StorageAccessKeyID, Value: "external-access-key"},
		{Key: StorageSecretAccessKey, Value: "external-secret-key"},
	}
}

func TestGetSourceStorageConfig(t *testing.T) {
	for _, options := range [][]*commonpb.KeyValuePair{nil, {{Key: "timeout", Value: "1h"}}} {
		config, err := GetSourceStorageConfig(options)
		require.NoError(t, err)
		require.Nil(t, config)
	}
	options := externalStorageOptions()
	before := fmt.Sprint(options)
	config, err := GetSourceStorageConfig(options)
	require.NoError(t, err)
	require.Equal(t, "external-bucket", config.Bucket)
	require.Equal(t, "s3.us-east-1.amazonaws.com", config.Endpoint)
	require.Equal(t, "external-access-key", config.AccessKeyID)
	require.Equal(t, "external-secret-key", config.SecretAccessKey)
	require.True(t, config.UseSSL)
	require.False(t, config.UseVirtualHost)
	require.Empty(t, config.Region)
	require.Equal(t, before, fmt.Sprint(options))

	options = append(options,
		&commonpb.KeyValuePair{Key: StorageUseSSL, Value: "false"},
		&commonpb.KeyValuePair{Key: StorageRegion, Value: "us-east-1"},
		&commonpb.KeyValuePair{Key: StorageUseVirtualHost, Value: "true"})
	options[1].Value = "minio.example.com:9000"
	config, err = GetSourceStorageConfig(options)
	require.NoError(t, err)
	require.False(t, config.UseSSL)
	require.True(t, config.UseVirtualHost)
	require.Equal(t, "us-east-1", config.Region)
	require.Equal(t, "minio.example.com:9000", config.Endpoint)

	// storage.type defaults to remote when other storage.* keys are supplied.
	config, err = GetSourceStorageConfig(externalStorageOptions()[1:])
	require.NoError(t, err)
	require.NotNil(t, config)
}

func TestGetSourceStorageConfigInvalid(t *testing.T) {
	tests := []struct{ name, key, value string }{
		{"local filesystem", StorageType, "local"},
		{"empty endpoint", StorageEndpoint, ""},
		{"malformed endpoint", StorageEndpoint, "://amazonaws.com"},
		{"scheme", StorageEndpoint, "https://s3.amazonaws.com"},
		{"path", StorageEndpoint, "s3.amazonaws.com/path"},
		{"userinfo", StorageEndpoint, "secret@minio.example.com"},
		{"query", StorageEndpoint, "minio.example.com?secret=value"},
		{"empty query", StorageEndpoint, "minio.example.com?"},
		{"fragment", StorageEndpoint, "minio.example.com#secret"},
		{"invalid port", StorageEndpoint, "minio.example.com:abc"},
		{"empty port", StorageEndpoint, "minio.example.com:"},
		{"zero port", StorageEndpoint, "minio.example.com:0"},
		{"large port", StorageEndpoint, "minio.example.com:65536"},
		{"wrong casing", "STORAGE.secretAccessKey", "SECRET_SENTINEL"},
		{"empty bucket", StorageBucket, ""},
		{"bucket path", StorageBucket, "bucket/path"},
		{"empty access key", StorageAccessKeyID, ""},
		{"empty secret key", StorageSecretAccessKey, ""},
		{"blank secret key", StorageSecretAccessKey, " "},
		{"invalid SSL", StorageUseSSL, "SECRET_SENTINEL"},
		{"invalid virtual host", StorageUseVirtualHost, "SECRET_SENTINEL"},
		{"unknown option", "storage.secretAccesKey", "SECRET_SENTINEL"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := externalStorageOptions()
			found := false
			for _, option := range options {
				if option.Key == test.key {
					option.Value = test.value
					found = true
				}
			}
			if !found {
				options = append(options, &commonpb.KeyValuePair{Key: test.key, Value: test.value})
			}
			config, err := GetSourceStorageConfig(options)
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.Nil(t, config)
			require.NotContains(t, err.Error(), "SECRET_SENTINEL")
			require.NotContains(t, err.Error(), "external-secret-key")
		})
	}
	for _, key := range []string{StorageEndpoint, StorageBucket, StorageAccessKeyID, StorageSecretAccessKey} {
		t.Run("missing "+key, func(t *testing.T) {
			var options []*commonpb.KeyValuePair
			for _, option := range externalStorageOptions() {
				if option.Key != key {
					options = append(options, option)
				}
			}
			_, err := GetSourceStorageConfig(options)
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
		})
	}
	options := append(externalStorageOptions(), &commonpb.KeyValuePair{Key: StorageSecretAccessKey, Value: "SECOND_SECRET"})
	_, err := GetSourceStorageConfig(options)
	require.ErrorContains(t, err, "duplicate")
	require.NotContains(t, err.Error(), "SECOND_SECRET")
}

func TestValidateSourceStorageFilePaths(t *testing.T) {
	for _, key := range []string{
		"data.json",
		"a/../data.json",
		"a//data.json",
		"./pk.npy",
		"insert_log/data.parquet",
		"a/b:c.json",
		"v1:2026/data.parquet",
		"urn:dataset:1/rows.json",
	} {
		require.NoError(t, ValidateSourceStorageFilePaths([]string{key}), key)
	}
	for _, tc := range []struct {
		key     string
		wantMsg string
	}{
		{"", "Object name cannot be empty"},
		{" ", "Object name cannot be empty"},
		{"/data.json", "relative object keys"},
		{"s3://bucket/data.json", "relative object keys"},
		{"https://host/data.json", "relative object keys"},
		{strings.Repeat("a", 1025), "longer than 1024"},
		{string([]byte{0xff, 0xfe}), "non UTF-8"},
	} {
		err := ValidateSourceStorageFilePaths([]string{"valid.json", tc.key})
		require.ErrorIs(t, err, merr.ErrParameterInvalid, tc.key)
		require.ErrorContains(t, err, tc.wantMsg, tc.key)
	}
}

func TestRemoveSourceStorageCredentials(t *testing.T) {
	options := append(externalStorageOptions(),
		&commonpb.KeyValuePair{Key: StorageUseSSL, Value: "false"},
		&commonpb.KeyValuePair{Key: StorageRegion, Value: "us-east-1"},
		&commonpb.KeyValuePair{Key: StorageUseVirtualHost, Value: "true"},
		&commonpb.KeyValuePair{Key: "STORAGE.futurePassword", Value: "FUTURE_SECRET"},
		&commonpb.KeyValuePair{Key: ExternalSpec, Value: `{"extfs":{"access_key_value":"SNAP_SECRET"}}`},
		&commonpb.KeyValuePair{Key: SnapshotSourceURI, Value: "s3://snap/meta"},
		&commonpb.KeyValuePair{Key: "timeout", Value: "1h"},
		nil,
	)

	cleaned := RemoveSourceStorageCredentials(options)
	rendered := fmt.Sprint(cleaned)
	require.NotContains(t, rendered, "external-access-key")
	require.NotContains(t, rendered, "external-secret-key")
	require.NotContains(t, rendered, "FUTURE_SECRET")
	require.NotContains(t, rendered, "SNAP_SECRET")
	require.NotContains(t, rendered, "s3://snap/meta")
	require.Contains(t, rendered, "s3.us-east-1.amazonaws.com")
	require.Contains(t, rendered, "external-bucket")
	require.Contains(t, rendered, "timeout")

	// Retaining non-secret structural storage.* keys preserves external-source
	// identity so terminal jobs can never silently fall back to cluster storage.
	require.True(t, HasSourceStorageOptions(cleaned))
	config, err := GetSourceStorageConfig(cleaned)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Nil(t, config)
}
