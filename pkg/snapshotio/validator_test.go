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

package snapshotio

import (
	"context"
	"math"
	"net/url"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	objectstore "github.com/milvus-io/milvus/pkg/v3/objectstorage/store"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	snapshotio "github.com/milvus-io/milvus/pkg/v3/snapshotio/format"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func validateSnapshotForeignStorageForTest(
	direction Direction,
	foreignURI string,
	externalSpec string,
) (*objectstorage.Config, string, string, string, error) {
	bucket, root, scheme, endpoint, err := parseSnapshotForeignURI(direction, foreignURI)
	if err != nil {
		return nil, "", "", "", err
	}
	cfg := objectstorage.NewDefaultConfig()
	if _, _, err := applySnapshotExternalSpecToConfig(cfg, scheme, endpoint, externalSpec); err != nil {
		return nil, "", "", "", err
	}
	return cfg, bucket, root, scheme, nil
}

func TestValidateSnapshotForeignStorageRejectsUnsupportedAuthKeys(t *testing.T) {
	foreignURI := "s3://foreign-bucket/root/snapshots/100/metadata/1.json"
	tests := []struct {
		name    string
		spec    string
		wantErr string
	}{
		{
			name:    "role arn",
			spec:    `{"extfs":{"role_arn":"arn:aws:iam::1:role/r"}}`,
			wantErr: "not supported for snapshot",
		},
		{
			name:    "gcp target service account",
			spec:    `{"extfs":{"gcp_target_service_account":"sa@project.iam.gserviceaccount.com"}}`,
			wantErr: "not supported for snapshot",
		},
		{
			name:    "anonymous",
			spec:    `{"extfs":{"anonymous":"true"}}`,
			wantErr: "not supported for snapshot",
		},
		{
			name:    "sas token",
			spec:    `{"extfs":{"sas_token":"sig=secret"}}`,
			wantErr: "is not allowed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, _, err := validateSnapshotForeignStorageForTest(DirectionRestore, foreignURI, tt.spec)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestValidateSnapshotForeignStorageRestoreRawAKSKAllowed(t *testing.T) {
	cfg, _, _, _, err := validateSnapshotForeignStorageForTest(
		DirectionRestore,
		"s3://foreign-bucket/root/snapshots/100/metadata/1.json",
		`{"extfs":{"access_key_id":"AK","access_key_value":"SK","cloud_provider":"aws","region":"us-west-2"}}`,
	)
	require.NoError(t, err)
	assert.Equal(t, "AK", cfg.AccessKeyID)
	assert.Equal(t, "SK", cfg.SecretAccessKeyID)
}

func TestValidateSnapshotForeignStorageRestoreGCPServiceAccountJSONAllowed(t *testing.T) {
	credentialJSON := `{"type":"service_account","project_id":"snapshot-project"}`
	cfg, _, _, _, err := validateSnapshotForeignStorageForTest(
		DirectionRestore,
		"gs://foreign-bucket/root/snapshots/100/metadata/1.json",
		`{"extfs":{"cloud_provider":"gcpnative","credential_json":"{\"type\":\"service_account\",\"project_id\":\"snapshot-project\"}"}}`,
	)
	require.NoError(t, err)
	assert.Equal(t, credentialJSON, cfg.GcpCredentialJSON)
	assert.False(t, cfg.UseIAM)
}

func TestApplySnapshotExternalSpecUseIAMClearsInheritedCredentials(t *testing.T) {
	cfg := objectstorage.NewDefaultConfig()
	cfg.AccessKeyID = "instance-ak"
	cfg.SecretAccessKeyID = "instance-sk"
	cfg.GcpCredentialJSON = `{"type":"service_account"}`
	_, _, err := applySnapshotExternalSpecToConfig(
		cfg,
		"gs",
		"",
		`{"extfs":{"cloud_provider":"gcpnative","use_iam":"true"}}`,
	)
	require.NoError(t, err)
	assert.True(t, cfg.UseIAM)
	assert.Empty(t, cfg.AccessKeyID)
	assert.Empty(t, cfg.SecretAccessKeyID)
	assert.Empty(t, cfg.GcpCredentialJSON)
}

func TestApplySnapshotExternalSpecUseIAMPreservesAzureAccountName(t *testing.T) {
	cfg := objectstorage.NewDefaultConfig()
	cfg.CloudProvider = objectstorage.CloudProviderAzure
	cfg.AccessKeyID = "azure-account"
	cfg.SecretAccessKeyID = "instance-account-key"
	cfg.GcpCredentialJSON = `{"type":"service_account"}`
	_, _, err := applySnapshotExternalSpecToConfig(
		cfg,
		"azure",
		"",
		`{"extfs":{"cloud_provider":"azure","region":"public","use_iam":"true"}}`,
	)

	require.NoError(t, err)
	assert.True(t, cfg.UseIAM)
	assert.Equal(t, "azure-account", cfg.AccessKeyID)
	assert.Empty(t, cfg.SecretAccessKeyID)
	assert.Empty(t, cfg.GcpCredentialJSON)
	assert.False(t, cfg.IgnoreAzureConnectionString)
}

func TestApplySnapshotExternalSpecAzureRawCredentialsIgnoreConnectionString(t *testing.T) {
	cfg := objectstorage.NewDefaultConfig()
	cfg.CloudProvider = objectstorage.CloudProviderAzure
	cfg.AccessKeyID = "instance-account"
	cfg.SecretAccessKeyID = "instance-key"
	_, _, err := applySnapshotExternalSpecToConfig(
		cfg,
		"azure",
		"",
		`{"extfs":{"cloud_provider":"azure","region":"public","access_key_id":"request-account","access_key_value":"request-key"}}`,
	)

	require.NoError(t, err)
	assert.Equal(t, "request-account", cfg.AccessKeyID)
	assert.Equal(t, "request-key", cfg.SecretAccessKeyID)
	assert.True(t, cfg.IgnoreAzureConnectionString)
}

func TestApplySnapshotExternalSpecIgnoresRequestSSLCACert(t *testing.T) {
	instanceCfg := objectstorage.NewDefaultConfig()
	instanceCfg.CloudProvider = objectstorage.CloudProviderAWS
	instanceCfg.SslCACert = "instance-ca.pem"
	cfg := cloneObjectStorageConfig(instanceCfg)

	_, _, err := applySnapshotExternalSpecToConfig(
		cfg,
		"s3",
		"",
		`{"extfs":{"use_iam":"true","ssl_ca_cert":"request-ca.pem"}}`,
	)

	require.NoError(t, err)
	assert.Equal(t, "instance-ca.pem", cfg.SslCACert)
}

func TestValidateSnapshotForeignStorageRestoreRejectsEmptyRawCredentialFields(t *testing.T) {
	foreignURI := "s3://foreign-bucket/root/snapshots/100/metadata/1.json"
	_, _, _, _, err := validateSnapshotForeignStorageForTest(
		DirectionRestore,
		foreignURI,
		`{"extfs":{"access_key_id":"","access_key_value":"","cloud_provider":"aws","region":"us-west-2"}}`,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be set together and non-empty")
}

func TestValidateSnapshotForeignStorageRejectsUnmappableExtfsKeys(t *testing.T) {
	foreignURI := "s3://foreign-bucket/root/snapshots/100/metadata/1.json"
	tests := []struct {
		name string
		spec string
	}{
		{
			name: "session name",
			spec: `{"extfs":{"session_name":"snapshot-session"}}`,
		},
		{
			name: "external id",
			spec: `{"extfs":{"external_id":"confused-deputy-secret"}}`,
		},
		{
			name: "bucket name",
			spec: `{"extfs":{"bucket_name":"ignored-bucket"}}`,
		},
		{
			name: "load frequency",
			spec: `{"extfs":{"load_frequency":"60"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, _, err := validateSnapshotForeignStorageForTest(DirectionExport, foreignURI, tt.spec)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not supported for snapshot")
		})
	}
}

func TestValidateSnapshotForeignStorageRejectsSpecWithoutExtfs(t *testing.T) {
	foreignURI := "s3://foreign-bucket/root/snapshots/100/metadata/1.json"
	for _, spec := range []string{`{}`, `{"format":"parquet"}`, `{"extfs":{}}`} {
		t.Run(spec, func(t *testing.T) {
			_, _, _, _, err := validateSnapshotForeignStorageForTest(DirectionExport, foreignURI, spec)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "external_spec.extfs is required")
		})
	}
}

func TestValidateSnapshotForeignStorageRejectsPartialRawAKSK(t *testing.T) {
	_, _, _, _, err := validateSnapshotForeignStorageForTest(
		DirectionExport,
		"s3://foreign-bucket/root/snapshots/100/metadata/1.json",
		`{"extfs":{"access_key_id":"AK","cloud_provider":"aws","region":"us-west-2"}}`,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be set together and non-empty")
}

func TestValidateSnapshotForeignStorageRejectsUseIAMFalse(t *testing.T) {
	_, _, _, _, err := validateSnapshotForeignStorageForTest(
		DirectionExport,
		"s3://foreign-bucket/root/snapshots/100/metadata/1.json",
		`{"extfs":{"use_iam":"false","cloud_provider":"aws","region":"us-west-2"}}`,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "use_iam=false is not supported")
}

func TestValidateSnapshotForeignStorageRejectsCredentialModeConflicts(t *testing.T) {
	foreignURI := "s3://foreign-bucket/root/snapshots/100/metadata/1.json"
	tests := []struct {
		name string
		spec string
	}{
		{
			name: "use iam with raw",
			spec: `{"extfs":{"use_iam":"true","access_key_id":"AK","access_key_value":"SK","cloud_provider":"aws","region":"us-west-2"}}`,
		},
		{
			name: "use iam with gcp service account json",
			spec: `{"extfs":{"use_iam":"true","credential_json":"{}","cloud_provider":"gcpnative"}}`,
		},
		{
			name: "raw credentials with gcp service account json",
			spec: `{"extfs":{"access_key_id":"AK","access_key_value":"SK","credential_json":"{}","cloud_provider":"gcpnative"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, _, err := validateSnapshotForeignStorageForTest(DirectionExport, foreignURI, tt.spec)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "credential modes are mutually exclusive")
		})
	}
}

func TestValidateSnapshotForeignStorageRestoreWithExternalSpec(t *testing.T) {
	cfg, bucket, root, _, err := validateSnapshotForeignStorageForTest(
		DirectionRestore,
		"s3://foreign-bucket/tenant-a/snapshots/100/metadata/1.json",
		`{"extfs":{"cloud_provider":"aws","region":"us-west-2"}}`,
	)
	require.NoError(t, err)
	assert.Equal(t, "foreign-bucket", bucket)
	assert.Equal(t, "tenant-a", root)
	assert.Equal(t, "aws", cfg.CloudProvider)
	assert.Equal(t, "us-west-2", cfg.Region)
}

func TestValidateSnapshotForeignStorageExportUsesTargetRoot(t *testing.T) {
	_, bucket, root, _, err := validateSnapshotForeignStorageForTest(
		DirectionExport,
		"s3://foreign-bucket/export-root",
		`{"extfs":{"cloud_provider":"aws","region":"us-west-2"}}`,
	)
	require.NoError(t, err)
	assert.Equal(t, "foreign-bucket", bucket)
	assert.Equal(t, "export-root", root)
}

func TestValidateSnapshotForeignStorageCopySourceUsesSourceRoot(t *testing.T) {
	_, bucket, root, _, err := validateSnapshotForeignStorageForTest(
		DirectionCopySource,
		"s3://foreign-bucket/source-root/files",
		`{"extfs":{"cloud_provider":"aws","region":"us-west-2"}}`,
	)
	require.NoError(t, err)
	assert.Equal(t, "foreign-bucket", bucket)
	assert.Equal(t, "source-root/files", root)
}

func TestValidateSnapshotForeignStorageExportObjectKeyUsesInstanceBucketPlaceholder(t *testing.T) {
	_, bucket, root, scheme, err := validateSnapshotForeignStorageForTest(
		DirectionExport,
		"export-root",
		"",
	)
	require.NoError(t, err)
	assert.Empty(t, bucket)
	assert.Equal(t, "export-root", root)
	assert.Empty(t, scheme)
}

func TestValidateSnapshotForeignStorageRestoreRejectsObjectKey(t *testing.T) {
	for _, externalSpec := range []string{
		"",
		`{"extfs":{"cloud_provider":"aws","region":"us-west-2"}}`,
	} {
		_, _, _, _, err := validateSnapshotForeignStorageForTest(
			DirectionRestore,
			"export-root/snapshots/1/metadata/1.json",
			externalSpec,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "complete URI")
	}
}

func TestValidateSnapshotForeignStorageRestoreRejectsMissingSnapshotMetadataAnchor(t *testing.T) {
	_, _, _, _, err := validateSnapshotForeignStorageForTest(
		DirectionRestore,
		"s3://foreign-bucket/restored/x/meta.json",
		"",
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "snapshots/{collectionID}/metadata/{snapshotID}")
}

func TestDeriveForeignRootRestoreAtBucketRoot(t *testing.T) {
	root, err := DeriveForeignRoot(DirectionRestore, "snapshots/1/metadata/2.json")
	require.NoError(t, err)
	assert.Empty(t, root)
}

func TestParseForeignURIRejectsInvalidInputs(t *testing.T) {
	tests := []struct {
		name string
		uri  string
	}{
		{
			name: "userinfo",
			uri:  "s3://user:pass@bucket/root/object",
		},
		{
			name: "path traversal",
			uri:  "s3://bucket/root/../object",
		},
		{
			name: "missing object",
			uri:  "s3://bucket",
		},
		{
			name: "query parameters",
			uri:  "s3://bucket/root/object?X-Amz-Signature=secret",
		},
		{
			name: "fragment",
			uri:  "s3://bucket/root/object#credential",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, err := ParseForeignURI(tt.uri)
			require.Error(t, err)
		})
	}
}

func TestParseForeignURIPreservesSchemelessObjectKeyCharacters(t *testing.T) {
	objectKey := "root/a:b?query#fragment%2Fvalue"

	bucket, parsedKey, endpoint, err := ParseForeignURI(objectKey)

	require.NoError(t, err)
	assert.Empty(t, bucket)
	assert.Equal(t, objectKey, parsedKey)
	assert.Empty(t, endpoint)
}

func TestParseForeignURIRejectsTraversalInSchemelessObjectKey(t *testing.T) {
	_, _, _, err := ParseForeignURI("root/../secret")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "path traversal")
}

func TestBuildInstanceSnapshotURIRoundTrip(t *testing.T) {
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING", "")

	objectKey := "files/snapshots/100/metadata/1.json"
	tests := []struct {
		name         string
		cfg          *objectstorage.Config
		wantURI      string
		wantProvider string
		wantRegion   string
		externalSpec string
	}{
		{
			name: "AWS",
			cfg: &objectstorage.Config{
				BucketName: "snapshot-bucket", CloudProvider: objectstorage.CloudProviderAWS,
				Region: "us-west-2", UseSSL: true,
			},
			wantURI:      "https://s3.us-west-2.amazonaws.com/snapshot-bucket/" + objectKey,
			wantProvider: objectstorage.CloudProviderAWS,
			wantRegion:   "us-west-2",
		},
		{
			name: "Aliyun",
			cfg: &objectstorage.Config{
				BucketName: "snapshot-bucket", CloudProvider: objectstorage.CloudProviderAliyun,
				Region: "cn-hangzhou", UseSSL: true,
			},
			wantURI:      "https://oss-cn-hangzhou.aliyuncs.com/snapshot-bucket/" + objectKey,
			wantProvider: objectstorage.CloudProviderAliyun,
			wantRegion:   "cn-hangzhou",
		},
		{
			name: "Tencent",
			cfg: &objectstorage.Config{
				BucketName: "snapshot-bucket", CloudProvider: objectstorage.CloudProviderTencent,
				Region: "ap-shanghai", UseSSL: true,
			},
			wantURI:      "https://cos.ap-shanghai.myqcloud.com/snapshot-bucket/" + objectKey,
			wantProvider: objectstorage.CloudProviderTencent,
			wantRegion:   "ap-shanghai",
		},
		{
			name: "Huawei",
			cfg: &objectstorage.Config{
				BucketName: "snapshot-bucket", CloudProvider: objectstorage.CloudProviderHuawei,
				Region: "cn-north-4", UseSSL: true,
			},
			wantURI:      "https://obs.cn-north-4.myhuaweicloud.com/snapshot-bucket/" + objectKey,
			wantProvider: objectstorage.CloudProviderHuawei,
			wantRegion:   "cn-north-4",
		},
		{
			name: "native GCS",
			cfg: &objectstorage.Config{
				BucketName: "snapshot-bucket", CloudProvider: objectstorage.CloudProviderGCPNative,
			},
			wantURI:      "gs://snapshot-bucket/" + objectKey,
			wantProvider: objectstorage.CloudProviderGCPNative,
		},
		{
			name: "Azure",
			cfg: &objectstorage.Config{
				Address: "core.windows.net", BucketName: "snapshot-container",
				CloudProvider: objectstorage.CloudProviderAzure, AccessKeyID: "snapshot-account",
				IgnoreAzureConnectionString: true,
			},
			wantURI:      "azure://snapshot-account.blob.core.windows.net/snapshot-container/" + objectKey,
			wantProvider: objectstorage.CloudProviderAzure,
		},
		{
			name: "custom S3-compatible endpoint",
			cfg: &objectstorage.Config{
				Address: "localhost:9000", BucketName: "snapshot-bucket",
				CloudProvider: objectstorage.CloudProviderAWS,
			},
			wantURI:      "minio://localhost:9000/snapshot-bucket/" + objectKey,
			externalSpec: `{"format":"milvus-table","extfs":{"cloud_provider":"minio","access_key_id":"ak","access_key_value":"sk"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uri, err := BuildInstanceSnapshotURI(tt.cfg, objectKey)
			require.NoError(t, err)
			assert.Equal(t, tt.wantURI, uri)
			if tt.externalSpec != "" {
				require.NoError(t, externalspec.ValidateSourceAndSpec(uri, tt.externalSpec))
			}

			bucket, parsedKey, endpoint, err := ParseForeignURI(uri)
			require.NoError(t, err)
			assert.Equal(t, tt.cfg.BucketName, bucket)
			assert.Equal(t, objectKey, parsedKey)

			parsedURI, err := url.Parse(uri)
			require.NoError(t, err)
			restored := objectstorage.NewDefaultConfig()
			_, _, err = applySnapshotExternalSpecToConfig(restored, parsedURI.Scheme, endpoint, "")
			require.NoError(t, err)
			assert.Equal(t, tt.wantProvider, restored.CloudProvider)
			assert.Equal(t, tt.wantRegion, restored.Region)
		})
	}
}

func TestApplySnapshotExternalSpecRejectsURIProviderConflict(t *testing.T) {
	cfg := objectstorage.NewDefaultConfig()
	_, _, err := applySnapshotExternalSpecToConfig(
		cfg,
		"https",
		"oss-cn-hangzhou.aliyuncs.com",
		`{"extfs":{"cloud_provider":"aws","region":"us-west-2","use_iam":"true"}}`,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match snapshot URI provider")
}

func TestApplySnapshotExternalSpecAzureNoSpecDefaultPort(t *testing.T) {
	// The no-spec same-account check compares the configured endpoint against
	// the URI host; the default https port may appear on either side — Milvus
	// normalizes minio.address to host:port, while URI writing usually drops
	// it — and must not turn the comparison into a mismatch.
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING", "")
	newCfg := func(address string) *objectstorage.Config {
		return &objectstorage.Config{
			Address:                     address,
			BucketName:                  "snapshot-container",
			AccessKeyID:                 "snapshot-account",
			SecretAccessKeyID:           "key",
			CloudProvider:               objectstorage.CloudProviderAzure,
			UseSSL:                      true,
			IgnoreAzureConnectionString: true,
		}
	}

	t.Run("port on the configured side", func(t *testing.T) {
		hasSpec, _, err := applySnapshotExternalSpecToConfig(
			newCfg("core.windows.net:443"),
			"azure",
			"snapshot-account.blob.core.windows.net",
			"",
		)
		require.NoError(t, err)
		assert.False(t, hasSpec)
	})

	t.Run("port on the URI side", func(t *testing.T) {
		hasSpec, _, err := applySnapshotExternalSpecToConfig(
			newCfg("core.windows.net"),
			"azure",
			"snapshot-account.blob.core.windows.net:443",
			"",
		)
		require.NoError(t, err)
		assert.False(t, hasSpec)
	})

	t.Run("different account still rejected", func(t *testing.T) {
		_, _, err := applySnapshotExternalSpecToConfig(
			newCfg("core.windows.net:443"),
			"azure",
			"other-account.blob.core.windows.net",
			"",
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not match the instance storage credential")
	})
}

func TestBuildStorageConfigSnapshotURIQualifiesObjectKey(t *testing.T) {
	cfg := StorageConfigFromObjectConfig(&objectstorage.Config{
		BucketName:    "snapshot-bucket",
		CloudProvider: objectstorage.CloudProviderAliyun,
		Region:        "cn-hangzhou",
		UseSSL:        true,
	}, "remote")

	uri, err := BuildStorageConfigSnapshotURI(cfg, "export-root/snapshots/100/metadata/1.json")

	require.NoError(t, err)
	assert.Equal(t,
		"https://oss-cn-hangzhou.aliyuncs.com/snapshot-bucket/export-root/snapshots/100/metadata/1.json",
		uri,
	)
}

func TestDeriveForeignRootRequiresTerminalCanonicalAnchor(t *testing.T) {
	tests := []struct {
		name      string
		objectKey string
		wantRoot  string
		wantErr   bool
	}{
		{
			name:      "uses terminal anchor",
			objectKey: "prefix/snapshots/7/metadata/9/fake/snapshots/100/metadata/2.json",
			wantRoot:  "prefix/snapshots/7/metadata/9/fake",
		},
		{
			name:      "rejects trailing path",
			objectKey: "root/snapshots/100/metadata/2.json/extra",
			wantErr:   true,
		},
		{
			name:      "rejects zero collection",
			objectKey: "root/snapshots/0/metadata/2.json",
			wantErr:   true,
		},
		{
			name:      "rejects non numeric snapshot",
			objectKey: "root/snapshots/100/metadata/latest.json",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root, err := DeriveForeignRoot(DirectionRestore, tt.objectKey)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantRoot, root)
		})
	}
}

func TestParseForeignRootURIAllowsBucketRoot(t *testing.T) {
	tests := []struct {
		name         string
		uri          string
		wantBucket   string
		wantEndpoint string
	}{
		{name: "s3", uri: "s3://bucket", wantBucket: "bucket"},
		{name: "gcs", uri: "gs://bucket", wantBucket: "bucket"},
		{name: "minio endpoint", uri: "minio://minio.example.com/bucket", wantBucket: "bucket", wantEndpoint: "minio.example.com"},
		{name: "https endpoint", uri: "https://storage.example.com/bucket", wantBucket: "bucket", wantEndpoint: "storage.example.com"},
		{name: "azure endpoint", uri: "azure://blob.core.windows.net/container", wantBucket: "container", wantEndpoint: "blob.core.windows.net"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, objectKey, endpoint, err := ParseForeignRootURI(tt.uri)
			require.NoError(t, err)
			assert.Equal(t, tt.wantBucket, bucket)
			assert.Empty(t, objectKey)
			assert.Equal(t, tt.wantEndpoint, endpoint)
		})
	}
}

func TestParseSnapshotForeignURICopySourceAllowsBucketRoot(t *testing.T) {
	bucket, root, scheme, endpoint, err := parseSnapshotForeignURI(
		DirectionCopySource,
		"s3://foreign-bucket",
	)

	require.NoError(t, err)
	assert.Equal(t, "foreign-bucket", bucket)
	assert.Empty(t, root)
	assert.Equal(t, "s3", scheme)
	assert.Empty(t, endpoint)
}

func TestSnapshotWriterSaveDefaultsToReferencedLayout(t *testing.T) {
	ctx := context.Background()
	cm := newTestChunkManager(t.TempDir())
	snapshot := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           11,
			CollectionId: 22,
			Name:         "snapshot_default_layout",
		},
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{Name: "collection_default_layout"},
		},
	}

	metadataPath, err := NewSnapshotWriter(cm).Save(ctx, snapshot)
	require.NoError(t, err)
	assert.Equal(t, datapb.SnapshotLayout_SnapshotLayoutReferenced, snapshot.Layout)

	readSnapshot, err := NewSnapshotReader(cm).ReadSnapshot(ctx, metadataPath, false)
	require.NoError(t, err)
	assert.Equal(t, datapb.SnapshotLayout_SnapshotLayoutReferenced, readSnapshot.Layout)
	assert.Equal(t, int64(11), readSnapshot.SnapshotInfo.GetId())
	assert.Equal(t, "collection_default_layout", readSnapshot.Collection.GetSchema().GetName())
}

func TestSnapshotReaderReadSnapshotTreatsUnknownLayoutAsReferenced(t *testing.T) {
	ctx := context.Background()
	cm := newTestChunkManager(t.TempDir())
	metadataPath := path.Join(cm.RootPath(), "snapshots/22/metadata/11.json")
	metadata := &datapb.SnapshotMetadata{
		FormatVersion: SnapshotFormatVersion,
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           11,
			CollectionId: 22,
			Name:         "snapshot_unknown_layout",
		},
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{Name: "collection_unknown_layout"},
		},
		Layout: datapb.SnapshotLayout_SnapshotLayoutUnknown,
	}
	data, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(metadata)
	require.NoError(t, err)
	require.NoError(t, cm.Write(ctx, metadataPath, data))

	readSnapshot, err := NewSnapshotReader(cm).ReadSnapshot(ctx, metadataPath, false)
	require.NoError(t, err)
	assert.Equal(t, datapb.SnapshotLayout_SnapshotLayoutReferenced, readSnapshot.Layout)
	assert.Equal(t, int64(11), readSnapshot.SnapshotInfo.GetId())
	assert.Equal(t, "collection_unknown_layout", readSnapshot.Collection.GetSchema().GetName())
}

func TestSnapshotReaderReadSnapshotRejectsMissingSnapshotInfo(t *testing.T) {
	ctx := context.Background()
	cm := newTestChunkManager(t.TempDir())
	metadataPath := path.Join(cm.RootPath(), "snapshots/22/metadata/11.json")
	metadata := &datapb.SnapshotMetadata{
		FormatVersion: SnapshotFormatVersion,
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{Name: "collection_missing_snapshot_info"},
		},
		Layout: datapb.SnapshotLayout_SnapshotLayoutReferenced,
	}
	data, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(metadata)
	require.NoError(t, err)
	require.NoError(t, cm.Write(ctx, metadataPath, data))

	readSnapshot, err := NewSnapshotReader(cm).ReadSnapshot(ctx, metadataPath, false)
	require.Error(t, err)
	assert.Nil(t, readSnapshot)
	assert.Contains(t, err.Error(), "snapshot info cannot be nil")
}

func TestApplySnapshotExternalSpecAzureSourceSAS(t *testing.T) {
	newAzureCfg := func() *objectstorage.Config {
		return &objectstorage.Config{
			Address:           "core.windows.net",
			BucketName:        "instance-container",
			AccessKeyID:       "instance-account",
			SecretAccessKeyID: "instance-key",
			CloudProvider:     objectstorage.CloudProviderAzure,
		}
	}

	t.Run("sets the SAS and trims the leading question mark", func(t *testing.T) {
		cfg := newAzureCfg()
		_, _, err := applySnapshotExternalSpecToConfig(
			cfg,
			"azure",
			"core.windows.net",
			`{"extfs":{"cloud_provider":"azure","access_key_id":"backup-account","access_key_value":"backup-key","source_sas_token":"?sv=2024-08-04&sig=abc"}}`,
		)
		require.NoError(t, err)
		assert.Equal(t, "sv=2024-08-04&sig=abc", cfg.AzureSourceSAS)
	})

	t.Run("requires the azure provider", func(t *testing.T) {
		cfg := &objectstorage.Config{
			Address:           "s3.us-west-2.amazonaws.com",
			BucketName:        "instance-bucket",
			AccessKeyID:       "instance-ak",
			SecretAccessKeyID: "instance-sk",
			CloudProvider:     objectstorage.CloudProviderAWS,
			Region:            "us-west-2",
		}
		_, _, err := applySnapshotExternalSpecToConfig(
			cfg,
			"s3",
			"",
			`{"extfs":{"cloud_provider":"aws","region":"us-west-2","source_sas_token":"sv=2024-08-04&sig=abc"}}`,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "source_sas_token requires cloud_provider=")
	})

	t.Run("rejects an empty token", func(t *testing.T) {
		cfg := newAzureCfg()
		_, _, err := applySnapshotExternalSpecToConfig(
			cfg,
			"azure",
			"core.windows.net",
			`{"extfs":{"cloud_provider":"azure","source_sas_token":""}}`,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "source_sas_token must be non-empty")
	})

	t.Run("is not a credential mode and combines with raw credentials", func(t *testing.T) {
		cfg := newAzureCfg()
		hasSpec, _, err := applySnapshotExternalSpecToConfig(
			cfg,
			"azure",
			"core.windows.net",
			`{"extfs":{"cloud_provider":"azure","access_key_id":"backup-account","access_key_value":"backup-key","source_sas_token":"sv=2024-08-04&sig=abc"}}`,
		)
		require.NoError(t, err)
		assert.True(t, hasSpec)
		assert.Equal(t, "backup-account", cfg.AccessKeyID)
		assert.Equal(t, "sv=2024-08-04&sig=abc", cfg.AzureSourceSAS)
	})
}

func TestSnapshotExportFiles_BucketOverlap(t *testing.T) {
	for _, mode := range []string{"disjoint", "metadata", "literal metadata", "Avro", "data", "cross bucket"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			target := path.Join(root, "backup")
			if mode == "literal metadata" {
				target += "?Version=1"
			}
			sourceRoot := path.Join(root, "source")
			if mode == "data" || mode == "cross bucket" {
				sourceRoot = path.Join(target, "files")
			}
			cm := newTestChunkManager(sourceRoot)
			manifestDir, metadata := GetSnapshotPaths(target, 100, 1)
			snapshot := &SnapshotData{
				SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100},
				Collection:   &datapb.CollectionDescription{}, MetadataPath: path.Join(sourceRoot, "metadata.json"),
				ManifestPaths: []string{path.Join(sourceRoot, "1.avro")},
				Segments: []*datapb.SegmentDescription{{
					SegmentId: 1,
					Binlogs:   []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: path.Join(sourceRoot, "data")}}}},
				}},
			}
			switch mode {
			case "metadata", "literal metadata":
				snapshot.MetadataPath = metadata
			case "Avro":
				snapshot.ManifestPaths[0] = GetSegmentManifestPath(manifestDir, 1)
			}
			targetBucket := "source"
			if mode == "cross bucket" {
				targetBucket = "target"
			}
			items, err := buildSnapshotExportFiles(context.Background(), cm, cm, "source", targetBucket,
				snapshot, target, nil, nativeManifestReader(&indexpb.StorageConfig{}))
			if mode != "disjoint" && mode != "cross bucket" {
				require.ErrorContains(t, err, "overlaps source snapshot object")
				require.Nil(t, items)
				return
			}
			require.NoError(t, err)
			require.Len(t, items, 2)
			require.Equal(t, snapshot.Segments[0].Binlogs[0].Binlogs[0].LogPath, items[0].SourcePath)
			require.Equal(t, path.Join(target, "files/data"), items[0].DestinationPath)
		})
	}
}

// Retain the input guards previously covered through the removed exported planner.
func TestSnapshotExportFiles_InvalidInputs(t *testing.T) {
	for _, mode := range []string{"nil snapshot", "nil info", "nil collection", "nil segment", "nil index", "nil source", "nil target", "external collection", "foreign target", "empty root", "missing bundle root", "manifest count", "foreign Avro"} {
		t.Run(mode, func(t *testing.T) {
			cm := newTestChunkManager(t.TempDir())
			var sourceCM, targetCM ChunkManager = cm, cm
			snapshot := &SnapshotData{SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100}, Collection: &datapb.CollectionDescription{}}
			target := path.Join(cm.RootPath(), "export")
			switch mode {
			case "nil snapshot":
				snapshot = nil
			case "nil info":
				snapshot.SnapshotInfo = nil
			case "nil collection":
				snapshot.Collection = nil
			case "nil segment":
				snapshot.Segments = []*datapb.SegmentDescription{nil}
			case "nil index":
				snapshot.Indexes = []*indexpb.IndexInfo{nil}
			case "nil source":
				sourceCM = nil
			case "nil target":
				targetCM = nil
			case "external collection":
				snapshot.Collection.Schema = &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{ExternalField: "source_id"}}}
			case "foreign target":
				target = "s3://other/export"
			case "empty root":
				target = "/"
			case "missing bundle root":
				snapshot.Layout = datapb.SnapshotLayout_SnapshotLayoutSelfContained
			case "manifest count":
				snapshot.Segments = []*datapb.SegmentDescription{{SegmentId: 1}}
			case "foreign Avro":
				snapshot.Segments = []*datapb.SegmentDescription{{SegmentId: 1}}
				snapshot.ManifestPaths = []string{"s3://other/1.avro"}
			}
			items, err := buildSnapshotExportFiles(context.Background(), sourceCM, targetCM, "bucket", "bucket", snapshot, target, nil, nativeManifestReader(&indexpb.StorageConfig{}))
			require.Error(t, err)
			require.Nil(t, items, "invalid preparation must never return a usable copy list")
		})
	}
}

func TestSnapshotExportMetadata_RequiresCompleteMetadata(t *testing.T) {
	cm := newTestChunkManager(t.TempDir())
	err := prepareSnapshotExportMetadata(context.Background(), cm, &SnapshotData{}, nil)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestSnapshotExportManifest_V6PathsAndIndexMarker(t *testing.T) {
	for _, mode := range []string{"legacy", "V3 unknown", "V3 empty", "V3 indexes", "invalid path", "encode failure"} {
		t.Run(mode, func(t *testing.T) {
			logs := func() []*datapb.FieldBinlog {
				return []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: "source/data"}, {LogPath: ""}}}}
			}
			segment := &datapb.SegmentDescription{
				SegmentId: 1, Binlogs: logs(), Deltalogs: logs(), Statslogs: logs(), Bm25Statslogs: logs(),
				IndexFiles:        []*indexpb.IndexFilePathInfo{{IndexFilePaths: []string{"s3://bucket/source/index", "source-sibling/index"}}},
				TextIndexFiles:    map[int64]*datapb.TextIndexStats{1: {Files: []string{"source/text"}}},
				JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{2: {Files: []string{"source/json"}}},
			}
			if strings.HasPrefix(mode, "V3") {
				segment.StorageVersion = StorageV3
				segment.Binlogs[0].Binlogs[0].LogPath = "legacy/insert/placeholder"
				switch mode {
				case "V3 empty":
					segment.ManifestHasIndex = proto.Bool(false)
				case "V3 indexes":
					segment.ManifestHasIndex = proto.Bool(true)
				}
			} else if mode == "invalid path" {
				segment.Deltalogs[0].Binlogs[0].LogPath = "source/../outside"
			} else if mode == "encode failure" {
				defer mockey.Mock(snapshotio.MarshalSegmentManifest).Return(nil, merr.ErrServiceUnavailable).Build().UnPatch()
			}
			before := proto.Clone(segment)
			data, err := marshalSnapshotExportManifest(segment, "source", "target")
			require.True(t, proto.Equal(before, segment))
			if mode == "invalid path" || mode == "encode failure" {
				require.Error(t, err)
				require.Nil(t, data)
				if mode == "encode failure" {
					require.ErrorIs(t, err, merr.ErrServiceUnavailable)
				}
				return
			}
			require.NoError(t, err)
			exported, err := snapshotio.ParseSegmentManifest(data, snapshotio.SnapshotFormatVersion)
			require.NoError(t, err)
			require.Equal(t, []string{"target/files/index", "target/files/source-sibling/index"}, exported.GetIndexFiles()[0].IndexFilePaths)
			for _, fieldLogs := range [][]*datapb.FieldBinlog{exported.Deltalogs, exported.Statslogs, exported.Bm25Statslogs} {
				require.Equal(t, "target/files/data", fieldLogs[0].Binlogs[0].LogPath)
				require.Empty(t, fieldLogs[0].Binlogs[1].LogPath)
			}
			if mode == "legacy" {
				require.Equal(t, "target/files/data", exported.Binlogs[0].Binlogs[0].LogPath)
				require.Equal(t, []string{"target/files/text"}, exported.TextIndexFiles[1].Files)
				require.Equal(t, []string{"target/files/json"}, exported.JsonKeyIndexFiles[2].Files)
			} else {
				require.Empty(t, exported.Binlogs)
				require.Equal(t, mode != "V3 empty", exported.GetManifestHasIndex())
				require.Equal(t, []string{"source/text"}, exported.TextIndexFiles[1].Files)
				require.Equal(t, []string{"source/json"}, exported.JsonKeyIndexFiles[2].Files)
			}
		})
	}
}

func TestSnapshotExportSegment_RequiresOneSegment(t *testing.T) {
	for _, segments := range [][]*datapb.SegmentDescription{nil, {nil}, {{SegmentId: 1}, {SegmentId: 2}}} {
		err := copySnapshotExportSegment(context.Background(), "", nil, &SnapshotData{Segments: segments}, "", "", nil, nil, nil)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
	}
}

func TestSnapshotExportShard_CanceledAfterManifestWrites(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	target := newTestChunkManager(t.TempDir())
	snapshot := &SnapshotData{SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100}, Segments: []*datapb.SegmentDescription{{SegmentId: 1}}}
	manifestDir, _ := GetSnapshotPaths(target.RootPath(), 100, 1)
	destination := GetSegmentManifestPath(manifestDir, 1)
	var original func(context.Context, []FileCopy, chan struct{}, func(context.Context, FileCopy) error) error
	calls := 0
	defer mockey.Mock(CopyFiles).Origin(&original).To(func(ctx context.Context, items []FileCopy, permits chan struct{}, copy func(context.Context, FileCopy) error) error {
		err := original(ctx, items, permits, copy)
		calls++
		if calls == 2 {
			// Exercise cancellation immediately after the last executor has drained
			// and checked its context, but before the shard publishes completion.
			cancel()
		}
		return err
	}).Build().UnPatch()
	err := copySnapshotExportSegment(ctx, "", []FileCopy{{SourcePath: "source/1.avro", DestinationPath: destination, IsMetadata: true}},
		snapshot, "source", target.RootPath(), &ResolvedForeignStorage{ForeignCM: target, TargetStore: &struct{ objectstore.ObjectStore }{}}, make(chan struct{}, 1), nil)
	require.ErrorIs(t, err, context.Canceled)
	exists, err := target.Exist(context.Background(), destination)
	require.NoError(t, err)
	require.True(t, exists, "confirmed Avro writes do not override cancellation")
}

func TestSnapshotSegmentVisitorValidation(t *testing.T) {
	for _, kind := range []string{"unbuffered", "duplicate native mapping", "rebase descriptor", "invalid path", "outside bundle", "cancel after read"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			snapshot := &SnapshotData{
				ManifestPaths: []string{"bundle/snapshots/100/manifests/1/1.avro"},
				MetadataPath:  "bundle/snapshots/100/metadata/1.json",
				SegmentIDs:    []int64{1},
			}
			segment := &datapb.SegmentDescription{SegmentId: 1}
			permits := make(chan struct{}, 1)
			rebase := false
			switch kind {
			case "unbuffered":
				permits = make(chan struct{})
			case "duplicate native mapping":
				snapshot.StorageManifests = []*datapb.StorageV2SegmentManifest{
					{SegmentId: 1, Manifest: "a"}, {SegmentId: 1, Manifest: "b"},
				}
			case "rebase descriptor":
				segment.ManifestPath = "{invalid-descriptor"
				rebase = true
			case "invalid path":
				segment.Deltalogs = []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: "../escape"}}}}
			case "outside bundle":
				snapshot.Layout = datapb.SnapshotLayout_SnapshotLayoutSelfContained
				segment.Deltalogs = []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: "outside/delta"}}}}
			}
			defer mockey.Mock((*SnapshotReader).readManifestFile).To(func(context.Context, string, int) (*datapb.SegmentDescription, error) {
				if kind == "cancel after read" {
					cancel()
				}
				return segment, nil
			}).Build().UnPatch()
			visits := 0
			err := NewSnapshotReader(nil, WithManifestReadPermits(permits)).visitSnapshotSegments(
				ctx, snapshot, "old", "bundle", rebase,
				func(context.Context, *SnapshotData) error { visits++; return nil })
			require.Error(t, err)
			require.Zero(t, visits, "invalid segment must not enter the copy stage")
			require.Empty(t, permits)
		})
	}
}

func TestSnapshotReaderRejectsTopLevelPathOutsideBundle(t *testing.T) {
	cm := newTestChunkManager(t.TempDir())
	metadata := &datapb.SnapshotMetadata{
		FormatVersion:         int32(SnapshotFormatVersion),
		SnapshotInfo:          &datapb.SnapshotInfo{Id: 1, CollectionId: 100, S3Location: "bundle/snapshots/100/metadata/1.json"},
		Collection:            &datapb.CollectionDescription{},
		Layout:                datapb.SnapshotLayout_SnapshotLayoutSelfContained,
		ManifestList:          []string{"bundle/snapshots/100/manifests/1/1.avro"},
		Storagev2ManifestList: []*datapb.StorageV2SegmentManifest{{SegmentId: 1, Manifest: "outside/native"}},
	}
	data, err := protojson.Marshal(metadata)
	require.NoError(t, err)
	defer mockey.Mock((*testChunkManager).Read).Return(data, nil).Build().UnPatch()
	_, err = NewSnapshotReader(cm).ReadSnapshot(context.Background(), metadata.SnapshotInfo.S3Location, false)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.ErrorContains(t, err, "outside snapshot data root")
}

func TestSnapshotSegmentVisitorCancellationWhileWaitingForPermit(t *testing.T) {
	// An occupied permit must remain owned by the other operation on cancellation.
	permits := make(chan struct{}, 1)
	permits <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	snapshot := &SnapshotData{ManifestPaths: []string{"1.avro"}}
	err := NewSnapshotReader(nil, WithManifestReadPermits(permits)).visitSnapshotSegments(
		ctx, snapshot, "", "", false,
		func(context.Context, *SnapshotData) error {
			t.Error("copy must not start without a read permit")
			return nil
		})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Len(t, permits, 1)
}

// PrepareSnapshotExportMetadataForTest exposes metadata preparation to external tests.

type testChunkManager struct{ root string }

func newTestChunkManager(root string) *testChunkManager { return &testChunkManager{root} }
func (s *testChunkManager) RootPath() string            { return s.root }
func (s *testChunkManager) Read(_ context.Context, key string) ([]byte, error) {
	b, err := os.ReadFile(key)
	if os.IsNotExist(err) {
		return nil, merr.WrapErrIoKeyNotFound(key)
	}
	return b, err
}

func (s *testChunkManager) Write(_ context.Context, key string, b []byte) error {
	if err := os.MkdirAll(path.Dir(key), 0o755); err != nil {
		return err
	}
	return os.WriteFile(key, b, 0o600)
}
func (s *testChunkManager) Remove(_ context.Context, key string) error { return os.Remove(key) }
func (s *testChunkManager) MultiRemove(ctx context.Context, keys []string) error {
	for _, key := range keys {
		if err := s.Remove(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

func (s *testChunkManager) Exist(_ context.Context, key string) (bool, error) {
	_, err := os.Stat(key)
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil, err
}

func TestEndpointAllowlistRejectsPrivateIPUnlessAllowlisted(t *testing.T) {
	assert.False(t, endpointAllowedByAllowlist(
		"http://127.0.0.1:9000",
		"",
		objectstorage.CloudProviderAWS,
		"us-west-2",
	))
	assert.True(t, endpointAllowedByAllowlist(
		"http://127.0.0.1:9000",
		"127.0.0.1:9000",
		objectstorage.CloudProviderAWS,
		"us-west-2",
	))
}

func TestEndpointsCompatibleRejectsDifferentCustomS3Endpoints(t *testing.T) {
	assert.False(t, endpointsCompatible(
		"minio-a.example.com:9000",
		"minio-b.example.com:9000",
		"minio-b.example.com:9000",
		"minio",
		"",
	))
}

func TestS3EndpointCompatibilityRejectsNonObjectStorageAWSHost(t *testing.T) {
	assert.False(t, endpointAllowedByAllowlist(
		"ec2.us-west-2.amazonaws.com",
		"",
		objectstorage.CloudProviderAWS,
		"us-west-2",
	))
	assert.False(t, endpointsCompatible(
		"s3.us-west-2.amazonaws.com",
		"ec2.us-west-2.amazonaws.com",
		"ec2.us-west-2.amazonaws.com",
		objectstorage.CloudProviderAWS,
		"us-west-2",
	))
}

type snapshotTestStore struct {
	copy  func(context.Context, objectstore.CopyRequest) (int64, error)
	read  func(context.Context, objectstore.Object) ([]byte, error)
	write func(context.Context, objectstore.Object, []byte) error
}

func (s snapshotTestStore) Read(ctx context.Context, o objectstore.Object) ([]byte, error) {
	return s.read(ctx, o)
}

func (s snapshotTestStore) Write(ctx context.Context, o objectstore.Object, b []byte) error {
	return s.write(ctx, o, b)
}

func (s snapshotTestStore) Copy(ctx context.Context, req objectstore.CopyRequest) (int64, error) {
	return s.copy(ctx, req)
}

func TestSnapshotCopyDependencyAndLocationValidation(t *testing.T) {
	ctx := context.Background()
	store := snapshotTestStore{read: func(context.Context, objectstore.Object) ([]byte, error) { return nil, merr.ErrIoPermissionDenied }}
	store.copy = func(context.Context, objectstore.CopyRequest) (int64, error) {
		t.Fatal("invalid plan reached copying")
		return 0, nil
	}
	var err error
	for _, uri := range []string{"", "s3://", "s3://bucket/metadata.json", "s3://bucket/root/snapshots/100/metadata/1.json"} {
		_, err := CopySnapshot(ctx, store, store, CopyRequest{SourceMetadataURI: uri}, Options{Concurrency: 1})
		require.Error(t, err)
	}
	snapshot := &SnapshotData{MetadataPath: "s3://source/root/snapshots/100/metadata/1.json", SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100}, Collection: &datapb.CollectionDescription{}, ManifestPaths: []string{"root/snapshots/100/manifests/1/10.avro"}}
	for _, root := range []string{"", "s3://", "relative/path", "https://host/bucket/a//b", "https://user:secret@host/bucket/root", "s3://target/root?token=x", "s3://target/root#fragment", "s3://target/root/../x"} {
		_, err = resolveSnapshotTarget(store, root)
		require.Error(t, err)
	}
	resolved, err := resolveSnapshotTarget(store, "s3://target/dest")
	require.NoError(t, err)
	for _, r := range []segmentRange{{Start: -1, End: 1}, {End: 0}, {End: 2}} {
		_, err = copyRange(ctx, snapshot, nil, nil, resolved, "s3://target/dest", r, make(chan struct{}, 1), nil, nil)
		require.Error(t, err)
	}
	_, err = publishSnapshot(ctx, resolved, publication{MetadataURI: "s3://target/dest/snapshots/100/metadata/1.json", StagingMetadataURI: "s3://other/dest/_staging/metadata.json"})
	require.Error(t, err)
	_, err = publishSnapshot(ctx, resolved, publication{MetadataURI: "s3://target/dest/snapshots/100/metadata/1.json", StagingMetadataURI: "s3://target/dest/_staging/metadata.json"})
	require.ErrorIs(t, err, merr.ErrIoPermissionDenied)
	_, err = publishSnapshot(ctx, resolved, publication{MetadataURI: "relative/path", StagingMetadataURI: "s3://target/dest/_staging/metadata.json"})
	require.Error(t, err)
	_, err = resolveSnapshotTarget(nil, "s3://target/dest")
	require.Error(t, err)
	for _, descriptor := range []string{"{broken", `{"base_path":"","ver":1}`, `{"base_path":"root","ver":-1}`} {
		_, err = ReadNativeManifestFiles(descriptor, nil)
		require.Error(t, err)
	}
	_, err = ReadNativeManifestFiles(`{"base_path":"root","ver":1}`, nil)
	require.ErrorContains(t, err, "source storage config")
}

func TestReadManifestBytes(t *testing.T) {
	for _, mode := range []string{"success", "zero", "bad descriptor", "empty base", "negative revision", "foreign bucket", "read error", "decode error", "nil result", "cancel before read", "cancel after read", "cancel after decode"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			base, descriptor := "root/segment", `{"base_path":"root/segment","ver":7}`
			switch mode {
			case "zero":
				descriptor = `{"base_path":"root/segment","ver":0}`
			case "bad descriptor":
				descriptor = "{bad"
			case "empty base":
				descriptor = `{"base_path":"","ver":7}`
			case "negative revision":
				descriptor = `{"base_path":"root/segment","ver":-1}`
			case "foreign bucket":
				descriptor = `{"base_path":"s3://other/root/segment","ver":7}`
			case "cancel before read":
				cancel()
			}
			reads, decodes := 0, 0
			cm := newTestChunkManager("root")
			defer mockey.Mock((*testChunkManager).Read).To(func(_ *testChunkManager, _ context.Context, key string) ([]byte, error) {
				reads++
				require.Equal(t, "root/segment/_metadata/manifest-7.avro", key)
				if mode == "read error" {
					return nil, merr.ErrIoPermissionDenied
				}
				if mode == "cancel after read" {
					cancel()
				}
				return []byte("manifest bytes"), nil
			}).Build().UnPatch()
			defer mockey.Mock(DecodeManifest).To(func(data []byte, gotBase string) (*snapshotio.NativeManifestFiles, error) {
				decodes++
				require.Equal(t, []byte("manifest bytes"), data)
				require.Equal(t, base, gotBase)
				if mode == "decode error" {
					return nil, merr.ErrDataIntegrity
				}
				if mode == "nil result" {
					return nil, nil
				}
				if mode == "cancel after decode" {
					cancel()
				}
				return &snapshotio.NativeManifestFiles{ColumnFiles: []string{"root/segment/data"}}, nil
			}).Build().UnPatch()
			files, err := readManifestBytes(ctx, cm, "source", descriptor)
			switch mode {
			case "success":
				require.NoError(t, err)
				require.Equal(t, "root/segment/_metadata/manifest-7.avro", files.ManifestFile)
				require.Equal(t, []string{"root/segment/data"}, files.ColumnFiles)
				require.Equal(t, 1, reads)
				require.Equal(t, 1, decodes)
			case "zero":
				require.NoError(t, err)
				require.Empty(t, files.ManifestFile)
				require.Zero(t, reads)
				require.Zero(t, decodes)
			default:
				require.Error(t, err)
				require.Nil(t, files)
				if strings.HasPrefix(mode, "cancel") {
					require.ErrorIs(t, err, context.Canceled)
				}
				if mode == "read error" {
					require.ErrorIs(t, err, merr.ErrIoPermissionDenied)
				}
				if mode == "decode error" {
					require.ErrorIs(t, err, merr.ErrDataIntegrity)
				}
				if mode == "foreign bucket" {
					require.Zero(t, reads)
				}
			}
		})
	}
}

func TestPlanFileCopies(t *testing.T) {
	for _, mode := range []string{"deduplicate", "conflict", "mapping error"} {
		t.Run(mode, func(t *testing.T) {
			items, err := PlanFileCopies([]string{"s3://bucket/key", "key"}, func(src string) (FileCopy, error) {
				if mode == "mapping error" {
					return FileCopy{}, merr.ErrDataIntegrity
				}
				if mode == "conflict" && src == "key" {
					src = "other"
				}
				return FileCopy{SourcePath: src, DestinationPath: "target"}, nil
			})
			if mode != "deduplicate" {
				require.ErrorIs(t, err, merr.ErrDataIntegrity)
				require.Nil(t, items)
				return
			}
			require.Len(t, items, 1)
			require.Equal(t, "s3://bucket/key", items[0].SourcePath)
		})
	}
}

func TestExecuteFileCopies(t *testing.T) {
	for _, mode := range []string{"known", "metadata", "negative", "overflow", "provider error", "unconfirmed", "late confirmation", "size and provider error"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			items := []FileCopy{{SourcePath: "a", DestinationPath: "b", IsMetadata: mode == "metadata"}}
			if mode == "overflow" {
				items = append(items, FileCopy{SourcePath: "c", DestinationPath: "d"})
			}
			var progress int64
			report, err := ExecuteFileCopies(ctx, items, make(chan struct{}, 1), func(context.Context, FileCopy) (FileCopyOutcome, error) {
				outcome := FileCopyOutcome{Confirmed: true, Bytes: 3}
				switch mode {
				case "negative":
					outcome.Bytes = -1
				case "overflow":
					outcome.Bytes = math.MaxInt64
				case "provider error":
					return FileCopyOutcome{}, merr.ErrIoPermissionDenied
				case "unconfirmed":
					return FileCopyOutcome{}, nil
				case "late confirmation":
					cancel()
					return outcome, context.DeadlineExceeded
				case "size and provider error":
					outcome.Bytes = -1
					return outcome, merr.ErrIoPermissionDenied
				}
				return outcome, nil
			}, func(n int64) { progress += n })
			switch mode {
			case "known":
				require.NoError(t, err)
				require.EqualValues(t, 3, report.CopiedBytes)
			case "metadata":
				require.NoError(t, err)
				require.Zero(t, report.CopiedBytes)
			case "negative", "overflow":
				require.ErrorIs(t, err, merr.ErrDataIntegrity)
			case "provider error", "size and provider error":
				require.ErrorIs(t, err, merr.ErrIoPermissionDenied)
			case "unconfirmed":
				require.ErrorIs(t, err, merr.ErrServiceInternal)
			case "late confirmation":
				require.ErrorIs(t, err, context.DeadlineExceeded)
			}
			if mode == "provider error" || mode == "unconfirmed" {
				require.Empty(t, report.CopiedPaths)
			} else {
				require.Len(t, report.CopiedPaths, len(items))
			}
			require.Equal(t, report.CopiedBytes, progress)
		})
	}
	_, err := ExecuteFileCopies(context.Background(), nil, make(chan struct{}, 1), nil, nil)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestExecuteFileCopiesDrainsConfirmedWrites(t *testing.T) {
	started := make(chan struct{})
	report, err := ExecuteFileCopies(context.Background(), []FileCopy{
		{SourcePath: "active", DestinationPath: "confirmed"},
		{SourcePath: "failed", DestinationPath: "absent"},
	}, make(chan struct{}, 2), func(ctx context.Context, file FileCopy) (FileCopyOutcome, error) {
		if file.SourcePath == "failed" {
			<-started
			return FileCopyOutcome{}, merr.ErrIoPermissionDenied
		}
		close(started)
		<-ctx.Done()
		// The provider confirms this write after the other copy has failed.
		return FileCopyOutcome{Confirmed: true, Bytes: 7}, nil
	}, nil)
	require.ErrorIs(t, err, merr.ErrIoPermissionDenied)
	require.Equal(t, []string{"confirmed"}, report.CopiedPaths)
	require.EqualValues(t, 7, report.CopiedBytes)
}

func TestEnumerateSegmentFilesRawInventory(t *testing.T) {
	logs := func(p string) []*datapb.FieldBinlog {
		return []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: p}}}}
	}
	native := &snapshotio.NativeManifestFiles{
		ManifestFile: "base/_metadata/manifest",
		ColumnFiles:  []string{"s3://bucket/base/data", "base/data"}, DeltaFiles: []string{"base/delta"},
		StatsFiles: []string{"base/stats"}, LobFiles: []snapshotio.LobFileInfo{{Path: "base/lob"}},
	}
	segment := &datapb.SegmentDescription{
		StorageVersion: StorageV3, SegmentId: 3, NumOfRows: 1,
		Deltalogs: logs("base/delta"), IndexFiles: []*indexpb.IndexFilePathInfo{{IndexFilePaths: []string{"base/index"}}},
	}
	got, err := EnumerateSegmentFiles(context.Background(), segment, func(context.Context, string) (*snapshotio.NativeManifestFiles, error) { return native, nil })
	require.NoError(t, err)
	require.Same(t, native, got.Manifest)
	paths := []string{}
	for _, ref := range got.Files {
		paths = append(paths, ref.Path)
		require.Empty(t, ref.NormalizedPath)
	}
	require.ElementsMatch(t, []string{"s3://bucket/base/data", "base/data", "base/delta", "base/stats", "base/lob", "base/_metadata/manifest", "base/delta", "base/index"}, paths)

	segment.StorageVersion = StorageV2
	segment.ManifestPath = "v2-manifest"
	segment.Binlogs = logs("insert")
	segment.TextIndexFiles = map[int64]*datapb.TextIndexStats{1: {Files: []string{"text"}}}
	segment.JsonKeyIndexFiles = map[int64]*datapb.JsonKeyStats{1: {Files: []string{"json-key"}}, 2: {Files: []string{"json-stats"}, JsonKeyStatsDataFormat: 2}}
	got, err = EnumerateSegmentFiles(context.Background(), segment, nil)
	require.NoError(t, err)
	require.Nil(t, got.Manifest)
	kinds := map[string]SnapshotFileType{}
	for _, ref := range got.Files {
		kinds[ref.Path] = ref.Type
	}
	require.Equal(t, map[string]SnapshotFileType{
		"insert": SnapshotFileTypeInsertBinlog, "text": SnapshotFileTypeTextIndexFile,
		"json-key": SnapshotFileTypeJSONKeyIndexFile, "json-stats": SnapshotFileTypeJSONStatsFile,
		"v2-manifest": SnapshotFileTypeStorageV2Manifest, "base/delta": SnapshotFileTypeDeltaBinlog, "base/index": SnapshotFileTypeIndexFile,
	}, kinds)
}

func TestEnumerateSegmentFilesErrors(t *testing.T) {
	for _, mode := range []string{"nil-segment", "nil-reader", "nil-inventory", "read-error", "cancel-before", "cancel-during", "missing-manifest"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			segment := &datapb.SegmentDescription{StorageVersion: StorageV3, NumOfRows: 1}
			reader := ManifestReader(func(context.Context, string) (*snapshotio.NativeManifestFiles, error) {
				switch mode {
				case "nil-inventory":
					return nil, nil
				case "read-error":
					return nil, merr.ErrServiceUnavailable
				case "cancel-during":
					cancel()
				}
				return &snapshotio.NativeManifestFiles{}, nil
			})
			switch mode {
			case "nil-segment":
				segment = nil
			case "nil-reader":
				reader = nil
			case "cancel-before":
				cancel()
			}
			got, err := EnumerateSegmentFiles(ctx, segment, reader)
			require.Nil(t, got)
			require.Error(t, err)
			switch mode {
			case "read-error":
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			case "cancel-before", "cancel-during":
				require.ErrorIs(t, err, context.Canceled)
			}
		})
	}
}

func TestDecodeManifest(t *testing.T) {
	// Raw MILV v1 fixture: no columns, one delta, one stat; exercises the
	// native legacy decoder without duplicating its serialization logic.
	legacy := []byte{
		0xac, 0xb1, 0xca, 0xd4, 0x09, 0x02, 0x00, 0x02, 0x14, 'd', 'e', 'l', 'e', 't',
		'e', '.', 'l', 'o', 'g', 0x00, 0x0e, 0x00, 0x02, 0x08, 'r', 'o', 'w', 's',
		0x02, 0x10, 's', 't', 'a', 't', '.', 'b', 'i', 'n', 0x00, 0x00,
	}
	files, err := DecodeManifest(legacy, "root/segment")
	require.NoError(t, err)
	require.Equal(t, []string{"root/segment/_delta/delete.log"}, files.DeltaFiles)
	require.Equal(t, []string{"root/segment/_stats/stat.bin"}, files.StatsFiles)
	emptyReference := append(append([]byte(nil), legacy[:8]...), 0)
	emptyReference = append(emptyReference, legacy[19:]...)
	_, err = DecodeManifest(emptyReference, "root/segment")
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	unsupportedDelta := append([]byte(nil), legacy...)
	unsupportedDelta[19] = 2 // Non-primary-key delta cannot be represented by this FFI.
	for _, tc := range []struct {
		name, base string
		data       []byte
	}{
		{"empty", "root/segment", nil},
		{"missing base", "", legacy},
		{"corrupt", "root/segment", []byte("invalid")},
		{"truncated", "root/segment", legacy[:5]},
		{"unsupported delta", "root/segment", unsupportedDelta},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files, err := DecodeManifest(tc.data, tc.base)
			if len(tc.data) == 0 || tc.base == "" {
				require.ErrorIs(t, err, merr.ErrDataIntegrity)
			} else {
				require.ErrorIs(t, err, objectstorage.ErrLoonPermanent)
			}
			require.Nil(t, files)
		})
	}
}

func TestSnapshotTargetObjectURI(t *testing.T) {
	for _, tc := range []struct{ root, key, want string }{
		{"s3://bucket/root", "root/_staging/metadata.json", "s3://bucket/root/_staging/metadata.json"},
		{"s3://s3.us-west-2.amazonaws.com/bucket/root/", "root/a.json", "s3://s3.us-west-2.amazonaws.com/bucket/root/a.json"},
		{"https://host:9000/bucket/root/", "root/a.json", "https://host:9000/bucket/root/a.json"},
		{"http://host/bucket", "a.json", "http://host/bucket/a.json"},
		{"minio://host/bucket/", "a.json", "minio://host/bucket/a.json"},
		{"gs://bucket/root", "root/a.json", "gs://bucket/root/a.json"},
		{"azure://account.blob.core.windows.net/bucket/root", "root/a.json", "azure://account.blob.core.windows.net/bucket/root/a.json"},
		{"https://host/bucket/a%20b%25%23", "a b%#/x.json", "https://host/bucket/a%20b%25%23/x.json"},
		{"/tmp/root", "/tmp/root/a.json", "/tmp/root/a.json"},
	} {
		t.Run(tc.root, func(t *testing.T) {
			got, err := snapshotTargetObjectURI(tc.root, tc.key)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			_, key, _, err := ParseForeignURI(got)
			require.NoError(t, err)
			require.Equal(t, tc.key, key)
		})
	}
	for _, tc := range []struct{ root, key string }{
		{"s3://", "a"},
		{"relative", "a"},
		{"s3://bucket/root", "../a"},
		{"https://host/bucket/a//b", "a/b/x"},
		{"s3://bucket/a%2F%2Fb", "a/b/x"},
		{"s3://bucket/root?secret=x", "a"},
		{"https://user:secret@host/bucket/root", "a"},
	} {
		_, err := snapshotTargetObjectURI(tc.root, tc.key)
		require.Error(t, err)
	}
}

func TestPublicationStorageLocation(t *testing.T) {
	store := snapshotTestStore{read: func(context.Context, objectstore.Object) ([]byte, error) {
		t.Fatal("mismatched publication must fail before storage access")
		return nil, nil
	}}
	resolved, err := resolveSnapshotTarget(store, "https://host/bucket/root")
	require.NoError(t, err)
	for _, staging := range []string{
		"https://other/bucket/root/_staging/metadata.json",
		"http://host/bucket/root/_staging/metadata.json",
		"s3://bucket/root/_staging/metadata.json",
		"root/_staging/metadata.json",
	} {
		_, err := publishSnapshot(context.Background(), resolved, publication{
			MetadataURI: "https://host/bucket/root/snapshots/100/metadata/1.json", StagingMetadataURI: staging,
		})
		require.Error(t, err)
	}
}

func TestExportTestAdaptersRejectInvalidTargets(t *testing.T) {
	ctx := context.Background()
	_, err := BuildSnapshotExportFilesForTest(ctx, nil, nil, "", "", nil, "/target", nil, nil)
	require.Error(t, err)
	_, err = BuildSnapshotExportPlanForTest(ctx, nil, nil, "", "", nil, "/target", &indexpb.StorageConfig{}, nil)
	require.Error(t, err)
}
