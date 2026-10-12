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

package storage

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	milvusstorage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func patchRemoteChunkManager(t *testing.T, captured *[]objectstorage.Config) {
	t.Helper()

	patch := mockey.Mock(milvusstorage.NewRemoteChunkManager).To(
		func(ctx context.Context, cfg *objectstorage.Config) (*milvusstorage.RemoteChunkManager, error) {
			_ = ctx
			*captured = append(*captured, *cfg)
			return milvusstorage.NewRemoteChunkManagerForTesting(nil, cfg.BucketName, cfg.RootPath), nil
		},
	).Build()
	t.Cleanup(func() { patch.UnPatch() })
}

func TestResolveForeignStorageLayer1OverridesBucketRoot(t *testing.T) {
	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	instanceCfg := &objectstorage.Config{
		Address:           "s3.us-west-2.amazonaws.com",
		BucketName:        "instance-bucket",
		RootPath:          "by-dev",
		AccessKeyID:       "instance-ak",
		SecretAccessKeyID: "instance-sk",
		UseSSL:            true,
		CloudProvider:     objectstorage.CloudProviderAWS,
		Region:            "us-west-2",
		RequestTimeoutMs:  1000,
		SslTLSMinVersion:  "1.2",
	}

	resolved, err := ResolveForeignStorage(
		context.Background(),
		instanceCfg,
		DirectionExport,
		"s3://foreign-bucket/relocated",
		"",
	)
	require.NoError(t, err)
	require.Len(t, captured, 1)
	assert.Equal(t, "foreign-bucket", captured[0].BucketName)
	assert.Equal(t, "relocated", captured[0].RootPath)

	require.NotNil(t, resolved.ForeignStorageConfig)
	assert.Equal(t, "foreign-bucket", resolved.ForeignStorageConfig.GetBucketName())
	assert.Equal(t, "relocated", resolved.ForeignStorageConfig.GetRootPath())
	assert.Equal(t, "instance-ak", resolved.ForeignStorageConfig.GetAccessKeyID())
	assert.Equal(t, "instance-sk", resolved.ForeignStorageConfig.GetSecretAccessKey())
	assert.Equal(t, paramtable.Get().MinioCfg.UseCRC32C.GetAsBool(), resolved.ForeignStorageConfig.GetUseCrc32CChecksum())
	assert.Equal(t, "remote", resolved.ForeignStorageConfig.GetStorageType())
	assert.NotNil(t, resolved.Copier)
	assert.NotNil(t, resolved.ForeignCM)
}

func TestResolveForeignStorageExportObjectKeyUsesInstanceBucket(t *testing.T) {
	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	resolved, err := ResolveForeignStorage(
		context.Background(),
		&objectstorage.Config{
			Address:       "s3.us-west-2.amazonaws.com",
			BucketName:    "instance-bucket",
			RootPath:      "by-dev",
			CloudProvider: objectstorage.CloudProviderAWS,
			Region:        "us-west-2",
		},
		DirectionExport,
		"export-root",
		"",
	)

	require.NoError(t, err)
	require.Len(t, captured, 1)
	assert.Equal(t, "instance-bucket", captured[0].BucketName)
	assert.Equal(t, "export-root", captured[0].RootPath)
	assert.Equal(t, "instance-bucket", resolved.ForeignBucket)
	assert.Equal(t, "instance-bucket", resolved.ForeignStorageConfig.GetBucketName())
	assert.Equal(t, "export-root", resolved.ForeignStorageConfig.GetRootPath())
}

func TestResolveForeignStorageRestoreDoesNotCreateForeignSourceBucket(t *testing.T) {
	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	_, err := ResolveForeignStorage(
		context.Background(),
		&objectstorage.Config{
			Address:       "s3.us-west-2.amazonaws.com",
			BucketName:    "instance-bucket",
			RootPath:      "by-dev",
			CreateBucket:  true,
			CloudProvider: objectstorage.CloudProviderAWS,
			Region:        "us-west-2",
		},
		DirectionRestore,
		"s3://foreign-bucket/root/snapshots/1/metadata/1.json",
		"",
	)
	require.NoError(t, err)
	require.Len(t, captured, 1)
	assert.False(t, captured[0].CreateBucket)
}

func TestResolveForeignStorageAppliesRawCredentialsFromExternalSpec(t *testing.T) {
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING", "")

	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	t.Run("s3 aksk", func(t *testing.T) {
		resolved, err := ResolveForeignStorage(
			context.Background(),
			&objectstorage.Config{
				Address:           "s3.us-west-2.amazonaws.com",
				BucketName:        "instance-bucket",
				RootPath:          "by-dev",
				AccessKeyID:       "instance-ak",
				SecretAccessKeyID: "instance-sk",
				CloudProvider:     objectstorage.CloudProviderAWS,
				Region:            "us-west-2",
			},
			DirectionRestore,
			"s3://foreign-bucket/root/snapshots/1/metadata/1.json",
			`{"extfs":{"cloud_provider":"aws","region":"us-west-2","access_key_id":"foreign-ak","access_key_value":"foreign-sk"}}`,
		)
		require.NoError(t, err)
		assert.Equal(t, "foreign-ak", resolved.ForeignStorageConfig.GetAccessKeyID())
		assert.Equal(t, "foreign-sk", resolved.ForeignStorageConfig.GetSecretAccessKey())
	})

	t.Run("gcp native iam", func(t *testing.T) {
		resolved, err := ResolveForeignStorage(
			context.Background(),
			&objectstorage.Config{
				BucketName:    "instance-bucket",
				RootPath:      "by-dev",
				UseIAM:        true,
				CloudProvider: objectstorage.CloudProviderGCPNative,
			},
			DirectionRestore,
			"gs://foreign-bucket/root/snapshots/1/metadata/1.json",
			`{"extfs":{"cloud_provider":"gcpnative","use_iam":"true"}}`,
		)
		require.NoError(t, err)
		assert.Empty(t, resolved.ForeignStorageConfig.GetAddress())
		assert.True(t, resolved.ForeignStorageConfig.GetUseIAM())
	})

	t.Run("azure account key", func(t *testing.T) {
		resolved, err := ResolveForeignStorage(
			context.Background(),
			&objectstorage.Config{
				Address:           "core.windows.net",
				BucketName:        "instance-container",
				RootPath:          "by-dev",
				AccessKeyID:       "azure-account",
				SecretAccessKeyID: "old-key",
				CloudProvider:     objectstorage.CloudProviderAzure,
			},
			DirectionRestore,
			"azure://core.windows.net/foreign-container/root/snapshots/1/metadata/1.json",
			`{"extfs":{"cloud_provider":"azure","access_key_id":"azure-account","access_key_value":"azure-key"}}`,
		)
		require.NoError(t, err)
		assert.Equal(t, "azure-account", resolved.ForeignStorageConfig.GetAccessKeyID())
		assert.Equal(t, "azure-key", resolved.ForeignStorageConfig.GetSecretAccessKey())
		assert.Equal(t, objectstorage.CloudProviderAzure, resolved.ForeignStorageConfig.GetCloudProvider())
	})
}

func TestResolveForeignStorageLayer2RestoreCopierPreservesSpecCredentialMode(t *testing.T) {
	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	_, err := ResolveForeignStorage(
		context.Background(),
		&objectstorage.Config{
			Address:           "s3.us-west-2.amazonaws.com",
			BucketName:        "instance-bucket",
			RootPath:          "by-dev",
			AccessKeyID:       "instance-ak",
			SecretAccessKeyID: "instance-sk",
			UseIAM:            true,
			IAMEndpoint:       "instance-iam",
			CloudProvider:     objectstorage.CloudProviderAWS,
			Region:            "us-west-2",
		},
		DirectionRestore,
		"s3://foreign-bucket/root/snapshots/1/metadata/1.json",
		`{"extfs":{"cloud_provider":"aws","region":"us-west-2","iam_endpoint":"foreign-iam","access_key_id":"foreign-ak","access_key_value":"foreign-sk"}}`,
	)
	require.NoError(t, err)
	require.Len(t, captured, 2)

	copyCfg := captured[1]
	assert.Equal(t, "s3.us-west-2.amazonaws.com", copyCfg.Address)
	assert.Equal(t, "instance-bucket", copyCfg.BucketName)
	assert.Equal(t, "foreign-ak", copyCfg.AccessKeyID)
	assert.Equal(t, "foreign-sk", copyCfg.SecretAccessKeyID)
	assert.False(t, copyCfg.UseIAM)
	assert.Equal(t, "foreign-iam", copyCfg.IAMEndpoint)
}

func TestResolveForeignStorageCopySourceAcceptsSourceRoot(t *testing.T) {
	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	resolved, err := ResolveForeignStorage(
		context.Background(),
		&objectstorage.Config{
			Address:           "s3.us-west-2.amazonaws.com",
			BucketName:        "instance-bucket",
			RootPath:          "by-dev",
			AccessKeyID:       "instance-ak",
			SecretAccessKeyID: "instance-sk",
			CloudProvider:     objectstorage.CloudProviderAWS,
			Region:            "us-west-2",
		},
		DirectionCopySource,
		"s3://foreign-bucket/source-root/files",
		`{"extfs":{"cloud_provider":"aws","region":"us-west-2","access_key_id":"foreign-ak","access_key_value":"foreign-sk"}}`,
	)
	require.NoError(t, err)
	require.Len(t, captured, 2)

	copyCfg := captured[1]
	assert.Equal(t, "instance-bucket", copyCfg.BucketName)
	assert.Equal(t, "by-dev", copyCfg.RootPath)
	assert.Equal(t, "foreign-ak", copyCfg.AccessKeyID)
	assert.Equal(t, "foreign-sk", copyCfg.SecretAccessKeyID)

	assert.Equal(t, "foreign-bucket", captured[0].BucketName)
	assert.Equal(t, "source-root/files", captured[0].RootPath)
	require.NotNil(t, resolved.ForeignStorageConfig)
	assert.Equal(t, "foreign-bucket", resolved.ForeignStorageConfig.GetBucketName())
	assert.Equal(t, "source-root/files", resolved.ForeignStorageConfig.GetRootPath())
	assert.Equal(t, "foreign-ak", resolved.ForeignStorageConfig.GetAccessKeyID())
	assert.Equal(t, "foreign-sk", resolved.ForeignStorageConfig.GetSecretAccessKey())
}

func TestResolveForeignStorageSkipsBucketChecks(t *testing.T) {
	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	_, err := ResolveForeignStorage(
		context.Background(),
		&objectstorage.Config{
			Address:       "s3.us-west-2.amazonaws.com",
			BucketName:    "instance-bucket",
			RootPath:      "by-dev",
			CloudProvider: objectstorage.CloudProviderAWS,
			Region:        "us-west-2",
		},
		DirectionRestore,
		"s3://foreign-bucket/source-root/snapshots/1/metadata/1.json",
		`{"extfs":{"cloud_provider":"aws","region":"us-west-2","access_key_id":"foreign-ak","access_key_value":"foreign-sk"}}`,
	)
	require.NoError(t, err)
	require.Len(t, captured, 2)
	assert.True(t, captured[0].SkipBucketCheck)
	assert.True(t, captured[1].SkipBucketCheck)
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

func TestResolveForeignStorage_RejectsCrossCloud(t *testing.T) {
	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	_, err := ResolveForeignStorage(
		context.Background(),
		&objectstorage.Config{
			Address:       "s3.us-west-2.amazonaws.com",
			BucketName:    "local",
			CloudProvider: objectstorage.CloudProviderAWS,
			Region:        "us-west-2",
		},
		DirectionRestore,
		"gs://foreign/root/snapshots/1/metadata/1.json",
		`{"extfs":{"cloud_provider":"gcpnative","region":"us-central1","use_iam":"true"}}`,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), unsupportedServerSideCopyMessage)
	assert.Contains(t, err.Error(), "streaming")
	assert.Equal(t, merr.InputError, merr.GetErrorType(err))
	assert.Empty(t, captured)
}

func TestResolveForeignStorage_RejectsIndependentS3CompatibleEndpoints(t *testing.T) {
	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	_, err := ResolveForeignStorage(
		context.Background(),
		&objectstorage.Config{
			Address:       "minio-a.example.com:9000",
			BucketName:    "local",
			CloudProvider: "minio",
		},
		DirectionExport,
		"minio://minio-b.example.com:9000/foreign/root",
		`{"extfs":{"cloud_provider":"minio","use_iam":"true"}}`,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), unsupportedServerSideCopyMessage)
	assert.Equal(t, merr.InputError, merr.GetErrorType(err))
	assert.Empty(t, captured)
}

func TestResolveForeignStorageRejectsGCPNativeDifferentCustomEndpoints(t *testing.T) {
	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	_, err := ResolveForeignStorage(
		context.Background(),
		&objectstorage.Config{
			Address:       "gcs-a.example.com",
			BucketName:    "instance-bucket",
			CloudProvider: objectstorage.CloudProviderGCPNative,
		},
		DirectionRestore,
		"https://gcs-b.example.com/foreign-bucket/root/snapshots/1/metadata/1.json",
		`{"extfs":{"cloud_provider":"gcpnative","use_iam":"true"}}`,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), unsupportedServerSideCopyMessage)
	assert.Equal(t, merr.InputError, merr.GetErrorType(err))
	assert.Empty(t, captured)
}

func TestResolveForeignStorageRejectsLocalInstanceConfig(t *testing.T) {
	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	_, err := ResolveForeignStorage(
		context.Background(),
		&objectstorage.Config{
			RootPath: "/tmp/milvus",
		},
		DirectionExport,
		"s3://foreign-bucket/root/manifest.json",
		"",
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), unsupportedServerSideCopyMessage)
	assert.Equal(t, merr.SystemError, merr.GetErrorType(err))
	assert.Empty(t, captured)
}

func TestResolveForeignStorageLayer2RejectsSpecProviderWithoutEndpoint(t *testing.T) {
	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	_, err := ResolveForeignStorage(
		context.Background(),
		&objectstorage.Config{
			Address:       "s3.us-west-2.amazonaws.com",
			BucketName:    "instance-bucket",
			CloudProvider: objectstorage.CloudProviderAWS,
			Region:        "us-west-2",
		},
		DirectionExport,
		"s3://foreign-bucket/root/manifest.json",
		`{"extfs":{"cloud_provider":"aliyun"}}`,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires a derivable region")
	assert.Empty(t, captured)
}

func TestResolveForeignStorageLayer2DerivedEndpointSetsUseSSL(t *testing.T) {
	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	_, err := ResolveForeignStorage(
		context.Background(),
		&objectstorage.Config{
			Address:       "s3.us-west-2.amazonaws.com",
			BucketName:    "instance-bucket",
			UseSSL:        false,
			CloudProvider: objectstorage.CloudProviderAWS,
			Region:        "us-west-2",
		},
		DirectionExport,
		"s3://foreign-bucket/root/manifest.json",
		`{"extfs":{"cloud_provider":"aws","region":"us-west-2"}}`,
	)
	require.NoError(t, err)
	require.Len(t, captured, 1)
	assert.Equal(t, "s3.us-west-2.amazonaws.com", captured[0].Address)
	assert.True(t, captured[0].UseSSL)
}

func TestResolveForeignStorageLayer1RejectsDifferentURIFamily(t *testing.T) {
	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	_, err := ResolveForeignStorage(
		context.Background(),
		&objectstorage.Config{
			Address:       "s3.us-west-2.amazonaws.com",
			BucketName:    "instance-bucket",
			CloudProvider: objectstorage.CloudProviderAWS,
			Region:        "us-west-2",
		},
		DirectionRestore,
		"gs://foreign-bucket/root/snapshots/1/metadata/1.json",
		"",
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), unsupportedServerSideCopyMessage)
	assert.Equal(t, merr.InputError, merr.GetErrorType(err))
	assert.Empty(t, captured)
}

func TestResolveForeignStorageLayer2RejectsURISchemeProviderMismatch(t *testing.T) {
	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	_, err := ResolveForeignStorage(
		context.Background(),
		&objectstorage.Config{
			Address:       "s3.us-west-2.amazonaws.com",
			BucketName:    "instance-bucket",
			CloudProvider: objectstorage.CloudProviderAWS,
			Region:        "us-west-2",
		},
		DirectionRestore,
		"gs://foreign-bucket/root/snapshots/1/metadata/1.json",
		`{"extfs":{"cloud_provider":"aws","region":"us-west-2"}}`,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match snapshot URI provider")
	assert.Equal(t, merr.InputError, merr.GetErrorType(err))
	assert.Empty(t, captured)
}

func TestResolveForeignStorageLayer1RejectsDifferentURIEndpoint(t *testing.T) {
	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	_, err := ResolveForeignStorage(
		context.Background(),
		&objectstorage.Config{
			Address:       "minio-a.example.com:9000",
			BucketName:    "instance-bucket",
			CloudProvider: "minio",
		},
		DirectionExport,
		"minio://minio-b.example.com:9000/foreign-bucket/root/manifest.json",
		"",
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), unsupportedServerSideCopyMessage)
	assert.Empty(t, captured)
}

func TestResolveForeignStorageExportRawAKSK(t *testing.T) {
	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	resolved, err := ResolveForeignStorage(
		context.Background(),
		&objectstorage.Config{
			Address:       "s3.us-west-2.amazonaws.com",
			BucketName:    "instance-bucket",
			CloudProvider: objectstorage.CloudProviderAWS,
			Region:        "us-west-2",
		},
		DirectionExport,
		"s3://foreign-bucket/root/manifest.json",
		`{"extfs":{"cloud_provider":"aws","region":"us-west-2","access_key_id":"foreign-ak","access_key_value":"foreign-sk"}}`,
	)
	require.NoError(t, err)
	require.Len(t, captured, 1)
	assert.Equal(t, "foreign-ak", captured[0].AccessKeyID)
	assert.Equal(t, "foreign-sk", captured[0].SecretAccessKeyID)
	assert.Equal(t, "foreign-ak", resolved.ForeignStorageConfig.GetAccessKeyID())
	assert.Equal(t, "foreign-sk", resolved.ForeignStorageConfig.GetSecretAccessKey())
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

func azureInstanceCfg() *objectstorage.Config {
	return &objectstorage.Config{
		Address:           "core.windows.net",
		BucketName:        "instance-container",
		RootPath:          "by-dev",
		AccessKeyID:       "instance-account",
		SecretAccessKeyID: "instance-key",
		CloudProvider:     objectstorage.CloudProviderAzure,
	}
}

func TestResolveForeignStorageAzureCrossAccountExportWithSourceSAS(t *testing.T) {
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING", "")

	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	resolved, err := ResolveForeignStorage(
		context.Background(),
		azureInstanceCfg(),
		DirectionExport,
		"azure://backup-account.blob.core.windows.net/backup-container/export-root",
		`{"extfs":{"cloud_provider":"azure","access_key_id":"backup-account","access_key_value":"backup-key","source_sas_token":"?sv=2024-08-04&sig=abc"}}`,
	)
	require.NoError(t, err)
	assert.Equal(t, "backup-container", resolved.ForeignBucket)

	// Export builds one client: the foreign-account copier, whose cross-account
	// source URLs must address the instance account under the read SAS.
	require.Len(t, captured, 1)
	assert.Equal(t, "backup-account", captured[0].AccessKeyID)
	assert.Equal(t, "instance-account.blob.core.windows.net", captured[0].AzureSourceEndpoint)
	assert.Equal(t, "sv=2024-08-04&sig=abc", captured[0].AzureSourceSAS)
	// The destination (foreign) transport is forced to TLS for canonical Azure
	// endpoints, but the source URL scheme must follow the source — here the
	// instance account, which has UseSSL=false.
	assert.True(t, captured[0].UseSSL)
	assert.False(t, captured[0].AzureSourceUseSSL)
}

func TestResolveForeignStorageAzureCrossAccountExportWithSourceSASDefaultPort(t *testing.T) {
	// Regression: Milvus normalizes minio.address to host:port, so a real
	// instance config reads "core.windows.net:443", while snapshot URI parsing
	// drops ports. The default-port spelling difference must not break the
	// same-cloud comparison that gates a SAS-authorized cross-account copy.
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING", "")

	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	instanceCfg := azureInstanceCfg()
	instanceCfg.Address = "core.windows.net:443"
	instanceCfg.UseSSL = true

	resolved, err := ResolveForeignStorage(
		context.Background(),
		instanceCfg,
		DirectionExport,
		"azure://backup-account.blob.core.windows.net/backup-container/export-root",
		`{"extfs":{"cloud_provider":"azure","access_key_id":"backup-account","access_key_value":"backup-key","source_sas_token":"sv=2024-08-04&sig=abc"}}`,
	)
	require.NoError(t, err)
	assert.Equal(t, "backup-container", resolved.ForeignBucket)

	// The copy source endpoint is emitted in the canonical portless form.
	require.Len(t, captured, 1)
	assert.Equal(t, "instance-account.blob.core.windows.net", captured[0].AzureSourceEndpoint)
	assert.True(t, captured[0].AzureSourceUseSSL)
}

func TestResolveForeignStorageAzureSameAccountExportDefaultPort(t *testing.T) {
	// Same regression on the no-SAS path: a same-account export must not be
	// misclassified as cross-account just because the instance address carries
	// the default https port.
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING", "")

	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	instanceCfg := azureInstanceCfg()
	instanceCfg.Address = "core.windows.net:443"
	instanceCfg.UseSSL = true

	_, err := ResolveForeignStorage(
		context.Background(),
		instanceCfg,
		DirectionExport,
		"azure://instance-account.blob.core.windows.net/instance-container/export-root",
		`{"extfs":{"cloud_provider":"azure","access_key_id":"instance-account","access_key_value":"instance-key"}}`,
	)
	require.NoError(t, err)
	require.Len(t, captured, 1)
}

func TestResolveForeignStorageAzureCrossAccountSourceSASNonDefaultPort(t *testing.T) {
	// Only the scheme's default port is normalized away; a non-default port is
	// preserved and still fails the same-cloud check.
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING", "")

	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	instanceCfg := azureInstanceCfg()
	instanceCfg.Address = "core.windows.net:1443"
	instanceCfg.UseSSL = true

	_, err := ResolveForeignStorage(
		context.Background(),
		instanceCfg,
		DirectionExport,
		"azure://backup-account.blob.core.windows.net/backup-container/export-root",
		`{"extfs":{"cloud_provider":"azure","access_key_id":"backup-account","access_key_value":"backup-key","source_sas_token":"sv=2024-08-04&sig=abc"}}`,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must stay within one Azure cloud")
}

func TestResolveForeignStorageAzureCrossAccountRestoreWithSourceSAS(t *testing.T) {
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING", "")

	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	_, err := ResolveForeignStorage(
		context.Background(),
		azureInstanceCfg(),
		DirectionRestore,
		"azure://backup-account.blob.core.windows.net/backup-container/root/snapshots/1/metadata/1.json",
		`{"extfs":{"cloud_provider":"azure","access_key_id":"backup-account","access_key_value":"backup-key","source_sas_token":"sv=2024-08-04&sig=abc"}}`,
	)
	require.NoError(t, err)

	require.Len(t, captured, 2)
	foreignCM, copier := captured[0], captured[1]
	// Metadata still reads from the foreign account with its own credential,
	// and the metadata client config must not carry the source-copy fields.
	assert.Equal(t, "backup-account", foreignCM.AccessKeyID)
	assert.Empty(t, foreignCM.AzureSourceEndpoint)
	assert.Empty(t, foreignCM.AzureSourceSAS)
	// The copy request writes the instance bucket, so the instance credential
	// authorizes it and the source side is the foreign account under the SAS.
	assert.Equal(t, "instance-account", copier.AccessKeyID)
	assert.Equal(t, "instance-container", copier.BucketName)
	assert.Equal(t, "backup-account.blob.core.windows.net", copier.AzureSourceEndpoint)
	assert.Equal(t, "sv=2024-08-04&sig=abc", copier.AzureSourceSAS)
	// The destination is the instance account (UseSSL=false), but the source
	// URL scheme must follow the source — the foreign canonical Azure account,
	// whose transport is TLS.
	assert.False(t, copier.UseSSL)
	assert.True(t, copier.AzureSourceUseSSL)
}

func TestResolveForeignStorageAzureCrossAccountRestoreConstructsRealClients(t *testing.T) {
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING", "")

	// Regression test: a restore/copy-source request carrying a source SAS used
	// to fail before doing any work, because the foreign metadata client was
	// constructed from a config that had AzureSourceSAS but no
	// AzureSourceEndpoint, a combination the Azure client constructor rejects.
	// Exercise the real NewRemoteChunkManager constructor (client construction
	// is offline) instead of mocking it. The fixture keys are base64
	// ("instance-key"/"backup-key") because the Azure SDK decodes the account
	// key eagerly at construction; nothing is ever signed here.
	// #nosec G101 -- offline fixture credentials, see above.
	instanceCfg := &objectstorage.Config{
		Address:           "core.windows.net",
		BucketName:        "instance-container",
		RootPath:          "by-dev",
		AccessKeyID:       "instance-account",
		SecretAccessKeyID: "aW5zdGFuY2Uta2V5",
		CloudProvider:     objectstorage.CloudProviderAzure,
	}
	externalSpec := `{"extfs":{"cloud_provider":"azure","access_key_id":"backup-account","access_key_value":"YmFja3VwLWtleQ==","source_sas_token":"sv=2024-08-04&sig=abc"}}`

	t.Run("restore", func(t *testing.T) {
		resolved, err := ResolveForeignStorage(
			context.Background(),
			instanceCfg,
			DirectionRestore,
			"azure://backup-account.blob.core.windows.net/backup-container/root/snapshots/1/metadata/1.json",
			externalSpec,
		)
		require.NoError(t, err)
		assert.Equal(t, "backup-container", resolved.ForeignBucket)
		assert.NotNil(t, resolved.ForeignCM)
		assert.NotNil(t, resolved.Copier)
	})

	t.Run("copy source", func(t *testing.T) {
		resolved, err := ResolveForeignStorage(
			context.Background(),
			instanceCfg,
			DirectionCopySource,
			"azure://backup-account.blob.core.windows.net/backup-container/root",
			externalSpec,
		)
		require.NoError(t, err)
		assert.Equal(t, "backup-container", resolved.ForeignBucket)
		assert.NotNil(t, resolved.ForeignCM)
		assert.NotNil(t, resolved.Copier)
	})
}

func TestResolveForeignStorageAzureCrossAccountWithoutSASRejected(t *testing.T) {
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING", "")

	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	_, err := ResolveForeignStorage(
		context.Background(),
		azureInstanceCfg(),
		DirectionExport,
		"azure://backup-account.blob.core.windows.net/backup-container/export-root",
		`{"extfs":{"cloud_provider":"azure","access_key_id":"backup-account","access_key_value":"backup-key"}}`,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), unsupportedServerSideCopyMessage)
	assert.Empty(t, captured)
}

func TestResolveForeignStorageAzureSameAccountWithSASRejected(t *testing.T) {
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING", "")

	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	_, err := ResolveForeignStorage(
		context.Background(),
		azureInstanceCfg(),
		DirectionExport,
		"azure://instance-account.blob.core.windows.net/backup-container/export-root",
		`{"extfs":{"cloud_provider":"azure","access_key_id":"instance-account","access_key_value":"backup-key","source_sas_token":"sv=2024-08-04&sig=abc"}}`,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "source_sas_token applies only when the copy crosses Azure storage accounts")
	assert.Empty(t, captured)
}

func TestResolveForeignStorageAzureCrossCloudSASRejected(t *testing.T) {
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING", "")

	var captured []objectstorage.Config
	patchRemoteChunkManager(t, &captured)

	_, err := ResolveForeignStorage(
		context.Background(),
		azureInstanceCfg(),
		DirectionExport,
		"azure://backup-account.blob.core.chinacloudapi.cn/backup-container/export-root",
		`{"extfs":{"cloud_provider":"azure","access_key_id":"backup-account","access_key_value":"backup-key","source_sas_token":"sv=2024-08-04&sig=abc"}}`,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must stay within one Azure cloud")
	assert.Empty(t, captured)
}
