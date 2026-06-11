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

package snapshotstorage

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func patchRemoteChunkManager(t *testing.T, captured *[]objectstorage.Config) {
	t.Helper()

	patch := mockey.Mock(storage.NewRemoteChunkManager).To(
		func(ctx context.Context, cfg *objectstorage.Config) (*storage.RemoteChunkManager, error) {
			_ = ctx
			*captured = append(*captured, *cfg)
			return storage.NewRemoteChunkManagerForTesting(nil, cfg.BucketName, cfg.RootPath), nil
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
	require.Len(t, captured, 2)
	assert.Equal(t, "instance-bucket", captured[0].BucketName)
	assert.Equal(t, "foreign-bucket", captured[1].BucketName)
	assert.Equal(t, "relocated", captured[1].RootPath)

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

func TestResolveForeignStorageSameBucketObjectKeyUsesInstanceBucket(t *testing.T) {
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
		DirectionRestore,
		"export-root/snapshots/1/metadata/1.json",
		"",
	)

	require.NoError(t, err)
	require.Len(t, captured, 2)
	assert.Equal(t, "instance-bucket", captured[0].BucketName)
	assert.Equal(t, "instance-bucket", captured[1].BucketName)
	assert.Equal(t, "export-root", captured[1].RootPath)
	assert.Equal(t, "instance-bucket", resolved.ForeignBucket)
	assert.Equal(t, "export-root", resolved.ForeignRoot)
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
	require.Len(t, captured, 2)
	assert.False(t, captured[1].CreateBucket)
}

func TestResolveForeignStorageAppliesRawCredentialsFromExternalSpec(t *testing.T) {
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

	copyCfg := captured[0]
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

	copyCfg := captured[0]
	assert.Equal(t, "instance-bucket", copyCfg.BucketName)
	assert.Equal(t, "by-dev", copyCfg.RootPath)
	assert.Equal(t, "foreign-ak", copyCfg.AccessKeyID)
	assert.Equal(t, "foreign-sk", copyCfg.SecretAccessKeyID)

	assert.Equal(t, "foreign-bucket", captured[1].BucketName)
	assert.Equal(t, "source-root/files", captured[1].RootPath)
	require.NotNil(t, resolved.ForeignStorageConfig)
	assert.Equal(t, "foreign-bucket", resolved.ForeignStorageConfig.GetBucketName())
	assert.Equal(t, "source-root/files", resolved.ForeignStorageConfig.GetRootPath())
	assert.Equal(t, "foreign-ak", resolved.ForeignStorageConfig.GetAccessKeyID())
	assert.Equal(t, "foreign-sk", resolved.ForeignStorageConfig.GetSecretAccessKey())
}

func TestEndpointAllowlistRejectsPrivateIPUnlessAllowlisted(t *testing.T) {
	assert.False(t, EndpointAllowedByAllowlist(
		"http://127.0.0.1:9000",
		"s3.us-west-2.amazonaws.com",
		"",
		objectstorage.CloudProviderAWS,
		"us-west-2",
	))
	assert.True(t, EndpointAllowedByAllowlist(
		"http://127.0.0.1:9000",
		"s3.us-west-2.amazonaws.com",
		"127.0.0.1:9000",
		objectstorage.CloudProviderAWS,
		"us-west-2",
	))
}

func TestResolveProviderCopyCapabilityRejectsUnsupportedProviderPair(t *testing.T) {
	_, err := ResolveProviderCopyCapability(
		context.Background(),
		&objectstorage.Config{
			Address:       "s3.us-west-2.amazonaws.com",
			BucketName:    "instance-bucket",
			CloudProvider: objectstorage.CloudProviderAWS,
			Region:        "us-west-2",
		},
		&objectstorage.Config{
			BucketName:    "foreign-bucket",
			CloudProvider: objectstorage.CloudProviderGCPNative,
		},
		&ValidatedSpec{CloudProvider: objectstorage.CloudProviderGCPNative},
		DirectionRestore,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), unsupportedServerSideCopyMessage)
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
	assert.Empty(t, captured)
}

func TestResolveProviderCopyCapabilityRejectsGCPNativeDifferentCustomEndpoints(t *testing.T) {
	_, err := ResolveProviderCopyCapability(
		context.Background(),
		&objectstorage.Config{
			Address:       "gcs-a.example.com",
			BucketName:    "instance-bucket",
			CloudProvider: objectstorage.CloudProviderGCPNative,
		},
		&objectstorage.Config{
			Address:       "gcs-b.example.com",
			BucketName:    "foreign-bucket",
			CloudProvider: objectstorage.CloudProviderGCPNative,
		},
		&ValidatedSpec{CloudProvider: objectstorage.CloudProviderGCPNative},
		DirectionRestore,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), unsupportedServerSideCopyMessage)
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
	require.Len(t, captured, 2)
	assert.Equal(t, "s3.us-west-2.amazonaws.com", captured[1].Address)
	assert.True(t, captured[1].UseSSL)
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
	assert.Contains(t, err.Error(), unsupportedServerSideCopyMessage)
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
	require.Len(t, captured, 2)
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
	assert.False(t, EndpointAllowedByAllowlist(
		"ec2.us-west-2.amazonaws.com",
		"s3.us-west-2.amazonaws.com",
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
