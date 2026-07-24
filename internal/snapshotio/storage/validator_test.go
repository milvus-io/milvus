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
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
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
	if _, _, err := applySnapshotExternalSpecToConfig(cfg, objectstorage.NewDefaultConfig(), scheme, endpoint, externalSpec); err != nil {
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
	instanceCfg := objectstorage.NewDefaultConfig()
	instanceCfg.CloudProvider = objectstorage.CloudProviderGCPNative

	_, _, err := applySnapshotExternalSpecToConfig(
		cfg,
		instanceCfg,
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
	instanceCfg := objectstorage.NewDefaultConfig()
	instanceCfg.CloudProvider = objectstorage.CloudProviderAzure

	_, _, err := applySnapshotExternalSpecToConfig(
		cfg,
		instanceCfg,
		"azure",
		"",
		`{"extfs":{"cloud_provider":"azure","region":"public","use_iam":"true"}}`,
	)

	require.NoError(t, err)
	assert.True(t, cfg.UseIAM)
	assert.Equal(t, "azure-account", cfg.AccessKeyID)
	assert.Empty(t, cfg.SecretAccessKeyID)
	assert.Empty(t, cfg.GcpCredentialJSON)
}

func TestApplySnapshotExternalSpecIgnoresRequestSSLCACert(t *testing.T) {
	instanceCfg := objectstorage.NewDefaultConfig()
	instanceCfg.CloudProvider = objectstorage.CloudProviderAWS
	instanceCfg.SslCACert = "instance-ca.pem"
	cfg := cloneObjectStorageConfig(instanceCfg)

	_, _, err := applySnapshotExternalSpecToConfig(
		cfg,
		instanceCfg,
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
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
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
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
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
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
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
