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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateSnapshotForeignStorageRejectsUnsupportedAuthKeys(t *testing.T) {
	foreignURI := "s3://foreign-bucket/root/snapshots/s1/metadata/manifest.json"
	tests := []struct {
		name string
		spec string
	}{
		{
			name: "role arn",
			spec: `{"extfs":{"role_arn":"arn:aws:iam::1:role/r"}}`,
		},
		{
			name: "gcp target service account",
			spec: `{"extfs":{"gcp_target_service_account":"sa@project.iam.gserviceaccount.com"}}`,
		},
		{
			name: "anonymous",
			spec: `{"extfs":{"anonymous":"true"}}`,
		},
		{
			name: "sas token",
			spec: `{"extfs":{"sas_token":"sig=secret"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateSnapshotForeignStorage(DirectionRestore, foreignURI, tt.spec)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not supported for snapshot")
		})
	}
}

func TestValidateSnapshotForeignStorageRestoreRawAKSKAllowed(t *testing.T) {
	validated, err := ValidateSnapshotForeignStorage(
		DirectionRestore,
		"s3://foreign-bucket/root/snapshots/s1/metadata/manifest.json",
		`{"extfs":{"access_key_id":"AK","access_key_value":"SK","cloud_provider":"aws","region":"us-west-2"}}`,
	)
	require.NoError(t, err)
	assert.Equal(t, "AK", validated.RawAccessKeyID)
	assert.Equal(t, "SK", validated.RawSecretKey)
}

func TestValidateSnapshotForeignStorageRestoreRejectsEmptyRawCredentialFields(t *testing.T) {
	foreignURI := "s3://foreign-bucket/root/snapshots/s1/metadata/manifest.json"
	_, err := ValidateSnapshotForeignStorage(
		DirectionRestore,
		foreignURI,
		`{"extfs":{"access_key_id":"","access_key_value":"","cloud_provider":"aws","region":"us-west-2"}}`,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be set together and non-empty")
}

func TestValidateSnapshotForeignStorageRejectsUnmappableExtfsKeys(t *testing.T) {
	foreignURI := "s3://foreign-bucket/root/snapshots/s1/metadata/manifest.json"
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
			_, err := ValidateSnapshotForeignStorage(DirectionExport, foreignURI, tt.spec)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not supported for snapshot")
		})
	}
}

func TestValidateSnapshotForeignStorageRejectsSpecWithoutExtfs(t *testing.T) {
	foreignURI := "s3://foreign-bucket/root/snapshots/s1/metadata/manifest.json"
	for _, spec := range []string{`{}`, `{"format":"parquet"}`, `{"extfs":{}}`} {
		t.Run(spec, func(t *testing.T) {
			_, err := ValidateSnapshotForeignStorage(DirectionExport, foreignURI, spec)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "external_spec.extfs is required")
		})
	}
}

func TestValidateSnapshotForeignStorageRejectsPartialRawAKSK(t *testing.T) {
	_, err := ValidateSnapshotForeignStorage(
		DirectionExport,
		"s3://foreign-bucket/root/snapshots/s1/metadata/manifest.json",
		`{"extfs":{"access_key_id":"AK","cloud_provider":"aws","region":"us-west-2"}}`,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be set together and non-empty")
}

func TestValidateSnapshotForeignStorageRejectsUseIAMFalse(t *testing.T) {
	_, err := ValidateSnapshotForeignStorage(
		DirectionExport,
		"s3://foreign-bucket/root/snapshots/s1/metadata/manifest.json",
		`{"extfs":{"use_iam":"false","cloud_provider":"aws","region":"us-west-2"}}`,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "use_iam=false is not supported")
}

func TestValidateSnapshotForeignStorageRejectsCredentialModeConflicts(t *testing.T) {
	foreignURI := "s3://foreign-bucket/root/snapshots/s1/metadata/manifest.json"
	tests := []struct {
		name string
		spec string
	}{
		{
			name: "use iam with raw",
			spec: `{"extfs":{"use_iam":"true","access_key_id":"AK","access_key_value":"SK","cloud_provider":"aws","region":"us-west-2"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateSnapshotForeignStorage(DirectionExport, foreignURI, tt.spec)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "credential modes are mutually exclusive")
		})
	}
}

func TestValidateSnapshotForeignStorageRejectsInvalidDirection(t *testing.T) {
	_, err := ValidateSnapshotForeignStorage(
		Direction(999),
		"s3://foreign-bucket/root/snapshots/s1/metadata/manifest.json",
		"",
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "direction")
}

func TestValidateSnapshotForeignStorageRestoreWithExternalSpec(t *testing.T) {
	validated, err := ValidateSnapshotForeignStorage(
		DirectionRestore,
		"s3://foreign-bucket/tenant-a/snapshots/s1/metadata/manifest.json",
		`{"extfs":{"cloud_provider":"aws","region":"us-west-2"}}`,
	)
	require.NoError(t, err)
	assert.Equal(t, DirectionRestore, validated.Direction)
	assert.Equal(t, "foreign-bucket", validated.ForeignBucket)
	assert.Equal(t, "tenant-a", validated.ForeignRoot)
	assert.Equal(t, "aws", validated.CloudProvider)
	assert.Equal(t, "us-west-2", validated.Region)
}

func TestValidateSnapshotForeignStorageExportUsesTargetRoot(t *testing.T) {
	validated, err := ValidateSnapshotForeignStorage(
		DirectionExport,
		"s3://foreign-bucket/export-root",
		`{"extfs":{"cloud_provider":"aws","region":"us-west-2"}}`,
	)
	require.NoError(t, err)
	assert.Equal(t, "foreign-bucket", validated.ForeignBucket)
	assert.Equal(t, "export-root", validated.ForeignRoot)
}

func TestValidateSnapshotForeignStorageCopySourceUsesSourceRoot(t *testing.T) {
	validated, err := ValidateSnapshotForeignStorage(
		DirectionCopySource,
		"s3://foreign-bucket/source-root/files",
		`{"extfs":{"cloud_provider":"aws","region":"us-west-2"}}`,
	)
	require.NoError(t, err)
	assert.Equal(t, DirectionCopySource, validated.Direction)
	assert.Equal(t, "foreign-bucket", validated.ForeignBucket)
	assert.Equal(t, "source-root/files", validated.ForeignRoot)
}

func TestValidateSnapshotForeignStorageObjectKeyUsesInstanceBucketPlaceholder(t *testing.T) {
	validated, err := ValidateSnapshotForeignStorage(
		DirectionRestore,
		"export-root/snapshots/1/metadata/1.json",
		"",
	)
	require.NoError(t, err)
	assert.Empty(t, validated.ForeignBucket)
	assert.Equal(t, "export-root", validated.ForeignRoot)
	assert.Equal(t, "", validated.Scheme)
}

func TestValidateSnapshotForeignStorageRestoreRejectsMissingSnapshotMetadataAnchor(t *testing.T) {
	_, err := ValidateSnapshotForeignStorage(
		DirectionRestore,
		"s3://foreign-bucket/restored/x/meta.json",
		"",
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "snapshots/{collectionID}/metadata/{snapshotID}")
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, err := ParseForeignURI(tt.uri)
			require.Error(t, err)
		})
	}
}
