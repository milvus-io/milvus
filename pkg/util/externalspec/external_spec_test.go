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

package externalspec

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseExternalSpec_Empty(t *testing.T) {
	spec, err := ParseExternalSpec("")
	require.NoError(t, err)
	assert.Equal(t, "parquet", spec.Format)
}

func TestParseExternalSpec_InvalidJSON(t *testing.T) {
	_, err := ParseExternalSpec("{bad json")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid external spec JSON")
}

func TestParseExternalSpec_DefaultFormat(t *testing.T) {
	spec, err := ParseExternalSpec(`{"columns":["a","b"]}`)
	require.NoError(t, err)
	assert.Equal(t, "parquet", spec.Format)
	assert.Equal(t, []string{"a", "b"}, spec.Columns)
}

func TestParseExternalSpec_SupportedFormats(t *testing.T) {
	for _, fmt := range []string{"parquet", "lance-table", "vortex"} {
		spec, err := ParseExternalSpec(`{"format":"` + fmt + `"}`)
		require.NoError(t, err, "format %s should be supported", fmt)
		assert.Equal(t, fmt, spec.Format)
	}
}

func TestParseExternalSpec_UnsupportedFormat(t *testing.T) {
	_, err := ParseExternalSpec(`{"format":"csv"}`)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported format")
	assert.Contains(t, err.Error(), "lance-table")
	assert.Contains(t, err.Error(), "parquet")
	assert.Contains(t, err.Error(), "vortex")
}

func TestParseExternalSpec_ValidExtfs(t *testing.T) {
	spec, err := ParseExternalSpec(`{"format":"parquet","extfs":{"region":"us-west-2","use_ssl":"true"}}`)
	require.NoError(t, err)
	assert.Equal(t, "us-west-2", spec.Extfs["region"])
	assert.Equal(t, "true", spec.Extfs["use_ssl"])
}

func TestParseExternalSpec_NotAllowedExtfs(t *testing.T) {
	_, err := ParseExternalSpec(`{"format":"parquet","extfs":{"shell_command":"rm -rf /"}}`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not allowed")
}

func TestParseExternalSpec_BooleanExtfsValidation(t *testing.T) {
	_, err := ParseExternalSpec(`{"format":"parquet","extfs":{"use_iam":"yes"}}`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be \"true\" or \"false\"")

	spec, err := ParseExternalSpec(`{"format":"parquet","extfs":{"use_iam":"true"}}`)
	require.NoError(t, err)
	assert.Equal(t, "true", spec.Extfs["use_iam"])
}

func TestParseExternalSpec_BucketExtfsAllowed(t *testing.T) {
	spec, err := ParseExternalSpec(`{"format":"parquet","extfs":{"bucket_name":"my-bucket"}}`)
	require.NoError(t, err)
	assert.Equal(t, "my-bucket", spec.Extfs["bucket_name"])
}

func TestParseExternalSpec_AddressRejected(t *testing.T) {
	_, err := ParseExternalSpec(`{"format":"parquet","extfs":{"address":"https://s3.us-west-2.amazonaws.com"}}`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not allowed")
}

func TestValidateExternalSource(t *testing.T) {
	t.Run("empty_rejected", func(t *testing.T) {
		err := ValidateExternalSource("")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty")
	})

	t.Run("relative_path_no_scheme_rejected", func(t *testing.T) {
		err := ValidateExternalSource("my-data/parquet/")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "explicit scheme")
	})

	t.Run("allowed_schemes", func(t *testing.T) {
		for _, src := range []string{
			"s3://bucket/prefix",
			"s3a://bucket/prefix",
			"aws://bucket/prefix",
			"minio://bucket/prefix",
			"oss://bucket/prefix",
			"gs://bucket/prefix",
			"gcs://bucket/prefix",
			"azure://container/prefix",
		} {
			assert.NoError(t, ValidateExternalSource(src), "should accept %s", src)
		}
	})

	t.Run("file_scheme_rejected", func(t *testing.T) {
		err := ValidateExternalSource("file:///tmp/data")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not allowed")
	})

	t.Run("http_scheme_rejected", func(t *testing.T) {
		err := ValidateExternalSource("http://169.254.169.254/metadata")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not allowed")
	})

	t.Run("userinfo_rejected", func(t *testing.T) {
		err := ValidateExternalSource("s3://ak:sk@bucket/prefix")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must not embed credentials")
	})

	t.Run("empty_host_rejected", func(t *testing.T) {
		err := ValidateExternalSource("s3:///bucket/path")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-empty host")
	})

	t.Run("invalid_url_rejected", func(t *testing.T) {
		err := ValidateExternalSource("s3://bad host/path")
		require.Error(t, err)
	})
}

// minimalValidSpec returns an extfs block that ValidateExtfsComplete accepts
// for AWS-family schemes: AK/SK pair + region.
func minimalValidSpec() string {
	return `{"format":"parquet","extfs":{"access_key_id":"AK","access_key_value":"SK","region":"us-east-1","cloud_provider":"aws"}}`
}

func TestValidateSourceAndSpec(t *testing.T) {
	t.Run("both_valid", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix", minimalValidSpec())
		assert.NoError(t, err)
	})

	t.Run("invalid_source", func(t *testing.T) {
		err := ValidateSourceAndSpec("file:///tmp/x", minimalValidSpec())
		require.Error(t, err)
	})

	t.Run("invalid_spec_redacts", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix", `{bad json`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "<redacted>")
	})

	t.Run("missing_credentials_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix", `{"format":"parquet"}`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "<redacted>")
	})

	t.Run("missing_region_for_aws_scheme_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix",
			`{"format":"parquet","extfs":{"access_key_id":"AK","access_key_value":"SK","cloud_provider":"aws"}}`)
		require.Error(t, err)
	})

	t.Run("missing_cloud_provider_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix",
			`{"format":"parquet","extfs":{"access_key_id":"AK","access_key_value":"SK","region":"us-east-1"}}`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cloud_provider is required")
	})

	t.Run("invalid_cloud_provider_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix",
			`{"format":"parquet","extfs":{"access_key_id":"AK","access_key_value":"SK","region":"us-east-1","cloud_provider":"unknown"}}`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is not supported")
	})

	t.Run("minio_cloud_provider_milvus_form", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://localhost:9000/mybucket/path",
			`{"format":"parquet","extfs":{"access_key_id":"AK","access_key_value":"SK","region":"us-east-1","cloud_provider":"minio"}}`)
		assert.NoError(t, err)
	})

	t.Run("gcs_scheme_does_not_require_region", func(t *testing.T) {
		err := ValidateSourceAndSpec("gs://bucket/prefix",
			`{"format":"parquet","extfs":{"access_key_id":"AK","access_key_value":"SK","cloud_provider":"gcp"}}`)
		assert.NoError(t, err)
	})

	t.Run("role_arn_mode", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix",
			`{"format":"parquet","extfs":{"role_arn":"arn:aws:iam::1:role/r","region":"us-east-1","cloud_provider":"aws"}}`)
		assert.NoError(t, err)
	})

	t.Run("use_iam_alone_mode", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix",
			`{"format":"parquet","extfs":{"use_iam":"true","region":"us-east-1","cloud_provider":"aws"}}`)
		assert.NoError(t, err)
	})

	t.Run("anonymous_mode", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix",
			`{"format":"parquet","extfs":{"anonymous":"true","region":"us-east-1","cloud_provider":"aws"}}`)
		assert.NoError(t, err)
	})

	t.Run("ak_without_sk_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix",
			`{"format":"parquet","extfs":{"access_key_id":"AK","region":"us-east-1","cloud_provider":"aws"}}`)
		require.Error(t, err)
	})

	t.Run("anonymous_with_aksk_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix",
			`{"format":"parquet","extfs":{"anonymous":"true","access_key_id":"AK","access_key_value":"SK","region":"us-east-1","cloud_provider":"aws"}}`)
		require.Error(t, err)
	})

	t.Run("multiple_modes_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix",
			`{"format":"parquet","extfs":{"role_arn":"arn:aws:iam::1:role/r","access_key_id":"AK","access_key_value":"SK","region":"us-east-1","cloud_provider":"aws"}}`)
		require.Error(t, err)
	})

	t.Run("gcp_impersonation_mode", func(t *testing.T) {
		err := ValidateSourceAndSpec("gs://bucket/prefix",
			`{"format":"parquet","extfs":{"gcp_target_service_account":"sa@proj.iam.gserviceaccount.com","cloud_provider":"gcp"}}`)
		assert.NoError(t, err)
	})

	t.Run("gcp_impersonation_with_gcs_scheme", func(t *testing.T) {
		err := ValidateSourceAndSpec("gcs://bucket/prefix",
			`{"format":"parquet","extfs":{"gcp_target_service_account":"sa@proj.iam.gserviceaccount.com","cloud_provider":"gcp"}}`)
		assert.NoError(t, err)
	})

	t.Run("gcp_impersonation_on_aws_scheme_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix",
			`{"format":"parquet","extfs":{"gcp_target_service_account":"sa@proj.iam.gserviceaccount.com","region":"us-east-1","cloud_provider":"aws"}}`)
		require.Error(t, err)
	})

	t.Run("gcp_impersonation_malformed_email_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("gs://bucket/prefix",
			`{"format":"parquet","extfs":{"gcp_target_service_account":"not-an-email","cloud_provider":"gcp"}}`)
		require.Error(t, err)
	})

	t.Run("gcp_impersonation_missing_at_sign_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("gs://bucket/prefix",
			`{"format":"parquet","extfs":{"gcp_target_service_account":"saproj.iam.gserviceaccount.com","cloud_provider":"gcp"}}`)
		require.Error(t, err)
	})

	t.Run("gcp_impersonation_with_aksk_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("gs://bucket/prefix",
			`{"format":"parquet","extfs":{"gcp_target_service_account":"sa@proj.iam.gserviceaccount.com","access_key_id":"AK","access_key_value":"SK","cloud_provider":"gcp"}}`)
		require.Error(t, err)
	})

	t.Run("gcp_impersonation_with_anonymous_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("gs://bucket/prefix",
			`{"format":"parquet","extfs":{"gcp_target_service_account":"sa@proj.iam.gserviceaccount.com","anonymous":"true","cloud_provider":"gcp"}}`)
		require.Error(t, err)
	})
}

func TestRedactExternalSpec(t *testing.T) {
	t.Run("empty_returns_empty", func(t *testing.T) {
		assert.Equal(t, "", RedactExternalSpec(""))
	})

	t.Run("invalid_json_returns_placeholder", func(t *testing.T) {
		assert.Equal(t, "<invalid spec>", RedactExternalSpec("{not json"))
	})

	t.Run("secrets_masked", func(t *testing.T) {
		out := RedactExternalSpec(`{"format":"parquet","extfs":{"access_key_id":"AKIA","access_key_value":"SEC","region":"us"}}`)
		assert.Contains(t, out, `"access_key_id":"***"`)
		assert.Contains(t, out, `"access_key_value":"***"`)
		assert.Contains(t, out, `"region":"us"`)
		assert.NotContains(t, out, "AKIA")
		assert.NotContains(t, out, "SEC")
	})

	t.Run("empty_secret_values_untouched", func(t *testing.T) {
		out := RedactExternalSpec(`{"format":"parquet","extfs":{"access_key_id":""}}`)
		assert.Contains(t, out, `"access_key_id":""`)
	})
}

func TestParseExternalSpec_ArnKeys(t *testing.T) {
	t.Run("all_arn_keys_accepted", func(t *testing.T) {
		spec, err := ParseExternalSpec(`{
			"format":"parquet",
			"extfs":{
				"role_arn":"arn:aws:iam::123456789012:role/test-role",
				"session_name":"my-session",
				"external_id":"ext-123",
				"load_frequency":"900"
			}
		}`)
		require.NoError(t, err)
		assert.Equal(t, "arn:aws:iam::123456789012:role/test-role", spec.Extfs["role_arn"])
		assert.Equal(t, "my-session", spec.Extfs["session_name"])
		assert.Equal(t, "ext-123", spec.Extfs["external_id"])
		assert.Equal(t, "900", spec.Extfs["load_frequency"])
	})

	t.Run("role_arn_with_existing_keys", func(t *testing.T) {
		spec, err := ParseExternalSpec(`{
			"format":"parquet",
			"extfs":{
				"region":"us-west-2",
				"cloud_provider":"aws",
				"role_arn":"arn:aws:iam::123456789012:role/cross-account"
			}
		}`)
		require.NoError(t, err)
		assert.Equal(t, "us-west-2", spec.Extfs["region"])
		assert.Equal(t, "arn:aws:iam::123456789012:role/cross-account", spec.Extfs["role_arn"])
	})

	t.Run("arn_keys_preserved_in_extfs_map", func(t *testing.T) {
		// extfs kv parsing preserves role_arn / load_frequency; downstream
		// extfs flattening + C++ InjectExternalSpecProperties consume these.
		spec := &ExternalSpec{
			Extfs: map[string]string{
				"role_arn":       "arn:aws:iam::123456789012:role/test",
				"load_frequency": "3600",
			},
		}
		assert.Equal(t, "arn:aws:iam::123456789012:role/test", spec.Extfs["role_arn"])
		assert.Equal(t, "3600", spec.Extfs["load_frequency"])
	})
}

func TestValidateExtfsComplete_Azure(t *testing.T) {
	t.Run("azure_shared_key", func(t *testing.T) {
		err := ValidateExtfsComplete("azure://core.windows.net/container/path", map[string]string{
			"access_key_id":    "mystorageacct",
			"access_key_value": "base64key",
			"cloud_provider":   "azure",
		})
		assert.NoError(t, err)
	})

	t.Run("azure_workload_identity", func(t *testing.T) {
		err := ValidateExtfsComplete("azure://core.windows.net/container/path", map[string]string{
			"use_iam":        "true",
			"cloud_provider": "azure",
		})
		assert.NoError(t, err)
	})

	t.Run("azure_no_region_required", func(t *testing.T) {
		err := ValidateExtfsComplete("azure://core.windows.net/container/path", map[string]string{
			"access_key_id":    "mystorageacct",
			"access_key_value": "base64key",
			"cloud_provider":   "azure",
		})
		assert.NoError(t, err)
	})

	t.Run("azure_scheme_with_gcp_provider_rejected", func(t *testing.T) {
		err := ValidateExtfsComplete("azure://core.windows.net/container/path", map[string]string{
			"access_key_id":    "AK",
			"access_key_value": "SK",
			"cloud_provider":   "gcp",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "scheme=azure requires extfs.cloud_provider")
	})

	t.Run("azure_provider_with_s3_scheme_rejected", func(t *testing.T) {
		err := ValidateExtfsComplete("s3://bucket/path", map[string]string{
			"access_key_id":    "AK",
			"access_key_value": "SK",
			"region":           "us-east-1",
			"cloud_provider":   "azure",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cloud_provider=azure requires scheme=azure")
	})

	t.Run("azure_with_gcp_target_service_account_rejected", func(t *testing.T) {
		err := ValidateExtfsComplete("azure://core.windows.net/container/path", map[string]string{
			"gcp_target_service_account": "sa@proj.iam.gserviceaccount.com",
			"cloud_provider":             "azure",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "only valid for GCP")
	})
}

// TestParseExternalSpec_SnapshotIDStringEncoded covers the JS/JSON-safe
// string form of snapshot_id (Iceberg int64 IDs exceed JS Number.MAX_SAFE_INTEGER).
// Bare-number form is rejected by `,string` json tag — clients must quote the value.
func TestParseExternalSpec_SnapshotIDStringEncoded(t *testing.T) {
	t.Run("string_form_accepted", func(t *testing.T) {
		spec, err := ParseExternalSpec(`{"format":"iceberg-table","snapshot_id":"5320540205222981137"}`)
		require.NoError(t, err)
		require.NotNil(t, spec.SnapshotID)
		assert.Equal(t, int64(5320540205222981137), *spec.SnapshotID)
	})

	t.Run("bare_number_rejected", func(t *testing.T) {
		_, err := ParseExternalSpec(`{"format":"iceberg-table","snapshot_id":5320540205222981137}`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid external spec JSON")
	})

	t.Run("non_numeric_string_rejected", func(t *testing.T) {
		_, err := ParseExternalSpec(`{"format":"iceberg-table","snapshot_id":"abc"}`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid external spec JSON")
	})

	t.Run("omitted_yields_nil", func(t *testing.T) {
		spec, err := ParseExternalSpec(`{"format":"iceberg-table"}`)
		require.NoError(t, err)
		assert.Nil(t, spec.SnapshotID)
	})
}
