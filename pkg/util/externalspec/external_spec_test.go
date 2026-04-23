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

func TestParseExternalSpec_AddressAndBucketExtfsAllowed(t *testing.T) {
	spec, err := ParseExternalSpec(`{"format":"parquet","extfs":{"address":"https://s3.us-west-2.amazonaws.com","bucket_name":"my-bucket"}}`)
	require.NoError(t, err)
	assert.Equal(t, "https://s3.us-west-2.amazonaws.com", spec.Extfs["address"])
	assert.Equal(t, "my-bucket", spec.Extfs["bucket_name"])
}

func TestBuildExtfsOverrides(t *testing.T) {
	t.Run("empty_extfs_returns_nil", func(t *testing.T) {
		spec := &ExternalSpec{Format: "parquet"}
		assert.Nil(t, spec.BuildExtfsOverrides("extfs.42."))
	})

	t.Run("prepends_prefix_to_every_key", func(t *testing.T) {
		spec := &ExternalSpec{
			Format: "parquet",
			Extfs: map[string]string{
				"region":  "us-west-2",
				"use_ssl": "true",
			},
		}
		out := spec.BuildExtfsOverrides("extfs.42.")
		assert.Equal(t, "us-west-2", out["extfs.42.region"])
		assert.Equal(t, "true", out["extfs.42.use_ssl"])
		assert.Len(t, out, 2)
	})
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
	return `{"format":"parquet","extfs":{"access_key_id":"AK","access_key_value":"SK","region":"us-east-1"}}`
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
			`{"format":"parquet","extfs":{"access_key_id":"AK","access_key_value":"SK"}}`)
		require.Error(t, err)
	})

	t.Run("gcs_scheme_does_not_require_region", func(t *testing.T) {
		err := ValidateSourceAndSpec("gs://bucket/prefix",
			`{"format":"parquet","extfs":{"access_key_id":"AK","access_key_value":"SK"}}`)
		assert.NoError(t, err)
	})

	t.Run("role_arn_mode", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix",
			`{"format":"parquet","extfs":{"role_arn":"arn:aws:iam::1:role/r","region":"us-east-1"}}`)
		assert.NoError(t, err)
	})

	t.Run("use_iam_alone_mode", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix",
			`{"format":"parquet","extfs":{"use_iam":"true","region":"us-east-1"}}`)
		assert.NoError(t, err)
	})

	t.Run("anonymous_mode", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix",
			`{"format":"parquet","extfs":{"anonymous":"true","region":"us-east-1"}}`)
		assert.NoError(t, err)
	})

	t.Run("ak_without_sk_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix",
			`{"format":"parquet","extfs":{"access_key_id":"AK","region":"us-east-1"}}`)
		require.Error(t, err)
	})

	t.Run("anonymous_with_aksk_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix",
			`{"format":"parquet","extfs":{"anonymous":"true","access_key_id":"AK","access_key_value":"SK","region":"us-east-1"}}`)
		require.Error(t, err)
	})

	t.Run("multiple_modes_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix",
			`{"format":"parquet","extfs":{"role_arn":"arn:aws:iam::1:role/r","access_key_id":"AK","access_key_value":"SK","region":"us-east-1"}}`)
		require.Error(t, err)
	})

	t.Run("gcp_impersonation_mode", func(t *testing.T) {
		err := ValidateSourceAndSpec("gs://bucket/prefix",
			`{"format":"parquet","extfs":{"gcp_target_service_account":"sa@proj.iam.gserviceaccount.com"}}`)
		assert.NoError(t, err)
	})

	t.Run("gcp_impersonation_with_gcs_scheme", func(t *testing.T) {
		err := ValidateSourceAndSpec("gcs://bucket/prefix",
			`{"format":"parquet","extfs":{"gcp_target_service_account":"sa@proj.iam.gserviceaccount.com"}}`)
		assert.NoError(t, err)
	})

	t.Run("gcp_impersonation_on_aws_scheme_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix",
			`{"format":"parquet","extfs":{"gcp_target_service_account":"sa@proj.iam.gserviceaccount.com","region":"us-east-1"}}`)
		require.Error(t, err)
	})

	t.Run("gcp_impersonation_malformed_email_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("gs://bucket/prefix",
			`{"format":"parquet","extfs":{"gcp_target_service_account":"not-an-email"}}`)
		require.Error(t, err)
	})

	t.Run("gcp_impersonation_missing_at_sign_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("gs://bucket/prefix",
			`{"format":"parquet","extfs":{"gcp_target_service_account":"saproj.iam.gserviceaccount.com"}}`)
		require.Error(t, err)
	})

	t.Run("gcp_impersonation_with_aksk_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("gs://bucket/prefix",
			`{"format":"parquet","extfs":{"gcp_target_service_account":"sa@proj.iam.gserviceaccount.com","access_key_id":"AK","access_key_value":"SK"}}`)
		require.Error(t, err)
	})

	t.Run("gcp_impersonation_with_anonymous_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("gs://bucket/prefix",
			`{"format":"parquet","extfs":{"gcp_target_service_account":"sa@proj.iam.gserviceaccount.com","anonymous":"true"}}`)
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

func TestBuildFormatProperties(t *testing.T) {
	t.Run("non_iceberg_empty", func(t *testing.T) {
		snap := int64(42)
		spec := &ExternalSpec{Format: FormatParquet, SnapshotID: &snap}
		assert.Empty(t, spec.BuildFormatProperties())
	})

	t.Run("iceberg_without_snapshot_empty", func(t *testing.T) {
		spec := &ExternalSpec{Format: FormatIcebergTable}
		assert.Empty(t, spec.BuildFormatProperties())
	})

	t.Run("iceberg_with_snapshot", func(t *testing.T) {
		snap := int64(12345)
		spec := &ExternalSpec{Format: FormatIcebergTable, SnapshotID: &snap}
		props := spec.BuildFormatProperties()
		assert.Equal(t, "12345", props[PropertyIcebergSnapshotID])
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

	t.Run("arn_keys_flow_through_extfs_overrides", func(t *testing.T) {
		spec := &ExternalSpec{
			Extfs: map[string]string{
				"role_arn":       "arn:aws:iam::123456789012:role/test",
				"load_frequency": "3600",
			},
		}
		out := spec.BuildExtfsOverrides("extfs.42.")
		assert.Equal(t, "arn:aws:iam::123456789012:role/test", out["extfs.42.role_arn"])
		assert.Equal(t, "3600", out["extfs.42.load_frequency"])
	})
}

func TestNormalizeExternalSource(t *testing.T) {
	specWithAddr := `{"format":"parquet","extfs":{"address":"https://s3.us-west-2.amazonaws.com"}}`
	specBareAddr := `{"format":"parquet","extfs":{"address":"minio:9000"}}`
	specNoAddr := `{"format":"parquet","extfs":{"region":"us-west-2"}}`
	specEmpty := ""

	cases := []struct {
		name     string
		source   string
		spec     string
		expected string
	}{
		{"empty_source", "", specWithAddr, ""},
		{"empty_spec", "s3://bucket/key", specEmpty, "s3://bucket/key"},
		{"spec_without_address", "s3://bucket/key", specNoAddr, "s3://bucket/key"},
		{"invalid_spec_json", "s3://bucket/key", `{bad`, "s3://bucket/key"},
		{
			"aws_style_rewritten",
			"s3://my-bucket/data",
			specWithAddr,
			"s3://s3.us-west-2.amazonaws.com/my-bucket/data",
		},
		{
			"aws_style_bare_endpoint",
			"s3://my-bucket/data",
			specBareAddr,
			"s3://minio:9000/my-bucket/data",
		},
		{
			"aws_style_bucket_only",
			"s3://my-bucket",
			specWithAddr,
			"s3://s3.us-west-2.amazonaws.com/my-bucket",
		},
		{
			"aws_style_trailing_slash",
			"s3://my-bucket/",
			specWithAddr,
			"s3://s3.us-west-2.amazonaws.com/my-bucket/",
		},
		{
			"empty_host_unchanged",
			"s3:///bucket/key",
			specWithAddr,
			"s3:///bucket/key",
		},
		{
			"relative_unchanged",
			"path/to/data",
			specWithAddr,
			"path/to/data",
		},
		{
			"unsafe_host_unchanged",
			"s3://[::1]/key",
			specWithAddr,
			"s3://[::1]/key",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, NormalizeExternalSource(tc.source, tc.spec))
		})
	}
}

func TestStripURLScheme_Pkg(t *testing.T) {
	assert.Equal(t, "host:9000", stripURLScheme("http://host:9000"))
	assert.Equal(t, "s3.amazonaws.com", stripURLScheme("https://s3.amazonaws.com"))
	assert.Equal(t, "minio:9000", stripURLScheme("minio:9000"))
	assert.Equal(t, "s3://x", stripURLScheme("s3://x"))
}

func TestIsSafeURIHost_Pkg(t *testing.T) {
	assert.True(t, isSafeURIHost("host"))
	assert.True(t, isSafeURIHost("host:9000"))
	assert.False(t, isSafeURIHost(""))
	assert.False(t, isSafeURIHost("user@host"))
	assert.False(t, isSafeURIHost("[::1]"))
	assert.False(t, isSafeURIHost("host/path"))
}

func TestDeriveEndpoint(t *testing.T) {
	cases := []struct {
		name     string
		provider string
		region   string
		want     string
		ok       bool
	}{
		{"aws_std", "aws", "us-west-2", "https://s3.us-west-2.amazonaws.com", true},
		{"aws_china", "aws", "cn-north-1", "https://s3.cn-north-1.amazonaws.com.cn", true},
		{"aws_region_missing", "aws", "", "", false},
		{"aws_case_insensitive", "AWS", "us-east-1", "https://s3.us-east-1.amazonaws.com", true},
		{"gcp_any_region", "gcp", "us-central1", "https://storage.googleapis.com", true},
		{"gcp_no_region", "gcp", "", "https://storage.googleapis.com", true},
		{"aliyun_hangzhou", "aliyun", "cn-hangzhou", "https://oss-cn-hangzhou.aliyuncs.com", true},
		{"aliyun_no_region", "aliyun", "", "", false},
		{"tencent_guangzhou", "tencent", "ap-guangzhou", "https://cos.ap-guangzhou.myqcloud.com", true},
		{"huawei_north4", "huawei", "cn-north-4", "https://obs.cn-north-4.myhuaweicloud.com", true},
		{"azure_not_derivable", "azure", "eastus", "", false},
		{"unknown_provider", "foo", "us-west-2", "", false},
		{"empty_provider", "", "us-west-2", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := DeriveEndpoint(tc.provider, tc.region)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestEffectiveAddressFromExtfs(t *testing.T) {
	// Tier 1 wins over Tier 2 (explicit address trumps derivation).
	assert.Equal(t, "https://custom.example.com",
		EffectiveAddressFromExtfs(map[string]string{
			"address":        "https://custom.example.com",
			"cloud_provider": "aws",
			"region":         "us-west-2",
		}))
	// Tier 2: derivation fills in when address is absent.
	assert.Equal(t, "https://s3.us-west-2.amazonaws.com",
		EffectiveAddressFromExtfs(map[string]string{
			"cloud_provider": "aws",
			"region":         "us-west-2",
		}))
	// Empty address falls through to Tier 2.
	assert.Equal(t, "https://storage.googleapis.com",
		EffectiveAddressFromExtfs(map[string]string{
			"address":        "",
			"cloud_provider": "gcp",
		}))
	// Tier 3 (no effective endpoint).
	assert.Equal(t, "",
		EffectiveAddressFromExtfs(map[string]string{
			"cloud_provider": "azure", // not derivable
			"region":         "eastus",
		}))
	assert.Equal(t, "", EffectiveAddressFromExtfs(nil))
	assert.Equal(t, "", EffectiveAddressFromExtfs(map[string]string{}))
}

func TestNormalizeExternalSource_Tier2(t *testing.T) {
	// Tier 2: no address, provider+region present, URI host is bucket.
	specTier2 := `{"format":"parquet","extfs":{"cloud_provider":"aws","region":"us-west-2"}}`
	assert.Equal(t,
		"s3://s3.us-west-2.amazonaws.com/my-bucket/key",
		NormalizeExternalSource("s3://my-bucket/key", specTier2))

	// Tier 2 equality guard: URI host already matches derived endpoint →
	// Milvus form, leave unchanged.
	assert.Equal(t,
		"s3://s3.us-west-2.amazonaws.com/real-bucket/data",
		NormalizeExternalSource("s3://s3.us-west-2.amazonaws.com/real-bucket/data", specTier2))

	// Tier 1 wins over Tier 2.
	specBoth := `{"format":"parquet","extfs":{"address":"https://custom.example.com","cloud_provider":"aws","region":"us-west-2"}}`
	assert.Equal(t,
		"s3://custom.example.com/my-bucket/key",
		NormalizeExternalSource("s3://my-bucket/key", specBoth))

	// GCP Tier 2: region-less derivation.
	specGCP := `{"format":"parquet","extfs":{"cloud_provider":"gcp"}}`
	assert.Equal(t,
		"gs://storage.googleapis.com/bucket/data",
		NormalizeExternalSource("gs://bucket/data", specGCP))

	// Azure: not derivable, not rewritten.
	specAzure := `{"format":"parquet","extfs":{"cloud_provider":"azure","region":"eastus"}}`
	assert.Equal(t,
		"s3://my-bucket/key",
		NormalizeExternalSource("s3://my-bucket/key", specAzure))
}
