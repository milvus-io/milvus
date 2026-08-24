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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	for _, fmt := range []string{"parquet", "lance-table", "vortex", "iceberg-table", "milvus-table"} {
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
	spec, err := ParseExternalSpec(`{"format":"parquet","extfs":{"region":"us-west-2","use_ssl":"true","endpoint_url":"https://s3.example.com"}}`)
	require.NoError(t, err)
	assert.Equal(t, "us-west-2", spec.Extfs["region"])
	assert.Equal(t, "true", spec.Extfs["use_ssl"])
	assert.Equal(t, "https://s3.example.com", spec.Extfs["endpoint_url"])
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
			"minio://localhost:9000/bucket/prefix",
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

	t.Run("minio_empty_endpoint_rejected", func(t *testing.T) {
		err := ValidateExternalSource("minio:///bucket/path")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-empty host")
	})

	t.Run("invalid_url_rejected", func(t *testing.T) {
		err := ValidateExternalSource("s3://bad host/path")
		require.Error(t, err)
	})
}

func TestRedactExternalSource(t *testing.T) {
	testCases := []struct {
		name   string
		source string
		want   string
	}{
		{name: "empty", source: "", want: ""},
		{name: "normal URI", source: "s3://bucket/path", want: "s3://bucket/path"},
		{name: "userinfo", source: "s3://ACCESS_SENTINEL:SECRET_SENTINEL@bucket/path", want: "<redacted>"},
		{name: "query", source: "s3://bucket/path?token=QUERY_SECRET_SENTINEL", want: "<redacted>"},
		{name: "fragment", source: "s3://bucket/path#FRAGMENT_SECRET_SENTINEL", want: "<redacted>"},
		{name: "malformed", source: "s3://MALFORMED_SECRET_SENTINEL bad/path", want: "<redacted>"},
		{name: "opaque URI", source: "s3:ACCESS_SENTINEL:SECRET_SENTINEL@bucket/path", want: "<redacted>"},
		{name: "scheme-less URI", source: "ACCESS_SENTINEL:SECRET_SENTINEL@bucket/path", want: "<redacted>"},
		{name: "host-less URI", source: "s3:///ACCESS_SENTINEL/SECRET_SENTINEL", want: "<redacted>"},
		{name: "unsupported scheme", source: "ftp://ACCESS_SENTINEL:" + "SECRET_SENTINEL@host/path", want: "<redacted>"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.want, RedactExternalSource(testCase.source))
		})
	}
}

// minimalValidSpec returns an extfs block that ValidateExtfsComplete accepts
// for AWS-family schemes: AK/SK pair + region.
func minimalValidSpec() string {
	return `{"format":"parquet","extfs":{"access_key_id":"AK","access_key_value":"SK","region":"us-east-1","cloud_provider":"aws"}}`
}

func TestValidateSourceAndSpec(t *testing.T) {
	requireParameterInvalid := func(t *testing.T, err error) {
		t.Helper()
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
	}

	t.Run("both_valid", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix", minimalValidSpec())
		assert.NoError(t, err)
	})

	t.Run("invalid_source", func(t *testing.T) {
		err := ValidateSourceAndSpec("file:///tmp/x", minimalValidSpec())
		requireParameterInvalid(t, err)
	})

	t.Run("source_credentials_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://SOURCE_ACCESS_ERROR_SENTINEL:SOURCE_SECRET_ERROR_SENTINEL@bucket/prefix", minimalValidSpec())
		requireParameterInvalid(t, err)
		assert.Contains(t, err.Error(), "external_source is invalid")
		assert.Contains(t, err.Error(), "must not embed credentials")
	})

	t.Run("malformed_source_redacts_parse_error", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://SOURCE_PARSE_ERROR_SENTINEL bad/prefix", minimalValidSpec())
		requireParameterInvalid(t, err)
		assert.NotContains(t, err.Error(), "SOURCE_PARSE_ERROR_SENTINEL")
		assert.Contains(t, err.Error(), "external_source is invalid")
		assert.Contains(t, err.Error(), "external_source is not a valid URL")
	})

	t.Run("invalid_spec_includes_parse_error", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix", `{bad json`)
		requireParameterInvalid(t, err)
		assert.Contains(t, err.Error(), "external_spec is invalid")
		assert.Contains(t, err.Error(), "invalid external spec JSON")
	})

	t.Run("invalid_format_includes_validation_error", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix", `{"format":"FORMAT_VALUE_SECRET_SENTINEL"}`)
		requireParameterInvalid(t, err)
		assert.Contains(t, err.Error(), "FORMAT_VALUE_SECRET_SENTINEL")
		assert.Contains(t, err.Error(), "external_spec is invalid")
		assert.Contains(t, err.Error(), "unsupported format")
	})

	t.Run("invalid_extfs_value_includes_validation_error", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix", `{"format":"parquet","extfs":{"access_key_id":"AK","access_key_value":"SK","region":"us-east-1","cloud_provider":"CLOUD_PROVIDER_SECRET_SENTINEL"}}`)
		requireParameterInvalid(t, err)
		assert.Contains(t, err.Error(), "CLOUD_PROVIDER_SECRET_SENTINEL")
		assert.Contains(t, err.Error(), "cloud_provider_secret_sentinel")
		assert.Contains(t, err.Error(), "external_spec is invalid")
		assert.Contains(t, err.Error(), "must use the canonical lowercase value")
	})

	t.Run("missing_credentials_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix", `{"format":"parquet"}`)
		require.ErrorIs(t, err, merr.ErrParameterMissing)
		assert.Contains(t, err.Error(), "external_spec is invalid")
		assert.Contains(t, err.Error(), "cloud_provider is required")
	})

	t.Run("missing_region_for_aws_scheme_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix",
			`{"format":"parquet","extfs":{"access_key_id":"AK","access_key_value":"SK","cloud_provider":"aws"}}`)
		require.ErrorIs(t, err, merr.ErrParameterMissing)
	})

	t.Run("missing_cloud_provider_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix",
			`{"format":"parquet","extfs":{"access_key_id":"AK","access_key_value":"SK","region":"us-east-1"}}`)
		require.ErrorIs(t, err, merr.ErrParameterMissing)
		assert.Contains(t, err.Error(), "external_spec is invalid")
	})

	t.Run("invalid_cloud_provider_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix",
			`{"format":"parquet","extfs":{"access_key_id":"AK","access_key_value":"SK","region":"us-east-1","cloud_provider":"unknown"}}`)
		requireParameterInvalid(t, err)
		assert.Contains(t, err.Error(), "external_spec is invalid")
	})

	t.Run("minio_cloud_provider_milvus_form", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://localhost:9000/mybucket/path",
			`{"format":"parquet","extfs":{"access_key_id":"AK","access_key_value":"SK","region":"us-east-1","cloud_provider":"minio"}}`)
		assert.NoError(t, err)
	})

	t.Run("minio_scheme_selects_minio_validation_semantics", func(t *testing.T) {
		err := ValidateSourceAndSpec("minio://localhost:9000/mybucket/path",
			`{"format":"parquet","extfs":{"access_key_id":"AK","access_key_value":"SK"}}`)
		assert.NoError(t, err)
	})

	t.Run("minio_scheme_rejects_non_minio_cloud_provider", func(t *testing.T) {
		err := ValidateSourceAndSpec("minio://localhost:9000/mybucket/path",
			`{"format":"parquet","extfs":{"access_key_id":"AK","access_key_value":"SK","region":"us-east-1","cloud_provider":"aws"}}`)
		requireParameterInvalid(t, err)
		assert.Contains(t, err.Error(), "external_spec is invalid")
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
		require.ErrorIs(t, err, merr.ErrParameterMissing)
	})

	t.Run("anonymous_with_aksk_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix",
			`{"format":"parquet","extfs":{"anonymous":"true","access_key_id":"AK","access_key_value":"SK","region":"us-east-1","cloud_provider":"aws"}}`)
		requireParameterInvalid(t, err)
	})

	t.Run("multiple_modes_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix",
			`{"format":"parquet","extfs":{"role_arn":"arn:aws:iam::1:role/r","access_key_id":"AK","access_key_value":"SK","region":"us-east-1","cloud_provider":"aws"}}`)
		requireParameterInvalid(t, err)
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
		requireParameterInvalid(t, err)
	})

	t.Run("gcp_impersonation_malformed_email_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("gs://bucket/prefix",
			`{"format":"parquet","extfs":{"gcp_target_service_account":"not-an-email","cloud_provider":"gcp"}}`)
		requireParameterInvalid(t, err)
	})

	t.Run("gcp_impersonation_missing_at_sign_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("gs://bucket/prefix",
			`{"format":"parquet","extfs":{"gcp_target_service_account":"saproj.iam.gserviceaccount.com","cloud_provider":"gcp"}}`)
		requireParameterInvalid(t, err)
	})

	t.Run("gcp_impersonation_with_aksk_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("gs://bucket/prefix",
			`{"format":"parquet","extfs":{"gcp_target_service_account":"sa@proj.iam.gserviceaccount.com","access_key_id":"AK","access_key_value":"SK","cloud_provider":"gcp"}}`)
		requireParameterInvalid(t, err)
	})

	t.Run("gcp_impersonation_with_anonymous_rejected", func(t *testing.T) {
		err := ValidateSourceAndSpec("gs://bucket/prefix",
			`{"format":"parquet","extfs":{"gcp_target_service_account":"sa@proj.iam.gserviceaccount.com","anonymous":"true","cloud_provider":"gcp"}}`)
		requireParameterInvalid(t, err)
	})
}

func TestValidateEndpointURL(t *testing.T) {
	validExtfs := func(endpoint string) map[string]string {
		return map[string]string{
			ExtfsKeyAccessKeyID:    "AK",
			ExtfsKeyAccessKeyValue: "SK",
			ExtfsKeyCloudProvider:  CloudProviderMinIO,
			ExtfsKeyRegion:         "us-east-1",
			ExtfsKeyEndpointURL:    endpoint,
		}
	}

	for _, endpoint := range []string{
		"http://rook-ceph-rgw.storage.svc:80",
		"https://s3.example.com",
		"http://127.0.0.1:9000/",
	} {
		t.Run("valid_"+endpoint, func(t *testing.T) {
			assert.NoError(t, ValidateExtfsComplete("s3://my-bucket/object", validExtfs(endpoint)))
		})
	}

	t.Run("matching_use_ssl", func(t *testing.T) {
		extfs := validExtfs("https://s3.example.com")
		extfs[ExtfsKeyUseSSL] = "true"
		assert.NoError(t, ValidateExtfsComplete("s3://my-bucket/object", extfs))
	})

	t.Run("cloud_provider_optional", func(t *testing.T) {
		extfs := validExtfs("http://rook-ceph-rgw.storage.svc:80")
		delete(extfs, ExtfsKeyCloudProvider)
		assert.NoError(t, ValidateExtfsComplete("s3://my-bucket/object", extfs))
	})

	t.Run("issue_minimal_configuration", func(t *testing.T) {
		err := ValidateSourceAndSpec(
			"s3://iceberg-warehouse/lakehouse/sec/rag_chunks/metadata/00001.json",
			`{"format":"iceberg-table","snapshot_id":"517536919083875277","extfs":{"access_key_id":"AK","access_key_value":"SK","endpoint_url":"http://rook-ceph-rgw.storage.svc:80","region":"us-east-1","use_ssl":"false"}}`,
		)
		assert.NoError(t, err)
	})

	t.Run("minio_scheme", func(t *testing.T) {
		extfs := validExtfs("http://localhost:9000")
		delete(extfs, ExtfsKeyCloudProvider)
		delete(extfs, ExtfsKeyRegion)
		assert.NoError(t, ValidateExtfsComplete("minio://my-bucket/object", extfs))
	})

	tests := []struct {
		name       string
		source     string
		endpoint   string
		mutate     func(map[string]string)
		wantErrMsg string
	}{
		{name: "empty", source: "s3://bucket/key", endpoint: "", wantErrMsg: "non-empty URL"},
		{name: "surrounding_whitespace", source: "s3://bucket/key", endpoint: " http://host:9000", wantErrMsg: "surrounding whitespace"},
		{name: "parse_error", source: "s3://bucket/key", endpoint: "http://[::1", wantErrMsg: "invalid extfs.endpoint_url"},
		{name: "unsupported_endpoint_scheme", source: "s3://bucket/key", endpoint: "ftp://host", wantErrMsg: "scheme must be http or https"},
		{name: "missing_host", source: "s3://bucket/key", endpoint: "http:///", wantErrMsg: "non-empty host"},
		{name: "empty_port", source: "s3://bucket/key", endpoint: "http://host:", wantErrMsg: "empty port"},
		{name: "out_of_range_port", source: "s3://bucket/key", endpoint: "http://host:65536", wantErrMsg: "invalid port"},
		{name: "userinfo", source: "s3://bucket/key", endpoint: "http://user:pass@host", wantErrMsg: "must not embed credentials"},
		{name: "query", source: "s3://bucket/key", endpoint: "http://host?tenant=a", wantErrMsg: "must not contain a query"},
		{name: "empty_query", source: "s3://bucket/key", endpoint: "http://host?", wantErrMsg: "must not contain a query"},
		{name: "fragment", source: "s3://bucket/key", endpoint: "http://host#frag", wantErrMsg: "must not contain a fragment"},
		{name: "path", source: "s3://bucket/key", endpoint: "http://host/api", wantErrMsg: "must not contain a path"},
		{name: "escaped_path", source: "s3://bucket/key", endpoint: "http://host/%2F", wantErrMsg: "must not contain a path"},
		{name: "unsupported_source_scheme", source: "gs://bucket/key", endpoint: "https://storage.googleapis.com", wantErrMsg: "only supported for S3-compatible"},
		{
			name:     "bucket_name_conflict",
			source:   "s3://bucket/key",
			endpoint: "http://host",
			mutate: func(extfs map[string]string) {
				extfs[ExtfsKeyBucketName] = "bucket"
			},
			wantErrMsg: "bucket_name cannot be set",
		},
		{
			name:     "use_ssl_conflict",
			source:   "s3://bucket/key",
			endpoint: "https://host",
			mutate: func(extfs map[string]string) {
				extfs[ExtfsKeyUseSSL] = "false"
			},
			wantErrMsg: "conflicts with extfs.endpoint_url",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			extfs := validExtfs(test.endpoint)
			if test.mutate != nil {
				test.mutate(extfs)
			}
			err := ValidateExtfsComplete(test.source, extfs)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.wantErrMsg)
		})
	}
}

func TestRedactExternalSpec(t *testing.T) {
	t.Run("empty_returns_empty", func(t *testing.T) {
		assert.Equal(t, "", RedactExternalSpec(""))
	})

	t.Run("invalid_json_returns_placeholder", func(t *testing.T) {
		assert.Equal(t, "<invalid spec>", RedactExternalSpec("{not json"))
	})

	t.Run("invalid_extfs_returns_placeholder", func(t *testing.T) {
		assert.Equal(t, "<invalid spec>", RedactExternalSpec(`{"format":"parquet","extfs":"not-an-object"}`))
	})

	t.Run("secrets_masked", func(t *testing.T) {
		out := RedactExternalSpec(`{"format":"parquet","extfs":{"access_key_id":"AKIA","access_key_value":"SEC","credential_json":"{\"private_key\":\"SECRET\"}","endpoint_url":"https://s3.example.com","region":"us"}}`)
		assert.Contains(t, out, `"access_key_id":"***"`)
		assert.Contains(t, out, `"access_key_value":"***"`)
		assert.Contains(t, out, `"credential_json":"***"`)
		assert.Contains(t, out, `"endpoint_url":"https://s3.example.com"`)
		assert.Contains(t, out, `"region":"us"`)
		assert.NotContains(t, out, "AKIA")
		assert.NotContains(t, out, "SEC")
		assert.NotContains(t, out, "private_key")
	})

	t.Run("empty_secret_values_untouched", func(t *testing.T) {
		out := RedactExternalSpec(`{"format":"parquet","extfs":{"access_key_id":""}}`)
		assert.Contains(t, out, `"access_key_id":""`)
	})

	t.Run("non_string_secret_values_masked", func(t *testing.T) {
		out := RedactExternalSpec(`{"format":"parquet","extfs":{"access_key_id":123,"ssl_ca_cert":true}}`)
		assert.Contains(t, out, `"access_key_id":"***"`)
		assert.Contains(t, out, `"ssl_ca_cert":"***"`)
	})

	t.Run("snapshot_id_string_preserved_as_string", func(t *testing.T) {
		out := RedactExternalSpec(`{"format":"iceberg-table","snapshot_id":"5320540205222981137","extfs":null}`)
		assert.Contains(t, out, `"snapshot_id":"5320540205222981137"`)
	})

	t.Run("invalid_snapshot_id_returns_placeholder", func(t *testing.T) {
		assert.Equal(t, "<invalid spec>", RedactExternalSpec(`{"format":"iceberg-table","snapshot_id":"abc"}`))
		assert.Equal(t, "<invalid spec>", RedactExternalSpec(`{"format":"iceberg-table","snapshot_id":{}}`))
	})

	t.Run("preserves_unknown_top_level_fields", func(t *testing.T) {
		out := RedactExternalSpec(`{
			"format":"parquet",
			"cloud_extra":{
				"volume_uri":"volume://tk-stagexxx2222222/test/external-collection/",
				"volume_id":"volume-ifi5qkwp89z4ljtkvali",
				"integration_id":"integ-lir5xfbcgrkla6fjc39w15qjk",
				"path":"test/external-collection/"
			},
			"extfs":{
				"cloud_provider":"aws",
				"region":"us-west-2",
				"use_iam":"true",
				"role_arn":"arn:aws:iam::306787409409:role/lentitude-bucket-role",
				"external_id":"zilliz-external-sO1cjGS2Vgpyan"
			}
		}`)

		var got map[string]any
		require.NoError(t, json.Unmarshal([]byte(out), &got))
		require.Contains(t, got, "cloud_extra")
		cloudExtra, ok := got["cloud_extra"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "volume://tk-stagexxx2222222/test/external-collection/", cloudExtra["volume_uri"])
		assert.Equal(t, "volume-ifi5qkwp89z4ljtkvali", cloudExtra["volume_id"])
		assert.Equal(t, "integ-lir5xfbcgrkla6fjc39w15qjk", cloudExtra["integration_id"])
		assert.Equal(t, "test/external-collection/", cloudExtra["path"])

		extfs, ok := got["extfs"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "aws", extfs["cloud_provider"])
		assert.Equal(t, "arn:aws:iam::306787409409:role/lentitude-bucket-role", extfs["role_arn"])
		assert.Equal(t, "***", extfs["external_id"])
		assert.NotContains(t, out, "zilliz-external-sO1cjGS2Vgpyan")
	})
}

func TestRedactExternalSpecForLog(t *testing.T) {
	assert.Equal(t, "", RedactExternalSpecForLog(""))
	redacted := RedactExternalSpecForLog(`{
		"format":"parquet",
		"columns":["id","vector"],
		"snapshot_id":"123",
		"future_top_level":"TOP_LEVEL_SECRET_SENTINEL",
		"extfs":{
			"cloud_provider":"aws",
			"region":"us-west-2",
			"future_password":"FUTURE_SECRET_SENTINEL"
		}
	}`)
	assert.JSONEq(t, `{"format":"parquet","columns":["id","vector"],"snapshot_id":"123","extfs":"***"}`, redacted)
	assert.NotContains(t, redacted, "TOP_LEVEL_SECRET_SENTINEL")
	assert.NotContains(t, redacted, "FUTURE_SECRET_SENTINEL")
	assert.Equal(t, "<redacted>", RedactExternalSpecForLog(`{malformed SECRET_SENTINEL`))
}

func TestRedactCollectionSchemaForLog(t *testing.T) {
	assert.Nil(t, RedactCollectionSchemaForLog(nil))

	schema := &schemapb.CollectionSchema{
		Name:           "external_collection",
		ExternalSource: "s3://ACCESS_SENTINEL:SECRET_SENTINEL@bucket/path",
		ExternalSpec:   `{"format":"parquet","future_secret":"TOP_LEVEL_SENTINEL","extfs":{"access_key_value":"SPEC_SENTINEL"}}`,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
		},
	}

	redacted := RedactCollectionSchemaForLog(schema)
	require.NotSame(t, schema, redacted)
	assert.Equal(t, "external_collection", redacted.GetName())
	assert.Equal(t, schema.GetFields(), redacted.GetFields())
	assert.Equal(t, "<redacted>", redacted.GetExternalSource())
	assert.JSONEq(t, `{"format":"parquet","extfs":"***"}`, redacted.GetExternalSpec())
	assert.NotContains(t, redacted.String(), "ACCESS_SENTINEL")
	assert.NotContains(t, redacted.String(), "SECRET_SENTINEL")
	assert.NotContains(t, redacted.String(), "TOP_LEVEL_SENTINEL")
	assert.NotContains(t, redacted.String(), "SPEC_SENTINEL")
	assert.Contains(t, schema.GetExternalSource(), "ACCESS_SENTINEL")
	assert.Contains(t, schema.GetExternalSpec(), "SPEC_SENTINEL")
}

func TestExternalSpecMarshalJSON(t *testing.T) {
	t.Run("snapshot_id_marshaled_as_string", func(t *testing.T) {
		snapshotID := int64(5320540205222981137)
		out, err := json.Marshal(ExternalSpec{
			Format:     FormatIcebergTable,
			Columns:    []string{"id", "vec"},
			Extfs:      map[string]string{"cloud_provider": "aws"},
			SnapshotID: &snapshotID,
		})
		require.NoError(t, err)
		assert.Contains(t, string(out), `"snapshot_id":"5320540205222981137"`)

		var got map[string]any
		require.NoError(t, json.Unmarshal(out, &got))
		assert.Equal(t, FormatIcebergTable, got["format"])
		assert.Equal(t, "5320540205222981137", got["snapshot_id"])
	})

	t.Run("nil_snapshot_id_omitted", func(t *testing.T) {
		out, err := json.Marshal(ExternalSpec{Format: FormatParquet})
		require.NoError(t, err)
		assert.NotContains(t, string(out), "snapshot_id")
		assert.Contains(t, string(out), `"format":"parquet"`)
	})
}

func TestIsCloudEndpointHost(t *testing.T) {
	for _, host := range []string{
		"s3.us-west-2.amazonaws.com",
		"s3.cn-north-1.amazonaws.com.cn",
		"storage.googleapis.com",
		"oss-cn-hangzhou.aliyuncs.com",
		"cos.ap-shanghai.myqcloud.com",
		"obs.cn-north-4.myhuaweicloud.com",
		"acct.blob.core.windows.net",
		"acct.blob.core.chinacloudapi.cn",
		"acct.blob.core.usgovcloudapi.net",
		"acct.blob.core.cloudapi.de",
		"S3.US-WEST-2.AMAZONAWS.COM",
	} {
		assert.True(t, IsCloudEndpointHost(host), "host %s should be recognized", host)
	}

	for _, host := range []string{
		"localhost:9000",
		"bucket",
		"amazonaws.com",
	} {
		assert.False(t, IsCloudEndpointHost(host), "host %s should not be recognized", host)
	}
}

func TestDeriveEndpoint(t *testing.T) {
	tests := []struct {
		name          string
		cloudProvider string
		region        string
		expected      string
	}{
		{"aws_requires_region", CloudProviderAWS, "", ""},
		{"aws_standard", CloudProviderAWS, "us-west-2", "https://s3.us-west-2.amazonaws.com"},
		{"aws_china", CloudProviderAWS, "cn-north-1", "https://s3.cn-north-1.amazonaws.com.cn"},
		{"gcp_global", CloudProviderGCP, "", "https://storage.googleapis.com"},
		{"aliyun_requires_region", CloudProviderAliyun, "", ""},
		{"aliyun_region", CloudProviderAliyun, "cn-hangzhou", "https://oss-cn-hangzhou.aliyuncs.com"},
		{"tencent_requires_region", CloudProviderTencent, "", ""},
		{"tencent_region", CloudProviderTencent, "ap-shanghai", "https://cos.ap-shanghai.myqcloud.com"},
		{"huawei_requires_region", CloudProviderHuawei, "", ""},
		{"huawei_region", CloudProviderHuawei, "cn-north-4", "https://obs.cn-north-4.myhuaweicloud.com"},
		{"azure_requires_region", CloudProviderAzure, "", ""},
		{"azure_china", CloudProviderAzure, "china", "core.chinacloudapi.cn"},
		{"azure_usgov", CloudProviderAzure, "usgov", "core.usgovcloudapi.net"},
		{"azure_usdod", CloudProviderAzure, "usdod", "core.usgovcloudapi.net"},
		{"azure_germany", CloudProviderAzure, "germany", "core.cloudapi.de"},
		{"azure_public", CloudProviderAzure, "public", "core.windows.net"},
		{"case_insensitive_provider", "AWS", "us-east-1", "https://s3.us-east-1.amazonaws.com"},
		{"unknown_provider", "minio", "us-east-1", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, DeriveEndpoint(tt.cloudProvider, tt.region))
		})
	}
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
	azureBrokerSpec := func() map[string]string {
		return map[string]string{ //nolint:gosec // synthetic test values, not credentials
			"access_key_id":             "mystorageacct",
			"cloud_provider":            "azure",
			"region":                    "westus3",
			"azure_client_id":           "client",
			"azure_tenant_id":           "tenant",
			"azure_credential_endpoint": "https://broker.example.com/v1/credentials/assume-role",
		}
	}

	t.Run("azure_credential_broker", func(t *testing.T) {
		err := ValidateExtfsComplete(
			"azure://core.windows.net/container/path",
			azureBrokerSpec(),
		)
		assert.NoError(t, err)
	})

	t.Run("azure_credential_broker_rejects_noncanonical_provider_case", func(t *testing.T) {
		extfs := azureBrokerSpec()
		extfs["cloud_provider"] = "Azure"
		err := ValidateExtfsComplete("azure://core.windows.net/container/path", extfs)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must use the canonical lowercase value \"azure\"")
	})

	t.Run("azure_credential_mode_missing", func(t *testing.T) {
		err := ValidateExtfsComplete("azure://core.windows.net/container/path", map[string]string{
			"cloud_provider": "azure",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "credential mode missing")
	})

	t.Run("azure_credential_broker_missing_required_field", func(t *testing.T) {
		for _, key := range []string{
			"access_key_id",
			"region",
			"azure_client_id",
			"azure_tenant_id",
			"azure_credential_endpoint",
		} {
			t.Run(key, func(t *testing.T) {
				extfs := azureBrokerSpec()
				delete(extfs, key)
				err := ValidateExtfsComplete("azure://core.windows.net/container/path", extfs)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "is required for Azure credential broker mode")
			})
		}
	})

	t.Run("azure_credential_broker_invalid_endpoint", func(t *testing.T) {
		for _, endpoint := range []string{
			"file:///tmp/credentials",
			"http:///v1/credentials/assume-role",
			"not-a-url",
		} {
			t.Run(endpoint, func(t *testing.T) {
				extfs := azureBrokerSpec()
				extfs["azure_credential_endpoint"] = endpoint
				err := ValidateExtfsComplete("azure://core.windows.net/container/path", extfs)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "must be a valid HTTP(S) URL")
			})
		}
	})

	t.Run("azure_credential_broker_requires_azure_provider_and_scheme", func(t *testing.T) {
		tests := []struct {
			name     string
			source   string
			provider string
		}{
			{name: "wrong_provider", source: "azure://core.windows.net/container/path", provider: "aws"},
			{name: "wrong_scheme", source: "s3://bucket/path", provider: "azure"},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				extfs := azureBrokerSpec()
				extfs["cloud_provider"] = test.provider
				err := ValidateExtfsComplete(test.source, extfs)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "requires scheme=\"azure\"")
			})
		}
	})

	t.Run("azure_credential_broker_rejects_other_credential_modes", func(t *testing.T) {
		tests := []struct {
			name  string
			key   string
			value string
		}{
			{name: "account_key", key: "access_key_value", value: "base64key"},
			{name: "workload_identity", key: "use_iam", value: "true"},
			{name: "role_arn", key: "role_arn", value: "arn:aws:iam::1:role/r"},
			{name: "gcp_impersonation", key: "gcp_target_service_account", value: "sa@proj.iam.gserviceaccount.com"},
			{name: "anonymous", key: "anonymous", value: "true"},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				extfs := azureBrokerSpec()
				extfs[test.key] = test.value
				err := ValidateExtfsComplete("azure://core.windows.net/container/path", extfs)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "credential modes are mutually exclusive")
			})
		}
	})

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

func TestParseExternalSpec_SnapshotID(t *testing.T) {
	t.Run("string_form_accepted", func(t *testing.T) {
		spec, err := ParseExternalSpec(`{"format":"iceberg-table","snapshot_id":"5320540205222981137"}`)
		require.NoError(t, err)
		require.NotNil(t, spec.SnapshotID)
		assert.Equal(t, int64(5320540205222981137), *spec.SnapshotID)
	})

	t.Run("bare_number_accepted", func(t *testing.T) {
		spec, err := ParseExternalSpec(`{"format":"iceberg-table","snapshot_id":5320540205222981137}`)
		require.NoError(t, err)
		require.NotNil(t, spec.SnapshotID)
		assert.Equal(t, int64(5320540205222981137), *spec.SnapshotID)
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

func TestRedactExternalSpec_SnapshotIDNumber(t *testing.T) {
	redacted := RedactExternalSpec(`{"format":"iceberg-table","snapshot_id":5320540205222981137}`)
	assert.Contains(t, redacted, `"snapshot_id":"5320540205222981137"`)
	assert.NotEqual(t, "<invalid spec>", redacted)
}
