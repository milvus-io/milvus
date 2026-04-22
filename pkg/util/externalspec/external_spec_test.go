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

	t.Run("relative_path_no_scheme_allowed", func(t *testing.T) {
		assert.NoError(t, ValidateExternalSource("my-data/parquet/"))
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

	t.Run("empty_host_allowed_same_endpoint", func(t *testing.T) {
		assert.NoError(t, ValidateExternalSource("s3:///bucket/path"))
	})

	t.Run("invalid_url_rejected", func(t *testing.T) {
		err := ValidateExternalSource("s3://bad host/path")
		require.Error(t, err)
	})
}

func TestValidateSourceAndSpec(t *testing.T) {
	t.Run("both_valid", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix", `{"format":"parquet"}`)
		assert.NoError(t, err)
	})

	t.Run("invalid_source", func(t *testing.T) {
		err := ValidateSourceAndSpec("file:///tmp/x", `{"format":"parquet"}`)
		require.Error(t, err)
	})

	t.Run("invalid_spec_redacts", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/prefix", `{bad json`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "<redacted>")
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
