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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateExternalSource(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		err := ValidateExternalSource("")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "empty")
	})

	t.Run("no scheme is same-bucket path", func(t *testing.T) {
		err := ValidateExternalSource("bucket/path")
		assert.NoError(t, err)
	})

	t.Run("rejected scheme http", func(t *testing.T) {
		err := ValidateExternalSource("http://attacker.example.com/x")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not allowed")
	})

	t.Run("rejected scheme gopher", func(t *testing.T) {
		err := ValidateExternalSource("gopher://169.254.169.254/")
		assert.Error(t, err)
	})

	t.Run("allowed s3", func(t *testing.T) {
		err := ValidateExternalSource("s3://s3.us-west-2.amazonaws.com/bucket/path/")
		assert.NoError(t, err)
	})

	t.Run("allowed s3a", func(t *testing.T) {
		err := ValidateExternalSource("s3a://bucket/path/")
		assert.NoError(t, err)
	})

	t.Run("allowed oss", func(t *testing.T) {
		err := ValidateExternalSource("oss://bucket/path/")
		assert.NoError(t, err)
	})

	t.Run("allowed gs", func(t *testing.T) {
		err := ValidateExternalSource("gs://bucket/path/")
		assert.NoError(t, err)
	})

	t.Run("allowed gcs", func(t *testing.T) {
		err := ValidateExternalSource("gcs://bucket/path/")
		assert.NoError(t, err)
	})

	t.Run("allowed file", func(t *testing.T) {
		err := ValidateExternalSource("file:///tmp/iceberg-table/")
		assert.NoError(t, err)
	})

	// Regression: these schemes were previously rejected by Go-side validation
	// even though the C++ segcore loon FFI prefix list accepts them. That
	// mismatch meant a cross-bucket URI written via the loon-native prefix
	// failed at CreateCollection time with a misleading "scheme not allowed"
	// error.
	t.Run("allowed aws (loon FFI native)", func(t *testing.T) {
		err := ValidateExternalSource("aws://s3.us-east-1.amazonaws.com/bucket/path/")
		assert.NoError(t, err)
	})

	t.Run("allowed minio (loon FFI native)", func(t *testing.T) {
		err := ValidateExternalSource("minio://minio.example.com/bucket/path/")
		assert.NoError(t, err)
	})

	t.Run("scheme is case insensitive", func(t *testing.T) {
		err := ValidateExternalSource("S3://bucket/")
		assert.NoError(t, err)
	})

	// Regression: URL-embedded userinfo must be rejected. These credentials
	// travel through logs before being silently discarded by the storage
	// layer, defeating RedactExternalSpec. Force callers to put credentials
	// in the extfs map instead.
	t.Run("rejected userinfo access_key", func(t *testing.T) {
		err := ValidateExternalSource("s3://AKIAEXAMPLE@bucket/path/")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "credentials")
	})
	t.Run("rejected userinfo access_key_and_secret", func(t *testing.T) {
		err := ValidateExternalSource("s3://user:secret@s3.us-east-1.amazonaws.com/bucket/")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "credentials")
	})

	// Regression: empty-host is ambiguous for cloud schemes and must be
	// rejected. file:// is exempt.
	t.Run("empty host s3 is same-endpoint cross-bucket", func(t *testing.T) {
		err := ValidateExternalSource("s3:///bucket/path/")
		assert.NoError(t, err)
	})
	t.Run("empty host oss is same-endpoint cross-bucket", func(t *testing.T) {
		err := ValidateExternalSource("oss:///bucket/path/")
		assert.NoError(t, err)
	})
	t.Run("file scheme exempt from empty host", func(t *testing.T) {
		// file:///tmp/foo is the canonical absolute-path shape and must
		// stay accepted even though Host is empty.
		err := ValidateExternalSource("file:///tmp/iceberg/")
		assert.NoError(t, err)
	})
}

func TestRedactExternalSpec(t *testing.T) {
	t.Run("empty input", func(t *testing.T) {
		assert.Equal(t, "", RedactExternalSpec(""))
	})

	t.Run("invalid json", func(t *testing.T) {
		out := RedactExternalSpec("not-json")
		// Must not echo any user-supplied content back into the log line.
		assert.Equal(t, "<invalid spec>", out)
	})

	t.Run("redacts access_key_id and access_key_value", func(t *testing.T) {
		in := `{"format":"parquet","extfs":{"access_key_id":"AKIA1234","access_key_value":"super-secret","region":"us-west-2"}}`
		out := RedactExternalSpec(in)
		assert.NotContains(t, out, "AKIA1234")
		assert.NotContains(t, out, "super-secret")
		assert.Contains(t, out, "***")
		// Non-secret keys are preserved verbatim.
		assert.Contains(t, out, "us-west-2")
	})

	t.Run("redacts ssl_ca_cert", func(t *testing.T) {
		in := `{"format":"parquet","extfs":{"ssl_ca_cert":"-----BEGIN CERTIFICATE-----\nABCDEF\n-----END CERTIFICATE-----"}}`
		out := RedactExternalSpec(in)
		assert.NotContains(t, out, "BEGIN CERTIFICATE")
		assert.Contains(t, out, "***")
	})

	t.Run("empty secret value stays empty", func(t *testing.T) {
		in := `{"format":"parquet","extfs":{"access_key_id":""}}`
		out := RedactExternalSpec(in)
		// Empty values do not get replaced with ***; they stay empty.
		assert.Contains(t, out, `"access_key_id":""`)
	})

	t.Run("preserves format and snapshot id", func(t *testing.T) {
		in := `{"format":"iceberg-table","snapshot_id":12345}`
		out := RedactExternalSpec(in)
		assert.Contains(t, out, "iceberg-table")
		assert.Contains(t, out, "12345")
	})

	t.Run("no extfs", func(t *testing.T) {
		in := `{"format":"parquet"}`
		out := RedactExternalSpec(in)
		assert.Contains(t, out, "parquet")
		assert.NotContains(t, out, "extfs")
	})
}

func TestParseExternalSpec_NotAllowedExtfs(t *testing.T) {
	// Sanity check that ParseExternalSpec rejects unknown extfs keys, the
	// allowlist that ValidateExternalSource depends on for downstream safety.
	_, err := ParseExternalSpec(`{"format":"parquet","extfs":{"shell_command":"rm -rf /"}}`)
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "not allowed"))
}

// TestValidateSourceAndSpec exercises the Proxy/RootCoord entry-point wrapper
// that combines ValidateExternalSource + ParseExternalSpec. The happy path and
// both failure branches must be tested directly so a future refactor cannot
// silently disable either validator. The error-wrapping via
// merr.WrapErrParameterInvalid is also part of the contract that external
// callers depend on for error-message format.
func TestValidateSourceAndSpec(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/path/", `{"format":"parquet"}`)
		assert.NoError(t, err)
	})

	t.Run("happy_path_with_extfs", func(t *testing.T) {
		err := ValidateSourceAndSpec(
			"s3://s3.us-east-1.amazonaws.com/bucket/path/",
			`{"format":"parquet","extfs":{"region":"us-east-1","use_iam":"true"}}`,
		)
		assert.NoError(t, err)
	})

	t.Run("rejects_invalid_source_scheme", func(t *testing.T) {
		err := ValidateSourceAndSpec("http://attacker.example.com/x", `{"format":"parquet"}`)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "external_source")
	})

	t.Run("rejects_source_with_userinfo", func(t *testing.T) {
		// Confirms that the userinfo hardening from ValidateExternalSource
		// reaches through the wrapper.
		err := ValidateSourceAndSpec("s3://AKIAEXAMPLE@bucket/path/", `{"format":"parquet"}`)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "external_source")
	})

	t.Run("rejects_invalid_spec_json", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/path/", `{not valid json`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "external_spec")
		// Raw invalid input must NOT be echoed back — the wrapper uses
		// "<redacted>" as the value in the parameter-invalid error so a
		// malicious payload never reaches logs or audit trails.
		assert.NotContains(t, err.Error(), "not valid json")
		assert.Contains(t, err.Error(), "<redacted>")
	})

	t.Run("rejects_spec_unsupported_format", func(t *testing.T) {
		err := ValidateSourceAndSpec("s3://bucket/path/", `{"format":"csv"}`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "external_spec")
	})

	t.Run("rejects_spec_unknown_extfs_key", func(t *testing.T) {
		err := ValidateSourceAndSpec(
			"s3://bucket/path/",
			`{"format":"parquet","extfs":{"shell_command":"rm -rf /"}}`,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "external_spec")
	})
}

// TestBuildFormatProperties exercises the format-specific property builder.
// Iceberg's snapshot_id is the only format-specific property today; other
// formats return an empty map.
func TestBuildFormatProperties(t *testing.T) {
	t.Run("parquet_returns_empty", func(t *testing.T) {
		spec := &ExternalSpec{Format: FormatParquet}
		assert.Empty(t, spec.BuildFormatProperties())
	})

	t.Run("iceberg_without_snapshot_id_returns_empty", func(t *testing.T) {
		spec := &ExternalSpec{Format: FormatIcebergTable}
		assert.Empty(t, spec.BuildFormatProperties())
	})

	t.Run("iceberg_with_snapshot_id_emits_property", func(t *testing.T) {
		snap := int64(1234567890)
		spec := &ExternalSpec{Format: FormatIcebergTable, SnapshotID: &snap}
		props := spec.BuildFormatProperties()
		assert.Equal(t, "1234567890", props[PropertyIcebergSnapshotID])
	})
}

// TestBuildExtfsOverrides exercises the prefix-prepending helper that datanode
// uses to convert user-provided extfs overrides into the loon FFI property
// key space.
func TestBuildExtfsOverrides(t *testing.T) {
	t.Run("empty_extfs_returns_nil", func(t *testing.T) {
		spec := &ExternalSpec{Format: FormatParquet}
		assert.Nil(t, spec.BuildExtfsOverrides("extfs.42."))
	})

	t.Run("prepends_prefix_to_every_key", func(t *testing.T) {
		spec := &ExternalSpec{
			Format: FormatParquet,
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
