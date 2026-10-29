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

package specutil

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestParseExternalSpec(t *testing.T) {
	t.Run("empty defaults to parquet", func(t *testing.T) {
		spec, err := ParseExternalSpec("")
		require.NoError(t, err)
		require.Equal(t, FormatParquet, spec.Format)
	})

	t.Run("default format keeps columns", func(t *testing.T) {
		spec, err := ParseExternalSpec(`{"columns":["a","b"]}`)
		require.NoError(t, err)
		require.Equal(t, FormatParquet, spec.Format)
		require.Equal(t, []string{"a", "b"}, spec.Columns)
	})

	t.Run("milvus table accepted", func(t *testing.T) {
		spec, err := ParseExternalSpec(`{"format":"milvus-table"}`)
		require.NoError(t, err)
		require.Equal(t, FormatMilvusTable, spec.Format)
	})

	t.Run("invalid JSON rejected", func(t *testing.T) {
		_, err := ParseExternalSpec("{bad json")
		require.ErrorContains(t, err, "invalid external spec JSON")
	})

	t.Run("unsupported format rejected", func(t *testing.T) {
		_, err := ParseExternalSpec(`{"format":"csv"}`)
		require.ErrorContains(t, err, "unsupported format")
	})

	t.Run("unknown extfs key rejected", func(t *testing.T) {
		_, err := ParseExternalSpec(`{"format":"parquet","extfs":{"unknown":"value"}}`)
		require.ErrorContains(t, err, `extfs key "unknown" is not allowed`)
	})

	t.Run("boolean extfs value validated", func(t *testing.T) {
		_, err := ParseExternalSpec(`{"format":"parquet","extfs":{"use_iam":"yes"}}`)
		require.ErrorContains(t, err, `extfs key "use_iam" must be "true" or "false"`)

		spec, err := ParseExternalSpec(`{"format":"parquet","extfs":{"use_iam":"true"}}`)
		require.NoError(t, err)
		require.Equal(t, "true", spec.Extfs[ExtfsKeyUseIAM])
	})

	t.Run("Azure credential broker keys accepted", func(t *testing.T) {
		spec, err := ParseExternalSpec(`{
			"format":"parquet",
			"extfs":{
				"azure_client_id":"client",
				"azure_tenant_id":"tenant",
				"azure_credential_endpoint":"https://broker.example.com/v1/credentials/assume-role"
			}
		}`)
		require.NoError(t, err)
		require.Equal(t, "client", spec.Extfs[ExtfsKeyAzureClientID])
		require.Equal(t, "tenant", spec.Extfs[ExtfsKeyAzureTenantID])
		require.Equal(t, "https://broker.example.com/v1/credentials/assume-role",
			spec.Extfs[ExtfsKeyAzureCredentialEndpoint])
	})

	t.Run("snapshot id accepts string and number", func(t *testing.T) {
		spec, err := ParseExternalSpec(`{"format":"iceberg-table","snapshot_id":"5320540205222981137"}`)
		require.NoError(t, err)
		require.NotNil(t, spec.SnapshotID)
		require.Equal(t, int64(5320540205222981137), *spec.SnapshotID)

		spec, err = ParseExternalSpec(`{"format":"iceberg-table","snapshot_id":5320540205222981137}`)
		require.NoError(t, err)
		require.NotNil(t, spec.SnapshotID)
		require.Equal(t, int64(5320540205222981137), *spec.SnapshotID)
	})

	t.Run("invalid snapshot id rejected", func(t *testing.T) {
		_, err := ParseExternalSpec(`{"format":"iceberg-table","snapshot_id":"abc"}`)
		require.ErrorContains(t, err, "invalid external spec JSON")
	})

	t.Run("GCP credential Broker key accepted", func(t *testing.T) {
		spec, err := ParseExternalSpec(`{"format":"parquet","extfs":{"credential_endpoint":"http://broker.example/v1/credentials/assume-role"}}`)
		require.NoError(t, err)
		require.Equal(t, "http://broker.example/v1/credentials/assume-role", spec.Extfs[ExtfsKeyCredentialEndpoint])
	})
}

func TestParseExternalSpecRejectsDuplicateKeys(t *testing.T) {
	tests := []struct {
		name string
		spec string
		key  string
	}{
		{name: "top level", spec: `{"format":"parquet","format":"vortex"}`, key: "format"},
		{name: "extfs", spec: `{"format":"parquet","extfs":{"use_iam":"true","use_iam":"false"}}`, key: "use_iam"},
		{name: "extfs secret", spec: `{"extfs":{"access_key_id":"a","access_key_value":"s","access_key_value":"t"}}`, key: "access_key_value"},
		{name: "duplicate extfs object", spec: `{"extfs":{"use_iam":"true"},"extfs":{"anonymous":"true"}}`, key: "extfs"},
		{name: "nested in unknown key", spec: `{"format":"parquet","x":{"y":[{"z":1,"z":2}]}}`, key: "z"},
		{name: "whitespace and newlines", spec: "{\n \"format\" : \"parquet\",\n \"format\":\"parquet\"\n}", key: "format"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseExternalSpec(test.spec)
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.ErrorContains(t, err, `duplicate key "`+test.key+`"`)
		})
	}

	t.Run("same key in sibling objects is not a duplicate", func(t *testing.T) {
		spec, err := ParseExternalSpec(`{"format":"parquet","extfs":{"region":"us-east-1"},"other":{"region":"x"}}`)
		require.NoError(t, err)
		require.Equal(t, "us-east-1", spec.Extfs[ExtfsKeyRegion])
	})

	t.Run("syntax errors still reported by the parser", func(t *testing.T) {
		for _, spec := range []string{`{bad json`, `{"format":"parquet"} trailing`, `   `, `[]`, `{"format":"parquet",}`} {
			_, err := ParseExternalSpec(spec)
			require.ErrorIs(t, err, merr.ErrParameterInvalid, spec)
			require.ErrorContains(t, err, "invalid external spec JSON", spec)
		}
	})
}

func TestParseExternalSpecRejectsCaseAliasKeys(t *testing.T) {
	tests := []struct {
		spec      string
		key       string
		canonical string
	}{
		{spec: `{"Format":"vortex"}`, key: "Format", canonical: "format"},
		{spec: `{"FORMAT":"vortex"}`, key: "FORMAT", canonical: "format"},
		{spec: `{"Columns":["a"]}`, key: "Columns", canonical: "columns"},
		{spec: `{"EXTFS":{"use_iam":"true"}}`, key: "EXTFS", canonical: "extfs"},
		{spec: `{"Snapshot_ID":"1"}`, key: "Snapshot_ID", canonical: "snapshot_id"},
		// Canonical and alias together: the alias would otherwise win silently.
		{spec: `{"format":"parquet","Format":"vortex"}`, key: "Format", canonical: "format"},
	}
	for _, test := range tests {
		t.Run(test.key, func(t *testing.T) {
			_, err := ParseExternalSpec(test.spec)
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.ErrorContains(t, err, `key "`+test.key+`" must be spelled "`+test.canonical+`"`)
		})
	}

	t.Run("unknown top-level keys still ignored", func(t *testing.T) {
		spec, err := ParseExternalSpec(`{"format":"parquet","formats":"x","extra":1}`)
		require.NoError(t, err)
		require.Equal(t, FormatParquet, spec.Format)
	})

	t.Run("extfs keys are matched exactly by the allowlist", func(t *testing.T) {
		_, err := ParseExternalSpec(`{"format":"parquet","extfs":{"Use_IAM":"true"}}`)
		require.ErrorContains(t, err, `extfs key "Use_IAM" is not allowed`)
	})
}

func TestExternalSpecMarshalJSON(t *testing.T) {
	snapshotID := int64(5320540205222981137)
	out, err := json.Marshal(ExternalSpec{
		Format:     FormatIcebergTable,
		SnapshotID: &snapshotID,
	})
	require.NoError(t, err)
	require.Contains(t, string(out), `"snapshot_id":"5320540205222981137"`)

	out, err = json.Marshal(ExternalSpec{Format: FormatParquet})
	require.NoError(t, err)
	require.NotContains(t, string(out), "snapshot_id")
}
