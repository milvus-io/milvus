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

func TestBrokerPolicy(t *testing.T) {
	endpoint := "https://broker.example.com/v1/credentials/assume-role"
	policy, err := NewBrokerPolicy([]string{endpoint, endpoint})
	require.NoError(t, err)
	require.Len(t, policy.Endpoints(), 1)
	for _, tc := range []struct{ raw, normalized string }{
		{"HTTPS://BROKER.example.com:443/v1/credentials/assume-role", endpoint},
		{"http://broker:080/path", "http://broker/path"},
		{"http://[::1]:80/path", "http://[::1]/path"},
		{"https://[2001:db8::1]:8443/path", "https://[2001:db8::1]:8443/path"},
		{"https://broker:8443/a_b/", "https://broker:8443/a_b/"},
		{"https://broker", "https://broker/"},
		{"HTTPS://BROKER:443/path?api-version=1&scope=a%2Fb+x&scope=y", "https://broker/path?api-version=1&scope=a%2Fb+x&scope=y"},
		{"https://broker?", "https://broker/?"},
	} {
		actual, err := NormalizeBrokerEndpoint(tc.raw)
		require.NoError(t, err, tc.raw)
		require.Equal(t, tc.normalized, actual)
	}
	for _, raw := range []string{"", "ftp://broker/path", "https:///path", "https://user:secret@broker/path", "https://broker/path#", "https://broker/path#f", "https://broker:", "https://broker:0/path", "https://broker:65536/path", "https://*.broker/path", "https://broker/%zz", "https://broker/a b", "https://broker:000080/path", "https://[invalid]/path", "https://[gg::1]/path", "https://[::ffff:invalid]/path", "https://[127.0.0.1]/path", "http://2001:db8::1/path", "https://broker!example/path", "https://broker/a%2Fb/", "https://broker/allowed//admin", "https://broker/a/../admin", "https://broker/./admin", "https://broker/a:b"} {
		_, err := NormalizeBrokerEndpoint(raw)
		require.Error(t, err, raw)
		_, err = NewBrokerPolicy([]string{raw})
		require.Error(t, err, raw)
	}
	for _, raw := range []string{endpoint + "/", "http://broker.example.com/v1/credentials/assume-role", "https://broker.example.com.evil/v1/credentials/assume-role", "https://broker.example.com/other"} {
		require.Error(t, policy.Validate(map[string]string{ExtfsKeyCredentialEndpoint: raw}))
	}
	extfs := map[string]string{ExtfsKeyCloudProvider: "azure", ExtfsKeyAzureCredentialEndpoint: endpoint}
	require.NoError(t, policy.Validate(extfs))
	require.NotContains(t, extfs, ExtfsKeyCredentialEndpoint)
	extfs[ExtfsKeyCredentialEndpoint] = "HTTPS://BROKER.example.com:443/v1/credentials/assume-role"
	require.NoError(t, policy.Validate(extfs))
	extfs[ExtfsKeyCredentialEndpoint] = endpoint + "/"
	require.Error(t, policy.Validate(extfs))
	extfs[ExtfsKeyCloudProvider] = "aws"
	require.Error(t, policy.Validate(extfs))
	require.Error(t, policy.Validate(map[string]string{ExtfsKeyCredentialEndpoint: "invalid"}))
	var empty *BrokerPolicy
	require.Empty(t, empty.Endpoints())
	require.NoError(t, empty.Validate(nil))
	require.Error(t, empty.Validate(map[string]string{ExtfsKeyCredentialEndpoint: endpoint}))
	copy := policy.Endpoints()
	copy[0] = "https://other"
	require.NoError(t, policy.Validate(map[string]string{ExtfsKeyCredentialEndpoint: endpoint}))
}

func TestBrokerQuerySyntaxAndExactIdentity(t *testing.T) {
	const endpoint = "https://broker/token?scope=a,b&next=https://nested/token?x=1&scope=a%2Fb+x&scope=a%2fb&json=%5B1%5D&punct=!$'()*+,-._~;=:@/?%25"
	policy, err := NewBrokerPolicy([]string{endpoint})
	require.NoError(t, err)
	normalized, err := NormalizeBrokerEndpoint(endpoint)
	require.NoError(t, err)
	require.Equal(t, endpoint, normalized)
	for _, field := range []string{ExtfsKeyCredentialEndpoint, ExtfsKeyAzureCredentialEndpoint} {
		extfs := map[string]string{ExtfsKeyCloudProvider: "azure", field: endpoint}
		require.NoError(t, policy.Validate(extfs))
		for _, denied := range []string{"https://nested/token?x=1", "https://broker/token?scope=a", endpoint + "&extra=1"} {
			extfs[field] = denied
			require.Error(t, policy.Validate(extfs), denied)
		}
	}
	for _, query := range []string{"x=%", "x=%2", "x=%zz", "x=%0g", "q=[1]", "q={1}", "q=|", "q=^", "q=`", "q=\""} {
		endpoint := "https://broker/token?" + query
		_, err := NormalizeBrokerEndpoint(endpoint)
		require.Error(t, err, query)
		_, err = NewBrokerPolicy([]string{endpoint})
		require.Error(t, err, query)
	}
}
