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
