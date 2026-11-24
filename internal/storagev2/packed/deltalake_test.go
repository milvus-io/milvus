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

//go:build dynamic

package packed

import (
	"context"
	"fmt"
	"maps"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestDeltaLakeSnapshotIdentity(t *testing.T) {
	first := Fragment{
		FilePath: "data.parquet", EndRow: 10,
		Properties: map[string]string{"metadata": `{"version":1,"table_uri":"s3://bucket/table/","snapshot_version":0}`, "file_size": "123"},
	}
	next := first
	next.Properties = maps.Clone(first.Properties)
	next.Properties["metadata"] = `{"version":1,"table_uri":"s3://bucket/table/","snapshot_version":1}`
	require.NotEqual(t, FragmentIdentity(first), FragmentIdentity(next), "same-count DV changes must invalidate old segments")
	clone := first
	clone.Properties = maps.Clone(first.Properties)
	require.Equal(t, FragmentIdentity(first), FragmentIdentity(clone))
}

func TestDeltaLakeMetadataRecognition(t *testing.T) {
	for _, test := range []struct {
		name     string
		metadata string
		want     bool
	}{
		{"snapshot_zero", `{"version":1,"table_uri":"s3://bucket/table/","snapshot_version":0}`, true},
		{"max_snapshot", `{"version":1,"table_uri":"s3://bucket/table/","snapshot_version":9223372036854775807}`, true},
		{"missing_snapshot", `{"version":1,"table_uri":"s3://bucket/table/"}`, false},
		{"null_snapshot", `{"version":1,"table_uri":"s3://bucket/table/","snapshot_version":null}`, false},
		{"paimon", `{"version":1,"read_path":"direct-file","snapshot_id":1}`, false},
		{"iceberg", `[{"path":"delete.parquet"}]`, false},
		{"absent", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, ok := parseDeltaLakeMetadata(map[string]string{"metadata": test.metadata})
			require.Equal(t, test.want, ok)
		})
	}
}

// A real Avro manifest round trip exercises the storage-owned property map and
// format recovery used when a worker restarts or an existing segment is reused.
func TestDeltaLakeManifestRoundTrip(t *testing.T) {
	paramtable.Init()
	root := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", RootPath: root, BucketName: root}
	original := Fragment{
		FilePath: filepath.Join(root, "data.parquet"), EndRow: 10, RowCount: 10,
		Properties: map[string]string{"metadata": `{"version":1,"table_uri":"s3://bucket/table/","snapshot_version":0}`, "file_size": "123", "future_property": "opaque"},
	}
	manifest, err := CreateManifestForSegment(filepath.Join(root, "segment"), []string{"pk", "vector"}, externalspec.FormatDeltaLakeTable, []Fragment{original}, config)
	require.NoError(t, err)
	fragments, err := ReadFragmentsFromManifest(manifest, config, []string{"pk"})
	require.NoError(t, err)
	require.Len(t, fragments, 1)
	require.Equal(t, original.Properties, fragments[0].Properties)
	require.Equal(t, FragmentIdentity(original), FragmentIdentity(fragments[0]))
	base, version, err := UnmarshalManifestPath(manifest)
	require.NoError(t, err)
	// Read the same manifest as a worker's explore-manifest input, with a row
	// target smaller than this file. The descriptor must remain indivisible.
	fragments, err = FetchFragmentsFromExternalSourceWithRange(context.Background(), externalspec.FormatDeltaLakeTable, []string{"pk"}, "s3://bucket/table", config, 0, 1,
		fmt.Sprintf("%s/_metadata/manifest-%d.avro", base, version), ExternalFetchOptions{RowLimit: 3})
	require.NoError(t, err)
	require.Len(t, fragments, 1)
	require.Equal(t, int64(10), fragments[0].RowCount)
	require.Equal(t, FragmentIdentity(original), FragmentIdentity(fragments[0]))
}

func TestDeltaLakeSnapshotProperty(t *testing.T) {
	paramtable.Init()
	root := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", RootPath: root, BucketName: root}
	for _, selector := range []string{"0", `"0"`, "-1", "9223372036854775807"} {
		t.Run(selector, func(t *testing.T) {
			props, err := MakePropertiesFromStorageConfig(config, nil)
			require.NoError(t, err)
			defer FreeProperties(props)
			require.NoError(t, injectExternalSpecProperties(props, 42, "s3://bucket/table", `{"format":"deltalake-table","snapshot_id":`+selector+`}`))
			expected := selector
			if selector == `"0"` {
				expected = "0"
			}
			require.Equal(t, expected, loonPropertyString(props, "reader.exttable.snapshot_id"))
		})
	}
}
