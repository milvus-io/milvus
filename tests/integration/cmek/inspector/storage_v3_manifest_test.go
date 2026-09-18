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

package inspector

import (
	"bytes"
	"strings"
	"testing"

	"github.com/iskorotkov/avro/v2/ocf"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestManifestLocatorV3RejectsAmbiguousRevision(t *testing.T) {
	for _, input := range []string{
		`{}`, `null`, `{"base_path":"root/segment"}`,
		`{"base_path":"root/segment","ver":0}`,
		`{"base_path":"root/segment","ver":-1}`,
		`{"base_path":"root/../segment","ver":2}`,
		`{"base_path":"root/segment","ver":2,"ver":3}`,
		`{"base_path":"root/segment","ver":2,"key":"canary"}`,
		`{"base_path":"root/segment","ver":2} {}`,
	} {
		t.Run(input, func(t *testing.T) {
			_, err := ParseManifestLocatorV3(input)
			require.Error(t, err)
		})
	}
	locator, err := ParseManifestLocatorV3(`{"base_path":"root/segment","ver":7}`)
	require.NoError(t, err)
	require.Equal(t, "root/segment/_metadata/manifest-7.avro", locator.ObjectPath())
}

func TestLocateManifestsV3PreservesEverySegment(t *testing.T) {
	segments := []*datapb.SegmentInfo{
		{ID: 11, CollectionID: 7, NumOfRows: 4, StorageVersion: 3, ManifestPath: `{"base_path":"root/11","ver":3}`},
		{ID: 12, CollectionID: 7, NumOfRows: 4, StorageVersion: 3, ManifestPath: `{"base_path":"root/12","ver":9}`},
	}
	references, err := LocateManifestsV3(segments, 7)
	require.NoError(t, err)
	require.Len(t, references, 2)
	require.Equal(t, "root/11/_metadata/manifest-3.avro", references[0].Locator.ObjectPath())
	require.Equal(t, "root/12/_metadata/manifest-9.avro", references[1].Locator.ObjectPath())
	for _, input := range [][]*datapb.SegmentInfo{
		nil,
		{nil},
		{segments[0], segments[0]},
		{{ID: 11, CollectionID: 8, NumOfRows: 4, StorageVersion: 3, ManifestPath: segments[0].ManifestPath}},
		{{ID: 11, CollectionID: 7, NumOfRows: 4, StorageVersion: 2, ManifestPath: segments[0].ManifestPath}},
		{{ID: 11, CollectionID: 7, StorageVersion: 3, ManifestPath: segments[0].ManifestPath}},
	} {
		_, err := LocateManifestsV3(input, 7)
		require.Error(t, err)
	}
}

const manifestV3TestSchema = `{"type":"record","name":"Manifest","fields":[
 {"name":"column_groups","type":{"type":"array","items":{"type":"record","name":"ColumnGroup","fields":[
 {"name":"columns","type":{"type":"array","items":"string"}},
 {"name":"format","type":"string"},
 {"name":"files","type":{"type":"array","items":{"type":"record","name":"File","fields":[
 {"name":"path","type":"string"},{"name":"start_index","type":"long"},{"name":"end_index","type":"long"},
 {"name":"properties","type":{"type":"map","values":"string"}}]}}}]}}},
 {"name":"stats","type":{"type":"map","values":{"type":"record","name":"Statistics","fields":[
 {"name":"paths","type":{"type":"array","items":"string"}},{"name":"metadata","type":{"type":"map","values":"string"}}]}}}
 ]}`

func manifestV3Fixture() map[string]any {
	return map[string]any{
		"column_groups": []any{
			map[string]any{"columns": []string{"0", "1", "100"}, "format": "parquet", "files": []any{
				map[string]any{"path": "a.parquet", "start_index": int64(0), "end_index": int64(2), "properties": map[string]string{"file_size": "1024", "footer_size": "512"}},
				map[string]any{"path": "b.parquet", "start_index": int64(2), "end_index": int64(4), "properties": map[string]string{}},
			}},
			map[string]any{"columns": []string{"101"}, "format": "parquet", "files": []any{
				map[string]any{"path": "c.parquet", "start_index": int64(0), "end_index": int64(2), "properties": map[string]string{}},
				map[string]any{"path": "d.parquet", "start_index": int64(2), "end_index": int64(4), "properties": map[string]string{}},
			}},
		},
		"stats": map[string]any{"bloom_filter.100": map[string]any{"paths": []string{"_stats/bloom_filter.100/7"}, "metadata": map[string]string{}}},
	}
}

func encodeManifestV3(t *testing.T, schema string, records ...map[string]any) []byte {
	t.Helper()
	var output bytes.Buffer
	encoder, err := ocf.NewEncoder(schema, &output)
	require.NoError(t, err)
	for _, record := range records {
		require.NoError(t, encoder.Encode(record))
	}
	require.NoError(t, encoder.Close())
	return output.Bytes()
}

func TestManifestV3PreservesEveryColumnGroupAndFile(t *testing.T) {
	objects, err := ParseParquetObjectsV3(encodeManifestV3(t, manifestV3TestSchema, manifestV3Fixture()), "root/11")
	require.NoError(t, err)
	require.Equal(t, []ParquetObjectV3{
		{Path: "root/11/_data/a.parquet", Columns: []string{"0", "1", "100"}, Rows: 2},
		{Path: "root/11/_data/b.parquet", Columns: []string{"0", "1", "100"}, Rows: 2},
		{Path: "root/11/_data/c.parquet", Columns: []string{"101"}, Rows: 2},
		{Path: "root/11/_data/d.parquet", Columns: []string{"101"}, Rows: 2},
	}, objects)
}

func TestManifestV3EnumeratesAllObjectsAcrossSegments(t *testing.T) {
	segments := []*datapb.SegmentInfo{
		{ID: 11, CollectionID: 7, NumOfRows: 4, StorageVersion: 3, ManifestPath: `{"base_path":"root/11","ver":3}`},
		{ID: 12, CollectionID: 7, NumOfRows: 4, StorageVersion: 3, ManifestPath: `{"base_path":"root/12","ver":9}`},
	}
	references, err := LocateManifestsV3(segments, 7)
	require.NoError(t, err)
	var paths []string
	for _, reference := range references {
		objects, err := ParseParquetObjectsV3(encodeManifestV3(t, manifestV3TestSchema, manifestV3Fixture()), reference.Locator.BasePath)
		require.NoError(t, err)
		for _, object := range objects {
			paths = append(paths, object.Path)
			require.EqualValues(t, 2, object.Rows)
		}
	}
	require.Equal(t, []string{
		"root/11/_data/a.parquet", "root/11/_data/b.parquet", "root/11/_data/c.parquet", "root/11/_data/d.parquet",
		"root/12/_data/a.parquet", "root/12/_data/b.parquet", "root/12/_data/c.parquet", "root/12/_data/d.parquet",
	}, paths)
}

func TestManifestV3RejectsPayloadAndUnclassifiedMetadata(t *testing.T) {
	for _, field := range []string{"user_payload", "text_value", "dek", "cipher_context", "future_field"} {
		t.Run(field, func(t *testing.T) {
			record := manifestV3Fixture()
			record[field] = "sensitive-canary"
			schema := strings.Replace(manifestV3TestSchema, `"fields":[`, `"fields":[{"name":"`+field+`","type":"string"},`, 1)
			_, err := ParseParquetObjectsV3(encodeManifestV3(t, schema, record), "root/11")
			require.Error(t, err)
			require.NotContains(t, err.Error(), "sensitive-canary")
		})
	}
	for _, property := range []string{"writer.enc.key", "dek", "payload", "unclassified"} {
		t.Run("file property "+property, func(t *testing.T) {
			record := manifestV3Fixture()
			file := record["column_groups"].([]any)[0].(map[string]any)["files"].([]any)[0].(map[string]any)
			file["properties"].(map[string]string)[property] = "sensitive-canary"
			_, err := ParseParquetObjectsV3(encodeManifestV3(t, manifestV3TestSchema, record), "root/11")
			require.Error(t, err)
			require.NotContains(t, err.Error(), "sensitive-canary")
		})
	}
	t.Run("stats metadata", func(t *testing.T) {
		record := manifestV3Fixture()
		record["stats"].(map[string]any)["bloom_filter.100"].(map[string]any)["metadata"] = map[string]string{"payload": "sensitive-canary"}
		_, err := ParseParquetObjectsV3(encodeManifestV3(t, manifestV3TestSchema, record), "root/11")
		require.Error(t, err)
		require.NotContains(t, err.Error(), "sensitive-canary")
	})
}

func TestManifestV3RejectsIncompleteAndMalformedObjects(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"empty groups", func(r map[string]any) { r["column_groups"] = []any{} }},
		{"empty files", func(r map[string]any) { r["column_groups"].([]any)[0].(map[string]any)["files"] = []any{} }},
		{"invalid columns", func(r map[string]any) {
			r["column_groups"].([]any)[0].(map[string]any)["columns"] = []string{"user-value"}
		}},
		{"negative range", func(r map[string]any) {
			r["column_groups"].([]any)[0].(map[string]any)["files"].([]any)[0].(map[string]any)["start_index"] = int64(-1)
		}},
		{"empty range", func(r map[string]any) {
			r["column_groups"].([]any)[0].(map[string]any)["files"].([]any)[0].(map[string]any)["end_index"] = int64(0)
		}},
		{"duplicate object", func(r map[string]any) {
			r["column_groups"].([]any)[0].(map[string]any)["files"].([]any)[1].(map[string]any)["path"] = "a.parquet"
		}},
		{"duplicate resolved object", func(r map[string]any) {
			r["column_groups"].([]any)[0].(map[string]any)["files"].([]any)[1].(map[string]any)["path"] = "root/11/_data/a.parquet"
		}},
		{"invalid size", func(r map[string]any) {
			r["column_groups"].([]any)[0].(map[string]any)["files"].([]any)[0].(map[string]any)["properties"] = map[string]string{"file_size": "payload"}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			record := manifestV3Fixture()
			test.mutate(record)
			_, err := ParseParquetObjectsV3(encodeManifestV3(t, manifestV3TestSchema, record), "root/11")
			require.Error(t, err)
		})
	}
	raw := encodeManifestV3(t, manifestV3TestSchema, manifestV3Fixture())
	_, err := ParseParquetObjectsV3(raw[:len(raw)-4], "root/11")
	require.Error(t, err)
	_, err = ParseParquetObjectsV3(encodeManifestV3(t, manifestV3TestSchema, manifestV3Fixture(), manifestV3Fixture()), "root/11")
	require.ErrorContains(t, err, "multiple records")
}

func TestManifestLocatorV3RejectsMalformedJSON(t *testing.T) {
	for _, input := range []string{
		`{"base_path":7,"ver":1}`,
		`{"base_path":"root/11","ver":"1"}`,
		`{"base_path":"root/11","ver":1,]}`,
		`{"base_path":"root/11","ver":1`,
	} {
		t.Run(input, func(t *testing.T) {
			_, err := ParseManifestLocatorV3(input)
			require.Error(t, err)
		})
	}
}

func TestLocateManifestsV3RejectsInvalidOrSharedLocator(t *testing.T) {
	for _, locator := range []string{`{}`, `{"base_path":"root/11","ver":3}`} {
		segments := []*datapb.SegmentInfo{
			{ID: 11, CollectionID: 7, NumOfRows: 4, StorageVersion: 3, ManifestPath: `{"base_path":"root/11","ver":3}`},
			{ID: 12, CollectionID: 7, NumOfRows: 4, StorageVersion: 3, ManifestPath: locator},
		}
		references, err := LocateManifestsV3(segments, 7)
		require.Error(t, err)
		require.Nil(t, references, "invalid segment metadata must not yield a partial object inventory")
	}
}

func TestManifestV3RejectsInvalidContainer(t *testing.T) {
	t.Run("invalid base path", func(t *testing.T) {
		raw := encodeManifestV3(t, manifestV3TestSchema, manifestV3Fixture())
		_, err := ParseParquetObjectsV3(raw, "../segment")
		require.ErrorContains(t, err, "invalid manifest base path")
	})
	t.Run("invalid OCF header", func(t *testing.T) {
		_, err := ParseParquetObjectsV3([]byte("not an Avro container"), "root/11")
		require.Error(t, err)
	})
	t.Run("no records", func(t *testing.T) {
		_, err := ParseParquetObjectsV3(encodeManifestV3(t, manifestV3TestSchema), "root/11")
		require.ErrorContains(t, err, "no record")
	})
}

func TestManifestV3ValidatesStatistics(t *testing.T) {
	for _, test := range []struct {
		name     string
		kind     string
		paths    []string
		metadata map[string]string
		wantErr  string
	}{
		{"bloom filter", "bloom_filter.100", []string{"_stats/bloom/1"}, map[string]string{"memory_size": "1024"}, ""},
		{"bm25", "bm25.101", []string{"_stats/bm25/1"}, map[string]string{"memory_size": "0"}, ""},
		{"unknown kind", "payload.100", []string{"_stats/1"}, nil, "unclassified manifest statistics"},
		{"missing field", "bloom_filter", []string{"_stats/1"}, nil, "unclassified manifest statistics"},
		{"nonnumeric field", "bloom_filter.secret", []string{"_stats/1"}, nil, "invalid manifest statistics"},
		{"negative field", "bloom_filter.-1", []string{"_stats/1"}, nil, "invalid manifest statistics"},
		{"no paths", "bloom_filter.100", []string{}, nil, "invalid manifest statistics"},
		{"path traversal", "bloom_filter.100", []string{"../secret"}, nil, "invalid manifest statistics path"},
		{"negative size", "bloom_filter.100", []string{"_stats/1"}, map[string]string{"memory_size": "-1"}, "invalid manifest numeric property"},
		{"overflow size", "bloom_filter.100", []string{"_stats/1"}, map[string]string{"memory_size": "9223372036854775808"}, "invalid manifest numeric property"},
	} {
		t.Run(test.name, func(t *testing.T) {
			record := manifestV3Fixture()
			metadata := test.metadata
			if metadata == nil {
				metadata = map[string]string{}
			}
			record["stats"] = map[string]any{test.kind: map[string]any{"paths": test.paths, "metadata": metadata}}
			objects, err := ParseParquetObjectsV3(encodeManifestV3(t, manifestV3TestSchema, record), "root/11")
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				require.Nil(t, objects)
				return
			}
			require.NoError(t, err)
			require.Len(t, objects, 4, "statistics must not be mistaken for raw-data objects")
		})
	}
}

func TestManifestV3ValidatesAuxiliaryArtifacts(t *testing.T) {
	const deltaField = `{"name":"delta_logs","type":{"type":"array","items":{"type":"record","name":"Delta","fields":[{"name":"path","type":"string"},{"name":"num_entries","type":"long"}]}}}`
	const indexField = `{"name":"indexes","type":{"type":"array","items":{"type":"record","name":"Index","fields":[{"name":"path","type":"string"},{"name":"field_id","type":"long"},{"name":"num_rows","type":"long"},{"name":"serialized_size","type":"long"},{"name":"mem_size","type":"long"},{"name":"properties","type":{"type":"map","values":"string"}}]}}}`
	const lobField = `{"name":"lob_files","type":{"type":"array","items":{"type":"record","name":"LOB","fields":[{"name":"path","type":"string"}]}}}`
	for _, test := range []struct {
		name    string
		field   string
		key     string
		item    map[string]any
		wantErr string
	}{
		{"delta", deltaField, "delta_logs", map[string]any{"path": "_delta/1", "num_entries": int64(1)}, ""},
		{"invalid delta path", deltaField, "delta_logs", map[string]any{"path": "../secret", "num_entries": int64(1)}, "invalid manifest delta log"},
		{"negative delta entries", deltaField, "delta_logs", map[string]any{"path": "_delta/1", "num_entries": int64(-1)}, "invalid manifest delta log"},
		{"index", indexField, "indexes", map[string]any{"path": "_index/1", "field_id": int64(100), "num_rows": int64(4), "serialized_size": int64(32), "mem_size": int64(64), "properties": map[string]string{}}, ""},
		{"invalid index path", indexField, "indexes", map[string]any{"path": "../secret", "field_id": int64(100), "num_rows": int64(4), "serialized_size": int64(32), "mem_size": int64(64), "properties": map[string]string{}}, "invalid manifest index metadata"},
		{"index secret property", indexField, "indexes", map[string]any{"path": "_index/1", "field_id": int64(100), "num_rows": int64(4), "serialized_size": int64(32), "mem_size": int64(64), "properties": map[string]string{"dek": "sensitive-canary"}}, "unclassified manifest property"},
		{"unsupported LOB", lobField, "lob_files", map[string]any{"path": "_lob/1"}, "unexpectedly produced LOB files"},
	} {
		t.Run(test.name, func(t *testing.T) {
			schema := strings.Replace(manifestV3TestSchema, `"fields":[`, `"fields":[`+test.field+`,`, 1)
			record := manifestV3Fixture()
			record[test.key] = []any{test.item}
			objects, err := ParseParquetObjectsV3(encodeManifestV3(t, schema, record), "root/11")
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				require.NotContains(t, err.Error(), "sensitive-canary")
				require.Nil(t, objects)
				return
			}
			require.NoError(t, err)
			require.Len(t, objects, 4, "auxiliary artifacts must not be mistaken for raw-data objects")
		})
	}
}

func TestManifestV3RejectsUnclassifiedContainerMetadata(t *testing.T) {
	var output bytes.Buffer
	encoder, err := ocf.NewEncoder(manifestV3TestSchema, &output,
		ocf.WithMetadata(map[string][]byte{"dek": []byte("sensitive-canary")}))
	require.NoError(t, err)
	require.NoError(t, encoder.Encode(manifestV3Fixture()))
	require.NoError(t, encoder.Close())
	objects, err := ParseParquetObjectsV3(output.Bytes(), "root/11")
	require.ErrorContains(t, err, "unclassified manifest OCF metadata")
	require.NotContains(t, err.Error(), "sensitive-canary")
	require.Nil(t, objects)
}
