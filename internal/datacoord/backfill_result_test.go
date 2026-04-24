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

package datacoord

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
)

// mixedBackfillResultJSON mirrors the fixture shape in
// spark-milvus/docs/backfill-result-json-format.md.
const mixedBackfillResultJSON = `{
  "success": true,
  "collectionId": 450000000001,
  "partitionId":  450000000002,
  "segmentsProcessed": 2,
  "newFieldNames": ["embedding_v2", "score"],
  "segments": {
    "450000000100": {
      "version": 42,
      "rowCount": 333334,
      "outputPath": "s3a://bucket/seg/100",
      "manifestPaths": ["s3a://bucket/seg/100/manifest/0042"]
    },
    "450000000101": {
      "version": -1,
      "rowCount": 333333,
      "outputPath": "s3a://bucket/seg/101",
      "manifestPaths": [
        "s3a://bucket/seg/101/100/7",
        "s3a://bucket/seg/101/101/9"
      ],
      "storage_version": 2,
      "column_groups": [
        {
          "field_ids":    [100],
          "binlog_files": ["s3a://bucket/seg/101/100/7"],
          "row_count": 333333
        },
        {
          "field_ids":    [101],
          "binlog_files": ["s3a://bucket/seg/101/101/9"],
          "row_count": 333333
        }
      ]
    }
  }
}`

func TestBackfillResult_ParseMixedV2V3(t *testing.T) {
	var r BackfillResult
	err := json.Unmarshal([]byte(mixedBackfillResultJSON), &r)
	assert.NoError(t, err)
	assert.True(t, r.Success)
	assert.Equal(t, int64(450000000001), r.CollectionID)
	assert.Equal(t, int64(450000000002), r.PartitionID)
	assert.ElementsMatch(t, []string{"embedding_v2", "score"}, r.NewFieldNames)
	assert.Len(t, r.Segments, 2)

	v3 := r.Segments["450000000100"]
	assert.False(t, v3.IsV2())
	assert.Equal(t, int64(42), v3.Version)

	v2 := r.Segments["450000000101"]
	assert.True(t, v2.IsV2())
	assert.Equal(t, int64(-1), v2.Version)
	assert.Len(t, v2.ColumnGroups, 2)
	assert.Equal(t, int64(333333), v2.ColumnGroups[0].RowCount)
	assert.Equal(t, []int64{100}, v2.ColumnGroups[0].FieldIDs)
}

func TestBackfillResult_IsV2(t *testing.T) {
	v2 := storage.StorageV2
	v3 := storage.StorageV3
	zero := int64(0)

	cases := []struct {
		name string
		seg  BackfillSegment
		want bool
	}{
		{
			name: "absent storage version -> V3",
			seg:  BackfillSegment{},
			want: false,
		},
		{
			name: "storage_version=2 with groups -> V2",
			seg: BackfillSegment{
				StorageVersion: &v2,
				ColumnGroups:   []BackfillV2ColumnGroup{{FieldIDs: []int64{100}, BinlogFiles: []string{"a"}, RowCount: 1}},
			},
			want: true,
		},
		{
			name: "storage_version=2 without groups -> V3 (defensive)",
			seg:  BackfillSegment{StorageVersion: &v2},
			want: false,
		},
		{
			name: "storage_version=3 -> V3",
			seg:  BackfillSegment{StorageVersion: &v3},
			want: false,
		},
		{
			name: "storage_version=0 ptr -> V3",
			seg:  BackfillSegment{StorageVersion: &zero},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.seg.IsV2())
		})
	}
}

func TestNormalizeObjectKey(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		bucket   string
		wantKey  string
		wantErr  bool
		errMatch string
	}{
		{"s3a with matching bucket", "s3a://bkt/path/foo", "bkt", "path/foo", false, ""},
		{"s3 scheme", "s3://bkt/path/foo", "bkt", "path/foo", false, ""},
		{"s3n scheme", "s3n://bkt/deep/path/f", "bkt", "deep/path/f", false, ""},
		{"gs scheme", "gs://mybkt/a/b", "mybkt", "a/b", false, ""},
		{"minio scheme", "minio://bkt/a", "bkt", "a", false, ""},
		{"no scheme, leading slash", "/data/foo", "", "data/foo", false, ""},
		{"no scheme, no slash", "data/foo", "", "data/foo", false, ""},
		{"no scheme, ignore bucket arg", "data/foo", "bkt", "data/foo", false, ""},
		{"empty path", "", "", "", true, "empty object path"},
		{"bucket mismatch", "s3a://other/foo", "bkt", "", true, "differs from datacoord bucket"},
		{"empty expected bucket skips check", "s3a://anything/foo", "", "foo", false, ""},
		{"unsupported scheme", "hdfs://host/path", "", "", true, "unsupported object URI scheme"},
		{"missing key", "s3a://bkt", "bkt", "", true, "missing object key"},
		// trailing slash produces an empty key -- chunkManager.Read with an
		// empty key has undefined behavior, must be rejected.
		{"empty key after scheme", "s3a://bkt/", "bkt", "", true, "empty object key"},
		{"empty key no scheme", "/", "", "", true, "empty object key"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeObjectKey(tc.raw, tc.bucket)
			if tc.wantErr {
				assert.Error(t, err)
				if tc.errMatch != "" {
					assert.Contains(t, err.Error(), tc.errMatch)
				}
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.wantKey, got)
		})
	}
}

func TestParseLogIDFromKey(t *testing.T) {
	cases := []struct {
		key    string
		wantID int64
		ok     bool
	}{
		{"seg/101/100/7", 7, true},
		{"7", 7, true},
		{"seg/101/100/12345", 12345, true},
		{"seg/101/100/foo", 0, false},
		{"seg/101/100/7.parquet", 0, false},
		{"", 0, false},
	}
	for _, tc := range cases {
		got, ok := parseLogIDFromKey(tc.key)
		assert.Equal(t, tc.ok, ok, tc.key)
		assert.Equal(t, tc.wantID, got, tc.key)
	}
}

func TestBuildV2Groups(t *testing.T) {
	v2 := storage.StorageV2

	t.Run("single file per group", func(t *testing.T) {
		seg := &BackfillSegment{
			StorageVersion: &v2,
			ColumnGroups: []BackfillV2ColumnGroup{
				{
					FieldIDs:    []int64{100},
					BinlogFiles: []string{"s3a://bkt/seg/1/100/7"},
					RowCount:    1000,
				},
			},
		}
		groups, err := buildV2Groups("bkt", seg)
		assert.NoError(t, err)
		assert.Len(t, groups, 1)
		fb := groups[100]
		assert.Equal(t, int64(100), fb.GetFieldID())
		// ChildFields must be populated — index creation and backfill-
		// compaction detection look up fields via ChildFields, not FieldID.
		assert.Equal(t, []int64{100}, fb.GetChildFields())
		assert.Len(t, fb.GetBinlogs(), 1)
		assert.Equal(t, int64(1000), fb.GetBinlogs()[0].GetEntriesNum())
		assert.Equal(t, int64(7), fb.GetBinlogs()[0].GetLogID())
		// LogPath must be empty at persistence -- catalog.checkLogID
		// rejects non-empty LogPath. DecompressBinLog rebuilds it on load.
		assert.Equal(t, "", fb.GetBinlogs()[0].GetLogPath())
	})

	t.Run("rejects binlog file with non-numeric trailing segment", func(t *testing.T) {
		seg := &BackfillSegment{
			ColumnGroups: []BackfillV2ColumnGroup{
				{FieldIDs: []int64{100}, BinlogFiles: []string{"s3a://bkt/seg/1/100/7.parquet"}, RowCount: 1},
			},
		}
		_, err := buildV2Groups("bkt", seg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "non-numeric trailing segment")
	})

	t.Run("multi file splits row_count evenly with remainder on last", func(t *testing.T) {
		seg := &BackfillSegment{
			StorageVersion: &v2,
			ColumnGroups: []BackfillV2ColumnGroup{
				{
					FieldIDs:    []int64{200},
					BinlogFiles: []string{"s3a://bkt/seg/1/200/1", "s3a://bkt/seg/1/200/2", "s3a://bkt/seg/1/200/3"},
					RowCount:    10,
				},
			},
		}
		groups, err := buildV2Groups("bkt", seg)
		assert.NoError(t, err)
		fb := groups[200]
		binlogs := fb.GetBinlogs()
		assert.Len(t, binlogs, 3)
		assert.Equal(t, int64(3), binlogs[0].GetEntriesNum())
		assert.Equal(t, int64(3), binlogs[1].GetEntriesNum())
		assert.Equal(t, int64(4), binlogs[2].GetEntriesNum())
		// sum preserved
		sum := int64(0)
		for _, b := range binlogs {
			sum += b.GetEntriesNum()
		}
		assert.Equal(t, int64(10), sum)
	})

	t.Run("rejects multi field_ids", func(t *testing.T) {
		seg := &BackfillSegment{
			ColumnGroups: []BackfillV2ColumnGroup{
				{FieldIDs: []int64{100, 200}, BinlogFiles: []string{"x"}, RowCount: 1},
			},
		}
		_, err := buildV2Groups("", seg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "field_ids")
	})

	t.Run("rejects empty binlog files", func(t *testing.T) {
		seg := &BackfillSegment{
			ColumnGroups: []BackfillV2ColumnGroup{
				{FieldIDs: []int64{100}, BinlogFiles: nil, RowCount: 1},
			},
		}
		_, err := buildV2Groups("", seg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no binlog files")
	})

	t.Run("rejects non-positive row_count", func(t *testing.T) {
		for _, rc := range []int64{0, -1} {
			seg := &BackfillSegment{
				ColumnGroups: []BackfillV2ColumnGroup{
					{FieldIDs: []int64{100}, BinlogFiles: []string{"x"}, RowCount: rc},
				},
			}
			_, err := buildV2Groups("", seg)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "non-positive row_count")
		}
	})

	t.Run("rejects duplicate field id groups", func(t *testing.T) {
		seg := &BackfillSegment{
			ColumnGroups: []BackfillV2ColumnGroup{
				{FieldIDs: []int64{100}, BinlogFiles: []string{"path/100/1"}, RowCount: 1},
				{FieldIDs: []int64{100}, BinlogFiles: []string{"path/100/2"}, RowCount: 1},
			},
		}
		_, err := buildV2Groups("", seg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate column group")
	})

	t.Run("bucket mismatch bubbles up", func(t *testing.T) {
		seg := &BackfillSegment{
			ColumnGroups: []BackfillV2ColumnGroup{
				{FieldIDs: []int64{100}, BinlogFiles: []string{"s3a://other/foo/7"}, RowCount: 1},
			},
		}
		_, err := buildV2Groups("expected", seg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "differs from datacoord bucket")
	})
}
