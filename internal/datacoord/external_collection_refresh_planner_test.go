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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
)

func externalRefreshPlannerFiles(paths ...string) []packed.FileInfo {
	files := make([]packed.FileInfo, 0, len(paths))
	for _, path := range paths {
		files = append(files, packed.FileInfo{FilePath: path})
	}
	return files
}

func externalRefreshPlannerFragments(paths ...string) []packed.Fragment {
	fragments := make([]packed.Fragment, 0, len(paths))
	for _, path := range paths {
		fragments = append(fragments, packed.Fragment{FilePath: path})
	}
	return fragments
}

func TestSummarizeExternalRefreshOwnership(t *testing.T) {
	tests := []struct {
		name         string
		files        []string
		fragments    packed.SegmentFragments
		filesPerTask int64
		expected     externalRefreshOwnershipSummary
	}{
		{
			name:  "reports_closure_and_file_path_diff",
			files: []string{"f0", "f1", "f2", "f3"},
			fragments: packed.SegmentFragments{
				10: externalRefreshPlannerFragments("f0", "f2"),
				20: externalRefreshPlannerFragments("old"),
				30: externalRefreshPlannerFragments("f3"),
			},
			filesPerTask: 2,
			expected: externalRefreshOwnershipSummary{
				BaseTaskCount:            2,
				FinalTaskCount:           1,
				ClosureRemovedBoundaries: 1,
				MaxTaskFiles:             4,
				MaxOwnedSegments:         3,
				BaselineFilePaths:        4,
				AddedFilePaths:           1,
				RemovedFilePaths:         1,
				UnchangedFilePaths:       3,
			},
		},
		{
			name:         "reports_tasks_without_owned_segments",
			files:        []string{"f0", "f1", "f2", "f3", "f4"},
			fragments:    packed.SegmentFragments{10: externalRefreshPlannerFragments("f0")},
			filesPerTask: 2,
			expected: externalRefreshOwnershipSummary{
				BaseTaskCount:             3,
				FinalTaskCount:            3,
				MaxTaskFiles:              2,
				MaxOwnedSegments:          1,
				TasksWithoutOwnedSegments: 2,
				BaselineFilePaths:         1,
				AddedFilePaths:            4,
				UnchangedFilePaths:        1,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			files := externalRefreshPlannerFiles(test.files...)
			_, summary, err := planExternalRefreshOwnership("", "", files, test.fragments, test.filesPerTask)
			require.NoError(t, err)
			require.Equal(t, test.expected, summary)
		})
	}
}

func TestPlanExternalRefreshOwnership(t *testing.T) {
	tests := []struct {
		name         string
		files        []string
		fragments    packed.SegmentFragments
		filesPerTask int64
		expected     []externalRefreshOwnershipTaskPlan
	}{
		{
			name:         "preserves_balanced_chunks",
			files:        []string{"f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9"},
			fragments:    packed.SegmentFragments{10: externalRefreshPlannerFragments("f4"), 20: externalRefreshPlannerFragments("f5")},
			filesPerTask: 6,
			expected: []externalRefreshOwnershipTaskPlan{
				{FileIndexBegin: 0, FileIndexEnd: 5, OwnedSegmentIDs: []int64{10}},
				{FileIndexBegin: 5, FileIndexEnd: 10, OwnedSegmentIDs: []int64{20}},
			},
		},
		{
			name:         "merges_crossed_chunks",
			files:        []string{"f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11"},
			fragments:    packed.SegmentFragments{10: externalRefreshPlannerFragments("f3", "f4")},
			filesPerTask: 5,
			expected: []externalRefreshOwnershipTaskPlan{
				{FileIndexBegin: 0, FileIndexEnd: 8, OwnedSegmentIDs: []int64{10}},
				{FileIndexBegin: 8, FileIndexEnd: 12},
			},
		},
		{
			name:  "transitive_chunk_merge",
			files: []string{"f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11"},
			fragments: packed.SegmentFragments{
				10: externalRefreshPlannerFragments("f7", "f8"),
				20: externalRefreshPlannerFragments("f3", "f4"),
			},
			filesPerTask: 5,
			expected: []externalRefreshOwnershipTaskPlan{
				{FileIndexBegin: 0, FileIndexEnd: 12, OwnedSegmentIDs: []int64{10, 20}},
			},
		},
		{
			name:  "interleaved_and_shared_files",
			files: []string{"f0", "f1", "f2", "f3", "f4", "f5"},
			fragments: packed.SegmentFragments{
				10: externalRefreshPlannerFragments("f0", "f4"),
				20: externalRefreshPlannerFragments("f1", "f3"),
				30: externalRefreshPlannerFragments("f3", "f3"),
			},
			filesPerTask: 2,
			expected: []externalRefreshOwnershipTaskPlan{
				{FileIndexBegin: 0, FileIndexEnd: 6, OwnedSegmentIDs: []int64{10, 20, 30}},
			},
		},
		{
			name:  "missing_and_conservative_segments",
			files: []string{"f0", "f1", "f2", "f3", "f4", "f5"},
			fragments: packed.SegmentFragments{
				10: externalRefreshPlannerFragments(""),
				30: externalRefreshPlannerFragments("missing"),
				40: externalRefreshPlannerFragments("missing", "f4", "f4"),
			},
			filesPerTask: 2,
			expected: []externalRefreshOwnershipTaskPlan{
				{FileIndexBegin: 0, FileIndexEnd: 2, OwnedSegmentIDs: []int64{10, 30}},
				{FileIndexBegin: 2, FileIndexEnd: 4},
				{FileIndexBegin: 4, FileIndexEnd: 6, OwnedSegmentIDs: []int64{40}},
			},
		},
		{
			name:         "no_baseline",
			files:        []string{"f0", "f1", "f2", "f3", "f4"},
			filesPerTask: 2,
			expected: []externalRefreshOwnershipTaskPlan{
				{FileIndexBegin: 0, FileIndexEnd: 2},
				{FileIndexBegin: 2, FileIndexEnd: 4},
				{FileIndexBegin: 4, FileIndexEnd: 5},
			},
		},
		{
			name:         "large_target_does_not_overflow",
			files:        []string{"f0", "f1", "f2"},
			filesPerTask: int64(9223372036854775807),
			expected:     []externalRefreshOwnershipTaskPlan{{FileIndexBegin: 0, FileIndexEnd: 3}},
		},
		{
			name:         "preserves_explore_order",
			files:        []string{"z", "a", "m"},
			fragments:    packed.SegmentFragments{10: externalRefreshPlannerFragments("a")},
			filesPerTask: 1,
			expected: []externalRefreshOwnershipTaskPlan{
				{FileIndexBegin: 0, FileIndexEnd: 1},
				{FileIndexBegin: 1, FileIndexEnd: 2, OwnedSegmentIDs: []int64{10}},
				{FileIndexBegin: 2, FileIndexEnd: 3},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tasks, _, err := planExternalRefreshOwnership(
				"",
				"",
				externalRefreshPlannerFiles(test.files...),
				test.fragments,
				test.filesPerTask,
			)
			require.NoError(t, err)
			require.Equal(t, test.expected, tasks)
		})
	}
}

func TestPlanExternalRefreshOwnershipRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name         string
		files        []packed.FileInfo
		fragments    packed.SegmentFragments
		filesPerTask int64
		errContains  string
	}{
		{name: "duplicate_file_path", files: externalRefreshPlannerFiles("f0", "f1", "f0"), filesPerTask: 1, errContains: "duplicate file path"},
		{name: "empty_explored_files", filesPerTask: 1, errContains: "at least one explored file"},
		{name: "non_positive_files_per_task", files: externalRefreshPlannerFiles("f0"), errContains: "must be positive"},
		{name: "invalid_explored_file", files: []packed.FileInfo{{}}, filesPerTask: 1, errContains: "invalid file"},
		{name: "non_positive_baseline_segment", files: externalRefreshPlannerFiles("f0"), fragments: packed.SegmentFragments{0: nil}, filesPerTask: 1, errContains: "non-positive segment ID"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := planExternalRefreshOwnership("", "", test.files, test.fragments, test.filesPerTask)
			require.ErrorContains(t, err, test.errContains)
		})
	}
}

func TestPlanExternalRefreshOwnershipMultipleDataSplits(t *testing.T) {
	files := []packed.FileInfo{
		externalRefreshPlannerDataSplit(2, "split-a"),
		externalRefreshPlannerDataSplit(2, "split-b"),
		externalRefreshPlannerDataSplit(2, "split-c"),
	}
	fragment := func(file packed.FileInfo) packed.Fragment {
		return packed.Fragment{FilePath: file.FilePath, EndRow: file.NumRows, RowCount: file.NumRows, Properties: file.Properties}
	}
	for _, test := range []struct {
		name      string
		fragments packed.SegmentFragments
		expected  []externalRefreshOwnershipTaskPlan
	}{
		{
			name: "initial_refresh",
			expected: []externalRefreshOwnershipTaskPlan{
				{FileIndexBegin: 0, FileIndexEnd: 1},
				{FileIndexBegin: 1, FileIndexEnd: 2},
				{FileIndexBegin: 2, FileIndexEnd: 3},
			},
		},
		{
			name:      "unchanged_split_matches_only_its_descriptor",
			fragments: packed.SegmentFragments{10: {fragment(files[1])}},
			expected: []externalRefreshOwnershipTaskPlan{
				{FileIndexBegin: 0, FileIndexEnd: 1},
				{FileIndexBegin: 1, FileIndexEnd: 2, OwnedSegmentIDs: []int64{10}},
				{FileIndexBegin: 2, FileIndexEnd: 3},
			},
		},
		{
			name:      "old_segment_spans_task_boundary",
			fragments: packed.SegmentFragments{10: {fragment(files[0]), fragment(files[1])}},
			expected: []externalRefreshOwnershipTaskPlan{
				{FileIndexBegin: 0, FileIndexEnd: 2, OwnedSegmentIDs: []int64{10}},
				{FileIndexBegin: 2, FileIndexEnd: 3},
			},
		},
		{
			name: "replaced_or_removed_split_is_owned_once",
			fragments: packed.SegmentFragments{
				10: {fragment(externalRefreshPlannerDataSplit(1, "split-b"))},
				20: {fragment(externalRefreshPlannerDataSplit(2, "removed-split"))},
			},
			expected: []externalRefreshOwnershipTaskPlan{
				{FileIndexBegin: 0, FileIndexEnd: 1, OwnedSegmentIDs: []int64{10, 20}},
				{FileIndexBegin: 1, FileIndexEnd: 2},
				{FileIndexBegin: 2, FileIndexEnd: 3},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			tasks, summary, err := planExternalRefreshOwnership(externalspec.FormatPaimonTable, externalspec.FormatPaimonTable, files, test.fragments, 1)
			require.NoError(t, err)
			require.Equal(t, test.expected, tasks)
			// Several descriptors still refer to only one physical table path.
			require.Equal(t, 1, summary.AddedFilePaths+summary.UnchangedFilePaths)
		})
	}
	t.Run("duplicate_descriptor", func(t *testing.T) {
		_, _, err := planExternalRefreshOwnership(externalspec.FormatPaimonTable, externalspec.FormatPaimonTable, []packed.FileInfo{files[0], files[0]}, nil, 1)
		require.ErrorContains(t, err, "duplicate file path")
	})
	t.Run("mixed_direct_file_and_data_split", func(t *testing.T) {
		direct := packed.FileInfo{
			FilePath: "s3://endpoint/bucket/table/a.parquet", NumRows: 10,
			Properties: map[string]string{"metadata": `{"version":1,"read_path":"direct-file","record_count":10}`},
		}
		mixed := append(append([]packed.FileInfo(nil), files...), direct)
		tasks, _, err := planExternalRefreshOwnership(externalspec.FormatPaimonTable, externalspec.FormatPaimonTable, mixed,
			packed.SegmentFragments{10: {fragment(files[1]), fragment(direct)}}, 1)
		require.NoError(t, err)
		require.Equal(t, []externalRefreshOwnershipTaskPlan{
			{FileIndexBegin: 0, FileIndexEnd: 1},
			{FileIndexBegin: 1, FileIndexEnd: 4, OwnedSegmentIDs: []int64{10}},
		}, tasks)
	})
	t.Run("malformed_descriptor", func(t *testing.T) {
		invalid := files[0]
		invalid.Properties = map[string]string{"metadata": "not-json"}
		_, _, err := planExternalRefreshOwnership(externalspec.FormatPaimonTable, externalspec.FormatPaimonTable, []packed.FileInfo{invalid}, nil, 1)
		require.ErrorContains(t, err, "invalid Paimon metadata descriptor")
		_, _, err = planExternalRefreshOwnership(externalspec.FormatPaimonTable, externalspec.FormatPaimonTable, files,
			packed.SegmentFragments{10: {fragment(invalid)}}, 1)
		require.ErrorContains(t, err, "invalid Paimon metadata descriptor")
	})
}

func externalRefreshPlannerDataSplit(snapshot int64, payload string) packed.FileInfo {
	// The planner must treat the payload as opaque; decoding it belongs to Storage.
	return packed.FileInfo{
		FilePath: "s3://endpoint/bucket/table", NumRows: 10,
		Properties: map[string]string{
			"metadata": fmt.Sprintf(`{"version":1,"read_path":"data-split","data_format":"paimon","bucket":0,"snapshot_id":%d,"record_count":10,"table_location":"s3://bucket/table","codec":"paimon-split","payload":%q}`, snapshot, payload),
		},
	}
}

func TestPlanExternalRefreshOwnershipFileBasedFormats(t *testing.T) {
	for _, format := range []string{
		externalspec.FormatParquet, externalspec.FormatVortex,
		externalspec.FormatIcebergTable, externalspec.FormatLanceTable, externalspec.FormatPaimonTable,
	} {
		t.Run(format, func(t *testing.T) {
			oldProperties := map[string]string{"metadata": `{"version":1,"read_path":"direct-file","snapshot_id":1,"record_count":20}`}
			newProperties := map[string]string{"metadata": `{"version":1,"read_path":"direct-file","snapshot_id":2,"record_count":20}`}
			if format != externalspec.FormatPaimonTable {
				oldProperties = map[string]string{"metadata": "opaque-old-metadata"}
				newProperties = map[string]string{"metadata": "opaque-new-metadata"}
			}
			files := externalRefreshPlannerFiles("a.parquet", "b.parquet", "c.parquet")
			for i := range files {
				files[i].Properties = newProperties
			}
			fragments := packed.SegmentFragments{10: {
				{FilePath: "b.parquet", StartRow: 0, EndRow: 10, Properties: oldProperties},
				{FilePath: "b.parquet", StartRow: 10, EndRow: 20, Properties: oldProperties},
				{FilePath: "c.parquet", StartRow: 0, EndRow: 10, Properties: oldProperties},
			}}
			tasks, _, err := planExternalRefreshOwnership(format, format, files, fragments, 1)
			require.NoError(t, err)
			require.Equal(t, []externalRefreshOwnershipTaskPlan{
				{FileIndexBegin: 0, FileIndexEnd: 1},
				{FileIndexBegin: 1, FileIndexEnd: 3, OwnedSegmentIDs: []int64{10}},
			}, tasks)
			_, _, err = planExternalRefreshOwnership(format, format, append(files, files[0]), nil, 1)
			require.ErrorContains(t, err, "duplicate file path")
		})
	}
}

func TestPlanExternalRefreshOwnershipCrossFormatToPaimon(t *testing.T) {
	direct := packed.FileInfo{
		FilePath: "s3://endpoint/bucket/table/data.parquet", NumRows: 10,
		Properties: map[string]string{"metadata": `{"version":1,"read_path":"direct-file","record_count":10}`},
	}
	for _, baselineFormat := range []string{externalspec.FormatParquet, externalspec.FormatIcebergTable, externalspec.FormatLanceTable} {
		for _, file := range []packed.FileInfo{direct, externalRefreshPlannerDataSplit(1, "split-a")} {
			readPath, _, err := packed.PaimonRoutingMeta(file.Properties)
			require.NoError(t, err)
			for _, samePath := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/same_path=%t", baselineFormat, readPath, samePath), func(t *testing.T) {
					old := packed.Fragment{FilePath: "s3://endpoint/bucket/old/data.parquet", EndRow: 10, RowCount: 10}
					if baselineFormat != externalspec.FormatParquet {
						old.Properties = map[string]string{"metadata": "opaque-format-metadata"}
					}
					if samePath {
						old.FilePath = file.FilePath
					}
					files := []packed.FileInfo{externalRefreshPlannerDataSplit(1, "other-split"), file}
					tasks, _, err := planExternalRefreshOwnership(externalspec.FormatPaimonTable, baselineFormat,
						files, packed.SegmentFragments{10: {old}}, 1)
					require.NoError(t, err)
					expected := []externalRefreshOwnershipTaskPlan{
						{FileIndexBegin: 0, FileIndexEnd: 1},
						{FileIndexBegin: 1, FileIndexEnd: 2},
					}
					owner := 0
					if samePath && readPath == "direct-file" {
						owner = 1
					}
					expected[owner].OwnedSegmentIDs = []int64{10}
					require.Equal(t, expected, tasks)
				})
			}
		}
	}
}
