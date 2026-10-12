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

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func externalRefreshPlannerFiles(paths ...string) []*datapb.ExternalFileInfo {
	files := make([]*datapb.ExternalFileInfo, 0, len(paths))
	for _, path := range paths {
		files = append(files, &datapb.ExternalFileInfo{FilePath: path})
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
			_, summary, err := planExternalRefreshOwnership(files, test.fragments, test.filesPerTask)
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
		files        []*datapb.ExternalFileInfo
		fragments    packed.SegmentFragments
		filesPerTask int64
		errContains  string
	}{
		{name: "duplicate_file_path", files: externalRefreshPlannerFiles("f0", "f1", "f0"), filesPerTask: 1, errContains: "duplicate file path"},
		{name: "empty_explored_files", filesPerTask: 1, errContains: "at least one explored file"},
		{name: "non_positive_files_per_task", files: externalRefreshPlannerFiles("f0"), errContains: "must be positive"},
		{name: "invalid_explored_file", files: []*datapb.ExternalFileInfo{nil}, filesPerTask: 1, errContains: "invalid file"},
		{name: "non_positive_baseline_segment", files: externalRefreshPlannerFiles("f0"), fragments: packed.SegmentFragments{0: nil}, filesPerTask: 1, errContains: "non-positive segment ID"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := planExternalRefreshOwnership(test.files, test.fragments, test.filesPerTask)
			require.ErrorContains(t, err, test.errContains)
		})
	}
}
