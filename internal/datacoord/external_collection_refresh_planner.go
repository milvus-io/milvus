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
	"sort"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// externalRefreshOwnershipPlanVersion identifies the persisted task contract:
// exclusive segment ownership with task results stored in object storage so
// etcd metadata remains bounded.
const externalRefreshOwnershipPlanVersion = int32(2)

func isSupportedExternalRefreshOwnershipPlanVersion(version int32) bool {
	return version == externalRefreshOwnershipPlanVersion
}

// externalRefreshOwnershipTaskPlan is the immutable ownership assigned to one
// DataNode task before task IDs are allocated and persisted.
type externalRefreshOwnershipTaskPlan struct {
	FileIndexBegin  int64
	FileIndexEnd    int64
	OwnedSegmentIDs []int64
}

// externalRefreshOwnershipSummary contains job-level planning counters used
// only for logs and operational diagnostics.
type externalRefreshOwnershipSummary struct {
	BaseTaskCount             int
	FinalTaskCount            int
	ClosureRemovedBoundaries  int
	MaxTaskFiles              int
	MaxOwnedSegments          int
	TasksWithoutOwnedSegments int
	BaselineFilePaths         int
	AddedFilePaths            int
	RemovedFilePaths          int
	UnchangedFilePaths        int
}

// planExternalRefreshOwnership assigns each explored file index and baseline
// segment to exactly one continuous task range. filesPerTask determines the
// balanced base ranges; segment references then merge boundaries until every
// surviving file of a segment is visible to its owner task. Manifest I/O and
// task/segment ID allocation deliberately stay outside this pure planner.
func planExternalRefreshOwnership(
	format, baselineFormat string,
	files []packed.FileInfo,
	segmentFragments packed.SegmentFragments,
	filesPerTask int64,
) ([]externalRefreshOwnershipTaskPlan, externalRefreshOwnershipSummary, error) {
	summary := externalRefreshOwnershipSummary{}
	if len(files) == 0 && format != externalspec.FormatPaimonTable {
		return nil, summary, merr.WrapErrServiceInternalMsg(
			"external refresh ownership planning requires at least one explored file",
		)
	}
	if filesPerTask <= 0 {
		return nil, summary, merr.WrapErrServiceInternalMsg(
			"external refresh files per task must be positive, got %d",
			filesPerTask,
		)
	}

	// Files are keyed by ManifestPathKey, matching the DataNode's fragment
	// identity, so a milvus-table source manifest is matched by revision.
	fileIndexes := make(map[externalRefreshFileKey]int64, len(files))
	filePaths := make(map[string]struct{}, len(files))
	for index, file := range files {
		if file.FilePath == "" {
			return nil, summary, merr.WrapErrDataIntegrityMsg(
				"external refresh Explore result contains an invalid file at index %d",
				index,
			)
		}
		key, err := externalRefreshOwnershipKey(format, file.FilePath, file.Properties)
		if err != nil {
			return nil, summary, err
		}
		if previous, ok := fileIndexes[key]; ok {
			return nil, summary, merr.WrapErrDataIntegrityMsg(
				"external refresh Explore result contains duplicate file path %q at indexes %d and %d",
				file.FilePath,
				previous,
				index,
			)
		}
		fileIndexes[key] = int64(index)
		filePaths[key.path] = struct{}{}
	}

	fileCount := int64(len(files))
	// Use filesPerTask to choose the base task count, then rebalance the files
	// into near-equal chunks instead of leaving one disproportionately small tail.
	// An empty Paimon snapshot still needs one task to retire its old segments.
	chunkCount := max(1, int((fileCount-1)/filesPerTask+1))
	summary.BaseTaskCount = chunkCount
	chunkSize := (fileCount-1)/int64(chunkCount) + 1
	mergeUntil := make([]int, chunkCount)
	baselineFilePaths := make(map[string]struct{})

	segmentIDs := make([]int64, 0, len(segmentFragments))
	for segmentID := range segmentFragments {
		segmentIDs = append(segmentIDs, segmentID)
	}
	sort.Slice(segmentIDs, func(i, j int) bool { return segmentIDs[i] < segmentIDs[j] })
	ownerChunks := make([]int, 0, len(segmentIDs))
	// Each baseline segment constrains the base chunks containing its first and
	// last surviving file to one task. If none of its old files survive, the
	// zero-value chunk 0 owns it so exactly one task can classify it as removed.
	for _, segmentID := range segmentIDs {
		if segmentID <= 0 {
			return nil, summary, merr.WrapErrDataIntegrityMsg(
				"external refresh baseline contains non-positive segment ID %d",
				segmentID,
			)
		}

		firstFileIndex := fileCount
		lastFileIndex := int64(-1)
		for _, fragment := range segmentFragments[segmentID] {
			filePath := packed.ManifestPathKey(fragment.FilePath)
			// File diff metrics compare unique paths, not fragment row ranges.
			if filePath != "" {
				if _, ok := baselineFilePaths[filePath]; !ok {
					baselineFilePaths[filePath] = struct{}{}
					if _, ok := filePaths[filePath]; ok {
						summary.UnchangedFilePaths++
					} else {
						summary.RemovedFilePaths++
					}
				}
			}
			key, err := externalRefreshOwnershipKey(baselineFormat, fragment.FilePath, fragment.Properties)
			if err != nil {
				return nil, summary, err
			}
			if fileIndex, ok := fileIndexes[key]; ok {
				firstFileIndex = min(firstFileIndex, fileIndex)
				lastFileIndex = max(lastFileIndex, fileIndex)
			}
		}
		firstChunk := 0
		lastChunk := 0
		if lastFileIndex >= 0 {
			firstChunk = int(firstFileIndex / chunkSize)
			lastChunk = int(lastFileIndex / chunkSize)
		}
		ownerChunks = append(ownerChunks, firstChunk)
		mergeUntil[firstChunk] = max(mergeUntil[firstChunk], lastChunk)
	}

	chunkTasks := make([]int, chunkCount)
	tasks := make([]externalRefreshOwnershipTaskPlan, 0, chunkCount)
	// Compute transitive interval closure. Entering a chunk already included in
	// the current range can extend lastChunk, so scan until no included chunk
	// requires a later boundary.
	for firstChunk := 0; firstChunk < chunkCount; {
		lastChunk := firstChunk
		for chunk := firstChunk; chunk <= lastChunk; chunk++ {
			lastChunk = max(lastChunk, mergeUntil[chunk])
		}

		taskIndex := len(tasks)
		for chunk := firstChunk; chunk <= lastChunk; chunk++ {
			chunkTasks[chunk] = taskIndex
		}

		fileIndexEnd := min(int64(lastChunk+1)*chunkSize, fileCount)
		tasks = append(tasks, externalRefreshOwnershipTaskPlan{
			FileIndexBegin: int64(firstChunk) * chunkSize,
			FileIndexEnd:   fileIndexEnd,
		})
		firstChunk = lastChunk + 1
	}
	for index, ownerChunk := range ownerChunks {
		taskIndex := chunkTasks[ownerChunk]
		tasks[taskIndex].OwnedSegmentIDs = append(tasks[taskIndex].OwnedSegmentIDs, segmentIDs[index])
	}

	summary.FinalTaskCount = len(tasks)
	summary.ClosureRemovedBoundaries = summary.BaseTaskCount - summary.FinalTaskCount
	summary.BaselineFilePaths = len(baselineFilePaths)
	summary.AddedFilePaths = len(filePaths) - summary.UnchangedFilePaths
	for _, task := range tasks {
		summary.MaxTaskFiles = max(summary.MaxTaskFiles, int(task.FileIndexEnd-task.FileIndexBegin))
		summary.MaxOwnedSegments = max(summary.MaxOwnedSegments, len(task.OwnedSegmentIDs))
		if len(task.OwnedSegmentIDs) == 0 {
			summary.TasksWithoutOwnedSegments++
		}
	}
	return tasks, summary, nil
}

type externalRefreshFileKey struct {
	path          string
	splitIdentity string
}

func externalRefreshOwnershipKey(format, filePath string, properties map[string]string) (externalRefreshFileKey, error) {
	key := externalRefreshFileKey{path: packed.ManifestPathKey(filePath)}
	if format != externalspec.FormatPaimonTable || filePath == "" {
		return key, nil
	}
	readPath, _, err := packed.PaimonRoutingMeta(properties)
	if err != nil {
		return key, err
	}
	if readPath == packed.PaimonDataSplitReadPath {
		// DataSplits share a table path but carry distinct immutable descriptors.
		// Leave row ranges unset: ownership is per entry, not per fragment range.
		key.splitIdentity = packed.FragmentIdentity(packed.Fragment{FilePath: filePath, Properties: properties})
	}
	return key, nil
}
