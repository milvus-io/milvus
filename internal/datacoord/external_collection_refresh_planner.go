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
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// externalRefreshOwnershipPlanVersion identifies the persisted task contract:
// exclusive segment ownership with task results stored in object storage so
// etcd metadata remains bounded.
//
// Version 3 adds the per-segment baseline manifests (base_manifests) consumed
// as compare-and-swap inputs at finalization. A binary that does not know
// field 24 must not apply version-3 records: without the CAS it would apply
// stale worker results over newer segments as silent lost updates. Keep the
// gate in isSupportedExternalRefreshOwnershipPlanVersion an exact match so an
// older binary fails such records loudly instead of mis-applying them. An
// in-flight version-2 job across the upgrade fails once with "retry refresh"
// and is safe to resubmit.
const externalRefreshOwnershipPlanVersion = int32(3)

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
	files []*datapb.ExternalFileInfo,
	segmentFragments packed.SegmentFragments,
	filesPerTask int64,
) ([]externalRefreshOwnershipTaskPlan, externalRefreshOwnershipSummary, error) {
	summary := externalRefreshOwnershipSummary{}
	if len(files) == 0 {
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

	fileIndexes := make(map[string]int64, len(files))
	for index, file := range files {
		if file == nil || file.GetFilePath() == "" {
			return nil, summary, merr.WrapErrDataIntegrityMsg(
				"external refresh Explore result contains an invalid file at index %d",
				index,
			)
		}
		filePath := file.GetFilePath()
		if previous, ok := fileIndexes[filePath]; ok {
			return nil, summary, merr.WrapErrDataIntegrityMsg(
				"external refresh Explore result contains duplicate file path %q at indexes %d and %d",
				filePath,
				previous,
				index,
			)
		}
		fileIndexes[filePath] = int64(index)
	}

	fileCount := int64(len(files))
	// Use filesPerTask to choose the base task count, then rebalance the files
	// into near-equal chunks instead of leaving one disproportionately small tail.
	chunkCount := int((fileCount-1)/filesPerTask + 1)
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
			// File diff metrics compare unique paths, not fragment row ranges.
			if fragment.FilePath != "" {
				if _, ok := baselineFilePaths[fragment.FilePath]; !ok {
					baselineFilePaths[fragment.FilePath] = struct{}{}
					if _, ok := fileIndexes[fragment.FilePath]; ok {
						summary.UnchangedFilePaths++
					} else {
						summary.RemovedFilePaths++
					}
				}
			}
			if fileIndex, ok := fileIndexes[fragment.FilePath]; ok {
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
	summary.AddedFilePaths = len(fileIndexes) - summary.UnchangedFilePaths
	for _, task := range tasks {
		summary.MaxTaskFiles = max(summary.MaxTaskFiles, int(task.FileIndexEnd-task.FileIndexBegin))
		summary.MaxOwnedSegments = max(summary.MaxOwnedSegments, len(task.OwnedSegmentIDs))
		if len(task.OwnedSegmentIDs) == 0 {
			summary.TasksWithoutOwnedSegments++
		}
	}
	return tasks, summary, nil
}
