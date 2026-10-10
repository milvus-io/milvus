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

// This file owns the reshard stage: turning a job's source files into
// ReshardTasks, plus the pure packing and slot-sizing helpers that stage
// builds on. It is recovery-safe: files already covered by an existing task
// are skipped, so a crash between task creation and the job-state write
// replays cleanly.

import (
	"math"
	"sort"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/importutilv2/reshardmem"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// createReshardTasks groups the not-yet-covered source files and creates one
// ReshardTask per group, then advances the job to Resharding. The create-then-
// transition order matters: the tasks are persisted before the job state write
// so a crash in between replays cleanly.
func createReshardTasks(jc *importV3JobContext) error {
	job := jc.job
	files := job.GetFiles()
	// Planning only validates that the schema yields a sort order; dispatch
	// re-derives it from the frozen job schema.
	if _, err := importutilv2.SortFieldIDs(job.GetSchema()); err != nil {
		return err
	}
	covered, err := coveredSourceIDs(jc)
	if err != nil {
		return err
	}
	missingFiles := lo.Filter(files, func(file *internalpb.ImportFile, _ int) bool {
		_, ok := covered[file.GetId()]
		return !ok
	})
	// Nothing to plan when every source already has a task: skip the BFD
	// grouping entirely so a covered-retry only advances the job state.
	var missing [][]reshardSource
	if len(missingFiles) > 0 {
		missing, err = groupSources(jc, missingFiles)
		if err != nil {
			return err
		}
	}
	if len(missing) > 0 {
		start, _, err := jc.alloc.AllocN(int64(len(missing)))
		if err != nil {
			return err
		}
		for i, sources := range missing {
			if err := createTask(jc, start+int64(i), sources); err != nil {
				return err
			}
		}
	}
	return jc.transitionTo(internalpb.ImportJobState_Resharding)
}

func createTask(jc *importV3JobContext, taskID int64, sources []reshardSource) error {
	job := jc.job
	sourceIDs := lo.Map(sources, func(s reshardSource, _ int) int64 { return s.file.GetId() })
	if _, err := importFragmentSizeBytes(getImportV3Config()); err != nil {
		return err
	}
	task := newReshardTask(&datapb.ReshardTask{
		JobId:        job.GetJobID(),
		TaskId:       taskID,
		CollectionId: job.GetCollectionID(),
		State:        datapb.ImportTaskStateV2_Pending,
		RunId:        1,
		NodeId:       NullNodeID,
		FileIds:      sourceIDs,
		CreatedTime:  time.Now().Format(time.RFC3339),
	}, jc.importMeta, jc.meta, jc.alloc)
	return jc.importMeta.AddTask(jc.ctx, task)
}

// coveredSourceIDs returns the source file IDs already owned by the job's
// ReshardTasks, validating the task set against the job's file list.
func coveredSourceIDs(jc *importV3JobContext) (map[int64]struct{}, error) {
	job := jc.job
	tasks := jc.importMeta.GetTaskByJob(jc.ctx, job.GetJobID(), WithType(ReshardTaskType))
	jobFiles := make(map[int64]struct{}, len(job.GetFiles()))
	for _, file := range job.GetFiles() {
		jobFiles[file.GetId()] = struct{}{}
	}
	covered := make(map[int64]struct{}, len(jobFiles))
	for _, generic := range tasks {
		task, ok := generic.(*reshardTask)
		if !ok {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 reshard task set contains an unexpected task type")
		}
		if task.GetState() == datapb.ImportTaskStateV2_Failed {
			return nil, merr.WrapErrImportSysFailedMsg("import v3 reshard task %d failed: %s", task.GetTaskID(), task.GetReason())
		}
		t := task.task.Load()
		if t.GetCollectionId() != job.GetCollectionID() {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 reshard task identity mismatch")
		}
		if len(t.GetFileIds()) == 0 {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 reshard task has no input file")
		}
		for _, fileID := range t.GetFileIds() {
			if _, ok := jobFiles[fileID]; !ok {
				return nil, merr.WrapErrDataIntegrityMsg("import v3 reshard task contains file %d outside the job", fileID)
			}
			if _, ok := covered[fileID]; ok {
				return nil, merr.WrapErrDataIntegrityMsg("import v3 reshard task set contains duplicate file %d", fileID)
			}
			covered[fileID] = struct{}{}
		}
	}
	return covered, nil
}

type reshardSource struct {
	file *internalpb.ImportFile
	size int64
}

// groupSources implements stable one-dimensional BFD. ImportFile is the atom:
// no path/file splitting, no backtracking, and an oversized file owns one bin.
// Equal-size files keep their job ordinal; equal-fit bins keep their creation
// ordinal. Only the grouped sources are returned; bin sizes are a transient
// packing detail.
//
// The bin target mirrors RegroupImportFiles: one task holds at most one
// segment's worth of data per (vchannel, partition) bucket, capped by
// MaxSizeInMBPerImportTask. Import V3 never handles L0, so the segment target
// comes straight from getExpectedSegmentSize.
//
// Source size comes from the job's PreImportTaskV3 stats (preImportV3PackingSizes):
// per file, the decoded total_memory_size when the worker measured one, else the
// physical file_size. Both are measured on the DataNode — the backup path sizes
// its expanded insert+delta object list without reading any content — so
// DataCoord makes no object-store call while packing.
func groupSources(jc *importV3JobContext, files []*internalpb.ImportFile) ([][]reshardSource, error) {
	ctx, job := jc.ctx, jc.job
	sizesByFile := preImportV3PackingSizes(jc.importMeta.GetTaskByJob(ctx, job.GetJobID(), WithType(PreImportTaskV3Type)))
	sources := make([]reshardSource, 0, len(files))
	for _, file := range files {
		sources = append(sources, reshardSource{file: file, size: sizesByFile[file.GetId()]})
	}
	sort.SliceStable(sources, func(i, j int) bool {
		return sources[i].size > sources[j].size
	})
	cfg := getImportV3Config()
	fragmentTarget := cfg.fragmentSizeInMB * 1024 * 1024
	threshold := cfg.maxSizeInMBPerImportTask * 1024 * 1024
	target := min(fragmentTarget*int64(len(job.GetVchannels()))*int64(len(job.GetPartitionIDs())*2), threshold)
	if target <= 0 {
		return nil, merr.WrapErrImportSysFailedMsg("import v3 reshard BFD target must be positive")
	}
	type bin struct {
		sources []reshardSource
		size    int64
	}
	bins := make([]bin, 0)
	for _, source := range sources {
		best := -1
		bestRemaining := int64(math.MaxInt64)
		if source.size <= target {
			for i := range bins {
				remaining := target - bins[i].size - source.size
				if remaining >= 0 && remaining < bestRemaining {
					best, bestRemaining = i, remaining
				}
			}
		}
		if best < 0 {
			bins = append(bins, bin{sources: []reshardSource{source}, size: source.size})
			continue
		}
		bins[best].sources = append(bins[best].sources, source)
		bins[best].size += source.size
	}
	return lo.Map(bins, func(b bin, _ int) []reshardSource { return b.sources }), nil
}

func calculateImportV3Slots(workingSet, memoryPerSlot int64) int64 {
	if memoryPerSlot <= 0 {
		// A zero or negative slot size is a configuration error. Keep the slot
		// helper total and fail closed instead of dividing by zero; paramtable
		// startup validation is the primary guard for the real config value.
		return 1
	}
	return max((workingSet+memoryPerSlot-1)/memoryPerSlot, 1)
}

// calculateReshardTaskSlot converts the shared reshard memory model
// (importutilv2/reshardmem -- the single source of truth also used by the
// DataNode runtime) into slots. The charge covers the GC-scaled resident set
// including the structural overhead of shredded fragments (nFields is the
// temporary schema's field count, the same schema the DataNode accounts
// with), the prepare pipeline and one sort copy. The DataNode's per-bucket
// tail cap bounds the resident accounting at min(buckets, cap) x F, and its
// dynamic free-memory checkpoint spills below that ceiling whenever the real
// process memory is tighter than the per-task budgets assumed.
func calculateReshardTaskSlot(mem reshardmem.Model, memoryPerSlot, buckets, bucketCap, nFields int64) int64 {
	return calculateImportV3Slots(mem.WorkingSet(buckets, bucketCap, nFields), memoryPerSlot)
}

func calculateImportTaskV3Slot(readBuffer, writerBuffer, memoryPerSlot int64, fanIn int) int64 {
	workingSet := int64(fanIn)*2*readBuffer + 3*readBuffer + writerBuffer
	return calculateImportV3Slots(workingSet, memoryPerSlot)
}
