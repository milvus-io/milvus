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

// This file is the count-only preimport phase of an ordinary Import V3 job: it
// creates the PreImportV3 tasks, reads their per-file counts back, and drives
// the two-phase ID-range assignment. The two preimport state handlers
// (PreImporting and AssigningIDRange) share the tail here.

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/util/importutilv2/importid"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
)

// importV3NeedsIDRanges reports whether the V3 job needs per-file ID ranges: it has a
// resolvable primary key and is neither a backup import (which keeps embedded
// PK/RowID/ts) nor an L0 import. Unlike the V2 path this is deliberately not
// gated by dataCoord.import.enableIDRangeMsg: enableImportV3 already guarantees
// the whole cluster understands the UpdateImport message, and V3 has no
// local-allocator fallback that keeps PK/RowID identical across clusters.
func importV3NeedsIDRanges(job ImportJob) bool {
	return importid.NeedsFileIDRanges(job.GetSchema(), job.GetOptions())
}

// createPreImportV3Tasks groups the not-yet-covered files and creates one
// count-only PreImportV3 task per group, then advances the job to PreImporting.
// It is recovery-safe: files already covered by an existing PreImportV3 task
// are skipped, so a crash between task creation and the state write replays
// cleanly.
func createPreImportV3Tasks(jc *importV3JobContext) error {
	lacks := preImportV3LackFiles(jc)
	if len(lacks) == 0 {
		if jc.job.GetState() == internalpb.ImportJobState_Pending {
			return jc.transitionTo(internalpb.ImportJobState_PreImporting)
		}
		return nil
	}
	fileGroups := lo.Chunk(lacks, Params.DataCoordCfg.FilesPerPreImportTask.GetAsInt())
	idStart, _, err := jc.alloc.AllocN(int64(len(fileGroups)))
	if err != nil {
		return err
	}
	tasks := make([]ImportTask, 0, len(fileGroups))
	for i, files := range fileGroups {
		fileStats := lo.Map(files, func(f *internalpb.ImportFile, _ int) *datapb.ImportV3FileStats {
			return &datapb.ImportV3FileStats{FileId: f.GetId()}
		})
		tasks = append(tasks, newPreImportTaskV3(&datapb.PreImportTaskV3{
			JobId:        jc.job.GetJobID(),
			TaskId:       idStart + int64(i),
			CollectionId: jc.job.GetCollectionID(),
			State:        datapb.ImportTaskStateV2_Pending,
			FileStats:    fileStats,
			CreatedTime:  time.Now().Format(time.RFC3339),
		}, jc.importMeta))
	}
	for _, task := range tasks {
		if err := jc.importMeta.AddTask(jc.ctx, task); err != nil {
			return err
		}
	}
	return jc.transitionTo(internalpb.ImportJobState_PreImporting)
}

// preImportV3LackFiles returns the job's files that no PreImportV3 task covers
// yet, in the job's own file order, so the grouping (and therefore the task
// layout) is deterministic across ticks and recovery.
func preImportV3LackFiles(jc *importV3JobContext) []*internalpb.ImportFile {
	covered := make(map[int64]struct{})
	for _, task := range jc.importMeta.GetTaskByJob(jc.ctx, jc.job.GetJobID(), WithType(PreImportTaskV3Type)) {
		for _, stat := range task.(*preImportV3Task).GetV3FileStats() {
			covered[stat.GetFileId()] = struct{}{}
		}
	}
	lacks := make([]*internalpb.ImportFile, 0, len(jc.job.GetFiles()))
	for _, file := range jc.job.GetFiles() {
		if _, ok := covered[file.GetId()]; !ok {
			lacks = append(lacks, file)
		}
	}
	return lacks
}

// ensurePreImportV3IDRanges allocates the exact per-file ID ranges on the
// primary and broadcasts the UpdateImport message; a secondary waits for the
// replicated copy. It mirrors the V2 ensureIDRanges but reads the exact counts
// from the V3 count-only PreImportV3 tasks. Its error semantics are local to
// this wait state (role flips and transient broadcast failures retry; only
// caller-input defects fail the job), so it manages them instead of returning
// an error.
func ensurePreImportV3IDRanges(jc *importV3JobContext) {
	ctx, cancel := context.WithTimeout(jc.ctx, 10*time.Second)
	defer cancel()

	var role replicateutil.Role
	replicating := false
	if jc.hooks.getReplicationRole != nil {
		r, rep, err := jc.hooks.getReplicationRole(ctx)
		if err != nil {
			jc.log.Warn(ctx, "cannot determine replication role before UpdateImport broadcast, will retry next tick", mlog.Err(err))
			return
		}
		role, replicating = r, rep
	}
	if replicating && role == replicateutil.RoleSecondary {
		jc.log.Debug(ctx, "waiting for replicated UpdateImport")
		return
	}
	if jc.hooks.assignImportIDRange == nil {
		jc.log.Error(ctx, "assignImportIDRange hook is nil but import requires ID ranges; this is a programming error")
		return
	}

	tasks := jc.importMeta.GetTaskByJob(jc.ctx, jc.job.GetJobID(), WithType(PreImportTaskV3Type))
	fileRows, ok := preImportV3FileRows(jc.ctx, jc.job, tasks, jc.log)
	if !ok {
		return
	}
	jc.log.Info(ctx, "triggering UpdateImport broadcast",
		mlog.Int("fileCount", len(jc.job.GetFiles())), mlog.Int64("totalRows", lo.Sum(fileRows)))

	if err := jc.hooks.assignImportIDRange(ctx, jc.job, fileRows); err != nil {
		if errors.Is(err, broadcaster.ErrNotPrimary) {
			jc.log.Info(ctx, "role flipped to standby while importing, waiting for replicated UpdateImport")
		} else if merr.GetErrorType(err) == merr.InputError {
			jc.failJob(err.Error())
		} else {
			jc.log.Warn(ctx, "UpdateImport broadcast failed, will retry next tick", mlog.Err(err))
		}
		return
	}
}

// finishPreImportV3 runs the tail shared by "preimport complete, ranges
// present" (or not needed): the cross-cluster divergence gate, the zero-rows
// exit, then Resharding. The PreImporting and AssigningIDRange states both end
// here, so the PreImport stage latency is attributed to this single, correct
// label.
func finishPreImportV3(jc *importV3JobContext) error {
	tasks := jc.importMeta.GetTaskByJob(jc.ctx, jc.job.GetJobID(), WithType(PreImportTaskV3Type))

	// Divergence gate: for every ranged file the local preimport count must equal
	// the size of the range the primary allocated. A mismatch means the same file
	// holds different rows on the two clusters, so fail loudly with both numbers
	// before any segment is written.
	fileRows, ok := preImportV3FileRows(jc.ctx, jc.job, tasks, jc.log)
	if !ok {
		return nil
	}
	if importV3NeedsIDRanges(jc.job) {
		for i, f := range jc.job.GetFiles() {
			r := f.GetIdRange()
			if r == nil {
				continue
			}
			if fileRows[i] != r.GetEnd()-r.GetBegin() {
				reason := fmt.Sprintf("import file %d local row count %d does not match the reserved ID range size %d (cross-cluster file divergence)",
					f.GetId(), fileRows[i], r.GetEnd()-r.GetBegin())
				jc.log.Warn(jc.ctx, "UpdateImport divergence; failing import", mlog.String("reason", reason))
				jc.failJob(reason)
				return nil
			}
		}
	}

	// Zero-rows exit: nothing to read, no Reshard task and no cursor is needed.
	// It runs after the divergence gate, so a locally empty side still has to
	// match the peer's ranges before it may commit as empty. It is gated on
	// importV3NeedsIDRanges because only an ordinary (ranged) import's zero count
	// means empty input: a backup job legitimately reports zero rows and is sized
	// on the DataNode, so it must fall through to Reshard.
	var totalRows int64
	for _, rows := range fileRows {
		totalRows += rows
	}
	if totalRows == 0 && importV3NeedsIDRanges(jc.job) {
		_ = jc.finishEmptyJob()
		return nil
	}

	if err := createReshardTasks(jc); err != nil {
		return err
	}
	return nil
}

// preImportV3FileRows returns the per-file row counts aligned with
// job.GetFiles() order, summed from the completed PreImportV3 task stats by
// fileID. A file with no stat is an internal error; ok=false tells the caller
// to retry next tick.
func preImportV3FileRows(ctx context.Context, job ImportJob, tasks []ImportTask, log *mlog.Logger) ([]int64, bool) {
	rowsByFile := make(map[int64]int64)
	for _, t := range tasks {
		for _, stat := range t.(*preImportV3Task).GetV3FileStats() {
			rowsByFile[stat.GetFileId()] += stat.GetTotalRows()
		}
	}
	files := job.GetFiles()
	if f, missing := lo.Find(files, func(f *internalpb.ImportFile) bool {
		_, ok := rowsByFile[f.GetId()]
		return !ok
	}); missing {
		log.Warn(ctx, "preimport v3 stats missing for an import file, will retry next tick", mlog.Int64("fileID", f.GetId()))
		return nil, false
	}
	return lo.Map(files, func(f *internalpb.ImportFile, _ int) int64 {
		return rowsByFile[f.GetId()]
	}), true
}

// preImportV3PackingSizes returns the BFD packing size per file (summed from the
// PreImportV3 task stats by fileID): the decoded total_memory_size when the
// worker measured one, else the physical file_size. The ordinary worker reports
// a decoded size (metadata estimate or scan); the backup worker reports a
// size-only stat (file_size, zero rows) measured from its expanded object list.
// Both are measured on the DataNode, so no object-store call happens here.
func preImportV3PackingSizes(tasks []ImportTask) map[int64]int64 {
	out := make(map[int64]int64)
	for _, t := range tasks {
		for _, stat := range t.(*preImportV3Task).GetV3FileStats() {
			size := stat.GetTotalMemorySize()
			if size <= 0 {
				size = stat.GetFileSize()
			}
			out[stat.GetFileId()] += size
		}
	}
	return out
}
