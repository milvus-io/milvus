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

// This file is the Import V3 job state machine. Each job state is a stateless
// importV3StateHandler whose Handle runs once per checker tick with the one
// importV3JobContext that carries the checker dependencies and the job;
// handlers never see each other. A handler returns an error to say "this tick
// failed": the checker's dispatch classifies it (see isTerminalImportV3JobErr)
// and either fails the job or leaves it for the next tick. Handlers with
// special error semantics (the AssigningIDRange wait, the auto-commit
// broadcast) manage them internally and return nil.
//
// The file also owns the count-only preimport stage (importV3PreImportStage) shared
// by the two preimport handlers, and the terminal-job GC pipeline (importV3JobGC)
// the checker's low-frequency loop runs once per terminal job per tick.

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/util/importutilv2/importid"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// importV3JobContext is the one working context every handler, planner and the
// GC pipeline receives. It embeds the checker, so the shared dependencies (ctx,
// meta, broker, alloc, importMeta, cluster, hooks) are reachable directly, and
// adds the single job this tick works on plus a job-scoped logger. The checker
// remains the single owner of the dependencies; jc is the per-job view of it.
type importV3JobContext struct {
	*importCheckerV3
	job     ImportJob
	log     *mlog.Logger
	metrics *importV3JobMetrics
}

func newImportV3JobContext(checker *importCheckerV3, job ImportJob) *importV3JobContext {
	return &importV3JobContext{
		importCheckerV3: checker,
		job:             job,
		log:             mlog.With(mlog.FieldJobID(job.GetJobID())),
		metrics:         newImportV3JobMetrics(job),
	}
}

// allTasksDone reports whether every task reached Completed. A Failed task is
// returned so the caller can fail the job with its reason; a task that is not
// terminal yet returns nil with done=false (keep waiting).
func allTasksDone(tasks []ImportTask) (failed ImportTask, done bool) {
	for _, t := range tasks {
		switch t.GetState() {
		case datapb.ImportTaskStateV2_Failed:
			return t, false
		case datapb.ImportTaskStateV2_Completed:
		default:
			return nil, false
		}
	}
	return nil, true
}

// reshardTasksAllCompleted reports whether every ReshardTask has reached the
// catalog-verified Completed marker. It inspects only task states and never
// reads result manifests — the Completed marker is gated on acceptResult
// validating the manifest, so this check needs no object-store access and the
// Resharding tail does no redundant reads while waiting for slow tasks.
func (jc *importV3JobContext) reshardTasksAllCompleted() (bool, error) {
	tasks := jc.importMeta.GetTaskByJob(jc.ctx, jc.job.GetJobID(), WithType(ReshardTaskType))
	if len(tasks) == 0 {
		return false, nil
	}
	for _, generic := range tasks {
		if _, ok := generic.(*reshardTask); !ok {
			return false, merr.WrapErrDataIntegrityMsg("import v3 reshard result set contains an unexpected task type")
		}
	}
	// A failed task must terminate the job even when DataCoord crashed between
	// the task marker and the job marker; without this the job would wait for a
	// Completed marker that can never arrive.
	if failed, done := allTasksDone(tasks); failed != nil {
		return false, merr.WrapErrImportSysFailedMsg("import v3 reshard task %d failed: %s", failed.GetTaskID(), failed.GetReason())
	} else if !done {
		return false, nil
	}
	return true, nil
}

// transitionTo applies a job state update and logs on failure. The caller
// decides whether the error is worth propagating; a failed catalog write
// simply replays on the next tick.
//
// A failed write leaves the job in its current state, so the checker re-enters
// this handler on the next tick and retries — callers may therefore
// deliberately ignore the returned error. Use failJob (or return the error)
// instead when this tick must terminate the job.
func (jc *importV3JobContext) transitionTo(state internalpb.ImportJobState, actions ...UpdateJobAction) error {
	actions = append(actions, UpdateJobState(state))
	if err := jc.importMeta.UpdateJob(jc.ctx, jc.job.GetJobID(), actions...); err != nil {
		jc.log.Warn(jc.ctx, "failed to update job state", mlog.String("state", state.String()), mlog.Err(err))
		return err
	}
	return nil
}

// failJob marks the job Failed with the given reason.
func (jc *importV3JobContext) failJob(reason string) {
	if err := jc.transitionTo(internalpb.ImportJobState_Failed, UpdateJobReason(reason)); err != nil {
		jc.log.Warn(jc.ctx, "failed to update job state to Failed", mlog.Err(err))
	}
}

// recordStageLatency closes the job's current stage span and exports it.
func (jc *importV3JobContext) recordStageLatency(stage string) time.Duration {
	duration := jc.job.GetTR().RecordSpan()
	jc.metrics.observeStage(stage, duration)
	return duration
}

// importV3StateHandler is one state of the Import V3 job lifecycle. Handle runs
// once per checker tick while the job is in this state. A returned error is
// classified by the checker's dispatch: terminal fails the job, anything else
// retries on the next tick.
type importV3StateHandler interface {
	Handle(jc *importV3JobContext) error
}

// handlerFor maps a job state to its handler. Handlers are stateless: they
// receive everything they need through the jc passed to Handle, so one shared
// instance per state is enough and the wiring stays explicit.
func handlerFor(state internalpb.ImportJobState) importV3StateHandler {
	switch state {
	case internalpb.ImportJobState_Pending:
		return pendingHandler{}
	case internalpb.ImportJobState_PreImporting:
		return preImportingHandler{}
	case internalpb.ImportJobState_AssigningIDRange:
		return assigningIDRangeHandler{}
	case internalpb.ImportJobState_Resharding:
		return reshardingHandler{}
	case internalpb.ImportJobState_Planning:
		return planningHandler{}
	case internalpb.ImportJobState_Importing:
		return importingHandler{}
	case internalpb.ImportJobState_IndexBuilding:
		return indexBuildingHandler{}
	case internalpb.ImportJobState_Uncommitted:
		return uncommittedHandler{}
	case internalpb.ImportJobState_Committing:
		return committingHandler{}
	case internalpb.ImportJobState_Failed:
		return failedHandler{}
	default:
		return nil
	}
}

// pendingHandler is the first V3 state. An ordinary import must know the exact
// row count of every source file before Reshard can generate deterministic
// PK/RowID, so it first runs a count-only preimport phase; the exact ID ranges
// are then allocated and broadcast in AssigningIDRange. A backup import carries
// its own PK/RowID, needs no ranges, and goes straight to Resharding.
type pendingHandler struct{}

func (h pendingHandler) Handle(jc *importV3JobContext) error {
	if importV3NeedsIDRanges(jc.job) {
		return newImportV3PreImportStage(jc).createTasks()
	}
	return newReshardTaskPlanner(jc).CreateTasks()
}

// preImportingHandler waits for every count-only PreImportV2 task to complete.
// Once the exact per-file counts are known the job either parks in
// AssigningIDRange for the primary to allocate and broadcast the exact ranges,
// or (when ranges are already present from a previous tick) runs the shared
// tail.
type preImportingHandler struct{}

func (h preImportingHandler) Handle(jc *importV3JobContext) error {
	stage := newImportV3PreImportStage(jc)
	if !stage.allCountsReady() {
		return nil
	}
	if importV3NeedsIDRanges(jc.job) && !jobIDRangesSet(jc.job) {
		_ = jc.transitionTo(internalpb.ImportJobState_AssigningIDRange)
		return nil
	}
	return stage.finish(newReshardTaskPlanner(jc))
}

// assigningIDRangeHandler is the wait state for the per-file ID ranges. The
// primary allocates the exact ranges and broadcasts the ImportIDRange message;
// a secondary waits for the replicated copy, and the ack callback applies it
// to the job meta. Once present the job runs the divergence gate and the
// shared tail.
type assigningIDRangeHandler struct{}

func (h assigningIDRangeHandler) Handle(jc *importV3JobContext) error {
	stage := newImportV3PreImportStage(jc)
	if !jobIDRangesSet(jc.job) {
		stage.ensureIDRanges()
		return nil
	}
	return stage.finish(newReshardTaskPlanner(jc))
}

// reshardingHandler waits for every ReshardTask of the job to reach the
// catalog-verified Completed marker, then advances to Planning. It inspects
// only task states — no result manifests are read while waiting for slow
// tasks. Empty input jobs (zero total rows) are detected by the planner after
// reading the manifests, shortcutting to Uncommitted/Completed exactly like
// the legacy import path does for empty preimports.
type reshardingHandler struct{}

func (h reshardingHandler) Handle(jc *importV3JobContext) error {
	completed, err := jc.reshardTasksAllCompleted()
	if err != nil {
		return err
	}
	if !completed {
		return nil
	}
	jc.recordStageLatency(metrics.ImportStageReshard)
	_ = jc.transitionTo(internalpb.ImportJobState_Planning)
	return nil
}

type planningHandler struct{}

func (h planningHandler) Handle(jc *importV3JobContext) error {
	return newImportV3Planner(jc).Plan()
}

type importingHandler struct{}

func (h importingHandler) Handle(jc *importV3JobContext) error {
	currentSchemaVersion, err := validateImportV3Schema(jc.meta, jc.job.GetCollectionID(), jc.job.GetSchema())
	if err != nil {
		return err
	}
	tasks := jc.importMeta.GetTaskByJob(jc.ctx, jc.job.GetJobID(), WithType(ImportTaskV3Type))
	if len(tasks) == 0 {
		return nil
	}
	if failed, done := allTasksDone(tasks); failed != nil {
		// Recover the crash window between the task marker and the job marker:
		// a Failed task can never become Completed, so without this branch the
		// job would wait forever (or until an external timeout).
		jc.failJob(fmt.Sprintf("import v3 task %d failed: %s", failed.GetTaskID(), failed.GetReason()))
		return nil
	} else if !done {
		return nil
	}
	// Persist all completed-segment schema-version bumps before advancing the
	// job. IndexBuilding is the marker-last write: a crash or a failed segment
	// update leaves the job in Importing and this loop retries on the next tick.
	segmentOps := make([]UpdateOperator, 0, len(tasks))
	for _, task := range tasks {
		segmentID := task.(*importTaskV3).task.Load().GetSegmentId()
		if segmentID == 0 {
			continue
		}
		segment := jc.meta.GetSegment(jc.ctx, segmentID)
		if segment != nil && segment.GetNumOfRows() > 0 && segment.GetSchemaVersion() != currentSchemaVersion {
			segmentOps = append(segmentOps, updateImportV3SchemaVersion(segmentID, currentSchemaVersion))
		}
	}
	if len(segmentOps) > 0 {
		if err := jc.meta.UpdateSegmentsInfo(jc.ctx, segmentOps...); err != nil {
			jc.log.Warn(jc.ctx, "update import v3 segment schema version failed", mlog.Err(err))
			return nil
		}
	}
	if err := jc.transitionTo(internalpb.ImportJobState_IndexBuilding); err != nil {
		return nil
	}
	importDuration := jc.recordStageLatency(metrics.ImportStageImport)
	jc.log.Info(jc.ctx, "import v3 import done", mlog.Duration("jobTimeCost/import", importDuration))
	return nil
}

type indexBuildingHandler struct{}

func (h indexBuildingHandler) Handle(jc *importV3JobContext) error {
	tasks := jc.importMeta.GetTaskByJob(jc.ctx, jc.job.GetJobID(), WithType(ImportTaskV3Type))
	segmentIDs := make([]int64, 0)
	for _, task := range tasks {
		if segmentID := task.(*importTaskV3).task.Load().GetSegmentId(); segmentID != 0 {
			segment := jc.meta.GetHealthySegment(jc.ctx, segmentID)
			if segment == nil || segment.GetNumOfRows() == 0 {
				continue
			}
			segmentIDs = append(segmentIDs, segmentID)
		}
	}
	healthySegments := jc.meta.GetSegments(segmentIDs, isSegmentHealthy)
	unindexed := jc.meta.indexMeta.GetUnindexedSegments(jc.job.GetCollectionID(), healthySegments)
	if Params.DataCoordCfg.WaitForIndex.GetAsBool() && len(unindexed) > 0 {
		for _, segmentID := range unindexed {
			select {
			case getBuildIndexChSingleton() <- segmentID:
			default:
			}
		}
		return nil
	}
	if err := jc.transitionTo(internalpb.ImportJobState_Uncommitted); err != nil {
		return nil
	}
	buildIndexDuration := jc.recordStageLatency(metrics.ImportStageBuildIndex)
	jc.log.Info(jc.ctx, "import v3 build index done", mlog.Duration("jobTimeCost/buildIndex", buildIndexDuration))
	return nil
}

// uncommittedHandler handles jobs in the Uncommitted state. If
// auto_commit=true, it triggers a commit via broadcastCommitImportMessage. If
// auto_commit=false, it waits for an explicit CommitImport RPC from the
// platform.
type uncommittedHandler struct{}

func (h uncommittedHandler) Handle(jc *importV3JobContext) error {
	if !jc.job.GetAutoCommit() {
		// Wait for explicit CommitImport from the replication platform.
		return nil
	}
	// auto_commit=true: trigger commit by broadcasting the WAL message.
	// Repeated invocations across ticks are safe: the broadcaster's exclusive
	// collection-level resource-key lock serializes overlapping broadcasts, the
	// ack callback only transitions when the job is still Uncommitted, and
	// HandleCommitVchannel is idempotent on committed_vchannels.
	if jc.hooks.commitImport == nil {
		jc.log.Error(jc.ctx, "commit hook is nil but auto_commit=true; this is a programming error")
		return nil
	}
	if err := jc.hooks.commitImport(jc.ctx, jc.job); err != nil {
		jc.log.Warn(jc.ctx, "auto-commit broadcast failed, will retry on next tick", mlog.Err(err))
	}
	return nil
}

// committingHandler handles jobs in the Committing state. Once all vchannels
// have acknowledged the commit fence, the job transitions to Completed.
type committingHandler struct{}

func (h committingHandler) Handle(jc *importV3JobContext) error {
	// When Vchannels is empty, len == len is trivially true. This handles the degenerate
	// case of a zero-channel import (e.g., empty collection); proceed to Completed immediately.
	if len(jc.job.GetCommittedVchannels()) < len(jc.job.GetVchannels()) {
		return nil // still waiting for remaining vchannels
	}
	completeTime := time.Now().Format("2006-01-02T15:04:05Z07:00")
	if err := jc.transitionTo(internalpb.ImportJobState_Completed, UpdateJobCompleteTime(completeTime)); err != nil {
		return nil
	}
	totalDuration := jc.job.GetTR().ElapseSpan()
	jc.metrics.observeTotal(totalDuration)
	jc.log.Info(jc.ctx, "import job Committing done, all vchannels committed",
		mlog.Duration("jobTimeCost/total", totalDuration))
	return nil
}

type failedHandler struct{}

func (h failedHandler) Handle(jc *importV3JobContext) error {
	tasks := jc.importMeta.GetTaskByJob(jc.ctx, jc.job.GetJobID(), WithStates(datapb.ImportTaskStateV2_None, datapb.ImportTaskStateV2_Pending,
		datapb.ImportTaskStateV2_InProgress, datapb.ImportTaskStateV2_Completed, datapb.ImportTaskStateV2_Retry))
	if len(tasks) == 0 {
		return nil
	}
	jc.log.Warn(jc.ctx, "Import job has failed, all tasks with the same jobID will be marked as failed",
		mlog.String("reason", jc.job.GetReason()))
	for _, task := range tasks {
		err := jc.importMeta.UpdateTask(jc.ctx, task.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed),
			UpdateReason(jc.job.GetReason()))
		if err != nil {
			jc.log.Warn(jc.ctx, "failed to update import task state to failed", WrapTaskLog(task, mlog.Err(err))...)
			continue
		}
	}
	return nil
}

// importV3PreImportStage is the count-only preimport phase of an ordinary Import V3
// job: it creates the PreImportV2 tasks, reads their per-file counts back, and
// drives the two-phase ID-range assignment. The two preimport state handlers
// share it.
type importV3PreImportStage struct {
	jc *importV3JobContext
}

func newImportV3PreImportStage(jc *importV3JobContext) *importV3PreImportStage {
	return &importV3PreImportStage{jc: jc}
}

// importV3NeedsIDRanges reports whether the V3 job needs per-file ID ranges: it has a
// resolvable primary key and is neither a backup import (which keeps embedded
// PK/RowID/ts) nor an L0 import. Unlike the V2 path this is deliberately not
// gated by dataCoord.import.enableIDRangeMsg: enableImportV3 already guarantees
// the whole cluster understands the ImportIDRange message, and V3 has no
// local-allocator fallback that keeps PK/RowID identical across clusters.
func importV3NeedsIDRanges(job ImportJob) bool {
	return importid.NeedsFileIDRanges(job.GetSchema(), job.GetOptions())
}

// createTasks groups the not-yet-covered files and creates one count-only
// PreImportV2 task per group, then advances the job to PreImporting. It is
// recovery-safe: files already covered by an existing PreImportV2 task are
// skipped, so a crash between task creation and the state write replays
// cleanly.
func (s *importV3PreImportStage) createTasks() error {
	jc := s.jc
	lacks := s.getLackFiles()
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
		fileStats := lo.Map(files, func(f *internalpb.ImportFile, _ int) *datapb.ImportFileStats {
			return &datapb.ImportFileStats{ImportFile: f}
		})
		tasks = append(tasks, newPreImportV2Task(&datapb.PreImportV2Task{
			JobID:        jc.job.GetJobID(),
			TaskID:       idStart + int64(i),
			CollectionID: jc.job.GetCollectionID(),
			State:        datapb.ImportTaskStateV2_Pending,
			FileStats:    fileStats,
			CreatedTime:  time.Now().Format("2006-01-02T15:04:05Z07:00"),
		}, jc.importMeta))
	}
	for _, task := range tasks {
		if err := jc.importMeta.AddTask(jc.ctx, task); err != nil {
			return err
		}
	}
	return jc.transitionTo(internalpb.ImportJobState_PreImporting)
}

func (s *importV3PreImportStage) getLackFiles() []*internalpb.ImportFile {
	jc := s.jc
	lacks := lo.KeyBy(jc.job.GetFiles(), func(file *internalpb.ImportFile) int64 { return file.GetId() })
	exists := jc.importMeta.GetTaskByJob(jc.ctx, jc.job.GetJobID(), WithType(PreImportV2TaskType))
	for _, task := range exists {
		for _, file := range task.GetFileStats() {
			delete(lacks, file.GetImportFile().GetId())
		}
	}
	return lo.Values(lacks)
}

// allCountsReady reports whether every count-only PreImportV2 task has
// completed.
func (s *importV3PreImportStage) allCountsReady() bool {
	jc := s.jc
	tasks := jc.importMeta.GetTaskByJob(jc.ctx, jc.job.GetJobID(), WithType(PreImportV2TaskType))
	if len(tasks) == 0 {
		return false
	}
	for _, task := range tasks {
		if task.GetState() != datapb.ImportTaskStateV2_Completed {
			return false
		}
	}
	return true
}

// ensureIDRanges allocates the exact per-file ID ranges on the primary and
// broadcasts the ImportIDRange message; a secondary waits for the replicated
// copy. It mirrors the V2 ensureIDRanges but reads the exact counts from the
// V3 count-only PreImportV2 tasks. Its error semantics are local to this wait
// state (role flips and transient broadcast failures retry; only caller-input
// defects fail the job), so it manages them instead of returning an error.
func (s *importV3PreImportStage) ensureIDRanges() {
	jc := s.jc
	ctx, cancel := context.WithTimeout(jc.ctx, 10*time.Second)
	defer cancel()

	var role replicateutil.Role
	replicating := false
	if jc.hooks.getReplicationRole != nil {
		r, rep, err := jc.hooks.getReplicationRole(ctx)
		if err != nil {
			jc.log.Warn(ctx, "cannot determine replication role before ImportIDRange broadcast, will retry next tick", mlog.Err(err))
			return
		}
		role, replicating = r, rep
	}
	if replicating && role == replicateutil.RoleSecondary {
		jc.log.Debug(ctx, "waiting for replicated ImportIDRange")
		return
	}
	if jc.hooks.assignImportIDRange == nil {
		jc.log.Error(ctx, "assignImportIDRange hook is nil but import requires ID ranges; this is a programming error")
		return
	}

	tasks := jc.importMeta.GetTaskByJob(jc.ctx, jc.job.GetJobID(), WithType(PreImportV2TaskType))
	fileRows, ok := preImportV2FileRows(jc.ctx, jc.job, tasks, jc.log)
	if !ok {
		return
	}
	jc.log.Info(ctx, "triggering ImportIDRange broadcast",
		mlog.Int("fileCount", len(jc.job.GetFiles())), mlog.Int64("totalRows", lo.Sum(fileRows)))

	if err := jc.hooks.assignImportIDRange(ctx, jc.job, fileRows); err != nil {
		if errors.Is(err, broadcaster.ErrNotPrimary) {
			jc.log.Info(ctx, "role flipped to standby while importing, waiting for replicated ImportIDRange")
		} else if merr.GetErrorType(err) == merr.InputError {
			jc.failJob(err.Error())
		} else {
			jc.log.Warn(ctx, "ImportIDRange broadcast failed, will retry next tick", mlog.Err(err))
		}
		return
	}
}

// finish runs the tail shared by "preimport complete, ranges present" (or not
// needed): the cross-cluster divergence gate, the zero-rows exit, then
// Resharding.
func (s *importV3PreImportStage) finish(reshard *reshardTaskPlanner) error {
	jc := s.jc
	tasks := jc.importMeta.GetTaskByJob(jc.ctx, jc.job.GetJobID(), WithType(PreImportV2TaskType))

	// Divergence gate: for every ranged file the local preimport count must equal
	// the size of the range the primary allocated. A mismatch means the same file
	// holds different rows on the two clusters, so fail loudly with both numbers
	// before any segment is written.
	fileRows, ok := preImportV2FileRows(jc.ctx, jc.job, tasks, jc.log)
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
				jc.log.Warn(jc.ctx, "ImportIDRange divergence; failing import", mlog.String("reason", reason))
				jc.failJob(reason)
				return nil
			}
		}
	}

	// Zero-rows exit: nothing to read, no Reshard task and no cursor is needed.
	// It runs after the divergence gate, so a locally empty side still has to
	// match the peer's ranges before it may commit as empty.
	var totalRows int64
	for _, rows := range fileRows {
		totalRows += rows
	}
	if totalRows == 0 {
		if jc.job.GetAutoCommit() {
			_ = jc.transitionTo(internalpb.ImportJobState_Completed)
		} else {
			_ = jc.transitionTo(internalpb.ImportJobState_Uncommitted)
		}
		return nil
	}

	return reshard.CreateTasks()
}

// preImportV2FileRows returns the per-file row counts aligned with
// job.GetFiles() order, summed from the completed PreImportV2 task stats by
// fileID. A file with no stat is an internal error; ok=false tells the caller
// to retry next tick.
func preImportV2FileRows(ctx context.Context, job ImportJob, tasks []ImportTask, log *mlog.Logger) ([]int64, bool) {
	rowsByFile := make(map[int64]int64)
	for _, t := range tasks {
		for _, stat := range t.GetFileStats() {
			rowsByFile[stat.GetImportFile().GetId()] += stat.GetTotalRows()
		}
	}
	files := job.GetFiles()
	if f, missing := lo.Find(files, func(f *internalpb.ImportFile) bool {
		_, ok := rowsByFile[f.GetId()]
		return !ok
	}); missing {
		log.Warn(ctx, "preimport v2 stats missing for an import file, will retry next tick", mlog.Int64("fileID", f.GetId()))
		return nil, false
	}
	return lo.Map(files, func(f *internalpb.ImportFile, _ int) int64 {
		return rowsByFile[f.GetId()]
	}), true
}

// preImportV2LogicalBytes returns the decoded (logical) size per file, summed
// from the PreImportV2 task stats by fileID. It is used to pack reshard tasks by
// the same unit as the segment target. Backup jobs skip preimport and get an
// empty map, so the caller falls back to physical size.
func preImportV2LogicalBytes(tasks []ImportTask) map[int64]int64 {
	out := make(map[int64]int64)
	for _, t := range tasks {
		for _, stat := range t.GetFileStats() {
			out[stat.GetImportFile().GetId()] += stat.GetTotalMemorySize()
		}
	}
	return out
}

// importV3JobGC is the terminal-job cleanup pipeline. Its phases are derived from
// durable facts on every tick; no separate GC record exists. collect runs one
// GC tick for one terminal job.
type importV3JobGC struct {
	jc *importV3JobContext
}

func newImportV3JobGC(jc *importV3JobContext) *importV3JobGC {
	return &importV3JobGC{jc: jc}
}

// collect runs the GC phases in order: quiesce makes progress as soon as the
// job is terminal and keeps retrying version-aware Drops; task removal inside
// it waits for cleanupTs. Delete only starts once cleanupTs has passed and
// every task is unbound/removed.
func (g *importV3JobGC) collect() {
	if g.jc.job.GetState() != internalpb.ImportJobState_Completed &&
		g.jc.job.GetState() != internalpb.ImportJobState_Failed {
		return
	}
	if !g.quiesce() {
		return
	}
	if !time.Now().After(tsoutil.PhysicalTime(g.jc.job.GetCleanupTs())) {
		return
	}
	if !g.rollbackFailedReplicate() {
		return
	}
	g.deleteJob()
}

// quiesce drops still-bound V3 tasks and removes tasks that are no longer
// pinned to a node. Removal only starts once cleanupTs has passed so the task
// records keep backing the job's row accounting during the retention window.
// It reports true only when every task of the job is already unbound and
// removed from catalog.
func (g *importV3JobGC) quiesce() bool {
	jc := g.jc
	tasks := jc.importMeta.GetTaskByJob(jc.ctx, jc.job.GetJobID())
	ready := true
	for _, task := range tasks {
		if task.GetNodeID() != NullNodeID {
			// The scheduler drops a terminal task once when it observes the state.
			// GC retries that best-effort version-aware Drop every tick, so a
			// transient RPC failure or a lost node cannot pin the job forever.
			if jc.cluster != nil {
				task.DropTaskOnWorker(jc.cluster)
			}
			ready = false
			continue
		}
		if jc.job.GetState() == internalpb.ImportJobState_Failed && task.GetType() == ImportTaskV3Type {
			segmentIDs := []int64(nil)
			if segmentID := task.(*importTaskV3).task.Load().GetSegmentId(); segmentID != 0 {
				segmentIDs = append(segmentIDs, segmentID)
			}
			if len(segmentIDs) > 0 {
				if err := jc.meta.UpdateSegmentsInfo(jc.ctx, dropImportV3Segments(segmentIDs)); err != nil {
					jc.log.Warn(jc.ctx, "drop import v3 segments during failed job GC", WrapTaskLog(task, mlog.Err(err))...)
					ready = false
					continue
				}
			}
		}
		// Task records back the job's row accounting (getImportRowsInfo sums
		// them), so they must survive the retention window like V2 GC does;
		// dropping their worker binding above is still safe to do early.
		if !time.Now().After(tsoutil.PhysicalTime(jc.job.GetCleanupTs())) {
			ready = false
			continue
		}
		if err := jc.importMeta.RemoveTask(jc.ctx, task.GetTaskID()); err != nil {
			jc.log.Warn(jc.ctx, "remove task failed during GC", WrapTaskLog(task, mlog.Err(err))...)
			ready = false
			continue
		}
		jc.log.Info(jc.ctx, "task removed during GC quiesce", WrapTaskLog(task)...)
	}
	return ready
}

// rollbackFailedReplicate releases the peer cluster before source GC drops a
// failed 2PC import. It is called only on the deletion tick, so a job sitting
// inside the retention window does not re-broadcast RollbackImport every tick.
// A crash between a successful broadcast and RemoveJob repeats the broadcast
// at most once, which is the same idempotency boundary V2 GC already has.
func (g *importV3JobGC) rollbackFailedReplicate() bool {
	jc := g.jc
	if jc.hooks.rollbackImport == nil || jc.hooks.getReplicationRole == nil ||
		jc.job.GetState() != internalpb.ImportJobState_Failed || jc.job.GetAutoCommit() {
		return true
	}
	replicateCheckCtx, cancel := context.WithTimeout(jc.ctx, 10*time.Second)
	_, replicating, err := jc.hooks.getReplicationRole(replicateCheckCtx)
	cancel()
	switch {
	case err != nil:
		jc.log.Warn(jc.ctx, "cannot determine replication status before GC of failed import job, will retry", mlog.Err(err))
		return false
	case replicating:
		rollbackCtx, rollbackCancel := context.WithTimeout(jc.ctx, 10*time.Second)
		err := jc.hooks.rollbackImport(rollbackCtx, jc.job)
		rollbackCancel()
		if err != nil && !isPermanentRollbackErr(err) {
			jc.log.Warn(jc.ctx, "failed to broadcast rollback before GC of failed replicate import job, will retry", mlog.Err(err))
			return false
		}
		jc.log.Info(jc.ctx, "proceeding with GC of failed replicate import job after rollback attempt")
	}
	return true
}

// deleteJob removes the job's temporary OSS prefix, any remaining task catalog
// entries, and the job itself. Every step is idempotent, so a crash in between
// simply resumes on the next GC tick.
func (g *importV3JobGC) deleteJob() {
	jc := g.jc
	prefix := path.Join(jc.meta.chunkManager.RootPath(), metautil.BuildImportV3JobPath(jc.job.GetJobID())) + "/"
	if err := jc.meta.chunkManager.RemoveWithPrefix(jc.ctx, prefix); err != nil {
		jc.log.Warn(jc.ctx, "remove import job temporary objects failed", mlog.Err(err))
		return
	}
	tasks := jc.importMeta.GetTaskByJob(jc.ctx, jc.job.GetJobID())
	for _, task := range tasks {
		if err := jc.importMeta.RemoveTask(jc.ctx, task.GetTaskID()); err != nil {
			jc.log.Warn(jc.ctx, "remove task failed during GC delete", WrapTaskLog(task, mlog.Err(err))...)
			return
		}
	}
	if err := jc.importMeta.RemoveJob(jc.ctx, jc.job.GetJobID()); err != nil {
		jc.log.Warn(jc.ctx, "remove import job failed", mlog.Err(err))
		return
	}
	jc.log.Info(jc.ctx, "import job removed")
}
