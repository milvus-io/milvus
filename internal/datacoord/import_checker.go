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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/importutilv2/importid"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

type ImportChecker interface {
	Start()
	Close()
}

// importCheckerHooks bundles the coordinator callbacks the import checker invokes,
// injected as one named unit (instead of a growing positional-arg list) so the checker
// does not depend on *Server. A nil callback disables the corresponding behavior; tests
// inject only the hooks they exercise.
type importCheckerHooks struct {
	// commitImport acquires the collection DDL lock, persists Committing, and
	// broadcasts a CommitImport WAL message. Required in production; a nil value
	// is a programming error only when reached on the auto_commit=true path.
	commitImport func(ctx context.Context, job ImportJob) error
	// rollbackImport broadcasts a RollbackImport WAL message. nil disables GC self-heal.
	rollbackImport func(ctx context.Context, job ImportJob) error
	// getReplicationRole reports this cluster's live replication role and whether it is
	// replicating at all. A non-nil error means the role is indeterminate and the caller
	// must NOT allocate (a secondary that allocated would diverge from the primary's
	// authoritative range). nil hook is treated as "not replicating → primary" so tests
	// that inject only assignImportIDRange proceed to allocate/broadcast.
	getReplicationRole func(ctx context.Context) (role replicateutil.Role, replicating bool, err error)
	// assignImportIDRange allocates the per-file ID ranges from the post-preimport row
	// counts and broadcasts the UpdateImport WAL message. Required in production for
	// autoID (non-backup, non-L0) imports; a nil value disables the two-phase range
	// assignment and is only valid in tests that do not exercise the PreImporting range gate.
	assignImportIDRange func(ctx context.Context, job ImportJob, fileRows []int64) error
}

type importChecker struct {
	ctx        context.Context
	meta       *meta
	broker     broker.Broker
	alloc      allocator.Allocator
	importMeta ImportMeta
	ci         CompactionInspector
	handler    Handler
	// cluster lets GC retry a worker drop the scheduler could not land. See
	// checkGC.
	cluster session.Cluster
	// scheduler serializes terminal task updates with worker callbacks for the
	// same task. It is nil only in tests that do not exercise that concurrency.
	scheduler task.GlobalScheduler

	hooks importCheckerHooks

	closeOnce sync.Once
	closeChan chan struct{}
}

func NewImportChecker(ctx context.Context,
	meta *meta,
	broker broker.Broker,
	alloc allocator.Allocator,
	importMeta ImportMeta,
	ci CompactionInspector,
	handler Handler,
	cluster session.Cluster,
	scheduler task.GlobalScheduler,
	hooks importCheckerHooks,
) ImportChecker {
	return &importChecker{
		ctx:        ctx,
		meta:       meta,
		broker:     broker,
		alloc:      alloc,
		importMeta: importMeta,
		ci:         ci,
		handler:    handler,
		cluster:    cluster,
		scheduler:  scheduler,
		hooks:      hooks,
		closeChan:  make(chan struct{}),
	}
}

// Start runs the checker loops until Close. The state-machine loop and the
// timeout/GC loop deliberately run on separate goroutines: checkGC's rollback
// broadcast can park on the ctx-insensitive resource-key lock (see checkGC), and
// isolating it guarantees the state machine keeps making progress no matter how
// long GC blocks. All state shared by the two loops lives behind importMeta's
// mutex (which already serves concurrent RPC and ack-callback goroutines), and
// UpdateJob refuses transitions out of Completed/Failed, so the loops cannot
// resurrect or regress each other's terminal states.
func (c *importChecker) Start() {
	mlog.Info(c.ctx, "start import checker")
	go c.runGCLoop()
	c.runStateMachineLoop()
}

func (c *importChecker) runStateMachineLoop() {
	ticker := time.NewTicker(Params.DataCoordCfg.ImportCheckIntervalHigh.GetAsDuration(time.Second)) // 2s
	defer ticker.Stop()
	for {
		select {
		case <-c.closeChan:
			mlog.Info(c.ctx, "import checker state-machine loop exited")
			return
		case <-ticker.C:
			jobs := c.importMeta.GetJobBy(c.ctx)
			for _, job := range jobs {
				if !funcutil.SliceSetEqual[string](job.GetVchannels(), job.GetReadyVchannels()) {
					// wait for all channels to send signals
					mlog.RatedDebug(c.ctx, rate.Limit(30), "waiting for all channels to send signals",
						mlog.Strings("vchannels", job.GetVchannels()),
						mlog.Strings("readyVchannels", job.GetReadyVchannels()),
						mlog.FieldJobID(job.GetJobID()))
					continue
				}
				switch job.GetState() {
				case internalpb.ImportJobState_Pending:
					c.checkPendingJob(job)
				case internalpb.ImportJobState_PreImporting:
					c.checkPreImportingJob(job)
				case internalpb.ImportJobState_AssigningIDRange:
					c.checkAssigningIDRangeJob(job)
				case internalpb.ImportJobState_Importing:
					c.checkImportingJob(job)
				case internalpb.ImportJobState_Sorting:
					c.checkSortingJob(job)
				case internalpb.ImportJobState_IndexBuilding:
					c.checkIndexBuildingJob(job)
				case internalpb.ImportJobState_Uncommitted:
					c.checkUncommittedJob(job)
				case internalpb.ImportJobState_Committing:
					c.checkCommittingJob(job)
				case internalpb.ImportJobState_Failed:
					c.checkFailedJob(job)
				}
			}
		}
	}
}

func (c *importChecker) runGCLoop() {
	ticker := time.NewTicker(Params.DataCoordCfg.ImportCheckIntervalLow.GetAsDuration(time.Second)) // 2min
	defer ticker.Stop()
	for {
		select {
		case <-c.closeChan:
			mlog.Info(c.ctx, "import checker gc loop exited")
			return
		case <-ticker.C:
			jobs := c.importMeta.GetJobBy(c.ctx)
			for _, job := range jobs {
				c.tryTimeoutJob(job)
				c.checkGC(job)
			}
			jobsByColl := lo.GroupBy(jobs, func(job ImportJob) int64 {
				return job.GetCollectionID()
			})
			for collID, collJobs := range jobsByColl {
				c.checkCollection(collID, collJobs)
			}
			c.LogJobStats(jobs)
			c.LogTaskStats()
		}
	}
}

func (c *importChecker) Close() {
	c.closeOnce.Do(func() {
		close(c.closeChan)
	})
}

func (c *importChecker) LogJobStats(jobs []ImportJob) {
	byState := lo.GroupBy(jobs, func(job ImportJob) string {
		return job.GetState().String()
	})
	stateNum := make(map[string]int)
	for state := range internalpb.ImportJobState_value {
		if state == internalpb.ImportJobState_None.String() {
			continue
		}
		num := len(byState[state])
		stateNum[state] = num
		metrics.ImportJobs.WithLabelValues(state).Set(float64(num))
	}
	mlog.Info(c.ctx, "import job stats", mlog.Any("stateNum", stateNum))
}

func (c *importChecker) LogTaskStats() {
	logFunc := func(tasks []ImportTask, taskType TaskType) {
		byState := lo.GroupBy(tasks, func(t ImportTask) datapb.ImportTaskStateV2 {
			return t.GetState()
		})
		pending := len(byState[datapb.ImportTaskStateV2_Pending])
		inProgress := len(byState[datapb.ImportTaskStateV2_InProgress])
		retrying := len(byState[datapb.ImportTaskStateV2_Retry])
		completed := len(byState[datapb.ImportTaskStateV2_Completed])
		failed := len(byState[datapb.ImportTaskStateV2_Failed])
		mlog.Info(c.ctx, "import task stats", mlog.String("type", taskType.String()),
			mlog.Int("pending", pending), mlog.Int("inProgress", inProgress),
			mlog.Int("retrying", retrying), mlog.Int("completed", completed), mlog.Int("failed", failed))
		metrics.ImportTasks.WithLabelValues(taskType.String(), datapb.ImportTaskStateV2_Pending.String()).Set(float64(pending))
		metrics.ImportTasks.WithLabelValues(taskType.String(), datapb.ImportTaskStateV2_InProgress.String()).Set(float64(inProgress))
		metrics.ImportTasks.WithLabelValues(taskType.String(), datapb.ImportTaskStateV2_Retry.String()).Set(float64(retrying))
		metrics.ImportTasks.WithLabelValues(taskType.String(), datapb.ImportTaskStateV2_Completed.String()).Set(float64(completed))
		metrics.ImportTasks.WithLabelValues(taskType.String(), datapb.ImportTaskStateV2_Failed.String()).Set(float64(failed))
	}
	tasks := c.importMeta.GetTaskBy(c.ctx, WithType(PreImportTaskType))
	logFunc(tasks, PreImportTaskType)
	tasks = c.importMeta.GetTaskBy(c.ctx, WithType(ImportTaskType))
	logFunc(tasks, ImportTaskType)
}

func (c *importChecker) getLackFilesForPreImports(job ImportJob) []*internalpb.ImportFile {
	lacks := lo.KeyBy(job.GetFiles(), func(file *internalpb.ImportFile) int64 {
		return file.GetId()
	})
	exists := c.importMeta.GetTaskByJob(c.ctx, job.GetJobID(), WithType(PreImportTaskType))
	for _, task := range exists {
		for _, file := range task.GetFileStats() {
			delete(lacks, file.GetImportFile().GetId())
		}
	}
	return lo.Values(lacks)
}

func (c *importChecker) getLackFilesForImports(job ImportJob) []*datapb.ImportFileStats {
	preimports := c.importMeta.GetTaskByJob(c.ctx, job.GetJobID(), WithType(PreImportTaskType))
	lacks := make(map[int64]*datapb.ImportFileStats)
	for _, task := range preimports {
		for _, stat := range task.GetFileStats() {
			lacks[stat.GetImportFile().GetId()] = stat
		}
	}
	exists := c.importMeta.GetTaskByJob(c.ctx, job.GetJobID(), WithType(ImportTaskType))
	for _, task := range exists {
		for _, stat := range task.GetFileStats() {
			delete(lacks, stat.GetImportFile().GetId())
		}
	}
	return lo.Values(lacks)
}

func (c *importChecker) checkPendingJob(job ImportJob) {
	log := mlog.With(mlog.FieldJobID(job.GetJobID()))
	lacks := c.getLackFilesForPreImports(job)
	if len(lacks) > 0 {
		fileGroups := lo.Chunk(lacks, Params.DataCoordCfg.FilesPerPreImportTask.GetAsInt())
		newTasks, err := NewPreImportTasks(fileGroups, job, c.alloc, c.importMeta)
		if err != nil {
			log.Warn(c.ctx, "new preimport tasks failed", mlog.Err(err))
			return
		}
		for _, task := range newTasks {
			if err := c.importMeta.AddTask(c.ctx, task); err != nil {
				// The task write may already be durable even when its response is
				// lost. Continuing from stale memory would let the next Pending pass
				// publish another task for the same files. Restart and rebuild the
				// authoritative task set from catalog. Cancellation during shutdown
				// is not an ambiguous live write and returns normally.
				if c.ctx != nil && c.ctx.Err() == nil {
					mlog.Fatal(c.ctx, "preimport task publication failed; terminating process",
						WrapTaskLog(task, mlog.Err(err))...)
					// Fatal does not return in production. A test hook may replace it,
					// so stop here instead of publishing more tasks or advancing the job.
					return
				}
				log.Warn(c.ctx, "add preimport task failed", WrapTaskLog(task, mlog.Err(err))...)
				return
			}
			log.Info(c.ctx, "add new preimport task", WrapTaskLog(task, mlog.Int("fileCount", len(task.GetFileStats())))...)
		}
	}

	// Tasks are persisted before the job state. If the final state write failed,
	// the next Pending pass sees no missing files and retries this idempotent write.
	if err := c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_PreImporting)); err != nil {
		log.Warn(c.ctx, "failed to update job state to PreImporting", mlog.Err(err))
		return
	}
	pendingDuration := job.GetTR().RecordSpan()
	metrics.ImportJobLatency.WithLabelValues(metrics.ImportStagePending).Observe(float64(pendingDuration.Milliseconds()))
	log.Info(c.ctx, "import job start to execute", mlog.Duration("jobTimeCost/pending", pendingDuration))
}

// checkPreImportingJob handles the preimport phase: it waits for every PreImport task to
// complete, then decides how the job leaves the phase. A ranged job (resolvable primary key,
// not backup/L0) parks in AssigningIDRange, where the primary broadcasts — and a secondary
// waits for — the UpdateImport message. Every other job goes straight to Import-task
// creation. The transition is symmetric across clusters, so the arrival order of "all
// preimport completed" and "ranges applied" does not matter.
func (c *importChecker) checkPreImportingJob(job ImportJob) {
	log := mlog.With(mlog.FieldJobID(job.GetJobID()))

	preimports := c.importMeta.GetTaskByJob(c.ctx, job.GetJobID(), WithType(PreImportTaskType))
	if !lo.EveryBy(preimports, func(t ImportTask) bool {
		// Preimport tasks are not fully completed, thus generating imports should not be triggered.
		return t.GetState() == datapb.ImportTaskStateV2_Completed
	}) {
		return
	}

	updateJobState := func(state internalpb.ImportJobState, actions ...UpdateJobAction) {
		actions = append(actions, UpdateJobState(state))
		err := c.importMeta.UpdateJob(c.ctx, job.GetJobID(), actions...)
		if err != nil {
			log.Warn(c.ctx, "failed to update import job state",
				mlog.String("state", state.String()), mlog.Err(err))
			return
		}
		preImportDuration := job.GetTR().RecordSpan()
		metrics.ImportJobLatency.WithLabelValues(metrics.ImportStagePreImport).Observe(float64(preImportDuration.Milliseconds()))
		log.Info(c.ctx, "import job preimport done", mlog.String("state", state.String()), mlog.Duration("jobTimeCost/preimport", preImportDuration))
	}

	if needsIDRanges(job) && !jobIDRangesSet(job) {
		updateJobState(internalpb.ImportJobState_AssigningIDRange)
		return
	}

	c.createImportTasks(job, preimports, updateJobState)
}

// checkAssigningIDRangeJob is the wait state for the per-file ID ranges. The job completed
// preimport and is parked here: the primary allocates the ranges and broadcasts the
// UpdateImport message (ensureIDRanges), a secondary waits for the replicated copy, and the
// ack callback applies the ranges to the job meta. Once the ranges are present the job runs
// the shared Import-task tail. Preimport is deliberately NOT re-checked here — entering the
// state already implies it completed, both on the PreImporting transition and on restart
// recovery — and the timeout/abort handling is tryTimeoutJob's job, not this handler's.
func (c *importChecker) checkAssigningIDRangeJob(job ImportJob) {
	preimports := c.importMeta.GetTaskByJob(c.ctx, job.GetJobID(), WithType(PreImportTaskType))
	if !jobIDRangesSet(job) {
		c.ensureIDRanges(job, preimports)
		return
	}

	// Ranges applied: run the shared tail. This transition only advances the TimeRecorder
	// past the wait — it must NOT emit another PreImport stage metric, that span already ended
	// at the PreImporting → AssigningIDRange transition.
	log := mlog.With(mlog.FieldJobID(job.GetJobID()))
	updateJobState := func(state internalpb.ImportJobState, actions ...UpdateJobAction) {
		actions = append(actions, UpdateJobState(state))
		err := c.importMeta.UpdateJob(c.ctx, job.GetJobID(), actions...)
		if err != nil {
			log.Warn(c.ctx, "failed to update job state to Importing", mlog.Err(err))
			return
		}
		waitDuration := job.GetTR().RecordSpan()
		log.Info(c.ctx, "import job id range assigned", mlog.String("state", state.String()), mlog.Duration("jobTimeCost/assigningIDRange", waitDuration))
	}
	c.createImportTasks(job, preimports, updateJobState)
}

// createImportTasks is the shared tail both preimport-complete states run once the job's
// per-file ID ranges are present (or not needed): the cross-cluster divergence check, the
// zero-rows exit, then the disk-quota/regroup/NewImportTasks step into Importing. The caller
// supplies updateJobState so each entry state closes its own stage span (PreImport for
// PreImporting, the range wait for AssigningIDRange).
func (c *importChecker) createImportTasks(job ImportJob, preimports []ImportTask, updateJobState func(state internalpb.ImportJobState, actions ...UpdateJobAction)) {
	log := mlog.With(mlog.FieldJobID(job.GetJobID()))

	// Divergence check: for every ranged file the local preimport count must equal the size
	// of the range the primary allocated. A mismatch means the same file holds different rows
	// on the two clusters — the precondition CDC import already requires operators to
	// guarantee — so fail loudly with both numbers. It runs before the zero-rows exit and
	// before any Import task (hence any segment) exists, so a cluster that counted a
	// different number of rows fails instead of committing an empty or partial import. Files
	// without a range (backup/L0, or a legacy job) have nothing to compare against.
	if needsIDRanges(job) {
		expectedByFile := lo.SliceToMap(
			lo.Filter(job.GetFiles(), func(f *internalpb.ImportFile, _ int) bool { return f.GetIdRange() != nil }),
			func(f *internalpb.ImportFile) (int64, int64) {
				r := f.GetIdRange()
				return f.GetId(), r.GetEnd() - r.GetBegin()
			})
		stats := lo.FlatMap(preimports, func(t ImportTask, _ int) []*datapb.ImportFileStats {
			return t.GetFileStats()
		})
		if stat, divergent := lo.Find(stats, func(s *datapb.ImportFileStats) bool {
			expected, ranged := expectedByFile[s.GetImportFile().GetId()]
			return ranged && s.GetTotalRows() != expected
		}); divergent {
			fileID := stat.GetImportFile().GetId()
			localRows, reservedRows := stat.GetTotalRows(), expectedByFile[fileID]
			log.Warn(c.ctx, "UpdateImport divergence: local row count differs from the reserved range size; failing import — files must be identical across clusters",
				mlog.Int64("fileID", fileID), mlog.Int64("localRows", localRows), mlog.Int64("reservedRows", reservedRows))
			updateJobState(internalpb.ImportJobState_Failed, UpdateJobReason(fmt.Sprintf(
				"import file %d local row count %d does not match the reserved ID range size %d (cross-cluster file divergence)",
				fileID, localRows, reservedRows)))
			return
		}
	}

	// Zero-rows exit: nothing to read, no Import task and no cursor is needed. It runs after
	// the divergence check, so a locally empty side still has to match the peer's ranges
	// before it may commit as empty.
	totalRows := lo.SumBy(lo.FlatMap(preimports, func(t ImportTask, _ int) []*datapb.ImportFileStats {
		return t.GetFileStats()
	}), func(stat *datapb.ImportFileStats) int64 {
		return stat.GetTotalRows()
	})
	if totalRows == 0 {
		if job.GetAutoCommit() {
			log.Info(c.ctx, "no data to import, auto_commit=true, transitioning directly to Completed")
			updateJobState(internalpb.ImportJobState_Completed)
		} else {
			log.Info(c.ctx, "no data to import, auto_commit=false, transitioning to Uncommitted")
			updateJobState(internalpb.ImportJobState_Uncommitted)
		}
		return
	}

	// Keep one sort decision for the whole job. New jobs read the compaction
	// switch; recovery reads the decision from existing origin segments.
	existingTasks := c.importMeta.GetTaskByJob(c.ctx, job.GetJobID(), WithType(ImportTaskType))
	sortPlanned, err := importSortPlannedForJob(c.ctx, job, existingTasks, c.meta)
	if err != nil {
		log.Warn(c.ctx, "invalid existing import sort plan", mlog.Err(err))
		updateJobState(internalpb.ImportJobState_Failed, UpdateJobReason(err.Error()))
		return
	}

	lacks := c.getLackFilesForImports(job)

	// Stamp the authoritative per-file ranges onto the task fileStats about to be regrouped
	// into Import tasks, so they persist inside ImportTaskV2 meta and flow through
	// AssembleImportRequest into the datanode's per-file ID range.
	c.stampIDRangesOntoStats(job, lacks)

	requestSize, err := CheckDiskQuota(c.ctx, job, c.meta, c.importMeta)
	if err != nil {
		log.Warn(c.ctx, "import failed, disk quota exceeded", mlog.Err(err))
		updateJobState(internalpb.ImportJobState_Failed, UpdateJobReason(err.Error()))
		return
	}
	if len(lacks) == 0 {
		// All import tasks may have been durably added before the final job write
		// failed. Retrying that write is the idempotent recovery path: do not
		// allocate another task or another set of segments.
		updateJobState(internalpb.ImportJobState_Importing, UpdateRequestedDiskSize(requestSize))
		return
	}

	segmentMaxSize := GetSegmentMaxSize(job, c.meta)
	groups := RegroupImportFiles(job, lacks, segmentMaxSize)
	newTasks, newSegments, err := NewImportTasks(c.ctx, groups, job, c.alloc, c.meta, c.importMeta, segmentMaxSize, sortPlanned)
	if err != nil {
		log.Warn(c.ctx, "new import tasks failed", mlog.Err(err))
		return
	}
	importMeta, ok := c.importMeta.(*importMeta)
	if !ok {
		err = merr.WrapErrImportSysFailedMsg("import task publication requires catalog-backed metadata")
		log.Warn(c.ctx, "add new import tasks failed", mlog.Err(err))
		updateJobState(internalpb.ImportJobState_Failed, UpdateJobReason(err.Error()))
		return
	}
	if err = importMeta.addImportTasks(c.ctx, c.meta, newTasks, newSegments); err != nil {
		if c.ctx.Err() == nil {
			mlog.Fatal(c.ctx, "import task plan publication failed; terminating process", mlog.Err(err))
		}
		log.Warn(c.ctx, "add new import tasks failed", mlog.Err(err))
		updateJobState(internalpb.ImportJobState_Failed, UpdateJobReason(err.Error()))
		return
	}
	for _, t := range newTasks {
		log.Info(c.ctx, "add new import task", WrapTaskLog(t, mlog.Int("fileCount", len(t.GetFileStats())))...)
	}

	updateJobState(internalpb.ImportJobState_Importing, UpdateRequestedDiskSize(requestSize))
}

// needsIDRanges reports whether the job requires per-file ID ranges: it has a resolvable
// primary key and is neither a backup (which keeps embedded PK/RowID/ts) nor an L0 import
// (which carries no PK/RowID). The range supplies the primary key on autoID collections
// and the RowID on explicit-PK ones, so the datanode's PK/RowID stay deterministic across
// clusters and the task-level IDRange is only consumed by binlog logIDs. A schema without
// a resolvable primary key is left to normal validation (no ranges).
//
// The whole two-phase path is version-gated behind ImportEnableIDRangeMsg. While the
// gate reads false (the default "auto" resolves to "false" until the MixCoord confirmator
// flips it after every node, streaming nodes included, reaches the gate version), this
// returns false so the job never enters AssigningIDRange and never broadcasts the new
// UpdateImport V2 message. That is what keeps an older streaming node's flusher from
// panicking on a message type it does not know during a rolling upgrade; the legacy
// local-allocator path takes over instead.
//
// A package function, not a checker method: the UpdateImport ack callback needs the same
// condition to reject a range message for a job that carries none.
func needsIDRanges(job ImportJob) bool {
	return idRangeMsgEnabled() && importid.NeedsFileIDRanges(job.GetSchema(), job.GetOptions())
}

// idRangeMsgEnabled reports whether the two-phase per-file ID range path is active. It is
// version-gated (see ImportEnableIDRangeMsg): the effective value is the legacy behavior
// ("false") until the MixCoord version-gate confirmator observes every online node at or
// above the gate version and flips the config to "true". Reads the resolved value, so an
// explicit "true"/"false" and the embed-etcd single-process shortcut are all honored.
func idRangeMsgEnabled() bool {
	return Params.DataCoordCfg.ImportEnableIDRangeMsg.GetAsBool()
}

// jobIDRangesSet reports whether every job file has a non-nil reserved ID range. It is a
// nil check, NOT End>Begin: a zero-row file legitimately carries an empty (Begin==End)
// range that still counts as set.
func jobIDRangesSet(job ImportJob) bool {
	return lo.EveryBy(job.GetFiles(), func(f *internalpb.ImportFile) bool {
		return f.GetIdRange() != nil
	})
}

// ensureIDRanges triggers (primary / non-replicating cluster) or waits for (secondary) the
// UpdateImport broadcast that populates the job's per-file ID ranges. Called from the
// AssigningIDRange state when the ranges are unset; the job stays in AssigningIDRange
// either way, bounded by its timeoutTs.
func (c *importChecker) ensureIDRanges(job ImportJob, preimports []ImportTask) {
	log := mlog.With(mlog.FieldJobID(job.GetJobID()))

	// Bound the WAL append / ack wait of the broadcast, so a stalled WAL or unavailable
	// streamingnode becomes a transient status instead of parking the state-machine loop
	// under the server-lifetime c.ctx (same pattern as checkGC).
	//
	// This does NOT bound the resource-key acquire: the lock is ctx-insensitive (same as
	// checkGC), so a wait behind a pending broadcast on the same collection ends only when
	// that broadcast's ack callback runs (MarkAckCallbackDone releases the lock), and it parks
	// this single state-machine goroutine meanwhile. A timeout is just another transient
	// status → retry next tick.
	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()

	// Decide the replication role. A nil hook means the feature is disabled (tests): treat
	// as a non-replicating primary and proceed to allocate/broadcast. An indeterminate
	// (error) role must NOT allocate — a secondary that allocated would diverge from the
	// primary's authoritative range — so wait and retry next tick.
	var role replicateutil.Role
	replicating := false
	if c.hooks.getReplicationRole != nil {
		r, rep, err := c.hooks.getReplicationRole(ctx)
		if err != nil {
			log.Warn(ctx, "cannot determine replication role before UpdateImport broadcast, will retry next tick", mlog.Err(err))
			return
		}
		role, replicating = r, rep
	}

	if replicating && role == replicateutil.RoleSecondary {
		// Secondary: the primary broadcasts UpdateImport and it is replicated here; that
		// ack callback applies the ranges. Do NOT allocate locally (it would diverge from
		// the primary's authoritative range). Debug, not Info: this repeats every ~2s tick.
		log.Debug(ctx, "waiting for replicated UpdateImport")
		return
	}

	if c.hooks.assignImportIDRange == nil {
		log.Error(ctx, "assignImportIDRange hook is nil but autoID import requires ID ranges; this is a programming error")
		return
	}

	// Build the per-file row counts, aligned with job.GetFiles() order, from the
	// completed preimport stats. A missing stat is an internal error (preimport is fully
	// completed before the gate); retry next tick.
	fileRows, ok := c.preimportFileRows(job, preimports)
	if !ok {
		return
	}

	log.Info(ctx, "triggering UpdateImport broadcast",
		mlog.Int("fileCount", len(job.GetFiles())), mlog.Int64("totalRows", lo.Sum(fileRows)))

	if err := c.hooks.assignImportIDRange(ctx, job, fileRows); err != nil {
		if errors.Is(err, broadcaster.ErrNotPrimary) {
			// Stale role during switchover: this cluster thought it was primary but the
			// broadcaster rejected the append. Treat as "wait" — the real primary broadcasts
			// the authoritative range and it is replicated here. Retry next tick.
			log.Info(ctx, "role flipped to standby while importing, waiting for replicated UpdateImport")
		} else if merr.GetErrorType(err) == merr.InputError {
			// Permanent: the request content itself cannot be ranged (a single file holding
			// more than one allocation batch, or a negative count). No retry changes that, so
			// fail the job now and keep the precise reason (e.g. "split the file") instead of
			// retrying every tick until tryTimeoutJob replaces it with a generic timeout.
			// Allocator and WAL failures stay retriable in the branch below.
			if updateErr := c.importMeta.UpdateJob(ctx, job.GetJobID(),
				UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(err.Error())); updateErr != nil {
				log.Warn(ctx, "failed to mark import job failed after a non-retriable range allocation error", mlog.Err(updateErr))
			}
		} else {
			log.Warn(ctx, "UpdateImport broadcast failed, will retry next tick", mlog.Err(err))
		}
		return
	}
}

// preimportFileRows returns the per-file row counts aligned with job.GetFiles() order,
// summed from the completed preimport task stats by fileID. A file with no stat is an
// internal error — preimport is fully completed before the gate — and ok=false tells the
// caller to retry next tick.
func (c *importChecker) preimportFileRows(job ImportJob, preimports []ImportTask) ([]int64, bool) {
	log := mlog.With(mlog.FieldJobID(job.GetJobID()))
	rowsByFile := make(map[int64]int64)
	for _, t := range preimports {
		for _, stat := range t.GetFileStats() {
			rowsByFile[stat.GetImportFile().GetId()] += stat.GetTotalRows()
		}
	}
	files := job.GetFiles()
	if f, missing := lo.Find(files, func(f *internalpb.ImportFile) bool {
		_, ok := rowsByFile[f.GetId()]
		return !ok
	}); missing {
		log.Warn(c.ctx, "preimport stats missing for an import file, will retry next tick", mlog.Int64("fileID", f.GetId()))
		return nil, false
	}
	return lo.Map(files, func(f *internalpb.ImportFile, _ int) int64 {
		return rowsByFile[f.GetId()]
	}), true
}

// stampIDRangesOntoStats copies the job's authoritative per-file ID ranges onto the task
// fileStats (by fileID) so they persist into ImportTaskV2 meta and flow through
// AssembleImportRequest. An already-equal range is left untouched (idempotent).
func (c *importChecker) stampIDRangesOntoStats(job ImportJob, lacks []*datapb.ImportFileStats) {
	rangeByFile := lo.SliceToMap(
		lo.Filter(job.GetFiles(), func(f *internalpb.ImportFile, _ int) bool { return f.GetIdRange() != nil }),
		func(f *internalpb.ImportFile) (int64, *commonpb.IDRange) {
			return f.GetId(), f.GetIdRange()
		})
	for _, stat := range lacks {
		importFile := stat.GetImportFile()
		if importFile == nil {
			continue
		}
		r, ok := rangeByFile[importFile.GetId()]
		if !ok {
			continue
		}
		cur := importFile.GetIdRange()
		if cur.GetBegin() == r.GetBegin() && cur.GetEnd() == r.GetEnd() {
			continue
		}
		importFile.IdRange = r
	}
}

func (c *importChecker) checkImportingJob(job ImportJob) {
	log := mlog.With(mlog.FieldJobID(job.GetJobID()))
	tasks := c.importMeta.GetTaskByJob(c.ctx, job.GetJobID(), WithType(ImportTaskType), WithRequestSource())
	for _, t := range tasks {
		if t.GetState() != datapb.ImportTaskStateV2_Completed {
			return
		}
	}
	err := c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Sorting))
	if err != nil {
		log.Warn(c.ctx, "failed to update job state to Stats", mlog.Err(err))
		return
	}
	importDuration := job.GetTR().RecordSpan()
	metrics.ImportJobLatency.WithLabelValues(metrics.ImportStageImport).Observe(float64(importDuration.Milliseconds()))
	log.Info(c.ctx, "import job import done", mlog.Duration("jobTimeCost/import", importDuration))
}

func (c *importChecker) checkSortingJob(job ImportJob) {
	log := mlog.With(mlog.FieldJobID(job.GetJobID()))
	updateJobState := func(state internalpb.ImportJobState, reason string) {
		err := c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(state), UpdateJobReason(reason))
		if err != nil {
			log.Warn(c.ctx, "failed to update job state", mlog.Err(err))
			return
		}
		statsDuration := job.GetTR().RecordSpan()
		metrics.ImportJobLatency.WithLabelValues(metrics.ImportStageStats).Observe(float64(statsDuration.Milliseconds()))
		log.Info(c.ctx, "import job stats done", mlog.String("state", state.String()), mlog.Duration("jobTimeCost/stats", statsDuration))
	}

	// Read the decision persisted on the origin segments. This keeps a job
	// stable if the compaction switch changes after its tasks were created.
	tasks := c.importMeta.GetTaskByJob(c.ctx, job.GetJobID(), WithType(ImportTaskType))
	sortPlanned, err := importSortPlannedForJob(c.ctx, job, tasks, c.meta)
	if err != nil {
		log.Warn(c.ctx, "invalid import sort plan", mlog.Err(err))
		updateJobState(internalpb.ImportJobState_Failed, err.Error())
		return
	}
	if !sortPlanned {
		updateJobState(internalpb.ImportJobState_IndexBuilding, "")
		return
	}

	// Check and trigger stats tasks.
	var (
		taskCnt    = 0
		doneCnt    = 0
		collection *collectionInfo
	)
	for _, task := range tasks {
		originSegmentIDs := task.(*importTask).GetSegmentIDs()
		taskCnt += len(originSegmentIDs)
		for _, originSegmentID := range originSegmentIDs {
			logger := mlog.With(WrapTaskLog(task, mlog.Int64("origin", originSegmentID))...)
			// The sorted output is discovered through the segment's compactionTo
			// edge, written by CompleteCompactionMutation when the sort stage
			// committed it -- no preallocated target ID exists in the plan.
			if outputs, _ := c.meta.GetCompactionTo(originSegmentID); len(outputs) > 0 {
				// sort compaction is already done
				doneCnt++
				continue
			}
			originSegment := c.meta.GetHealthySegment(c.ctx, originSegmentID)
			if originSegment == nil {
				// createSortCompactionTask deliberately drops a zero-row origin and
				// creates no output. Do not treat every missing/unhealthy origin as
				// completed: only the durable marker written by that branch is valid.
				if isExplicitZeroRowOriginSkip(job, c.meta.GetSegment(c.ctx, originSegmentID)) {
					doneCnt++
				} else {
					logger.Warn(c.ctx, "sort origin and its output are both unavailable")
				}
				continue
			}
			// if not compacting, trigger sort compaction task
			isCompacting := c.meta.IsSegmentCompacting(originSegmentID)
			if !isCompacting {
				// Fetch once per job pass, only when a non-empty origin needs a
				// sort plan. Repeating the same failed lookup for every origin
				// would stall all jobs sharing this checker loop.
				if originSegment.GetNumOfRows() != 0 && collection == nil {
					collection, err = c.handler.GetCollection(c.ctx, job.GetCollectionID())
					if err != nil {
						log.Warn(c.ctx, "failed to get collection for import sorting",
							mlog.FieldCollectionID(job.GetCollectionID()), mlog.Err(err))
						if errors.Is(err, merr.ErrCollectionNotFound) {
							updateJobState(internalpb.ImportJobState_Failed,
								fmt.Sprintf("collection %d dropped", job.GetCollectionID()))
						}
						return
					}
					if collection == nil {
						log.Warn(c.ctx, "collection lookup returned no metadata for import sorting",
							mlog.FieldCollectionID(job.GetCollectionID()))
						return
					}
				}
				compactionTask, err := createSortCompactionTask(c.ctx, task, originSegment, c.meta, collection, c.alloc)
				if err != nil {
					logger.Warn(c.ctx, "create sort compaction task failed", mlog.Err(err))
					continue
				}
				if compactionTask == nil {
					logger.Info(c.ctx, "maybe it no need to create sort compaction task")
					doneCnt++
					continue
				}
				err = c.ci.enqueueCompaction(compactionTask)
				if err != nil {
					logger.Warn(c.ctx, "sort compaction task enqueue failed", mlog.Err(err))
					continue
				}
				logger.Info(c.ctx, "create sort compaction task and enqueue success")
			}
		}
	}

	// All segments are stats-ed. Update job state to `IndexBuilding`.
	if taskCnt == doneCnt {
		updateJobState(internalpb.ImportJobState_IndexBuilding, "")
	}
}

// isExplicitZeroRowOriginSkip recognizes the durable marker written when a
// preallocated import origin received no rows. The origin is dropped without
// any data target being produced.
func isExplicitZeroRowOriginSkip(job ImportJob, origin *SegmentInfo) bool {
	return job != nil && origin != nil &&
		origin.GetState() == commonpb.SegmentState_Dropped &&
		origin.GetNumOfRows() == 0 && origin.GetIsImporting() &&
		origin.GetCollectionID() == job.GetCollectionID() &&
		(len(job.GetPartitionIDs()) == 0 || lo.Contains(job.GetPartitionIDs(), origin.GetPartitionID())) &&
		(len(job.GetVchannels()) == 0 || lo.Contains(job.GetVchannels(), origin.GetInsertChannel()))
}

func isExplicitZeroRowSortedOutputSkip(job ImportJob, originSegmentID int64, output *SegmentInfo) bool {
	return job != nil && output != nil &&
		output.GetState() == commonpb.SegmentState_Dropped &&
		output.GetNumOfRows() == 0 && output.GetIsImporting() &&
		output.GetCollectionID() == job.GetCollectionID() &&
		(len(job.GetPartitionIDs()) == 0 || lo.Contains(job.GetPartitionIDs(), output.GetPartitionID())) &&
		(len(job.GetVchannels()) == 0 || lo.Contains(job.GetVchannels(), output.GetInsertChannel())) &&
		lo.Contains(output.GetCompactionFrom(), originSegmentID)
}

func (c *importChecker) getValidatedImportTargets(job ImportJob, tasks []ImportTask, sortPlanned bool) ([]int64, error) {
	targetSegmentIDs := make([]int64, 0)
	seen := make(map[int64]struct{})
	originCount := 0
	zeroRowSkipCount := 0
	for _, task := range tasks {
		importTask := task.(*importTask)
		originSegmentIDs := importTask.GetSegmentIDs()
		if !sortPlanned {
			originCount += len(originSegmentIDs)
			for _, segmentID := range originSegmentIDs {
				if isExplicitZeroRowOriginSkip(job, c.meta.GetSegment(c.ctx, segmentID)) {
					zeroRowSkipCount++
					continue
				}
				if err := c.validateImportTarget(job, segmentID, seen, false, 0); err != nil {
					return nil, err
				}
				targetSegmentIDs = append(targetSegmentIDs, segmentID)
			}
			continue
		}
		// The sorted output is the segment the origin was compacted into,
		// discovered through the durable compactionTo edge. An origin without
		// an output is either a completed zero-row skip or durable plan
		// corruption.
		for _, originSegmentID := range originSegmentIDs {
			originCount++
			outputs, _ := c.meta.GetCompactionTo(originSegmentID)
			if len(outputs) == 0 {
				origin := c.meta.GetSegment(c.ctx, originSegmentID)
				if isExplicitZeroRowOriginSkip(job, origin) {
					zeroRowSkipCount++
					continue
				}
				// A legacy job planned without sort has healthy, visible,
				// importing origins and no output. A sort-planned origin is
				// invisible, so visibility distinguishes that legacy shape from
				// a missing sorted output, which is durable plan corruption.
				if origin != nil && isSegmentHealthy(origin) && !origin.GetIsInvisible() && origin.GetIsImporting() {
					if err := c.validateImportTarget(job, originSegmentID, seen, false, 0); err != nil {
						return nil, err
					}
					targetSegmentIDs = append(targetSegmentIDs, originSegmentID)
					continue
				}
				return nil, merr.WrapErrImportSysFailedMsg(
					"invalid import target plan: origin segment %d has no sorted output", originSegmentID)
			}
			for _, output := range outputs {
				segmentID := output.GetID()
				// A zero-row sorted output is published Dropped (all rows
				// expired or deleted before the sort): the branch is a
				// completed empty result, exactly like a zero-row origin
				// skip. It must not be added as a target, and must not fail
				// the job. Other drop shapes are unreachable here: importing
				// segments are excluded from every compaction selector, so no
				// downstream compaction can consume an output while the job is
				// still in flight.
				if isExplicitZeroRowSortedOutputSkip(job, originSegmentID, output) {
					zeroRowSkipCount++
					continue
				}
				if err := c.validateImportTarget(job, segmentID, seen, true, originSegmentID); err != nil {
					return nil, err
				}
				targetSegmentIDs = append(targetSegmentIDs, segmentID)
			}
		}
	}
	if len(targetSegmentIDs) == 0 {
		if originCount > 0 && zeroRowSkipCount == originCount {
			// Every origin was an explicitly skipped zero-row branch: a valid
			// empty result.
			return targetSegmentIDs, nil
		}
		return nil, merr.WrapErrImportSysFailedMsg("invalid import target plan: job %d has no target segments", job.GetJobID())
	}
	return targetSegmentIDs, nil
}

// validateImportTarget checks one target segment against the job's plan and,
// for a sorted target, against its origin's compaction edge.
func (c *importChecker) validateImportTarget(job ImportJob, segmentID int64, seen map[int64]struct{}, sortPlanned bool, originSegmentID int64) error {
	if _, ok := seen[segmentID]; ok {
		return merr.WrapErrImportSysFailedMsg(
			"invalid import target plan: segment %d is selected more than once", segmentID)
	}
	seen[segmentID] = struct{}{}

	segment := c.meta.GetSegment(c.ctx, segmentID)
	if segment == nil {
		return merr.WrapErrImportSysFailedMsg(
			"invalid import target plan: segment %d is missing", segmentID)
	}
	if !isSegmentHealthy(segment) {
		return merr.WrapErrImportSysFailedMsg(
			"invalid import target plan: segment %d is unhealthy in state %s",
			segmentID, segment.GetState().String())
	}
	if segment.GetState() != commonpb.SegmentState_Flushed {
		return merr.WrapErrImportSysFailedMsg(
			"invalid import target plan: segment %d must be Flushed, got %s",
			segmentID, segment.GetState().String())
	}
	if segment.GetCollectionID() != job.GetCollectionID() {
		return merr.WrapErrImportSysFailedMsg(
			"invalid import target plan: segment %d belongs to collection %d, expected %d",
			segmentID, segment.GetCollectionID(), job.GetCollectionID())
	}
	if len(job.GetPartitionIDs()) > 0 && !lo.Contains(job.GetPartitionIDs(), segment.GetPartitionID()) {
		return merr.WrapErrImportSysFailedMsg(
			"invalid import target plan: segment %d belongs to partition %d outside job partitions %v",
			segmentID, segment.GetPartitionID(), job.GetPartitionIDs())
	}
	if len(job.GetVchannels()) > 0 && !lo.Contains(job.GetVchannels(), segment.GetInsertChannel()) {
		return merr.WrapErrImportSysFailedMsg(
			"invalid import target plan: segment %d belongs to channel %q outside job channels %v",
			segmentID, segment.GetInsertChannel(), job.GetVchannels())
	}
	if !segment.GetIsImporting() {
		return merr.WrapErrImportSysFailedMsg(
			"invalid import target plan: segment %d is already published", segmentID)
	}
	if sortPlanned {
		// Namespace-enabled collections mark their output IsSortedByNamespace
		// instead of IsSorted (sort_compaction sets one or the other).
		if (!segment.GetIsSorted() && !segment.GetIsSortedByNamespace()) || !lo.Contains(segment.GetCompactionFrom(), originSegmentID) {
			return merr.WrapErrImportSysFailedMsg(
				"invalid import target plan: sorted segment %d does not derive from origin segment %d",
				segmentID, originSegmentID)
		}
	}
	return nil
}

func (c *importChecker) checkIndexBuildingJob(job ImportJob) {
	log := mlog.With(mlog.FieldJobID(job.GetJobID()))
	tasks := c.importMeta.GetTaskByJob(c.ctx, job.GetJobID(), WithType(ImportTaskType))
	sortPlanned, err := importSortPlannedForJob(c.ctx, job, tasks, c.meta)
	if err != nil {
		log.Warn(c.ctx, "invalid import sort plan", mlog.Err(err))
		if updateErr := c.importMeta.UpdateJob(c.ctx, job.GetJobID(),
			UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(err.Error())); updateErr != nil {
			log.Warn(c.ctx, "failed to update invalid import job to Failed", mlog.Err(updateErr))
		}
		return
	}
	targetSegmentIDs, err := c.getValidatedImportTargets(job, tasks, sortPlanned)
	if err != nil {
		// Import completion persists origin metadata before task completion, and
		// sorting persists its target before advancing the job here. A missing,
		// dropped, foreign, or already-published target is therefore durable plan
		// corruption rather than an eventually-ready index; fail immediately
		// instead of waiting until the job timeout.
		log.Warn(c.ctx, "invalid import target segments", mlog.Err(err))
		if updateErr := c.importMeta.UpdateJob(c.ctx, job.GetJobID(),
			UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(err.Error())); updateErr != nil {
			log.Warn(c.ctx, "failed to update invalid import job to Failed", mlog.Err(updateErr))
		}
		return
	}

	unindexed := c.meta.indexMeta.GetUnindexedSegments(job.GetCollectionID(), targetSegmentIDs)
	if Params.DataCoordCfg.WaitForIndex.GetAsBool() && len(unindexed) > 0 && !importutilv2.IsL0Import(job.GetOptions()) {
		for _, segmentID := range unindexed {
			select {
			case getBuildIndexChSingleton() <- segmentID: // accelerate index building:
			default:
			}
		}
		log.Debug(c.ctx, "waiting for import segments building index...", mlog.Int64s("unindexed", unindexed))
		return
	}
	buildIndexDuration := job.GetTR().RecordSpan()
	metrics.ImportJobLatency.WithLabelValues(metrics.ImportStageBuildIndex).Observe(float64(buildIndexDuration.Milliseconds()))
	log.Info(c.ctx, "import job build index done", mlog.Duration("jobTimeCost/buildIndex", buildIndexDuration))

	// Both auto-commit and explicit commit use the CommitImport broadcast
	// callback to publish segment visibility and complete the job. Until then
	// imported segments remain invisible in Uncommitted.
	err = c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Uncommitted))
	if err != nil {
		log.Warn(c.ctx, "failed to update job state to Uncommitted", mlog.Err(err))
		return
	}
	LogResultSegmentsInfo(job.GetJobID(), c.meta, targetSegmentIDs)
	log.Info(c.ctx, "import job indexes built, transitioned to Uncommitted",
		mlog.Bool("autoCommit", job.GetAutoCommit()))
}

// checkUncommittedJob handles jobs in the Uncommitted state.
// If auto_commit=true, the commit hook acquires the collection DDL lock,
// persists the commit intent, and broadcasts the CommitImport message. A
// failed broadcast is replayed from Committing.
// If auto_commit=false, it waits for an explicit CommitImport RPC from the platform.
func (c *importChecker) checkUncommittedJob(job ImportJob) {
	log := mlog.With(mlog.FieldJobID(job.GetJobID()))
	if !job.GetAutoCommit() {
		// Wait for explicit CommitImport from the replication platform.
		return
	}
	// auto_commit=true: trigger commit by broadcasting the WAL message.
	// Repeated invocations across ticks are safe: the broadcaster's exclusive
	// collection-level resource-key lock serializes overlapping broadcasts, the
	// ack callback only transitions when the job is still Uncommitted, and
	// the completed callback is a no-op on replay.
	if c.hooks.commitImport == nil {
		log.Error(c.ctx, "commit hook is nil but auto_commit=true; this is a programming error",
			mlog.Err(merr.WrapErrServiceInternalMsg("commit hook is nil for auto-commit import job")))
		return
	}
	if len(job.GetVchannels()) == 0 {
		log.Warn(c.ctx, "cannot commit import job without vchannels",
			mlog.Err(merr.WrapErrImportSysFailedMsg("job %d has no vchannels", job.GetJobID())))
		return
	}
	c.broadcastCommitImport(job, "auto-commit")
}

// checkCommittingJob handles jobs in the Committing state.
// It replays the durable commit intent until all vchannels acknowledge the
// fence, then transitions the job to Completed. Replays may create duplicate
// WAL messages; commit handling is idempotent and broadcaster tombstone GC
// removes each finished broadcast task.
func (c *importChecker) checkCommittingJob(job ImportJob) {
	log := mlog.With(mlog.FieldJobID(job.GetJobID()))
	if len(job.GetVchannels()) == 0 {
		log.Warn(c.ctx, "cannot complete committing import job without vchannels")
		return
	}
	if job.GetCommitByCoordinator() {
		// The local commit intent is persisted before the broadcast. Replaying
		// closes a crash between those writes; only the callback may publish
		// visibility and Completed for coordinator-owned jobs.
		c.broadcastCommitImport(job, "commit recovery")
		return
	}
	if !funcutil.SliceSetEqual(job.GetCommittedVchannels(), job.GetVchannels()) {
		c.broadcastCommitImport(job, "commit recovery")
		return
	}
	completeTime := time.Now().Format("2006-01-02T15:04:05Z07:00")
	if err := c.importMeta.UpdateJob(c.ctx, job.GetJobID(),
		UpdateJobState(internalpb.ImportJobState_Completed),
		UpdateJobCompleteTime(completeTime),
	); err != nil {
		log.Warn(c.ctx, "failed to transition Committing to Completed", mlog.Err(err))
		return
	}
	totalDuration := job.GetTR().ElapseSpan()
	metrics.ImportJobLatency.WithLabelValues(metrics.TotalLabel).Observe(float64(totalDuration.Milliseconds()))
	log.Info(c.ctx, "import job Committing done, all vchannels committed",
		mlog.Duration("jobTimeCost/total", totalDuration))
}

func (c *importChecker) broadcastCommitImport(job ImportJob, operation string) {
	log := mlog.With(mlog.FieldJobID(job.GetJobID()))
	if c.hooks.commitImport == nil {
		log.Error(c.ctx, "commit hook is nil; this is a programming error",
			mlog.String("operation", operation),
			mlog.Err(merr.WrapErrServiceInternalMsg("commit hook is nil for %s", operation)))
		return
	}
	if err := c.hooks.commitImport(c.ctx, job); err != nil {
		if errors.Is(err, broadcaster.ErrNotPrimary) {
			// A secondary recovers the replicated broadcast from streaming
			// metadata; it cannot originate another CommitImport itself.
			log.Debug(c.ctx, "commit replay skipped on non-primary cluster",
				mlog.String("operation", operation))
			return
		}
		log.Warn(c.ctx, "commit broadcast failed, will retry on next tick",
			mlog.String("operation", operation), mlog.Err(err))
	}
}

func (c *importChecker) checkFailedJob(job ImportJob) {
	c.tryFailingTasks(job)
}

func (c *importChecker) tryFailingTasks(job ImportJob) {
	tasks := c.importMeta.GetTaskByJob(c.ctx, job.GetJobID(), WithStates(datapb.ImportTaskStateV2_Pending,
		datapb.ImportTaskStateV2_InProgress, datapb.ImportTaskStateV2_Completed, datapb.ImportTaskStateV2_Retry))
	if len(tasks) == 0 {
		return
	}
	mlog.Warn(c.ctx, "Import job has failed, all tasks with the same jobID will be marked as failed",
		mlog.FieldJobID(job.GetJobID()), mlog.String("reason", job.GetReason()))
	for _, task := range tasks {
		update := func() {
			err := c.importMeta.UpdateTask(c.ctx, task.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed),
				UpdateReason(job.GetReason()))
			if err != nil {
				mlog.Warn(c.ctx, "failed to update import task state to failed", WrapTaskLog(task, mlog.Err(err))...)
			}
		}
		if c.scheduler == nil {
			update()
		} else {
			c.scheduler.Update(task.GetTaskID(), update)
		}
	}
}

func (c *importChecker) tryTimeoutJob(job ImportJob) {
	switch job.GetState() {
	case internalpb.ImportJobState_Failed, internalpb.ImportJobState_Completed,
		internalpb.ImportJobState_Committing:
		// Fast path on this tick's snapshot only; UpdateJobState enforces the
		// rule against the current state under importMeta's lock.
		return
	}
	// Legacy or edge records may carry no timeout; mirror the copy-segment
	// guard and leave them to explicit failure paths.
	if job.GetTimeoutTs() == 0 {
		return
	}
	timeoutTime := tsoutil.PhysicalTime(job.GetTimeoutTs())
	if time.Now().After(timeoutTime) {
		mlog.Warn(c.ctx, "Import timeout, expired the specified time limit",
			mlog.FieldJobID(job.GetJobID()), mlog.Time("timeoutTime", timeoutTime))
		err := c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed),
			UpdateJobReason("import timeout"))
		if err != nil {
			mlog.Warn(c.ctx, "failed to update job state to Failed", mlog.FieldJobID(job.GetJobID()), mlog.Err(err))
		}
	}
}

func (c *importChecker) checkCollection(collectionID int64, jobs []ImportJob) {
	if len(jobs) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()
	has, err := c.broker.HasCollection(ctx, collectionID)
	if err != nil {
		mlog.Warn(c.ctx, "verify existence of collection failed", mlog.Int64("collection", collectionID), mlog.Err(err))
		return
	}
	if !has {
		jobs = lo.Filter(jobs, func(job ImportJob, _ int) bool {
			return job.GetState() != internalpb.ImportJobState_Failed && job.GetState() != internalpb.ImportJobState_Completed
		})
		for _, job := range jobs {
			err = c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed),
				UpdateJobReason(fmt.Sprintf("collection %d dropped", collectionID)))
			if err != nil {
				mlog.Warn(c.ctx, "failed to update job state to Failed", mlog.FieldJobID(job.GetJobID()), mlog.Err(err))
			}
		}
	}
}

func (c *importChecker) checkGC(job ImportJob) {
	if job.GetState() != internalpb.ImportJobState_Completed &&
		job.GetState() != internalpb.ImportJobState_Failed {
		return
	}
	cleanupTime := tsoutil.PhysicalTime(job.GetCleanupTs())
	if time.Now().After(cleanupTime) {
		log := mlog.With(mlog.FieldJobID(job.GetJobID()))
		gcRetention := Params.DataCoordCfg.ImportTaskRetention.GetAsDuration(time.Second)
		log.Info(c.ctx, "job has reached the GC retention",
			mlog.Time("cleanupTime", cleanupTime), mlog.Duration("gcRetention", gcRetention))
		tasks := c.importMeta.GetTaskByJob(c.ctx, job.GetJobID())
		shouldRemoveJob := true
		for _, task := range tasks {
			if !c.cleanupTaskForGC(job, task.GetTaskID()) {
				shouldRemoveJob = false
			}
		}
		if !shouldRemoveJob {
			return
		}
		// In a CDC replicating cluster, a failed 2PC source import must release the
		// peer cluster's replicated Uncommitted job before we drop it — otherwise the
		// peer is stranded with invisible imported segments and no recovery path, since
		// source GC never touches the peer. Removal of the job is itself the idempotency
		// guard: once gone we never re-broadcast. Auto-commit jobs have no 2PC peer to
		// release, so they skip the gate entirely.
		if c.hooks.rollbackImport != nil && c.hooks.getReplicationRole != nil &&
			job.GetState() == internalpb.ImportJobState_Failed && !job.GetAutoCommit() {
			// The check reaches the streaming balancer future, which blocks until the
			// balancer is registered — under the server-lifetime c.ctx that would park
			// the GC loop during the window before streamingcoord registers
			// it (e.g. a restart recovering a job already past retention). Bound it like
			// checkCollection does; a timeout is just another indeterminate status.
			replicateCheckCtx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
			_, replicating, err := c.hooks.getReplicationRole(replicateCheckCtx)
			cancel()
			switch {
			case err != nil:
				// Indeterminate replication status (e.g. a transient balancer error during
				// shutdown, when streamingcoord stops before datacoord). Removing the job now
				// could strand a replicating peer's Uncommitted job with no recovery path,
				// which is irreversible — a false "not replicating" costs nothing but a retry,
				// so keep the job and re-evaluate on the next GC tick.
				log.Warn(c.ctx, "cannot determine replication status before GC of failed import job, will retry", mlog.Err(err))
				return
			case replicating:
				// Broadcast the RollbackImport to release the peer. A transient error keeps
				// the job to retry next tick; a permanent error (standby ErrNotPrimary, or the
				// collection was dropped — itself a replicated DDL, so the peer fails its own
				// job independently) falls through to GC, since retrying it forever would leak
				// the job's metadata.
				//
				// Bound the broadcast like the replication check above: it blocks in
				// BlockUntilDone until every vchannel append succeeds, and under the
				// server-lifetime c.ctx an unavailable streamingnode would park this
				// loop until shutdown. A timeout is just another transient status —
				// keep the job and retry on the next GC tick. The resource-key lock on
				// the broadcast path is still ctx-insensitive (making it fail-fast is a
				// follow-up), which is one reason this GC loop runs on its own
				// goroutine (see Start): even an unbounded park here can only delay
				// GC, never the import state machine.
				rollbackCtx, rollbackCancel := context.WithTimeout(c.ctx, 10*time.Second)
				err := c.hooks.rollbackImport(rollbackCtx, job)
				rollbackCancel()
				if err != nil && !isPermanentRollbackErr(err) {
					log.Warn(c.ctx, "failed to broadcast rollback before GC of failed replicate import job, will retry", mlog.Err(err))
					return
				}
				log.Info(c.ctx, "proceeding with GC of failed replicate import job after rollback attempt")
			}
		}
		err := c.importMeta.RemoveJob(c.ctx, job.GetJobID())
		if err != nil {
			log.Warn(c.ctx, "remove import job failed", mlog.Err(err))
			return
		}
		log.Info(c.ctx, "import job removed")
	}
}

func (c *importChecker) cleanupTaskForGC(job ImportJob, taskID int64) bool {
	cleaned := false
	cleanup := func() {
		// Create may have persisted an assignment while Finalize waited for its
		// callback. Re-read worker ownership and failed-import cleanup anchors only
		// after that callback drains.
		latest := c.importMeta.GetTask(c.ctx, taskID)
		if latest == nil {
			cleaned = true
			return
		}
		if job.GetState() == internalpb.ImportJobState_Failed && latest.GetType() == ImportTaskType {
			importTask := latest.(*importTask)
			if len(importTask.GetSegmentIDs()) != 0 || len(importTask.GetSortedSegmentIDs()) != 0 {
				return
			}
		}
		if latest.GetNodeID() != NullNodeID {
			if c.cluster == nil {
				mlog.Warn(c.ctx, "cannot drop assigned import task during GC", WrapTaskLog(latest)...)
				return
			}
			if err := dropImportTaskOnWorker(latest, c.cluster); err != nil {
				mlog.Warn(c.ctx, "failed to drop import task on its worker during GC, will retry",
					WrapTaskLog(latest, mlog.Err(err))...)
				return
			}
		}
		if err := c.importMeta.RemoveTask(c.ctx, taskID); err != nil {
			mlog.Warn(c.ctx, "remove task failed during GC", WrapTaskLog(latest, mlog.Err(err))...)
			return
		}
		mlog.Info(c.ctx, "reached GC retention, task removed", WrapTaskLog(latest)...)
		cleaned = true
	}
	if c.scheduler == nil {
		// Tests that do not exercise scheduler concurrency use the direct path.
		cleanup()
	} else {
		c.scheduler.Finalize(taskID, cleanup)
	}
	return cleaned
}

// isPermanentRollbackErr reports whether a RollbackImport broadcast error is permanent,
// i.e. retrying it can never succeed, so the failed job should still be GC'd rather than
// retried forever (which would leak its metadata). Everything else is treated as transient
// and retried on the next GC tick — misclassifying a transient error as permanent would
// drop a replicating job without releasing the peer, which is irreversible.
func isPermanentRollbackErr(err error) bool {
	// ErrNotPrimary: this cluster is a replication standby, not the primary that owns the
	// broadcast; its own failed job is independent and safe to drop.
	// ErrCollectionNotFound: the collection was dropped. DropCollection is itself a
	// replicated DDL, so the peer marks its own import job Failed independently — there is
	// no peer left to release, and the broadcast can never succeed.
	// errRollbackImportNoVchannels: the job carries no vchannels (fixed at creation), so
	// the broadcast has no peer to address and can never succeed.
	return errors.Is(err, broadcaster.ErrNotPrimary) || errors.Is(err, merr.ErrCollectionNotFound) ||
		errors.Is(err, errRollbackImportNoVchannels)
}
