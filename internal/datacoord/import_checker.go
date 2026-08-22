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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	// commitImport broadcasts a CommitImport WAL message. Required in production; a nil
	// value is a programming error only when reached on the auto_commit=true path.
	commitImport func(ctx context.Context, job ImportJob) error
	// rollbackImport broadcasts a RollbackImport WAL message. nil disables GC self-heal.
	rollbackImport func(ctx context.Context, job ImportJob) error
	// isReplicatingCluster reports whether this cluster is currently replicating. A
	// non-nil error means the status is indeterminate (e.g. a transient balancer error
	// during shutdown) and the caller must not make an irreversible GC decision. nil hook
	// is treated as "not replicating" (GC self-heal disabled).
	isReplicatingCluster func(ctx context.Context) (bool, error)
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
		completed := len(byState[datapb.ImportTaskStateV2_Completed])
		failed := len(byState[datapb.ImportTaskStateV2_Failed])
		mlog.Info(c.ctx, "import task stats", mlog.String("type", taskType.String()),
			mlog.Int("pending", pending), mlog.Int("inProgress", inProgress),
			mlog.Int("completed", completed), mlog.Int("failed", failed))
		metrics.ImportTasks.WithLabelValues(taskType.String(), datapb.ImportTaskStateV2_Pending.String()).Set(float64(pending))
		metrics.ImportTasks.WithLabelValues(taskType.String(), datapb.ImportTaskStateV2_InProgress.String()).Set(float64(inProgress))
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
	exists := c.importMeta.GetTaskBy(c.ctx, WithType(PreImportTaskType), WithJob(job.GetJobID()))
	for _, task := range exists {
		for _, file := range task.GetFileStats() {
			delete(lacks, file.GetImportFile().GetId())
		}
	}
	return lo.Values(lacks)
}

// validateImportTaskSet verifies the publication boundary between PreImporting
// and Importing. Import tasks are persisted one by one, followed by the job
// state, so a restart can observe a complete task set while the job is still
// PreImporting. File IDs alone are not sufficient to identify a complete plan:
// duplicate, foreign, or stale file stats could otherwise hide a missing file.
//
// The returned files are precisely the preimport stats not covered by an
// existing import task. An empty result is safe to use as the commit marker for
// the already-persisted task plan.
func validateImportTaskSet(job ImportJob, preimports, imports []ImportTask) ([]*datapb.ImportFileStats, error) {
	jobFiles := make(map[int64]*internalpb.ImportFile, len(job.GetFiles()))
	for _, file := range job.GetFiles() {
		if file == nil {
			return nil, merr.WrapErrImportSysFailedMsg("invalid import task plan: job %d contains a nil file", job.GetJobID())
		}
		if _, ok := jobFiles[file.GetId()]; ok {
			return nil, merr.WrapErrImportSysFailedMsg(
				"invalid import task plan: job %d contains duplicate file %d", job.GetJobID(), file.GetId())
		}
		jobFiles[file.GetId()] = file
	}

	expected := make(map[int64]*datapb.ImportFileStats, len(jobFiles))
	for _, task := range preimports {
		if task == nil {
			return nil, merr.WrapErrImportSysFailedMsg("invalid import task plan: job %d contains a nil preimport task", job.GetJobID())
		}
		if task.GetCollectionID() != job.GetCollectionID() {
			return nil, merr.WrapErrImportSysFailedMsg(
				"invalid import task plan: preimport task %d belongs to collection %d, expected %d",
				task.GetTaskID(), task.GetCollectionID(), job.GetCollectionID())
		}
		for _, stat := range task.GetFileStats() {
			if stat == nil || stat.GetImportFile() == nil {
				return nil, merr.WrapErrImportSysFailedMsg(
					"invalid import task plan: preimport task %d contains nil file stats", task.GetTaskID())
			}
			fileID := stat.GetImportFile().GetId()
			jobFile, ok := jobFiles[fileID]
			if !ok || !proto.Equal(jobFile, stat.GetImportFile()) {
				return nil, merr.WrapErrImportSysFailedMsg(
					"invalid import task plan: preimport task %d contains unknown or stale file %d",
					task.GetTaskID(), fileID)
			}
			if _, ok := expected[fileID]; ok {
				return nil, merr.WrapErrImportSysFailedMsg(
					"invalid import task plan: file %d appears in multiple preimport results", fileID)
			}
			expected[fileID] = stat
		}
	}
	if len(expected) != len(jobFiles) {
		return nil, merr.WrapErrImportSysFailedMsg(
			"invalid import task plan: preimport results cover %d of %d files", len(expected), len(jobFiles))
	}

	covered := make(map[int64]struct{}, len(expected))
	for _, task := range imports {
		if task == nil {
			return nil, merr.WrapErrImportSysFailedMsg("invalid import task plan: job %d contains a nil import task", job.GetJobID())
		}
		if task.GetCollectionID() != job.GetCollectionID() {
			return nil, merr.WrapErrImportSysFailedMsg(
				"invalid import task plan: import task %d belongs to collection %d, expected %d",
				task.GetTaskID(), task.GetCollectionID(), job.GetCollectionID())
		}
		if len(task.GetFileStats()) == 0 {
			return nil, merr.WrapErrImportSysFailedMsg(
				"invalid import task plan: import task %d contains no files", task.GetTaskID())
		}
		for _, stat := range task.GetFileStats() {
			if stat == nil || stat.GetImportFile() == nil {
				return nil, merr.WrapErrImportSysFailedMsg(
					"invalid import task plan: import task %d contains nil file stats", task.GetTaskID())
			}
			fileID := stat.GetImportFile().GetId()
			expectedStat, ok := expected[fileID]
			if !ok || !proto.Equal(expectedStat, stat) {
				return nil, merr.WrapErrImportSysFailedMsg(
					"invalid import task plan: import task %d contains unknown or stale file stats for file %d",
					task.GetTaskID(), fileID)
			}
			if _, ok := covered[fileID]; ok {
				return nil, merr.WrapErrImportSysFailedMsg(
					"invalid import task plan: file %d appears in multiple import tasks", fileID)
			}
			covered[fileID] = struct{}{}
		}
	}

	lacks := make([]*datapb.ImportFileStats, 0, len(expected)-len(covered))
	for fileID, stat := range expected {
		if _, ok := covered[fileID]; !ok {
			lacks = append(lacks, stat)
		}
	}
	return lacks, nil
}

func (c *importChecker) checkPendingJob(job ImportJob) {
	log := mlog.With(mlog.FieldJobID(job.GetJobID()))
	lacks := c.getLackFilesForPreImports(job)
	if len(lacks) == 0 {
		return
	}
	fileGroups := lo.Chunk(lacks, Params.DataCoordCfg.FilesPerPreImportTask.GetAsInt())

	newTasks, err := NewPreImportTasks(fileGroups, job, c.alloc, c.importMeta)
	if err != nil {
		log.Warn(c.ctx, "new preimport tasks failed", mlog.Err(err))
		return
	}
	for _, t := range newTasks {
		err = c.importMeta.AddTask(c.ctx, t)
		if err != nil {
			log.Warn(c.ctx, "add preimport task failed", WrapTaskLog(t, mlog.Err(err))...)
			return
		}
		log.Info(c.ctx, "add new preimport task", WrapTaskLog(t, mlog.Any("fileStats", t.GetFileStats()))...)
	}

	err = c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_PreImporting))
	if err != nil {
		log.Warn(c.ctx, "failed to update job state to PreImporting", mlog.Err(err))
		return
	}
	pendingDuration := job.GetTR().RecordSpan()
	metrics.ImportJobLatency.WithLabelValues(metrics.ImportStagePending).Observe(float64(pendingDuration.Milliseconds()))
	log.Info(c.ctx, "import job start to execute", mlog.Duration("jobTimeCost/pending", pendingDuration))
}

func (c *importChecker) checkPreImportingJob(job ImportJob) {
	log := mlog.With(mlog.FieldJobID(job.GetJobID()))

	preimports := c.importMeta.GetTaskBy(c.ctx, WithType(PreImportTaskType), WithJob(job.GetJobID()))
	totalRows := int64(0)
	for _, t := range preimports {
		if t.GetState() != datapb.ImportTaskStateV2_Completed {
			// Preimport tasks are not fully completed, thus generating imports should not be triggered.
			return
		}
		totalRows += lo.SumBy(t.GetFileStats(), func(stat *datapb.ImportFileStats) int64 {
			return stat.GetTotalRows()
		})
	}

	updateJobState := func(state internalpb.ImportJobState, actions ...UpdateJobAction) {
		actions = append(actions, UpdateJobState(state))
		err := c.importMeta.UpdateJob(c.ctx, job.GetJobID(), actions...)
		if err != nil {
			log.Warn(c.ctx, "failed to update job state to Importing", mlog.Err(err))
			return
		}
		preImportDuration := job.GetTR().RecordSpan()
		metrics.ImportJobLatency.WithLabelValues(metrics.ImportStagePreImport).Observe(float64(preImportDuration.Milliseconds()))
		log.Info(c.ctx, "import job preimport done", mlog.String("state", state.String()), mlog.Duration("jobTimeCost/preimport", preImportDuration))
	}

	if totalRows == 0 {
		if job.GetAutoCommit() {
			// auto-commit: no data to import, skip Uncommitted directly to Completed
			log.Info(c.ctx, "no data to import, auto_commit=true, transitioning directly to Completed")
			updateJobState(internalpb.ImportJobState_Completed)
		} else {
			// replication cluster: surface Uncommitted so platform can observe and commit
			log.Info(c.ctx, "no data to import, auto_commit=false, transitioning to Uncommitted")
			updateJobState(internalpb.ImportJobState_Uncommitted)
		}
		return
	}

	// A previous process may have persisted only part of this job's import
	// tasks. Reject corrupt task metadata before creating tasks for the
	// remaining files; the sort plan itself comes from the durable job options
	// and needs no inheritance.
	existingTasks := c.importMeta.GetTaskBy(c.ctx, WithType(ImportTaskType), WithJob(job.GetJobID()))
	if _, err := importSortPlannedForJob(job, existingTasks); err != nil {
		log.Warn(c.ctx, "invalid existing import sort plan", mlog.Err(err))
		updateJobState(internalpb.ImportJobState_Failed, UpdateJobReason(err.Error()))
		return
	}

	lacks, err := validateImportTaskSet(job, preimports, existingTasks)
	if err != nil {
		log.Warn(c.ctx, "invalid existing import task plan", mlog.Err(err))
		updateJobState(internalpb.ImportJobState_Failed, UpdateJobReason(err.Error()))
		return
	}

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
	newTasks, err := NewImportTasks(groups, job, c.alloc, c.meta, c.importMeta, segmentMaxSize)
	if err != nil {
		log.Warn(c.ctx, "new import tasks failed", mlog.Err(err))
		return
	}
	for _, t := range newTasks {
		err = c.importMeta.AddTask(c.ctx, t)
		if err != nil {
			log.Warn(c.ctx, "add new import task failed", WrapTaskLog(t, mlog.Err(err))...)
			updateJobState(internalpb.ImportJobState_Failed, UpdateJobReason(err.Error()))
			return
		}
		log.Info(c.ctx, "add new import task", WrapTaskLog(t, mlog.Any("fileStats", t.GetFileStats()))...)
	}

	updateJobState(internalpb.ImportJobState_Importing, UpdateRequestedDiskSize(requestSize))
}

func (c *importChecker) checkImportingJob(job ImportJob) {
	log := mlog.With(mlog.FieldJobID(job.GetJobID()))
	tasks := c.importMeta.GetTaskBy(c.ctx, WithType(ImportTaskType), WithJob(job.GetJobID()), WithRequestSource())
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

	// Whether this job sorts comes from its durable job options -- every
	// normal import sorts, L0 never does -- not from a live switch. The sort
	// stage allocates its own output per origin and the checker discovers it
	// through the origin's compactionTo edge.
	tasks := c.importMeta.GetTaskBy(c.ctx, WithType(ImportTaskType), WithJob(job.GetJobID()))
	sortPlanned, err := importSortPlannedForJob(job, tasks)
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
		taskCnt = 0
		doneCnt = 0
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
				if isExplicitZeroRowSortSkip(job, c.meta.GetSegment(c.ctx, originSegmentID)) {
					doneCnt++
				} else {
					logger.Warn(c.ctx, "sort origin and its output are both unavailable")
				}
				continue
			}
			// if not compacting, trigger sort compaction task
			isCompacting := c.meta.IsSegmentCompacting(originSegmentID)
			if !isCompacting {
				compactionTask, err := createSortCompactionTask(c.ctx, task, originSegment, c.meta, c.handler, c.alloc)
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

// isExplicitZeroRowSortSkip recognizes the durable marker written by
// createSortCompactionTask when an imported origin received no rows. The
// origin is dropped without any sorted output being produced.
func isExplicitZeroRowSortSkip(job ImportJob, origin *SegmentInfo) bool {
	return job != nil && origin != nil &&
		origin.GetState() == commonpb.SegmentState_Dropped &&
		origin.GetNumOfRows() == 0 && origin.GetIsImporting() &&
		origin.GetCollectionID() == job.GetCollectionID() &&
		(len(job.GetPartitionIDs()) == 0 || lo.Contains(job.GetPartitionIDs(), origin.GetPartitionID())) &&
		(len(job.GetVchannels()) == 0 || lo.Contains(job.GetVchannels(), origin.GetInsertChannel()))
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
				if isExplicitZeroRowSortSkip(job, origin) {
					zeroRowSkipCount++
					continue
				}
				// A job planned without sort by an older binary (dataCoord.enableCompaction
				// off at planning time) has healthy, visible, importing origins
				// and no sorted output: the origin is the final imported
				// segment. Under this PR a sort-planned origin is always
				// published invisible, so visibility distinguishes the legacy
				// shape from a sort job whose output vanished -- which remains
				// durable plan corruption.
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
				if output.GetState() == commonpb.SegmentState_Dropped {
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
		if sortPlanned && originCount > 0 && zeroRowSkipCount == originCount {
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
		if !(segment.GetIsSorted() || segment.GetIsSortedByNamespace()) || !lo.Contains(segment.GetCompactionFrom(), originSegmentID) {
			return merr.WrapErrImportSysFailedMsg(
				"invalid import target plan: sorted segment %d does not derive from origin segment %d",
				segmentID, originSegmentID)
		}
	}
	return nil
}

func (c *importChecker) checkIndexBuildingJob(job ImportJob) {
	log := mlog.With(mlog.FieldJobID(job.GetJobID()))
	tasks := c.importMeta.GetTaskBy(c.ctx, WithType(ImportTaskType), WithJob(job.GetJobID()))
	sortPlanned, err := importSortPlannedForJob(job, tasks)
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

	// 2PC: hand off to Uncommitted regardless of auto_commit. Segment visibility
	// (is_importing=false) is cleared only by HandleCommitVchannel after the WAL
	// commit fence is processed per vchannel; auto_commit=true jobs are then
	// driven through the commit broadcast by checkUncommittedJob.
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
// If auto_commit=true, it triggers a commit via broadcastCommitImportMessage.
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
	// HandleCommitVchannel is idempotent on committed_vchannels.
	if c.hooks.commitImport == nil {
		log.Error(c.ctx, "commit hook is nil but auto_commit=true; this is a programming error")
		return
	}
	if err := c.hooks.commitImport(c.ctx, job); err != nil {
		log.Warn(c.ctx, "auto-commit broadcast failed, will retry on next tick", mlog.Err(err))
	}
}

// checkCommittingJob handles jobs in the Committing state.
// Once all vchannels have acknowledged the commit fence, the job transitions to Completed.
func (c *importChecker) checkCommittingJob(job ImportJob) {
	log := mlog.With(mlog.FieldJobID(job.GetJobID()))
	// When Vchannels is empty, len == len is trivially true. This handles the degenerate
	// case of a zero-channel import (e.g., empty collection); proceed to Completed immediately.
	if len(job.GetCommittedVchannels()) < len(job.GetVchannels()) {
		return // still waiting for remaining vchannels
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

func (c *importChecker) checkFailedJob(job ImportJob) {
	c.tryFailingTasks(job)
}

func (c *importChecker) tryFailingTasks(job ImportJob) {
	tasks := c.importMeta.GetTaskBy(c.ctx, WithJob(job.GetJobID()), WithStates(datapb.ImportTaskStateV2_Pending,
		datapb.ImportTaskStateV2_InProgress, datapb.ImportTaskStateV2_Completed, datapb.ImportTaskStateV2_Retry))
	if len(tasks) == 0 {
		return
	}
	mlog.Warn(c.ctx, "Import job has failed, all tasks with the same jobID will be marked as failed",
		mlog.FieldJobID(job.GetJobID()), mlog.String("reason", job.GetReason()))
	for _, task := range tasks {
		err := c.importMeta.UpdateTask(c.ctx, task.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed),
			UpdateReason(job.GetReason()))
		if err != nil {
			mlog.Warn(c.ctx, "failed to update import task state to failed", WrapTaskLog(task, mlog.Err(err))...)
			continue
		}
	}
}

func (c *importChecker) tryTimeoutJob(job ImportJob) {
	if job.GetState() == internalpb.ImportJobState_Failed ||
		job.GetState() == internalpb.ImportJobState_Completed {
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
		tasks := c.importMeta.GetTaskBy(c.ctx, WithJob(job.GetJobID()))
		shouldRemoveJob := true
		for _, task := range tasks {
			if job.GetState() == internalpb.ImportJobState_Failed && task.GetType() == ImportTaskType {
				if len(task.(*importTask).GetSegmentIDs()) != 0 {
					shouldRemoveJob = false
					continue
				}
			}
			if task.GetNodeID() != NullNodeID {
				// The task still names a worker, so its DataNode may still be
				// holding it -- importv2's taskManager.Sweep is deliberately a
				// no-op, making this drop the only reclamation there is. The
				// scheduler sends exactly one drop when it releases a task and
				// nothing retried a failed one, so the record sat here forever,
				// blocking its own removal and its job's. Retry it: DropImportTask
				// releases the assignment on success and on ErrNodeNotFound, and
				// the next GC round removes the task once it does.
				if dropErr := DropImportTask(task, c.cluster, c.importMeta); dropErr != nil {
					log.Warn(c.ctx, "failed to drop import task on its worker during GC, will retry",
						WrapTaskLog(task, mlog.Err(dropErr))...)
				}
				shouldRemoveJob = false
				continue
			}
			err := c.importMeta.RemoveTask(c.ctx, task.GetTaskID())
			if err != nil {
				log.Warn(c.ctx, "remove task failed during GC", WrapTaskLog(task, mlog.Err(err))...)
				shouldRemoveJob = false
				continue
			}
			log.Info(c.ctx, "reached GC retention, task removed", WrapTaskLog(task)...)
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
		if c.hooks.rollbackImport != nil && c.hooks.isReplicatingCluster != nil &&
			job.GetState() == internalpb.ImportJobState_Failed && !job.GetAutoCommit() {
			// The check reaches the streaming balancer future, which blocks until the
			// balancer is registered — under the server-lifetime c.ctx that would park
			// the GC loop during the window before streamingcoord registers
			// it (e.g. a restart recovering a job already past retention). Bound it like
			// checkCollection does; a timeout is just another indeterminate status.
			replicateCheckCtx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
			replicating, err := c.hooks.isReplicatingCluster(replicateCheckCtx)
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
