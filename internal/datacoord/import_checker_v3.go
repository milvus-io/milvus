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
	"math"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	importbinlog "github.com/milvus-io/milvus/internal/util/importutilv2/binlog"
	importcommon "github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/internal/util/importutilv2/reshardmem"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// importCheckerV3 is the ImportTaskV3 state machine. It only owns V3 jobs
// (ImportJob.version == ImportJobVersionV3) and never touches the legacy
// PreImportTask/ImportTaskV2 path, which remains fully owned by importChecker.
type importCheckerV3 struct {
	ctx        context.Context
	meta       *meta
	broker     broker.Broker
	alloc      allocator.Allocator
	importMeta ImportMeta
	ci         CompactionInspector
	handler    Handler
	cluster    session.Cluster

	hooks importCheckerHooks

	closeOnce sync.Once
	closeChan chan struct{}
}

func NewImportCheckerV3(ctx context.Context,
	meta *meta,
	broker broker.Broker,
	alloc allocator.Allocator,
	importMeta ImportMeta,
	ci CompactionInspector,
	handler Handler,
	cluster session.Cluster,
	hooks importCheckerHooks,
) ImportChecker {
	return &importCheckerV3{
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
func (c *importCheckerV3) Start() {
	mlog.Info(c.ctx, "start import checker v3")
	go c.runGCLoop()
	c.runStateMachineLoop()
}

func (c *importCheckerV3) runStateMachineLoop() {
	ticker := time.NewTicker(Params.DataCoordCfg.ImportCheckIntervalHigh.GetAsDuration(time.Second)) // 2s
	defer ticker.Stop()
	for {
		select {
		case <-c.closeChan:
			mlog.Info(c.ctx, "import checker v3 state-machine loop exited")
			return
		case <-ticker.C:
			jobs := c.importMeta.GetJobBy(c.ctx)
			for _, job := range jobs {
				if job.GetVersion() != datapb.ImportJobVersion_ImportJobVersionV3 {
					continue
				}
				if !funcutil.SliceSetEqual[string](job.GetVchannels(), job.GetReadyVchannels()) {
					// wait for all channels to send signals
					mlog.Info(c.ctx, "waiting for all channels to send signals",
						mlog.Strings("vchannels", job.GetVchannels()),
						mlog.Strings("readyVchannels", job.GetReadyVchannels()),
						mlog.FieldJobID(job.GetJobID()))
					continue
				}
				switch job.GetState() {
				case internalpb.ImportJobState_Pending:
					c.checkPendingJob(job)
				case internalpb.ImportJobState_Resharding:
					c.checkReshardJob(job)
				case internalpb.ImportJobState_Planning:
					c.checkPlanningJob(job)
				case internalpb.ImportJobState_Importing:
					c.checkImportingJob(job)
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

func (c *importCheckerV3) runGCLoop() {
	ticker := time.NewTicker(Params.DataCoordCfg.ImportCheckIntervalLow.GetAsDuration(time.Second)) // 2min
	defer ticker.Stop()
	for {
		select {
		case <-c.closeChan:
			mlog.Info(c.ctx, "import checker v3 gc loop exited")
			return
		case <-ticker.C:
			jobs := c.importMeta.GetJobBy(c.ctx)
			for _, job := range jobs {
				if job.GetVersion() != datapb.ImportJobVersion_ImportJobVersionV3 {
					continue
				}
				c.tryTimeoutJob(job)
				c.checkGC(job)
			}
			jobsByColl := lo.GroupBy(lo.Filter(jobs, func(job ImportJob, _ int) bool {
				return job.GetVersion() == datapb.ImportJobVersion_ImportJobVersionV3
			}), func(job ImportJob) int64 {
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

func (c *importCheckerV3) Close() {
	c.closeOnce.Do(func() {
		close(c.closeChan)
	})
}

func (c *importCheckerV3) LogJobStats(jobs []ImportJob) {
	stateNum := make(map[string]int)
	for _, version := range []datapb.ImportJobVersion{
		datapb.ImportJobVersion_ImportJobVersionV1,
		datapb.ImportJobVersion_ImportJobVersionV3,
	} {
		versionJobs := lo.Filter(jobs, func(job ImportJob, _ int) bool { return job.GetVersion() == version })
		byState := lo.GroupBy(versionJobs, func(job ImportJob) string { return job.GetState().String() })
		for state := range internalpb.ImportJobState_value {
			if state == internalpb.ImportJobState_None.String() {
				continue
			}
			num := len(byState[state])
			stateNum[state] += num
			metrics.ImportJobs.WithLabelValues(state, version.String()).Set(float64(num))
		}
	}
	mlog.Info(c.ctx, "import job stats", mlog.Any("stateNum", stateNum))
}

func (c *importCheckerV3) LogTaskStats() {
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
	tasks := c.importMeta.GetTaskBy(c.ctx, WithType(ReshardTaskType))
	logFunc(tasks, ReshardTaskType)
	tasks = c.importMeta.GetTaskBy(c.ctx, WithType(ImportTaskV3Type))
	logFunc(tasks, ImportTaskV3Type)
}

// checkPendingJob is the first V3 state. It restores existing ReshardTasks,
// fills missing source bins, and advances the job once every bin has a task.
func (c *importCheckerV3) checkPendingJob(job ImportJob) {
	log := mlog.With(mlog.FieldJobID(job.GetJobID()))
	if err := c.createReshardTasks(job); err != nil {
		log.Warn(c.ctx, "create import v3 reshard tasks failed", mlog.Err(err))
		if importcommon.IsTerminalImportV3Err(err) {
			if updateErr := c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(err.Error())); updateErr != nil {
				log.Warn(c.ctx, "failed to update job state to Failed", mlog.Err(updateErr))
			}
		}
	}
}

// checkReshardJob waits for every ReshardTask of the job to reach the
// catalog-verified Completed marker, then advances to Planning. It inspects
// only task states — no result manifests are read while waiting for slow
// tasks. Empty input jobs (zero total rows) are detected by planV3Job after
// reading the manifests, shortcutting to Uncommitted/Completed exactly like
// the legacy import path does for empty preimports.
func (c *importCheckerV3) checkReshardJob(job ImportJob) {
	log := mlog.With(mlog.FieldJobID(job.GetJobID()))
	completed, err := c.reshardTasksAllCompleted(job)
	if err != nil {
		log.Warn(c.ctx, "validate import v3 reshard task states failed", mlog.Err(err))
		if importcommon.IsTerminalImportV3Err(err) {
			if updateErr := c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(err.Error())); updateErr != nil {
				log.Warn(c.ctx, "failed to update job state to Failed", mlog.Err(updateErr))
			}
		}
		return
	}
	if !completed {
		return
	}
	reshardDuration := job.GetTR().RecordSpan()
	metrics.ImportJobLatency.WithLabelValues(metrics.ImportStageReshard, job.GetVersion().String()).Observe(float64(reshardDuration.Milliseconds()))
	if err := c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Planning)); err != nil {
		log.Warn(c.ctx, "failed to update job state to Planning", mlog.Err(err))
		return
	}
}

func (c *importCheckerV3) checkPlanningJob(job ImportJob) {
	log := mlog.With(mlog.FieldJobID(job.GetJobID()))
	if err := c.planV3Job(job); err != nil {
		log.Warn(c.ctx, "plan import v3 job failed", mlog.Err(err))
		// Disk-quota exhaustion fails the job immediately, matching the V2
		// checker (import_checker.go): the quota verdict is not input the user
		// can fix by resubmitting the same request, and a silent retry loop
		// would re-read every reshard manifest on each 2s tick while the job
		// reports a fixed 40% with an empty reason. Every other non-terminal
		// planning error retries on the next tick until the job timeout.
		if errors.Is(err, merr.ErrServiceQuotaExceeded) || importcommon.IsTerminalImportV3Err(err) {
			if updateErr := c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(err.Error())); updateErr != nil {
				log.Warn(c.ctx, "failed to update job state to Failed", mlog.Err(updateErr))
			}
		}
	}
}

func (c *importCheckerV3) checkImportingJob(job ImportJob) {
	log := mlog.With(mlog.FieldJobID(job.GetJobID()))
	currentSchemaVersion, err := validateImportV3Schema(c.meta, job.GetCollectionID(), job.GetSchema())
	if err != nil {
		log.Warn(c.ctx, "import v3 schema validation failed", mlog.Err(err))
		if importcommon.IsTerminalImportV3Err(err) {
			if updateErr := c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(err.Error())); updateErr != nil {
				log.Warn(c.ctx, "failed to update job state to Failed", mlog.Err(updateErr))
			}
		}
		return
	}
	tasks := c.importMeta.GetTaskByJob(c.ctx, job.GetJobID(), WithType(ImportTaskV3Type))
	if len(tasks) == 0 {
		return
	}
	for _, task := range tasks {
		if task.GetState() == datapb.ImportTaskStateV2_Failed {
			// Recover the crash window between the task marker and the job marker:
			// a Failed task can never become Completed, so without this branch the
			// job would wait forever (or until an external timeout).
			reason := fmt.Sprintf("import v3 task %d failed: %s", task.GetTaskID(), task.GetReason())
			if updateErr := c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(reason)); updateErr != nil {
				log.Warn(c.ctx, "failed to update job state to Failed after task failure", mlog.Err(updateErr))
			}
			return
		}
		if task.GetState() != datapb.ImportTaskStateV2_Completed {
			return
		}
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
		segment := c.meta.GetSegment(c.ctx, segmentID)
		if segment != nil && segment.GetNumOfRows() > 0 && segment.GetSchemaVersion() != currentSchemaVersion {
			segmentOps = append(segmentOps, updateImportV3SchemaVersion(segmentID, currentSchemaVersion))
		}
	}
	if len(segmentOps) > 0 {
		if err := c.meta.UpdateSegmentsInfo(c.ctx, segmentOps...); err != nil {
			log.Warn(c.ctx, "update import v3 segment schema version failed", mlog.Err(err))
			return
		}
	}
	if err := c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_IndexBuilding)); err != nil {
		log.Warn(c.ctx, "failed to update job state to IndexBuilding", mlog.Err(err))
		return
	}
	importDuration := job.GetTR().RecordSpan()
	metrics.ImportJobLatency.WithLabelValues(metrics.ImportStageImport, job.GetVersion().String()).Observe(float64(importDuration.Milliseconds()))
}

func (c *importCheckerV3) checkIndexBuildingJob(job ImportJob) {
	log := mlog.With(mlog.FieldJobID(job.GetJobID()))
	tasks := c.importMeta.GetTaskByJob(c.ctx, job.GetJobID(), WithType(ImportTaskV3Type))
	segmentIDs := make([]int64, 0)
	for _, task := range tasks {
		if segmentID := task.(*importTaskV3).task.Load().GetSegmentId(); segmentID != 0 {
			segment := c.meta.GetHealthySegment(c.ctx, segmentID)
			if segment == nil || segment.GetNumOfRows() == 0 {
				continue
			}
			segmentIDs = append(segmentIDs, segmentID)
		}
	}
	healthySegments := c.meta.GetSegments(segmentIDs, isSegmentHealthy)
	unindexed := c.meta.indexMeta.GetUnindexedSegments(job.GetCollectionID(), healthySegments)
	if Params.DataCoordCfg.WaitForIndex.GetAsBool() && len(unindexed) > 0 {
		for _, segmentID := range unindexed {
			select {
			case getBuildIndexChSingleton() <- segmentID:
			default:
			}
		}
		return
	}
	if err := c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Uncommitted)); err != nil {
		log.Warn(c.ctx, "failed to update job state to Uncommitted", mlog.Err(err))
		return
	}
	buildIndexDuration := job.GetTR().RecordSpan()
	metrics.ImportJobLatency.WithLabelValues(metrics.ImportStageBuildIndex, job.GetVersion().String()).Observe(float64(buildIndexDuration.Milliseconds()))
}

// checkUncommittedJob handles jobs in the Uncommitted state.
// If auto_commit=true, it triggers a commit via broadcastCommitImportMessage.
// If auto_commit=false, it waits for an explicit CommitImport RPC from the platform.
func (c *importCheckerV3) checkUncommittedJob(job ImportJob) {
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
func (c *importCheckerV3) checkCommittingJob(job ImportJob) {
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
	metrics.ImportJobLatency.WithLabelValues(metrics.TotalLabel, job.GetVersion().String()).Observe(float64(totalDuration.Milliseconds()))
	log.Info(c.ctx, "import job Committing done, all vchannels committed",
		mlog.Duration("jobTimeCost/total", totalDuration))
}

func (c *importCheckerV3) checkFailedJob(job ImportJob) {
	c.tryFailingTasks(job)
}

func (c *importCheckerV3) tryFailingTasks(job ImportJob) {
	tasks := c.importMeta.GetTaskByJob(c.ctx, job.GetJobID(), WithStates(datapb.ImportTaskStateV2_None, datapb.ImportTaskStateV2_Pending,
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

func (c *importCheckerV3) tryTimeoutJob(job ImportJob) {
	if job.GetState() == internalpb.ImportJobState_Failed ||
		job.GetState() == internalpb.ImportJobState_Completed ||
		job.GetState() == internalpb.ImportJobState_Committing {
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

func (c *importCheckerV3) checkCollection(collectionID int64, jobs []ImportJob) {
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
			return job.GetState() != internalpb.ImportJobState_Failed &&
				job.GetState() != internalpb.ImportJobState_Completed &&
				job.GetState() != internalpb.ImportJobState_Committing
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

func (c *importCheckerV3) checkGC(job ImportJob) {
	if job.GetState() != internalpb.ImportJobState_Completed &&
		job.GetState() != internalpb.ImportJobState_Failed {
		return
	}
	log := mlog.With(mlog.FieldJobID(job.GetJobID()))

	// GC phases are derived from durable facts on every tick; no separate GC
	// record exists. Quiesce makes progress as soon as the job is terminal and
	// keeps retrying version-aware Drops; task removal inside it waits for
	// cleanupTs. Delete only starts once cleanupTs has passed and every task
	// is unbound/removed.
	if !c.quiesceImportJob(job, log) {
		return
	}
	if !time.Now().After(tsoutil.PhysicalTime(job.GetCleanupTs())) {
		return
	}
	if !c.rollbackFailedReplicateImport(job, log) {
		return
	}
	c.deleteImportJob(job, log)
}

// quiesceImportJob drops still-bound V3 tasks and removes tasks that are no
// longer pinned to a node. Removal only starts once cleanupTs has passed so
// the task records keep backing the job's row accounting during the retention
// window. It reports true only when every task of the job is already unbound
// and removed from catalog.
func (c *importCheckerV3) quiesceImportJob(job ImportJob, log *mlog.Logger) bool {
	tasks := c.importMeta.GetTaskByJob(c.ctx, job.GetJobID())
	ready := true
	for _, task := range tasks {
		if task.GetNodeID() != NullNodeID {
			// The scheduler drops a terminal task once when it observes the state.
			// GC retries that best-effort version-aware Drop every tick, so a
			// transient RPC failure or a lost node cannot pin the job forever.
			if c.cluster != nil {
				switch t := task.(type) {
				case *reshardTask:
					t.DropTaskOnWorker(c.cluster)
				case *importTaskV3:
					t.DropTaskOnWorker(c.cluster)
				}
			}
			ready = false
			continue
		}
		if job.GetState() == internalpb.ImportJobState_Failed && task.GetType() == ImportTaskV3Type {
			segmentIDs := []int64(nil)
			if segmentID := task.(*importTaskV3).task.Load().GetSegmentId(); segmentID != 0 {
				segmentIDs = append(segmentIDs, segmentID)
			}
			if len(segmentIDs) > 0 {
				if err := c.meta.UpdateSegmentsInfo(c.ctx, dropImportV3Segments(segmentIDs)); err != nil {
					log.Warn(c.ctx, "drop import v3 segments during failed job GC", WrapTaskLog(task, mlog.Err(err))...)
					ready = false
					continue
				}
			}
		}
		// Task records back the job's row accounting (getImportRowsInfo sums
		// them), so they must survive the retention window like V2 GC does;
		// dropping their worker binding above is still safe to do early.
		if !time.Now().After(tsoutil.PhysicalTime(job.GetCleanupTs())) {
			ready = false
			continue
		}
		if err := c.importMeta.RemoveTask(c.ctx, task.GetTaskID()); err != nil {
			log.Warn(c.ctx, "remove task failed during GC", WrapTaskLog(task, mlog.Err(err))...)
			ready = false
			continue
		}
		log.Info(c.ctx, "task removed during GC quiesce", WrapTaskLog(task)...)
	}
	return ready
}

// rollbackFailedReplicateImport releases the peer cluster before source GC drops
// a failed 2PC import. It is called only on the deletion tick, so a job sitting
// inside the retention window does not re-broadcast RollbackImport every tick.
// A crash between a successful broadcast and RemoveJob repeats the broadcast at
// most once, which is the same idempotency boundary V2 GC already has.
func (c *importCheckerV3) rollbackFailedReplicateImport(job ImportJob, log *mlog.Logger) bool {
	if c.hooks.rollbackImport == nil || c.hooks.isReplicatingCluster == nil ||
		job.GetState() != internalpb.ImportJobState_Failed || job.GetAutoCommit() {
		return true
	}
	replicateCheckCtx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	replicating, err := c.hooks.isReplicatingCluster(replicateCheckCtx)
	cancel()
	switch {
	case err != nil:
		log.Warn(c.ctx, "cannot determine replication status before GC of failed import job, will retry", mlog.Err(err))
		return false
	case replicating:
		rollbackCtx, rollbackCancel := context.WithTimeout(c.ctx, 10*time.Second)
		err := c.hooks.rollbackImport(rollbackCtx, job)
		rollbackCancel()
		if err != nil && !isPermanentRollbackErr(err) {
			log.Warn(c.ctx, "failed to broadcast rollback before GC of failed replicate import job, will retry", mlog.Err(err))
			return false
		}
		log.Info(c.ctx, "proceeding with GC of failed replicate import job after rollback attempt")
	}
	return true
}

// deleteImportJob removes the job's temporary OSS prefix, any remaining task
// catalog entries, and the job itself. Every step is idempotent, so a crash in
// between simply resumes on the next GC tick.
func (c *importCheckerV3) deleteImportJob(job ImportJob, log *mlog.Logger) {
	prefix := path.Join(c.meta.chunkManager.RootPath(), metautil.BuildImportV3JobPath(job.GetJobID())) + "/"
	if err := c.meta.chunkManager.RemoveWithPrefix(c.ctx, prefix); err != nil {
		log.Warn(c.ctx, "remove import job temporary objects failed", mlog.Err(err))
		return
	}
	tasks := c.importMeta.GetTaskByJob(c.ctx, job.GetJobID())
	for _, task := range tasks {
		if err := c.importMeta.RemoveTask(c.ctx, task.GetTaskID()); err != nil {
			log.Warn(c.ctx, "remove task failed during GC delete", WrapTaskLog(task, mlog.Err(err))...)
			return
		}
	}
	if err := c.importMeta.RemoveJob(c.ctx, job.GetJobID()); err != nil {
		log.Warn(c.ctx, "remove import job failed", mlog.Err(err))
		return
	}
	log.Info(c.ctx, "import job removed")
}

// The V3 planner is deliberately small and deterministic. A restart keeps
// ready tasks, fills the missing logical tasks, and then advances the job
// state without depending on map iteration.

func calculateV3Slots(workingSet, memoryPerSlot int64) int64 {
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
// DataNode runtime) into slots. The charge is a CEILING, not a reservation:
// the DataNode spills resident data once it exceeds slot x memoryLimitPerSlot,
// and its dynamic free-memory checkpoint spills below that ceiling whenever
// the real process memory is tighter than the per-task budgets assumed. No
// churn/GC multiplier is applied on top; that headroom is still being
// measured.
func calculateReshardTaskSlot(mem reshardmem.Model, memoryPerSlot, buckets, bucketCap int64) int64 {
	return calculateV3Slots(mem.WorkingSet(buckets, bucketCap), memoryPerSlot)
}

func calculateV3ImportTaskSlot(readBuffer, writerBuffer, memoryPerSlot int64, fanIn int) int64 {
	workingSet := int64(fanIn)*2*readBuffer + 3*readBuffer + writerBuffer
	return calculateV3Slots(workingSet, memoryPerSlot)
}

// effectiveImportV3FanIn is the fan-in written into a per-segment task plan.
// A segment never opens more readers than it has fragments, so slot estimation
// charges only for the readers that can actually run instead of the global
// configured maximum.
func effectiveImportV3FanIn(configured, fragmentCount int) int {
	fanIn := configured
	if fragmentCount > 0 && fragmentCount < fanIn {
		fanIn = fragmentCount
	}
	// fragmentMergeFanIn is refreshable but only range-checked at startup, so
	// clamp the upper bound here too; MergeExecutor rejects fan-in > 1024.
	if fanIn > 1024 {
		fanIn = 1024
	}
	// MergeExecutor validates fan-in >= 2; a single-fragment segment still
	// runs the normal one-head merge path with that validation intact.
	if fanIn < 2 {
		fanIn = 2
	}
	return fanIn
}

func (c *importCheckerV3) createReshardTasks(job ImportJob) error {
	files := job.GetFiles()
	// Planning only validates the sort spec; dispatch re-derives it from the
	// frozen job schema.
	if _, err := getDefaultSortSpec(job.GetSchema()); err != nil {
		return err
	}
	covered, err := c.loadReshardSourceIDs(job)
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
		missing, err = c.groupReshardSources(job, missingFiles, importutilv2.IsBackup(job.GetOptions()))
		if err != nil {
			return err
		}
	}
	if len(missing) > 0 {
		start, _, err := c.alloc.AllocN(int64(len(missing)))
		if err != nil {
			return err
		}
		for i, sources := range missing {
			if err := c.createReshardTask(job, start+int64(i), sources); err != nil {
				return err
			}
		}
	}
	if err := c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Resharding)); err != nil {
		return err
	}
	pendingDuration := job.GetTR().RecordSpan()
	metrics.ImportJobLatency.WithLabelValues(metrics.ImportStagePending, job.GetVersion().String()).Observe(float64(pendingDuration.Milliseconds()))
	return nil
}

func (c *importCheckerV3) createReshardTask(job ImportJob, taskID int64, sources []reshardSource) error {
	sourceIDs := lo.Map(sources, func(s reshardSource, _ int) int64 { return s.file.GetId() })
	fragmentSize, err := importFragmentSizeBytes()
	if err != nil {
		return err
	}
	slot := calculateReshardTaskSlot(reshardmem.Model{
		ReadBuffer:     Params.DataNodeCfg.ImportBaseBufferSize.GetAsInt64(),
		FragmentTarget: fragmentSize,
	}, Params.DataCoordCfg.ImportMemoryLimitPerSlot.GetAsInt64(),
		int64(len(job.GetVchannels())*len(job.GetPartitionIDs())),
		Params.DataCoordCfg.ReshardResidentBucketCap.GetAsInt64(),
	)
	task := newReshardTask(&datapb.ReshardTask{JobId: job.GetJobID(), TaskId: taskID, CollectionId: job.GetCollectionID(), State: datapb.ImportTaskStateV2_Pending, RunId: 1, NodeId: NullNodeID, Slot: slot, SourceIds: sourceIDs}, c.importMeta, c.meta, c.alloc)
	return c.importMeta.AddTask(c.ctx, task)
}

func buildV3SourceFileSpec(job ImportJob, source reshardSource) (*datapb.SourceFileSpec, error) {
	var fileType datapb.ImportFileType
	var err error
	if importutilv2.IsBackup(job.GetOptions()) {
		fileType = datapb.ImportFileType_BackupBinlog
	} else {
		fileType, err = importutilv2.GetFileType(source.file)
		if err != nil {
			return nil, err
		}
	}
	spec := &datapb.SourceFileSpec{
		File:     proto.Clone(source.file).(*internalpb.ImportFile),
		FileType: fileType,
		Options:  &datapb.ReaderOptions{},
	}
	if fileType == datapb.ImportFileType_Csv {
		separator, err := importutilv2.GetCSVSep(job.GetOptions())
		if err != nil {
			return nil, err
		}
		nullKey, err := importutilv2.GetCSVNullKey(job.GetOptions())
		if err != nil {
			return nil, err
		}
		spec.Options.Separator = string(separator)
		spec.Options.NullKey = nullKey
	}
	if fileType == datapb.ImportFileType_BackupBinlog {
		startTS, endTS, err := importutilv2.ParseTimeRange(job.GetOptions())
		if err != nil {
			return nil, err
		}
		storageVersion, err := importutilv2.GetStorageVersion(job.GetOptions())
		if err != nil {
			return nil, err
		}
		spec.Options.StartTs = startTS
		spec.Options.EndTs = endTS
		spec.Options.StorageVersion = storageVersion
	}
	return spec, nil
}

// buildReshardTaskPlan re-derives the complete execution input of one
// ReshardTask at dispatch time from the frozen ImportJob, the task's source
// ownership and current configuration. No planning object is written to
// object storage for reshard tasks.
func buildReshardTaskPlan(job ImportJob, p *datapb.ReshardTask) (*datapb.ReshardTaskPlan, error) {
	sortSpec, err := getDefaultSortSpec(job.GetSchema())
	if err != nil {
		return nil, err
	}
	backup := importutilv2.IsBackup(job.GetOptions())
	jobFiles := lo.SliceToMap(job.GetFiles(), func(f *internalpb.ImportFile) (int64, *internalpb.ImportFile) {
		return f.GetId(), f
	})
	specs := make([]*datapb.SourceFileSpec, 0, len(p.GetSourceIds()))
	for _, fileID := range p.GetSourceIds() {
		file := jobFiles[fileID]
		if file == nil {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 reshard task %d references source %d outside the job", p.GetTaskId(), fileID)
		}
		spec, err := buildV3SourceFileSpec(job, reshardSource{file: file})
		if err != nil {
			return nil, err
		}
		specs = append(specs, spec)
	}
	fragmentSize, err := importFragmentSizeBytes()
	if err != nil {
		return nil, err
	}
	return &datapb.ReshardTaskPlan{
		CollectionId: job.GetCollectionID(),
		Schema:       job.GetSchema(),
		TempSchema:   buildImportV3TempSchema(job.GetSchema(), backup),
		Vchannels:    append([]string(nil), job.GetVchannels()...),
		Partitions:   append([]int64(nil), job.GetPartitionIDs()...),
		Sort:         sortSpec,
		FragmentSize: fragmentSize,
		Sources:      specs,
		Backup:       backup,
	}, nil
}

// importFragmentSizeBytes resolves the live fragment size config in bytes. The
// parameter is refreshable but only validated once at startup, so a hot
// refresh to an empty, non-numeric or non-positive value must fail the consumer
// loudly instead of reaching the DataNode as a zero fragment target (which
// would flush every non-empty bucket after every batch).
func importFragmentSizeBytes() (int64, error) {
	sizeInMB := Params.DataCoordCfg.ImportFragmentSize.GetAsInt64()
	if sizeInMB <= 0 {
		return 0, merr.WrapErrImportSysFailedMsg(
			"dataCoord.import.fragmentSizeInMB must be positive, got %q",
			Params.DataCoordCfg.ImportFragmentSize.GetValue())
	}
	return sizeInMB * 1024 * 1024, nil
}

func (c *importCheckerV3) loadReshardSourceIDs(job ImportJob) (map[int64]struct{}, error) {
	tasks := c.importMeta.GetTaskByJob(c.ctx, job.GetJobID(), WithType(ReshardTaskType))
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
		p := task.task.Load()
		if p.GetCollectionId() != job.GetCollectionID() {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 reshard task identity mismatch")
		}
		if len(p.GetSourceIds()) == 0 {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 reshard task has no source")
		}
		for _, fileID := range p.GetSourceIds() {
			if _, ok := jobFiles[fileID]; !ok {
				return nil, merr.WrapErrDataIntegrityMsg("import v3 reshard task contains source %d outside the job", fileID)
			}
			if _, ok := covered[fileID]; ok {
				return nil, merr.WrapErrDataIntegrityMsg("import v3 reshard task set contains duplicate source %d", fileID)
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

// groupReshardSources implements stable one-dimensional BFD. ImportFile is
// the atom: no path/file splitting, no backtracking, and an oversized file owns
// one bin. Equal-size files keep their job ordinal; equal-fit bins keep their
// creation ordinal. Only the grouped sources are returned; bin sizes are a
// transient packing detail.
//
// The bin target mirrors RegroupImportFiles: one task holds at most one
// segment's worth of data per (vchannel, partition) bucket, capped by
// MaxSizeInMBPerImportTask. Import V3 never handles L0, so the segment target
// comes straight from getExpectedSegmentSize.
func (c *importCheckerV3) groupReshardSources(job ImportJob, files []*internalpb.ImportFile, backup bool) ([][]reshardSource, error) {
	// Expand each file's object list first (serial; it is a per-file prefix
	// listing), then stat all objects across all files concurrently. The
	// checker goroutine serially processes every V3 job, so a serial per-object
	// cm.Size loop would delay state transitions of all other jobs for the
	// duration of one backup import's sizing (V2 sizes files in parallel on
	// DataNodes during PreImport).
	type sizedFile struct {
		file  *internalpb.ImportFile
		paths []string
		size  int64
	}
	expanded := make([]sizedFile, 0, len(files))
	for _, file := range files {
		paths := append([]string(nil), file.GetPaths()...)
		if backup {
			insertObjects, deltaObjects, err := importbinlog.ExpandObjects(c.ctx, c.meta.chunkManager, paths)
			if err != nil {
				return nil, err
			}
			paths = paths[:0]
			for _, fieldPaths := range insertObjects {
				paths = append(paths, fieldPaths...)
			}
			paths = append(paths, deltaObjects...)
		}
		expanded = append(expanded, sizedFile{file: file, paths: paths})
	}
	sizes := make([]int64, len(expanded))
	g, gctx := errgroup.WithContext(c.ctx)
	g.SetLimit(16)
	for i := range expanded {
		g.Go(func() error {
			size, err := storage.GetFilesSize(gctx, expanded[i].paths, c.meta.chunkManager)
			if err != nil {
				return merr.Wrapf(err, "estimate import v3 source file %d", expanded[i].file.GetId())
			}
			sizes[i] = size
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	sources := make([]reshardSource, 0, len(expanded))
	for i := range expanded {
		sources = append(sources, reshardSource{file: expanded[i].file, size: sizes[i]})
	}
	sort.SliceStable(sources, func(i, j int) bool {
		return sources[i].size > sources[j].size
	})
	segmentTarget := getExpectedSegmentSize(c.meta, job.GetCollectionID(), job.GetSchema())
	threshold := Params.DataCoordCfg.MaxSizeInMBPerImportTask.GetAsInt64() * 1024 * 1024
	target := min(segmentTarget*int64(len(job.GetVchannels()))*int64(len(job.GetPartitionIDs())), threshold)
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

func getDefaultSortSpec(schema *schemapb.CollectionSchema) (*datapb.SortSpec, error) {
	pk, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, merr.Wrap(err, "import v3 schema has no primary key")
	}
	toSpec := func(field *schemapb.FieldSchema) (*datapb.SortFieldSpec, error) {
		return &datapb.SortFieldSpec{FieldId: field.GetFieldID(), DataType: field.GetDataType()}, nil
	}
	spec := &datapb.SortSpec{}
	if schema.GetEnableNamespace() {
		partitionKey, err := typeutil.GetPartitionKeyFieldSchema(schema)
		if err != nil {
			return nil, merr.Wrap(err, "import v3 namespace schema has no partition key")
		}
		field, err := toSpec(partitionKey)
		if err != nil {
			return nil, err
		}
		spec.Fields = append(spec.Fields, field)
	}
	field, err := toSpec(pk)
	if err != nil {
		return nil, err
	}
	spec.Fields = append(spec.Fields, field)
	return spec, nil
}

// buildImportV3TempSchema describes the fields physically present in
// immutable fragments. Ordinary fragments carry user fields plus materialized
// PK/RowID and every function output column (Reshard computes the missing
// ones); timestamp is supplied by the import data timestamp at final merge.
// Backup fragments retain source timestamp and their source-provided function
// outputs, so they use the full system field schema.
func buildImportV3TempSchema(schema *schemapb.CollectionSchema, backup bool) *schemapb.CollectionSchema {
	cloned := proto.Clone(schema).(*schemapb.CollectionSchema)
	// Temporary fragments and intermediate merge runs store TEXT as raw UTF-8
	// strings, not as manifest binary LOB references. Map TEXT to VarChar in the
	// temporary schema so the ordinary storage sort/merge/writer paths can handle
	// it without a storage-layer special case. The synthetic max_length keeps
	// TEXT's full length budget rather than VarChar's smaller default.
	for _, field := range cloned.Fields {
		if field.GetDataType() == schemapb.DataType_Text {
			field.DataType = schemapb.DataType_VarChar
			field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{
				Key:   common.MaxLengthKey,
				Value: fmt.Sprintf("%d", Params.ProxyCfg.MaxTextLength.GetAsInt64()),
			})
		}
	}
	if backup {
		return typeutil.AppendSystemFields(cloned)
	}
	cloned.Fields = append(cloned.Fields, &schemapb.FieldSchema{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64})
	return cloned
}

type v3PlanningFragment struct {
	sourceID       int64
	channelIndex   int32
	partitionIndex int32
	seq            int64
	path           string
	rows           int64
	// bytes carries the fragment's normalized decoded bytes as reported by the
	// reshard manifest (see writeReshardFragment), the same metric the fragment
	// target uses for cutting. Planning packs dataCoord.segment.maxSize against
	// this metric.
	bytes int64
}

// reshardTasksAllCompleted reports whether every ReshardTask has reached the
// catalog-verified Completed marker. It inspects only task states and never
// reads result manifests — the Completed marker is gated on acceptResult
// validating the manifest, so this check needs no object-store access and the
// Resharding tail does no redundant reads while waiting for slow tasks.
func (c *importCheckerV3) reshardTasksAllCompleted(job ImportJob) (bool, error) {
	tasks := c.importMeta.GetTaskByJob(c.ctx, job.GetJobID(), WithType(ReshardTaskType))
	if len(tasks) == 0 {
		return false, nil
	}
	for _, generic := range tasks {
		task, ok := generic.(*reshardTask)
		if !ok {
			return false, merr.WrapErrDataIntegrityMsg("import v3 reshard result set contains an unexpected task type")
		}
		if task.GetState() == datapb.ImportTaskStateV2_Failed {
			// A failed task must terminate the job even when DataCoord crashed
			// between the task marker and the job marker. Without this branch the
			// job would wait for a Completed marker that can never arrive.
			return false, merr.WrapErrImportSysFailedMsg("import v3 reshard task %d failed: %s", task.GetTaskID(), task.GetReason())
		}
		if task.GetState() != datapb.ImportTaskStateV2_Completed {
			return false, nil
		}
	}
	return true, nil
}

func (c *importCheckerV3) cleanupPreparingV3ImportTasks(job ImportJob) error {
	tasks := c.importMeta.GetTaskByJob(c.ctx, job.GetJobID(), WithType(ImportTaskV3Type))
	for _, generic := range tasks {
		task, ok := generic.(*importTaskV3)
		if !ok {
			return merr.WrapErrDataIntegrityMsg("import v3 planning task set contains an unexpected task type")
		}
		if task.GetState() != datapb.ImportTaskStateV2_None {
			continue
		}
		if segmentID := task.task.Load().GetSegmentId(); segmentID != 0 {
			if err := c.meta.UpdateSegmentsInfo(c.ctx, dropImportV3Segments([]int64{segmentID})); err != nil {
				return err
			}
		}
		if err := c.importMeta.RemoveTask(c.ctx, task.GetTaskID()); err != nil {
			return err
		}
	}
	return nil
}

// loadExistingImportV3Fragments returns the fragment refs already owned by the
// job's persisted ImportTaskV3 records. Fragment ownership is the only
// planning fact kept on the task record; it is what Planning recovery needs
// to fill missing coverage without re-assigning fragments.
func (c *importCheckerV3) loadExistingImportV3Fragments(job ImportJob) ([]*datapb.FragmentRef, error) {
	tasks := c.importMeta.GetTaskByJob(c.ctx, job.GetJobID(), WithType(ImportTaskV3Type))
	existing := make([]*datapb.FragmentRef, 0)
	for _, generic := range tasks {
		task, ok := generic.(*importTaskV3)
		if !ok {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 planning task set contains an unexpected task type")
		}
		if task.GetState() == datapb.ImportTaskStateV2_None {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 task %d is still preparing", task.GetTaskID())
		}
		if task.GetState() == datapb.ImportTaskStateV2_Failed {
			return nil, merr.WrapErrImportSysFailedMsg("import v3 task %d failed: %s", task.GetTaskID(), task.GetReason())
		}
		p := task.task.Load()
		if p.GetSegmentId() == 0 {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 task %d has no segment", p.GetTaskId())
		}
		if p.GetCollectionId() != job.GetCollectionID() {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 task identity mismatch")
		}
		if p.GetVchannel() == "" || p.GetPartitionId() == 0 || len(p.GetFragments()) == 0 {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 task segment is incomplete")
		}
		existing = append(existing, p.GetFragments()...)
	}
	return existing, nil
}

func (c *importCheckerV3) planV3Job(job ImportJob) error {
	if _, err := validateImportV3Schema(c.meta, job.GetCollectionID(), job.GetSchema()); err != nil {
		return err
	}
	if err := validateImportV3StorageVersion(job.GetSchema()); err != nil {
		return err
	}
	if err := c.cleanupPreparingV3ImportTasks(job); err != nil {
		return err
	}
	existingFragments, err := c.loadExistingImportV3Fragments(job)
	if err != nil {
		return err
	}
	// Planning only validates the sort spec; dispatch re-derives it from the
	// frozen job schema.
	if _, err := getDefaultSortSpec(job.GetSchema()); err != nil {
		return err
	}
	reshards := c.importMeta.GetTaskByJob(c.ctx, job.GetJobID(), WithType(ReshardTaskType))
	if len(reshards) == 0 {
		return merr.WrapErrImportSysFailedMsg("import v3 planning has no completed reshard tasks")
	}
	if len(job.GetVchannels()) == 0 {
		return merr.WrapErrDataIntegrityMsg("import v3 job has no vchannels")
	}
	fragments := make([]v3PlanningFragment, 0)
	for _, generic := range reshards {
		t := generic.(*reshardTask)
		if t.GetState() != datapb.ImportTaskStateV2_Completed {
			return nil
		}
		p := t.task.Load()
		manifest, err := loadReshardResultManifest(c.ctx, c.meta.chunkManager, p.GetJobId(), p.GetTaskId(), p.GetRunId())
		if err != nil {
			return err
		}
		if err := validateReshardManifest(manifest); err != nil {
			return err
		}
		for _, f := range manifest.GetFragments() {
			fragments = append(fragments, v3PlanningFragment{
				sourceID: p.GetTaskId(), channelIndex: f.GetChannelIndex(), partitionIndex: f.GetPartitionIndex(),
				seq: f.GetSeq(), path: f.GetPath(), rows: f.GetRows(), bytes: f.GetLogicalBytes(),
			})
		}
	}
	sort.Slice(fragments, func(i, j int) bool {
		a, b := fragments[i], fragments[j]
		if a.channelIndex != b.channelIndex {
			return a.channelIndex < b.channelIndex
		}
		if a.partitionIndex != b.partitionIndex {
			return a.partitionIndex < b.partitionIndex
		}
		if a.sourceID != b.sourceID {
			return a.sourceID < b.sourceID
		}
		return a.seq < b.seq
	})
	targetSchema := typeutil.AppendSystemFields(job.GetSchema())
	// The segment target and the fragment bytes below must stay in the same
	// normalized decoded-bytes metric: changing one side's metric without the
	// other reopens a fragment/segment size mismatch.
	target := getExpectedSegmentSize(c.meta, job.GetCollectionID(), job.GetSchema())
	for _, f := range fragments {
		if f.channelIndex < 0 || int(f.channelIndex) >= len(job.GetVchannels()) || f.partitionIndex < 0 || int(f.partitionIndex) >= len(job.GetPartitionIDs()) {
			return merr.WrapErrDataIntegrityMsg("import v3 fragment bucket ordinal is out of range")
		}
	}
	totalFragmentBytes := lo.SumBy(fragments, func(f v3PlanningFragment) int64 { return f.bytes })
	totalRows := lo.SumBy(fragments, func(f v3PlanningFragment) int64 { return f.rows })
	// Empty input job (zero total rows): shortcut to Uncommitted/Completed
	// exactly like the legacy import path does for empty preimports. The
	// Reshard-stage latency was already recorded by checkReshardJob; only the
	// total latency remains. buildV3SegmentPlans always yields at least one
	// plan for non-empty fragments, so a non-empty job never lands here.
	if totalRows == 0 {
		if len(existingFragments) > 0 {
			return merr.WrapErrDataIntegrityMsg("import v3 planning has tasks but no segment plan")
		}
		state := internalpb.ImportJobState_Uncommitted
		if job.GetAutoCommit() {
			state = internalpb.ImportJobState_Completed
		}
		if err := c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(state)); err != nil {
			return err
		}
		if state == internalpb.ImportJobState_Completed {
			totalDuration := job.GetTR().ElapseSpan()
			metrics.ImportJobLatency.WithLabelValues(metrics.TotalLabel, job.GetVersion().String()).Observe(float64(totalDuration.Milliseconds()))
		}
		return nil
	}
	segmentPlans := buildV3SegmentPlans(fragments, job.GetVchannels(), job.GetPartitionIDs(), target)
	writerSpec, err := buildImportV3WriterSpec(targetSchema)
	if err != nil {
		return err
	}
	taskSpecs := segmentPlans
	// totalFragmentBytes is the normalized data volume (same metric as the
	// legacy CheckDiskQuota's TotalMemorySize); CheckDiskQuotaV3 scales it for
	// the fragment plus intermediate peak.
	requestedDiskSize, err := CheckDiskQuotaV3(c.ctx, job, c.meta, c.importMeta, totalFragmentBytes)
	if err != nil {
		return err
	}
	mlog.Info(c.ctx, "import v3 planning packed segment plans",
		mlog.FieldJobID(job.GetJobID()),
		mlog.Int("reshardTasks", len(reshards)),
		mlog.Int("fragments", len(fragments)),
		mlog.Int("segmentPlans", len(segmentPlans)),
		mlog.Int("existingTasks", len(existingFragments)),
		mlog.Int64("rows", totalRows),
		mlog.Int64("logicalBytes", totalFragmentBytes),
		mlog.Int64("segmentTargetBytes", target),
		mlog.Int64("requestedDiskSize", requestedDiskSize),
	)

	var missing []v3ImportTaskSpec
	if len(existingFragments) == 0 {
		missing = taskSpecs
	} else {
		missing, err = missingV3ImportTaskSpecs(fragments, existingFragments, job.GetVchannels(), job.GetPartitionIDs(), target)
		if err != nil {
			return err
		}
	}
	if len(missing) > 0 {
		taskStart, _, err := c.alloc.AllocN(int64(len(missing)))
		if err != nil {
			return err
		}
		segmentStart, _, err := c.alloc.AllocN(int64(len(missing)))
		if err != nil {
			return err
		}
		for i, spec := range missing {
			if err := c.createImportV3Task(job, writerSpec, taskStart+int64(i), segmentStart+int64(i), spec); err != nil {
				return err
			}
		}
	}
	if err := c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Importing), UpdateRequestedDiskSize(requestedDiskSize)); err != nil {
		return err
	}
	planningDuration := job.GetTR().RecordSpan()
	metrics.ImportJobLatency.WithLabelValues(metrics.ImportStagePlanning, job.GetVersion().String()).Observe(float64(planningDuration.Milliseconds()))
	return nil
}

type v3ImportTaskSpec struct {
	channel     string
	partitionID int64
	fragments   []*datapb.FragmentRef
	rows        int64
}

func v3FragmentRefKey(path string, rows int64) string {
	return fmt.Sprintf("%s/%d", path, rows)
}

// buildV3SegmentPlans packs the canonical fragment sequence into per-bucket
// task specs. The caller must have validated bucket ordinals against the job's
// vchannel/partition arrays. Both the target and f.bytes are normalized
// decoded logical bytes (see v3PlanningFragment.bytes): with the default 1GiB
// segment target and 128MiB fragment target a plan holds on the order of 8
// fragments, inside fragmentMergeFanIn (16), so segments usually merge in
// one round; the exact count is schema-dependent.
func buildV3SegmentPlans(fragments []v3PlanningFragment, vchannels []string, partitionIDs []int64, target int64) []v3ImportTaskSpec {
	specs := make([]v3ImportTaskSpec, 0)
	var current *v3ImportTaskSpec
	var currentBytes int64
	for _, f := range fragments {
		vchannel := vchannels[f.channelIndex]
		partitionID := partitionIDs[f.partitionIndex]
		if current == nil || current.channel != vchannel || current.partitionID != partitionID || (currentBytes > 0 && currentBytes+f.bytes > target) {
			specs = append(specs, v3ImportTaskSpec{channel: vchannel, partitionID: partitionID})
			current = &specs[len(specs)-1]
			currentBytes = 0
		}
		current.fragments = append(current.fragments, &datapb.FragmentRef{Path: f.path, RowCount: f.rows})
		current.rows += f.rows
		currentBytes += f.bytes
	}
	return specs
}

// missingV3ImportTaskSpecs returns the per-segment plans that must still be
// created during Planning recovery. Existing tasks are kept as-is; only
// fragment coverage matters, and newly created tasks are packed from the
// canonical fragment sequence after skipping fragments already owned by a
// ready task.
func missingV3ImportTaskSpecs(
	fragments []v3PlanningFragment,
	existing []*datapb.FragmentRef,
	vchannels []string,
	partitionIDs []int64,
	target int64,
) ([]v3ImportTaskSpec, error) {
	fullKeys := make(map[string]struct{}, len(fragments))
	for _, f := range fragments {
		fullKeys[v3FragmentRefKey(f.path, f.rows)] = struct{}{}
	}
	covered := make(map[string]struct{}, len(fragments))
	for _, ref := range existing {
		key := v3FragmentRefKey(ref.GetPath(), ref.GetRowCount())
		if _, ok := fullKeys[key]; !ok {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 task references fragment outside current planning input: %s", ref.GetPath())
		}
		if _, ok := covered[key]; ok {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 tasks contain duplicate fragment: %s", ref.GetPath())
		}
		covered[key] = struct{}{}
	}
	missingFragments := make([]v3PlanningFragment, 0, len(fragments)-len(covered))
	for _, f := range fragments {
		if _, ok := covered[v3FragmentRefKey(f.path, f.rows)]; !ok {
			missingFragments = append(missingFragments, f)
		}
	}
	if len(missingFragments) == 0 {
		return nil, nil
	}
	return buildV3SegmentPlans(missingFragments, vchannels, partitionIDs, target), nil
}

// importV3LogRangeWidth returns the number of log IDs one imported segment
// consumes under the given writer spec: one manifest plus one log per BM25
// field, and for StorageV2 one packed binlog per column group. Every (re)alloc
// of a task's LogRange must size it from the same spec the dispatch plan
// carries, so a hot-flipped storage version cannot exhaust a range that was
// sized for the old version.
func importV3LogRangeWidth(writerSpec *datapb.WriterSpec) (int64, error) {
	perSegment := int64(1 + len(writerSpec.GetBm25Fields()))
	if writerSpec.GetStorageVersion() == storage.StorageV2 {
		perSegment += int64(len(writerSpec.GetGroups()))
	}
	if perSegment <= 0 || perSegment > math.MaxUint32 {
		return 0, merr.WrapErrImportSysFailedMsg("import v3 log id budget is invalid")
	}
	return perSegment, nil
}

func (c *importCheckerV3) createImportV3Task(
	job ImportJob,
	writerSpec *datapb.WriterSpec,
	taskID, segmentID int64,
	spec v3ImportTaskSpec,
) error {
	if job.GetDataTs() == 0 {
		// DataTs drives the formal writer's row timestamps. Allocating log IDs
		// here and discarding them only advances the allocator; dispatch would
		// still carry zero and corrupt TTL/commit fencing, so fail the planning
		// step instead.
		return merr.WrapErrImportSysFailedMsg("import v3 job %d has no data timestamp", job.GetJobID())
	}
	perSegment, err := importV3LogRangeWidth(writerSpec)
	if err != nil {
		return err
	}
	logBegin, logEnd, err := c.alloc.AllocN(perSegment)
	if err != nil {
		return err
	}
	fanIn := effectiveImportV3FanIn(Params.DataCoordCfg.FragmentMergeFanIn.GetAsInt(), len(spec.fragments))
	slot := calculateV3ImportTaskSlot(
		Params.DataNodeCfg.ImportBaseBufferSize.GetAsInt64(),
		packed.DefaultWriteBufferSize,
		Params.DataCoordCfg.ImportMemoryLimitPerSlot.GetAsInt64(),
		fanIn,
	)
	task := newImportTaskV3(&datapb.ImportTaskV3{
		JobId: job.GetJobID(), TaskId: taskID, CollectionId: job.GetCollectionID(),
		State: datapb.ImportTaskStateV2_None, RunId: 1, NodeId: NullNodeID,
		SegmentId: segmentID, LogRange: &datapb.IDRange{Begin: logBegin, End: logEnd},
		Slot: slot, Rows: spec.rows,
		Fragments: append([]*datapb.FragmentRef(nil), spec.fragments...),
		Vchannel:  spec.channel, PartitionId: spec.partitionID,
	}, c.importMeta, c.meta, c.alloc)
	if err := c.importMeta.AddTask(c.ctx, task); err != nil {
		return err
	}
	if _, err := addImportSegment(c.ctx, c.meta, segmentID, job.GetJobID(), taskID, job.GetCollectionID(), spec.partitionID, spec.channel, datapb.SegmentLevel_L1, writerSpec.GetStorageVersion(), int32(writerSpec.GetSchemaVersion())); err != nil {
		return err
	}
	return c.importMeta.UpdateTask(c.ctx, taskID, UpdateState(datapb.ImportTaskStateV2_Pending))
}

// validateImportV3StorageVersion fails a TEXT collection when the current
// storage version cannot write TEXT LOB columns.
func validateImportV3StorageVersion(schema *schemapb.CollectionSchema) error {
	if importStorageVersion(false) == storage.StorageV3 {
		return nil
	}
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		if field.GetDataType() == schemapb.DataType_Text {
			return merr.WrapErrImportSysFailedMsg("import v3 TEXT columns require common.storage.useLoonFFI")
		}
	}
	return nil
}

// buildImportV3WriterSpec derives the writer parameters from the frozen target
// schema and current configuration.
func buildImportV3WriterSpec(targetSchema *schemapb.CollectionSchema) (*datapb.WriterSpec, error) {
	ttl, err := common.GetCollectionTTL(targetSchema.GetProperties())
	if err != nil {
		return nil, err
	}
	writerFormat := Params.DataNodeCfg.StorageFormat.GetValue()
	writerSpec := &datapb.WriterSpec{
		StorageVersion: importStorageVersion(false),
		SchemaVersion:  int64(targetSchema.GetVersion()),
		Format:         writerFormat,
		V2:             &datapb.V2PackedIOConfig{BufferSize: packed.DefaultWriteBufferSize, MultipartSize: packed.DefaultMultiPartUploadSize},
		TtlNanos:       ttl.Nanoseconds(),
		BloomType:      Params.CommonCfg.BloomFilterType.GetValue(),
		BloomFpp:       Params.CommonCfg.MaxBloomFalsePositive.GetAsFloat(),
	}
	for _, function := range targetSchema.GetFunctions() {
		if function.GetType() == schemapb.FunctionType_BM25 {
			writerSpec.Bm25Fields = append(writerSpec.Bm25Fields, function.GetOutputFieldIds()...)
		}
	}
	sort.Slice(writerSpec.Bm25Fields, func(i, j int) bool { return writerSpec.Bm25Fields[i] < writerSpec.Bm25Fields[j] })
	for _, field := range typeutil.GetAllFieldSchemas(targetSchema) {
		if field.GetDataType() == schemapb.DataType_Text {
			writerSpec.Text = append(writerSpec.Text, &datapb.TextColumnWriteSpec{
				FieldId: field.GetFieldID(), InlineLimit: Params.DataNodeCfg.TextInlineThreshold.GetAsInt64(),
				LobLimit: Params.DataNodeCfg.TextMaxLobFileBytes.GetAsInt64(), FlushLimit: Params.DataNodeCfg.TextFlushThresholdBytes.GetAsInt64(),
			})
		}
	}
	columnGroups := storagecommon.SplitColumns(typeutil.GetAllFieldSchemas(targetSchema), map[int64]storagecommon.ColumnStats{}, storagecommon.DefaultPolicies()...)
	columnGroups = storagecommon.FillColumnGroupFormats(columnGroups, writerFormat)
	writerSpec.Groups = make([]*datapb.ColumnGroupSpec, 0, len(columnGroups))
	for _, group := range columnGroups {
		writerSpec.Groups = append(writerSpec.Groups, &datapb.ColumnGroupSpec{Id: group.GroupID, Fields: append([]int64(nil), group.Fields...), Format: group.Format})
	}
	return writerSpec, nil
}

// buildImportV3TaskPlan re-derives the complete execution input of one
// ImportTaskV3 at dispatch time from durable records and current
// configuration. Only fragment ownership lives on the task record; no
// planning object is written to object storage for import tasks.
func buildImportV3TaskPlan(job ImportJob, p *datapb.ImportTaskV3) (*datapb.ImportTaskPlan, error) {
	if p.GetRows() <= 0 {
		return nil, merr.WrapErrImportSysFailedMsg("import v3 task %d has no planned rows", p.GetTaskId())
	}
	sortSpec, err := getDefaultSortSpec(job.GetSchema())
	if err != nil {
		return nil, err
	}
	backup := importutilv2.IsBackup(job.GetOptions())
	targetSchema := typeutil.AppendSystemFields(job.GetSchema())
	if err := validateImportV3StorageVersion(targetSchema); err != nil {
		return nil, err
	}
	writerSpec, err := buildImportV3WriterSpec(targetSchema)
	if err != nil {
		return nil, err
	}
	writerSpec.PkCapacity = max(p.GetRows(), 1)
	return &datapb.ImportTaskPlan{
		Sort:         sortSpec,
		Vchannel:     p.GetVchannel(),
		PartitionId:  p.GetPartitionId(),
		Fragments:    p.GetFragments(),
		Rows:         p.GetRows(),
		FanIn:        int32(effectiveImportV3FanIn(Params.DataCoordCfg.FragmentMergeFanIn.GetAsInt(), len(p.GetFragments()))),
		Writer:       writerSpec,
		Schema:       targetSchema,
		TempSchema:   buildImportV3TempSchema(job.GetSchema(), backup),
		DataTs:       job.GetDataTs(),
		CollectionId: job.GetCollectionID(),
		Backup:       backup,
	}, nil
}
