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

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

type ImportChecker interface {
	Start()
	Close()
}

type importChecker struct {
	ctx                 context.Context
	meta                *meta
	broker              broker.Broker
	alloc               allocator.Allocator
	importMeta          ImportMeta
	ci                  CompactionInspector
	handler             Handler
	l0CompactionTrigger TriggerManager

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
	l0CompactionTrigger TriggerManager,
) ImportChecker {
	return &importChecker{
		ctx:                 ctx,
		meta:                meta,
		broker:              broker,
		alloc:               alloc,
		importMeta:          importMeta,
		ci:                  ci,
		l0CompactionTrigger: l0CompactionTrigger,
		handler:             handler,
		closeChan:           make(chan struct{}),
	}
}

func (c *importChecker) Start() {
	log.Info("start import checker")
	var (
		ticker1 = time.NewTicker(Params.DataCoordCfg.ImportCheckIntervalHigh.GetAsDuration(time.Second)) // 2s
		ticker2 = time.NewTicker(Params.DataCoordCfg.ImportCheckIntervalLow.GetAsDuration(time.Second))  // 2min
	)
	defer ticker1.Stop()
	defer ticker2.Stop()
	for {
		select {
		case <-c.closeChan:
			log.Info("import checker exited")
			return
		case <-ticker1.C:
			jobs := c.importMeta.GetJobBy(c.ctx)
			for _, job := range jobs {
				if !funcutil.SliceSetEqual[string](job.GetVchannels(), job.GetReadyVchannels()) {
					// wait for all channels to send signals
					log.Info("waiting for all channels to send signals",
						zap.Strings("vchannels", job.GetVchannels()),
						zap.Strings("readyVchannels", job.GetReadyVchannels()),
						zap.Int64("jobID", job.GetJobID()))
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
				case internalpb.ImportJobState_Failed:
					c.checkFailedJob(job)
				}
			}
		case <-ticker2.C:
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
	log.Info("import job stats", zap.Any("stateNum", stateNum))
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
		log.Info("import task stats", zap.String("type", taskType.String()),
			zap.Int("pending", pending), zap.Int("inProgress", inProgress),
			zap.Int("completed", completed), zap.Int("failed", failed))
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

func (c *importChecker) getLackFilesForImports(job ImportJob) []*datapb.ImportFileStats {
	preimports := c.importMeta.GetTaskBy(c.ctx, WithType(PreImportTaskType), WithJob(job.GetJobID()))
	lacks := make(map[int64]*datapb.ImportFileStats, 0)
	for _, t := range preimports {
		for _, stat := range t.GetFileStats() {
			lacks[stat.GetImportFile().GetId()] = stat
		}
	}
	exists := c.importMeta.GetTaskBy(c.ctx, WithType(ImportTaskType), WithJob(job.GetJobID()))
	for _, task := range exists {
		for _, file := range task.GetFileStats() {
			delete(lacks, file.GetImportFile().GetId())
		}
	}
	return lo.Values(lacks)
}

func (c *importChecker) checkPendingJob(job ImportJob) {
	log := log.With(zap.Int64("jobID", job.GetJobID()))
	lacks := c.getLackFilesForPreImports(job)
	if len(lacks) == 0 {
		return
	}
	fileGroups := lo.Chunk(lacks, Params.DataCoordCfg.FilesPerPreImportTask.GetAsInt())

	newTasks, err := NewPreImportTasks(fileGroups, job, c.alloc, c.importMeta)
	if err != nil {
		log.Warn("new preimport tasks failed", zap.Error(err))
		return
	}
	for _, t := range newTasks {
		err = c.importMeta.AddTask(c.ctx, t)
		if err != nil {
			log.Warn("add preimport task failed", WrapTaskLog(t, zap.Error(err))...)
			return
		}
		log.Info("add new preimport task", WrapTaskLog(t, zap.Any("fileStats", t.GetFileStats()))...)
	}

	err = c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_PreImporting))
	if err != nil {
		log.Warn("failed to update job state to PreImporting", zap.Error(err))
		return
	}
	pendingDuration := job.GetTR().RecordSpan()
	metrics.ImportJobLatency.WithLabelValues(metrics.ImportStagePending).Observe(float64(pendingDuration.Milliseconds()))
	log.Info("import job start to execute", zap.Duration("jobTimeCost/pending", pendingDuration))
}

func (c *importChecker) checkPreImportingJob(job ImportJob) {
	log := log.With(zap.Int64("jobID", job.GetJobID()))

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
			log.Warn("failed to update job state to Importing", zap.Error(err))
			return
		}
		preImportDuration := job.GetTR().RecordSpan()
		metrics.ImportJobLatency.WithLabelValues(metrics.ImportStagePreImport).Observe(float64(preImportDuration.Milliseconds()))
		log.Info("import job preimport done", zap.String("state", state.String()), zap.Duration("jobTimeCost/preimport", preImportDuration))
	}

	if totalRows == 0 {
		log.Info("no data to import, skip the subsequent stages, just update job state to Completed")
		updateJobState(internalpb.ImportJobState_Completed)
		return
	}

	lacks := c.getLackFilesForImports(job)
	if len(lacks) == 0 {
		return
	}

	requestSize, err := CheckDiskQuota(c.ctx, job, c.meta, c.importMeta)
	if err != nil {
		log.Warn("import failed, disk quota exceeded", zap.Error(err))
		updateJobState(internalpb.ImportJobState_Failed, UpdateJobReason(err.Error()))
		return
	}

	segmentMaxSize := GetSegmentMaxSize(job, c.meta)
	groups := RegroupImportFiles(job, lacks, segmentMaxSize)
	newTasks, err := NewImportTasks(groups, job, c.alloc, c.meta, c.importMeta, segmentMaxSize)
	if err != nil {
		log.Warn("new import tasks failed", zap.Error(err))
		return
	}
	for _, t := range newTasks {
		err = c.importMeta.AddTask(c.ctx, t)
		if err != nil {
			log.Warn("add new import task failed", WrapTaskLog(t, zap.Error(err))...)
			updateJobState(internalpb.ImportJobState_Failed, UpdateJobReason(err.Error()))
			return
		}
		log.Info("add new import task", WrapTaskLog(t, zap.Any("fileStats", t.GetFileStats()))...)
	}

	updateJobState(internalpb.ImportJobState_Importing, UpdateRequestedDiskSize(requestSize))
}

func (c *importChecker) checkImportingJob(job ImportJob) {
	log := log.With(zap.Int64("jobID", job.GetJobID()))
	tasks := c.importMeta.GetTaskBy(c.ctx, WithType(ImportTaskType), WithJob(job.GetJobID()), WithRequestSource())
	for _, t := range tasks {
		if t.GetState() != datapb.ImportTaskStateV2_Completed {
			return
		}
	}
	err := c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Sorting))
	if err != nil {
		log.Warn("failed to update job state to Stats", zap.Error(err))
		return
	}
	importDuration := job.GetTR().RecordSpan()
	metrics.ImportJobLatency.WithLabelValues(metrics.ImportStageImport).Observe(float64(importDuration.Milliseconds()))
	log.Info("import job import done", zap.Duration("jobTimeCost/import", importDuration))
}

func (c *importChecker) checkSortingJob(job ImportJob) {
	log := log.With(zap.Int64("jobID", job.GetJobID()))
	updateJobState := func(state internalpb.ImportJobState, reason string) {
		err := c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(state), UpdateJobReason(reason))
		if err != nil {
			log.Warn("failed to update job state", zap.Error(err))
			return
		}
		statsDuration := job.GetTR().RecordSpan()
		metrics.ImportJobLatency.WithLabelValues(metrics.ImportStageStats).Observe(float64(statsDuration.Milliseconds()))
		log.Info("import job stats done", zap.String("state", state.String()), zap.Duration("jobTimeCost/stats", statsDuration))
	}

	// Skip stats stage if not enable stats or is l0 import.
	if !enableSortCompaction() ||
		importutilv2.IsL0Import(job.GetOptions()) {
		updateJobState(internalpb.ImportJobState_IndexBuilding, "")
		return
	}

	// Check and trigger stats tasks.
	var (
		taskCnt = 0
		doneCnt = 0
	)
	tasks := c.importMeta.GetTaskBy(c.ctx, WithType(ImportTaskType), WithJob(job.GetJobID()))
	for _, task := range tasks {
		originSegmentIDs := task.(*importTask).GetSegmentIDs()
		sortSegmentIDs := task.(*importTask).GetSortedSegmentIDs()
		taskCnt += len(originSegmentIDs)
		for i, originSegmentID := range originSegmentIDs {
			taskLogFields := WrapTaskLog(task, zap.Int64("origin", originSegmentID), zap.Int64("target", sortSegmentIDs[i]))
			originSegment := c.meta.GetHealthySegment(c.ctx, originSegmentID)
			targetSegment := c.meta.GetHealthySegment(c.ctx, sortSegmentIDs[i])
			if originSegment == nil {
				// import zero num rows segment
				doneCnt++
				continue
			}
			if targetSegment == nil {
				compactionTask, err := createSortCompactionTask(c.ctx, originSegment, sortSegmentIDs[i], c.meta, c.handler, c.alloc)
				if err != nil {
					log.Warn("create sort compaction task failed", zap.Int64("segmentID", originSegmentID), zap.Error(err))
					continue
				}
				if compactionTask == nil {
					log.Info("maybe it no need to create sort compaction task", zap.Int64("segmentID", originSegmentID))
					doneCnt++
					continue
				}
				log.Info("create sort compaction task success", taskLogFields...)
				err = c.ci.enqueueCompaction(compactionTask)
				if err != nil {
					log.Warn("sort compaction task enqueue failed", zap.Error(err))
					continue
				}
				continue
			}
			doneCnt++
		}
	}

	// All segments are stats-ed. Update job state to `IndexBuilding`.
	if taskCnt == doneCnt {
		updateJobState(internalpb.ImportJobState_IndexBuilding, "")
	}
}

func (c *importChecker) checkIndexBuildingJob(job ImportJob) {
	log := log.With(zap.Int64("jobID", job.GetJobID()))
	tasks := c.importMeta.GetTaskBy(c.ctx, WithType(ImportTaskType), WithJob(job.GetJobID()))
	originSegmentIDs := lo.FlatMap(tasks, func(t ImportTask, _ int) []int64 {
		return t.(*importTask).GetSegmentIDs()
	})
	statsSegmentIDs := lo.FlatMap(tasks, func(t ImportTask, _ int) []int64 {
		return t.(*importTask).GetSortedSegmentIDs()
	})

	targetSegmentIDs := statsSegmentIDs
	if !enableSortCompaction() {
		targetSegmentIDs = originSegmentIDs
	}

	healthySegments := c.meta.GetSegments(targetSegmentIDs, func(segment *SegmentInfo) bool {
		return isSegmentHealthy(segment)
	})
	unindexed := c.meta.indexMeta.GetUnindexedSegments(job.GetCollectionID(), healthySegments)
	if Params.DataCoordCfg.WaitForIndex.GetAsBool() && len(unindexed) > 0 && !importutilv2.IsL0Import(job.GetOptions()) {
		for _, segmentID := range unindexed {
			select {
			case getBuildIndexChSingleton() <- segmentID: // accelerate index building:
			default:
			}
		}
		log.Debug("waiting for import segments building index...", zap.Int64s("unindexed", unindexed))
		return
	}
	buildIndexDuration := job.GetTR().RecordSpan()
	metrics.ImportJobLatency.WithLabelValues(metrics.ImportStageBuildIndex).Observe(float64(buildIndexDuration.Milliseconds()))
	log.Info("import job build index done", zap.Duration("jobTimeCost/buildIndex", buildIndexDuration))

	// wait l0 segment import and block l0 compaction
	log.Info("start to pause l0 segment compacting", zap.Int64("jobID", job.GetJobID()))
	<-c.l0CompactionTrigger.GetPauseCompactionChan(job.GetJobID(), job.GetCollectionID())
	log.Info("l0 segment compacting paused", zap.Int64("jobID", job.GetJobID()))

	if c.waitL0ImortTaskDone(job) {
		return
	}
	waitL0ImportDuration := job.GetTR().RecordSpan()
	metrics.ImportJobLatency.WithLabelValues(metrics.ImportStageWaitL0Import).Observe(float64(buildIndexDuration.Milliseconds()))
	log.Info("import job l0 import done", zap.Duration("jobTimeCost/l0Import", waitL0ImportDuration))

	if c.updateSegmentState(originSegmentIDs, statsSegmentIDs) {
		return
	}
	// all finished, update import job state to `Completed`.
	completeTime := time.Now().Format("2006-01-02T15:04:05Z07:00")
	err := c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Completed), UpdateJobCompleteTime(completeTime))
	if err != nil {
		log.Warn("failed to update job state to Completed", zap.Error(err))
		return
	}
	totalDuration := job.GetTR().ElapseSpan()
	metrics.ImportJobLatency.WithLabelValues(metrics.TotalLabel).Observe(float64(totalDuration.Milliseconds()))
	<-c.l0CompactionTrigger.GetResumeCompactionChan(job.GetJobID(), job.GetCollectionID())

	LogResultSegmentsInfo(job.GetJobID(), c.meta, targetSegmentIDs)
	log.Info("import job all completed", zap.Duration("jobTimeCost/total", totalDuration))
}

func (c *importChecker) waitL0ImortTaskDone(job ImportJob) bool {
	// wait all lo import tasks to be completed
	l0ImportTasks := c.importMeta.GetTaskBy(c.ctx, WithType(ImportTaskType), WithJob(job.GetJobID()), WithL0CompactionSource())
	for _, t := range l0ImportTasks {
		if t.GetState() != datapb.ImportTaskStateV2_Completed {
			log.Info("waiting for l0 import task...",
				zap.Int64s("taskIDs", lo.Map(l0ImportTasks, func(t ImportTask, _ int) int64 {
					return t.GetTaskID()
				})))
			return true
		}
	}
	return false
}

func (c *importChecker) updateSegmentState(originSegmentIDs, statsSegmentIDs []int64) bool {
	// Here, all segment indexes have been successfully built, try unset isImporting flag for all segments.
	isImportingSegments := lo.Filter(append(originSegmentIDs, statsSegmentIDs...), func(segmentID int64, _ int) bool {
		segment := c.meta.GetSegment(c.ctx, segmentID)
		if segment == nil {
			log.Warn("cannot find segment", zap.Int64("segmentID", segmentID))
			return false
		}
		return segment.GetIsImporting()
	})
	channels, err := c.meta.GetSegmentsChannels(isImportingSegments)
	if err != nil {
		log.Warn("get segments channels failed", zap.Error(err))
		return true
	}
	for _, segmentID := range isImportingSegments {
		channelCP := c.meta.GetChannelCheckpoint(channels[segmentID])
		if channelCP == nil {
			log.Warn("nil channel checkpoint")
			return true
		}
		op1 := UpdateStartPosition([]*datapb.SegmentStartPosition{{StartPosition: channelCP, SegmentID: segmentID}})
		op2 := UpdateDmlPosition(segmentID, channelCP)
		op3 := UpdateIsImporting(segmentID, false)
		err = c.meta.UpdateSegmentsInfo(c.ctx, op1, op2, op3)
		if err != nil {
			log.Warn("update import segment failed", zap.Error(err))
			return true
		}
	}
	return false
}

func (c *importChecker) checkFailedJob(job ImportJob) {
	c.tryFailingTasks(job)
}

func (c *importChecker) tryFailingTasks(job ImportJob) {
	tasks := c.importMeta.GetTaskBy(c.ctx, WithJob(job.GetJobID()), WithStates(datapb.ImportTaskStateV2_Pending,
		datapb.ImportTaskStateV2_InProgress, datapb.ImportTaskStateV2_Completed))
	if len(tasks) == 0 {
		return
	}
	log.Warn("Import job has failed, all tasks with the same jobID will be marked as failed",
		zap.Int64("jobID", job.GetJobID()), zap.String("reason", job.GetReason()))
	for _, task := range tasks {
		err := c.importMeta.UpdateTask(c.ctx, task.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed),
			UpdateReason(job.GetReason()))
		if err != nil {
			log.Warn("failed to update import task state to failed", WrapTaskLog(task, zap.Error(err))...)
			continue
		}
	}
}

func (c *importChecker) tryTimeoutJob(job ImportJob) {
	timeoutTime := tsoutil.PhysicalTime(job.GetTimeoutTs())
	if time.Now().After(timeoutTime) {
		log.Warn("Import timeout, expired the specified time limit",
			zap.Int64("jobID", job.GetJobID()), zap.Time("timeoutTime", timeoutTime))
		err := c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed),
			UpdateJobReason("import timeout"))
		if err != nil {
			log.Warn("failed to update job state to Failed", zap.Int64("jobID", job.GetJobID()), zap.Error(err))
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
		log.Warn("verify existence of collection failed", zap.Int64("collection", collectionID), zap.Error(err))
		return
	}
	if !has {
		jobs = lo.Filter(jobs, func(job ImportJob, _ int) bool {
			return job.GetState() != internalpb.ImportJobState_Failed
		})
		for _, job := range jobs {
			err = c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed),
				UpdateJobReason(fmt.Sprintf("collection %d dropped", collectionID)))
			if err != nil {
				log.Warn("failed to update job state to Failed", zap.Int64("jobID", job.GetJobID()), zap.Error(err))
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
		log := log.With(zap.Int64("jobID", job.GetJobID()))
		GCRetention := Params.DataCoordCfg.ImportTaskRetention.GetAsDuration(time.Second)
		log.Info("job has reached the GC retention",
			zap.Time("cleanupTime", cleanupTime), zap.Duration("GCRetention", GCRetention))
		tasks := c.importMeta.GetTaskBy(c.ctx, WithJob(job.GetJobID()))
		shouldRemoveJob := true
		for _, task := range tasks {
			if job.GetState() == internalpb.ImportJobState_Failed && task.GetType() == ImportTaskType {
				if len(task.(*importTask).GetSegmentIDs()) != 0 || len(task.(*importTask).GetSortedSegmentIDs()) != 0 {
					shouldRemoveJob = false
					continue
				}
			}
			if task.GetNodeID() != NullNodeID {
				shouldRemoveJob = false
				continue
			}
			err := c.importMeta.RemoveTask(c.ctx, task.GetTaskID())
			if err != nil {
				log.Warn("remove task failed during GC", WrapTaskLog(task, zap.Error(err))...)
				shouldRemoveJob = false
				continue
			}
			log.Info("reached GC retention, task removed", WrapTaskLog(task)...)
		}
		if !shouldRemoveJob {
			return
		}
		err := c.importMeta.RemoveJob(c.ctx, job.GetJobID())
		if err != nil {
			log.Warn("remove import job failed", zap.Error(err))
			return
		}
		log.Info("import job removed")
	}
}
