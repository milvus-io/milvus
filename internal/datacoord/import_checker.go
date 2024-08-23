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
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type ImportChecker interface {
	Start()
	Close()
}

type importChecker struct {
	meta    *meta
	broker  broker.Broker
	cluster Cluster
	alloc   allocator.Allocator
	sm      Manager
	imeta   ImportMeta

	closeOnce sync.Once
	closeChan chan struct{}
}

func NewImportChecker(meta *meta,
	broker broker.Broker,
	cluster Cluster,
	alloc allocator.Allocator,
	sm Manager,
	imeta ImportMeta,
) ImportChecker {
	return &importChecker{
		meta:      meta,
		broker:    broker,
		cluster:   cluster,
		alloc:     alloc,
		sm:        sm,
		imeta:     imeta,
		closeChan: make(chan struct{}),
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
			jobs := c.imeta.GetJobBy()
			for _, job := range jobs {
				switch job.GetState() {
				case internalpb.ImportJobState_Pending:
					c.checkPendingJob(job)
				case internalpb.ImportJobState_PreImporting:
					c.checkPreImportingJob(job)
				case internalpb.ImportJobState_Importing:
					c.checkImportingJob(job)
				case internalpb.ImportJobState_Failed:
					c.tryFailingTasks(job)
				}
			}
		case <-ticker2.C:
			jobs := c.imeta.GetJobBy()
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
			c.LogStats()
		}
	}
}

func (c *importChecker) Close() {
	c.closeOnce.Do(func() {
		close(c.closeChan)
	})
}

func (c *importChecker) LogStats() {
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
	tasks := c.imeta.GetTaskBy(WithType(PreImportTaskType))
	logFunc(tasks, PreImportTaskType)
	tasks = c.imeta.GetTaskBy(WithType(ImportTaskType))
	logFunc(tasks, ImportTaskType)
}

func (c *importChecker) getLackFilesForPreImports(job ImportJob) []*internalpb.ImportFile {
	lacks := lo.KeyBy(job.GetFiles(), func(file *internalpb.ImportFile) int64 {
		return file.GetId()
	})
	exists := c.imeta.GetTaskBy(WithType(PreImportTaskType), WithJob(job.GetJobID()))
	for _, task := range exists {
		for _, file := range task.GetFileStats() {
			delete(lacks, file.GetImportFile().GetId())
		}
	}
	return lo.Values(lacks)
}

func (c *importChecker) getLackFilesForImports(job ImportJob) []*datapb.ImportFileStats {
	preimports := c.imeta.GetTaskBy(WithType(PreImportTaskType), WithJob(job.GetJobID()))
	lacks := make(map[int64]*datapb.ImportFileStats, 0)
	for _, t := range preimports {
		if t.GetState() != datapb.ImportTaskStateV2_Completed {
			// Preimport tasks are not fully completed, thus generating imports should not be triggered.
			return nil
		}
		for _, stat := range t.GetFileStats() {
			lacks[stat.GetImportFile().GetId()] = stat
		}
	}
	exists := c.imeta.GetTaskBy(WithType(ImportTaskType), WithJob(job.GetJobID()))
	for _, task := range exists {
		for _, file := range task.GetFileStats() {
			delete(lacks, file.GetImportFile().GetId())
		}
	}
	return lo.Values(lacks)
}

func (c *importChecker) checkPendingJob(job ImportJob) {
	lacks := c.getLackFilesForPreImports(job)
	if len(lacks) == 0 {
		return
	}
	fileGroups := lo.Chunk(lacks, Params.DataCoordCfg.FilesPerPreImportTask.GetAsInt())

	newTasks, err := NewPreImportTasks(fileGroups, job, c.alloc)
	if err != nil {
		log.Warn("new preimport tasks failed", zap.Error(err))
		return
	}
	for _, t := range newTasks {
		err = c.imeta.AddTask(t)
		if err != nil {
			log.Warn("add preimport task failed", WrapTaskLog(t, zap.Error(err))...)
			return
		}
		log.Info("add new preimport task", WrapTaskLog(t)...)
	}
	err = c.imeta.UpdateJob(job.GetJobID(), UpdateJobState(internalpb.ImportJobState_PreImporting))
	if err != nil {
		log.Warn("failed to update job state to PreImporting", zap.Int64("jobID", job.GetJobID()), zap.Error(err))
	}
}

func (c *importChecker) checkPreImportingJob(job ImportJob) {
	lacks := c.getLackFilesForImports(job)
	if len(lacks) == 0 {
		return
	}

	requestSize, err := CheckDiskQuota(job, c.meta, c.imeta)
	if err != nil {
		log.Warn("import failed, disk quota exceeded", zap.Int64("jobID", job.GetJobID()), zap.Error(err))
		err = c.imeta.UpdateJob(job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(err.Error()))
		if err != nil {
			log.Warn("failed to update job state to Failed", zap.Int64("jobID", job.GetJobID()), zap.Error(err))
		}
		return
	}

	allDiskIndex := c.meta.indexMeta.AreAllDiskIndex(job.GetCollectionID(), job.GetSchema())
	groups := RegroupImportFiles(job, lacks, allDiskIndex)
	newTasks, err := NewImportTasks(groups, job, c.sm, c.alloc)
	if err != nil {
		log.Warn("new import tasks failed", zap.Error(err))
		return
	}
	for _, t := range newTasks {
		err = c.imeta.AddTask(t)
		if err != nil {
			log.Warn("add new import task failed", WrapTaskLog(t, zap.Error(err))...)
			return
		}
		log.Info("add new import task", WrapTaskLog(t)...)
	}
	err = c.imeta.UpdateJob(job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Importing), UpdateRequestedDiskSize(requestSize))
	if err != nil {
		log.Warn("failed to update job state to Importing", zap.Int64("jobID", job.GetJobID()), zap.Error(err))
	}
}

func (c *importChecker) checkImportingJob(job ImportJob) {
	log := log.With(zap.Int64("jobID", job.GetJobID()),
		zap.Int64("collectionID", job.GetCollectionID()))
	tasks := c.imeta.GetTaskBy(WithType(ImportTaskType), WithJob(job.GetJobID()))
	for _, t := range tasks {
		if t.GetState() != datapb.ImportTaskStateV2_Completed {
			return
		}
	}

	segmentIDs := lo.FlatMap(tasks, func(t ImportTask, _ int) []int64 {
		return t.(*importTask).GetSegmentIDs()
	})

	// Verify completion of index building for imported segments.
	unindexed := c.meta.indexMeta.GetUnindexedSegments(job.GetCollectionID(), segmentIDs)
	if Params.DataCoordCfg.WaitForIndex.GetAsBool() && len(unindexed) > 0 && !importutilv2.IsL0Import(job.GetOptions()) {
		log.Debug("waiting for import segments building index...", zap.Int64s("unindexed", unindexed))
		return
	}

	unfinished := lo.Filter(segmentIDs, func(segmentID int64, _ int) bool {
		segment := c.meta.GetSegment(segmentID)
		if segment == nil {
			log.Warn("cannot find segment, may be compacted", zap.Int64("segmentID", segmentID))
			return false
		}
		return segment.GetIsImporting()
	})

	channels, err := c.meta.GetSegmentsChannels(unfinished)
	if err != nil {
		log.Warn("get segments channels failed", zap.Error(err))
		return
	}
	for _, segmentID := range unfinished {
		channelCP := c.meta.GetChannelCheckpoint(channels[segmentID])
		if channelCP == nil {
			log.Warn("nil channel checkpoint")
			return
		}
		op1 := UpdateStartPosition([]*datapb.SegmentStartPosition{{StartPosition: channelCP, SegmentID: segmentID}})
		op2 := UpdateDmlPosition(segmentID, channelCP)
		op3 := UpdateIsImporting(segmentID, false)
		err = c.meta.UpdateSegmentsInfo(op1, op2, op3)
		if err != nil {
			log.Warn("update import segment failed", zap.Error(err))
			return
		}
	}

	completeTime := time.Now().Format("2006-01-02T15:04:05Z07:00")
	err = c.imeta.UpdateJob(job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Completed), UpdateJobCompleteTime(completeTime))
	if err != nil {
		log.Warn("failed to update job state to Completed", zap.Error(err))
		return
	}
	log.Info("import job completed")
}

func (c *importChecker) tryFailingTasks(job ImportJob) {
	tasks := c.imeta.GetTaskBy(WithJob(job.GetJobID()), WithStates(datapb.ImportTaskStateV2_Pending,
		datapb.ImportTaskStateV2_InProgress, datapb.ImportTaskStateV2_Completed))
	if len(tasks) == 0 {
		return
	}
	log.Warn("Import job has failed, all tasks with the same jobID"+
		" will be marked as failed", zap.Int64("jobID", job.GetJobID()))
	for _, task := range tasks {
		err := c.imeta.UpdateTask(task.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed),
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
		err := c.imeta.UpdateJob(job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed),
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
			err = c.imeta.UpdateJob(job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed),
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
		GCRetention := Params.DataCoordCfg.ImportTaskRetention.GetAsDuration(time.Second)
		log.Info("job has reached the GC retention", zap.Int64("jobID", job.GetJobID()),
			zap.Time("cleanupTime", cleanupTime), zap.Duration("GCRetention", GCRetention))
		tasks := c.imeta.GetTaskBy(WithJob(job.GetJobID()))
		shouldRemoveJob := true
		for _, task := range tasks {
			if job.GetState() == internalpb.ImportJobState_Failed && task.GetType() == ImportTaskType {
				if len(task.(*importTask).GetSegmentIDs()) != 0 {
					shouldRemoveJob = false
					continue
				}
			}
			if task.GetNodeID() != NullNodeID {
				shouldRemoveJob = false
				continue
			}
			err := c.imeta.RemoveTask(task.GetTaskID())
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
		err := c.imeta.RemoveJob(job.GetJobID())
		if err != nil {
			log.Warn("remove import job failed", zap.Int64("jobID", job.GetJobID()), zap.Error(err))
			return
		}
		log.Info("import job removed", zap.Int64("jobID", job.GetJobID()))
	}
}
