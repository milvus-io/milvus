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
	"sort"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type ImportChecker interface {
	Start()
	Close()
}

type importChecker struct {
	meta         *meta
	broker       broker.Broker
	cluster      Cluster
	alloc        allocator
	sm           Manager
	imeta        ImportMeta
	buildIndexCh chan UniqueID

	closeOnce sync.Once
	closeChan chan struct{}
}

func NewImportChecker(meta *meta,
	broker broker.Broker,
	cluster Cluster,
	alloc allocator,
	sm Manager,
	imeta ImportMeta,
	buildIndexCh chan UniqueID,
) ImportChecker {
	return &importChecker{
		meta:         meta,
		broker:       broker,
		cluster:      cluster,
		alloc:        alloc,
		sm:           sm,
		imeta:        imeta,
		buildIndexCh: buildIndexCh,
		closeChan:    make(chan struct{}),
	}
}

func (c *importChecker) Start() {
	log.Info("start import checker")
	var (
		stateTicker   = time.NewTicker(2 * time.Second)
		collTicker    = time.NewTicker(30 * time.Second)
		timeoutTicker = time.NewTicker(10 * time.Minute)
	)
	defer stateTicker.Stop()
	defer collTicker.Stop()
	defer timeoutTicker.Stop()
	for {
		select {
		case <-c.closeChan:
			log.Info("import checker exited")
			return
		case <-stateTicker.C:
			jobs := c.imeta.GetJobBy()
			for _, job := range jobs {
				if job.GetSchema() == nil {
					err := UpdateSchema(job, c.broker, c.imeta)
					if err != nil {
						continue
					}
				}
				c.checkLackPreImport(job)
				c.checkLackImports(job)
				c.checkImportState(job)
			}
		case <-timeoutTicker.C:
			tasks := c.imeta.GetTaskBy()
			tasksByJob := lo.GroupBy(tasks, func(t ImportTask) int64 {
				return t.GetJobID()
			})
			for jobID := range tasksByJob {
				c.checkTimeout(jobID)
				c.checkFailure(jobID)
				c.checkGC(jobID)
			}
		case <-collTicker.C:
			tasks := c.imeta.GetTaskBy()
			tasksByCollection := lo.GroupBy(tasks, func(t ImportTask) int64 {
				return t.GetCollectionID()
			})
			for collectionID := range tasksByCollection {
				c.checkCollection(collectionID)
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
		byState := lo.GroupBy(tasks, func(t ImportTask) internalpb.ImportState {
			return t.GetState()
		})
		pending := len(byState[internalpb.ImportState_Pending])
		inProgress := len(byState[internalpb.ImportState_InProgress])
		completed := len(byState[internalpb.ImportState_Completed])
		failed := len(byState[internalpb.ImportState_Failed])
		log.Info("import task stats", zap.String("type", taskType.String()),
			zap.Int("pending", pending), zap.Int("inProgress", inProgress),
			zap.Int("completed", completed), zap.Int("failed", failed))
		metrics.ImportTasks.WithLabelValues(taskType.String(), internalpb.ImportState_Pending.String()).Set(float64(pending))
		metrics.ImportTasks.WithLabelValues(taskType.String(), internalpb.ImportState_InProgress.String()).Set(float64(inProgress))
		metrics.ImportTasks.WithLabelValues(taskType.String(), internalpb.ImportState_Completed.String()).Set(float64(completed))
		metrics.ImportTasks.WithLabelValues(taskType.String(), internalpb.ImportState_Failed.String()).Set(float64(failed))
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
	lackPreImports := c.getLackFilesForPreImports(job)
	if len(lackPreImports) > 0 {
		// Preimport tasks are not fully generated; thus, generating imports should not be triggered.
		return nil
	}
	preimports := c.imeta.GetTaskBy(WithType(PreImportTaskType), WithJob(job.GetJobID()))
	lacks := make(map[int64]*datapb.ImportFileStats, 0)
	for _, t := range preimports {
		if t.GetState() != internalpb.ImportState_Completed {
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

func (c *importChecker) checkLackPreImport(job ImportJob) {
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
}

func (c *importChecker) checkLackImports(job ImportJob) {
	lacks := c.getLackFilesForImports(job)
	if len(lacks) == 0 {
		return
	}

	groups := RegroupImportFiles(job, lacks)
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
}

func (c *importChecker) checkImportState(job ImportJob) {
	tasks := c.imeta.GetTaskBy(WithType(ImportTaskType), WithJob(job.GetJobID()))
	for _, t := range tasks {
		if t.GetState() != internalpb.ImportState_Completed {
			return
		}
	}
	unfinished := make([]int64, 0)
	for _, task := range tasks {
		segmentIDs := task.(*importTask).GetSegmentIDs()
		for _, segmentID := range segmentIDs {
			segment := c.meta.GetSegment(segmentID)
			if segment == nil { // may be compacted
				continue
			}
			if segment.GetIsImporting() {
				unfinished = append(unfinished, segmentID)
			}
		}
	}
	if len(unfinished) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := c.sm.FlushImportSegments(ctx, job.GetCollectionID(), unfinished)
	if err != nil {
		log.Warn("flush imported segments failed", zap.Int64("jobID", job.GetJobID()),
			zap.Int64("collectionID", job.GetCollectionID()), zap.Int64s("segments", unfinished), zap.Error(err))
		return
	}

	for _, segmentID := range unfinished {
		err = AddImportSegment(c.cluster, c.meta, segmentID)
		if err != nil {
			log.Warn("add import segment failed", zap.Int64("jobID", job.GetJobID()),
				zap.Int64("collectionID", job.GetCollectionID()), zap.Error(err))
			return
		}
		c.buildIndexCh <- segmentID // accelerate index building
		err = c.meta.UnsetIsImporting(segmentID)
		if err != nil {
			log.Warn("unset importing flag failed", zap.Int64("jobID", job.GetJobID()),
				zap.Int64("collectionID", job.GetCollectionID()), zap.Error(err))
			return
		}
	}
}

func (c *importChecker) checkFailure(jobID int64) {
	tasks := c.imeta.GetTaskBy(WithJob(jobID))
	var (
		isFailed   bool
		failedTask ImportTask
	)
	for _, task := range tasks {
		if task.GetState() == internalpb.ImportState_Failed {
			isFailed = true
			failedTask = task
			log.Warn("Import has failed, all tasks with the same jobID will be marked as failed", WrapTaskLog(task)...)
			break
		}
	}
	if isFailed {
		for _, task := range tasks {
			if task.GetState() == internalpb.ImportState_Failed {
				continue
			}
			err := c.imeta.UpdateTask(task.GetTaskID(), UpdateState(internalpb.ImportState_Failed),
				UpdateReason(failedTask.GetReason()))
			if err != nil {
				continue
			}
		}
	}
}

func (c *importChecker) checkTimeout(jobID int64) {
	tasks := c.imeta.GetTaskBy(WithStates(internalpb.ImportState_InProgress), WithJob(jobID))
	if len(tasks) == 0 {
		return
	}
	for _, task := range tasks {
		if time.Since(task.GetLastActiveTime()) > 10*time.Minute {
			log.Warn("task progress is stagnant", WrapTaskLog(task)...)
		}
	}

	timeoutTime := tsoutil.PhysicalTime(c.imeta.GetJob(jobID).GetTimeoutTs())
	if time.Now().After(timeoutTime) {
		log.Warn("Import timeout, expired the specified time limit",
			zap.Int64("jobID", jobID), zap.Time("timeoutTime", timeoutTime))
		for _, task := range tasks {
			err := c.imeta.UpdateTask(task.GetTaskID(), UpdateState(internalpb.ImportState_Failed),
				UpdateReason("import timeout"))
			if err != nil {
				log.Warn("update task state failed", WrapTaskLog(task, zap.Error(err))...)
			}
		}
	}
}

func (c *importChecker) checkCollection(collectionID int64) {
	tasks := c.imeta.GetTaskBy(WithStates(internalpb.ImportState_Pending,
		internalpb.ImportState_InProgress), WithCollection(collectionID))
	if len(tasks) == 0 {
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
		for _, task := range tasks {
			err = c.imeta.UpdateTask(task.GetTaskID(), UpdateState(internalpb.ImportState_Failed),
				UpdateReason(fmt.Sprintf("collection %d dropped", collectionID)))
			if err != nil {
				log.Warn("update task state failed", WrapTaskLog(task, zap.Error(err))...)
			}
		}
	}
}

func (c *importChecker) checkGC(jobID int64) {
	GCRetention := Params.DataCoordCfg.ImportTaskRetention.GetAsDuration(time.Second)
	tasks := c.imeta.GetTaskBy(WithStates(internalpb.ImportState_Failed, internalpb.ImportState_Completed), WithJob(jobID))
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].GetLastActiveTime().After(tasks[j].GetLastActiveTime())
	})
	if len(tasks) == 0 {
		return
	}
	if time.Since(tasks[0].GetLastActiveTime()) >= GCRetention {
		log.Info("tasks has reached the GC retention",
			WrapTaskLog(tasks[0], zap.Time("taskLastActiveTime", tasks[0].GetLastActiveTime()),
				zap.Duration("GCRetention", GCRetention))...)
		err := c.imeta.RemoveJob(jobID)
		if err != nil {
			log.Warn("remove import job failed", WrapTaskLog(tasks[0])...)
			return
		}
		log.Info("reached GC retention, job removed", zap.Int64("jobID", jobID))
		for _, task := range tasks {
			err = c.imeta.RemoveTask(task.GetTaskID())
			if err != nil {
				log.Warn("remove task failed during GC", WrapTaskLog(task, zap.Error(err))...)
				continue
			}
			log.Info("reached GC retention, task removed", WrapTaskLog(task)...)
		}
	}
}
