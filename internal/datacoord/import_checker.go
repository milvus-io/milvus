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
	"github.com/milvus-io/milvus/pkg/metrics"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type ImportChecker interface {
	Start()
	Close()
}

type importChecker struct {
	meta         *meta
	cluster      Cluster
	alloc        allocator
	sm           Manager
	imeta        ImportMeta
	buildIndexCh chan UniqueID

	closeOnce sync.Once
	closeChan chan struct{}
}

func NewImportChecker(meta *meta,
	cluster Cluster,
	alloc allocator,
	sm Manager,
	imeta ImportMeta,
	buildIndexCh chan UniqueID,
) ImportChecker {
	return &importChecker{
		meta:         meta,
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
		stateTicker   = time.NewTicker(5 * time.Second)
		logTicker     = time.NewTicker(30 * time.Second)
		timeoutTicker = time.NewTicker(10 * time.Minute)
	)
	defer stateTicker.Stop()
	defer logTicker.Stop()
	defer timeoutTicker.Stop()
	for {
		select {
		case <-c.closeChan:
			log.Info("import checker exited")
			return
		case <-stateTicker.C:
			tasks := c.imeta.GetBy()
			tasksByReq := lo.GroupBy(tasks, func(t ImportTask) int64 {
				return t.GetRequestID()
			})
			for requestID := range tasksByReq {
				c.checkPreImportState(requestID)
				c.checkImportState(requestID)
			}
		case <-timeoutTicker.C:
			tasks := c.imeta.GetBy()
			tasksByReq := lo.GroupBy(tasks, func(t ImportTask) int64 {
				return t.GetRequestID()
			})
			for requestID := range tasksByReq {
				c.checkTimeout(requestID)
				c.checkGC(requestID)
			}
		case <-logTicker.C:
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
	tasks := c.imeta.GetBy(WithType(PreImportTaskType))
	logFunc(tasks, PreImportTaskType)
	tasks = c.imeta.GetBy(WithType(ImportTaskType))
	logFunc(tasks, ImportTaskType)
}

func (c *importChecker) checkPreImportState(requestID int64) {
	tasks := c.imeta.GetBy(WithType(PreImportTaskType), WithReq(requestID))
	if len(tasks) == 0 {
		return
	}
	for _, t := range tasks {
		if t.GetState() != internalpb.ImportState_Completed {
			return
		}
	}
	groups, err := RegroupImportFiles(tasks)
	if err != nil {
		log.Warn("regroup import files failed", zap.Int64("reqID", requestID), zap.Error(err))
		return
	}
	importTasks := c.imeta.GetBy(WithType(ImportTaskType), WithReq(requestID))
	if len(importTasks) == len(groups) {
		return // all imported are generated
	}
	for _, t := range importTasks { // happens only when txn of adding new import tasks failed
		err = DropImportTask(t, c.cluster, c.imeta)
		if err != nil {
			log.Warn("drop import failed", WrapLogFields(t, zap.Error(err))...)
			return
		}
		for _, segment := range t.(*importTask).GetSegmentIDs() {
			err = c.meta.DropSegment(segment)
			if err != nil {
				log.Warn("drop segment failed", WrapLogFields(t, zap.Error(err))...)
				return
			}
		}
		err = c.imeta.Remove(t.GetTaskID())
		if err != nil {
			log.Warn("remove import task failed", WrapLogFields(t, zap.Error(err))...)
			return
		}
	}
	pt := tasks[0].(*preImportTask)
	newTasks, err := NewImportTasks(groups, pt, c.sm, c.alloc)
	if err != nil {
		log.Warn("assemble import tasks failed", zap.Error(err))
		return
	}
	for _, t := range newTasks {
		err = c.imeta.Add(t)
		if err != nil {
			log.Warn("add new import task failed", WrapLogFields(t, zap.Error(err))...)
			return
		}
		log.Info("add new import task", WrapLogFields(t)...)
	}
}

func (c *importChecker) checkImportState(requestID int64) {
	tasks := c.imeta.GetBy(WithType(ImportTaskType), WithReq(requestID))
	for _, t := range tasks {
		if t.GetState() != internalpb.ImportState_Completed {
			return
		}
	}
	if AreAllTasksFinished(tasks, c.meta) {
		return
	}
	for _, task := range tasks {
		segmentIDs := task.(*importTask).GetSegmentIDs()
		_, err := c.sm.SealAllSegments(context.Background(), task.GetCollectionID(), segmentIDs, true)
		if err != nil {
			log.Warn("seal imported segments failed", WrapLogFields(task, zap.Error(err))...)
			return
		}
		for _, segmentID := range segmentIDs {
			err := AddImportSegment(c.cluster, c.meta, segmentID)
			if err != nil {
				log.Warn("add import segment failed", WrapLogFields(task, zap.Error(err))...)
				return
			}
			err = c.meta.UnsetIsImporting(segmentID)
			if err != nil {
				log.Warn("unset importing flag failed", WrapLogFields(task, zap.Error(err))...)
				return
			}
		}
		// accelerate building index
		for _, segmentID := range segmentIDs {
			c.buildIndexCh <- segmentID
		}
	}
}

func (c *importChecker) checkTimeout(requestID int64) {
	inactiveTimeout := Params.DataCoordCfg.ImportInactiveTimeout.GetAsDuration(time.Second)
	tasks := c.imeta.GetBy(WithStates(internalpb.ImportState_InProgress), WithReq(requestID))
	var isTimeout = false
	for _, task := range tasks {
		timeoutTime := tsoutil.PhysicalTime(task.GetTimeoutTs())
		if time.Now().After(timeoutTime) {
			isTimeout = true
			log.Warn("Import task timeout, expired the specified time limit",
				WrapLogFields(task, zap.Time("timeoutTime", timeoutTime))...)
			break
		}
		if time.Since(task.GetLastActiveTime()) > inactiveTimeout {
			isTimeout = true
			log.Warn("Import task timeout, task progress is stagnant",
				WrapLogFields(task, zap.Duration("inactiveTimeout", inactiveTimeout))...)
			break
		}
	}
	if !isTimeout {
		return
	}
	// fail all tasks with same requestID
	for _, task := range tasks {
		err := c.imeta.Update(task.GetTaskID(), UpdateState(internalpb.ImportState_Failed),
			UpdateReason(fmt.Sprintf("import timeout, requestID=%d, taskID=%d", task.GetRequestID(), task.GetTaskID())))
		if err != nil {
			log.Warn("update task state failed", WrapLogFields(task, zap.Error(err))...)
		}
	}
}

func (c *importChecker) checkGC(requestID int64) {
	GCRetention := Params.DataCoordCfg.ImportTaskRetention.GetAsDuration(time.Second)
	tasks := c.imeta.GetBy(WithStates(internalpb.ImportState_Failed, internalpb.ImportState_Completed), WithReq(requestID))
	var needGC = false
	for _, task := range tasks {
		if time.Since(task.GetLastActiveTime()) >= GCRetention {
			log.Info("the task has reached the GC retention",
				WrapLogFields(task, zap.Time("taskLastActiveTime", task.GetLastActiveTime()),
					zap.Duration("GCRetention", GCRetention))...)
			needGC = true
			break
		}
	}
	if !needGC {
		return
	}
	for _, task := range tasks {
		err := c.imeta.Remove(task.GetTaskID())
		if err != nil {
			log.Warn("remove task failed", WrapLogFields(task, zap.Error(err))...)
		}
		log.Info("reached GC retention, task removed", WrapLogFields(task)...)
	}
}
