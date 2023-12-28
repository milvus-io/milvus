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
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
)

type ImportChecker interface {
	Start()
	Close()
}

type importChecker struct {
	meta    *meta
	cluster Cluster
	alloc   allocator
	sm      Manager
	imeta   ImportMeta

	closeOnce sync.Once
	closeChan chan struct{}
}

func NewImportChecker(meta *meta,
	cluster Cluster,
	alloc allocator,
	sm Manager,
	imeta ImportMeta,
) ImportChecker {
	return &importChecker{
		meta:      meta,
		cluster:   cluster,
		alloc:     alloc,
		sm:        sm,
		imeta:     imeta,
		closeChan: make(chan struct{}),
	}
}

func (c *importChecker) Start() {
	log.Info("start import checker")
	checkTicker := time.NewTicker(5 * time.Second)
	defer checkTicker.Stop()
	logTicker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-c.closeChan:
			log.Info("import checker exited")
			return
		case <-checkTicker.C:
			tasks := c.imeta.GetBy()
			tasksByReq := lo.GroupBy(tasks, func(t ImportTask) int64 {
				return t.GetRequestID()
			})
			for requestID := range tasksByReq {
				c.checkPreImportState(requestID)
				c.checkImportState(requestID)
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
		byState := lo.GroupBy(tasks, func(t ImportTask) milvuspb.ImportState {
			return t.GetState()
		})
		log.Info("import task stats", zap.String("type", taskType.String()),
			zap.Int("pending", len(byState[milvuspb.ImportState_Pending])),
			zap.Int("inProgress", len(byState[milvuspb.ImportState_InProgress])),
			zap.Int("completed", len(byState[milvuspb.ImportState_Completed])),
			zap.Int("failed", len(byState[milvuspb.ImportState_Failed])))
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
		if t.GetState() != milvuspb.ImportState_Completed {
			return
		}
	}
	groups, err := RegroupImportFiles(tasks)
	if err != nil {
		log.Warn("regroup import files failed", zap.Int64("reqID", requestID))
		return
	}
	importTasks := c.imeta.GetBy(WithType(ImportTaskType), WithReq(requestID))
	if len(importTasks) == len(groups) {
		return // all imported are generated
	}
	for _, t := range importTasks { // happens only when txn failed
		err := c.cluster.DropImport(t.GetNodeID(), &datapb.DropImportRequest{
			TaskID:    t.GetTaskID(),
			RequestID: t.GetRequestID(),
		})
		if err != nil {
			log.Warn("drop import failed", WrapLogFields(t, err)...)
			return
		}
		for _, segment := range t.(*importTask).GetSegmentIDs() {
			err = c.meta.DropSegment(segment)
			if err != nil {
				log.Warn("drop segment failed", WrapLogFields(t, err)...)
				return
			}
		}
		err = c.imeta.Remove(t.GetTaskID())
		if err != nil {
			log.Warn("remove import task failed", WrapLogFields(t, err)...)
			return
		}
	}
	pt := tasks[0].(*preImportTask)
	newTasks, err := NewImportTasks(groups, pt.GetRequestID(), pt.GetCollectionID(), pt.GetSchema(), c.sm, c.alloc)
	if err != nil {
		log.Warn("assemble import tasks failed", zap.Error(err))
		return
	}
	for _, t := range newTasks {
		err = c.imeta.Add(t)
		if err != nil {
			log.Warn("add new import task failed", WrapLogFields(t, nil)...)
			return
		}
		log.Info("add new import task", WrapLogFields(t, nil)...)
	}
}

func (c *importChecker) checkImportState(requestID int64) {
	tasks := c.imeta.GetBy(WithType(ImportTaskType), WithReq(requestID))
	for _, t := range tasks {
		if t.GetState() != milvuspb.ImportState_Completed {
			return
		}
	}
	if AreAllTasksFinished(tasks, c.meta) {
		return
	}
	for _, task := range tasks {
		segmentIDs := task.(*importTask).GetSegmentIDs()
		for _, segmentID := range segmentIDs {
			err := AddImportSegment(c.cluster, c.meta, segmentID)
			if err != nil {
				log.Warn("add import segment failed", WrapLogFields(task, err)...)
				return
			}
			err = c.meta.UnsetIsImporting(segmentID)
			if err != nil {
				log.Warn("unset importing flag failed", WrapLogFields(task, err)...)
				return
			}
		}
	}
}
