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
	alloc "github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/samber/lo"
	"sync"
	"time"
)

type ImportChecker struct {
	meta      *meta
	cluster   Cluster
	allocator *alloc.IDAllocator
	manager   ImportTaskManager

	closeOnce sync.Once
	closeChan chan struct{}
}

func NewImportChecker(meta *meta,
	cluster Cluster,
	allocator *alloc.IDAllocator,
	manager ImportTaskManager) *ImportChecker {
	return &ImportChecker{
		meta:      meta,
		cluster:   cluster,
		allocator: allocator,
		manager:   manager,
		closeChan: make(chan struct{}),
	}
}

func (c *ImportChecker) Start() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.closeChan:
			log.Info("import checker exited")
			return
		case <-ticker.C:
			tasks := c.manager.GetBy()
			tasksByReq := lo.GroupBy(tasks, func(t ImportTask) int64 {
				return t.GetRequestID()
			})
			for requestID := range tasksByReq {
				c.checkPreImportState(requestID)
				c.checkImportState(requestID)
			}
		}
	}
}

func (c *ImportChecker) Close() {
	c.closeOnce.Do(func() {
		close(c.closeChan)
	})
}

func (c *ImportChecker) checkPreImportState(requestID int64) {
	tasks := c.manager.GetBy(WithType(PreImportTaskType), WithReq(requestID))
	for _, t := range tasks {
		if t.GetState() != datapb.ImportState_Completed {
			return
		}
	}
	importTasks := c.manager.GetBy(WithType(ImportTaskType), WithReq(requestID))
	if len(importTasks) == len(tasks) {
		return // all imported are generated
	}
	for _, t := range importTasks { // happens only when txn failed
		err := c.cluster.DropImport(context.TODO(), t.GetNodeID(), &datapb.DropImportRequest{
			TaskID:    t.GetTaskID(),
			RequestID: t.GetRequestID(),
		})
		if err != nil {
			log.Warn("")
			return
		}
		err = c.manager.Remove(t.GetTaskID())
		if err != nil {
			log.Warn("")
			return
		}
	}
	for _, t := range tasks {
		task, err := AssembleImportTask(t, c.allocator)
		if err != nil {
			log.Warn("")
			return
		}
		err = c.manager.Add(task)
		if err != nil {
			log.Warn("")
			return
		}
	}
}

func (c *ImportChecker) checkImportState(requestID int64) {
	tasks := c.manager.GetBy(WithType(ImportTaskType), WithReq(requestID))
	for _, t := range tasks {
		if t.GetState() != datapb.ImportState_Completed {
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
				log.Warn("")
				return
			}
			err = c.meta.UnsetIsImporting(segmentID)
			if err != nil {
				log.Warn("")
				return
			}
		}
	}
}
