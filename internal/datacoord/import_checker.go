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
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

type importChecker struct {
	manager   ImportTaskManager
	allocator *alloc.IDAllocator
	sm        *SegmentManager
	meta      *meta
	cluster   Cluster
}

func (c *importChecker) checkErr(task ImportTask, err error) {
	if !merr.IsRetryableErr(err) {
		err = c.manager.Update(task.GetTaskID(), UpdateState(datapb.ImportState_Failed))
		if err != nil {
			log.Warn("")
		}
		return
	}
	err = c.manager.Update(task.GetTaskID(), UpdateState(datapb.ImportState_Pending))
	if err != nil {
		log.Warn("")
	}
}

func (c *importChecker) getIdleNode() int64 {
	nodeIDs := lo.Map(c.cluster.GetSessions(), func(s *Session, _ int) int64 {
		return s.info.NodeID
	})
	for _, nodeID := range nodeIDs {
		resp, err := c.cluster.QueryImport(context.TODO(), nodeID, &datapb.QueryImportRequest{})
		if err != nil {
			log.Warn("")
			continue
		}
		if resp.GetSlots() > 0 {
			return nodeID
		}
	}
	return fakeNodeID
}

func (c *importChecker) checkPendingPreImport() {
	tasks := c.manager.GetBy(WithType(PreImportTaskType), WithStates(datapb.ImportState_Pending))
	for _, task := range tasks {
		nodeID := c.getIdleNode()
		if nodeID == fakeNodeID {
			log.Warn("no datanode can be scheduled", zap.Int64("taskID", task.GetTaskID()))
			return
		}
		req := AssemblePreImportRequest(task, c.meta)
		err := c.cluster.PreImport(context.TODO(), nodeID, req)
		if err != nil {
			log.Warn("")
			return
		}
		err = c.manager.Update(task.GetTaskID(),
			UpdateState(datapb.ImportState_InProgress),
			UpdateNodeID(nodeID))
		if err != nil {
			log.Warn("")
		}
	}
}

func (c *importChecker) checkPendingImport() {
	tasks := c.manager.GetBy(WithType(ImportTaskType), WithStates(datapb.ImportState_Pending))
	for _, task := range tasks {
		nodeID := c.getIdleNode()
		if nodeID == fakeNodeID {
			log.Warn("no datanode can be scheduled", zap.Int64("taskID", task.GetTaskID()))
			return
		}
		req, err := AssembleImportRequest(task, c.sm, c.meta, c.allocator)
		if err != nil {
			log.Warn("")
			return
		}
		err = c.cluster.ImportV2(context.TODO(), nodeID, req)
		if err != nil {
			log.Warn("")
			return
		}
		err = c.manager.Update(task.GetTaskID(),
			UpdateState(datapb.ImportState_InProgress),
			UpdateNodeID(nodeID))
		if err != nil {
			log.Warn("")
		}
	}
}

func (c *importChecker) checkCompletedOrFailed() {
	tasks := c.manager.GetBy(WithStates(datapb.ImportState_Completed, datapb.ImportState_Failed))
	for _, task := range tasks {
		if task.GetNodeID() == fakeNodeID {
			return
		}
		req := &datapb.DropImportRequest{
			RequestID: task.GetRequestID(),
			TaskID:    task.GetTaskID(),
		}
		err := c.cluster.DropImport(context.TODO(), task.GetNodeID(), req)
		if err != nil {
			log.Warn("")
			return
		}
		err = c.manager.Update(task.GetTaskID(), UpdateNodeID(fakeNodeID))
		if err != nil {
			log.Warn("")
		}
	}
}

func (c *importChecker) checkPreImportState(requestID int64) {
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
		taskID, err := c.allocator.AllocOne()
		if err != nil {
			log.Warn("")
			return
		}
		task := &importTask{
			&datapb.ImportTaskV2{
				RequestID:    requestID,
				TaskID:       taskID,
				CollectionID: t.GetCollectionID(),
				PartitionID:  t.GetPartitionID(),
				State:        datapb.ImportState_Pending,
				FileStats:    t.GetFileStats(),
			},
		}
		err = c.manager.Add(task)
		if err != nil {
			log.Warn("")
			return
		}
	}
}

func (c *importChecker) checkImportState(requestID int64) {
	tasks := c.manager.GetBy(WithType(ImportTaskType), WithReq(requestID))
	for _, t := range tasks {
		if t.GetState() != datapb.ImportState_Completed {
			return
		}
	}
	for _, task := range tasks {
		segmentIDs := task.(*importTask).GetSegmentIDs()
		for _, segmentID := range segmentIDs {
			err := AddImportSegment(c.cluster, c.meta, segmentID)
			if err != nil {
				log.Warn("")
				return
			}
		}
		for _, segmentID := range segmentIDs {
			err := c.meta.UnsetIsImporting(segmentID) // TODO: dyh, handle txn
			if err != nil {
				log.Warn("")
				return
			}
		}
	}
}

func (c *importChecker) checkPreImportProgress() {
	tasks := c.manager.GetBy(WithStates(datapb.ImportState_InProgress), WithType(PreImportTaskType))
	for _, task := range tasks {
		req := &datapb.QueryPreImportRequest{
			RequestID: task.GetRequestID(),
			TaskID:    task.GetTaskID(),
		}
		resp, err := c.cluster.QueryPreImport(context.TODO(), task.GetNodeID(), req)
		if err != nil {
			log.Warn("")
			c.checkErr(task, err)
			return
		}
		actions := []UpdateAction{UpdateFileStats(resp.GetFileStats())}
		if resp.GetState() == datapb.ImportState_Completed {
			actions = append(actions, UpdateState(datapb.ImportState_Completed))
		}
		// TODO: check if rows changed to save meta writing
		err = c.manager.Update(task.GetTaskID(), actions...)
		if err != nil {
			log.Warn("")
			return
		}
	}
}

func (c *importChecker) checkImportProgress() {
	tasks := c.manager.GetBy(WithStates(datapb.ImportState_InProgress), WithType(ImportTaskType))
	for _, task := range tasks {
		req := &datapb.QueryImportRequest{
			RequestID: task.GetRequestID(),
			TaskID:    task.GetTaskID(),
		}
		resp, err := c.cluster.QueryImport(context.TODO(), task.GetNodeID(), req)
		if err != nil {
			log.Warn("")
			c.checkErr(task, err)
			return
		}
		for _, info := range resp.GetImportSegmentsInfo() {
			operator := UpdateBinlogsOperator(info.GetSegmentID(), info.GetBinlogs(), info.GetStatslogs(), nil)
			err = c.meta.UpdateSegmentsInfo(operator)
			if err != nil {
				log.Warn("")
				continue
			}
			c.meta.SetCurrentRows(info.GetSegmentID(), info.GetImportedRows())
		}
		if resp.GetState() == datapb.ImportState_Completed {
			err = c.manager.Update(task.GetTaskID(), UpdateState(datapb.ImportState_Completed))
			if err != nil {
				log.Warn("")
			}
		}
	}
}
