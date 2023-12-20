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
	"github.com/milvus-io/milvus/pkg/util/merr"
	"go.uber.org/zap"

	"github.com/samber/lo"

	alloc "github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
)

const (
	fakeNodeID = -1
)

type ImportProcessor struct {
	manager   ImportTaskManager
	sm        *SegmentManager
	allocator *alloc.IDAllocator
	meta      *meta
	cluster   Cluster
}

func (p *ImportProcessor) process() {
	tasks := p.manager.GetBy()
	for _, task := range tasks {
		switch task.GetState() {
		case datapb.ImportState_Pending:
			switch task.GetType() {
			case PreImportTaskType:
				p.processPendingPreImport(task)
			case ImportTaskType:
				p.processPendingImport(task)
			}
		case datapb.ImportState_InProgress:
			switch task.GetType() {
			case PreImportTaskType:
				p.processInProgressPreImport(task)
			case ImportTaskType:
				p.processInProgressImport(task)
			}
		case datapb.ImportState_Failed, datapb.ImportState_Completed:
			p.processCompletedOrFailed(task)
		}
	}
}

func (p *ImportProcessor) checkErr(task ImportTask, err error) {
	if !merr.IsRetryableErr(err) {
		err = p.manager.Update(task.GetTaskID(), UpdateState(datapb.ImportState_Failed))
		if err != nil {
			log.Warn("")
		}
		return
	}
	err = p.manager.Update(task.GetTaskID(), UpdateState(datapb.ImportState_Pending))
	if err != nil {
		log.Warn("")
	}
}

func (p *ImportProcessor) getIdleNode() int64 {
	nodeIDs := lo.Map(p.cluster.GetSessions(), func(s *Session, _ int) int64 {
		return s.info.NodeID
	})
	for _, nodeID := range nodeIDs {
		resp, err := p.cluster.QueryImport(context.TODO(), nodeID, &datapb.QueryImportRequest{})
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

func (p *ImportProcessor) processPendingPreImport(task ImportTask) {
	nodeID := p.getIdleNode()
	if nodeID == fakeNodeID {
		log.Warn("no datanode can be scheduled", zap.Int64("taskID", task.GetTaskID()))
		return
	}
	req := AssemblePreImportRequest(task, p.meta)
	err := p.cluster.PreImport(context.TODO(), nodeID, req)
	if err != nil {
		log.Warn("")
		return
	}
	err = p.manager.Update(task.GetTaskID(),
		UpdateState(datapb.ImportState_InProgress),
		UpdateNodeID(nodeID))
	if err != nil {
		log.Warn("")
	}
}

func (p *ImportProcessor) processPendingImport(task ImportTask) {
	nodeID := p.getIdleNode()
	if nodeID == fakeNodeID {
		log.Warn("no datanode can be scheduled", zap.Int64("taskID", task.GetTaskID()))
		return
	}
	req, err := AssembleImportRequest(task, p.sm, p.meta, p.allocator)
	if err != nil {
		log.Warn("")
		return
	}
	err = p.cluster.ImportV2(context.TODO(), nodeID, req)
	if err != nil {
		log.Warn("")
		return
	}
	err = p.manager.Update(task.GetTaskID(),
		UpdateState(datapb.ImportState_InProgress),
		UpdateNodeID(nodeID))
	if err != nil {
		log.Warn("")
	}
}

func (p *ImportProcessor) processInProgressPreImport(task ImportTask) {
	req := &datapb.QueryPreImportRequest{
		RequestID: task.GetRequestID(),
		TaskID:    task.GetTaskID(),
	}
	resp, err := p.cluster.QueryPreImport(context.TODO(), task.GetNodeID(), req)
	if err != nil {
		log.Warn("")
		p.checkErr(task, err)
		return
	}
	actions := []UpdateAction{UpdateFileStats(resp.GetFileStats())}
	if resp.GetState() == datapb.ImportState_Completed {
		actions = append(actions, UpdateState(datapb.ImportState_Completed))
	}
	// TODO: check if rows changed to save meta op
	err = p.manager.Update(task.GetTaskID(), actions...)
	if err != nil {
		log.Warn("")
		return
	}
}

func (p *ImportProcessor) processInProgressImport(task ImportTask) {
	req := &datapb.QueryImportRequest{
		RequestID: task.GetRequestID(),
		TaskID:    task.GetTaskID(),
	}
	resp, err := p.cluster.QueryImport(context.TODO(), task.GetNodeID(), req)
	if err != nil {
		log.Warn("")
		p.checkErr(task, err)
		return
	}
	for _, info := range resp.GetImportSegmentsInfo() {
		operator := UpdateBinlogsOperator(info.GetSegmentID(), info.GetBinlogs(), info.GetStatslogs(), nil)
		err = p.meta.UpdateSegmentsInfo(operator)
		if err != nil {
			log.Warn("")
			continue
		}
		p.meta.SetCurrentRows(info.GetSegmentID(), info.GetImportedRows())
	}
	if resp.GetState() == datapb.ImportState_Completed {
		err = p.manager.Update(task.GetTaskID(), UpdateState(datapb.ImportState_Completed))
		if err != nil {
			log.Warn("")
		}
	}
}

func (p *ImportProcessor) processCompletedOrFailed(task ImportTask) {
	if task.GetNodeID() == fakeNodeID {
		return
	}
	req := &datapb.DropImportRequest{
		RequestID: task.GetRequestID(),
		TaskID:    task.GetTaskID(),
	}
	err := p.cluster.DropImport(context.TODO(), task.GetNodeID(), req)
	if err != nil {
		log.Warn("")
		return
	}
	err = p.manager.Update(task.GetTaskID(), UpdateNodeID(fakeNodeID))
	if err != nil {
		log.Warn("")
	}
}
