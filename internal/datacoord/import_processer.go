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

	"github.com/samber/lo"

	alloc "github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
)

const (
	fakeNodeID = -1
)

type importProcessor struct {
	ctx       context.Context
	manager   ImportTaskManager
	sm        *SegmentManager
	allocator *alloc.IDAllocator
	meta      *meta
	cluster   *Cluster
}

func (s *importProcessor) process() {
	tasks := s.manager.GetBy()
	for _, task := range tasks {
		switch task.State() {
		case datapb.ImportState_Pending:
			s.processPending(task)
		case datapb.ImportState_Preparing:
			s.processPreparing(task)
		case datapb.ImportState_InProgress:
			s.processInProgress(task)
		case datapb.ImportState_Failed, datapb.ImportState_Completed:
			s.processCompletedOrFailed(task)
		}
	}
}

func (s *importProcessor) getIdleNode() int64 {
	nodeIDs := lo.Map(s.cluster.GetSessions(), func(s *Session, _ int) int64 {
		return s.info.NodeID
	})
	for _, nodeID := range nodeIDs {
		resp, err := s.cluster.GetImportState(nodeID, &datapb.GetImportStateRequest{})
		if err != nil {
			log.Warn("")
			continue
		}
		if resp.GetSlots() > 0 {
			return nodeID
		}
	}
	return -1
}

func (s *importProcessor) processPending(task ImportTask) {
	nodeID := s.getIdleNode()
	req := AssemblePreImportRequest(task)
	err := s.cluster.PreImport(nodeID, req)
	if err != nil {
		log.Warn("")
		return
	}
	err = s.manager.Update(task.ID(), UpdateState(datapb.ImportState_Preparing))
	if err != nil {
		log.Warn("")
	}
}

func (s *importProcessor) processPreparing(task ImportTask) {
	if !IsPreImportDone(task) {
		return
	}
	nodeID := s.getIdleNode()
	req, err := AssembleImportRequest(task, s.sm, s.allocator)
	if err != nil {
		log.Warn("")
		return
	}
	err = s.cluster.ImportV2(nodeID, req)
	if err != nil {
		log.Warn("")
		return
	}
	err = s.manager.Update(task.ID(), UpdateState(datapb.ImportState_InProgress))
	if err != nil {
		log.Warn("")
	}
}

func (s *importProcessor) processInProgress(task ImportTask) {
	if !IsImportDone(task) {
		return
	}
	for _, segmentID := range task.SegmentIDs() {
		err := AddImportSegment(s.cluster, s.meta, segmentID)
		if err != nil {
			log.Warn("")
			return
		}
	}
	for _, segmentID := range task.SegmentIDs() {
		err := s.meta.UnsetIsImporting(segmentID)
		if err != nil {
			log.Warn("")
			return
		}
	}
	err := s.manager.Update(task.ID(), UpdateState(datapb.ImportState_Completed))
	if err != nil {
		log.Warn("")
	}
}

func (s *importProcessor) processCompletedOrFailed(task ImportTask) {
	if task.NodeID() == fakeNodeID {
		return
	}
	req := &datapb.DropImportRequest{
		RequestID: task.ReqID(),
		TaskID:    task.ID(),
	}
	err := s.cluster.DropImport(task.NodeID(), req)
	if err != nil {
		log.Warn("")
		return
	}
	err = s.manager.Update(task.ID(), UpdateNodeID(fakeNodeID))
	if err != nil {
		log.Warn("")
	}
}
