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

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

const (
	NullNodeID = -1
)

type ImportScheduler struct {
	meta    *meta
	cluster Cluster
	alloc   allocator
	sm      *SegmentManager
	imeta   ImportMeta

	closeOnce sync.Once
	closeChan chan struct{}
}

func NewImportScheduler(meta *meta,
	cluster Cluster,
	alloc allocator,
	sm *SegmentManager,
	imeta ImportMeta,
) *ImportScheduler {
	return &ImportScheduler{
		meta:      meta,
		cluster:   cluster,
		alloc:     alloc,
		sm:        sm,
		imeta:     imeta,
		closeChan: make(chan struct{}),
	}
}

func (s *ImportScheduler) Start() {
	log.Info("start import scheduler")
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.closeChan:
			log.Info("import scheduler exited")
			return
		case <-ticker.C:
			s.process()
		}
	}
}

func (s *ImportScheduler) Close() {
	s.closeOnce.Do(func() {
		close(s.closeChan)
	})
}

func (s *ImportScheduler) process() {
	tasks := s.imeta.GetBy()
	for _, task := range tasks {
		switch task.GetState() {
		case datapb.ImportState_Pending:
			switch task.GetType() {
			case PreImportTaskType:
				s.processPendingPreImport(task)
			case ImportTaskType:
				s.processPendingImport(task)
			}
		case datapb.ImportState_InProgress:
			switch task.GetType() {
			case PreImportTaskType:
				s.processInProgressPreImport(task)
			case ImportTaskType:
				s.processInProgressImport(task)
			}
		case datapb.ImportState_Failed, datapb.ImportState_Completed:
			s.processCompletedOrFailed(task)
		}
	}
}

func (s *ImportScheduler) checkErr(task ImportTask, err error) {
	if !merr.IsRetryableErr(err) {
		err = s.imeta.Update(task.GetTaskID(), UpdateState(datapb.ImportState_Failed))
		if err != nil {
			log.Warn("failed to update import task state to failed", WrapLogFields(task, err)...)
		}
		return
	}
	err = s.imeta.Update(task.GetTaskID(), UpdateState(datapb.ImportState_Pending))
	if err != nil {
		log.Warn("failed to update import task state to pending", WrapLogFields(task, err)...)
	}
}

func (s *ImportScheduler) getIdleNode() int64 {
	nodeIDs := lo.Map(s.cluster.GetSessions(), func(s *Session, _ int) int64 {
		return s.info.NodeID
	})
	for _, nodeID := range nodeIDs {
		resp, err := s.cluster.QueryImport(nodeID, &datapb.QueryImportRequest{})
		if err != nil {
			log.Warn("query import failed", zap.Error(err))
			continue
		}
		if resp.GetSlots() > 0 {
			return nodeID
		}
	}
	return NullNodeID
}

func (s *ImportScheduler) processPendingPreImport(task ImportTask) {
	nodeID := s.getIdleNode()
	if nodeID == NullNodeID {
		log.Warn("no datanode can be scheduled", WrapLogFields(task, nil)...)
		return
	}
	req := AssemblePreImportRequest(task)
	err := s.cluster.PreImport(nodeID, req)
	if err != nil {
		log.Warn("preimport failed", WrapLogFields(task, err)...)
		return
	}
	err = s.imeta.Update(task.GetTaskID(),
		UpdateState(datapb.ImportState_InProgress),
		UpdateNodeID(nodeID))
	if err != nil {
		log.Warn("update import task failed", WrapLogFields(task, err)...)
	}
}

func (s *ImportScheduler) processPendingImport(task ImportTask) {
	nodeID := s.getIdleNode()
	if nodeID == NullNodeID {
		log.Warn("no datanode can be scheduled", WrapLogFields(task, nil)...)
		return
	}
	req, err := AssembleImportRequest(task, s.sm, s.alloc, s.imeta)
	if err != nil {
		log.Warn("assemble import request failed", WrapLogFields(task, err)...)
		return
	}
	err = s.cluster.ImportV2(nodeID, req)
	if err != nil {
		log.Warn("import failed", WrapLogFields(task, err)...)
		return
	}
	err = s.imeta.Update(task.GetTaskID(),
		UpdateState(datapb.ImportState_InProgress),
		UpdateNodeID(nodeID))
	if err != nil {
		log.Warn("update import task failed", WrapLogFields(task, err)...)
	}
}

func (s *ImportScheduler) processInProgressPreImport(task ImportTask) {
	req := &datapb.QueryPreImportRequest{
		RequestID: task.GetRequestID(),
		TaskID:    task.GetTaskID(),
	}
	resp, err := s.cluster.QueryPreImport(task.GetNodeID(), req)
	if err != nil {
		log.Warn("query preimport failed", WrapLogFields(task, err)...)
		s.checkErr(task, err)
		return
	}
	actions := []UpdateAction{UpdateFileStats(resp.GetFileStats())}
	if resp.GetState() == datapb.ImportState_Completed {
		actions = append(actions, UpdateState(datapb.ImportState_Completed))
	}
	// TODO: check if rows changed to save meta op
	err = s.imeta.Update(task.GetTaskID(), actions...)
	if err != nil {
		log.Warn("update import task failed", WrapLogFields(task, err)...)
		return
	}
}

func (s *ImportScheduler) processInProgressImport(task ImportTask) {
	req := &datapb.QueryImportRequest{
		RequestID: task.GetRequestID(),
		TaskID:    task.GetTaskID(),
	}
	resp, err := s.cluster.QueryImport(task.GetNodeID(), req)
	if err != nil {
		log.Warn("query import failed", WrapLogFields(task, err)...)
		s.checkErr(task, err)
		return
	}
	for _, info := range resp.GetImportSegmentsInfo() {
		operator := UpdateBinlogsOperator(info.GetSegmentID(), info.GetBinlogs(), info.GetStatslogs(), nil)
		err = s.meta.UpdateSegmentsInfo(operator)
		if err != nil {
			log.Warn("update import segment info failed", WrapLogFields(task, err)...)
			continue
		}
		s.meta.SetCurrentRows(info.GetSegmentID(), info.GetImportedRows())
	}
	if resp.GetState() == datapb.ImportState_Completed {
		err = s.imeta.Update(task.GetTaskID(), UpdateState(datapb.ImportState_Completed))
		if err != nil {
			log.Warn("update import task failed", WrapLogFields(task, err)...)
		}
	}
}

func (s *ImportScheduler) processCompletedOrFailed(task ImportTask) {
	if task.GetNodeID() == NullNodeID {
		return
	}
	req := &datapb.DropImportRequest{
		RequestID: task.GetRequestID(),
		TaskID:    task.GetTaskID(),
	}
	err := s.cluster.DropImport(task.GetNodeID(), req)
	if err != nil {
		log.Warn("drop import failed", WrapLogFields(task, err)...)
		return
	}
	err = s.imeta.Update(task.GetTaskID(), UpdateNodeID(NullNodeID))
	if err != nil {
		log.Warn("update import task failed", WrapLogFields(task, err)...)
	}
}
