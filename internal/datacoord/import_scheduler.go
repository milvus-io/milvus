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
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/lock"
)

const (
	NullNodeID = -1
)

type ImportScheduler interface {
	Start()
	Close()
}

type importScheduler struct {
	meta    *meta
	cluster Cluster
	alloc   allocator
	imeta   ImportMeta

	buildIndexCh chan UniqueID

	closeOnce sync.Once
	closeChan chan struct{}
}

func NewImportScheduler(meta *meta,
	cluster Cluster,
	alloc allocator,
	imeta ImportMeta,
	buildIndexCh chan UniqueID,
) ImportScheduler {
	return &importScheduler{
		meta:         meta,
		cluster:      cluster,
		alloc:        alloc,
		imeta:        imeta,
		buildIndexCh: buildIndexCh,
		closeChan:    make(chan struct{}),
	}
}

func (s *importScheduler) Start() {
	log.Info("start import scheduler")
	ticker := time.NewTicker(Params.DataCoordCfg.ImportScheduleInterval.GetAsDuration(time.Second))
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

func (s *importScheduler) Close() {
	s.closeOnce.Do(func() {
		close(s.closeChan)
	})
}

func (s *importScheduler) process() {
	jobs := s.imeta.GetJobBy()
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].GetJobID() < jobs[j].GetJobID()
	})
	nodeSlots := s.peekSlots()
	for _, job := range jobs {
		tasks := s.imeta.GetTaskBy(WithJob(job.GetJobID()))
		for _, task := range tasks {
			switch task.GetState() {
			case datapb.ImportTaskStateV2_Pending:
				nodeID := s.getNodeID(task, nodeSlots)
				switch task.GetType() {
				case PreImportTaskType:
					s.processPendingPreImport(task, nodeID)
				case ImportTaskType:
					s.processPendingImport(task, nodeID)
				}
			case datapb.ImportTaskStateV2_InProgress:
				switch task.GetType() {
				case PreImportTaskType:
					s.processInProgressPreImport(task)
				case ImportTaskType:
					s.processInProgressImport(task)
				}
			case datapb.ImportTaskStateV2_Completed:
				s.processCompleted(task)
			case datapb.ImportTaskStateV2_Failed:
				s.processFailed(task)
			}
		}
	}
}

func (s *importScheduler) peekSlots() map[int64]int64 {
	nodeIDs := lo.Map(s.cluster.GetSessions(), func(s *Session, _ int) int64 {
		return s.info.NodeID
	})
	nodeSlots := make(map[int64]int64)
	mu := &lock.Mutex{}
	wg := &sync.WaitGroup{}
	for _, nodeID := range nodeIDs {
		wg.Add(1)
		go func(nodeID int64) {
			defer wg.Done()
			resp, err := s.cluster.QueryImport(nodeID, &datapb.QueryImportRequest{QuerySlot: true})
			if err != nil {
				log.Warn("query import failed", zap.Error(err))
				return
			}
			mu.Lock()
			defer mu.Unlock()
			nodeSlots[nodeID] = resp.GetSlots()
		}(nodeID)
	}
	wg.Wait()
	log.Debug("peek slots done", zap.Any("nodeSlots", nodeSlots))
	return nodeSlots
}

func (s *importScheduler) getNodeID(task ImportTask, nodeSlots map[int64]int64) int64 {
	var (
		nodeID   int64 = NullNodeID
		maxSlots int64 = -1
	)
	require := task.GetSlots()
	for id, slots := range nodeSlots {
		// find the most idle datanode
		if slots > 0 && slots >= require && slots > maxSlots {
			nodeID = id
			maxSlots = slots
		}
	}
	if nodeID != NullNodeID {
		nodeSlots[nodeID] -= require
	}
	return nodeID
}

func (s *importScheduler) processPendingPreImport(task ImportTask, nodeID int64) {
	if nodeID == NullNodeID {
		return
	}
	log.Info("processing pending preimport task...", WrapTaskLog(task)...)
	job := s.imeta.GetJob(task.GetJobID())
	req := AssemblePreImportRequest(task, job)
	err := s.cluster.PreImport(nodeID, req)
	if err != nil {
		log.Warn("preimport failed", WrapTaskLog(task, zap.Error(err))...)
		return
	}
	err = s.imeta.UpdateTask(task.GetTaskID(),
		UpdateState(datapb.ImportTaskStateV2_InProgress),
		UpdateNodeID(nodeID))
	if err != nil {
		log.Warn("update import task failed", WrapTaskLog(task, zap.Error(err))...)
		return
	}
	pendingDuration := task.GetTR().RecordSpan()
	metrics.ImportTaskLatency.WithLabelValues(metrics.ImportStagePending).Observe(float64(pendingDuration.Milliseconds()))
	log.Info("preimport task start to execute", WrapTaskLog(task, zap.Int64("scheduledNodeID", nodeID), zap.Duration("taskTimeCost/pending", pendingDuration))...)
}

func (s *importScheduler) processPendingImport(task ImportTask, nodeID int64) {
	if nodeID == NullNodeID {
		return
	}
	log.Info("processing pending import task...", WrapTaskLog(task)...)
	job := s.imeta.GetJob(task.GetJobID())
	req, err := AssembleImportRequest(task, job, s.meta, s.alloc)
	if err != nil {
		log.Warn("assemble import request failed", WrapTaskLog(task, zap.Error(err))...)
		return
	}
	err = s.cluster.ImportV2(nodeID, req)
	if err != nil {
		log.Warn("import failed", WrapTaskLog(task, zap.Error(err))...)
		return
	}
	err = s.imeta.UpdateTask(task.GetTaskID(),
		UpdateState(datapb.ImportTaskStateV2_InProgress),
		UpdateNodeID(nodeID))
	if err != nil {
		log.Warn("update import task failed", WrapTaskLog(task, zap.Error(err))...)
		return
	}
	pendingDuration := task.GetTR().RecordSpan()
	metrics.ImportTaskLatency.WithLabelValues(metrics.ImportStagePending).Observe(float64(pendingDuration.Milliseconds()))
	log.Info("import task start to execute", WrapTaskLog(task, zap.Int64("scheduledNodeID", nodeID), zap.Duration("taskTimeCost/pending", pendingDuration))...)
}

func (s *importScheduler) processInProgressPreImport(task ImportTask) {
	req := &datapb.QueryPreImportRequest{
		JobID:  task.GetJobID(),
		TaskID: task.GetTaskID(),
	}
	resp, err := s.cluster.QueryPreImport(task.GetNodeID(), req)
	if err != nil {
		updateErr := s.imeta.UpdateTask(task.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Pending))
		if updateErr != nil {
			log.Warn("failed to update preimport task state to pending", WrapTaskLog(task, zap.Error(updateErr))...)
		}
		log.Info("reset preimport task state to pending due to error occurs", WrapTaskLog(task, zap.Error(err))...)
		return
	}
	if resp.GetState() == datapb.ImportTaskStateV2_Failed {
		err = s.imeta.UpdateJob(task.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed),
			UpdateJobReason(resp.GetReason()))
		if err != nil {
			log.Warn("failed to update job state to Failed", zap.Int64("jobID", task.GetJobID()), zap.Error(err))
		}
		log.Warn("preimport failed", WrapTaskLog(task, zap.String("reason", resp.GetReason()))...)
		return
	}
	actions := []UpdateAction{UpdateFileStats(resp.GetFileStats())}
	if resp.GetState() == datapb.ImportTaskStateV2_Completed {
		actions = append(actions, UpdateState(datapb.ImportTaskStateV2_Completed))
	}
	err = s.imeta.UpdateTask(task.GetTaskID(), actions...)
	if err != nil {
		log.Warn("update preimport task failed", WrapTaskLog(task, zap.Error(err))...)
		return
	}
	log.Info("query preimport", WrapTaskLog(task, zap.String("state", resp.GetState().String()),
		zap.Any("fileStats", resp.GetFileStats()))...)
	if resp.GetState() == datapb.ImportTaskStateV2_Completed {
		preimportDuration := task.GetTR().RecordSpan()
		metrics.ImportTaskLatency.WithLabelValues(metrics.ImportStagePreImport).Observe(float64(preimportDuration.Milliseconds()))
		log.Info("preimport done", WrapTaskLog(task, zap.Duration("taskTimeCost/preimport", preimportDuration))...)
	}
}

func (s *importScheduler) processInProgressImport(task ImportTask) {
	req := &datapb.QueryImportRequest{
		JobID:  task.GetJobID(),
		TaskID: task.GetTaskID(),
	}
	resp, err := s.cluster.QueryImport(task.GetNodeID(), req)
	if err != nil {
		updateErr := s.imeta.UpdateTask(task.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Pending))
		if updateErr != nil {
			log.Warn("failed to update import task state to pending", WrapTaskLog(task, zap.Error(updateErr))...)
		}
		log.Info("reset import task state to pending due to error occurs", WrapTaskLog(task, zap.Error(err))...)
		return
	}
	if resp.GetState() == datapb.ImportTaskStateV2_Failed {
		err = s.imeta.UpdateJob(task.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(resp.GetReason()))
		if err != nil {
			log.Warn("failed to update job state to Failed", zap.Int64("jobID", task.GetJobID()), zap.Error(err))
		}
		log.Warn("import failed", WrapTaskLog(task, zap.String("reason", resp.GetReason()))...)
		return
	}

	collInfo := s.meta.GetCollection(task.GetCollectionID())
	dbName := ""
	if collInfo != nil {
		dbName = collInfo.DatabaseName
	}

	for _, info := range resp.GetImportSegmentsInfo() {
		segment := s.meta.GetSegment(info.GetSegmentID())
		if info.GetImportedRows() <= segment.GetNumOfRows() {
			continue // rows not changed, no need to update
		}
		diff := info.GetImportedRows() - segment.GetNumOfRows()
		op := UpdateImportedRows(info.GetSegmentID(), info.GetImportedRows())
		err = s.meta.UpdateSegmentsInfo(op)
		if err != nil {
			log.Warn("update import segment rows failed", WrapTaskLog(task, zap.Error(err))...)
			return
		}

		metrics.DataCoordBulkVectors.WithLabelValues(
			dbName,
			strconv.FormatInt(task.GetCollectionID(), 10),
		).Add(float64(diff))
	}
	if resp.GetState() == datapb.ImportTaskStateV2_Completed {
		for _, info := range resp.GetImportSegmentsInfo() {
			// try to parse path and fill logID
			err = binlog.CompressBinLogs(info.GetBinlogs(), info.GetDeltalogs(), info.GetStatslogs())
			if err != nil {
				log.Warn("fail to CompressBinLogs for import binlogs",
					WrapTaskLog(task, zap.Int64("segmentID", info.GetSegmentID()), zap.Error(err))...)
				return
			}
			op1 := UpdateBinlogsOperator(info.GetSegmentID(), info.GetBinlogs(), info.GetStatslogs(), info.GetDeltalogs())
			op2 := UpdateStatusOperator(info.GetSegmentID(), commonpb.SegmentState_Flushed)
			err = s.meta.UpdateSegmentsInfo(op1, op2)
			if err != nil {
				updateErr := s.imeta.UpdateJob(task.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(err.Error()))
				if updateErr != nil {
					log.Warn("failed to update job state to Failed", zap.Int64("jobID", task.GetJobID()), zap.Error(updateErr))
				}
				log.Warn("update import segment binlogs failed", WrapTaskLog(task, zap.String("err", err.Error()))...)
				return
			}
			select {
			case s.buildIndexCh <- info.GetSegmentID(): // accelerate index building:
			default:
			}
		}
		completeTime := time.Now().Format("2006-01-02T15:04:05Z07:00")
		err = s.imeta.UpdateTask(task.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Completed), UpdateCompleteTime(completeTime))
		if err != nil {
			log.Warn("update import task failed", WrapTaskLog(task, zap.Error(err))...)
			return
		}
		importDuration := task.GetTR().RecordSpan()
		metrics.ImportTaskLatency.WithLabelValues(metrics.ImportStageImport).Observe(float64(importDuration.Milliseconds()))
		log.Info("import done", WrapTaskLog(task, zap.Duration("taskTimeCost/import", importDuration))...)
	}
	log.Info("query import", WrapTaskLog(task, zap.String("state", resp.GetState().String()),
		zap.String("reason", resp.GetReason()))...)
}

func (s *importScheduler) processCompleted(task ImportTask) {
	err := DropImportTask(task, s.cluster, s.imeta)
	if err != nil {
		log.Warn("drop import failed", WrapTaskLog(task, zap.Error(err))...)
	}
}

func (s *importScheduler) processFailed(task ImportTask) {
	if task.GetType() == ImportTaskType {
		segments := task.(*importTask).GetSegmentIDs()
		for _, segment := range segments {
			op := UpdateStatusOperator(segment, commonpb.SegmentState_Dropped)
			err := s.meta.UpdateSegmentsInfo(op)
			if err != nil {
				log.Warn("drop import segment failed", WrapTaskLog(task, zap.Int64("segment", segment), zap.Error(err))...)
				return
			}
		}
		if len(segments) > 0 {
			err := s.imeta.UpdateTask(task.GetTaskID(), UpdateSegmentIDs(nil))
			if err != nil {
				log.Warn("update import task segments failed", WrapTaskLog(task, zap.Error(err))...)
			}
		}
	}
	err := DropImportTask(task, s.cluster, s.imeta)
	if err != nil {
		log.Warn("drop import failed", WrapTaskLog(task, zap.Error(err))...)
	}
}
