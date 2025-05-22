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
	"strconv"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ ImportTask = (*importTask)(nil)

type importTask struct {
	task atomic.Pointer[datapb.ImportTaskV2]

	alloc      allocator.Allocator
	meta       *meta
	importMeta ImportMeta
	tr         *timerecord.TimeRecorder
	times      *taskcommon.Times
}

func (t *importTask) GetJobID() int64 {
	return t.task.Load().GetJobID()
}

func (t *importTask) GetTaskID() int64 {
	return t.task.Load().GetTaskID()
}

func (t *importTask) GetCollectionID() int64 {
	return t.task.Load().GetCollectionID()
}

func (t *importTask) GetNodeID() int64 {
	return t.task.Load().GetNodeID()
}

func (t *importTask) GetState() datapb.ImportTaskStateV2 {
	return t.task.Load().GetState()
}

func (t *importTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	t.times.SetTaskTime(timeType, time)
}

func (t *importTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(t.times)
}

func (t *importTask) GetReason() string {
	return t.task.Load().GetReason()
}

func (t *importTask) GetFileStats() []*datapb.ImportFileStats {
	return t.task.Load().GetFileStats()
}

func (t *importTask) GetSegmentIDs() []int64 {
	return t.task.Load().GetSegmentIDs()
}

func (t *importTask) GetStatsSegmentIDs() []int64 {
	return t.task.Load().GetStatsSegmentIDs()
}

func (t *importTask) GetSource() datapb.ImportTaskSourceV2 {
	return t.task.Load().GetSource()
}

func (t *importTask) GetCreatedTime() string {
	return t.task.Load().GetCreatedTime()
}

func (t *importTask) GetCompleteTime() string {
	return t.task.Load().GetCompleteTime()
}

func (t *importTask) GetTaskType() taskcommon.Type {
	return taskcommon.Import
}

func (t *importTask) GetTaskState() taskcommon.State {
	return taskcommon.FromImportState(t.GetState())
}

func (t *importTask) GetTaskNodeID() int64 {
	return t.GetNodeID()
}

func (t *importTask) GetTaskSlot() int64 {
	// Consider the following two scenarios:
	// 1. Importing a large number of small files results in
	//    a small total data size, making file count unsuitable as a slot number.
	// 2. Importing a file with many shards number results in many segments and a small total data size,
	//    making segment count unsuitable as a slot number.
	// Taking these factors into account, we've decided to use the
	// minimum value between segment count and file count as the slot number.
	return int64(funcutil.Min(len(t.GetFileStats()), len(t.GetSegmentIDs())))
}

func (t *importTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	log.Info("processing pending import task...", WrapTaskLog(t)...)
	job := t.importMeta.GetJob(context.TODO(), t.GetJobID())
	req, err := AssembleImportRequest(t, job, t.meta, t.alloc)
	if err != nil {
		log.Warn("assemble import request failed", WrapTaskLog(t, zap.Error(err))...)
		return
	}
	err = cluster.CreateImport(nodeID, req, t.GetTaskSlot())
	if err != nil {
		log.Warn("import failed", WrapTaskLog(t, zap.Error(err))...)
		return
	}
	err = t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(),
		UpdateState(datapb.ImportTaskStateV2_InProgress),
		UpdateNodeID(nodeID))
	if err != nil {
		log.Warn("update import task failed", WrapTaskLog(t, zap.Error(err))...)
		return
	}
	pendingDuration := t.GetTR().RecordSpan()
	metrics.ImportTaskLatency.WithLabelValues(metrics.ImportStagePending).Observe(float64(pendingDuration.Milliseconds()))
	log.Info("import task start to execute", WrapTaskLog(t, zap.Int64("scheduledNodeID", nodeID), zap.Duration("taskTimeCost/pending", pendingDuration))...)
}

func (t *importTask) QueryTaskOnWorker(cluster session.Cluster) {
	req := &datapb.QueryImportRequest{
		JobID:  t.GetJobID(),
		TaskID: t.GetTaskID(),
	}
	resp, err := cluster.QueryImport(t.GetNodeID(), req)
	if err != nil {
		updateErr := t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Pending))
		if updateErr != nil {
			log.Warn("failed to update import task state to pending", WrapTaskLog(t, zap.Error(updateErr))...)
		}
		log.Info("reset import task state to pending due to error occurs", WrapTaskLog(t, zap.Error(err))...)
		return
	}
	if resp.GetState() == datapb.ImportTaskStateV2_Failed {
		err = t.importMeta.UpdateJob(context.TODO(), t.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(resp.GetReason()))
		if err != nil {
			log.Warn("failed to update job state to Failed", zap.Int64("jobID", t.GetJobID()), zap.Error(err))
		}
		log.Warn("import failed", WrapTaskLog(t, zap.String("reason", resp.GetReason()))...)
		return
	}

	collInfo := t.meta.GetCollection(t.GetCollectionID())
	dbName := ""
	if collInfo != nil {
		dbName = collInfo.DatabaseName
	}

	for _, info := range resp.GetImportSegmentsInfo() {
		segment := t.meta.GetSegment(context.TODO(), info.GetSegmentID())
		if info.GetImportedRows() <= segment.GetNumOfRows() {
			continue // rows not changed, no need to update
		}
		diff := info.GetImportedRows() - segment.GetNumOfRows()
		op := UpdateImportedRows(info.GetSegmentID(), info.GetImportedRows())
		err = t.meta.UpdateSegmentsInfo(context.TODO(), op)
		if err != nil {
			log.Warn("update import segment rows failed", WrapTaskLog(t, zap.Error(err))...)
			return
		}

		metrics.DataCoordBulkVectors.WithLabelValues(
			dbName,
			strconv.FormatInt(t.GetCollectionID(), 10),
		).Add(float64(diff))
	}
	if resp.GetState() == datapb.ImportTaskStateV2_Completed {
		for _, info := range resp.GetImportSegmentsInfo() {
			// try to parse path and fill logID
			err = binlog.CompressBinLogs(info.GetBinlogs(), info.GetDeltalogs(), info.GetStatslogs(), info.GetBm25Logs())
			if err != nil {
				log.Warn("fail to CompressBinLogs for import binlogs",
					WrapTaskLog(t, zap.Int64("segmentID", info.GetSegmentID()), zap.Error(err))...)
				return
			}
			op1 := UpdateBinlogsOperator(info.GetSegmentID(), info.GetBinlogs(), info.GetStatslogs(), info.GetDeltalogs(), info.GetBm25Logs())
			op2 := UpdateStatusOperator(info.GetSegmentID(), commonpb.SegmentState_Flushed)
			err = t.meta.UpdateSegmentsInfo(context.TODO(), op1, op2)
			if err != nil {
				updateErr := t.importMeta.UpdateJob(context.TODO(), t.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(err.Error()))
				if updateErr != nil {
					log.Warn("failed to update job state to Failed", zap.Int64("jobID", t.GetJobID()), zap.Error(updateErr))
				}
				log.Warn("update import segment binlogs failed", WrapTaskLog(t, zap.String("err", err.Error()))...)
				return
			}
		}
		completeTime := time.Now().Format("2006-01-02T15:04:05Z07:00")
		err = t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Completed), UpdateCompleteTime(completeTime))
		if err != nil {
			log.Warn("update import task failed", WrapTaskLog(t, zap.Error(err))...)
			return
		}
		importDuration := t.GetTR().RecordSpan()
		metrics.ImportTaskLatency.WithLabelValues(metrics.ImportStageImport).Observe(float64(importDuration.Milliseconds()))
		log.Info("import done", WrapTaskLog(t, zap.Duration("taskTimeCost/import", importDuration))...)
	}
	log.Info("query import", WrapTaskLog(t, zap.String("respState", resp.GetState().String()),
		zap.String("reason", resp.GetReason()))...)
}

func (t *importTask) DropTaskOnWorker(cluster session.Cluster) {
	err := DropImportTask(t, cluster, t.importMeta)
	if err != nil {
		log.Warn("drop import failed", WrapTaskLog(t, zap.Error(err))...)
		return
	}
	log.Info("drop import task done", WrapTaskLog(t, zap.Int64("nodeID", t.GetNodeID()))...)
}

func (t *importTask) GetType() TaskType {
	return ImportTaskType
}

func (t *importTask) GetTR() *timerecord.TimeRecorder {
	return t.tr
}

func (t *importTask) Clone() ImportTask {
	cloned := &importTask{
		alloc:      t.alloc,
		meta:       t.meta,
		importMeta: t.importMeta,
		tr:         t.tr,
		times:      t.times,
	}
	cloned.task.Store(typeutil.Clone(t.task.Load()))
	return cloned
}

func (t *importTask) MarshalJSON() ([]byte, error) {
	importTask := metricsinfo.ImportTask{
		JobID:        t.GetJobID(),
		TaskID:       t.GetTaskID(),
		CollectionID: t.GetCollectionID(),
		NodeID:       t.GetNodeID(),
		State:        t.GetState().String(),
		Reason:       t.GetReason(),
		TaskType:     t.GetType().String(),
		CreatedTime:  t.GetCreatedTime(),
		CompleteTime: t.GetCompleteTime(),
	}
	return json.Marshal(importTask)
}
