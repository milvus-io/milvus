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
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/json"
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

var _ ImportTask = (*preImportTask)(nil)

type preImportTask struct {
	task atomic.Pointer[datapb.PreImportTask]

	importMeta ImportMeta
	tr         *timerecord.TimeRecorder
	times      *taskcommon.Times
}

func (p *preImportTask) GetJobID() int64 {
	return p.task.Load().GetJobID()
}

func (p *preImportTask) GetTaskID() int64 {
	return p.task.Load().GetTaskID()
}

func (p *preImportTask) GetCollectionID() int64 {
	return p.task.Load().GetCollectionID()
}

func (p *preImportTask) GetNodeID() int64 {
	return p.task.Load().GetNodeID()
}

func (p *preImportTask) GetState() datapb.ImportTaskStateV2 {
	return p.task.Load().GetState()
}

func (p *preImportTask) GetReason() string {
	return p.task.Load().GetReason()
}

func (p *preImportTask) GetFileStats() []*datapb.ImportFileStats {
	return p.task.Load().GetFileStats()
}

func (p *preImportTask) GetCreatedTime() string {
	return p.task.Load().GetCreatedTime()
}

func (p *preImportTask) GetCompleteTime() string {
	return p.task.Load().GetCompleteTime()
}

func (p *preImportTask) GetTaskType() taskcommon.Type {
	return taskcommon.PreImport
}

func (p *preImportTask) GetTaskState() taskcommon.State {
	return taskcommon.FromImportState(p.GetState())
}

func (p *preImportTask) GetTaskSlot() int64 {
	return int64(funcutil.Min(len(p.GetFileStats())))
}

func (p *preImportTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	p.times.SetTaskTime(timeType, time)
}

func (p *preImportTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(p.times)
}

func (p *preImportTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	log.Info("processing pending preimport task...", WrapTaskLog(p)...)
	job := p.importMeta.GetJob(context.TODO(), p.GetJobID())
	req := AssemblePreImportRequest(p, job)

	err := cluster.CreatePreImport(nodeID, req, p.GetTaskSlot())
	if err != nil {
		log.Warn("preimport failed", WrapTaskLog(p, zap.Error(err))...)
		return
	}
	err = p.importMeta.UpdateTask(context.TODO(), p.GetTaskID(),
		UpdateState(datapb.ImportTaskStateV2_InProgress),
		UpdateNodeID(nodeID))
	if err != nil {
		log.Warn("update import task failed", WrapTaskLog(p, zap.Error(err))...)
		return
	}
	pendingDuration := p.GetTR().RecordSpan()
	metrics.ImportTaskLatency.WithLabelValues(metrics.ImportStagePending).Observe(float64(pendingDuration.Milliseconds()))
	log.Info("preimport task start to execute", WrapTaskLog(p, zap.Int64("scheduledNodeID", nodeID), zap.Duration("taskTimeCost/pending", pendingDuration))...)
}

func (p *preImportTask) QueryTaskOnWorker(cluster session.Cluster) {
	req := &datapb.QueryPreImportRequest{
		JobID:  p.GetJobID(),
		TaskID: p.GetTaskID(),
	}
	resp, err := cluster.QueryPreImport(p.GetNodeID(), req)
	if err != nil {
		updateErr := p.importMeta.UpdateTask(context.TODO(), p.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Pending))
		if updateErr != nil {
			log.Warn("failed to update preimport task state to pending", WrapTaskLog(p, zap.Error(updateErr))...)
		}
		log.Info("reset preimport task state to pending due to error occurs", WrapTaskLog(p, zap.Error(err))...)
		return
	}
	if resp.GetState() == datapb.ImportTaskStateV2_Failed {
		err = p.importMeta.UpdateJob(context.TODO(), p.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed),
			UpdateJobReason(resp.GetReason()))
		if err != nil {
			log.Warn("failed to update job state to Failed", zap.Int64("jobID", p.GetJobID()), zap.Error(err))
		}
		log.Warn("preimport failed", WrapTaskLog(p, zap.String("reason", resp.GetReason()))...)
		return
	}
	actions := []UpdateAction{UpdateFileStats(resp.GetFileStats())}
	if resp.GetState() == datapb.ImportTaskStateV2_Completed {
		actions = append(actions, UpdateState(datapb.ImportTaskStateV2_Completed))
	}
	err = p.importMeta.UpdateTask(context.TODO(), p.GetTaskID(), actions...)
	if err != nil {
		log.Warn("update preimport task failed", WrapTaskLog(p, zap.Error(err))...)
		return
	}
	log.Info("query preimport", WrapTaskLog(p, zap.String("respState", resp.GetState().String()),
		zap.Any("fileStats", resp.GetFileStats()))...)
	if resp.GetState() == datapb.ImportTaskStateV2_Completed {
		preimportDuration := p.GetTR().RecordSpan()
		metrics.ImportTaskLatency.WithLabelValues(metrics.ImportStagePreImport).Observe(float64(preimportDuration.Milliseconds()))
		log.Info("preimport done", WrapTaskLog(p, zap.Duration("taskTimeCost/preimport", preimportDuration))...)
	}
}

func (p *preImportTask) DropTaskOnWorker(cluster session.Cluster) {
	err := DropImportTask(p, cluster, p.importMeta)
	if err != nil {
		log.Warn("drop import failed", WrapTaskLog(p, zap.Error(err))...)
		return
	}
	log.Info("drop preimport task done", WrapTaskLog(p, zap.Int64("nodeID", p.GetNodeID()))...)
}

func (p *preImportTask) GetType() TaskType {
	return PreImportTaskType
}

func (p *preImportTask) GetTR() *timerecord.TimeRecorder {
	return p.tr
}

func (p *preImportTask) Clone() ImportTask {
	cloned := &preImportTask{
		importMeta: p.importMeta,
		tr:         p.tr,
		times:      p.times,
	}
	cloned.task.Store(typeutil.Clone(p.task.Load()))
	return cloned
}

func (p *preImportTask) GetSource() datapb.ImportTaskSourceV2 {
	return datapb.ImportTaskSourceV2_Request
}

func (p *preImportTask) MarshalJSON() ([]byte, error) {
	importTask := metricsinfo.ImportTask{
		JobID:        p.GetJobID(),
		TaskID:       p.GetTaskID(),
		CollectionID: p.GetCollectionID(),
		NodeID:       p.GetNodeID(),
		State:        p.GetState().String(),
		Reason:       p.GetReason(),
		TaskType:     p.GetType().String(),
		CreatedTime:  p.GetCreatedTime(),
		CompleteTime: p.GetCompleteTime(),
	}
	return json.Marshal(importTask)
}
