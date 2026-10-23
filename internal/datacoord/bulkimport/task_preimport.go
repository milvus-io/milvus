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

package bulkimport

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ ImportTask = (*PreImportTask)(nil)

type PreImportTask struct {
	Task atomic.Pointer[datapb.PreImportTask]

	importMeta ImportMeta
	Tr         *timerecord.TimeRecorder
	times      *taskcommon.Times
	retryTimes int64
}

func (p *PreImportTask) GetJobID() int64 {
	return p.Task.Load().GetJobID()
}

func (p *PreImportTask) GetTaskID() int64 {
	return p.Task.Load().GetTaskID()
}

func (p *PreImportTask) GetCollectionID() int64 {
	return p.Task.Load().GetCollectionID()
}

func (p *PreImportTask) GetNodeID() int64 {
	return p.Task.Load().GetNodeID()
}

func (p *PreImportTask) GetState() datapb.ImportTaskStateV2 {
	return p.Task.Load().GetState()
}

func (p *PreImportTask) GetReason() string {
	return p.Task.Load().GetReason()
}

func (p *PreImportTask) GetFileStats() []*datapb.ImportFileStats {
	return p.Task.Load().GetFileStats()
}

func (p *PreImportTask) GetCreatedTime() string {
	return p.Task.Load().GetCreatedTime()
}

func (p *PreImportTask) GetCompleteTime() string {
	return p.Task.Load().GetCompleteTime()
}

func (p *PreImportTask) GetTaskType() taskcommon.Type {
	return taskcommon.PreImport
}

func (p *PreImportTask) GetTaskState() taskcommon.State {
	return taskcommon.FromImportState(p.GetState())
}

func (p *PreImportTask) GetTaskSlot() int64 {
	return int64(CalculateTaskSlot(p, p.importMeta))
}

func (p *PreImportTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	p.times.SetTaskTime(timeType, time)
}

func (p *PreImportTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(p.times)
}

func (p *PreImportTask) GetTaskVersion() int64 {
	return p.retryTimes
}

func (p *PreImportTask) setState(state datapb.ImportTaskStateV2) {
	p.Task.Load().State = state
}

func (p *PreImportTask) setReason(reason string) {
	p.Task.Load().Reason = reason
}

func (p *PreImportTask) setCompleteTime(completeTime string) {
	p.Task.Load().CompleteTime = completeTime
}

func (p *PreImportTask) setNodeID(nodeID int64) {
	p.Task.Load().NodeID = nodeID
}

func (p *PreImportTask) setFileStats(fileStats []*datapb.ImportFileStats) {
	p.Task.Load().FileStats = fileStats
}

func (p *PreImportTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	mlog.Info(context.TODO(), "processing pending preimport task...", WrapTaskLog(p)...)
	job := p.importMeta.GetJob(context.TODO(), p.GetJobID())
	req, err := AssemblePreImportRequest(p, job)
	if err != nil {
		// Source inventories are immutable. Retrying the same malformed task
		// cannot restore its missing scope or deletes.
		if updateErr := p.importMeta.UpdateJob(context.TODO(), p.GetJobID(),
			UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(err.Error())); updateErr != nil {
			mlog.Warn(context.TODO(), "failed to mark preimport job failed after assemble error", WrapTaskLog(p, mlog.Err(updateErr))...)
		}
		return
	}

	err = cluster.CreatePreImport(nodeID, req, p.GetTaskSlot())
	if err != nil {
		mlog.Warn(context.TODO(), "preimport failed", WrapTaskLog(p, mlog.Err(err))...)
		p.retryTimes++
		return
	}
	err = p.importMeta.UpdateTask(context.TODO(), p.GetTaskID(),
		UpdateState(datapb.ImportTaskStateV2_InProgress),
		UpdateNodeID(nodeID))
	if err != nil {
		mlog.Warn(context.TODO(), "update import task failed", WrapTaskLog(p, mlog.Err(err))...)
		return
	}
	pendingDuration := p.GetTR().RecordSpan()
	metrics.ImportTaskLatency.WithLabelValues(metrics.ImportStagePending, p.GetType().String()).Observe(float64(pendingDuration.Milliseconds()))
	mlog.Info(context.TODO(), "preimport task start to execute", WrapTaskLog(p, mlog.Int64("scheduledNodeID", nodeID), mlog.Duration("taskTimeCost/pending", pendingDuration))...)
}

func (p *PreImportTask) QueryTaskOnWorker(cluster session.Cluster) {
	req := &datapb.QueryPreImportRequest{
		JobID:  p.GetJobID(),
		TaskID: p.GetTaskID(),
	}
	resp, err := cluster.QueryPreImport(p.GetNodeID(), req)
	if err != nil || resp.GetState() == datapb.ImportTaskStateV2_Retry {
		updateErr := p.importMeta.UpdateTask(context.TODO(), p.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Pending))
		if updateErr != nil {
			mlog.Warn(context.TODO(), "failed to update preimport task state to pending", WrapTaskLog(p, mlog.Err(updateErr))...)
		}
		mlog.Info(context.TODO(), "reset preimport task state to pending due to error occurs", WrapTaskLog(p, mlog.Err(err), mlog.String("reason", resp.GetReason()))...)
		return
	}
	if resp.GetState() == datapb.ImportTaskStateV2_Failed {
		err = p.importMeta.UpdateJob(context.TODO(), p.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed),
			UpdateJobReason(resp.GetReason()))
		if err != nil {
			mlog.Warn(context.TODO(), "failed to update job state to Failed", mlog.FieldJobID(p.GetJobID()), mlog.Err(err))
		}
		mlog.Warn(context.TODO(), "preimport failed", WrapTaskLog(p, mlog.String("reason", resp.GetReason()))...)
		return
	}
	actions := []UpdateAction{}
	if resp.GetState() == datapb.ImportTaskStateV2_InProgress {
		if resp.GetFileStats() == nil {
			return
		}
		actions = append(actions, UpdateFileStats(resp.GetFileStats()))
	}
	if resp.GetState() == datapb.ImportTaskStateV2_Completed {
		actions = append(actions, UpdateFileStats(resp.GetFileStats()))
		actions = append(actions, UpdateState(datapb.ImportTaskStateV2_Completed))
	}
	if len(actions) > 0 {
		err = p.importMeta.UpdateTask(context.TODO(), p.GetTaskID(), actions...)
		if err != nil {
			mlog.Warn(context.TODO(), "update preimport task failed", WrapTaskLog(p, mlog.Err(err))...)
			return
		}
	}
	mlog.Info(context.TODO(), "query preimport", WrapTaskLog(p, mlog.String("respState", resp.GetState().String()),
		mlog.Any("fileStats", resp.GetFileStats()))...)
	if resp.GetState() == datapb.ImportTaskStateV2_Completed {
		preimportDuration := p.GetTR().RecordSpan()
		metrics.ImportTaskLatency.WithLabelValues(metrics.ImportStagePreImport, p.GetType().String()).Observe(float64(preimportDuration.Milliseconds()))
		mlog.Info(context.TODO(), "preimport done", WrapTaskLog(p, mlog.Duration("taskTimeCost/preimport", preimportDuration))...)
	}
}

func (p *PreImportTask) DropTaskOnWorker(cluster session.Cluster) {
	err := DropImportTask(p, cluster, p.importMeta)
	if err != nil {
		mlog.Warn(context.TODO(), "drop import failed", WrapTaskLog(p, mlog.Err(err))...)
		return
	}
	mlog.Info(context.TODO(), "drop preimport task done", WrapTaskLog(p, mlog.FieldNodeID(p.GetNodeID()))...)
}

func (p *PreImportTask) GetType() TaskType {
	return PreImportTaskType
}

func (p *PreImportTask) GetTR() *timerecord.TimeRecorder {
	return p.Tr
}

func (p *PreImportTask) Clone() ImportTask {
	cloned := &PreImportTask{
		importMeta: p.importMeta,
		Tr:         p.Tr,
		times:      p.times,
	}
	cloned.Task.Store(typeutil.Clone(p.Task.Load()))
	return cloned
}

func (p *PreImportTask) GetSource() datapb.ImportTaskSourceV2 {
	return datapb.ImportTaskSourceV2_Request
}

func (p *PreImportTask) MarshalJSON() ([]byte, error) {
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
