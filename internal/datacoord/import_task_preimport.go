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

var _ ImportTask = (*preImportTask)(nil)

type preImportTask struct {
	task atomic.Pointer[datapb.PreImportTask]

	importMeta ImportMeta
	tr         *timerecord.TimeRecorder
	times      *taskcommon.Times
	retryTimes int64
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
	return int64(CalculateTaskSlot(p, p.importMeta))
}

func (p *preImportTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	p.times.SetTaskTime(timeType, time)
}

func (p *preImportTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(p.times)
}

func (p *preImportTask) GetTaskVersion() int64 {
	return p.retryTimes
}

func (p *preImportTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	mlog.Info(context.TODO(), "processing pending preimport task...", WrapTaskLog(p)...)
	job := p.importMeta.GetJob(context.TODO(), p.GetJobID())
	req := AssemblePreImportRequest(p, job)

	err := cluster.CreatePreImport(nodeID, req, p.GetTaskSlot())
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
	metrics.ImportTaskLatency.WithLabelValues(metrics.ImportStagePending).Observe(float64(pendingDuration.Milliseconds()))
	mlog.Info(context.TODO(), "preimport task start to execute", WrapTaskLog(p, mlog.Int64("scheduledNodeID", nodeID), mlog.Duration("taskTimeCost/pending", pendingDuration))...)
}

func (p *preImportTask) QueryTaskOnWorker(cluster session.Cluster) {
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
		metrics.ImportTaskLatency.WithLabelValues(metrics.ImportStagePreImport).Observe(float64(preimportDuration.Milliseconds()))
		mlog.Info(context.TODO(), "preimport done", WrapTaskLog(p, mlog.Duration("taskTimeCost/preimport", preimportDuration))...)
	}
}

func (p *preImportTask) DropTaskOnWorker(cluster session.Cluster) {
	err := DropImportTask(p, cluster, p.importMeta)
	if err != nil {
		mlog.Warn(context.TODO(), "drop import failed", WrapTaskLog(p, mlog.Err(err))...)
		return
	}
	mlog.Info(context.TODO(), "drop preimport task done", WrapTaskLog(p, mlog.FieldNodeID(p.GetNodeID()))...)
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
