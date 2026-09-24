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

var _ ImportTask = (*preImportV2Task)(nil)

// preImportV2Task is the Import V3 count-only preimport control record. It runs
// before the reshard stage so the primary can allocate exact per-file ID ranges
// from the exact row counts, instead of sizing files at broadcast. It does no
// hashing and carries no hashed bucket stats.
type preImportV2Task struct {
	task atomic.Pointer[datapb.PreImportV2Task]

	importMeta ImportMeta
	tr         *timerecord.TimeRecorder
	times      *taskcommon.Times
	retryTimes int64
}

func newPreImportV2Task(p *datapb.PreImportV2Task, importMeta ImportMeta) *preImportV2Task {
	t := &preImportV2Task{
		importMeta: importMeta,
		tr:         timerecord.NewTimeRecorder("preimport v2 task"),
		times:      taskcommon.NewTimes(),
	}
	t.task.Store(p)
	return t
}

func (p *preImportV2Task) GetJobID() int64 {
	return p.task.Load().GetJobID()
}

func (p *preImportV2Task) GetTaskID() int64 {
	return p.task.Load().GetTaskID()
}

func (p *preImportV2Task) GetCollectionID() int64 {
	return p.task.Load().GetCollectionID()
}

func (p *preImportV2Task) GetNodeID() int64 {
	return p.task.Load().GetNodeID()
}

func (p *preImportV2Task) GetState() datapb.ImportTaskStateV2 {
	return p.task.Load().GetState()
}

func (p *preImportV2Task) GetReason() string {
	return p.task.Load().GetReason()
}

func (p *preImportV2Task) GetFileStats() []*datapb.ImportFileStats {
	return p.task.Load().GetFileStats()
}

func (p *preImportV2Task) GetCreatedTime() string {
	return p.task.Load().GetCreatedTime()
}

func (p *preImportV2Task) GetCompleteTime() string {
	return p.task.Load().GetCompleteTime()
}

func (p *preImportV2Task) GetTaskType() taskcommon.Type {
	return taskcommon.PreImportV2
}

func (p *preImportV2Task) GetTaskState() taskcommon.State {
	return taskcommon.FromImportState(p.GetState())
}

func (p *preImportV2Task) GetTaskSlot() int64 {
	return int64(CalculateTaskSlot(p, p.importMeta))
}

func (p *preImportV2Task) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	p.times.SetTaskTime(timeType, time)
}

func (p *preImportV2Task) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(p.times)
}

func (p *preImportV2Task) GetTaskVersion() int64 {
	return p.retryTimes
}

func (p *preImportV2Task) setState(state datapb.ImportTaskStateV2) {
	p.task.Load().State = state
}

func (p *preImportV2Task) setReason(reason string) {
	p.task.Load().Reason = reason
}

func (p *preImportV2Task) setCompleteTime(completeTime string) {
	p.task.Load().CompleteTime = completeTime
}

func (p *preImportV2Task) setNodeID(nodeID int64) {
	p.task.Load().NodeID = nodeID
}

func (p *preImportV2Task) setFileStats(fileStats []*datapb.ImportFileStats) {
	p.task.Load().FileStats = fileStats
}

func (p *preImportV2Task) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	mlog.Info(context.TODO(), "processing pending preimport v2 task...", WrapTaskLog(p)...)
	job := p.importMeta.GetJob(context.TODO(), p.GetJobID())
	req := AssemblePreImportRequest(p, job)

	err := cluster.CreatePreImportV2(nodeID, req, p.GetTaskSlot())
	if err != nil {
		mlog.Warn(context.TODO(), "preimport v2 failed", WrapTaskLog(p, mlog.Err(err))...)
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
	mlog.Info(context.TODO(), "preimport v2 task start to execute", WrapTaskLog(p, mlog.Int64("scheduledNodeID", nodeID), mlog.Duration("taskTimeCost/pending", pendingDuration))...)
}

func (p *preImportV2Task) QueryTaskOnWorker(cluster session.Cluster) {
	req := &datapb.QueryPreImportRequest{
		JobID:  p.GetJobID(),
		TaskID: p.GetTaskID(),
	}
	resp, err := cluster.QueryPreImportV2(p.GetNodeID(), req)
	if err != nil || resp.GetState() == datapb.ImportTaskStateV2_Retry {
		updateErr := p.importMeta.UpdateTask(context.TODO(), p.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Pending))
		if updateErr != nil {
			mlog.Warn(context.TODO(), "failed to update preimport v2 task state to pending", WrapTaskLog(p, mlog.Err(updateErr))...)
		}
		mlog.Info(context.TODO(), "reset preimport v2 task state to pending due to error occurs", WrapTaskLog(p, mlog.Err(err), mlog.String("reason", resp.GetReason()))...)
		return
	}
	if resp.GetState() == datapb.ImportTaskStateV2_Failed {
		err = p.importMeta.UpdateJob(context.TODO(), p.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed),
			UpdateJobReason(resp.GetReason()))
		if err != nil {
			mlog.Warn(context.TODO(), "failed to update job state to Failed", mlog.FieldJobID(p.GetJobID()), mlog.Err(err))
		}
		mlog.Warn(context.TODO(), "preimport v2 failed", WrapTaskLog(p, mlog.String("reason", resp.GetReason()))...)
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
			mlog.Warn(context.TODO(), "update preimport v2 task failed", WrapTaskLog(p, mlog.Err(err))...)
			return
		}
	}
	mlog.Info(context.TODO(), "query preimport v2", WrapTaskLog(p, mlog.String("respState", resp.GetState().String()),
		mlog.Any("fileStats", resp.GetFileStats()))...)
	if resp.GetState() == datapb.ImportTaskStateV2_Completed {
		preimportDuration := p.GetTR().RecordSpan()
		metrics.ImportTaskLatency.WithLabelValues(metrics.ImportStagePreImport, p.GetType().String()).Observe(float64(preimportDuration.Milliseconds()))
		mlog.Info(context.TODO(), "preimport v2 done", WrapTaskLog(p, mlog.Duration("taskTimeCost/preimport", preimportDuration))...)
	}
}

func (p *preImportV2Task) DropTaskOnWorker(cluster session.Cluster) {
	if p.GetNodeID() == NullNodeID {
		return
	}
	err := cluster.DropPreImportV2(p.GetNodeID(), p.GetTaskID())
	if err != nil {
		mlog.Warn(context.TODO(), "drop preimport v2 failed", WrapTaskLog(p, mlog.Err(err))...)
		return
	}
	_ = p.importMeta.UpdateTask(context.TODO(), p.GetTaskID(), UpdateNodeID(NullNodeID))
}

func (p *preImportV2Task) GetType() TaskType {
	return PreImportV2TaskType
}

func (p *preImportV2Task) GetTR() *timerecord.TimeRecorder {
	return p.tr
}

func (p *preImportV2Task) Clone() ImportTask {
	cloned := &preImportV2Task{
		importMeta: p.importMeta,
		tr:         p.tr,
		times:      p.times,
	}
	cloned.task.Store(typeutil.Clone(p.task.Load()))
	return cloned
}

func (p *preImportV2Task) GetSource() datapb.ImportTaskSourceV2 {
	return datapb.ImportTaskSourceV2_Request
}

func (p *preImportV2Task) MarshalJSON() ([]byte, error) {
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
