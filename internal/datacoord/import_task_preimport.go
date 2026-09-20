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

	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
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

	alloc      allocator.Allocator
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
	return int64(CalculateTaskSlot(p, p.importMeta))
}

func (p *preImportTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	p.times.SetTaskTime(timeType, time)
}

func (p *preImportTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(p.times)
}

func (p *preImportTask) GetTaskVersion() int64 {
	return p.task.Load().GetTaskVersion()
}

func (p *preImportTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	ctx := context.TODO()
	mlog.Info(ctx, "processing pending preimport task...", WrapTaskLog(p)...)
	job := p.importMeta.GetJob(ctx, p.GetJobID())
	if p.importMeta.GetTask(ctx, p.GetTaskID()) == nil || job == nil ||
		job.GetState() == internalpb.ImportJobState_Failed ||
		job.GetState() == internalpb.ImportJobState_Completed {
		// GC may have finalized this scheduler identity after the inspector took
		// its enqueue snapshot. End the stale wrapper locally; there is no meta
		// left that could own a new worker attempt.
		local := typeutil.Clone(p.task.Load())
		local.State = datapb.ImportTaskStateV2_None
		p.task.Store(local)
		mlog.Info(ctx, "discarding stale preimport task before dispatch", WrapTaskLog(p)...)
		return
	}
	req := AssemblePreImportRequest(p, job)

	// Persist the assignment before crossing the at-least-once Create boundary.
	// Recording it afterwards loses the attempt whenever the worker accepted the
	// request but its response did not come back: the task stays Pending naming
	// nobody, so nothing can reclaim that attempt, and the scheduler hands the
	// same task ID to a second node while the first keeps running it. Failing to
	// write leaves the task Pending and undispatched, which is the safe side.
	err := p.importMeta.UpdateTask(context.TODO(), p.GetTaskID(),
		UpdateState(datapb.ImportTaskStateV2_InProgress),
		UpdateNodeID(nodeID))
	if err != nil {
		mlog.Warn(context.TODO(), "failed to persist preimport assignment, not sending task",
			WrapTaskLog(p, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
		return
	}
	err = cluster.CreatePreImport(nodeID, req, p.GetTaskSlot())
	if err != nil {
		mlog.Warn(context.TODO(), "preimport failed", WrapTaskLog(p, mlog.Err(err))...)
		// The Create outcome is ambiguous. Record retry debt and best-effort
		// clean up this attempt; the inspector publishes a fresh task ID.
		p.handoffRetry(cluster, err.Error())
		return
	}
	pendingDuration := p.GetTR().RecordSpan()
	metrics.ImportTaskLatency.WithLabelValues(metrics.ImportStagePending).Observe(float64(pendingDuration.Milliseconds()))
	mlog.Info(context.TODO(), "preimport task start to execute", WrapTaskLog(p, mlog.Int64("scheduledNodeID", nodeID), mlog.Duration("taskTimeCost/pending", pendingDuration))...)
}

func (p *preImportTask) QueryTaskOnWorker(cluster session.Cluster) {
	ctx := context.TODO()
	if p.importMeta.GetTask(ctx, p.GetTaskID()) == nil {
		// A stale inspector snapshot can be enqueued after terminal GC finalized
		// the old scheduler entry. Stop polling without recreating retry metadata.
		local := typeutil.Clone(p.task.Load())
		local.State = datapb.ImportTaskStateV2_None
		p.task.Store(local)
		mlog.Info(ctx, "discarding stale preimport task before query", WrapTaskLog(p)...)
		return
	}
	req := &datapb.QueryPreImportRequest{
		JobID:  p.GetJobID(),
		TaskID: p.GetTaskID(),
	}
	resp, err := cluster.QueryPreImport(p.GetNodeID(), req)
	if err != nil || resp.GetState() == datapb.ImportTaskStateV2_Retry ||
		resp.GetState() == datapb.ImportTaskStateV2_None {
		// Record retry debt and best-effort clean up this attempt. The inspector
		// rotates the task ID before dispatching the replacement.
		reason := ""
		if resp != nil {
			reason = resp.GetReason()
		}
		if err != nil {
			reason = err.Error()
		}
		p.handoffRetry(cluster, reason)
		fields := []mlog.Field{mlog.String("reason", reason)}
		if err != nil {
			fields = append(fields, mlog.Err(err))
		}
		mlog.Info(context.TODO(), "preimport attempt handed to business retry", WrapTaskLog(p, fields...)...)
		return
	}
	if resp.GetState() == datapb.ImportTaskStateV2_Failed {
		err = p.importMeta.UpdateJob(context.TODO(), p.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed),
			UpdateJobReason(resp.GetReason()))
		if err != nil {
			mlog.Warn(context.TODO(), "failed to update job state to Failed", mlog.FieldJobID(p.GetJobID()), mlog.Err(err))
			return
		}
		if taskErr := p.importMeta.UpdateTask(context.TODO(), p.GetTaskID(),
			UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason(resp.GetReason())); taskErr != nil {
			mlog.Warn(context.TODO(), "failed to update preimport task state to Failed", WrapTaskLog(p, mlog.Err(taskErr))...)
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

// handoffRetry records retry debt before best-effort worker cleanup. The
// inspector gives the replacement a fresh task ID, so a failed or delayed old
// Drop cannot affect the new attempt and must not block business-layer retry.
func (p *preImportTask) handoffRetry(cluster session.Cluster, reason string) {
	if err := p.importMeta.UpdateTask(context.TODO(), p.GetTaskID(),
		UpdateState(datapb.ImportTaskStateV2_Retry),
		UpdateReason(reason)); err != nil {
		mlog.Warn(context.TODO(), "failed to persist preimport retry handoff", WrapTaskLog(p, mlog.Err(err))...)
		// Stop scheduler polling locally. If DataCoord crashes before the fresh-ID
		// swap, the persisted InProgress assignment is queried again on recovery.
		local := typeutil.Clone(p.task.Load())
		local.State = datapb.ImportTaskStateV2_Retry
		local.Reason = reason
		p.task.Store(local)
	}
	if err := dropImportTaskOnWorker(p, cluster); err != nil {
		mlog.RatedWarn(context.TODO(), rate.Limit(1), "failed to drop old preimport attempt",
			WrapTaskLog(p, mlog.Err(err))...)
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
		alloc:      p.alloc,
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
