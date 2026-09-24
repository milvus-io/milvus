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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ ImportTask = (*preImportV3Task)(nil)

// preImportV3Task is the Import V3 count-only preimport control record. It runs
// before the reshard stage so the primary can allocate exact per-file ID ranges
// from the exact row counts, instead of sizing files at broadcast. It does no
// hashing and carries no hashed bucket stats.
type preImportV3Task struct {
	task atomic.Pointer[datapb.PreImportTaskV3]

	importMeta ImportMeta
	tr         *timerecord.TimeRecorder
	times      *taskcommon.Times
	retryTimes int64
}

func newPreImportTaskV3(p *datapb.PreImportTaskV3, importMeta ImportMeta) *preImportV3Task {
	t := &preImportV3Task{
		importMeta: importMeta,
		tr:         newTaskRecorder("preimport v3 task", p.GetCreatedTime()),
		times:      taskcommon.NewTimes(),
	}
	t.task.Store(p)
	return t
}

func (p *preImportV3Task) GetJobID() int64 {
	return p.task.Load().GetJobId()
}

func (p *preImportV3Task) GetTaskID() int64 {
	return p.task.Load().GetTaskId()
}

func (p *preImportV3Task) GetCollectionID() int64 {
	return p.task.Load().GetCollectionId()
}

func (p *preImportV3Task) GetNodeID() int64 {
	return p.task.Load().GetNodeId()
}

func (p *preImportV3Task) GetState() datapb.ImportTaskStateV2 {
	return p.task.Load().GetState()
}

func (p *preImportV3Task) GetReason() string {
	return p.task.Load().GetReason()
}

// GetFileStats returns nil: this task carries the V3-shaped stats readable via
// GetV3FileStats. It exists only to satisfy the generic ImportTask accessor.
func (p *preImportV3Task) GetFileStats() []*datapb.ImportFileStats {
	return nil
}

func (p *preImportV3Task) GetV3FileStats() []*datapb.ImportV3FileStats {
	return p.task.Load().GetFileStats()
}

func (p *preImportV3Task) GetCreatedTime() string {
	return p.task.Load().GetCreatedTime()
}

func (p *preImportV3Task) GetCompleteTime() string {
	return p.task.Load().GetCompleteTime()
}

func (p *preImportV3Task) GetTaskType() taskcommon.Type {
	return taskcommon.PreImportV3
}

func (p *preImportV3Task) GetTaskState() taskcommon.State {
	return taskcommon.FromImportState(p.GetState())
}

func (p *preImportV3Task) GetTaskSlot() int64 {
	return int64(CalculateTaskSlot(p, p.importMeta))
}

func (p *preImportV3Task) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	p.times.SetTaskTime(timeType, time)
}

func (p *preImportV3Task) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(p.times)
}

func (p *preImportV3Task) GetTaskVersion() int64 {
	return p.retryTimes
}

func (p *preImportV3Task) setState(state datapb.ImportTaskStateV2) {
	p.task.Load().State = state
}

func (p *preImportV3Task) setReason(reason string) {
	p.task.Load().Reason = reason
}

func (p *preImportV3Task) setCompleteTime(completeTime string) {
	p.task.Load().CompleteTime = completeTime
}

func (p *preImportV3Task) setNodeID(nodeID int64) {
	p.task.Load().NodeId = nodeID
}

func (p *preImportV3Task) setV3FileStats(fileStats []*datapb.ImportV3FileStats) {
	p.task.Load().FileStats = fileStats
}

func (p *preImportV3Task) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	mlog.Info(context.TODO(), "processing pending preimport v3 task...", WrapTaskLog(p)...)
	job := p.importMeta.GetJob(context.TODO(), p.GetJobID())
	req := AssemblePreImportV3Request(p, job)

	err := cluster.CreatePreImportV3(nodeID, req, p.GetTaskSlot())
	if err != nil {
		mlog.Warn(context.TODO(), "preimport v3 failed", WrapTaskLog(p, mlog.Err(err))...)
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
	mlog.Info(
		context.TODO(),
		"preimport v3 task start to execute",
		WrapTaskLog(p, mlog.Int64("scheduledNodeID", nodeID), mlog.Duration("taskTimeCost/pending", pendingDuration))...)
}

func (p *preImportV3Task) QueryTaskOnWorker(cluster session.Cluster) {
	req := &datapb.QueryPreImportV3Request{
		JobId:  p.GetJobID(),
		TaskId: p.GetTaskID(),
	}
	resp, err := cluster.QueryPreImportV3(p.GetNodeID(), req)
	if err != nil || resp.GetState() == datapb.ImportTaskStateV2_Retry {
		updateErr := p.importMeta.UpdateTask(context.TODO(), p.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Pending))
		if updateErr != nil {
			mlog.Warn(context.TODO(), "failed to update preimport v3 task state to pending", WrapTaskLog(p, mlog.Err(updateErr))...)
		}
		mlog.Info(
			context.TODO(),
			"reset preimport v3 task state to pending due to error occurs",
			WrapTaskLog(p, mlog.Err(err), mlog.String("reason", resp.GetReason()))...)
		return
	}
	if resp.GetState() == datapb.ImportTaskStateV2_Failed {
		// Only mark the TASK failed with its reason. The job is failed by the
		// state machine on the checker's next tick (preImportingHandler), so the
		// reason stays visible without the task wrapper writing the job state.
		updateErr := p.importMeta.UpdateTask(context.TODO(), p.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed),
			UpdateReason(resp.GetReason()))
		if updateErr != nil {
			mlog.Warn(context.TODO(), "failed to mark preimport v3 task failed", WrapTaskLog(p, mlog.Err(updateErr))...)
		}
		mlog.Warn(context.TODO(), "preimport v3 failed", WrapTaskLog(p, mlog.String("reason", resp.GetReason()))...)
		return
	}
	actions := []UpdateAction{}
	if resp.GetState() == datapb.ImportTaskStateV2_InProgress {
		if resp.GetFileStats() == nil {
			return
		}
		actions = append(actions, UpdateV3FileStats(resp.GetFileStats()))
	}
	if resp.GetState() == datapb.ImportTaskStateV2_Completed {
		actions = append(actions, UpdateV3FileStats(resp.GetFileStats()))
		actions = append(actions, UpdateState(datapb.ImportTaskStateV2_Completed))
	}
	if len(actions) > 0 {
		err = p.importMeta.UpdateTask(context.TODO(), p.GetTaskID(), actions...)
		if err != nil {
			mlog.Warn(context.TODO(), "update preimport v3 task failed", WrapTaskLog(p, mlog.Err(err))...)
			return
		}
	}
	mlog.Info(context.TODO(), "query preimport v3", WrapTaskLog(p, mlog.String("respState", resp.GetState().String()),
		mlog.Any("fileStats", resp.GetFileStats()))...)
	if resp.GetState() == datapb.ImportTaskStateV2_Completed {
		preimportDuration := p.GetTR().RecordSpan()
		metrics.ImportTaskLatency.WithLabelValues(metrics.ImportStagePreImport, p.GetType().String()).Observe(float64(preimportDuration.Milliseconds()))
		mlog.Info(context.TODO(), "preimport v3 done", WrapTaskLog(p, mlog.Duration("taskTimeCost/preimport", preimportDuration))...)
	}
}

func (p *preImportV3Task) DropTaskOnWorker(cluster session.Cluster) {
	if p.GetNodeID() == NullNodeID {
		return
	}
	// ErrNodeNotFound means the bound DataNode already left, so the drop is
	// vacuously done; clear the binding anyway. Returning on it (as the sibling
	// V3 adapters and V2 DropImportTask do not) would leave NodeID set forever,
	// and the terminal-job GC's quiesce phase would never complete, leaking the
	// job/task records and the job's object-store prefix.
	err := cluster.DropPreImportV3(p.GetNodeID(), p.GetTaskID())
	if err != nil && !errors.Is(err, merr.ErrNodeNotFound) {
		mlog.Warn(context.TODO(), "drop preimport v3 failed", WrapTaskLog(p, mlog.Err(err))...)
		return
	}
	_ = p.importMeta.UpdateTask(context.TODO(), p.GetTaskID(), UpdateNodeID(NullNodeID))
}

func (p *preImportV3Task) GetType() TaskType {
	return PreImportTaskV3Type
}

func (p *preImportV3Task) GetTR() *timerecord.TimeRecorder {
	return p.tr
}

func (p *preImportV3Task) Clone() ImportTask {
	cloned := &preImportV3Task{
		importMeta: p.importMeta,
		tr:         p.tr,
		times:      p.times,
	}
	cloned.task.Store(typeutil.Clone(p.task.Load()))
	return cloned
}

func (p *preImportV3Task) GetSource() datapb.ImportTaskSourceV2 {
	return datapb.ImportTaskSourceV2_Request
}

func (p *preImportV3Task) MarshalJSON() ([]byte, error) {
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
