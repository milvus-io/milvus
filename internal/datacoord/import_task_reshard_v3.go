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

// This file contains the DataCoord scheduler adapters for the V3 control
// records.  V3 keeps run_id inside the task record; it does not introduce a
// separate attempt proto or catalog level.  The adapters deliberately mirror
// the old ImportTask lifecycle: Create persists Running only after the worker
// accepts the request, Query moves transient failures back to Pending, and
// Drop treats a missing worker as ownership loss.

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

type reshardTask struct {
	task       atomic.Pointer[datapb.ReshardTask]
	importMeta ImportMeta
	meta       *meta
	alloc      allocator.Allocator
	tr         *timerecord.TimeRecorder
	times      *taskcommon.Times
	metrics    importTaskMetrics

	// sourceProgress is the latest per-source hashed rows the worker reported.
	// It is deliberately in-memory only: progress is transient, so it is not
	// persisted to the catalog and resets to empty when the task retries.
	progressMu     sync.Mutex
	sourceProgress []*datapb.ReshardSourceProgress
}

// setSourceProgress stores the worker-reported per-source hashed rows.
func (t *reshardTask) setSourceProgress(progress []*datapb.ReshardSourceProgress) {
	t.progressMu.Lock()
	t.sourceProgress = progress
	t.progressMu.Unlock()
}

// getSourceProgress returns the current per-source hashed rows.
func (t *reshardTask) getSourceProgress() []*datapb.ReshardSourceProgress {
	t.progressMu.Lock()
	defer t.progressMu.Unlock()
	return t.sourceProgress
}

func newReshardTask(p *datapb.ReshardTask, importMeta ImportMeta, meta *meta, alloc allocator.Allocator) *reshardTask {
	t := &reshardTask{
		importMeta: importMeta,
		meta:       meta,
		alloc:      alloc,
		tr:         newTaskRecorder("reshard task", p.GetCreatedTime()),
		times:      taskcommon.NewTimes(),
		metrics:    newImportTaskMetrics(ReshardTaskType),
	}
	t.task.Store(p)
	return t
}

func (t *reshardTask) GetJobID() int64        { return t.task.Load().GetJobId() }
func (t *reshardTask) GetTaskID() int64       { return t.task.Load().GetTaskId() }
func (t *reshardTask) GetCollectionID() int64 { return t.task.Load().GetCollectionId() }
func (t *reshardTask) GetNodeID() int64       { return t.task.Load().GetNodeId() }
func (t *reshardTask) GetType() TaskType      { return ReshardTaskType }
func (t *reshardTask) GetState() datapb.ImportTaskStateV2 {
	return t.task.Load().GetState()
}
func (t *reshardTask) GetReason() string                       { return t.task.Load().GetReason() }
func (t *reshardTask) GetCompleteTime() string                 { return t.task.Load().GetCompleteTime() }
func (t *reshardTask) GetFileStats() []*datapb.ImportFileStats { return nil }
func (t *reshardTask) GetSource() datapb.ImportTaskSourceV2    { return datapb.ImportTaskSourceV2_Request }
func (t *reshardTask) GetTR() *timerecord.TimeRecorder         { return t.tr }
func (t *reshardTask) GetTaskType() taskcommon.Type            { return taskcommon.Reshard }
func (t *reshardTask) GetTaskState() taskcommon.State {
	return taskcommon.FromImportState(t.task.Load().GetState())
}
func (t *reshardTask) GetTaskNodeID() int64 { return t.GetNodeID() }
func (t *reshardTask) GetTaskSlot() int64 {
	if job := t.importMeta.GetJob(context.TODO(), t.GetJobID()); job != nil {
		return reshardSlot(job, getImportV3Config())
	}
	return 1
}

func (t *reshardTask) GetTaskVersion() int64 { return t.task.Load().GetRunId() }

func (t *reshardTask) SetTaskTime(tt taskcommon.TimeType, tm time.Time) { t.times.SetTaskTime(tt, tm) }

func (t *reshardTask) GetTaskTime(tt taskcommon.TimeType) time.Time { return tt.GetTaskTime(t.times) }

func (t *reshardTask) setState(state datapb.ImportTaskStateV2) {
	t.task.Load().State = state
}

func (t *reshardTask) setReason(reason string) {
	t.task.Load().Reason = reason
}

func (t *reshardTask) setCompleteTime(completeTime string) {
	t.task.Load().CompleteTime = completeTime
}

func (t *reshardTask) setNodeID(nodeID int64) {
	t.task.Load().NodeId = nodeID
}

func (t *reshardTask) setRunID(runID int64) {
	t.task.Load().RunId = runID
}

// reshardDispatchable reports whether a ReshardTask may be dispatched while the
// job is in this state. Every V3 job creates its reshard tasks once preimport
// completes, a moment before the state flips to Resharding, so Resharding is the
// dispatchable state; Pending stays dispatchable to recover a reshard task
// persisted by a job created before the count-only preimport existed.
func reshardDispatchable(state internalpb.ImportJobState) bool {
	return state == internalpb.ImportJobState_Pending || state == internalpb.ImportJobState_Resharding
}

// importV3PreReshardState reports whether the job has not reached Resharding
// yet. ReshardTasks are created just before the state flips to Resharding, so a
// Pending reshard task observed in one of these states is early, not invalid.
func importV3PreReshardState(state internalpb.ImportJobState) bool {
	switch state {
	case internalpb.ImportJobState_Pending,
		internalpb.ImportJobState_PreImporting,
		internalpb.ImportJobState_AssigningIDRange:
		return true
	}
	return false
}

func (t *reshardTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	p := t.task.Load()
	if p.GetRunId() == 0 {
		t.fail("reshard task has no run")
		return
	}
	job := t.importMeta.GetJob(context.TODO(), p.GetJobId())
	if job == nil {
		t.fail("reshard task job is missing")
		return
	}
	if !reshardDispatchable(job.GetState()) {
		if importV3PreReshardState(job.GetState()) {
			// ReshardTasks are persisted a moment before the job advances to
			// Resharding (see createReshardTasks). The async dispatch
			// loop can therefore pick a Pending reshard task while the job is
			// still finishing preimport or the ID range gate: the task is simply
			// not dispatchable yet. Leave it Pending for a later tick instead of
			// failing the whole job.
			mlog.Info(context.TODO(), "reshard task is waiting for the job to reach Resharding",
				WrapTaskLog(t, mlog.String("jobState", job.GetState().String()))...)
			return
		}
		t.fail(fmt.Sprintf("reshard task cannot start: job %d is in state %s", p.GetJobId(), job.GetState()))
		return
	}
	cfg := getImportV3Config()
	plan, err := reshardPlan(job, p, cfg)
	if err != nil {
		t.fail(err.Error())
		return
	}
	req := &datapb.ReshardTaskRequest{
		JobId: p.GetJobId(), TaskId: p.GetTaskId(), RunId: p.GetRunId(),
		Slot: reshardSlot(job, cfg), StorageConfig: createStorageConfig(),
		PluginContext: GetReadPluginContext(job.GetOptions()),
		Plan:          plan,
	}
	WrapPluginContext(t.GetCollectionID(), job.GetSchema().GetProperties(), req)
	// PrepareRun: persist the intended run/node before the uncertain Create RPC.
	// If we crash after the RPC was accepted but before this write, recovery can
	// only see Pending and might re-dispatch the same run. Persisting first makes
	// recovery query exactly this node/run or safely allocate a larger run.
	if err := t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_InProgress), UpdateNodeID(nodeID)); err != nil {
		mlog.Warn(context.TODO(), "persist reshard task running state failed", WrapTaskLog(t, mlog.Err(err))...)
		return
	}
	err = cluster.CreateReshard(nodeID, req, t.GetCollectionID())
	if err != nil {
		mlog.Warn(context.TODO(), "create reshard task failed", WrapTaskLog(t, mlog.Err(err))...)
		if errors.Is(err, merr.ErrNodeNotFound) || !common.IsTerminalImportV3Err(err) {
			// The first node may have accepted the request, or the node vanished
			// between selection and dispatch. Never re-dispatch the same run;
			// allocate a new run before retrying.
			t.prepareRetry(cluster)
		} else {
			t.fail(err.Error())
		}
		return
	}
	pendingDuration := t.GetTR().RecordSpan()
	t.metrics.observe(metrics.ImportStagePending, pendingDuration)
}

func (t *reshardTask) QueryTaskOnWorker(cluster session.Cluster) {
	if t.GetState() != datapb.ImportTaskStateV2_InProgress {
		return
	}
	p := t.task.Load()
	resp, err := cluster.QueryReshard(t.GetNodeID(), &datapb.QueryReshardTaskRequest{JobId: p.GetJobId(), TaskId: p.GetTaskId(), RunId: p.GetRunId()})
	if err != nil {
		if errors.Is(err, merr.ErrNodeNotFound) || !common.IsTerminalImportV3Err(err) {
			t.prepareRetry(cluster)
		} else {
			t.fail(err.Error())
		}
		return
	}
	t.setSourceProgress(resp.GetSourceProgresses())
	if resp.GetState() == datapb.ImportTaskStateV2_Retry {
		t.prepareRetry(cluster)
		return
	}
	if resp.GetState() == datapb.ImportTaskStateV2_Failed {
		t.fail(resp.GetReason())
		return
	}
	if resp.GetState() == datapb.ImportTaskStateV2_Completed {
		if t.GetState() != datapb.ImportTaskStateV2_InProgress {
			return
		}
		// Late results are no-ops once the job has moved out of Resharding:
		// timeout/abort or a newer run has already changed the durable contract.
		if job := t.importMeta.GetJob(context.TODO(), p.GetJobId()); job == nil ||
			job.GetState() != internalpb.ImportJobState_Resharding {
			return
		}
		if err := t.acceptResult(); err != nil {
			if common.IsTerminalImportV3Err(err) {
				t.fail(err.Error())
				return
			}
			// A transient object-store or catalog error must not fail the job:
			// keep the task running so the next tick re-queries the same
			// completed result and replays acceptance.
			mlog.Warn(context.TODO(), "accept reshard result failed transiently, will retry", WrapTaskLog(t, mlog.Err(err))...)
			return
		}
		completeTime := time.Now().Format(time.RFC3339)
		if err := t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Completed), UpdateCompleteTime(completeTime)); err != nil {
			mlog.Warn(context.TODO(), "persist accepted reshard result marker failed", WrapTaskLog(t, mlog.Err(err))...)
			return
		}
		reshardDuration := t.GetTR().RecordSpan()
		t.metrics.observe(metrics.ImportStageReshard, reshardDuration)
	}
}

func (t *reshardTask) prepareRetry(cluster session.Cluster) {
	p := t.task.Load()
	job := t.importMeta.GetJob(context.TODO(), p.GetJobId())
	if job == nil || (job.GetState() != internalpb.ImportJobState_Pending && job.GetState() != internalpb.ImportJobState_Resharding) {
		t.fail(fmt.Sprintf("reshard task cannot retry: job %d is unavailable or terminal", p.GetJobId()))
		return
	}
	if t.GetNodeID() != NullNodeID {
		_ = cluster.DropReshard(t.GetNodeID(), &datapb.DropReshardTaskRequest{JobId: p.GetJobId(), TaskId: p.GetTaskId(), RunId: p.GetRunId()})
	}
	newRun := p.GetRunId() + 1
	_ = t.importMeta.UpdateTask(
		context.TODO(),
		t.GetTaskID(),
		UpdateRunID(newRun),
		UpdateNodeID(NullNodeID),
		UpdateState(datapb.ImportTaskStateV2_Pending),
		UpdateReason(""),
	)
	// The retry is a fresh run: drop the superseded run's progress so the
	// per-file counters do not carry stale hashed rows into the new attempt.
	t.setSourceProgress(nil)
}

func (t *reshardTask) acceptResult() error {
	if t.meta == nil || t.meta.chunkManager == nil {
		return merr.WrapErrImportSysFailedMsg("reshard result storage is unavailable")
	}
	p := t.task.Load()
	manifest, err := loadReshardResultManifest(context.TODO(), t.meta.chunkManager, p.GetJobId(), p.GetTaskId(), p.GetRunId())
	if err != nil {
		return err
	}
	return validateReshardManifest(manifest)
}

func (t *reshardTask) DropTaskOnWorker(cluster session.Cluster) {
	p := t.task.Load()
	if t.GetNodeID() != NullNodeID {
		err := cluster.DropReshard(t.GetNodeID(), &datapb.DropReshardTaskRequest{JobId: p.GetJobId(), TaskId: p.GetTaskId(), RunId: p.GetRunId()})
		if err != nil && !errors.Is(err, merr.ErrNodeNotFound) {
			return
		}
	}
	_ = t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateNodeID(NullNodeID))
}

// fail marks the reshard task Failed with its reason. It does NOT write the job
// state: a task only ever marks itself, and the state machine propagates the
// failure from the task marker on the checker's next tick (reshardingHandler's
// reshardTasksAllCompleted, or coveredSourceIDs when the job is still Pending).
func (t *reshardTask) fail(reason string) {
	if err := t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason(reason)); err != nil {
		mlog.Warn(context.TODO(), "failed to mark import v3 reshard task failed", WrapTaskLog(t, mlog.Err(err))...)
	}
}

func (t *reshardTask) Clone() ImportTask {
	c := newReshardTask(proto.Clone(t.task.Load()).(*datapb.ReshardTask), t.importMeta, t.meta, t.alloc)
	c.tr, c.times = t.tr, t.times
	c.setSourceProgress(t.getSourceProgress())
	return c
}

func (t *reshardTask) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		metricsinfo.ImportTask{
			JobID:        t.GetJobID(),
			TaskID:       t.GetTaskID(),
			CollectionID: t.GetCollectionID(),
			NodeID:       t.GetNodeID(),
			State:        t.GetState().String(),
			Reason:       t.GetReason(),
			TaskType:     t.GetType().String(),
		},
	)
}
