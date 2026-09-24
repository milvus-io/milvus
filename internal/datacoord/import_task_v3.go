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
	"strconv"
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
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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

type importTaskV3 struct {
	task       atomic.Pointer[datapb.ImportTaskV3]
	importMeta ImportMeta
	meta       *meta
	alloc      allocator.Allocator
	tr         *timerecord.TimeRecorder
	times      *taskcommon.Times
	metrics    importTaskMetrics
}

func newImportTaskV3(p *datapb.ImportTaskV3, importMeta ImportMeta, meta *meta, alloc allocator.Allocator) *importTaskV3 {
	t := &importTaskV3{
		importMeta: importMeta,
		meta:       meta,
		alloc:      alloc,
		tr:         newTaskRecorder("import v3 task", p.GetCreatedTime()),
		times:      taskcommon.NewTimes(),
		metrics:    newImportTaskMetrics(ImportTaskV3Type),
	}
	t.task.Store(p)
	return t
}
func (t *importTaskV3) GetJobID() int64        { return t.task.Load().GetJobId() }
func (t *importTaskV3) GetTaskID() int64       { return t.task.Load().GetTaskId() }
func (t *importTaskV3) GetCollectionID() int64 { return t.task.Load().GetCollectionId() }
func (t *importTaskV3) GetNodeID() int64       { return t.task.Load().GetNodeId() }
func (t *importTaskV3) GetType() TaskType      { return ImportTaskV3Type }
func (t *importTaskV3) GetState() datapb.ImportTaskStateV2 {
	return t.task.Load().GetState()
}
func (t *importTaskV3) GetReason() string                       { return t.task.Load().GetReason() }
func (t *importTaskV3) GetFileStats() []*datapb.ImportFileStats { return nil }
func (t *importTaskV3) GetSource() datapb.ImportTaskSourceV2 {
	return datapb.ImportTaskSourceV2_Request
}
func (t *importTaskV3) GetTR() *timerecord.TimeRecorder { return t.tr }
func (t *importTaskV3) GetTaskType() taskcommon.Type    { return taskcommon.ImportV3 }
func (t *importTaskV3) GetTaskState() taskcommon.State {
	return taskcommon.FromImportState(t.task.Load().GetState())
}
func (t *importTaskV3) GetTaskNodeID() int64 { return t.GetNodeID() }
func (t *importTaskV3) GetTaskSlot() int64 {
	if job := t.importMeta.GetJob(context.TODO(), t.GetJobID()); job != nil {
		return importSlot(len(t.task.Load().GetFragments()), getImportV3Config())
	}
	return 1
}

func (t *importTaskV3) GetTaskVersion() int64 { return t.task.Load().GetRunId() }

func (t *importTaskV3) SetTaskTime(tt taskcommon.TimeType, tm time.Time) { t.times.SetTaskTime(tt, tm) }

func (t *importTaskV3) GetTaskTime(tt taskcommon.TimeType) time.Time { return tt.GetTaskTime(t.times) }

func (t *importTaskV3) setState(state datapb.ImportTaskStateV2) {
	t.task.Load().State = state
}

func (t *importTaskV3) setReason(reason string) {
	t.task.Load().Reason = reason
}

func (t *importTaskV3) setNodeID(nodeID int64) {
	t.task.Load().NodeId = nodeID
}

func (t *importTaskV3) setRunID(runID int64) {
	t.task.Load().RunId = runID
}

func (t *importTaskV3) setLogRange(logRange *datapb.IDRange) {
	t.task.Load().LogRange = proto.Clone(logRange).(*datapb.IDRange)
}

func (t *importTaskV3) setSegmentIDs(segmentIDs []UniqueID) {
	if len(segmentIDs) == 0 {
		return
	}
	t.task.Load().SegmentId = segmentIDs[0]
}

func (t *importTaskV3) setOldSegmentIDs(segmentIDs []UniqueID) {
	if len(segmentIDs) == 0 {
		t.task.Load().OldSegmentIds = nil
		return
	}
	t.task.Load().OldSegmentIds = append([]int64(nil), segmentIDs...)
}

func (t *importTaskV3) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	p := t.task.Load()
	if p.GetRunId() == 0 || p.GetLogRange() == nil {
		t.fail("import v3 task has no run resources")
		return
	}
	job := t.importMeta.GetJob(context.TODO(), p.GetJobId())
	if job == nil {
		t.fail("import v3 task job is missing")
		return
	}
	if job.GetState() != internalpb.ImportJobState_Planning && job.GetState() != internalpb.ImportJobState_Importing {
		t.fail(fmt.Sprintf("import v3 task cannot start: job %d is in state %s", p.GetJobId(), job.GetState()))
		return
	}
	plan, slot, err := importPlan(job, p, getImportV3Config())
	if err != nil {
		t.fail(err.Error())
		return
	}
	// Drop the segments a superseded run used to own but this run no longer
	// references. The list is cleared in the same write that binds this run, so a
	// transient drop failure is retried on the next tick with the list still in
	// place rather than failing the task.
	if oldSegmentIDs := p.GetOldSegmentIds(); len(oldSegmentIDs) > 0 {
		if err := t.meta.UpdateSegmentsInfo(context.TODO(), dropImportV3Segments(oldSegmentIDs)); err != nil {
			mlog.Warn(context.TODO(), "drop superseded import v3 segments failed transiently, will retry",
				WrapTaskLog(t, mlog.Int64s("segmentIDs", oldSegmentIDs), mlog.Err(err))...)
			return
		}
	}
	// The segment record is created here, at dispatch, not during Planning, so a
	// task only ever owns a segment it is about to run. An existing record is
	// reused, so a crash between this write and the InProgress write below
	// resumes on the same segment instead of leaking it.
	if t.meta.GetSegment(context.TODO(), p.GetSegmentId()) == nil {
		// The storage version comes from THIS dispatch's plan writer spec, the
		// same config read that produced the plan, so a useLoonFFI flip between
		// Planning and dispatch cannot make the record disagree with the layout.
		if _, err := addImportSegment(context.TODO(), t.meta, p.GetSegmentId(), p.GetJobId(), p.GetTaskId(), p.GetCollectionId(), p.GetPartitionId(), p.GetVchannel(), datapb.SegmentLevel_L1, plan.GetWriter().GetStorageVersion(), job.GetSchema().GetVersion()); err != nil {
			mlog.Warn(context.TODO(), "create import v3 task segment failed transiently, will retry",
				WrapTaskLog(t, mlog.Err(err))...)
			return
		}
	}
	req := &datapb.ImportTaskV3Request{
		JobId: p.GetJobId(), TaskId: p.GetTaskId(), RunId: p.GetRunId(), SegmentId: p.GetSegmentId(),
		LogRange: p.GetLogRange(), Slot: slot,
		StorageConfig: createStorageConfig(),
		PluginContext: GetReadPluginContext(job.GetOptions()),
		Plan:          plan,
	}
	WrapPluginContext(t.GetCollectionID(), job.GetSchema().GetProperties(), req)
	// PrepareRun: persist Running/node before the uncertain Create RPC, so a crash
	// after acceptance never causes the same run to be re-dispatched elsewhere.
	// Clearing old_segment_ids here is safe: the superseded segments were dropped
	// just above, and the list only re-appears on the next retry.
	if err := t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_InProgress), UpdateNodeID(nodeID), UpdateOldSegmentIDs(nil)); err != nil {
		mlog.Warn(context.TODO(), "persist import v3 task running state failed", WrapTaskLog(t, mlog.Err(err))...)
		return
	}
	err = cluster.CreateImportV3(nodeID, req, t.GetCollectionID())
	if err != nil {
		mlog.Warn(context.TODO(), "create import v3 task failed", WrapTaskLog(t, mlog.Err(err))...)
		if errors.Is(err, merr.ErrNodeNotFound) || !common.IsTerminalImportV3Err(err) {
			// Never re-use a run whose Create result is uncertain, and treat a
			// vanished node as ownership lost. prepareRetry allocates a fresh
			// segment id and log range, records the superseded segment id in
			// old_segment_ids, and increments run_id; this segment is created
			// again only by the next dispatch.
			t.prepareRetry(cluster)
		} else {
			t.fail(err.Error())
		}
		return
	}
	pendingDuration := t.GetTR().RecordSpan()
	t.metrics.observe(metrics.ImportStagePending, pendingDuration)
}

func (t *importTaskV3) QueryTaskOnWorker(cluster session.Cluster) {
	if t.GetState() != datapb.ImportTaskStateV2_InProgress {
		return
	}
	p := t.task.Load()
	resp, err := cluster.QueryImportV3(t.GetNodeID(), &datapb.QueryImportTaskV3Request{JobId: p.GetJobId(), TaskId: p.GetTaskId(), RunId: p.GetRunId()})
	if err != nil {
		if errors.Is(err, merr.ErrNodeNotFound) || !common.IsTerminalImportV3Err(err) {
			t.prepareRetry(cluster)
		} else {
			t.fail(err.Error())
		}
		return
	}
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
		// A Completed reply that races timeout/abort/new-run ownership must be a
		// no-op; accepting it would publish segment metadata under a terminal job.
		if job := t.importMeta.GetJob(context.TODO(), p.GetJobId()); job == nil ||
			job.GetState() != internalpb.ImportJobState_Importing {
			return
		}
		segment := t.meta.GetSegment(context.TODO(), p.GetSegmentId())
		oldRows := int64(0)
		if segment != nil {
			oldRows = segment.GetNumOfRows()
		}
		if err := t.acceptResult(resp.GetSegments()); err != nil {
			if common.IsTerminalImportV3Err(err) {
				t.fail(err.Error())
				return
			}
			mlog.Warn(context.TODO(), "accept import result failed transiently, will retry", WrapTaskLog(t, mlog.Err(err))...)
			return
		}
		if results := resp.GetSegments(); len(results) == 1 && results[0].GetRows() > 0 {
			select {
			case getBuildIndexChSingleton() <- p.GetSegmentId():
			default:
			}
		}
		if err := t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Completed)); err != nil {
			mlog.Warn(context.TODO(), "persist accepted import v3 result marker failed", WrapTaskLog(t, mlog.Err(err))...)
			return
		}
		importDuration := t.GetTR().RecordSpan()
		t.metrics.observe(metrics.ImportStageImport, importDuration)
		newRows := int64(0)
		for _, result := range resp.GetSegments() {
			newRows += result.GetRows()
		}
		if diff := newRows - oldRows; diff > 0 {
			dbName := ""
			if coll := t.meta.GetCollection(p.GetCollectionId()); coll != nil {
				dbName = coll.DatabaseName
			}
			metrics.DataCoordBulkVectors.WithLabelValues(dbName, strconv.FormatInt(p.GetCollectionId(), 10)).Add(float64(diff))
		}
	}
}

func (t *importTaskV3) prepareRetry(cluster session.Cluster) {
	p := t.task.Load()
	job := t.importMeta.GetJob(context.TODO(), p.GetJobId())
	if job == nil || (job.GetState() != internalpb.ImportJobState_Planning && job.GetState() != internalpb.ImportJobState_Importing) {
		t.fail(fmt.Sprintf("import v3 task cannot retry: job %d is unavailable or terminal", p.GetJobId()))
		return
	}
	if t.alloc == nil || t.meta == nil || p.GetLogRange() == nil {
		t.fail("import v3 retry resources are unavailable")
		return
	}
	if t.GetNodeID() != NullNodeID {
		_ = cluster.DropImportV3(t.GetNodeID(), &datapb.DropImportTaskV3Request{JobId: p.GetJobId(), TaskId: p.GetTaskId(), RunId: p.GetRunId()})
	}
	// A retry only allocates the new run's identity: a fresh segment id and a
	// fresh log range. It creates and drops no segment record -- the next
	// dispatch creates the new segment and drops the superseded ones recorded in
	// old_segment_ids -- so a crashed retry never leaks a segment.
	newSegmentID, err := t.alloc.AllocID(context.TODO())
	if err != nil {
		// AllocID is a mixCoord RPC with a short timeout; a transient failure
		// must not fail the job (V2 parity: the import task path only counts
		// the attempt and returns). Nothing was allocated yet, so there is
		// nothing to roll back; the next tick re-enters prepareRetry through
		// the Query path, whose worker side reports ErrNodeNotFound for the
		// run dropped above.
		mlog.Warn(context.TODO(), "import v3 retry allocation failed transiently, will retry",
			WrapTaskLog(t, mlog.Err(err))...)
		return
	}
	// The fresh range is sized from the current storage version rather than
	// copied from the superseded range, so a hot-flipped useLoonFFI cannot
	// exhaust a range that was sized for the old version. A late writer of the
	// superseded run therefore cannot reuse the new run's log IDs.
	spec, err := writerSpec(typeutil.AppendSystemFields(job.GetSchema()), getImportV3Config())
	if err != nil {
		t.fail(err.Error())
		return
	}
	width, err := importV3LogRangeWidth(spec, bm25OutputFieldCount(job.GetSchema()))
	if err != nil {
		t.fail(err.Error())
		return
	}
	begin, end, err := t.alloc.AllocN(width)
	if err != nil {
		// Transient RPC failure: nothing was created, so leave the task alone for
		// the next tick.
		mlog.Warn(context.TODO(), "import v3 retry log range allocation failed transiently, will retry",
			WrapTaskLog(t, mlog.Err(err))...)
		return
	}
	newRun := p.GetRunId() + 1
	// One task write repoints the run and records the superseded segment id so
	// the next dispatch drops it. The task record is now the only durable owner
	// of that segment; a crash after this write leaves it for the next dispatch
	// (or, if the job fails first, the failed-job GC) to drop.
	oldSegmentIDs := append([]int64(nil), p.GetOldSegmentIds()...)
	if oldSegmentID := p.GetSegmentId(); oldSegmentID != 0 {
		oldSegmentIDs = append(oldSegmentIDs, oldSegmentID)
	}
	if err := t.importMeta.UpdateTask(
		context.TODO(),
		t.GetTaskID(),
		UpdateRunID(newRun),
		UpdateSegmentIDs([]int64{newSegmentID}),
		UpdateLogRange(&datapb.IDRange{Begin: begin, End: end}),
		UpdateOldSegmentIDs(oldSegmentIDs),
		UpdateNodeID(NullNodeID),
		UpdateState(datapb.ImportTaskStateV2_Pending),
		UpdateReason(""),
	); err != nil {
		// The write outcome is unknown (a timed-out etcd op may have committed).
		// Nothing was created, so there is nothing to roll back; retrying on the
		// next tick is safe.
		mlog.Warn(context.TODO(), "import v3 retry repoint failed transiently, will retry",
			WrapTaskLog(t, mlog.Err(err))...)
		return
	}
}

func (t *importTaskV3) acceptResult(results []*datapb.ImportTaskV3Result) error {
	if t.meta == nil {
		return merr.WrapErrImportSysFailedMsg("import v3 result metadata is unavailable")
	}
	p := t.task.Load()
	job := t.importMeta.GetJob(context.TODO(), p.GetJobId())
	if job == nil {
		return merr.WrapErrImportSysFailedMsg("import v3 job is unavailable during result acceptance")
	}
	schemaVersion, err := validateImportV3Schema(t.meta, p.GetCollectionId(), job.GetSchema())
	if err != nil {
		return err
	}
	if err := validateImportResults(results, 1); err != nil {
		return err
	}
	namespaceSorted := job.GetSchema().GetEnableNamespace()
	return applyImportResults(context.TODO(), t.meta, p.GetCollectionId(), schemaVersion, []int64{p.GetSegmentId()}, !namespaceSorted, namespaceSorted, results)
}

func (t *importTaskV3) DropTaskOnWorker(cluster session.Cluster) {
	p := t.task.Load()
	if t.GetNodeID() != NullNodeID {
		err := cluster.DropImportV3(t.GetNodeID(), &datapb.DropImportTaskV3Request{JobId: p.GetJobId(), TaskId: p.GetTaskId(), RunId: p.GetRunId()})
		if err != nil && !errors.Is(err, merr.ErrNodeNotFound) {
			return
		}
	}
	// No segment cleanup here: zero-row placeholders are dropped at acceptance,
	// and a failed task's preallocated segment is dropped by the failed-job GC
	// after this drop unbinds the worker. A completed task's accepted segment is
	// formal data owned by the commit path.
	_ = t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateNodeID(NullNodeID))
}

// fail marks the import task Failed with its reason. It does NOT write the job
// state: a task only ever marks itself, and importingHandler propagates the
// failure from the task marker on the checker's next tick (allTasksDone), or
// loadExistingFragments when the job is still Planning.
func (t *importTaskV3) fail(reason string) {
	if err := t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason(reason)); err != nil {
		mlog.Warn(context.TODO(), "failed to mark import v3 task failed", WrapTaskLog(t, mlog.Err(err))...)
	}
}

func (t *importTaskV3) Clone() ImportTask {
	c := newImportTaskV3(proto.Clone(t.task.Load()).(*datapb.ImportTaskV3), t.importMeta, t.meta, t.alloc)
	c.tr, c.times = t.tr, t.times
	return c
}

func (t *importTaskV3) MarshalJSON() ([]byte, error) {
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
