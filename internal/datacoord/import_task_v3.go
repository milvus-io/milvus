package datacoord

// This file contains the DataCoord scheduler adapters for the V3 control
// records.  V3 keeps run_id inside the task record; it does not introduce a
// separate attempt proto or catalog level.  The adapters deliberately mirror
// the old ImportTask lifecycle: Create persists Running only after the worker
// accepts the request, Query moves transient failures back to Pending, and
// Drop treats a missing worker as ownership loss.

import (
	"bytes"
	"context"
	"time"

	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

func v3StateToV2(state datapb.ReshardTask_State) datapb.ImportTaskStateV2 {
	switch state {
	case datapb.ReshardTask_Pending:
		return datapb.ImportTaskStateV2_Pending
	case datapb.ReshardTask_Running:
		return datapb.ImportTaskStateV2_InProgress
	case datapb.ReshardTask_Retry:
		return datapb.ImportTaskStateV2_Retry
	case datapb.ReshardTask_Completed:
		return datapb.ImportTaskStateV2_Completed
	case datapb.ReshardTask_Failed:
		return datapb.ImportTaskStateV2_Failed
	default:
		return datapb.ImportTaskStateV2_None
	}
}

func v3ImportStateToV2(state datapb.ImportTaskV3_State) datapb.ImportTaskStateV2 {
	switch state {
	case datapb.ImportTaskV3_Pending:
		return datapb.ImportTaskStateV2_Pending
	case datapb.ImportTaskV3_Running:
		return datapb.ImportTaskStateV2_InProgress
	case datapb.ImportTaskV3_Retry:
		return datapb.ImportTaskStateV2_Retry
	case datapb.ImportTaskV3_Completed:
		return datapb.ImportTaskStateV2_Completed
	case datapb.ImportTaskV3_Failed:
		return datapb.ImportTaskStateV2_Failed
	default:
		return datapb.ImportTaskStateV2_None
	}
}

func v2ToReshardState(state datapb.ImportTaskStateV2) datapb.ReshardTask_State {
	switch state {
	case datapb.ImportTaskStateV2_Pending:
		return datapb.ReshardTask_Pending
	case datapb.ImportTaskStateV2_InProgress:
		return datapb.ReshardTask_Running
	case datapb.ImportTaskStateV2_Retry:
		return datapb.ReshardTask_Retry
	case datapb.ImportTaskStateV2_Completed:
		return datapb.ReshardTask_Completed
	case datapb.ImportTaskStateV2_Failed:
		return datapb.ReshardTask_Failed
	default:
		return datapb.ReshardTask_None
	}
}

func v2ToImportV3State(state datapb.ImportTaskStateV2) datapb.ImportTaskV3_State {
	switch state {
	case datapb.ImportTaskStateV2_Pending:
		return datapb.ImportTaskV3_Pending
	case datapb.ImportTaskStateV2_InProgress:
		return datapb.ImportTaskV3_Running
	case datapb.ImportTaskStateV2_Retry:
		return datapb.ImportTaskV3_Retry
	case datapb.ImportTaskStateV2_Completed:
		return datapb.ImportTaskV3_Completed
	case datapb.ImportTaskStateV2_Failed:
		return datapb.ImportTaskV3_Failed
	default:
		return datapb.ImportTaskV3_None
	}
}

type reshardTask struct {
	task       atomic.Pointer[datapb.ReshardTask]
	importMeta ImportMeta
	meta       *meta
	alloc      allocator.Allocator
	tr         *timerecord.TimeRecorder
	times      *taskcommon.Times
	retryTimes int64
}

func newReshardTask(p *datapb.ReshardTask, importMeta ImportMeta, meta *meta, alloc ...allocator.Allocator) *reshardTask {
	t := &reshardTask{importMeta: importMeta, meta: meta, tr: timerecord.NewTimeRecorder("reshard task"), times: taskcommon.NewTimes()}
	if len(alloc) > 0 {
		t.alloc = alloc[0]
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
	return v3StateToV2(t.task.Load().GetState())
}
func (t *reshardTask) GetReason() string                       { return t.task.Load().GetReason() }
func (t *reshardTask) GetFileStats() []*datapb.ImportFileStats { return nil }
func (t *reshardTask) GetSource() datapb.ImportTaskSourceV2    { return datapb.ImportTaskSourceV2_Request }
func (t *reshardTask) GetTR() *timerecord.TimeRecorder         { return t.tr }
func (t *reshardTask) GetTaskType() taskcommon.Type            { return taskcommon.Reshard }
func (t *reshardTask) GetTaskState() taskcommon.State {
	return taskcommon.FromReshardState(t.task.Load().GetState())
}
func (t *reshardTask) GetTaskNodeID() int64                             { return t.GetNodeID() }
func (t *reshardTask) GetTaskSlot() int64                               { return t.task.Load().GetTaskSlot() }
func (t *reshardTask) GetTaskVersion() int64                            { return t.task.Load().GetRunId() }
func (t *reshardTask) SetTaskTime(tt taskcommon.TimeType, tm time.Time) { t.times.SetTaskTime(tt, tm) }
func (t *reshardTask) GetTaskTime(tt taskcommon.TimeType) time.Time     { return tt.GetTaskTime(t.times) }
func (t *reshardTask) RequireExactSlotAdmission() bool                  { return true }
func (t *reshardTask) MinimumImportTaskVersion() uint32                 { return 3 }
func (t *reshardTask) setState(state datapb.ImportTaskStateV2) {
	t.task.Load().State = v2ToReshardState(state)
}

func (t *reshardTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	p := t.task.Load()
	if p.GetTaskPlanRef() == "" || len(p.GetTaskPlanDigest()) == 0 || p.GetRunId() == 0 {
		t.fail("reshard task has no persisted plan or run", merr.Code(merr.ErrImportSysFailed))
		return
	}
	job := t.importMeta.GetJob(context.TODO(), p.GetJobId())
	if job == nil {
		t.fail("reshard task job is missing", merr.Code(merr.ErrImportSysFailed))
		return
	}
	req := &datapb.ReshardTaskRequest{
		JobId: p.GetJobId(), TaskId: p.GetTaskId(), RunId: p.GetRunId(),
		TaskPlanRef: p.GetTaskPlanRef(), TaskPlanDigest: p.GetTaskPlanDigest(),
		OutputPrefix: p.GetOutputPrefix(), TaskSlot: p.GetTaskSlot(), StorageConfig: createStorageConfig(),
		PluginContext: GetReadPluginContext(job.GetOptions()),
	}
	WrapPluginContext(t.GetCollectionID(), job.GetSchema().GetProperties(), req)
	err := cluster.CreateReshard(nodeID, req, t.GetCollectionID())
	if err != nil {
		mlog.Warn(context.TODO(), "create reshard task failed", WrapTaskLog(t, mlog.Err(err))...)
		if isImportTerminalError(err) {
			t.fail(err.Error(), importFailureCode(err))
		}
		return
	}
	if err := t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_InProgress), UpdateNodeID(nodeID)); err != nil {
		mlog.Warn(context.TODO(), "persist reshard task running state failed", WrapTaskLog(t, mlog.Err(err))...)
	}
}

func (t *reshardTask) QueryTaskOnWorker(cluster session.Cluster) {
	p := t.task.Load()
	resp, err := cluster.QueryReshard(t.GetNodeID(), &datapb.QueryReshardTaskRequest{JobId: p.GetJobId(), TaskId: p.GetTaskId(), RunId: p.GetRunId()})
	if err != nil {
		if isImportOwnershipLost(err) {
			t.prepareRetry(cluster)
		} else if isImportTerminalError(err) {
			t.fail(err.Error(), importFailureCode(err))
		} else {
			t.prepareRetry(cluster)
		}
		return
	}
	if resp.GetState() == datapb.ReshardTask_Retry {
		if isImportTerminalFailureCode(resp.GetFailureCode()) {
			t.fail(resp.GetReason(), resp.GetFailureCode())
			return
		}
		t.prepareRetry(cluster)
		return
	}
	if resp.GetState() == datapb.ReshardTask_Failed {
		t.fail(resp.GetReason(), resp.GetFailureCode())
		return
	}
	if resp.GetState() == datapb.ReshardTask_Completed {
		if t.GetState() != datapb.ImportTaskStateV2_InProgress {
			return
		}
		if err := t.acceptResult(resp.GetResultRef(), resp.GetResultDigest()); err != nil {
			t.fail(err.Error(), merr.Code(err))
			return
		}
		if err := t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateV3Result(resp.GetResultRef(), resp.GetResultDigest()), UpdateState(datapb.ImportTaskStateV2_Completed)); err != nil {
			mlog.Warn(context.TODO(), "persist accepted reshard result marker failed", WrapTaskLog(t, mlog.Err(err))...)
		}
	}
}

func (t *reshardTask) prepareRetry(cluster session.Cluster) {
	p := t.task.Load()
	if t.GetNodeID() != NullNodeID {
		_ = cluster.DropReshard(t.GetNodeID(), &datapb.DropReshardTaskRequest{JobId: p.GetJobId(), TaskId: p.GetTaskId(), RunId: p.GetRunId()})
	}
	newRun := p.GetRunId() + 1
	prefix := metautil.BuildImportV3ReshardOutputPath(p.GetJobId(), p.GetTaskId())
	_ = t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), updateReshardRun(newRun, prefix))
}

func (t *reshardTask) acceptResult(ref string, digest []byte) error {
	if t.meta == nil || t.meta.chunkManager == nil {
		return merr.WrapErrImportSysFailedMsg("reshard result storage is unavailable")
	}
	p := t.task.Load()
	manifest, err := loadReshardResultManifest(context.TODO(), t.meta.chunkManager, ref, p.GetOutputPrefix(), digest)
	if err != nil {
		return err
	}
	return validateReshardManifest(manifest, p.GetJobId(), p.GetTaskId(), p.GetRunId(), p.GetTaskPlanDigest())
}

func (t *reshardTask) DropTaskOnWorker(cluster session.Cluster) {
	p := t.task.Load()
	if t.GetNodeID() != NullNodeID {
		err := cluster.DropReshard(t.GetNodeID(), &datapb.DropReshardTaskRequest{JobId: p.GetJobId(), TaskId: p.GetTaskId(), RunId: p.GetRunId()})
		if err != nil && !isImportOwnershipLost(err) {
			return
		}
	}
	_ = t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateNodeID(NullNodeID))
}

func (t *reshardTask) fail(reason string, code int32) {
	_ = t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason(reason), updateV3FailureCode(code))
	_ = t.importMeta.UpdateJob(context.TODO(), t.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(reason), UpdateJobFailureCode(code))
}
func (t *reshardTask) Clone() ImportTask {
	c := newReshardTask(proto.Clone(t.task.Load()).(*datapb.ReshardTask), t.importMeta, t.meta, t.alloc)
	c.tr, c.times = t.tr, t.times
	return c
}
func (t *reshardTask) MarshalJSON() ([]byte, error) {
	return json.Marshal(metricsinfo.ImportTask{JobID: t.GetJobID(), TaskID: t.GetTaskID(), CollectionID: t.GetCollectionID(), NodeID: t.GetNodeID(), State: t.GetState().String(), Reason: t.GetReason(), TaskType: t.GetType().String()})
}

type importTaskV3 struct {
	task       atomic.Pointer[datapb.ImportTaskV3]
	importMeta ImportMeta
	meta       *meta
	alloc      allocator.Allocator
	tr         *timerecord.TimeRecorder
	times      *taskcommon.Times
	retryTimes int64
}

func newImportTaskV3(p *datapb.ImportTaskV3, importMeta ImportMeta, meta *meta, alloc ...allocator.Allocator) *importTaskV3 {
	t := &importTaskV3{importMeta: importMeta, meta: meta, tr: timerecord.NewTimeRecorder("import v3 task"), times: taskcommon.NewTimes()}
	if len(alloc) > 0 {
		t.alloc = alloc[0]
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
	return v3ImportStateToV2(t.task.Load().GetState())
}
func (t *importTaskV3) GetReason() string                       { return t.task.Load().GetReason() }
func (t *importTaskV3) GetFileStats() []*datapb.ImportFileStats { return nil }
func (t *importTaskV3) GetSource() datapb.ImportTaskSourceV2 {
	return datapb.ImportTaskSourceV2_Request
}
func (t *importTaskV3) GetTR() *timerecord.TimeRecorder { return t.tr }
func (t *importTaskV3) GetTaskType() taskcommon.Type    { return taskcommon.ImportV3 }
func (t *importTaskV3) GetTaskState() taskcommon.State {
	return taskcommon.FromImportV3State(t.task.Load().GetState())
}
func (t *importTaskV3) GetTaskNodeID() int64                             { return t.GetNodeID() }
func (t *importTaskV3) GetTaskSlot() int64                               { return t.task.Load().GetTaskSlot() }
func (t *importTaskV3) GetTaskVersion() int64                            { return t.task.Load().GetRunId() }
func (t *importTaskV3) SetTaskTime(tt taskcommon.TimeType, tm time.Time) { t.times.SetTaskTime(tt, tm) }
func (t *importTaskV3) GetTaskTime(tt taskcommon.TimeType) time.Time     { return tt.GetTaskTime(t.times) }
func (t *importTaskV3) RequireExactSlotAdmission() bool                  { return true }
func (t *importTaskV3) MinimumImportTaskVersion() uint32                 { return 3 }
func (t *importTaskV3) setState(state datapb.ImportTaskStateV2) {
	t.task.Load().State = v2ToImportV3State(state)
}

func (t *importTaskV3) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	p := t.task.Load()
	if p.GetTaskPlanRef() == "" || len(p.GetTaskPlanDigest()) == 0 || p.GetRunId() == 0 {
		t.fail("import v3 task has no persisted plan or run", merr.Code(merr.ErrImportSysFailed))
		return
	}
	job := t.importMeta.GetJob(context.TODO(), p.GetJobId())
	if job == nil || job.GetPlanningSnapshotRef() == "" || len(job.GetPlanningSnapshotDigest()) == 0 {
		t.fail("import v3 task has no persisted planning snapshot", merr.Code(merr.ErrImportSysFailed))
		return
	}
	plan, err := t.loadPlanForDispatch()
	if err != nil {
		t.fail(err.Error(), merr.Code(err))
		return
	}
	req := &datapb.ImportTaskV3Request{
		JobId: p.GetJobId(), TaskId: p.GetTaskId(), RunId: p.GetRunId(), TaskPlanRef: p.GetTaskPlanRef(),
		TaskPlanDigest: p.GetTaskPlanDigest(), OutputPrefix: p.GetOutputPrefix(), OutputSegmentIds: p.GetOutputSegmentIds(),
		LogIdRange: p.GetLogIdRange(), TaskSlot: p.GetTaskSlot(), PlanningGeneration: p.GetPlanningGeneration(),
		StorageConfig: createStorageConfig(), MergeFanIn: plan.GetMergeFanIn(),
		PlanningSnapshotRef: job.GetPlanningSnapshotRef(), PlanningSnapshotDigest: job.GetPlanningSnapshotDigest(),
		PluginContext: GetReadPluginContext(job.GetOptions()),
	}
	WrapPluginContext(t.GetCollectionID(), job.GetSchema().GetProperties(), req)
	err = cluster.CreateImportV3(nodeID, req, t.GetCollectionID())
	if err != nil {
		mlog.Warn(context.TODO(), "create import v3 task failed", WrapTaskLog(t, mlog.Err(err))...)
		if isImportTerminalError(err) {
			t.fail(err.Error(), importFailureCode(err))
		}
		return
	}
	if err := t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_InProgress), UpdateNodeID(nodeID)); err != nil {
		mlog.Warn(context.TODO(), "persist import v3 task running state failed", WrapTaskLog(t, mlog.Err(err))...)
	}
}

func (t *importTaskV3) loadPlanForDispatch() (*datapb.ImportTaskPlan, error) {
	p := t.task.Load()
	if t.meta == nil || t.meta.chunkManager == nil {
		return nil, merr.WrapErrImportSysFailedMsg("import v3 task plan storage is unavailable")
	}
	payload, err := t.meta.chunkManager.Read(context.TODO(), p.GetTaskPlanRef())
	if err != nil {
		return nil, merr.Wrap(err, "read import v3 task plan")
	}
	if !bytes.Equal(importV3Digest(payload), p.GetTaskPlanDigest()) {
		return nil, merr.WrapErrDataIntegrityMsg("import v3 task plan digest mismatch")
	}
	plan := &datapb.ImportTaskPlan{}
	if err := proto.Unmarshal(payload, plan); err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "unmarshal import v3 task plan")
	}
	if plan.GetJobId() != p.GetJobId() || plan.GetTaskId() != p.GetTaskId() ||
		plan.GetPlanningGeneration() != p.GetPlanningGeneration() || plan.GetMergeFanIn() < 2 || plan.GetMergeFanIn() > 1024 ||
		plan.GetRequiredLogIds() != p.GetLogIdRange().GetEnd()-p.GetLogIdRange().GetBegin() {
		return nil, merr.WrapErrDataIntegrityMsg("import v3 task plan dispatch contract mismatch")
	}
	return plan, nil
}

func (t *importTaskV3) QueryTaskOnWorker(cluster session.Cluster) {
	p := t.task.Load()
	resp, err := cluster.QueryImportV3(t.GetNodeID(), &datapb.QueryImportTaskV3Request{JobId: p.GetJobId(), TaskId: p.GetTaskId(), RunId: p.GetRunId()})
	if err != nil {
		if isImportOwnershipLost(err) {
			t.prepareRetry(cluster)
		} else if isImportTerminalError(err) {
			t.fail(err.Error(), importFailureCode(err))
		} else {
			t.prepareRetry(cluster)
		}
		return
	}
	if resp.GetState() == datapb.ImportTaskV3_Retry {
		if isImportTerminalFailureCode(resp.GetFailureCode()) {
			t.fail(resp.GetReason(), resp.GetFailureCode())
			return
		}
		t.prepareRetry(cluster)
		return
	}
	if resp.GetState() == datapb.ImportTaskV3_Failed {
		t.fail(resp.GetReason(), resp.GetFailureCode())
		return
	}
	if resp.GetState() == datapb.ImportTaskV3_Completed {
		if t.GetState() != datapb.ImportTaskStateV2_InProgress {
			return
		}
		if err := t.acceptResult(resp.GetResultRef(), resp.GetResultDigest()); err != nil {
			t.fail(err.Error(), merr.Code(err))
			return
		}
		if err := t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateV3Result(resp.GetResultRef(), resp.GetResultDigest()), UpdateState(datapb.ImportTaskStateV2_Completed)); err != nil {
			mlog.Warn(context.TODO(), "persist accepted import v3 result marker failed", WrapTaskLog(t, mlog.Err(err))...)
		}
	}
}

func (t *importTaskV3) prepareRetry(cluster session.Cluster) {
	p := t.task.Load()
	if t.alloc == nil || t.meta == nil || p.GetLogIdRange() == nil {
		t.fail("import v3 retry resources are unavailable", merr.Code(merr.ErrImportSysFailed))
		return
	}
	type skeletonIdentity struct {
		partitionID, storageVersion int64
		schemaVersion               int32
		vchannel                    string
	}
	identities := make([]skeletonIdentity, len(p.GetOutputSegmentIds()))
	for i, segmentID := range p.GetOutputSegmentIds() {
		segment := t.meta.GetSegment(context.TODO(), segmentID)
		if segment == nil {
			t.fail("import v3 retry skeleton is missing", merr.Code(merr.ErrDataIntegrity))
			return
		}
		identities[i] = skeletonIdentity{
			partitionID: segment.GetPartitionID(), vchannel: segment.GetInsertChannel(),
			schemaVersion: segment.GetSchemaVersion(), storageVersion: segment.GetStorageVersion(),
		}
	}
	if t.GetNodeID() != NullNodeID {
		_ = cluster.DropImportV3(t.GetNodeID(), &datapb.DropImportTaskV3Request{JobId: p.GetJobId(), TaskId: p.GetTaskId(), RunId: p.GetRunId()})
	}
	oldSegments := append([]int64(nil), p.GetOutputSegmentIds()...)
	job := t.importMeta.GetJob(context.TODO(), p.GetJobId())
	if job == nil {
		t.fail("import v3 retry job is missing", merr.Code(merr.ErrImportSysFailed))
		return
	}
	newSegments := make([]int64, len(oldSegments))
	cleanupNewSegments := func() {
		if len(newSegments) > 0 {
			_ = t.meta.UpdateSegmentsInfo(context.TODO(), dropImportV3Skeletons(newSegments, false))
		}
	}
	for i, identity := range identities {
		segment, err := AllocImportSegment(context.TODO(), t.alloc, t.meta,
			p.GetJobId(), p.GetTaskId(), p.GetCollectionId(), identity.partitionID,
			identity.vchannel, job.GetDataTs(), datapb.SegmentLevel_L1, identity.storageVersion)
		if err != nil {
			cleanupNewSegments()
			t.fail(err.Error(), merr.Code(merr.ErrImportSysFailed))
			return
		}
		newSegments[i] = segment.GetID()
		if err := t.meta.UpdateSegmentsInfo(context.TODO(), prepareImportV3Skeleton(segment.GetID(), identity.schemaVersion)); err != nil {
			cleanupNewSegments()
			t.fail(err.Error(), merr.Code(merr.ErrImportSysFailed))
			return
		}
	}
	// The existing log range is fixed for a plan. A retried run gets a fresh
	// range so a late old writer cannot reuse current segment log IDs.
	begin, end, err := t.alloc.AllocN(p.GetLogIdRange().GetEnd() - p.GetLogIdRange().GetBegin())
	if err != nil {
		cleanupNewSegments()
		t.fail(err.Error(), merr.Code(merr.ErrImportSysFailed))
		return
	}
	newRun := p.GetRunId() + 1
	prefix := metautil.BuildImportV3ImportOutputPath(p.GetJobId(), p.GetTaskId())
	if err := t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), updateImportV3Run(newRun, prefix, newSegments, &datapb.IDRange{Begin: begin, End: end})); err != nil {
		cleanupNewSegments()
		t.fail(err.Error(), merr.Code(merr.ErrImportSysFailed))
		return
	}
	if len(oldSegments) > 0 {
		if err := t.meta.UpdateSegmentsInfo(context.TODO(), dropImportV3Skeletons(oldSegments, false)); err != nil {
			mlog.Warn(context.TODO(), "drop old import v3 retry skeletons failed", WrapTaskLog(t, mlog.Err(err))...)
		}
	}
}

func prepareImportV3Skeleton(segmentID int64, schemaVersion int32) UpdateOperator {
	return func(pack *updateSegmentPack) bool {
		segment := pack.Get(segmentID)
		if segment == nil {
			return pack.fail(merr.WrapErrImportSysFailedMsg("import v3 skeleton %d is missing", segmentID))
		}
		segment.SchemaVersion = schemaVersion
		segment.IsInvisible = true
		return true
	}
}

func (t *importTaskV3) acceptResult(ref string, digest []byte) error {
	if t.meta == nil || t.meta.chunkManager == nil {
		return merr.WrapErrImportSysFailedMsg("import v3 result storage is unavailable")
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
	manifest, err := loadImportResultManifestV3(context.TODO(), t.meta.chunkManager, ref, p.GetOutputPrefix(), digest)
	if err != nil {
		return err
	}
	if err := validateImportResultManifest(manifest, p.GetJobId(), p.GetTaskId(), p.GetRunId(), p.GetPlanningGeneration(), p.GetTaskPlanDigest(), p.GetOutputSegmentIds()); err != nil {
		return err
	}
	return applyImportResultManifest(context.TODO(), t.meta, p.GetCollectionId(), schemaVersion, manifest)
}

func (t *importTaskV3) DropTaskOnWorker(cluster session.Cluster) {
	p := t.task.Load()
	if t.GetNodeID() != NullNodeID {
		err := cluster.DropImportV3(t.GetNodeID(), &datapb.DropImportTaskV3Request{JobId: p.GetJobId(), TaskId: p.GetTaskId(), RunId: p.GetRunId()})
		if err != nil && !isImportOwnershipLost(err) {
			return
		}
	}
	if t.meta != nil && len(p.GetOutputSegmentIds()) > 0 {
		zeroOnly := t.GetState() == datapb.ImportTaskStateV2_Completed
		if err := t.meta.UpdateSegmentsInfo(context.TODO(), dropImportV3Skeletons(p.GetOutputSegmentIds(), zeroOnly)); err != nil {
			mlog.Warn(context.TODO(), "drop import v3 skeletons failed", WrapTaskLog(t, mlog.Err(err))...)
			return
		}
	}
	_ = t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateNodeID(NullNodeID))
}
func (t *importTaskV3) fail(reason string, code int32) {
	_ = t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason(reason), updateV3FailureCode(code))
	_ = t.importMeta.UpdateJob(context.TODO(), t.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(reason), UpdateJobFailureCode(code))
}
func (t *importTaskV3) Clone() ImportTask {
	c := newImportTaskV3(proto.Clone(t.task.Load()).(*datapb.ImportTaskV3), t.importMeta, t.meta, t.alloc)
	c.tr, c.times = t.tr, t.times
	return c
}
func (t *importTaskV3) MarshalJSON() ([]byte, error) {
	return json.Marshal(metricsinfo.ImportTask{JobID: t.GetJobID(), TaskID: t.GetTaskID(), CollectionID: t.GetCollectionID(), NodeID: t.GetNodeID(), State: t.GetState().String(), Reason: t.GetReason(), TaskType: t.GetType().String()})
}
