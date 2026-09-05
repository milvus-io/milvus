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

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
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

var _ ImportTask = (*importTask)(nil)

type importTask struct {
	task atomic.Pointer[datapb.ImportTaskV2]

	// ctx is the checker's process context for scheduler callbacks, whose
	// interface does not carry a context.
	ctx        context.Context
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

func (t *importTask) GetTaskVersion() int64 {
	return t.task.Load().GetTaskVersion()
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

func (t *importTask) GetSortedSegmentIDs() []int64 {
	return t.task.Load().GetSortedSegmentIDs()
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

func (t *importTask) GetTaskSlot() int64 {
	return int64(CalculateTaskSlot(t, t.importMeta))
}

func (t *importTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	logCtx := t.ctx
	if logCtx == nil {
		logCtx = context.TODO()
	}
	mlog.Info(logCtx, "processing pending import task...", WrapTaskLog(t)...)
	job := t.importMeta.GetJob(logCtx, t.GetJobID())
	if t.importMeta.GetTask(logCtx, t.GetTaskID()) == nil || job == nil ||
		job.GetState() == internalpb.ImportJobState_Failed ||
		job.GetState() == internalpb.ImportJobState_Completed {
		// GC may have finalized this scheduler identity after the inspector took
		// its enqueue snapshot. End the stale wrapper locally; there is no meta
		// left that could own a new worker attempt.
		local := typeutil.Clone(t.task.Load())
		local.State = datapb.ImportTaskStateV2_None
		t.task.Store(local)
		mlog.Info(logCtx, "discarding stale import task before dispatch", WrapTaskLog(t)...)
		return
	}
	req, err := AssembleImportRequest(t, job, t.meta, t.alloc)
	if err != nil {
		mlog.Warn(logCtx, "assemble import request failed", WrapTaskLog(t, mlog.Err(err))...)
		if errors.Is(err, ErrPKRangeTooSmall) {
			// The one assemble failure a retry cannot fix: the reservation was
			// sized from an upper bound and preimport produced a larger exact
			// count. Neither number changes by rescheduling, so fail the job now
			// and keep the precise reason -- otherwise the job stays Importing
			// (checkImportingJob only advances once every task is Completed) until
			// tryTimeoutJob overwrites the reason with a generic timeout message.
			//
			// Only the job is updated here; the checker's tryFailingTasks marks
			// this task Failed on the next tick.
			if updateErr := t.importMeta.UpdateJob(context.TODO(), t.GetJobID(),
				UpdateJobState(internalpb.ImportJobState_Failed),
				UpdateJobReason(err.Error())); updateErr != nil {
				mlog.Warn(logCtx, "failed to mark import job failed after assemble error",
					WrapTaskLog(t, mlog.Err(updateErr))...)
			}
		}
		return
	}
	// Persist the assignment before crossing the at-least-once Create boundary.
	// Recording it afterwards loses the attempt whenever the worker accepted the
	// request but its response did not come back: the task stays Pending naming
	// nobody, so nothing can reclaim that attempt, and the scheduler hands the
	// same task ID to a second node while the first keeps running it. Failing to
	// write leaves the task Pending and undispatched, which is the safe side.
	err = t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(),
		UpdateState(datapb.ImportTaskStateV2_InProgress),
		UpdateNodeID(nodeID))
	if err != nil {
		mlog.Warn(logCtx, "failed to persist import assignment, not sending task",
			WrapTaskLog(t, mlog.FieldNodeID(nodeID), mlog.Err(err))...)
		return
	}
	err = cluster.CreateImport(nodeID, req, t.GetTaskSlot())
	if err != nil {
		mlog.Warn(logCtx, "import failed", WrapTaskLog(t, mlog.Err(err))...)
		// Import retries rotate task and segment IDs, so an ambiguous old worker
		// cannot collide with the replacement. Persist retry debt now; the
		// scheduler releases/drops this attempt and the inspector performs the
		// replacement on ImportScheduleInterval.
		t.handoffRetry(context.TODO(), cluster, err.Error())
		return
	}
	pendingDuration := t.GetTR().RecordSpan()
	metrics.ImportTaskLatency.WithLabelValues(metrics.ImportStagePending).Observe(float64(pendingDuration.Milliseconds()))
	mlog.Info(logCtx, "import task start to execute", WrapTaskLog(t, mlog.Int64("scheduledNodeID", nodeID), mlog.Duration("taskTimeCost/pending", pendingDuration))...)
}

func (t *importTask) QueryTaskOnWorker(cluster session.Cluster) {
	logCtx := t.ctx
	if logCtx == nil {
		logCtx = context.TODO()
	}
	req := &datapb.QueryImportRequest{
		JobID:  t.GetJobID(),
		TaskID: t.GetTaskID(),
	}
	resp, err := cluster.QueryImport(t.GetNodeID(), req)
	if t.importMeta.GetTask(t.ctx, t.GetTaskID()) == nil {
		oldTaskState := typeutil.Clone(t.task.Load())
		oldTaskState.State = datapb.ImportTaskStateV2_Failed
		t.task.Store(oldTaskState)
		mlog.Info(logCtx, "discarding import result for a task no longer in metadata", WrapTaskLog(t)...)
		return
	}
	if err != nil || resp.GetState() == datapb.ImportTaskStateV2_Retry ||
		resp.GetState() == datapb.ImportTaskStateV2_None {
		ctx := t.ctx
		reason := ""
		if resp != nil {
			reason = resp.GetReason()
		}
		if err != nil {
			reason = err.Error()
		}
		if t.importMeta.GetJob(ctx, t.GetJobID()) == nil {
			orphanReason := "import job is gone: " + reason
			if updateErr := t.importMeta.UpdateTask(ctx, t.GetTaskID(),
				UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason(orphanReason)); updateErr != nil {
				mlog.Warn(ctx, "failed to retire orphan import task", WrapTaskLog(t, mlog.Err(updateErr))...)
				local := typeutil.Clone(t.task.Load())
				local.State = datapb.ImportTaskStateV2_Failed
				local.Reason = orphanReason
				t.task.Store(local)
			}
			return
		}
		// Query only records retry debt. The import inspector owns the interval
		// and performs the catalog transaction that removes this old task and
		// publishes a fresh task/segment set.
		t.handoffRetry(ctx, cluster, reason)
		mlog.Info(ctx, "import attempt handed to business retry",
			WrapTaskLog(t, mlog.Err(err), mlog.String("reason", reason))...)
		return
	}
	if resp.GetState() == datapb.ImportTaskStateV2_Failed {
		err = t.importMeta.UpdateJob(context.TODO(), t.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(resp.GetReason()))
		if err != nil {
			mlog.Warn(logCtx, "failed to update job state to Failed", mlog.FieldJobID(t.GetJobID()), mlog.Err(err))
			return
		}
		if taskErr := t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(),
			UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason(resp.GetReason())); taskErr != nil {
			mlog.Warn(logCtx, "failed to update import task state to Failed", WrapTaskLog(t, mlog.Err(taskErr))...)
		}
		mlog.Warn(logCtx, "import failed", WrapTaskLog(t, mlog.String("reason", resp.GetReason()))...)
		return
	}

	// Import correctness does not depend on collection-name metadata here; it
	// is used only as an optional metric label. Avoid an additional RootCoord
	// lookup on the result-commit path solely for that label.
	dbName := ""

	var missingSegmentIDs []int64
	if resp.GetState() == datapb.ImportTaskStateV2_InProgress || resp.GetState() == datapb.ImportTaskStateV2_Completed {
		missingSegmentIDs, err = t.validateImportResponseSegments(logCtx, resp.GetImportSegmentsInfo(),
			resp.GetState() == datapb.ImportTaskStateV2_Completed)
		if err != nil {
			if updateErr := t.importMeta.UpdateJob(logCtx, t.GetJobID(),
				UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(err.Error())); updateErr != nil {
				mlog.Warn(logCtx, "failed to update invalid import job to Failed",
					mlog.FieldJobID(t.GetJobID()), mlog.Err(updateErr))
				return
			}
			if updateErr := t.importMeta.UpdateTask(logCtx, t.GetTaskID(),
				UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason(err.Error())); updateErr != nil {
				mlog.Warn(logCtx, "failed to update invalid import task to Failed", WrapTaskLog(t, mlog.Err(updateErr))...)
			}
			mlog.Warn(logCtx, "invalid import segment result", WrapTaskLog(t, mlog.Err(err))...)
			return
		}
		for _, info := range resp.GetImportSegmentsInfo() {
			segment := t.meta.GetSegment(context.TODO(), info.GetSegmentID())
			if segment == nil || info.GetImportedRows() <= segment.GetNumOfRows() {
				continue // rows not changed, no need to update
			}
			diff := info.GetImportedRows() - segment.GetNumOfRows()
			op := UpdateImportedRows(info.GetSegmentID(), info.GetImportedRows())
			err = t.meta.UpdateSegmentsInfo(context.TODO(), op)
			if err != nil {
				mlog.Warn(logCtx, "update import segment rows failed", WrapTaskLog(t, mlog.Err(err))...)
				return
			}
			mlog.Info(logCtx, "update import segment rows done", WrapTaskLog(t, mlog.FieldSegmentID(info.GetSegmentID()), mlog.Int64("importedRows", info.GetImportedRows()))...)

			metrics.DataCoordBulkVectors.WithLabelValues(
				dbName,
				strconv.FormatInt(t.GetCollectionID(), 10),
			).Add(float64(diff))
		}
	}
	if resp.GetState() == datapb.ImportTaskStateV2_Completed {
		totalRows := int64(0)
		job := t.importMeta.GetJob(context.TODO(), t.GetJobID())
		for _, info := range resp.GetImportSegmentsInfo() {
			// try to parse path and fill logID
			err = binlog.CompressBinLogs(info.GetBinlogs(), info.GetDeltalogs(), info.GetStatslogs(), info.GetBm25Logs())
			if err != nil {
				mlog.Warn(logCtx, "fail to CompressBinLogs for import binlogs",
					WrapTaskLog(t, mlog.FieldSegmentID(info.GetSegmentID()), mlog.Err(err))...)
				return
			}

			// Extract actual timestamps for segment positions. Prefer the
			// producer-reported Statistics (it knows the V3 manifest-side
			// footprint); fall back to array reconstruction for rolling
			// upgrade where the datanode ships no Statistics.
			// L0 imports carry only deletes; non-L0 imports carry inserts.
			importStats := info.GetStats()
			if importStats == nil {
				importStats = storage.BuildStatsFromFieldBinlogs(info.GetBinlogs(), info.GetStatslogs(), info.GetBm25Logs(), info.GetDeltalogs())
			}
			var minTs, maxTs uint64
			isL0Import := importutilv2.IsL0Import(job.GetOptions())
			if isL0Import {
				minTs = importStats.GetDeltaTimestampFrom()
				maxTs = importStats.GetDeltaTimestampTo()
			} else {
				minTs = importStats.GetTimestampFrom()
				maxTs = importStats.GetTimestampTo()
			}

			opBinlog := UpdateBinlogsOperator(info.GetSegmentID(), info.GetBinlogs(), info.GetStatslogs(), info.GetDeltalogs(), info.GetBm25Logs())
			opManifest := UpdateManifest(info.GetSegmentID(), info.GetManifestPath())
			opState := UpdateStatusOperator(info.GetSegmentID(), commonpb.SegmentState_Flushed)
			opPosition := UpdateImportSegmentPosition(info.GetSegmentID(), minTs, maxTs)
			// Persist the producer-built Statistics wholesale (chained after
			// UpdateBinlogsOperator so it wins over the array-derived value);
			// when nil it array-derives from the arrays just set above.
			opStats := UpdateSegmentStats(info.GetSegmentID(), info.GetStats())
			err = t.meta.UpdateSegmentsInfo(context.TODO(), opBinlog, opManifest, opState, opPosition, opStats)
			if err != nil {
				updateErr := t.importMeta.UpdateJob(context.TODO(), t.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(err.Error()))
				if updateErr != nil {
					mlog.Warn(logCtx, "failed to update job state to Failed", mlog.FieldJobID(t.GetJobID()), mlog.Err(updateErr))
				}
				mlog.Warn(logCtx, "update import segment binlogs failed", WrapTaskLog(t, mlog.Err(err))...)
				return
			}
			mlog.Info(logCtx, "update import segment info done", WrapTaskLog(t,
				mlog.FieldSegmentID(info.GetSegmentID()),
				mlog.Uint64("minTs", minTs),
				mlog.Uint64("maxTs", maxTs),
				mlog.Any("segmentInfo", info))...)
			totalRows += info.GetImportedRows()
		}
		if len(missingSegmentIDs) > 0 {
			operators := make([]UpdateOperator, 0, len(missingSegmentIDs))
			for _, segmentID := range missingSegmentIDs {
				operators = append(operators, UpdateStatusOperator(segmentID, commonpb.SegmentState_Dropped))
			}
			if err = t.meta.UpdateSegmentsInfo(logCtx, operators...); err != nil {
				updateErr := t.importMeta.UpdateJob(logCtx, t.GetJobID(),
					UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(err.Error()))
				if updateErr != nil {
					mlog.Warn(logCtx, "failed to update job state to Failed", mlog.FieldJobID(t.GetJobID()), mlog.Err(updateErr))
				}
				mlog.Warn(logCtx, "failed to retire empty import segments",
					WrapTaskLog(t, mlog.Int64s("segmentIDs", missingSegmentIDs), mlog.Err(err))...)
				return
			}
		}
		completeTime := time.Now().Format("2006-01-02T15:04:05Z07:00")
		err = t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Completed), UpdateCompleteTime(completeTime))
		if err != nil {
			mlog.Warn(logCtx, "update import task failed", WrapTaskLog(t, mlog.Err(err))...)
			return
		}
		importDuration := t.GetTR().RecordSpan()
		metrics.ImportTaskLatency.WithLabelValues(metrics.ImportStageImport).Observe(float64(importDuration.Milliseconds()))
		mlog.Info(logCtx, "import done", WrapTaskLog(t, mlog.Int64("totalRows", totalRows), mlog.Duration("taskTimeCost/import", importDuration))...)
	}
	mlog.Info(logCtx, "query import", WrapTaskLog(t, mlog.String("respState", resp.GetState().String()),
		mlog.String("reason", resp.GetReason()))...)
}

// validateImportResponseSegments checks the worker result against the output
// identities preallocated for this task before any segment metadata is changed.
// A completed response may omit a preallocated segment only when it still has
// the exact empty importing shape; the caller retires that unused identity.
func (t *importTask) validateImportResponseSegments(ctx context.Context, infos []*datapb.ImportSegmentInfo,
	completed bool,
) ([]int64, error) {
	job := t.importMeta.GetJob(ctx, t.GetJobID())
	if job == nil {
		return nil, merr.WrapErrImportSysFailedMsg("import job %d is missing", t.GetJobID())
	}
	if job.GetCollectionID() != t.GetCollectionID() {
		return nil, merr.WrapErrImportSysFailedMsg(
			"import task %d belongs to collection %d, expected %d",
			t.GetTaskID(), t.GetCollectionID(), job.GetCollectionID())
	}

	expected := make(map[int64]struct{}, len(t.GetSegmentIDs()))
	for _, segmentID := range t.GetSegmentIDs() {
		if _, ok := expected[segmentID]; ok {
			return nil, merr.WrapErrImportSysFailedMsg(
				"import task %d preallocates segment %d more than once", t.GetTaskID(), segmentID)
		}
		expected[segmentID] = struct{}{}
	}

	seen := make(map[int64]struct{}, len(infos))
	for _, info := range infos {
		segmentID := info.GetSegmentID()
		if _, ok := expected[segmentID]; !ok {
			return nil, merr.WrapErrImportSysFailedMsg(
				"import task %d returned unexpected segment %d", t.GetTaskID(), segmentID)
		}
		if _, ok := seen[segmentID]; ok {
			return nil, merr.WrapErrImportSysFailedMsg(
				"import task %d returned segment %d more than once", t.GetTaskID(), segmentID)
		}
		seen[segmentID] = struct{}{}

		segment := t.meta.GetSegment(ctx, segmentID)
		if err := validateImportResponseSegmentOwner(job, t.GetCollectionID(), segment); err != nil {
			return nil, err
		}
		if segment.GetState() != commonpb.SegmentState_Importing &&
			(!completed || segment.GetState() != commonpb.SegmentState_Flushed) {
			return nil, merr.WrapErrImportSysFailedMsg(
				"import task %d returned segment %d in state %s",
				t.GetTaskID(), segmentID, segment.GetState().String())
		}
	}

	if !completed {
		return nil, nil
	}
	missing := make([]int64, 0, len(expected)-len(seen))
	for _, segmentID := range t.GetSegmentIDs() {
		if _, ok := seen[segmentID]; ok {
			continue
		}
		segment := t.meta.GetSegment(ctx, segmentID)
		if err := validateImportResponseSegmentOwner(job, t.GetCollectionID(), segment); err != nil {
			return nil, err
		}
		if segment.GetState() != commonpb.SegmentState_Importing &&
			segment.GetState() != commonpb.SegmentState_Dropped {
			return nil, merr.WrapErrImportSysFailedMsg(
				"import task %d omitted segment %d in state %s",
				t.GetTaskID(), segmentID, segment.GetState().String())
		}
		if !isEmptyImportSegment(segment) {
			return nil, merr.WrapErrImportSysFailedMsg(
				"import task %d omitted non-empty segment %d", t.GetTaskID(), segmentID)
		}
		if segment.GetState() != commonpb.SegmentState_Dropped {
			missing = append(missing, segmentID)
		}
	}
	return missing, nil
}

func validateImportResponseSegmentOwner(job ImportJob, collectionID int64, segment *SegmentInfo) error {
	if segment == nil {
		return merr.WrapErrImportSysFailedMsg("preallocated import segment is missing")
	}
	if segment.GetCollectionID() != collectionID || segment.GetCollectionID() != job.GetCollectionID() {
		return merr.WrapErrImportSysFailedMsg(
			"import segment %d belongs to collection %d, expected %d",
			segment.GetID(), segment.GetCollectionID(), job.GetCollectionID())
	}
	if len(job.GetPartitionIDs()) > 0 && !typeutil.NewSet(job.GetPartitionIDs()...).Contain(segment.GetPartitionID()) {
		return merr.WrapErrImportSysFailedMsg(
			"import segment %d belongs to partition %d outside job partitions %v",
			segment.GetID(), segment.GetPartitionID(), job.GetPartitionIDs())
	}
	if len(job.GetVchannels()) > 0 && !typeutil.NewSet(job.GetVchannels()...).Contain(segment.GetInsertChannel()) {
		return merr.WrapErrImportSysFailedMsg(
			"import segment %d belongs to channel %q outside job channels %v",
			segment.GetID(), segment.GetInsertChannel(), job.GetVchannels())
	}
	if !segment.GetIsImporting() {
		return merr.WrapErrImportSysFailedMsg("import segment %d is already published", segment.GetID())
	}
	return nil
}

func isEmptyImportSegment(segment *SegmentInfo) bool {
	return segment.GetNumOfRows() == 0 &&
		len(segment.GetBinlogs()) == 0 && len(segment.GetStatslogs()) == 0 &&
		len(segment.GetDeltalogs()) == 0 && len(segment.GetBm25Statslogs()) == 0 &&
		len(segment.GetTextStatsLogs()) == 0 && len(segment.GetJsonKeyStats()) == 0 &&
		segment.GetManifestPath() == ""
}

// handoffRetry records retry debt for an import attempt. Import replacements
// use fresh task and segment IDs, so worker cleanup may remain best effort; the
// inspector performs the identity rotation before dispatching any replacement.
func (t *importTask) handoffRetry(ctx context.Context, cluster session.Cluster, reason string) {
	if ctx == nil {
		ctx = context.TODO()
	}
	if err := t.importMeta.UpdateTask(ctx, t.GetTaskID(),
		UpdateState(datapb.ImportTaskStateV2_Retry), UpdateReason(reason)); err != nil {
		mlog.Warn(ctx, "failed to persist import retry state", WrapTaskLog(t, mlog.Err(err))...)
		local := typeutil.Clone(t.task.Load())
		local.State = datapb.ImportTaskStateV2_Retry
		local.Reason = reason
		t.task.Store(local)
	}
	if err := dropImportTaskOnWorker(t, cluster); err != nil {
		mlog.Warn(ctx, "failed to drop old import attempt", WrapTaskLog(t, mlog.Err(err))...)
	}
}

func (t *importTask) DropTaskOnWorker(cluster session.Cluster) {
	logCtx := t.ctx
	if logCtx == nil {
		logCtx = context.TODO()
	}
	err := DropImportTask(t, cluster, t.importMeta)
	if err != nil {
		mlog.Warn(logCtx, "drop import failed", WrapTaskLog(t, mlog.Err(err))...)
		return
	}
	mlog.Info(logCtx, "drop import task done", WrapTaskLog(t, mlog.FieldNodeID(t.GetNodeID()))...)
}

func (t *importTask) GetType() TaskType {
	return ImportTaskType
}

func (t *importTask) GetTR() *timerecord.TimeRecorder {
	return t.tr
}

func (t *importTask) Clone() ImportTask {
	cloned := &importTask{
		ctx:        t.ctx,
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
