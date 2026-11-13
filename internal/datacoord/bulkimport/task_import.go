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
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/taskresource"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/importutilv2/importid"
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

var _ ImportTask = (*Task)(nil)

type Task struct {
	Task atomic.Pointer[datapb.ImportTaskV2]

	alloc      allocator.Allocator
	meta       *meta.Meta
	ImportMeta ImportMeta
	Tr         *timerecord.TimeRecorder
	times      *taskcommon.Times
	retryTimes int64
}

func (t *Task) GetJobID() int64 {
	return t.Task.Load().GetJobID()
}

func (t *Task) GetTaskID() int64 {
	return t.Task.Load().GetTaskID()
}

func (t *Task) GetCollectionID() int64 {
	return t.Task.Load().GetCollectionID()
}

func (t *Task) GetNodeID() int64 {
	return t.Task.Load().GetNodeID()
}

func (t *Task) GetState() datapb.ImportTaskStateV2 {
	return t.Task.Load().GetState()
}

func (t *Task) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	t.times.SetTaskTime(timeType, time)
}

func (t *Task) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(t.times)
}

func (t *Task) GetTaskVersion() int64 {
	return t.retryTimes
}

func (t *Task) GetReason() string {
	return t.Task.Load().GetReason()
}

func (t *Task) GetFileStats() []*datapb.ImportFileStats {
	return t.Task.Load().GetFileStats()
}

func (t *Task) GetSegmentIDs() []int64 {
	return t.Task.Load().GetSegmentIDs()
}

func (t *Task) GetSortedSegmentIDs() []int64 {
	return t.Task.Load().GetSortedSegmentIDs()
}

func (t *Task) GetSource() datapb.ImportTaskSourceV2 {
	return t.Task.Load().GetSource()
}

func (t *Task) GetCreatedTime() string {
	return t.Task.Load().GetCreatedTime()
}

func (t *Task) GetCompleteTime() string {
	return t.Task.Load().GetCompleteTime()
}

func (t *Task) GetTaskType() taskcommon.Type {
	return taskcommon.Import
}

func (t *Task) GetTaskState() taskcommon.State {
	return taskcommon.FromImportState(t.GetState())
}

func (t *Task) GetTaskNodeID() int64 {
	return t.GetNodeID()
}

func (t *Task) GetTaskSlot() int64 {
	return int64(CalculateTaskSlot(t, t.ImportMeta))
}

func (t *Task) setState(state datapb.ImportTaskStateV2) {
	t.Task.Load().State = state
}

func (t *Task) setReason(reason string) {
	t.Task.Load().Reason = reason
}

func (t *Task) setCompleteTime(completeTime string) {
	t.Task.Load().CompleteTime = completeTime
}

func (t *Task) setNodeID(nodeID int64) {
	t.Task.Load().NodeID = nodeID
}

func (t *Task) setSegmentIDs(segmentIDs []UniqueID) {
	t.Task.Load().SegmentIDs = segmentIDs
}

func (t *Task) setStatsSegmentIDs(segmentIDs []UniqueID) {
	t.Task.Load().SortedSegmentIDs = segmentIDs
}

// GetTaskResource prices an import by one read buffer per file, mirroring
// what the worker allocates (taskresource.Import). A snapshot-source task is
// priced on what its readers reserve, delete budgets included
// (snapshotImportBufferBytes), from the same formula as its slot; the memory
// factor applies to its row buffers only. It is not cached, and does not need
// to be: the inputs are the task's file stats and three cheap job fields, so
// re-reading them every round costs nothing a cache would save, and a job
// amended between enqueue and dispatch is priced on what it will actually run
// with.
func (t *Task) GetTaskResource() (taskcommon.Resource, bool) {
	job := t.ImportMeta.GetJob(context.TODO(), t.GetJobID())
	if job == nil {
		return taskresource.Default(), false
	}
	perFile := importFileBufferSize(t, job)
	if rowBytes, deleteBytes, ok := snapshotImportBufferBytes(t, job, perFile); ok {
		return taskresource.Import(rowBytes, deleteBytes), true
	}
	return taskresource.Import(taskresource.ImportBufferedBytes(t.GetFileStats(), perFile), 0), true
}

// NodeMemoryShare implements task.NodeMemoryBounded. Every reader of a
// non-L0 import reserves its buffer from the worker's import allocator, so the
// task's read buffers never exceed the allocator's share of the node; the
// hashed batches waiting for sync come on top and are covered by the import
// memory factor (taskresource.ImportTaskNodeMemoryShare). An L0 import does
// not go through that allocator and is not bounded.
func (t *Task) NodeMemoryShare() float64 {
	job := t.ImportMeta.GetJob(context.TODO(), t.GetJobID())
	if job == nil || importutilv2.IsL0Import(job.GetOptions()) {
		return 0
	}
	return taskresource.ImportTaskNodeMemoryShare()
}

func (t *Task) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	mlog.Info(context.TODO(), "processing pending import task...", WrapTaskLog(t)...)
	job := t.ImportMeta.GetJob(context.TODO(), t.GetJobID())
	req, err := AssembleImportRequest(t, job, t.meta, t.alloc)
	if err != nil {
		mlog.Warn(context.TODO(), "assemble import request failed", WrapTaskLog(t, mlog.Err(err))...)
		if errors.Is(err, importid.ErrIDRangeTooSmall) || errors.Is(err, errSnapshotTaskSource) {
			// Immutable source mismatches and insufficient reservations cannot
			// be repaired by retrying. Neither the source nor the per-file range
			// and row count changes by rescheduling, so fail the job now
			// and keep the precise reason -- otherwise the job stays Importing
			// (checkImportingJob only advances once every task is Completed) until
			// tryTimeoutJob overwrites the reason with a generic timeout message.
			//
			// Only the job is updated, as in preimport: the checker's tryFailingTasks
			// marks this task Failed on the next tick.
			if updateErr := t.ImportMeta.UpdateJob(context.TODO(), t.GetJobID(),
				UpdateJobState(internalpb.ImportJobState_Failed),
				UpdateJobReason(err.Error())); updateErr != nil {
				mlog.Warn(context.TODO(), "failed to mark import job failed after assemble error",
					WrapTaskLog(t, mlog.Err(updateErr))...)
			}
			return
		}
		t.retryTimes++
		return
	}
	resource, _ := t.GetTaskResource()
	err = cluster.CreateImport(nodeID, req, t.GetTaskSlot(), resource)
	if err != nil {
		mlog.Warn(context.TODO(), "import failed", WrapTaskLog(t, mlog.Err(err))...)
		t.retryTimes++
		return
	}
	err = t.ImportMeta.UpdateTask(context.TODO(), t.GetTaskID(),
		UpdateState(datapb.ImportTaskStateV2_InProgress),
		UpdateNodeID(nodeID))
	if err != nil {
		mlog.Warn(context.TODO(), "update import task failed", WrapTaskLog(t, mlog.Err(err))...)
		return
	}
	pendingDuration := t.GetTR().RecordSpan()
	metrics.ImportTaskLatency.WithLabelValues(metrics.ImportStagePending, t.GetType().String()).Observe(float64(pendingDuration.Milliseconds()))
	mlog.Info(context.TODO(), "import task start to execute", WrapTaskLog(t, mlog.Int64("scheduledNodeID", nodeID), mlog.Duration("taskTimeCost/pending", pendingDuration))...)
}

func (t *Task) QueryTaskOnWorker(cluster session.Cluster) {
	req := &datapb.QueryImportRequest{
		JobID:  t.GetJobID(),
		TaskID: t.GetTaskID(),
	}
	resp, err := cluster.QueryImport(t.GetNodeID(), req)
	if err != nil || resp.GetState() == datapb.ImportTaskStateV2_Retry {
		// Clear partial progress recorded from the failed attempt. Otherwise,
		// if the retried attempt's PickSegment lands on a different subset of
		// the preallocated segments, the segments it skips will keep stale
		// NumOfRows without insert binlogs — causing sort compaction to fail
		// with "unexpected row count" or EOF.
		if segmentIDs := t.GetSegmentIDs(); len(segmentIDs) > 0 {
			if resetErr := t.meta.UpdateSegmentsInfo(context.TODO(), meta.ResetImportingSegmentRows(segmentIDs...)); resetErr != nil {
				mlog.Warn(context.TODO(), "failed to reset import segment row counts on retry",
					WrapTaskLog(t, mlog.Err(resetErr))...)
				return
			}
		}
		updateErr := t.ImportMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Pending))
		if updateErr != nil {
			mlog.Warn(context.TODO(), "failed to update import task state to pending", WrapTaskLog(t, mlog.Err(updateErr))...)
		}
		mlog.Info(context.TODO(), "reset import task state to pending due to error occurs", WrapTaskLog(t, mlog.Err(err), mlog.String("reason", resp.GetReason()))...)
		return
	}
	if resp.GetState() == datapb.ImportTaskStateV2_Failed {
		err = t.ImportMeta.UpdateJob(context.TODO(), t.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(resp.GetReason()))
		if err != nil {
			mlog.Warn(context.TODO(), "failed to update job state to Failed", mlog.FieldJobID(t.GetJobID()), mlog.Err(err))
		}
		mlog.Warn(context.TODO(), "import failed", WrapTaskLog(t, mlog.String("reason", resp.GetReason()))...)
		return
	}

	collInfo := t.meta.GetCollection(t.GetCollectionID())
	dbName := ""
	if collInfo != nil {
		dbName = collInfo.DatabaseName
	}

	if resp.GetState() == datapb.ImportTaskStateV2_InProgress || resp.GetState() == datapb.ImportTaskStateV2_Completed {
		for _, info := range resp.GetImportSegmentsInfo() {
			segment := t.meta.GetSegment(context.TODO(), info.GetSegmentID())
			if info.GetImportedRows() <= segment.GetNumOfRows() {
				continue // rows not changed, no need to update
			}
			diff := info.GetImportedRows() - segment.GetNumOfRows()
			op := meta.UpdateImportedRows(info.GetSegmentID(), info.GetImportedRows())
			err = t.meta.UpdateSegmentsInfo(context.TODO(), op)
			if err != nil {
				mlog.Warn(context.TODO(), "update import segment rows failed", WrapTaskLog(t, mlog.Err(err))...)
				return
			}
			mlog.Info(context.TODO(), "update import segment rows done", WrapTaskLog(t, mlog.FieldSegmentID(info.GetSegmentID()), mlog.Int64("importedRows", info.GetImportedRows()))...)

			metrics.DataCoordBulkVectors.WithLabelValues(
				dbName,
				strconv.FormatInt(t.GetCollectionID(), 10),
			).Add(float64(diff))
		}
	}
	if resp.GetState() == datapb.ImportTaskStateV2_Completed {
		totalRows := int64(0)
		job := t.ImportMeta.GetJob(context.TODO(), t.GetJobID())
		for _, info := range resp.GetImportSegmentsInfo() {
			// try to parse path and fill logID
			err = binlog.CompressBinLogs(info.GetBinlogs(), info.GetDeltalogs(), info.GetStatslogs(), info.GetBm25Logs())
			if err != nil {
				mlog.Warn(context.TODO(), "fail to CompressBinLogs for import binlogs",
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

			opAccept := acceptImportSegmentOperator(info.GetSegmentID())
			opBinlog := meta.UpdateBinlogsOperator(info.GetSegmentID(), info.GetBinlogs(), info.GetStatslogs(), info.GetDeltalogs(), info.GetBm25Logs())
			opManifest := meta.UpdateManifest(info.GetSegmentID(), info.GetManifestPath())
			opState := meta.UpdateStatusOperator(info.GetSegmentID(), commonpb.SegmentState_Flushed)
			opPosition := meta.UpdateImportSegmentPosition(info.GetSegmentID(), minTs, maxTs)
			// Persist the producer-built Statistics wholesale (chained after
			// UpdateBinlogsOperator so it wins over the array-derived value);
			// when nil it array-derives from the arrays just set above.
			opStats := meta.UpdateSegmentStats(info.GetSegmentID(), info.GetStats())
			err = t.meta.UpdateSegmentsInfo(context.TODO(), opAccept, opBinlog, opManifest, opState, opPosition, opStats)
			if err != nil {
				updateErr := t.ImportMeta.UpdateJob(context.TODO(), t.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(err.Error()))
				if updateErr != nil {
					mlog.Warn(context.TODO(), "failed to update job state to Failed", mlog.FieldJobID(t.GetJobID()), mlog.Err(updateErr))
				}
				mlog.Warn(context.TODO(), "update import segment binlogs failed", WrapTaskLog(t, mlog.String("err", err.Error()))...)
				return
			}
			mlog.Info(context.TODO(), "update import segment info done", WrapTaskLog(t,
				mlog.FieldSegmentID(info.GetSegmentID()),
				mlog.Uint64("minTs", minTs),
				mlog.Uint64("maxTs", maxTs),
				mlog.Any("segmentInfo", info))...)
			totalRows += info.GetImportedRows()
		}
		completeTime := time.Now().Format(time.RFC3339)
		err = t.ImportMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Completed), UpdateCompleteTime(completeTime))
		if err != nil {
			mlog.Warn(context.TODO(), "update import task failed", WrapTaskLog(t, mlog.Err(err))...)
			return
		}
		importDuration := t.GetTR().RecordSpan()
		metrics.ImportTaskLatency.WithLabelValues(metrics.ImportStageImport, t.GetType().String()).Observe(float64(importDuration.Milliseconds()))
		mlog.Info(context.TODO(), "import done", WrapTaskLog(t, mlog.Int64("totalRows", totalRows), mlog.Duration("taskTimeCost/import", importDuration))...)
	}
	mlog.Info(context.TODO(), "query import", WrapTaskLog(t, mlog.String("respState", resp.GetState().String()),
		mlog.String("reason", resp.GetReason()))...)
}

func (t *Task) DropTaskOnWorker(cluster session.Cluster) {
	err := DropImportTask(t, cluster, t.ImportMeta)
	if err != nil {
		mlog.Warn(context.TODO(), "drop import failed", WrapTaskLog(t, mlog.Err(err))...)
		return
	}
	mlog.Info(context.TODO(), "drop import task done", WrapTaskLog(t, mlog.FieldNodeID(t.GetNodeID()))...)
}

func (t *Task) GetType() TaskType {
	return ImportTaskType
}

func (t *Task) GetTR() *timerecord.TimeRecorder {
	return t.Tr
}

func (t *Task) Clone() ImportTask {
	cloned := &Task{
		alloc:      t.alloc,
		meta:       t.meta,
		ImportMeta: t.ImportMeta,
		Tr:         t.Tr,
		times:      t.times,
	}
	cloned.Task.Store(typeutil.Clone(t.Task.Load()))
	return cloned
}

func (t *Task) MarshalJSON() ([]byte, error) {
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

// acceptImportSegmentOperator checks that a preallocated import segment can
// still take the DataNode's result. The segment is Importing until the first
// acceptance and Flushed when the task is replayed after a crash. Any other
// state means DropCollection, DropPartition or a failed-job GC retired the
// segment while the task was running, and its result must not revive it.
func acceptImportSegmentOperator(segmentID int64) meta.UpdateOperator {
	return func(pack *meta.UpdateSegmentPack) bool {
		segment := pack.Get(segmentID)
		if segment == nil {
			return pack.Fail(merr.WrapErrSegmentNotFound(segmentID, "import segment is missing"))
		}
		switch segment.GetState() {
		case commonpb.SegmentState_Importing, commonpb.SegmentState_Flushed:
			return true
		default:
			return pack.Fail(merr.WrapErrSegmentNotFound(segmentID,
				"import segment is "+segment.GetState().String()+", its result is not accepted"))
		}
	}
}

// importFileBufferSize mirrors importv2.ImportTask.GetBufferSize on the worker:
// the base buffer per (vchannel, partition) pair of the request; an L0 import
// uses the base buffer as is (importv2.L0ImportTask.GetBufferSize).
//
// The partitions are the ones the request carries (importTaskPartitionIDs),
// not the job's: a snapshot-source task imports into a single target
// partition, and the worker sizes its buffer from the request.
//
// It is deliberately NOT capped at the largest file. GetBufferSize reads a
// largest-file cap from the task's own ImportTaskV2.FileStats, but the worker
// never fills that field for an import task (NewImportTask leaves it nil), so
// the cap never fires there and capping here would under-price every import.
// The worker's remaining clamp, a percentage of its machine, is not applied.
func importFileBufferSize(task ImportTask, job ImportJob) int64 {
	base := Params.DataNodeCfg.ImportBaseBufferSize.GetAsInt64()
	if importutilv2.IsL0Import(job.GetOptions()) {
		return base
	}
	return base * int64(len(job.GetVchannels())) * int64(len(importTaskPartitionIDs(task, job)))
}
