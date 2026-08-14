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
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ ImportTask = (*importTask)(nil)

type importTask struct {
	task atomic.Pointer[datapb.ImportTaskV2]

	alloc      allocator.Allocator
	meta       *meta
	importMeta ImportMeta
	tr         *timerecord.TimeRecorder
	times      *taskcommon.Times
	retryTimes int64
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
	return t.retryTimes
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

func (t *importTask) GetTaskNodeID() int64 {
	return t.GetNodeID()
}

func (t *importTask) GetTaskSlot() int64 {
	return int64(CalculateTaskSlot(t, t.importMeta))
}

func (t *importTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	mlog.Info(context.TODO(), "processing pending import task...", WrapTaskLog(t)...)
	job := t.importMeta.GetJob(context.TODO(), t.GetJobID())
	req, err := AssembleImportRequest(t, job, t.meta, t.alloc)
	if err != nil {
		mlog.Warn(context.TODO(), "assemble import request failed", WrapTaskLog(t, mlog.Err(err))...)
		if errors.Is(err, ErrPKRangeTooSmall) {
			// The one assemble failure a retry cannot fix: the reservation was
			// sized from an upper bound and preimport produced a larger exact
			// count. Neither number changes by rescheduling, so fail the job now
			// and keep the precise reason -- otherwise the job stays Importing
			// (checkImportingJob only advances once every task is Completed) until
			// tryTimeoutJob overwrites the reason with a generic timeout message.
			//
			// Only the job is updated, as in the DataNode-reported failure path
			// below and in preimport: the checker's tryFailingTasks marks this
			// task Failed on the next tick.
			if updateErr := t.importMeta.UpdateJob(context.TODO(), t.GetJobID(),
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
	err = cluster.CreateImport(nodeID, req, t.GetTaskSlot())
	if err != nil {
		mlog.Warn(context.TODO(), "import failed", WrapTaskLog(t, mlog.Err(err))...)
		t.retryTimes++
		return
	}
	err = t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(),
		UpdateState(datapb.ImportTaskStateV2_InProgress),
		UpdateNodeID(nodeID))
	if err != nil {
		mlog.Warn(context.TODO(), "update import task failed", WrapTaskLog(t, mlog.Err(err))...)
		return
	}
	pendingDuration := t.GetTR().RecordSpan()
	metrics.ImportTaskLatency.WithLabelValues(metrics.ImportStagePending).Observe(float64(pendingDuration.Milliseconds()))
	mlog.Info(context.TODO(), "import task start to execute", WrapTaskLog(t, mlog.Int64("scheduledNodeID", nodeID), mlog.Duration("taskTimeCost/pending", pendingDuration))...)
}

func (t *importTask) QueryTaskOnWorker(cluster session.Cluster) {
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
			if resetErr := t.meta.UpdateSegmentsInfo(context.TODO(), ResetImportingSegmentRows(segmentIDs...)); resetErr != nil {
				mlog.Warn(context.TODO(), "failed to reset import segment row counts on retry",
					WrapTaskLog(t, mlog.Err(resetErr))...)
				return
			}
		}
		updateErr := t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Pending))
		if updateErr != nil {
			mlog.Warn(context.TODO(), "failed to update import task state to pending", WrapTaskLog(t, mlog.Err(updateErr))...)
		}
		mlog.Info(context.TODO(), "reset import task state to pending due to error occurs", WrapTaskLog(t, mlog.Err(err), mlog.String("reason", resp.GetReason()))...)
		return
	}
	if resp.GetState() == datapb.ImportTaskStateV2_Failed {
		err = t.importMeta.UpdateJob(context.TODO(), t.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(resp.GetReason()))
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
			op := UpdateImportedRows(info.GetSegmentID(), info.GetImportedRows())
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
			// L0 segments carry only deletes; other segments carry inserts.
			importStats := info.GetStats()
			if importStats == nil {
				importStats = storage.BuildStatsFromFieldBinlogs(info.GetBinlogs(), info.GetStatslogs(), info.GetBm25Logs(), info.GetDeltalogs())
			}
			segment := t.meta.GetSegment(context.TODO(), info.GetSegmentID())
			isDeleteSegment := segment != nil && segment.GetLevel() == datapb.SegmentLevel_L0
			var minTs, maxTs uint64
			if isDeleteSegment {
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
		completeTime := time.Now().Format("2006-01-02T15:04:05Z07:00")
		err = t.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Completed), UpdateCompleteTime(completeTime))
		if err != nil {
			mlog.Warn(context.TODO(), "update import task failed", WrapTaskLog(t, mlog.Err(err))...)
			return
		}
		importDuration := t.GetTR().RecordSpan()
		metrics.ImportTaskLatency.WithLabelValues(metrics.ImportStageImport).Observe(float64(importDuration.Milliseconds()))
		mlog.Info(context.TODO(), "import done", WrapTaskLog(t, mlog.Int64("totalRows", totalRows), mlog.Duration("taskTimeCost/import", importDuration))...)
	}
	mlog.Info(context.TODO(), "query import", WrapTaskLog(t, mlog.String("respState", resp.GetState().String()),
		mlog.String("reason", resp.GetReason()))...)
}

func (t *importTask) DropTaskOnWorker(cluster session.Cluster) {
	err := DropImportTask(t, cluster, t.importMeta)
	if err != nil {
		mlog.Warn(context.TODO(), "drop import failed", WrapTaskLog(t, mlog.Err(err))...)
		return
	}
	mlog.Info(context.TODO(), "drop import task done", WrapTaskLog(t, mlog.FieldNodeID(t.GetNodeID()))...)
}

func (t *importTask) GetType() TaskType {
	return ImportTaskType
}

func (t *importTask) GetTR() *timerecord.TimeRecorder {
	return t.tr
}

func (t *importTask) Clone() ImportTask {
	cloned := &importTask{
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
