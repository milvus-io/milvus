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
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var _ CompactionTask = (*l0CompactionTask)(nil)

type l0CompactionTask struct {
	taskProto atomic.Value // *datapb.CompactionTask

	allocator allocator.Allocator
	meta      CompactionMeta

	times                *taskcommon.Times
	committedV3Manifests map[int64]string
}

func (t *l0CompactionTask) GetTaskID() int64 {
	return t.GetTaskProto().GetPlanID()
}

func (t *l0CompactionTask) GetTaskType() taskcommon.Type {
	return taskcommon.Compaction
}

func (t *l0CompactionTask) GetTaskState() taskcommon.State {
	return taskcommon.FromCompactionState(t.GetTaskProto().GetState())
}

func (t *l0CompactionTask) GetTaskSlot() int64 {
	batchSize := paramtable.Get().CommonCfg.BloomFilterApplyBatchSize.GetAsInt()
	factor := paramtable.Get().DataCoordCfg.L0DeleteCompactionSlotUsage.GetAsInt64()
	slot := factor * t.GetTaskProto().GetTotalRows() / int64(batchSize)
	if slot < 1 {
		return 1
	}
	return slot
}

func (t *l0CompactionTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	t.times.SetTaskTime(timeType, time)
}

func (t *l0CompactionTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(t.times)
}

func (t *l0CompactionTask) GetTaskVersion() int64 {
	return int64(t.GetTaskProto().GetRetryTimes())
}

func (t *l0CompactionTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	log := mlog.With(mlog.Int64("triggerID", t.GetTaskProto().GetTriggerID()), mlog.FieldNodeID(t.GetTaskProto().GetNodeID()))
	plan, err := t.BuildCompactionRequest()
	if err != nil {
		log.Warn(context.TODO(), "l0CompactionTask failed to build compaction request", mlog.Err(err))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
		if err != nil {
			log.Warn(context.TODO(), "l0CompactionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
		}
		return
	}

	// Check if this is a fast finish case (no target segments to compact with)
	// Fast finish plan only contains L0 input segments, no target L1/L2 segments
	if len(plan.SegmentBinlogs) == len(t.GetTaskProto().GetInputSegments()) {
		log.Info(context.TODO(), "l0CompactionTask fast finish: no target segments, directly marking L0 segments as dropped",
			mlog.Int64("planID", t.GetTaskProto().GetPlanID()))

		// Save segment meta with empty output segments (marks L0 input segments as dropped)
		if err = t.saveSegmentMeta([]*datapb.CompactionSegment{}); err != nil {
			log.Warn(context.TODO(), "l0CompactionTask fast finish failed to save segment meta", mlog.Err(err))
			err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
			if err != nil {
				log.Warn(context.TODO(), "l0CompactionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
			}
			return
		}

		// Transition to meta_saved state
		if err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_meta_saved)); err != nil {
			log.Warn(context.TODO(), "l0CompactionTask fast finish failed to save task meta_saved state", mlog.Err(err))
			return
		}

		log.Info(context.TODO(), "l0CompactionTask fast finish completed", mlog.Int64("planID", t.GetTaskProto().GetPlanID()))
		return
	}

	err = cluster.CreateCompaction(nodeID, plan, t.GetTaskProto().GetCollectionID())
	if err != nil {
		originNodeID := t.GetTaskProto().GetNodeID()
		log.Warn(context.TODO(), "l0CompactionTask failed to notify compaction tasks to DataNode",
			mlog.Int64("planID", t.GetTaskProto().GetPlanID()),
			mlog.FieldNodeID(originNodeID),
			mlog.Err(err))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
		if err != nil {
			log.Warn(context.TODO(), "l0CompactionTask failed to updateAndSaveTaskMeta", mlog.Int64("planID", t.GetTaskProto().GetPlanID()), mlog.Err(err))
			return
		}
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", originNodeID), t.GetTaskProto().GetType().String(), metrics.Executing).Dec()
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", NullNodeID), t.GetTaskProto().GetType().String(), metrics.Pending).Inc()
		return
	}

	err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_executing), setNodeID(nodeID))
	if err != nil {
		log.Warn(context.TODO(), "l0CompactionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
	}
}

func (t *l0CompactionTask) QueryTaskOnWorker(cluster session.Cluster) {
	log := mlog.With(mlog.Int64("planID", t.GetTaskProto().GetPlanID()), mlog.FieldNodeID(t.GetTaskProto().GetNodeID()))
	result, err := cluster.QueryCompaction(t.GetTaskProto().GetNodeID(), &datapb.CompactionStateRequest{
		PlanID: t.GetTaskProto().GetPlanID(),
	})
	if err != nil || result == nil {
		log.Warn(context.TODO(), "l0CompactionTask failed to get compaction result", mlog.Err(err))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
		if err != nil {
			log.Warn(context.TODO(), "update l0 compaction task meta failed", mlog.Err(err))
		}
		return
	}
	switch result.GetState() {
	case datapb.CompactionTaskState_completed:
		err = t.meta.ValidateSegmentStateBeforeCompleteCompactionMutation(t.GetTaskProto())
		if err != nil {
			t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
			return
		}

		if err = t.saveSegmentMeta(result.GetSegments()); err != nil {
			log.Warn(context.TODO(), "l0CompactionTask failed to save segment meta", mlog.Err(err))
			return
		}

		if err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_meta_saved)); err != nil {
			log.Warn(context.TODO(), "l0CompactionTask failed to save task meta_saved state", mlog.Err(err))
			return
		}
		UpdateCompactionSegmentSizeMetrics(result.GetSegments())
		t.processMetaSaved()
	case datapb.CompactionTaskState_pipelining, datapb.CompactionTaskState_executing:
		return
	case datapb.CompactionTaskState_timeout:
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_timeout))
		if err != nil {
			log.Warn(context.TODO(), "update clustering compaction task meta failed", mlog.Err(err))
			return
		}
	case datapb.CompactionTaskState_failed:
		if err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed)); err != nil {
			log.Warn(context.TODO(), "l0CompactionTask failed to set task failed state", mlog.Err(err))
			return
		}
	default:
		log.Error(context.TODO(), "not support compaction task state", mlog.String("state", result.GetState().String()))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed))
		if err != nil {
			log.Warn(context.TODO(), "update clustering compaction task meta failed", mlog.Err(err))
			return
		}
	}
}

func (t *l0CompactionTask) DropTaskOnWorker(cluster session.Cluster) {
	if t.hasAssignedWorker() {
		err := cluster.DropCompaction(t.GetTaskProto().GetNodeID(), t.GetTaskProto().GetPlanID())
		if err != nil {
			mlog.Warn(context.TODO(), "l0CompactionTask unable to drop compaction plan", mlog.Int64("planID", t.GetTaskProto().GetPlanID()), mlog.Err(err))
		}
	}
}

func (t *l0CompactionTask) GetTaskProto() *datapb.CompactionTask {
	task := t.taskProto.Load()
	if task == nil {
		return nil
	}
	return task.(*datapb.CompactionTask)
}

func newL0CompactionTask(t *datapb.CompactionTask, allocator allocator.Allocator, meta CompactionMeta) *l0CompactionTask {
	task := &l0CompactionTask{
		allocator:            allocator,
		meta:                 meta,
		times:                taskcommon.NewTimes(),
		committedV3Manifests: make(map[int64]string),
	}
	task.taskProto.Store(t)
	return task
}

// Note: return True means exit this state machine.
// ONLY return True for Completed, Failed
func (t *l0CompactionTask) Process() bool {
	switch t.GetTaskProto().GetState() {
	case datapb.CompactionTaskState_meta_saved:
		return t.processMetaSaved()
	case datapb.CompactionTaskState_completed:
		return t.processCompleted()
	case datapb.CompactionTaskState_failed:
		return true
	case datapb.CompactionTaskState_timeout:
		return true
	default:
		return false
	}
}

func (t *l0CompactionTask) processMetaSaved() bool {
	err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_completed))
	if err != nil {
		mlog.Warn(context.TODO(), "l0CompactionTask unable to processMetaSaved", mlog.Int64("planID", t.GetTaskProto().GetPlanID()), mlog.Err(err))
		return false
	}
	return t.processCompleted()
}

func (t *l0CompactionTask) processCompleted() bool {
	t.resetSegmentCompacting()
	task := t.taskProto.Load().(*datapb.CompactionTask)
	mlog.Info(context.TODO(), "l0CompactionTask processCompleted done", mlog.Int64("planID", task.GetPlanID()),
		mlog.Duration("costs", time.Duration(task.GetEndTime()-task.GetStartTime())*time.Second))
	return true
}

func (t *l0CompactionTask) doClean() error {
	log := mlog.With(mlog.Int64("planID", t.GetTaskProto().GetPlanID()))
	err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_cleaned))
	if err != nil {
		log.Warn(context.TODO(), "l0CompactionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
		return err
	}

	// resetSegmentCompacting must be the last step of Clean, to make sure resetSegmentCompacting only called once
	// otherwise, it may unlock segments locked by other compaction tasks
	t.resetSegmentCompacting()
	log.Info(context.TODO(), "l0CompactionTask clean done")
	return nil
}

func (t *l0CompactionTask) Clean() bool {
	return t.doClean() == nil
}

func (t *l0CompactionTask) SetTask(task *datapb.CompactionTask) {
	t.taskProto.Store(task)
}

func (t *l0CompactionTask) GetLabel() string {
	return fmt.Sprintf("%d-%s", t.GetTaskProto().PartitionID, t.GetTaskProto().GetChannel())
}

func (t *l0CompactionTask) NeedReAssignNodeID() bool {
	return t.GetTaskProto().GetState() == datapb.CompactionTaskState_pipelining && (!t.hasAssignedWorker())
}

func (t *l0CompactionTask) ShadowClone(opts ...compactionTaskOpt) *datapb.CompactionTask {
	taskClone := proto.Clone(t.GetTaskProto()).(*datapb.CompactionTask)
	for _, opt := range opts {
		opt(taskClone)
	}
	return taskClone
}

func (t *l0CompactionTask) selectFlushedSegment() ([]*SegmentInfo, []*datapb.CompactionSegmentBinlogs, error) {
	taskProto := t.taskProto.Load().(*datapb.CompactionTask)
	// Select flushed L1/L2 segments for LevelZero compaction that meets the condition:
	// dmlPos < triggerInfo.pos
	flushedSegments := t.meta.SelectSegments(context.TODO(), WithCollection(taskProto.GetCollectionID()), SegmentFilterFunc(func(info *SegmentInfo) bool {
		return (taskProto.GetPartitionID() == common.AllPartitionsID || info.GetPartitionID() == taskProto.GetPartitionID()) &&
			info.GetInsertChannel() == taskProto.GetChannel() &&
			(info.GetState() == commonpb.SegmentState_Sealed || isFlushState(info.GetState())) &&
			!info.GetIsImporting() &&
			info.GetLevel() != datapb.SegmentLevel_L0 &&
			segmentEffectiveTs(info.SegmentInfo) < taskProto.GetPos().GetTimestamp()
	}))

	sealedSegBinlogs := []*datapb.CompactionSegmentBinlogs{}
	for _, info := range flushedSegments {
		// Sealed is unexpected, fail fast
		if info.GetState() == commonpb.SegmentState_Sealed {
			return nil, nil, merr.WrapErrServiceInternalMsg("L0 compaction selected invalid sealed segment %d", info.GetID())
		}

		sealedSegBinlogs = append(sealedSegBinlogs, &datapb.CompactionSegmentBinlogs{
			SegmentID:           info.GetID(),
			Field2StatslogPaths: info.GetStatslogs(),
			InsertChannel:       info.GetInsertChannel(),
			Level:               info.GetLevel(),
			CollectionID:        info.GetCollectionID(),
			PartitionID:         info.GetPartitionID(),
			IsSorted:            info.GetIsSorted(),
			IsSortedByNamespace: info.GetIsSortedByNamespace(),
			Manifest:            info.GetManifestPath(),
			CommitTimestamp:     info.GetCommitTimestamp(),
		})
	}

	return flushedSegments, sealedSegBinlogs, nil
}

func (t *l0CompactionTask) BuildCompactionRequest() (*datapb.CompactionPlan, error) {
	taskProto := t.taskProto.Load().(*datapb.CompactionTask)
	compactionParams, err := compaction.GenerateJSONParams(taskProto.GetSchema())
	if err != nil {
		return nil, err
	}
	plan := &datapb.CompactionPlan{
		PlanID:        taskProto.GetPlanID(),
		StartTime:     taskProto.GetStartTime(),
		Type:          taskProto.GetType(),
		Channel:       taskProto.GetChannel(),
		CollectionTtl: taskProto.GetCollectionTtl(),
		TotalRows:     taskProto.GetTotalRows(),
		Schema:        taskProto.GetSchema(),
		SlotUsage:     t.GetSlotUsage(),
		JsonParams:    compactionParams,
	}

	log := mlog.With(mlog.FieldTaskID(taskProto.GetTriggerID()), mlog.Int64("planID", plan.GetPlanID()))
	segments := make([]*SegmentInfo, 0)
	for _, segID := range taskProto.GetInputSegments() {
		segInfo := t.meta.GetHealthySegment(context.TODO(), segID)
		if segInfo == nil {
			return nil, merr.WrapErrSegmentNotFound(segID)
		}
		plan.SegmentBinlogs = append(plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			SegmentID:           segID,
			CollectionID:        segInfo.GetCollectionID(),
			PartitionID:         segInfo.GetPartitionID(),
			Level:               segInfo.GetLevel(),
			InsertChannel:       segInfo.GetInsertChannel(),
			Deltalogs:           segInfo.GetDeltalogs(),
			IsSorted:            segInfo.GetIsSorted(),
			IsSortedByNamespace: segInfo.GetIsSortedByNamespace(),
			Manifest:            segInfo.GetManifestPath(),
			CommitTimestamp:     segInfo.GetCommitTimestamp(),
		})
		segments = append(segments, segInfo)
	}

	flushedSegments, flushedSegBinlogs, err := t.selectFlushedSegment()
	if err != nil {
		log.Warn(context.TODO(), "invalid L0 compaction plan, unable to select flushed segments", mlog.Err(err))
		return nil, err
	}
	if len(flushedSegments) == 0 {
		// Fast finish: no target segments to compact with, return plan with only L0 segments
		log.Info(context.TODO(), "l0Compaction available non-L0 Segments is empty, will fast finish",
			mlog.Any("target position", taskProto.GetPos()))
		return plan, nil
	}

	segments = append(segments, flushedSegments...)
	logIDRange, err := PreAllocateBinlogIDs(t.allocator, segments, nil)
	if err != nil {
		return nil, err
	}
	plan.PreAllocatedLogIDs = logIDRange
	// BeginLogID is deprecated, but still assign it for compatibility.
	plan.BeginLogID = logIDRange.Begin

	plan.SegmentBinlogs = append(plan.SegmentBinlogs, flushedSegBinlogs...)
	log.Info(context.TODO(), "l0CompactionTask refreshed level zero compaction plan",
		mlog.Any("target position", taskProto.GetPos()),
		mlog.Any("target segments count", len(flushedSegBinlogs)),
		mlog.Any("PreAllocatedLogIDs", logIDRange))

	WrapPluginContext(taskProto.GetCollectionID(), taskProto.GetSchema().GetProperties(), plan)
	return plan, nil
}

func (t *l0CompactionTask) resetSegmentCompacting() {
	t.meta.SetSegmentsCompacting(context.TODO(), t.GetTaskProto().GetInputSegments(), false)
}

func (t *l0CompactionTask) hasAssignedWorker() bool {
	return t.GetTaskProto().GetNodeID() != 0 && t.GetTaskProto().GetNodeID() != NullNodeID
}

func (t *l0CompactionTask) SetNodeID(id UniqueID) error {
	return t.updateAndSaveTaskMeta(setNodeID(id))
}

func (t *l0CompactionTask) SaveTaskMeta() error {
	return t.saveTaskMeta(t.GetTaskProto())
}

func (t *l0CompactionTask) updateAndSaveTaskMeta(opts ...compactionTaskOpt) error {
	// if task state is completed, cleaned, failed, timeout, then do append end time and save
	if t.GetTaskProto().State == datapb.CompactionTaskState_completed ||
		t.GetTaskProto().State == datapb.CompactionTaskState_cleaned ||
		t.GetTaskProto().State == datapb.CompactionTaskState_failed ||
		t.GetTaskProto().State == datapb.CompactionTaskState_timeout {
		ts := time.Now().Unix()
		opts = append(opts, setEndTime(ts))
	}

	task := t.ShadowClone(opts...)
	err := t.saveTaskMeta(task)
	if err != nil {
		return err
	}
	t.SetTask(task)
	return nil
}

func (t *l0CompactionTask) saveTaskMeta(task *datapb.CompactionTask) error {
	return t.meta.SaveCompactionTask(context.TODO(), task)
}

func buildL0V3DeltaLogEntries(segmentID int64, deltalogs []*datapb.FieldBinlog) ([]packed.DeltaLogEntry, error) {
	entries := make([]packed.DeltaLogEntry, 0)
	for _, fieldBinlog := range deltalogs {
		for _, binlog := range fieldBinlog.GetBinlogs() {
			path := binlog.GetLogPath()
			if path == "" {
				return nil, merr.WrapErrServiceInternalMsg("L0 V3 compaction result missing deltalog path for segment %d, logID %d", segmentID, binlog.GetLogID())
			}
			entries = append(entries, packed.DeltaLogEntry{
				Path:       path,
				NumEntries: binlog.GetEntriesNum(),
			})
		}
	}
	return entries, nil
}

func (t *l0CompactionTask) saveSegmentMeta(outputSegs []*datapb.CompactionSegment) error {
	ctx := t.context()
	var operators []UpdateOperator
	v3Deltalogs := make(map[int64][]*datapb.FieldBinlog)
	for _, seg := range outputSegs {
		if len(seg.GetDeltalogs()) > 0 {
			// The manifest transaction must run outside UpdateSegmentsInfo: that
			// method holds segMu, whereas CommitSegmentManifest only holds the
			// per-segment lock while it performs object-storage I/O.
			current := t.meta.GetSegment(ctx, seg.GetSegmentID())
			if current != nil && current.GetStorageVersion() == storage.StorageV3 && current.GetManifestPath() != "" {
				// A target retired by a concurrent compaction while the L0 plan
				// was executing is gone for publication purposes: GetSegment
				// returns dropped segments, and CommitSegmentManifest would only
				// reject one with ErrSegmentNotFound. Skip it so the
				// input-segment retirement below still runs and the task reaches
				// meta_saved instead of re-polling a permanent error forever.
				if !isSegmentHealthy(current) {
					mlog.Warn(ctx, "L0 target segment no longer healthy; skipping deltalog publication",
						mlog.Int64("planID", t.GetTaskProto().GetPlanID()),
						mlog.FieldSegmentID(seg.GetSegmentID()))
					continue
				}
				// Append rather than assign: a duplicated target in the worker
				// output must keep both entries, as the serial path did (the
				// commit-side dedup handles overlaps).
				v3Deltalogs[seg.GetSegmentID()] = append(v3Deltalogs[seg.GetSegmentID()], seg.GetDeltalogs()...)
				continue
			}
			operators = append(operators, AddL0DeltalogsAndUpdateManifestOperator(
				seg.GetSegmentID(),
				seg.GetDeltalogs(),
				compaction.CreateStorageConfig(),
				t.committedV3Manifests,
			))
		}
	}

	if err := t.commitL0V3DeltalogsParallel(ctx, v3Deltalogs); err != nil {
		return err
	}

	for _, segID := range t.GetTaskProto().InputSegments {
		operators = append(operators, UpdateStatusOperator(segID, commonpb.SegmentState_Dropped), UpdateCompactedOperator(segID))
	}

	mlog.Info(context.TODO(), "meta update: update segments info for level zero compaction",
		mlog.Int64("planID", t.GetTaskProto().GetPlanID()),
	)

	return t.meta.UpdateSegmentsInfo(ctx, operators...)
}

// commitL0V3DeltalogsParallel publishes the V3 targets' deltalogs through
// CommitSegmentManifest with bounded concurrency. The expensive half of each
// commit — the loon manifest transaction — is object-storage I/O performed
// outside segMu, so the fan-out restores the parallelism the legacy operator
// path gets from the same dataCoord.compaction.levelzero.manifestUpdatePoolSize
// pool, while each segment keeps the per-segment ExpectedManifest CAS
// semantics unchanged. The catalog half still lands as one catalog.Update per
// segment; collapsing those into a single batched transaction is the batched
// CommitSegmentManifests follow-up (see the TODO in
// ddl_callbacks_batch_update_manifest.go).
func (t *l0CompactionTask) commitL0V3DeltalogsParallel(ctx context.Context, deltalogsBySegment map[int64][]*datapb.FieldBinlog) error {
	if len(deltalogsBySegment) == 0 {
		return nil
	}
	poolSize := paramtable.Get().DataCoordCfg.L0ManifestUpdatePoolSize.GetAsInt()
	if poolSize < 1 {
		poolSize = 1
	}
	if poolSize > len(deltalogsBySegment) {
		poolSize = len(deltalogsBySegment)
	}
	pool := conc.NewPool[struct{}](poolSize)
	defer pool.Release()

	futures := make([]*conc.Future[struct{}], 0, len(deltalogsBySegment))
	for segmentID, deltalogs := range deltalogsBySegment {
		futures = append(futures, pool.Submit(func() (struct{}, error) {
			err := t.commitL0V3Deltalogs(ctx, segmentID, deltalogs)
			// A target dropped between the saveSegmentMeta health check and the
			// commit (or during manifest I/O) is the same benign terminal
			// outcome as the pre-check there: the deltalogs are obsolete along
			// with the segment. Swallow it so the input-segment retirement still
			// runs — the ErrSegmentNotFound contract the stats caller already
			// honors in SetJobInfo.
			if err != nil && errors.Is(err, merr.ErrSegmentNotFound) {
				mlog.Warn(ctx, "L0 target segment dropped during manifest commit; skipping deltalog publication",
					mlog.Int64("planID", t.GetTaskProto().GetPlanID()),
					mlog.FieldSegmentID(segmentID), mlog.Err(err))
				return struct{}{}, nil
			}
			return struct{}{}, err
		}))
	}
	return conc.BlockOnAll(futures...)
}

func (t *l0CompactionTask) commitL0V3Deltalogs(ctx context.Context, segmentID int64, deltalogs []*datapb.FieldBinlog) error {
	current := t.meta.GetSegment(ctx, segmentID)
	if current == nil {
		return merr.WrapErrSegmentNotFound(segmentID)
	}
	if current.GetStorageVersion() != storage.StorageV3 || current.GetManifestPath() == "" {
		return merr.WrapErrServiceInternalMsg("L0 StorageV3 manifest commit requires a published manifest, segmentID=%d", segmentID)
	}

	// Drop deltalogs already registered on the segment before building the
	// manifest transaction. The same L0 output reaches saveSegmentMeta twice
	// whenever it re-runs (a failed catalog write, or a failed meta_saved
	// task-state write after the catalog write succeeded). Unlike the catalog
	// half, packed manifest commits append delta-log entries without any
	// deduplication, so a blind re-commit would leave duplicate entries in the
	// manifest and bump a fresh revision on every retry. Filtering by
	// (fieldID, logID) makes the manifest re-commit idempotent, mirroring the
	// catalog dedup in addDeltalogsToSegment; a full duplicate short-circuits
	// before any object-storage I/O.
	deltalogs = filterDuplicateFieldBinlogs(current.GetDeltalogs(), deltalogs)
	if len(deltalogs) == 0 {
		return nil
	}

	entries, err := buildL0V3DeltaLogEntries(segmentID, deltalogs)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}

	manifestMeta, ok := t.meta.(interface {
		CommitSegmentManifest(context.Context, SegmentManifestCommit) error
	})
	if !ok {
		return merr.WrapErrServiceInternalMsg("L0 StorageV3 manifest commit requires DataCoord meta implementation")
	}
	if err := manifestMeta.CommitSegmentManifest(ctx, SegmentManifestCommit{
		SegmentID:        segmentID,
		ExpectedManifest: current.GetManifestPath(),
		StorageConfig:    compaction.CreateStorageConfig(),
		Mutation: ManifestMutation{
			Type:    ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{DeltaLogs: entries},
		},
		CatalogMutation: SegmentCatalogMutation{
			// Keep the catalog half of L0 exactly on the established mutation
			// path so merging, stats accumulation, and retry deduplication are
			// shared with the legacy implementation.
			Operators: []UpdateOperator{AddL0DeltalogsOperator(segmentID, deltalogs)},
		},
	}); err != nil {
		return err
	}
	return nil
}

func (t *l0CompactionTask) context() context.Context {
	if meta, ok := t.meta.(*meta); ok && meta.ctx != nil {
		return meta.ctx
	}
	// Unit-test CompactionMeta implementations do not own the DataCoord
	// lifecycle context. Production tasks always take the meta context above.
	return context.Background()
}

func (t *l0CompactionTask) GetSlotUsage() int64 {
	return t.GetTaskSlot()
}
