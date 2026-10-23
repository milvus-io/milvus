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

package task

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var _ CompactionTask = (*BumpSchemaVersionTask)(nil)

type BumpSchemaVersionTask struct {
	taskProto          atomic.Value // *datapb.CompactionTask
	allocator          allocator.Allocator
	meta               meta.CompactionMeta
	compactionTaskMeta *CompactionTaskMeta
	ievm               index.IndexEngineVersionManager
	times              *taskcommon.Times
}

func NewBumpSchemaVersionTask(t *datapb.CompactionTask, allocator allocator.Allocator, mt meta.CompactionMeta, compactionTaskMeta *CompactionTaskMeta, ievm index.IndexEngineVersionManager) *BumpSchemaVersionTask {
	task := &BumpSchemaVersionTask{
		allocator:          allocator,
		meta:               mt,
		compactionTaskMeta: compactionTaskMeta,
		ievm:               ievm,
		times:              taskcommon.NewTimes(),
	}
	task.taskProto.Store(t)
	return task
}

func (t *BumpSchemaVersionTask) GetTaskID() int64 {
	return t.GetTaskProto().GetPlanID()
}

func (t *BumpSchemaVersionTask) GetTaskType() taskcommon.Type {
	return taskcommon.Compaction
}

func (t *BumpSchemaVersionTask) GetTaskState() taskcommon.State {
	return taskcommon.FromCompactionState(t.GetTaskProto().GetState())
}

func (t *BumpSchemaVersionTask) GetTaskProto() *datapb.CompactionTask {
	return t.taskProto.Load().(*datapb.CompactionTask)
}

func (t *BumpSchemaVersionTask) GetTaskSlot() int64 {
	return paramtable.Get().DataCoordCfg.BumpSchemaVersionCompactionSlotUsage.GetAsInt64()
}

func (t *BumpSchemaVersionTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	t.times.SetTaskTime(timeType, time)
}

func (t *BumpSchemaVersionTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(t.times)
}

func (t *BumpSchemaVersionTask) GetTaskVersion() int64 {
	return int64(t.GetTaskProto().GetRetryTimes())
}

func (t *BumpSchemaVersionTask) BuildCompactionRequest() (*datapb.CompactionPlan, error) {
	taskProto := t.GetTaskProto()
	if taskProto.GetSchema() == nil {
		return nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction task schema is nil")
	}
	compactionParams, err := compaction.GenerateJSONParams(taskProto.GetSchema())
	if err != nil {
		return nil, err
	}
	plan := &datapb.CompactionPlan{
		PlanID:                    taskProto.GetPlanID(),
		StartTime:                 taskProto.GetStartTime(),
		Type:                      taskProto.GetType(),
		Channel:                   taskProto.GetChannel(),
		CollectionTtl:             taskProto.GetCollectionTtl(),
		TotalRows:                 taskProto.GetTotalRows(),
		Schema:                    taskProto.GetSchema(),
		PreAllocatedSegmentIDs:    taskProto.GetPreAllocatedSegmentIDs(),
		SlotUsage:                 t.GetSlotUsage(),
		MaxSize:                   taskProto.GetMaxSize(),
		JsonParams:                compactionParams,
		CurrentScalarIndexVersion: t.ievm.ResolveScalarIndexVersion(),
		EnableManifestDelta:       true,
	}
	segments := make([]*meta.SegmentInfo, 0, len(taskProto.GetInputSegments()))
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
			FieldBinlogs:        segInfo.GetBinlogs(),
			Field2StatslogPaths: segInfo.GetStatslogs(),
			Deltalogs:           segInfo.GetDeltalogs(),
			IsSorted:            segInfo.GetIsSorted(),
			IsSortedByNamespace: segInfo.GetIsSortedByNamespace(),
			StorageVersion:      segInfo.GetStorageVersion(),
			Manifest:            segInfo.GetManifestPath(),
			CommitTimestamp:     segInfo.GetCommitTimestamp(),
		})
		segments = append(segments, segInfo)
	}

	logIDRange, err := PreAllocateBinlogIDs(t.allocator, segments, taskProto.GetSchema())
	if err != nil {
		return nil, err
	}
	plan.PreAllocatedLogIDs = logIDRange
	plan.BeginLogID = logIDRange.Begin
	meta.WrapPluginContext(taskProto.GetCollectionID(), taskProto.GetSchema().GetProperties(), plan)
	return plan, nil
}

func (t *BumpSchemaVersionTask) GetSlotUsage() int64 {
	return t.GetTaskSlot()
}

func (t *BumpSchemaVersionTask) GetLabel() string {
	return fmt.Sprintf("%d-%s", t.GetTaskProto().GetPartitionID(), t.GetTaskProto().GetChannel())
}

func (t *BumpSchemaVersionTask) SetTask(task *datapb.CompactionTask) {
	t.taskProto.Store(task)
}

func (t *BumpSchemaVersionTask) ShadowClone(opts ...compactionTaskOpt) *datapb.CompactionTask {
	cloned := proto.Clone(t.GetTaskProto()).(*datapb.CompactionTask)
	for _, opt := range opts {
		opt(cloned)
	}
	return cloned
}

func (t *BumpSchemaVersionTask) SetNodeID(nodeID int64) error {
	return t.UpdateAndSaveTaskMeta(SetNodeID(nodeID))
}

func (t *BumpSchemaVersionTask) NeedReAssignNodeID() bool {
	return t.GetTaskProto().GetState() == datapb.CompactionTaskState_pipelining && (t.GetTaskProto().GetNodeID() == 0 || t.GetTaskProto().GetNodeID() == meta.NullNodeID)
}

func (t *BumpSchemaVersionTask) saveTaskMeta(task *datapb.CompactionTask) error {
	return t.compactionTaskMeta.SaveCompactionTask(context.TODO(), task)
}

func (t *BumpSchemaVersionTask) SaveTaskMeta() error {
	return t.saveTaskMeta(t.GetTaskProto())
}

func (t *BumpSchemaVersionTask) Clean() bool {
	return t.doClean() == nil
}

func (t *BumpSchemaVersionTask) doClean() error {
	log := mlog.With(mlog.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
		mlog.Int64("PlanID", t.GetTaskProto().GetPlanID()),
		mlog.FieldCollectionID(t.GetTaskProto().GetCollectionID()))
	err := t.UpdateAndSaveTaskMeta(SetState(datapb.CompactionTaskState_cleaned))
	if err != nil {
		log.Warn(context.TODO(), "bumpSchemaVersionTask fail to updateAndSaveTaskMeta", mlog.Err(err))
		return err
	}
	// resetSegmentCompacting must be the last step of Clean, to make sure resetSegmentCompacting only called once
	// otherwise, it may unlock segments locked by other compaction tasks
	t.resetSegmentCompacting()
	log.Info(context.TODO(), "bumpSchemaVersionTask clean done")
	return nil
}

func (t *BumpSchemaVersionTask) resetSegmentCompacting() {
	t.meta.SetSegmentsCompacting(context.TODO(), t.GetTaskProto().GetInputSegments(), false)
}

func (t *BumpSchemaVersionTask) ProcessFailed() bool {
	return true
}

func (t *BumpSchemaVersionTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	log := mlog.With(mlog.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
		mlog.Int64("PlanID", t.GetTaskProto().GetPlanID()),
		mlog.FieldCollectionID(t.GetTaskProto().GetCollectionID()),
		mlog.FieldNodeID(nodeID))

	plan, err := t.BuildCompactionRequest()
	if err != nil {
		log.Warn(context.TODO(), "bumpSchemaVersionTask failed to build compaction request", mlog.Err(err))
		err = t.UpdateAndSaveTaskMeta(SetState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
		if err != nil {
			log.Warn(context.TODO(), "bumpSchemaVersionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
		}
		return
	}

	err = cluster.CreateCompaction(nodeID, plan, t.GetTaskProto().GetCollectionID())
	if err != nil {
		log.Warn(context.TODO(), "bumpSchemaVersionTask failed to notify compaction tasks to DataNode",
			mlog.Int64("planID", t.GetTaskProto().GetPlanID()),
			mlog.FieldNodeID(nodeID),
			mlog.Err(err))
		err = t.UpdateAndSaveTaskMeta(SetState(datapb.CompactionTaskState_pipelining), SetNodeID(meta.NullNodeID))
		if err != nil {
			log.Warn(context.TODO(), "bumpSchemaVersionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
		}
		return
	}

	log.Info(context.TODO(), "bumpSchemaVersionTask created task on worker", mlog.Int64("planID", t.GetTaskProto().GetPlanID()),
		mlog.FieldNodeID(nodeID))

	err = t.UpdateAndSaveTaskMeta(SetState(datapb.CompactionTaskState_executing), SetNodeID(nodeID))
	if err != nil {
		log.Warn(context.TODO(), "bumpSchemaVersionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
	}
}

func (t *BumpSchemaVersionTask) QueryTaskOnWorker(cluster session.Cluster) {
	log := mlog.With(mlog.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
		mlog.Int64("PlanID", t.GetTaskProto().GetPlanID()),
		mlog.FieldCollectionID(t.GetTaskProto().GetCollectionID()))
	result, err := cluster.QueryCompaction(t.GetTaskProto().GetNodeID(), &datapb.CompactionStateRequest{
		PlanID: t.GetTaskProto().GetPlanID(),
	})
	if err != nil || result == nil {
		if errors.Is(err, merr.ErrCompactionResultNotFound) {
			if dropErr := cluster.DropCompaction(t.GetTaskProto().GetNodeID(), t.GetTaskProto().GetPlanID()); dropErr != nil {
				log.Warn(context.TODO(), "bumpSchemaVersionTask failed to drop task with unavailable result", mlog.Err(dropErr))
				return
			}
		}
		if errors.Is(err, merr.ErrNodeNotFound) || errors.Is(err, merr.ErrCompactionResultNotFound) {
			if err := t.UpdateAndSaveTaskMeta(SetState(datapb.CompactionTaskState_pipelining), SetNodeID(meta.NullNodeID)); err != nil {
				log.Warn(context.TODO(), "bumpSchemaVersionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
			}
		}
		log.Warn(context.TODO(), "bumpSchemaVersionTask failed to get compaction result", mlog.Err(err))
		return
	}
	switch result.GetState() {
	case datapb.CompactionTaskState_completed:
		if len(result.GetSegments()) == 0 {
			log.Warn(context.TODO(), "bumpSchemaVersionTask illegal compaction results: no segments returned")
			if err := t.UpdateAndSaveTaskMeta(SetState(datapb.CompactionTaskState_failed),
				setFailReason("illegal compaction results: no segments returned")); err != nil {
				log.Warn(context.TODO(), "bumpSchemaVersionTask failed to setState failed", mlog.Err(err))
			}
			return
		}
		err = t.meta.ValidateSegmentStateBeforeCompleteCompactionMutation(t.GetTaskProto())
		if err != nil {
			if saveErr := t.UpdateAndSaveTaskMeta(SetState(datapb.CompactionTaskState_failed), setFailReason(err.Error())); saveErr != nil {
				log.Warn(context.TODO(), "bumpSchemaVersionTask failed to setState failed", mlog.Err(saveErr))
			}
			return
		}
		if err := t.SaveSegmentMeta(result); err != nil {
			log.Warn(context.TODO(), "bumpSchemaVersionTask failed to save segment meta", mlog.Err(err))
			if errors.Is(err, merr.ErrIllegalCompactionPlan) {
				if saveErr := t.UpdateAndSaveTaskMeta(SetState(datapb.CompactionTaskState_failed),
					setFailReason(err.Error())); saveErr != nil {
					log.Warn(context.TODO(), "bumpSchemaVersionTask failed to setState failed", mlog.Err(saveErr))
				}
			}
			return
		}
		UpdateCompactionSegmentSizeMetrics(result.GetSegments())
		t.processMetaSaved()
	case datapb.CompactionTaskState_pipelining, datapb.CompactionTaskState_executing:
		return
	case datapb.CompactionTaskState_timeout:
		err = t.UpdateAndSaveTaskMeta(SetState(datapb.CompactionTaskState_timeout), setFailReason("DataNode reported compaction timeout"))
		if err != nil {
			log.Warn(context.TODO(), "bumpSchemaVersionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
			return
		}
	case datapb.CompactionTaskState_failed:
		log.Warn(context.TODO(), "bumpSchemaVersionTask fail in datanode")
		if err := t.UpdateAndSaveTaskMeta(SetState(datapb.CompactionTaskState_failed),
			setFailReason(compactionFailReason(result))); err != nil {
			log.Warn(context.TODO(), "bumpSchemaVersionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
		}
	default:
		log.Error(context.TODO(), "not support compaction task state", mlog.String("state", result.GetState().String()))
		reason := fmt.Sprintf("unsupported compaction state: %s", result.GetState().String())
		if err = t.UpdateAndSaveTaskMeta(SetState(datapb.CompactionTaskState_failed),
			setFailReason(reason)); err != nil {
			log.Warn(context.TODO(), "bumpSchemaVersionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
			return
		}
	}
}

func (t *BumpSchemaVersionTask) DropTaskOnWorker(cluster session.Cluster) {
	log := mlog.With(mlog.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
		mlog.Int64("PlanID", t.GetTaskProto().GetPlanID()),
		mlog.FieldCollectionID(t.GetTaskProto().GetCollectionID()))
	if err := cluster.DropCompaction(t.GetTaskProto().GetNodeID(), t.GetTaskProto().GetPlanID()); err != nil {
		log.Warn(context.TODO(), "bumpSchemaVersionTask unable to drop compaction plan", mlog.Err(err))
	}
}

// Process performs the task's state machine
// Note: return True means exit this state machine.
// ONLY return True for Completed, Failed, Timeout
func (t *BumpSchemaVersionTask) Process() bool {
	log := mlog.With(mlog.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
		mlog.Int64("PlanID", t.GetTaskProto().GetPlanID()),
		mlog.FieldCollectionID(t.GetTaskProto().GetCollectionID()))
	lastState := t.GetTaskProto().GetState().String()
	processResult := false
	switch t.GetTaskProto().GetState() {
	case datapb.CompactionTaskState_meta_saved:
		processResult = t.processMetaSaved()
	case datapb.CompactionTaskState_completed:
		processResult = t.processCompleted()
	case datapb.CompactionTaskState_failed:
		processResult = t.ProcessFailed()
	case datapb.CompactionTaskState_timeout:
		processResult = true
	}
	currentState := t.GetTaskProto().GetState().String()
	if currentState != lastState {
		log.Info(context.TODO(), "schema bump compaction task state changed", mlog.String("lastState", lastState), mlog.String("currentState", currentState))
	}
	return processResult
}

func (t *BumpSchemaVersionTask) SaveSegmentMeta(result *datapb.CompactionPlanResult) error {
	log := mlog.With(mlog.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
		mlog.Int64("PlanID", t.GetTaskProto().GetPlanID()),
		mlog.FieldCollectionID(t.GetTaskProto().GetCollectionID()))
	if err := binlog.CompressCompactionBinlogs(result.GetSegments()); err != nil {
		return err
	}
	var newSegmentIDs []UniqueID
	if isMaterializationResult(result) {
		// In-place schema-bump materialization: DataCoord runs the StorageV3
		// manifest transaction itself via CommitSegmentManifest, which acquires
		// the per-segment lock then segMu and therefore MUST NOT run inside the
		// segMu-held CompleteCompactionMutation. Commit it out here, mirroring how
		// L0 commits V3 deltalogs (see l0CompactionTask.saveSegmentMeta).
		ids, err := t.commitBumpV3Materialization(context.TODO(), result)
		if err != nil {
			return err
		}
		newSegmentIDs = ids
	} else {
		newSegments, err := t.meta.CompleteCompactionMutation(context.TODO(), t.GetTaskProto(), result)
		if err != nil {
			return err
		}
		newSegmentIDs = lo.Map(newSegments, func(s *meta.SegmentInfo, _ int) UniqueID { return s.GetID() })
	}

	for _, newSegID := range newSegmentIDs {
		select {
		case meta.GetBuildIndexChSingleton() <- newSegID:
		default:
		}
	}

	// The SegmentMeta mutation is committed (schema bump rewrites manifests);
	// schedule an asynchronous DataView snapshot reconciliation so consumers
	// observe the new manifest versions.
	if mt, ok := t.meta.(*meta.Meta); ok {
		mt.RecomputeDataView(context.TODO(), t.GetTaskProto().GetCollectionID())
	}

	err := t.UpdateAndSaveTaskMeta(SetState(datapb.CompactionTaskState_meta_saved), setResultSegments(newSegmentIDs))
	if err != nil {
		log.Warn(context.TODO(), "bumpSchemaVersionTask failed to setState meta saved", mlog.Err(err))
		return err
	}
	log.Info(context.TODO(), "bumpSchemaVersionTask success to save segment meta")
	return nil
}

// isMaterializationResult reports whether a bump result is an in-place function
// materialization, which ships a serializable manifest delta for DataCoord to
// commit, rather than a version-bump-only or full-rewrite result adopted through
// CompleteCompactionMutation.
func isMaterializationResult(result *datapb.CompactionPlanResult) bool {
	segs := result.GetSegments()
	return len(segs) == 1 && segs[0].GetManifestDelta() != nil
}

// commitBumpV3Materialization publishes an in-place schema-bump materialization
// through CommitSegmentManifest: DataCoord runs the StorageV3 manifest
// transaction on the segment's CURRENT manifest from the datanode-shipped
// descriptors (rebasing against concurrent commits) and, atomically under segMu,
// upserts the new column groups, advances the schema version, and folds in the
// Stats increment. It returns the segment IDs to enqueue for index building.
//
// This is the schema-bump analog of l0CompactionTask.buildL0V3ManifestCommit
// (which now batches its targets through CommitSegmentManifests) and obeys the
// same lock contract: it never runs while segMu is held.
func (t *BumpSchemaVersionTask) commitBumpV3Materialization(ctx context.Context, result *datapb.CompactionPlanResult) ([]UniqueID, error) {
	if len(t.GetTaskProto().GetInputSegments()) != 1 {
		return nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction should have exactly one input segment")
	}
	if len(result.GetSegments()) != 1 {
		return nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction result should have exactly one segment")
	}
	resultSegment := result.GetSegments()[0]
	segmentID := t.GetTaskProto().GetInputSegments()[0]
	if resultSegment.GetSegmentID() != segmentID {
		return nil, merr.WrapErrIllegalCompactionPlanMsg("schema bump materialization result segment %d does not match input segment %d", resultSegment.GetSegmentID(), segmentID)
	}

	current := t.meta.GetSegment(ctx, segmentID)
	if current == nil {
		return nil, merr.WrapErrSegmentNotFound(segmentID)
	}
	if !meta.IsSegmentHealthy(current) {
		return nil, merr.WrapErrSegmentNotFound(segmentID, "input segment was dropped")
	}
	if current.GetStorageVersion() != storage.StorageV3 || current.GetManifestPath() == "" {
		return nil, merr.WrapErrServiceInternalMsg("schema bump materialization requires a published StorageV3 manifest, segmentID=%d", segmentID)
	}
	if current.GetIsInvisible() {
		return nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction input segment should not be invisible")
	}
	if t.GetTaskProto().GetSchema() == nil {
		return nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction requires task schema")
	}
	newSchemaVersion := t.GetTaskProto().GetSchema().GetVersion()

	// Restart-safe idempotent-replay guard. Materialization advances the segment
	// schema version to newSchemaVersion atomically with the column-group /
	// manifest / Stats commit (all in one CommitSegmentManifest catalog write),
	// and the segment is exclusively locked for compaction, so nothing else
	// advances its schema version. Therefore the persisted schema version is an
	// exactly-once token:
	//   - current < target: not yet applied (or a prior attempt failed before its
	//     catalog write, leaving the pointer and version untouched) -> commit.
	//   - current == target: already applied and persisted -> short-circuit before
	//     any object-storage I/O, so a saveSegmentMeta retry (even across a
	//     DataCoord restart, where a V3 segment's in-memory Binlogs are empty
	//     until rebuilt from the manifest) neither double-adds column groups to
	//     the manifest nor double-counts Stats.
	//   - current > target: a newer bump already superseded this stale task.
	// This replaces the resultManifest==currentManifest replay check the pre-baked
	// adoption path used, and unlike an in-memory Binlogs diff it survives restart.
	if current.GetSchemaVersion() == newSchemaVersion {
		mlog.Info(ctx, "schema bump materialization already applied; skipping manifest commit",
			mlog.Int64("planID", t.GetTaskProto().GetPlanID()),
			mlog.Int64("segmentID", segmentID),
			mlog.Int32("schemaVersion", newSchemaVersion))
		return []UniqueID{segmentID}, nil
	}
	if current.GetSchemaVersion() > newSchemaVersion {
		return nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction schema version is older than input segment")
	}

	delta := resultSegment.GetManifestDelta()
	if delta == nil || len(delta.GetColumnGroups()) == 0 {
		return nil, merr.WrapErrIllegalCompactionPlan("schema bump materialization result missing manifest delta")
	}

	manifestMeta, ok := t.meta.(interface {
		CommitSegmentManifest(context.Context, meta.SegmentManifestCommit) error
	})
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("schema bump materialization requires DataCoord meta implementation")
	}

	// The column groups are merged onto SegmentInfo.Binlogs idempotently by
	// FieldID, so the full result set is safe to pass even on the (schema-version
	// gated) forward path.
	// No ExpectedManifest: a structured mutation is generated from the pointer
	// current under the per-segment commit lock, and CommitSegmentManifest aborts
	// publication itself if the pointer moves during manifest I/O. Pinning the
	// pre-lock read here would only spuriously abort after a benign concurrent
	// commit (e.g. a stats publication) advanced the pointer.
	if err := manifestMeta.CommitSegmentManifest(ctx, meta.SegmentManifestCommit{
		SegmentID:     segmentID,
		StorageConfig: compaction.CreateStorageConfig(),
		Mutation: meta.ManifestMutation{
			Type: meta.ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{
				ColumnGroups: packed.ColumnGroupEntriesFromProto(delta.GetColumnGroups()),
				Stats:        packed.StatEntriesFromProto(delta.GetStats()),
			},
		},
		CatalogMutation: meta.SegmentCatalogMutation{
			Operators: []meta.UpdateOperator{
				meta.UpdateBumpSchemaVersionMaterializationOperator(segmentID, newSchemaVersion, resultSegment.GetInsertLogs(), resultSegment.GetStats()),
			},
		},
	}); err != nil {
		return nil, err
	}
	return []UniqueID{segmentID}, nil
}

func (t *BumpSchemaVersionTask) processMetaSaved() bool {
	err := t.UpdateAndSaveTaskMeta(SetState(datapb.CompactionTaskState_completed))
	if err != nil {
		mlog.Warn(context.TODO(), "bumpSchemaVersionTask unable to processMetaSaved",
			mlog.Int64("planID", t.GetTaskProto().GetPlanID()),
			mlog.Err(err))
		return false
	}
	return t.processCompleted()
}

func (t *BumpSchemaVersionTask) processCompleted() bool {
	log := mlog.With(mlog.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
		mlog.Int64("PlanID", t.GetTaskProto().GetPlanID()),
		mlog.FieldCollectionID(t.GetTaskProto().GetCollectionID()))
	log.Info(context.TODO(), "bumpSchemaVersionTask processCompleted done")
	return true
}

func (t *BumpSchemaVersionTask) UpdateAndSaveTaskMeta(opts ...compactionTaskOpt) error {
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
