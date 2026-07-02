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

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/util/segmentutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// refreshExternalCollectionTask wraps ExternalCollectionRefreshTask for scheduling.
// This is used by the global task scheduler to dispatch refresh tasks to DataNodes.
type refreshExternalCollectionTask struct {
	*datapb.ExternalCollectionRefreshTask

	times *taskcommon.Times

	refreshMeta *externalCollectionRefreshMeta
	mt          *meta
	allocator   allocator.Allocator
	// processFinishedJob is the per-job entry point on the refresh checker.
	// The task calls it synchronously after transitioning to a terminal state
	// so the finished-callback (schema update + WAL broadcast) fires before
	// the task method returns and progress polls observe a consistent state.
	// The checker still runs the same logic on its periodic tick as a safety
	// net for missed events. Set by the manager during task wrapping; nil in
	// unit tests.
	processFinishedJob func(jobID int64)
}

var _ globalTask.Task = (*refreshExternalCollectionTask)(nil)

func newRefreshExternalCollectionTask(
	t *datapb.ExternalCollectionRefreshTask,
	refreshMeta *externalCollectionRefreshMeta,
	mt *meta,
	alloc allocator.Allocator,
) *refreshExternalCollectionTask {
	return &refreshExternalCollectionTask{
		ExternalCollectionRefreshTask: t,
		times:                         taskcommon.NewTimes(),
		refreshMeta:                   refreshMeta,
		mt:                            mt,
		allocator:                     alloc,
	}
}

func (t *refreshExternalCollectionTask) GetTaskID() int64 {
	return t.TaskId
}

func (t *refreshExternalCollectionTask) GetTaskType() taskcommon.Type {
	return taskcommon.RefreshExternalCollection
}

func (t *refreshExternalCollectionTask) GetTaskState() taskcommon.State {
	// taskcommon.State is a type alias of indexpb.JobState, so this is type-safe.
	return t.GetState()
}

func (t *refreshExternalCollectionTask) GetTaskSlot() int64 {
	// External collection tasks are lightweight, use 1 slot
	return 1
}

func (t *refreshExternalCollectionTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	t.times.SetTaskTime(timeType, time)
}

func (t *refreshExternalCollectionTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(t.times)
}

func (t *refreshExternalCollectionTask) GetTaskVersion() int64 {
	return t.GetVersion()
}

// validateSource checks if this task's external source matches the current collection source
// Returns error if task has been superseded
func (t *refreshExternalCollectionTask) validateSource() error {
	if t.mt == nil {
		// Skip validation if mt is not provided (e.g., during inspector reload)
		return nil
	}

	// Validate against job-level snapshot to isolate in-flight tasks from schema changes.
	job := t.refreshMeta.GetJob(t.GetJobId())
	if job == nil {
		return merr.WrapErrServiceInternalMsg("job %d not found", t.GetJobId())
	}

	currentSource := job.GetExternalSource()
	currentSpec := job.GetExternalSpec()

	taskSource := t.GetExternalSource()
	taskSpec := t.GetExternalSpec()

	if currentSource != taskSource || currentSpec != taskSpec {
		return merr.WrapErrServiceInternalMsg(
			"task source mismatch: task source=%s/%s, job source=%s/%s (task belongs to a different refresh job)",
			taskSource, taskSpec, currentSource, currentSpec,
		)
	}

	return nil
}

func (t *refreshExternalCollectionTask) SetState(state indexpb.JobState, failReason string) {
	t.State = state
	t.FailReason = failReason
}

func (t *refreshExternalCollectionTask) UpdateStateWithMeta(state indexpb.JobState, failReason string) error {
	if err := t.refreshMeta.UpdateTaskState(t.GetTaskId(), state, failReason); err != nil {
		mlog.Warn(context.TODO(), "update refresh task state failed",
			mlog.Int64("taskID", t.GetTaskId()),
			mlog.String("state", state.String()),
			mlog.String("failReason", failReason),
			mlog.Err(err))
		return err
	}
	t.SetState(state, failReason)

	// When the task reaches a terminal state, synchronously drive per-job
	// processing on the checker. processJob is the single aggregation point
	// — it re-reads tasks, transitions job state, and fires the finish
	// callback + schema update + WAL broadcast before this method returns.
	// This guarantees that callers polling GetRefreshExternalCollectionProgress
	// observe a consistent state: when the job appears Finished, the schema
	// update has already been applied. The checker's periodic tick runs the
	// same logic as a safety net for missed events (e.g., DataCoord restart).
	if state == indexpb.JobState_JobStateFinished || state == indexpb.JobState_JobStateFailed {
		if t.processFinishedJob != nil {
			t.processFinishedJob(t.GetJobId())
		}
	}

	return nil
}

func (t *refreshExternalCollectionTask) UpdateProgressWithMeta(progress int64) error {
	if err := t.refreshMeta.UpdateTaskProgress(t.GetTaskId(), progress); err != nil {
		mlog.Warn(context.TODO(), "update refresh task progress failed",
			mlog.Int64("taskID", t.GetTaskId()),
			mlog.Int64("progress", progress),
			mlog.Err(err))
		return err
	}
	t.Progress = progress
	return nil
}

func (t *refreshExternalCollectionTask) UpdateResultWithMeta(
	state indexpb.JobState,
	failReason string,
	keptSegments []int64,
	updatedSegments []*datapb.SegmentInfo,
) error {
	if err := t.refreshMeta.UpdateTaskResult(t.GetTaskId(), state, failReason, keptSegments, updatedSegments); err != nil {
		mlog.Warn(context.TODO(), "update refresh task result failed",
			mlog.Int64("taskID", t.GetTaskId()),
			mlog.String("state", state.String()),
			mlog.String("failReason", failReason),
			mlog.Err(err))
		return err
	}
	t.SetState(state, failReason)
	t.KeptSegments = append([]int64(nil), keptSegments...)
	t.UpdatedSegments = cloneProtoSegments(updatedSegments)

	if state == indexpb.JobState_JobStateFinished || state == indexpb.JobState_JobStateFailed {
		if t.processFinishedJob != nil {
			t.processFinishedJob(t.GetJobId())
		}
	}

	return nil
}

func applyExternalCollectionSegmentUpdate(
	ctx context.Context,
	mt *meta,
	collectionID int64,
	keptSegmentIDs []int64,
	updatedSegments []*datapb.SegmentInfo,
	logFields ...mlog.Field,
) error {
	if mt == nil {
		return merr.WrapErrServiceInternalMsg("meta is nil, cannot update segments")
	}
	mlog.Info(context.TODO(), "processing external collection update response",
		append(logFields,
			mlog.Int64("collectionID", collectionID),
			mlog.Int("keptSegments", len(keptSegmentIDs)),
			mlog.Int("updatedSegments", len(updatedSegments)),
		)...)

	keptSegmentMap := make(map[int64]bool)
	for _, segID := range keptSegmentIDs {
		segment := mt.segments.GetSegment(segID)
		if segment == nil {
			return merr.WrapErrServiceInternalMsg("kept segment %d not found", segID)
		}
		if segment.GetCollectionID() != collectionID {
			return merr.WrapErrServiceInternalMsg("collection mismatch for kept segment %d: existing %d, want %d",
				segID, segment.GetCollectionID(), collectionID)
		}
		if segment.GetState() == commonpb.SegmentState_Dropped {
			return merr.WrapErrServiceInternalMsg("cannot keep dropped segment %d", segID)
		}
		keptSegmentMap[segID] = true
	}

	upsertSegmentMap := make(map[int64]*datapb.SegmentInfo)
	validUpdatedSegments := make([]*datapb.SegmentInfo, 0, len(updatedSegments))
	for _, seg := range updatedSegments {
		if seg == nil {
			continue
		}
		if err := validateExternalRefreshUpdatedSegment(seg, collectionID); err != nil {
			return err
		}
		if keptSegmentMap[seg.GetID()] {
			return merr.WrapErrServiceInternalMsg("segment %d cannot be both kept and updated", seg.GetID())
		}
		if _, ok := upsertSegmentMap[seg.GetID()]; ok {
			return merr.WrapErrServiceInternalMsg("duplicate updated segment %d", seg.GetID())
		}
		upsertSegmentMap[seg.GetID()] = seg
		validUpdatedSegments = append(validUpdatedSegments, seg)
	}

	// Safety validation: count current active segments and segments to be dropped
	currentSegments := mt.SelectSegments(ctx, CollectionFilter(collectionID))
	activeSegmentCount := 0
	segmentsToDrop := make([]int64, 0)
	existingSegmentMap := make(map[int64]*SegmentInfo)
	finalSegmentCount := 0
	for _, seg := range currentSegments {
		existingSegmentMap[seg.GetID()] = seg
		if seg.GetState() != commonpb.SegmentState_Dropped {
			activeSegmentCount++
			if !keptSegmentMap[seg.GetID()] && upsertSegmentMap[seg.GetID()] == nil {
				segmentsToDrop = append(segmentsToDrop, seg.GetID())
			} else {
				finalSegmentCount++
			}
		}
	}

	for _, incoming := range upsertSegmentMap {
		existing := existingSegmentMap[incoming.GetID()]
		if existing == nil {
			existing = mt.segments.GetSegment(incoming.GetID())
		}
		if existing != nil {
			if err := validateExternalRefreshPatch(existing, incoming, collectionID); err != nil {
				return err
			}
			continue
		}
		if err := validateExternalRefreshNewSegment(incoming); err != nil {
			return err
		}
		finalSegmentCount++
	}

	mlog.Info(context.TODO(), "segment update safety check",
		mlog.Int("currentActiveSegments", activeSegmentCount),
		mlog.Int("segmentsToDrop", len(segmentsToDrop)),
		mlog.Int("keptSegments", len(keptSegmentMap)),
		mlog.Int("upsertSegments", len(upsertSegmentMap)),
		mlog.Int("finalSegmentCount", finalSegmentCount))

	// Safety check: reject if dropping all segments without adding new ones
	// This prevents accidental data loss from malformed worker responses
	if activeSegmentCount > 0 && finalSegmentCount == 0 {
		mlog.Error(context.TODO(), "safety check failed: refusing to drop all segments without replacement",
			mlog.Int("activeSegmentCount", activeSegmentCount),
			mlog.Int("keptSegments", len(keptSegmentMap)),
			mlog.Int("updatedSegments", len(upsertSegmentMap)))
		return merr.WrapErrServiceInternalMsg("safety check failed: refusing to drop all %d segments without replacement (keptSegments=%d, updatedSegments=%d)",
			activeSegmentCount, len(keptSegmentMap), len(upsertSegmentMap))
	}

	// Safety check: warn if dropping more than configured ratio of segments
	if activeSegmentCount > 0 && len(segmentsToDrop) > 0 {
		dropRatio := float64(len(segmentsToDrop)) / float64(activeSegmentCount)
		threshold := paramtable.Get().DataCoordCfg.ExternalCollectionDropRatioWarn.GetAsFloat()
		if threshold <= 0 {
			threshold = 0.9
		}
		if dropRatio > threshold {
			mlog.Warn(context.TODO(), "high segment drop ratio detected",
				mlog.Float64("dropRatio", dropRatio),
				mlog.Float64("threshold", threshold),
				mlog.Int64s("segmentsToDrop", segmentsToDrop),
				mlog.Int("activeSegmentCount", activeSegmentCount))
		}
	}

	collInfo := mt.GetCollection(collectionID)
	if collInfo == nil {
		return merr.WrapErrServiceInternalMsg("collection %d not found in meta", collectionID)
	}
	// External collections are single-shard, single-partition (enforced at creation).
	// Assert exactly-one here to catch any invariant violation from data corruption or legacy data.
	if len(collInfo.VChannelNames) != 1 {
		return merr.WrapErrServiceInternalMsg("external collection %d expected exactly 1 VChannel, got %d", collectionID, len(collInfo.VChannelNames))
	}
	if len(collInfo.Partitions) != 1 {
		return merr.WrapErrServiceInternalMsg("external collection %d expected exactly 1 partition, got %d", collectionID, len(collInfo.Partitions))
	}
	insertChannel := collInfo.VChannelNames[0]
	partitionID := collInfo.Partitions[0]
	normalizedUpdatedSegments := make([]*datapb.SegmentInfo, 0, len(validUpdatedSegments))
	normalizedUpsertSegmentMap := make(map[int64]*datapb.SegmentInfo, len(upsertSegmentMap))
	for _, seg := range validUpdatedSegments {
		normalized := normalizeExternalRefreshUpdatedSegment(seg, collectionID, partitionID, insertChannel)
		normalizedUpdatedSegments = append(normalizedUpdatedSegments, normalized)
		normalizedUpsertSegmentMap[normalized.GetID()] = normalized
	}
	upsertSegmentMap = normalizedUpsertSegmentMap

	// Build update operators
	var operators []UpdateOperator
	var patchErr error

	validationOperator := func(modPack *updateSegmentPack) bool {
		for _, incoming := range upsertSegmentMap {
			existing := modPack.meta.segments.GetSegment(incoming.GetID())
			if existing != nil {
				if err := validateExternalRefreshPatch(existing, incoming, collectionID); err != nil {
					patchErr = err
					mlog.Warn(context.TODO(), "invalid external refresh segment patch",
						mlog.Int64("segmentID", incoming.GetID()),
						mlog.Err(err))
					return false
				}
			}
		}
		return true
	}
	operators = append(operators, validationOperator)

	// Operator 1: Drop segments not in kept list
	dropOperator := func(modPack *updateSegmentPack) bool {
		if patchErr != nil {
			return false
		}
		currentSegments := modPack.meta.segments.GetSegments()
		for _, seg := range currentSegments {
			// Skip segments not in this collection
			if seg.GetCollectionID() != collectionID {
				continue
			}

			// Skip segments that are already dropped
			if seg.GetState() == commonpb.SegmentState_Dropped {
				continue
			}

			// Drop segment if not kept or upserted by this refresh response.
			if !keptSegmentMap[seg.GetID()] && upsertSegmentMap[seg.GetID()] == nil {
				segment := modPack.Get(seg.GetID())
				if segment != nil {
					updateSegStateAndPrepareMetrics(segment, commonpb.SegmentState_Dropped, modPack.metricMutation)
					segment.DroppedAt = uint64(time.Now().UnixNano())
					modPack.segments[seg.GetID()] = segment
					mlog.Info(context.TODO(), "marking segment as dropped",
						mlog.Int64("segmentID", seg.GetID()),
						mlog.Int64("numRows", seg.GetNumOfRows()))
				}
			}
		}
		return true
	}
	operators = append(operators, dropOperator)

	// Operator 2: Add new segments or patch existing active segments.
	for _, seg := range normalizedUpdatedSegments {
		incoming := seg
		upsertOperator := func(modPack *updateSegmentPack) bool {
			if patchErr != nil {
				return false
			}
			existing := modPack.Get(incoming.GetID())
			if existing != nil {
				if err := validateExternalRefreshPatch(existing, incoming, collectionID); err != nil {
					patchErr = err
					mlog.Warn(context.TODO(), "invalid external refresh segment patch",
						mlog.Int64("segmentID", incoming.GetID()),
						mlog.Err(err))
					return false
				}

				patched := applyExternalRefreshPatch(existing, incoming)
				modPack.segments[incoming.GetID()] = patched
				modPack.increments[incoming.GetID()] = metastore.BinlogsIncrement{
					Segment: patched.SegmentInfo,
				}
				mlog.Info(context.TODO(), "patching existing segment",
					mlog.Int64("segmentID", incoming.GetID()),
					mlog.Int64("numRows", incoming.GetNumOfRows()),
					mlog.String("manifestPath", incoming.GetManifestPath()))
				return true
			}

			segInfo := NewSegmentInfo(incoming)
			modPack.segments[incoming.GetID()] = segInfo

			modPack.increments[incoming.GetID()] = metastore.BinlogsIncrement{
				Segment: incoming,
			}

			modPack.metricMutation.addNewSeg(
				commonpb.SegmentState_Flushed,
				incoming.GetLevel(),
				incoming.GetIsSorted(),
				incoming.GetStorageVersion(),
				segmentMetricFormatLabel(segInfo),
				incoming.GetNumOfRows(),
			)

			mlog.Info(context.TODO(), "adding new segment",
				mlog.Int64("segmentID", incoming.GetID()),
				mlog.Int64("numRows", incoming.GetNumOfRows()))
			return true
		}
		operators = append(operators, upsertOperator)
	}

	// Execute all operators atomically
	if err := mt.UpdateSegmentsInfo(ctx, operators...); err != nil {
		mlog.Warn(context.TODO(), "failed to update segments atomically", mlog.Err(err))
		return err
	}
	if patchErr != nil {
		return patchErr
	}

	mlog.Info(context.TODO(), "external collection segments updated successfully",
		mlog.Int("updatedSegments", len(updatedSegments)),
		mlog.Int("keptSegments", len(keptSegmentIDs)))

	return nil
}

func validateExternalRefreshUpdatedSegment(incoming *datapb.SegmentInfo, collectionID int64) error {
	if incoming.GetCollectionID() != 0 && incoming.GetCollectionID() != collectionID {
		return merr.WrapErrServiceInternalMsg("collection mismatch for segment %d: got %d, want %d",
			incoming.GetID(), incoming.GetCollectionID(), collectionID)
	}
	if incoming.GetManifestPath() == "" {
		return merr.WrapErrServiceInternalMsg("updated segment %d has empty manifest path", incoming.GetID())
	}
	if len(incoming.GetBinlogs()) == 0 {
		return merr.WrapErrServiceInternalMsg("updated segment %d has empty fake binlogs", incoming.GetID())
	}
	return nil
}

func normalizeExternalRefreshUpdatedSegment(
	incoming *datapb.SegmentInfo,
	collectionID int64,
	partitionID int64,
	insertChannel string,
) *datapb.SegmentInfo {
	normalized := proto.Clone(incoming).(*datapb.SegmentInfo)
	normalized.CollectionID = collectionID
	normalized.State = commonpb.SegmentState_Flushed
	if normalized.InsertChannel == "" {
		normalized.InsertChannel = insertChannel
	}
	if normalized.PartitionID == 0 {
		normalized.PartitionID = partitionID
	}
	return normalized
}

func validateExternalRefreshNewSegment(incoming *datapb.SegmentInfo) error {
	return validateExternalRefreshBinlogRowCount(incoming, incoming.GetNumOfRows())
}

func validateExternalRefreshPatch(oldSeg *SegmentInfo, incoming *datapb.SegmentInfo, collectionID int64) error {
	if oldSeg == nil {
		return merr.WrapErrServiceInternalMsg("existing segment is nil")
	}
	if oldSeg.GetCollectionID() != collectionID {
		return merr.WrapErrServiceInternalMsg("collection mismatch for segment %d: existing %d, want %d",
			oldSeg.GetID(), oldSeg.GetCollectionID(), collectionID)
	}
	if oldSeg.GetState() == commonpb.SegmentState_Dropped {
		return merr.WrapErrServiceInternalMsg("cannot patch dropped segment %d", oldSeg.GetID())
	}
	if incoming.GetCollectionID() != 0 && incoming.GetCollectionID() != collectionID {
		return merr.WrapErrServiceInternalMsg("collection mismatch for segment %d: got %d, want %d",
			incoming.GetID(), incoming.GetCollectionID(), collectionID)
	}
	if incoming.GetNumOfRows() != oldSeg.GetNumOfRows() {
		return merr.WrapErrServiceInternalMsg("row count changed for segment %d: got %d, want %d",
			incoming.GetID(), incoming.GetNumOfRows(), oldSeg.GetNumOfRows())
	}
	if incoming.GetStorageVersion() != 0 && incoming.GetStorageVersion() != oldSeg.GetStorageVersion() {
		return merr.WrapErrServiceInternalMsg("storage version changed for segment %d: got %d, want %d",
			incoming.GetID(), incoming.GetStorageVersion(), oldSeg.GetStorageVersion())
	}
	if incoming.GetSchemaVersion() < oldSeg.GetSchemaVersion() {
		return merr.WrapErrServiceInternalMsg("schema version rollback for segment %d: got %d, want >= %d",
			incoming.GetID(), incoming.GetSchemaVersion(), oldSeg.GetSchemaVersion())
	}
	if incoming.GetManifestPath() == "" {
		return merr.WrapErrServiceInternalMsg("patched segment %d has empty manifest path", incoming.GetID())
	}
	if len(incoming.GetBinlogs()) == 0 {
		return merr.WrapErrServiceInternalMsg("patched segment %d has empty fake binlogs", incoming.GetID())
	}
	if err := validateExternalRefreshBinlogRowCount(incoming, oldSeg.GetNumOfRows()); err != nil {
		return err
	}
	return nil
}

func validateExternalRefreshBinlogRowCount(segment *datapb.SegmentInfo, expectedRows int64) error {
	binlogRows := segmentutil.CalcRowCountFromBinLog(segment)
	if binlogRows == -1 {
		return merr.WrapErrServiceInternalMsg("invalid binlog row count for segment %d", segment.GetID())
	}
	if expectedRows > 0 && binlogRows != expectedRows {
		return merr.WrapErrServiceInternalMsg("binlog row count mismatch for segment %d: got %d, want %d",
			segment.GetID(), binlogRows, expectedRows)
	}
	if binlogRows > 0 && binlogRows != segment.GetNumOfRows() {
		return merr.WrapErrServiceInternalMsg("binlog row count mismatch for segment %d: got %d, segment rows %d",
			segment.GetID(), binlogRows, segment.GetNumOfRows())
	}
	return nil
}

func applyExternalRefreshPatch(oldSeg *SegmentInfo, incoming *datapb.SegmentInfo) *SegmentInfo {
	cloned := oldSeg.Clone()
	cloned.ManifestPath = incoming.GetManifestPath()
	cloned.SchemaVersion = incoming.GetSchemaVersion()
	cloned.Binlogs = incoming.GetBinlogs()
	cloned.JsonKeyStats = nil
	if incoming.GetStorageVersion() != 0 {
		cloned.StorageVersion = incoming.GetStorageVersion()
	}
	return cloned
}

// SetJobInfo processes a complete job-level response and updates segment information atomically.
func (t *refreshExternalCollectionTask) SetJobInfo(ctx context.Context, resp *datapb.RefreshExternalCollectionTaskResponse) error {
	return applyExternalCollectionSegmentUpdate(
		ctx,
		t.mt,
		t.GetCollectionId(),
		resp.GetKeptSegments(),
		resp.GetUpdatedSegments(),
		mlog.Int64("taskID", t.GetTaskId()),
	)
}

func (t *refreshExternalCollectionTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	timeout := paramtable.Get().DataCoordCfg.RequestTimeoutSeconds.GetAsDuration(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var err error
	defer func() {
		if err != nil {
			mlog.Warn(context.TODO(), "failed to create refresh task on worker", mlog.Err(err))
			if updateErr := t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, err.Error()); updateErr != nil {
				mlog.Warn(context.TODO(), "failed to persist Failed state after create error", mlog.Err(updateErr))
			}
		}
	}()

	mlog.Info(context.TODO(), "creating refresh task on worker")

	if t.mt == nil {
		err = merr.WrapErrServiceInternalMsg("meta is nil, cannot create task on worker")
		return
	}

	// Persist task version and nodeID before dispatching to worker
	if err = t.refreshMeta.UpdateTaskVersion(t.GetTaskId(), nodeID); err != nil {
		mlog.Warn(context.TODO(), "failed to update task version", mlog.Err(err))
		return
	}

	// Re-read task from meta to sync in-memory state (nodeID and version)
	updatedTask := t.refreshMeta.GetTask(t.GetTaskId())
	if updatedTask == nil {
		err = merr.WrapErrServiceInternalMsg("task %d not found after version update", t.GetTaskId())
		return
	}
	t.ExternalCollectionRefreshTask = updatedTask

	// Get current segments for the collection
	segments := t.mt.SelectSegments(ctx, CollectionFilter(t.GetCollectionId()))

	currentSegments := make([]*datapb.SegmentInfo, 0, len(segments))
	for _, seg := range segments {
		currentSegments = append(currentSegments, seg.SegmentInfo)
	}

	mlog.Info(context.TODO(), "collected current segments", mlog.Int("segmentCount", len(currentSegments)))

	// Pre-allocate segment IDs for data mapping
	preAllocCount := paramtable.Get().DataCoordCfg.ExternalCollectionPreAllocSegments.GetAsInt64()

	idBegin, idEnd, err := t.allocator.AllocN(preAllocCount)
	if err != nil {
		mlog.Warn(context.TODO(), "failed to batch allocate segment IDs", mlog.Err(err))
		return
	}

	idRange := &datapb.IDRange{
		Begin: idBegin,
		End:   idEnd,
	}

	mlog.Info(context.TODO(), "Pre-allocated segment IDs for external task",
		mlog.Int64("idBegin", idBegin),
		mlog.Int64("idEnd", idEnd),
		mlog.Int64("count", idEnd-idBegin))

	// Use the current collection schema as this task's snapshot. There is no
	// job/task-level schema-version gate for the current additive-only refresh
	// scope: if AddField races after this request is built, the task may finish
	// with the older schema and skip the new field, and a later refresh will
	// self-heal it through missing-column detection. Drop, rename, or type
	// changes must reintroduce stronger schema coordination, such as a gate or
	// lock, before they are supported.
	collInfo := t.mt.GetCollection(t.GetCollectionId())
	if collInfo == nil {
		err = merr.WrapErrServiceInternalMsg("collection %d not found in meta", t.GetCollectionId())
		return
	}
	if len(collInfo.Partitions) != 1 {
		err = merr.WrapErrServiceInternalMsg("external collection %d expected exactly 1 partition, got %d", t.GetCollectionId(), len(collInfo.Partitions))
		return
	}
	partitionID := collInfo.Partitions[0]

	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           t.GetCollectionId(),
		PartitionID:            partitionID,
		TaskID:                 t.GetTaskId(),
		CurrentSegments:        currentSegments,
		ExternalSource:         t.GetExternalSource(),
		ExternalSpec:           t.GetExternalSpec(),
		StorageConfig:          createStorageConfig(),
		Schema:                 collInfo.Schema,
		PreAllocatedSegmentIds: idRange,
		NumSegmentsExpected:    preAllocCount,
		ExploreManifestPath:    t.GetExploreManifestPath(),
		FileIndexBegin:         t.GetFileIndexBegin(),
		FileIndexEnd:           t.GetFileIndexEnd(),
	}

	// Submit task to worker via unified task system
	err = cluster.CreateRefreshExternalCollectionTask(nodeID, req)
	if err != nil {
		mlog.Warn(context.TODO(), "failed to create refresh task on worker", mlog.Err(err))
		return
	}

	// Mark task as in progress - QueryTaskOnWorker will check completion
	if err = t.UpdateStateWithMeta(indexpb.JobState_JobStateInProgress, ""); err != nil {
		mlog.Warn(context.TODO(), "failed to update task state to InProgress", mlog.Err(err))
		return
	}

	mlog.Info(context.TODO(), "refresh task submitted successfully")
}

func (t *refreshExternalCollectionTask) QueryTaskOnWorker(cluster session.Cluster) {
	// Check if job has been canceled/superseded before querying worker
	job := t.refreshMeta.GetJob(t.GetJobId())
	if job == nil {
		mlog.Info(context.TODO(), "job not found, task has been canceled")
		// Best-effort cleanup: try to drop task on worker if it was assigned
		if t.GetNodeId() != 0 {
			_ = cluster.DropRefreshExternalCollectionTask(t.GetNodeId(), t.GetTaskId())
		}
		if err := t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, "job canceled"); err != nil {
			mlog.Warn(context.TODO(), "failed to persist Failed state after job cancellation", mlog.Err(err))
		}
		return
	}
	if job.GetState() == indexpb.JobState_JobStateFailed {
		mlog.Info(context.TODO(), "job has been marked as failed, canceling task",
			mlog.String("jobFailReason", job.GetFailReason()))
		// Best-effort cleanup: try to drop task on worker if it was assigned
		if t.GetNodeId() != 0 {
			_ = cluster.DropRefreshExternalCollectionTask(t.GetNodeId(), t.GetTaskId())
		}
		if err := t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, "job canceled: "+job.GetFailReason()); err != nil {
			mlog.Warn(context.TODO(), "failed to persist Failed state after job cancellation", mlog.Err(err))
		}
		return
	}

	// Query task status from worker
	resp, err := cluster.QueryRefreshExternalCollectionTask(t.GetNodeId(), t.GetTaskId())
	if err != nil {
		mlog.Warn(context.TODO(), "query refresh task result failed", mlog.Err(err))
		// If query fails, mark task as failed
		if updateErr := t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, fmt.Sprintf("query task failed: %v", err)); updateErr != nil {
			mlog.Warn(context.TODO(), "failed to persist Failed state after query error", mlog.Err(updateErr))
		}
		return
	}

	state := resp.GetState()
	failReason := resp.GetFailReason()

	mlog.Info(context.TODO(), "queried refresh task status",
		mlog.String("state", state.String()),
		mlog.String("failReason", failReason))

	// Handle different task states
	switch state {
	case indexpb.JobState_JobStateFinished:
		// Validate source before processing - check if task has been superseded
		if err := t.validateSource(); err != nil {
			mlog.Warn(context.TODO(), "task validation failed, task has been superseded", mlog.Err(err))
			t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, err.Error())
			return
		}

		// Persist the task result. Segment metadata is applied once at the
		// job level after all sibling tasks have finished, so a single task
		// cannot drop segments produced by another task of the same job.
		if err := t.UpdateResultWithMeta(
			state,
			"",
			resp.GetKeptSegments(),
			resp.GetUpdatedSegments(),
		); err != nil {
			mlog.Warn(context.TODO(), "failed to update task state to Finished", mlog.Err(err))
			return
		}
		mlog.Info(context.TODO(), "refresh task completed successfully")

	case indexpb.JobState_JobStateFailed:
		// Task failed
		if err := t.UpdateStateWithMeta(state, failReason); err != nil {
			mlog.Warn(context.TODO(), "failed to update task state to Failed", mlog.Err(err))
			return
		}
		mlog.Warn(context.TODO(), "refresh task failed", mlog.String("reason", failReason))

	case indexpb.JobState_JobStateInProgress, indexpb.JobState_JobStateNone, indexpb.JobState_JobStateInit:
		// Task still in progress or not yet picked up by scheduler, no action needed
		mlog.Info(context.TODO(), "refresh task still in progress",
			mlog.String("state", state.String()))

	case indexpb.JobState_JobStateRetry:
		// Task needs retry - mark as failed
		mlog.Warn(context.TODO(), "refresh task in unexpected state, marking as failed",
			mlog.String("state", state.String()))
		if err := t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, fmt.Sprintf("task in unexpected state: %s", state.String())); err != nil {
			mlog.Warn(context.TODO(), "failed to persist Failed state for retry branch", mlog.Err(err))
		}

	default:
		mlog.Warn(context.TODO(), "refresh task in unknown state",
			mlog.String("state", state.String()))
	}
}

func (t *refreshExternalCollectionTask) DropTaskOnWorker(cluster session.Cluster) {
	// Drop task on worker to cancel execution and clean up resources
	err := cluster.DropRefreshExternalCollectionTask(t.GetNodeId(), t.GetTaskId())
	if err != nil {
		mlog.Warn(context.TODO(), "failed to drop refresh task on worker", mlog.Err(err))
		return
	}

	mlog.Info(context.TODO(), "refresh task dropped successfully")
}
