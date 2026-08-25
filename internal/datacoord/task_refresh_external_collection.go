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
	"github.com/milvus-io/milvus/internal/storagev2/packed"
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

// applyExternalCollectionSegmentUpdateForBaseline applies a caller-supplied
// immutable baseline: baseline IDs may be kept, patched, or removed, while IDs
// outside the baseline may only be added as new segments. The job-level path
// passes the published ownership baseline.
func applyExternalCollectionSegmentUpdateForBaseline(
	ctx context.Context,
	mt *meta,
	collectionID int64,
	baselineSegmentIDs []int64,
	keptSegmentIDs []int64,
	updatedSegments []*datapb.SegmentInfo,
	logFields ...mlog.Field,
) error {
	if mt == nil {
		return merr.WrapErrServiceInternalMsg("meta is nil, cannot update segments")
	}
	mlog.Info(ctx, "processing external collection update response",
		append(logFields,
			mlog.FieldCollectionID(collectionID),
			mlog.Int("keptSegments", len(keptSegmentIDs)),
			mlog.Int("updatedSegments", len(updatedSegments)),
		)...)

	keptSegmentMap := make(map[int64]bool)
	for _, segID := range keptSegmentIDs {
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

	// Build the desired final state from the caller-supplied baseline and durable
	// worker result. Current segment state is validated later while
	// UpdateSegmentsInfo holds the segment metadata write lock.
	segmentsToDrop := make([]int64, 0)
	baselineSegmentMap := make(map[int64]struct{}, len(baselineSegmentIDs))
	for _, segmentID := range baselineSegmentIDs {
		if _, ok := baselineSegmentMap[segmentID]; ok {
			return merr.WrapErrServiceInternalMsg("duplicate baseline segment %d", segmentID)
		}
		baselineSegmentMap[segmentID] = struct{}{}
	}

	for segmentID := range keptSegmentMap {
		if _, ok := baselineSegmentMap[segmentID]; !ok {
			return merr.WrapErrServiceInternalMsg("kept segment %d is outside the refresh baseline", segmentID)
		}
	}
	for segmentID := range baselineSegmentMap {
		if !keptSegmentMap[segmentID] && upsertSegmentMap[segmentID] == nil {
			segmentsToDrop = append(segmentsToDrop, segmentID)
		}
	}

	for segmentID, incoming := range upsertSegmentMap {
		if _, isPatch := baselineSegmentMap[segmentID]; isPatch {
			continue
		}
		if err := validateExternalRefreshNewSegment(incoming); err != nil {
			return err
		}
	}
	baselineSegmentCount := len(baselineSegmentMap)
	finalSegmentCount := len(keptSegmentMap) + len(upsertSegmentMap)

	mlog.Info(ctx, "segment update safety check",
		mlog.Int("baselineSegments", baselineSegmentCount),
		mlog.Int("segmentsToDrop", len(segmentsToDrop)),
		mlog.Int("keptSegments", len(keptSegmentMap)),
		mlog.Int("upsertSegments", len(upsertSegmentMap)),
		mlog.Int("finalSegmentCount", finalSegmentCount))

	// Safety check: reject if dropping all segments without adding new ones
	// This prevents accidental data loss from malformed worker responses
	if baselineSegmentCount > 0 && finalSegmentCount == 0 {
		mlog.Error(ctx, "safety check failed: refusing to drop all segments without replacement",
			mlog.Int("baselineSegmentCount", baselineSegmentCount),
			mlog.Int("keptSegments", len(keptSegmentMap)),
			mlog.Int("updatedSegments", len(upsertSegmentMap)))
		return merr.WrapErrServiceInternalMsg("safety check failed: refusing to drop all %d segments without replacement (keptSegments=%d, updatedSegments=%d)",
			baselineSegmentCount, len(keptSegmentMap), len(upsertSegmentMap))
	}

	// Safety check: warn if dropping more than configured ratio of segments
	if baselineSegmentCount > 0 && len(segmentsToDrop) > 0 {
		dropRatio := float64(len(segmentsToDrop)) / float64(baselineSegmentCount)
		threshold := paramtable.Get().DataCoordCfg.ExternalCollectionDropRatioWarn.GetAsFloat()
		if threshold <= 0 {
			threshold = 0.9
		}
		if dropRatio > threshold {
			mlog.Warn(ctx, "high segment drop ratio detected",
				mlog.Float64("dropRatio", dropRatio),
				mlog.Float64("threshold", threshold),
				mlog.Int64s("segmentsToDrop", segmentsToDrop),
				mlog.Int("baselineSegmentCount", baselineSegmentCount))
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
	// Segments whose incoming result the collection already carries - a new
	// segment written by an earlier apply, or a baseline patch already
	// installed. Both are skipped by the upsert operators below.
	alreadyAppliedSegments := make(map[int64]struct{})

	validationOperator := func(modPack *updateSegmentPack) bool {
		for segmentID := range baselineSegmentMap {
			existing := modPack.meta.segments.GetSegment(segmentID)
			incoming := upsertSegmentMap[segmentID]
			kept := keptSegmentMap[segmentID]

			if existing == nil {
				if !kept && incoming == nil {
					// A missing segment already satisfies the desired removal state.
					continue
				}
				return modPack.fail(merr.WrapErrServiceInternalMsg("baseline segment %d not found", segmentID))
			}
			if existing.GetCollectionID() != collectionID {
				return modPack.fail(merr.WrapErrServiceInternalMsg(
					"baseline segment %d belongs to collection %d, expected %d",
					segmentID,
					existing.GetCollectionID(),
					collectionID,
				))
			}
			if kept {
				if existing.GetState() == commonpb.SegmentState_Dropped {
					return modPack.fail(merr.WrapErrServiceInternalMsg("cannot keep dropped segment %d", segmentID))
				}
				continue
			}
			if incoming == nil {
				// Dropped is the replay-safe terminal state for an inferred removal.
				continue
			}
			if externalRefreshManifestAlreadyApplied(existing, incoming) {
				// Replaying a patch is NOT harmless: applyExternalRefreshPatch
				// clears TextStatsLogs and JsonKeyStats, so a second write
				// would discard a text index or JSON key stats built since the
				// first one, orphaning their files. A genuine patch always
				// installs a strictly newer manifest, so it still applies.
				alreadyAppliedSegments[segmentID] = struct{}{}
				mlog.Info(ctx, "external refresh segment patch already applied, skipping replay",
					mlog.FieldSegmentID(segmentID))
				continue
			}
			if err := validateExternalRefreshPatch(existing, incoming, collectionID); err != nil {
				mlog.Warn(ctx, "invalid external refresh segment patch",
					mlog.FieldSegmentID(incoming.GetID()),
					mlog.Err(err))
				return modPack.fail(err)
			}
		}

		for segmentID := range upsertSegmentMap {
			if _, isPatch := baselineSegmentMap[segmentID]; isPatch {
				continue
			}
			existing := modPack.meta.segments.GetSegment(segmentID)
			if existing == nil {
				continue
			}
			if externalRefreshNewSegmentAlreadyApplied(existing, upsertSegmentMap[segmentID]) {
				alreadyAppliedSegments[segmentID] = struct{}{}
				mlog.Info(ctx, "new external refresh segment already applied, skipping replay",
					mlog.FieldSegmentID(segmentID))
				continue
			}
			return modPack.fail(merr.WrapErrServiceInternalMsg(
				"new external refresh segment %d collides with existing metadata",
				segmentID,
			))
		}
		return true
	}
	operators = append(operators, validationOperator)

	// Operator 1: Drop only the segment IDs selected during validation. For an
	// ownership plan this list is limited to its immutable baseline.
	dropOperator := func(modPack *updateSegmentPack) bool {
		for _, segmentID := range segmentsToDrop {
			current := modPack.meta.segments.GetSegment(segmentID)
			if current == nil || current.GetState() == commonpb.SegmentState_Dropped {
				continue
			}
			segment := modPack.Get(segmentID)
			updateSegStateAndPrepareMetrics(segment, commonpb.SegmentState_Dropped, modPack.metricMutation)
			segment.DroppedAt = uint64(time.Now().UnixNano())
			modPack.segments[segmentID] = segment
			mlog.Info(ctx, "marking segment as dropped",
				mlog.FieldSegmentID(segmentID),
				mlog.Int64("numRows", segment.GetNumOfRows()))
		}
		return true
	}
	operators = append(operators, dropOperator)

	// Operator 2: Add new segments or patch existing active segments.
	for _, seg := range normalizedUpdatedSegments {
		incoming := seg
		upsertOperator := func(modPack *updateSegmentPack) bool {
			if _, ok := alreadyAppliedSegments[incoming.GetID()]; ok {
				return true
			}
			existing := modPack.Get(incoming.GetID())
			if existing != nil {
				patched := applyExternalRefreshPatch(existing, incoming)
				modPack.segments[incoming.GetID()] = patched
				modPack.increments[incoming.GetID()] = metastore.BinlogsIncrement{
					Segment: patched.SegmentInfo,
				}
				mlog.Info(ctx, "patching existing segment",
					mlog.FieldSegmentID(incoming.GetID()),
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

			mlog.Info(ctx, "adding new segment",
				mlog.FieldSegmentID(incoming.GetID()),
				mlog.Int64("numRows", incoming.GetNumOfRows()))
			return true
		}
		operators = append(operators, upsertOperator)
	}

	// Execute all operators atomically
	if err := mt.UpdateSegmentsInfo(ctx, operators...); err != nil {
		mlog.Warn(ctx, "failed to update segments atomically", mlog.Err(err))
		return err
	}

	mlog.Info(ctx, "external collection segments updated successfully",
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

// externalRefreshManifestAlreadyApplied recognizes an idempotent replay by the
// manifest a result would install. A manifest base path belongs to one segment,
// and its versions move forward as that segment gains later manifest updates.
// An equal or newer existing version therefore means the incoming refresh
// result has already been applied or superseded and must not be written again.
//
// Anything it cannot compare - an unparseable path, a different base path -
// reports false, so an unrecognized shape still takes the normal write path
// and its own validation.
//
// Do not compare fake binlogs here: V3 catalog persistence intentionally strips
// them, so their in-memory representation does not survive DataCoord restart.
func externalRefreshManifestAlreadyApplied(existing *SegmentInfo, incoming *datapb.SegmentInfo) bool {
	if existing == nil || incoming == nil {
		return false
	}
	comparison, err := packed.CompareManifestPath(existing.GetManifestPath(), incoming.GetManifestPath())
	return err == nil && comparison >= 0
}

// externalRefreshNewSegmentAlreadyApplied recognizes an idempotent replay of a
// new, non-baseline segment: the identity must match as well, because for a new
// segment a colliding ID that is NOT this result is a hard error rather than a
// replay.
func externalRefreshNewSegmentAlreadyApplied(existing *SegmentInfo, incoming *datapb.SegmentInfo) bool {
	if existing == nil || incoming == nil {
		return false
	}
	if existing.GetID() != incoming.GetID() ||
		existing.GetCollectionID() != incoming.GetCollectionID() ||
		existing.GetPartitionID() != incoming.GetPartitionID() {
		return false
	}
	return externalRefreshManifestAlreadyApplied(existing, incoming)
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
	cloned.TextStatsLogs = nil
	cloned.JsonKeyStats = nil
	if incoming.GetStorageVersion() != 0 {
		cloned.StorageVersion = incoming.GetStorageVersion()
	}
	return cloned
}

// getExternalRefreshSegmentSnapshots returns clones in the same order as
// segmentIDs while holding segMu across the full read, preventing one worker
// request from observing segment metadata from different update generations.
func getExternalRefreshSegmentSnapshots(mt *meta, segmentIDs []int64) []*SegmentInfo {
	mt.segMu.RLock()
	defer mt.segMu.RUnlock()

	result := make([]*SegmentInfo, len(segmentIDs))
	for i, segmentID := range segmentIDs {
		if segment := mt.segments.GetSegment(segmentID); segment != nil {
			result[i] = segment.Clone()
		}
	}
	return result
}

func (t *refreshExternalCollectionTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	ctx := context.TODO()
	log := mlog.With(
		mlog.FieldJobID(t.GetJobId()),
		mlog.FieldTaskID(t.GetTaskId()),
		mlog.FieldCollectionID(t.GetCollectionId()),
		mlog.FieldNodeID(nodeID),
	)
	var err error
	defer func() {
		if err != nil {
			log.Warn(ctx, "failed to create refresh task on worker", mlog.Err(err))
			if updateErr := t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, err.Error()); updateErr != nil {
				log.Warn(ctx, "failed to persist Failed state after create error", mlog.Err(updateErr))
			}
		}
	}()

	log.Info(ctx, "creating refresh task on worker",
		mlog.Int64("fileIndexBegin", t.GetFileIndexBegin()),
		mlog.Int64("fileIndexEnd", t.GetFileIndexEnd()),
		mlog.Int64("fileCount", t.GetFileIndexEnd()-t.GetFileIndexBegin()),
		mlog.Int("ownedSegments", len(t.GetOwnedSegmentIds())))

	if t.mt == nil {
		err = merr.WrapErrServiceInternalMsg("meta is nil, cannot create task on worker")
		return
	}
	if !isSupportedExternalRefreshOwnershipPlanVersion(t.GetOwnershipPlanVersion()) {
		err = merr.WrapErrServiceInternalMsg(
			"external refresh task %d has unsupported ownership plan version %d; retry refresh",
			t.GetTaskId(),
			t.GetOwnershipPlanVersion(),
		)
		return
	}

	// Persist task version and nodeID before dispatching to worker
	if err = t.refreshMeta.UpdateTaskVersion(t.GetTaskId(), nodeID); err != nil {
		log.Warn(ctx, "failed to update task version", mlog.Err(err))
		return
	}

	// Re-read task from meta to sync in-memory state (nodeID and version)
	updatedTask := t.refreshMeta.GetTask(t.GetTaskId())
	if updatedTask == nil {
		err = merr.WrapErrServiceInternalMsg("task %d not found after version update", t.GetTaskId())
		return
	}
	t.ExternalCollectionRefreshTask = updatedTask

	ownedSegmentIDs := t.GetOwnedSegmentIds()
	currentSegments := make([]*datapb.SegmentInfo, 0, len(ownedSegmentIDs))
	seenOwnedSegments := make(map[int64]struct{}, len(t.GetOwnedSegmentIds()))
	for _, segmentID := range ownedSegmentIDs {
		if _, ok := seenOwnedSegments[segmentID]; ok {
			err = merr.WrapErrServiceInternalMsg("task %d contains duplicate owned segment %d", t.GetTaskId(), segmentID)
			return
		}
		seenOwnedSegments[segmentID] = struct{}{}
	}

	segmentSnapshots := getExternalRefreshSegmentSnapshots(t.mt, ownedSegmentIDs)
	for i, segmentID := range ownedSegmentIDs {
		segment := segmentSnapshots[i]
		if segment == nil {
			err = merr.WrapErrServiceInternalMsg("owned segment %d not found for task %d", segmentID, t.GetTaskId())
			return
		}
		if segment.GetCollectionID() != t.GetCollectionId() {
			err = merr.WrapErrServiceInternalMsg(
				"owned segment %d belongs to collection %d, expected %d",
				segmentID,
				segment.GetCollectionID(),
				t.GetCollectionId(),
			)
			return
		}
		if !isSegmentHealthy(segment) {
			err = merr.WrapErrServiceInternalMsg("owned segment %d is not active", segmentID)
			return
		}
		currentSegments = append(currentSegments, segment.SegmentInfo)
	}

	log.Info(ctx, "collected owned current segments", mlog.Int("segmentCount", len(currentSegments)))

	// Pre-allocate segment IDs for data mapping
	preAllocCount := paramtable.Get().DataCoordCfg.ExternalCollectionPreAllocSegments.GetAsInt64()

	idBegin, idEnd, err := t.allocator.AllocN(preAllocCount)
	if err != nil {
		log.Warn(ctx, "failed to batch allocate segment IDs", mlog.Err(err))
		return
	}

	idRange := &datapb.IDRange{
		Begin: idBegin,
		End:   idEnd,
	}

	log.Info(ctx, "Pre-allocated segment IDs for external task",
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
		ClusterID:              paramtable.Get().CommonCfg.ClusterPrefix.GetValue(),
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
		TargetRowsPerSegment:   paramtable.Get().DataNodeCfg.ExternalCollectionTargetRowsPerSegment.GetAsInt64(),
	}

	// Submit task to worker via unified task system
	err = cluster.CreateRefreshExternalCollectionTask(nodeID, req)
	if err != nil {
		log.Warn(ctx, "failed to create refresh task on worker", mlog.Err(err))
		return
	}

	// Mark task as in progress - QueryTaskOnWorker will check completion
	if err = t.UpdateStateWithMeta(indexpb.JobState_JobStateInProgress, ""); err != nil {
		log.Warn(ctx, "failed to update task state to InProgress", mlog.Err(err))
		return
	}

	log.Info(ctx, "refresh task submitted successfully")
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
