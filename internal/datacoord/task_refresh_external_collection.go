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

var (
	// errExternalRefreshStaleManifest signals that a refresh column patch was
	// built on a manifest the segment has since advanced past (a concurrent
	// text/JSON index build or compaction committed in between). The job-level
	// apply aborts atomically and the refresh checker resets the job's tasks to
	// Init so the worker rebuilds the patch on the current manifest, instead of
	// silently completing with a segment still missing the refreshed columns.
	errExternalRefreshStaleManifest = errors.New("external refresh column patch built on a stale manifest")
	// errExternalRefreshNotReady signals that the job-level apply cannot run yet
	// because a task is mid-retry (reset to Init / result cleared). It is NOT a
	// failure: a concurrent aggregator that observes a mid-retry task returns
	// this so the job is left non-terminal instead of being marked Failed. The
	// path that owns the retry drives the job forward on a later tick.
	errExternalRefreshNotReady = errors.New("external refresh apply not ready, retrying")
	// errExternalRefreshPermanent marks a refresh failure as permanent so the
	// retry classifier fails the task instead of re-dispatching. This is an
	// explicit signal, decoupled from the merr Input/System classification: an
	// INTERNAL invariant violation (a Milvus bug or corrupted metadata) is a
	// System error per the blame test, yet retrying it is pointless because a
	// rerun deterministically reproduces it. Attach with errors.Mark so the
	// underlying merr class is preserved.
	errExternalRefreshPermanent = errors.New("permanent external refresh failure")
)

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
	// Fence the write to this attempt: a superseded (re-dispatched) attempt must
	// not overwrite the current attempt's state.
	applied, err := t.refreshMeta.UpdateTaskState(t.GetTaskId(), t.GetVersion(), state, failReason)
	if err != nil {
		mlog.Warn(context.TODO(), "update refresh task state failed",
			mlog.Int64("taskID", t.GetTaskId()),
			mlog.String("state", state.String()),
			mlog.String("failReason", failReason),
			mlog.Err(err))
		return err
	}
	if !applied {
		mlog.Info(context.TODO(), "refresh task state update skipped as superseded",
			mlog.Int64("taskID", t.GetTaskId()), mlog.Int64("version", t.GetVersion()))
		return nil
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
	// Fence the write to this attempt: a stale/late Query response from a
	// superseded attempt must not write its result over the current attempt.
	applied, err := t.refreshMeta.UpdateTaskResult(t.GetTaskId(), t.GetVersion(), state, failReason, keptSegments, updatedSegments)
	if err != nil {
		mlog.Warn(context.TODO(), "update refresh task result failed",
			mlog.Int64("taskID", t.GetTaskId()),
			mlog.String("state", state.String()),
			mlog.String("failReason", failReason),
			mlog.Err(err))
		return err
	}
	if !applied {
		// Superseded attempt: do not drive the job on a stale result.
		mlog.Info(context.TODO(), "refresh task result dropped as superseded, skipping job processing",
			mlog.Int64("taskID", t.GetTaskId()), mlog.Int64("version", t.GetVersion()))
		return nil
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
				// Optimistic-concurrency CAS, evaluated here inside the segMu
				// critical section (modPack.Get is the synchronized read) so the
				// decision is atomic with the patch. The worker built this result on
				// incoming.BaseManifest. Fail closed for an EXISTING segment: adopt
				// only when the base is present AND still equals the current
				// manifest. Reject when
				//   - the base is empty: a pre-CAS / rolling-upgrade worker that
				//     cannot prove it built on the current manifest — adopting it
				//     could blindly overwrite a concurrent commit; or
				//   - the base no longer matches: a concurrent text/JSON index build
				//     or compaction advanced the manifest in between — adopting would
				//     drop that commit.
				// Abort the whole apply atomically (modPack.fail sets updatePack.err,
				// so UpdateSegmentsInfo returns before persisting anything) and signal
				// errExternalRefreshStaleManifest; the refresh checker resets the job's
				// tasks to Init and the worker rebuilds on the current manifest (a
				// capable worker then produces a matching base), instead of silently
				// completing with a segment still missing the refreshed data.
				//
				// validateManifestSuccessor also enforces that the result is a legal
				// successor of the current manifest (same base path, strictly forward,
				// parseable), not just that the base matched — so a buggy / corrupt /
				// mixed-version worker that carries the right base but a result pointing
				// at another segment or an older version cannot silently corrupt the
				// segment pointer.
				//
				// NOTE: this deliberately fails CLOSED on an empty base, unlike the
				// shared sort/index stats path (updateStatsResultIfManifestMatches)
				// which fails open for older-DataNode compatibility. External refresh is
				// a manual, low-frequency operation not run during a rolling upgrade, so
				// it has no old-worker compatibility need and takes the stronger
				// guarantee on an existing segment.
				isReplay, adoptErr := validateManifestSuccessor(incoming.GetBaseManifest(), existing.GetManifestPath(), incoming.GetManifestPath())
				if adoptErr != nil {
					mlog.Warn(context.TODO(), "external refresh patch is not a valid successor; aborting apply to rebuild on the current manifest",
						mlog.Int64("segmentID", incoming.GetID()),
						mlog.String("baseManifest", incoming.GetBaseManifest()),
						mlog.String("currentManifest", existing.GetManifestPath()),
						mlog.String("resultManifest", incoming.GetManifestPath()),
						mlog.Err(adoptErr))
					return modPack.fail(errors.Wrapf(errExternalRefreshStaleManifest,
						"segment %d: %v", incoming.GetID(), adoptErr))
				}
				if isReplay && externalRefreshPatchIsNoop(existing, incoming) {
					// A *complete* no-op: the result manifest, schema version, fake
					// binlogs and storage version all already match the segment. Keep
					// it as-is so its text/JSON stats survive (re-applying would clear
					// them).
					return true
				}
				// A same-manifest replay that is NOT a full no-op still carries
				// metadata the segment has not absorbed yet — e.g. the worker found the
				// column already appended on the object store and returned the unchanged
				// manifest while still bumping the schema version and rebuilding the fake
				// binlogs for the new column. Manifest-pointer equality does not mean the
				// SegmentInfo metadata landed, so fall through to validate and apply it;
				// the pointer assignment in applyExternalRefreshPatch is a no-op because
				// result == current.
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

// externalRefreshPatchIsNoop reports whether applying incoming would leave the
// segment byte-for-byte identical in every field applyExternalRefreshPatch
// touches: the manifest pointer, the schema version, the fake binlogs, and the
// storage version. Only a full no-op may skip the patch (and so preserve the
// segment's text/JSON stats); a same-manifest result whose schema or binlogs
// differ must still be applied.
func externalRefreshPatchIsNoop(oldSeg *SegmentInfo, incoming *datapb.SegmentInfo) bool {
	if oldSeg.GetManifestPath() != incoming.GetManifestPath() {
		return false
	}
	if oldSeg.GetSchemaVersion() != incoming.GetSchemaVersion() {
		return false
	}
	// applyExternalRefreshPatch only overwrites the storage version when the
	// incoming one is non-zero, so a zero incoming version never changes it.
	if incoming.GetStorageVersion() != 0 && oldSeg.GetStorageVersion() != incoming.GetStorageVersion() {
		return false
	}
	oldBinlogs := oldSeg.GetBinlogs()
	newBinlogs := incoming.GetBinlogs()
	if len(oldBinlogs) != len(newBinlogs) {
		return false
	}
	for i := range oldBinlogs {
		if !proto.Equal(oldBinlogs[i], newBinlogs[i]) {
			return false
		}
	}
	return true
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
		if err == nil {
			return
		}
		// Classify by cause (see isRetryableRefreshFailure): a data/request error
		// fails the job (a rerun reproduces it); anything transient is re-dispatched.
		if isRetryableRefreshFailure(err) {
			mlog.Warn(context.TODO(), "failed to create refresh task on worker, retrying", mlog.Err(err))
			t.resetTask(err.Error())
			return
		}
		mlog.Warn(context.TODO(), "failed to create refresh task on worker, failing job", mlog.Err(err))
		if updateErr := t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, err.Error()); updateErr != nil {
			mlog.Warn(context.TODO(), "failed to persist Failed state after create error", mlog.Err(updateErr))
		}
	}()

	mlog.Info(context.TODO(), "creating refresh task on worker")

	if t.mt == nil {
		err = merr.WrapErrServiceInternalMsg("meta is nil, cannot create task on worker")
		return
	}

	// Fence a re-dispatch: if this task carries a node from a prior attempt (it
	// was reset to Init after a stale-manifest rebuild or a transient failure),
	// drop the stale worker-side entry first. The DataNode dedups by taskID, so
	// without this the re-dispatch would replay the prior result instead of
	// re-running. A transient drop failure returns and is retried on the next
	// tick (ErrNodeNotFound means the entry is already gone, so proceed).
	if prevNode := t.GetNodeId(); prevNode != 0 {
		if dropErr := cluster.DropRefreshExternalCollectionTask(prevNode, t.GetTaskId(), t.GetVersion()); dropErr != nil &&
			!errors.Is(dropErr, merr.ErrNodeNotFound) {
			err = dropErr
			return
		}
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
		// Collection gone (dropped) — a permanent, non-retryable condition.
		err = merr.WrapErrCollectionNotFound(t.GetCollectionId())
		return
	}
	if len(collInfo.Partitions) != 1 {
		// Internal metadata invariant violation (external collections are created
		// single-partition): a System error per the blame test, but deterministic
		// on rerun, so mark it permanent instead of re-dispatching forever.
		err = errors.Mark(
			merr.WrapErrServiceInternalMsg("external collection %d expected exactly 1 partition, got %d", t.GetCollectionId(), len(collInfo.Partitions)),
			errExternalRefreshPermanent)
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
		TargetRowsPerSegment:   paramtable.Get().DataNodeCfg.ExternalCollectionTargetRowsPerSegment.GetAsInt64(),
		// TaskVersion fences worker-side attempts: UpdateTaskVersion bumped the
		// persisted version above, so this dispatch supersedes any prior attempt
		// of the same taskID still lingering on a worker, and that attempt's
		// late writes are dropped by the worker's version guard.
		TaskVersion: t.GetVersion(),
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
			_ = cluster.DropRefreshExternalCollectionTask(t.GetNodeId(), t.GetTaskId(), t.GetVersion())
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
			_ = cluster.DropRefreshExternalCollectionTask(t.GetNodeId(), t.GetTaskId(), t.GetVersion())
		}
		if err := t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, "job canceled: "+job.GetFailReason()); err != nil {
			mlog.Warn(context.TODO(), "failed to persist Failed state after job cancellation", mlog.Err(err))
		}
		return
	}

	// Query task status from worker
	resp, err := cluster.QueryRefreshExternalCollectionTask(t.GetNodeId(), t.GetTaskId())
	if err != nil {
		mlog.Warn(context.TODO(), "query refresh task result failed, retrying", mlog.Err(err))
		// A query RPC failure is transient (node blip / restart / reassignment),
		// not a data error, so retry the task instead of failing the whole job:
		// drop the worker-side entry and re-dispatch on the next tick. If the node
		// is gone the drop no-ops and the re-dispatch lands on a live node.
		t.dropAndResetTaskOnWorker(cluster, fmt.Sprintf("query task failed: %v", err))
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
		// The worker asked for a retry (transient internal failure). Honor it by
		// dropping the worker-side task and re-dispatching, instead of failing the
		// whole job. This mirrors the stats path's Retry/None handling.
		mlog.Warn(context.TODO(), "refresh task reported retry by worker, re-dispatching",
			mlog.String("state", state.String()), mlog.String("failReason", failReason))
		t.dropAndResetTaskOnWorker(cluster, fmt.Sprintf("worker requested retry: %s", failReason))

	default:
		mlog.Warn(context.TODO(), "refresh task in unknown state",
			mlog.String("state", state.String()))
	}
}

func (t *refreshExternalCollectionTask) DropTaskOnWorker(cluster session.Cluster) {
	// Drop task on worker to cancel execution and clean up resources
	err := cluster.DropRefreshExternalCollectionTask(t.GetNodeId(), t.GetTaskId(), t.GetVersion())
	if err != nil {
		mlog.Warn(context.TODO(), "failed to drop refresh task on worker", mlog.Err(err))
		return
	}

	mlog.Info(context.TODO(), "refresh task dropped successfully")
}

// resetTask atomically returns the task to Init so the inspector re-enqueues it
// and the scheduler re-dispatches it via CreateTaskOnWorker. It clears the stale
// result/progress in the same write (ResetTaskForRetry) so job-level aggregation
// cannot adopt a stale result and progress polls do not report a done task.
func (t *refreshExternalCollectionTask) resetTask(reason string) {
	// Fence to this attempt: a superseded attempt must not reset a task that has
	// already been re-dispatched under a newer version.
	applied, err := t.refreshMeta.ResetTaskForRetry(t.GetTaskId(), t.GetVersion(), reason)
	if err != nil {
		mlog.Warn(context.TODO(), "failed to reset refresh task for retry",
			mlog.Int64("taskID", t.GetTaskId()), mlog.Err(err))
		return
	}
	if !applied {
		return
	}
	t.SetState(indexpb.JobState_JobStateInit, reason)
}

// dropAndResetTaskOnWorker mirrors the stats retry path: it drops the worker-side
// task first and only resets to Init once the drop succeeds (or the node is gone),
// so the re-dispatch actually re-runs the work instead of the DataNode replaying
// its cached result — the worker dedups by taskID, so an un-dropped entry would be
// returned verbatim. If the drop fails transiently, the task is left as-is and the
// drop is retried on the next tick.
func (t *refreshExternalCollectionTask) dropAndResetTaskOnWorker(cluster session.Cluster, reason string) {
	if t.GetNodeId() != 0 {
		if err := cluster.DropRefreshExternalCollectionTask(t.GetNodeId(), t.GetTaskId(), t.GetVersion()); err != nil &&
			!errors.Is(err, merr.ErrNodeNotFound) {
			mlog.Warn(context.TODO(), "failed to drop refresh task for retry, will retry drop next tick",
				mlog.Int64("taskID", t.GetTaskId()), mlog.Err(err))
			return
		}
	}
	t.resetTask(reason)
}

// isRetryableRefreshFailure reports whether a refresh task failure should be
// retried (re-dispatched) rather than failing the whole job. Permanent means "a
// rerun deterministically reproduces the failure": genuine request errors
// (ErrParameterInvalid / ErrParameterMissing), a dropped collection, or an
// internal invariant violation explicitly marked errExternalRefreshPermanent.
// Note that permanence is deliberately decoupled from the merr Input/System
// blame classification — a System-classed invariant violation is still
// permanent. Everything else (RPC, allocation, etcd write, node loss,
// not-ready) defaults to retryable so an unclassified transient error
// self-heals; the per-job timeout is the ultimate bound.
//
// Beyond request/config errors and the explicit invariant marker, the permanent
// set now includes the non-retriable data/storage classes (ErrDataIntegrity,
// ErrStorage) so a corrupt input or a hard storage error fails fast instead of
// being hammered to the job deadline. The permanent checks come first so an
// explicit mark always wins over any retriable class it may wrap.
func isRetryableRefreshFailure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errExternalRefreshPermanent) ||
		errors.Is(err, merr.ErrCollectionNotFound) ||
		errors.Is(err, merr.ErrParameterInvalid) ||
		errors.Is(err, merr.ErrParameterMissing) ||
		errors.Is(err, merr.ErrDataIntegrity) ||
		errors.Is(err, merr.ErrStorage) {
		return false
	}
	// Everything else — RPC, allocation, etcd write, node loss, not-ready, an
	// object-store / Loon transient, or an untyped error — defaults to retryable
	// so a transient blip self-heals; the per-job timeout is the ultimate bound.
	return true
}
