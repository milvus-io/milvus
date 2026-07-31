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
	"path"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/util/segmentutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
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
	// apply aborts atomically and FinishJobWithApply resets the job's tasks to
	// Init so the worker rebuilds the patch on the current manifest, instead of
	// silently completing with a segment still missing the refreshed columns.
	errExternalRefreshStaleManifest = errors.New("external refresh column patch built on a stale manifest")
	// errExternalRefreshUnverifiableAttempt is a rolling-upgrade/restart repair
	// signal: the persisted attempt predates metadata needed to prove that a birth
	// result belongs to its coordinator-allocated ID range. The job stays active
	// and rebuilds the task with a freshly persisted range before adoption.
	errExternalRefreshUnverifiableAttempt = errors.New("external refresh attempt result cannot be verified")
	// errExternalRefreshPermanent marks a refresh failure as permanent so the
	// retry classifier fails the task instead of re-dispatching. This is an
	// explicit signal, decoupled from the merr Input/System classification: an
	// INTERNAL invariant violation (a Milvus bug or corrupted metadata) is a
	// System error per the blame test, yet retrying it is pointless because a
	// rerun deterministically reproduces it. Attach with errors.Mark so the
	// underlying merr class is preserved.
	errExternalRefreshPermanent = errors.New("permanent external refresh failure")

	// errExternalRefreshTransientCommit marks an infrastructure failure raised
	// while committing an apply — a catalog/etcd write that may well succeed on
	// the next tick. FinishJobWithApply routes it back to Init like the CAS
	// sentinels, so an etcd blip does not discard work every worker already
	// finished; tryTimeoutJob still bounds the loop.
	//
	// Deterministic apply failures deliberately do NOT carry it. A transaction
	// over the metastore op limit, a collection mismatch or an invalid manifest
	// reproduces identically on every retry, so marking those retryable would
	// spin until the job timeout with no signal to act on. Attach with
	// errors.Mark so the underlying cause and its merr class are preserved.
	errExternalRefreshTransientCommit = errors.New("transient failure committing external refresh")
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
	applied, err := t.refreshMeta.UpdateTaskResult(
		t.GetTaskId(), t.GetVersion(), state, failReason, keptSegments, updatedSegments)
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
	t.UpdatedSegments = cloneRefreshUpdatedSegments(updatedSegments)

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
	baseManifests map[int64]string,
	logFields ...mlog.Field,
) error {
	return applyExternalCollectionSegmentUpdateWithActions(
		ctx, mt, collectionID, keptSegmentIDs, updatedSegments, baseManifests, nil, logFields...)
}

func applyExternalCollectionSegmentUpdateWithActions(
	ctx context.Context,
	mt *meta,
	collectionID int64,
	keptSegmentIDs []int64,
	updatedSegments []*datapb.SegmentInfo,
	baseManifests map[int64]string,
	extraActions []metastore.UpdateAction,
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
			mlog.Int("baseManifests", len(baseManifests)),
		)...)

	keptSegmentMap := make(map[int64]bool)
	for _, segID := range keptSegmentIDs {
		// mt.GetSegment takes segMu; SegmentsInfo itself is a bare map, so a
		// direct read here races a concurrent commit and kills the process.
		segment := mt.GetSegment(ctx, segID)
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
		if err := validateExternalRefreshUpdatedSegment(seg); err != nil {
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
	for segmentID, baseManifest := range baseManifests {
		if baseManifest == "" {
			return merr.WrapErrServiceInternalMsg("updated segment %d has an empty base manifest fence", segmentID)
		}
		if upsertSegmentMap[segmentID] == nil {
			return merr.WrapErrServiceInternalMsg("base manifest fence references missing updated segment %d", segmentID)
		}
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
			existing = mt.GetSegment(ctx, incoming.GetID())
		}
		if existing != nil {
			// Patching an existing segment is validated inside the upsert operator,
			// after the manifest CAS. Doing it here as well would re-introduce the
			// ordering bug: this read is outside segMu, so a concurrent schema bump
			// can make a stale-but-retryable result look like a hard schema rollback
			// and fail the job before the CAS ever runs.
			continue
		}
		if _, hasBase := baseManifests[incoming.GetID()]; hasBase {
			return merr.WrapErrServiceInternalMsg("new segment %d unexpectedly carries a base manifest fence", incoming.GetID())
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

	// Build update operators. Patch content is validated in exactly one place —
	// inside the upsert operator, after the manifest CAS — so the CAS gets to
	// classify a concurrent conflict as retryable before any content rule can
	// report it as a hard failure. Both rejection paths use modPack.fail, which
	// sets updatePack.err and makes UpdateSegmentsInfo return before persisting
	// anything, so the drop operator's pending mutations are discarded.
	var operators []UpdateOperator

	// Operator 1: Drop the segments selected from the validated snapshot above.
	// Do not re-scan the collection while applying: a concurrently-created
	// segment was not part of this refresh response and must not be dropped, and
	// keeping this set fixed also makes the preflight transaction count an upper
	// bound on the eventual atomic commit.
	dropOperator := func(modPack *updateSegmentPack) bool {
		for _, segmentID := range segmentsToDrop {
			segment := modPack.Get(segmentID)
			if segment != nil {
				updateSegStateAndPrepareMetrics(segment, commonpb.SegmentState_Dropped, modPack.metricMutation)
				segment.DroppedAt = uint64(time.Now().UnixNano())
				modPack.segments[segmentID] = segment
				mlog.Info(context.TODO(), "marking segment as dropped",
					mlog.Int64("segmentID", segmentID),
					mlog.Int64("numRows", segment.GetNumOfRows()))
			}
		}
		return true
	}
	operators = append(operators, dropOperator)

	// Operator 2: Add new segments or patch existing active segments.
	for _, seg := range validUpdatedSegments {
		incoming := seg
		baseManifest := baseManifests[incoming.GetID()]
		upsertOperator := func(modPack *updateSegmentPack) bool {
			existing := modPack.Get(incoming.GetID())
			if existing != nil {
				// Optimistic-concurrency CAS, evaluated here inside the segMu
				// critical section (modPack.Get is the synchronized read) so the
				// decision is atomic with the patch. DataCoord persisted baseManifest
				// from the exact currentSegments snapshot dispatched to the worker. Fail
				// closed for an EXISTING segment: adopt
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
				// tasks to Init and the next dispatch captures the current manifest as
				// its new coordinator-owned base, instead of silently
				// completing with a segment still missing the refreshed data.
				//
				// validateManifestSuccessor also enforces that the result is a legal
				// successor of the current manifest (same base path, strictly forward,
				// parseable), not just that the base matched — so a buggy / corrupt /
				// mixed-version worker that carries the right base but a result pointing
				// at another segment or an older version cannot silently corrupt the
				// segment pointer.
				//
				// NOTE: this deliberately fails CLOSED on an empty base. An old DataNode
				// or an old persisted task still supplies the complete SegmentInfo through
				// field 3/16, so no result is lost; it simply lacks the additive fence and
				// is rebuilt on a capable worker before an existing-segment write is
				// adopted.
				isReplay, adoptErr := validateManifestSuccessor(baseManifest, existing.GetManifestPath(), incoming.GetManifestPath())
				if adoptErr != nil {
					mlog.Warn(context.TODO(), "external refresh patch is not a valid successor; aborting apply to rebuild on the current manifest",
						mlog.Int64("segmentID", incoming.GetID()),
						mlog.String("baseManifest", baseManifest),
						mlog.String("currentManifest", existing.GetManifestPath()),
						mlog.String("resultManifest", incoming.GetManifestPath()),
						mlog.Err(adoptErr))
					return modPack.fail(errors.Wrapf(errExternalRefreshStaleManifest,
						"segment %d: %v", incoming.GetID(), adoptErr))
				}
				// This is the ONLY place the patch content is validated, and its
				// position is load-bearing in BOTH directions.
				//
				// It runs AFTER the CAS: a result that lost a concurrent race is stale
				// in every dimension at once (a schema bump advances the schema version
				// and the manifest in the same mutation), so validating content first
				// would report a stale patch as a hard "schema version rollback" and
				// fail the job instead of letting the CAS classify it as a retryable
				// stale-manifest conflict. Reaching here means base==current, so a
				// content violation really is a self-contradictory result.
				//
				// It also runs BEFORE the no-op short-circuit: externalRefreshPatchIsNoop
				// only compares the fields applyExternalRefreshPatch would write, so a
				// result that matches those yet violates an invariant it does NOT compare
				// (row count, collection ownership, dropped state, binlog row
				// consistency) would otherwise be reported as a successful replay.
				//
				// modPack.fail (not a deferred error flag) is what actually aborts:
				// UpdateSegmentsInfo only stops on updatePack.err, so this discards the
				// drop operator's pending mutations instead of persisting them.
				if err := validateExternalRefreshPatch(existing, incoming, collectionID); err != nil {
					mlog.Warn(context.TODO(), "invalid external refresh segment patch",
						mlog.Int64("segmentID", incoming.GetID()),
						mlog.Err(err))
					return modPack.fail(err)
				}
				if isReplay && externalRefreshPatchIsNoop(existing, incoming) {
					// A *complete* no-op: the result manifest, schema version, fake
					// binlogs and storage version all already match the segment. Keep
					// it as-is so its text/JSON stats survive (re-applying would clear
					// them). modPack.Get cloned it only for comparison, so remove that
					// untouched clone from the pending mutation set as well.
					//
					// This is the ONLY case that may skip the base check, and it is safe
					// precisely because it writes nothing. It has to be exempt: a crash
					// replay (AlterSegments landed, the job state did not) re-arrives with
					// current already advanced to result, so its base is legitimately
					// stale and demanding base==current would reject a correct replay
					// forever.
					delete(modPack.segments, incoming.GetID())
					return true
				}
				// A same-manifest replay that is NOT a full no-op still carries
				// metadata the segment has not absorbed yet — e.g. the worker found the
				// column already appended on the object store and returned the unchanged
				// manifest while still bumping the schema version and rebuilding the fake
				// binlogs for the new column. Manifest-pointer equality does not mean the
				// SegmentInfo metadata landed, so apply it; the pointer assignment in
				// applyExternalRefreshPatch is a no-op because result == current.
				//
				// But it is still a WRITE, so it owes the same proof as any other write:
				// validateManifestSuccessor short-circuits on result==current WITHOUT
				// looking at the base, so without this check a result carrying an empty
				// or stale base could still rewrite schema version and binlogs — exactly
				// the fail-closed guarantee the CAS comment above promises for an
				// existing segment. Re-assert it here rather than inside
				// validateManifestSuccessor: the schema-bump compaction caller shares
				// that helper and relies on the unconditional replay short-circuit for
				// its own crash recovery, where a stale base is expected and correct.
				if isReplay && (baseManifest == "" || baseManifest != existing.GetManifestPath()) {
					mlog.Warn(context.TODO(), "same-manifest external refresh patch carries no proof it was built on the current manifest; aborting apply",
						mlog.Int64("segmentID", incoming.GetID()),
						mlog.String("baseManifest", baseManifest),
						mlog.String("currentManifest", existing.GetManifestPath()),
						mlog.String("resultManifest", incoming.GetManifestPath()))
					return modPack.fail(errors.Wrapf(errExternalRefreshStaleManifest,
						"segment %d: same-manifest patch has base manifest %q, want %q",
						incoming.GetID(), baseManifest, existing.GetManifestPath()))
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

			// A genuinely new segment: DataCoord materializes the authoritative
			// SegmentInfo from only the worker-owned result fields (manifest / binlogs /
			// row count / versions) plus collection context it owns. Any collection,
			// partition, channel, state or level carried in the worker's SegmentInfo is
			// ignored.
			materialized := materializeExternalRefreshSegment(incoming, collectionID, partitionID, insertChannel)
			segInfo := NewSegmentInfo(materialized)
			modPack.segments[incoming.GetID()] = segInfo

			modPack.increments[incoming.GetID()] = metastore.BinlogsIncrement{
				Segment: materialized,
			}

			modPack.metricMutation.addNewSeg(
				materialized.GetState(),
				materialized.GetLevel(),
				materialized.GetIsSorted(),
				materialized.GetStorageVersion(),
				segmentMetricFormatLabel(segInfo),
				materialized.GetNumOfRows(),
			)

			mlog.Info(context.TODO(), "adding new segment",
				mlog.Int64("segmentID", incoming.GetID()),
				mlog.Int64("numRows", incoming.GetNumOfRows()))
			return true
		}
		operators = append(operators, upsertOperator)
	}

	if len(extraActions) > 0 {
		// Every snapshot-selected drop and every result that actually mutates the
		// segment contributes one action, followed by the supplied job action(s).
		// Check this before index invalidation, which is itself a catalog write.
		// The etcd/backend cap cannot be raised by DataCoord, and splitting segment
		// changes from the Finished marker would violate atomic adoption.
		//
		// Full no-ops are excluded: the upsert operator drops them from the pending
		// mutation set, so they cost nothing at commit time. Counting them here
		// would reject a crash replay — which re-sends every already-committed
		// segment — purely on its size, before the code that recognizes it as a
		// replay ever runs. The count is therefore a lower bound on what will be
		// written; the exact count is re-checked under segMu immediately before
		// commit, and that check is the authority.
		mutatingUpdates := 0
		for _, incoming := range validUpdatedSegments {
			if current := mt.GetSegment(ctx, incoming.GetID()); current != nil &&
				externalRefreshPatchIsNoop(current, incoming) {
				continue
			}
			mutatingUpdates++
		}
		operationCount := len(segmentsToDrop) + mutatingUpdates + len(extraActions)
		if err := validateExternalRefreshAtomicTxnSize(operationCount); err != nil {
			return err
		}
	}

	// Invalidate indexes before publishing an in-place patch. Removing an index
	// unnecessarily is safe (the inspector rebuilds it); publishing new segment
	// data while leaving an old index visible is not. The persisted DataManifest
	// fence also lets restart recovery find any index build that raced this pass.
	if err := invalidateExternalRefreshIndexes(ctx, mt, validUpdatedSegments); err != nil {
		return err
	}

	var err error
	if len(extraActions) > 0 {
		err = mt.UpdateSegmentsInfoWithActions(ctx, extraActions, operators...)
	} else {
		err = mt.UpdateSegmentsInfo(ctx, operators...)
	}
	if err != nil {
		mlog.Warn(context.TODO(), "failed to update external collection segments", mlog.Err(err))
		return err
	}
	// Catch an index build that started after the pre-commit invalidation but read
	// the old segment manifest. This cleanup is best-effort after the segment/job
	// transaction has committed; the persisted DataManifest mismatch is the
	// durable repair signal for the index inspector if this call fails or we crash.
	if cleanupErr := invalidateExternalRefreshIndexes(ctx, mt, validUpdatedSegments); cleanupErr != nil {
		mlog.Warn(ctx, "failed to remove stale indexes after external refresh commit; inspector will retry",
			mlog.Err(cleanupErr))
	}
	mlog.Info(context.TODO(), "external collection segments updated successfully",
		mlog.Int("updatedSegments", len(updatedSegments)),
		mlog.Int("keptSegments", len(keptSegmentIDs)))

	return nil
}

func invalidateExternalRefreshIndexes(
	ctx context.Context,
	mt *meta,
	updatedSegments []*datapb.SegmentInfo,
) error {
	if mt == nil || mt.indexMeta == nil {
		return nil
	}
	for _, incoming := range updatedSegments {
		if incoming == nil {
			continue
		}
		// Go through meta's locked accessor: SegmentsInfo is a bare map guarded
		// only by segMu, and this runs before UpdateSegmentsInfo takes it, so a
		// direct read races a concurrent stats/compaction commit and aborts the
		// process with "concurrent map read and map write".
		current := mt.GetSegment(ctx, incoming.GetID())
		if current == nil {
			continue
		}
		// Two conditions, because this runs on both sides of the segment commit.
		// Pre-commit the segment still holds the old manifest, so every existing
		// index is already stale by the fence and metadataChanges only echoes it.
		// Post-commit the segment holds the incoming manifest, so metadataChanges
		// is false and the fence alone catches a build that registered against the
		// old manifest during the window. metadataChanges therefore adds coverage
		// only for a same-manifest patch whose binlogs or schema version moved;
		// over-invalidating there costs a rebuild, while under-invalidating would
		// leave an index serving over republished data.
		metadataChanges := !externalRefreshPatchIsNoop(current, incoming)
		for _, segIndex := range mt.indexMeta.GetAllSegmentIndexes(incoming.GetID()) {
			if !metadataChanges && !segmentIndexManifestStale(segIndex, incoming.GetManifestPath()) {
				continue
			}
			if err := mt.indexMeta.RemoveSegmentIndex(ctx, segIndex.BuildID); err != nil {
				// A catalog write, not a verdict on the result: mark it transient so
				// an etcd blip reschedules the apply instead of failing a job whose
				// workers all succeeded.
				return errors.Mark(
					merr.Wrapf(err, "remove stale index build %d for refreshed segment %d",
						segIndex.BuildID, incoming.GetID()),
					errExternalRefreshTransientCommit)
			}
		}
	}
	return nil
}

func validateExternalRefreshUpdatedSegment(incoming *datapb.SegmentInfo) error {
	if incoming.GetID() <= 0 {
		return merr.WrapErrServiceInternalMsg("updated segment has invalid segment ID %d", incoming.GetID())
	}
	if incoming.GetManifestPath() == "" {
		return merr.WrapErrServiceInternalMsg("updated segment %d has empty manifest path", incoming.GetID())
	}
	if len(incoming.GetBinlogs()) == 0 {
		return merr.WrapErrServiceInternalMsg("updated segment %d has empty fake binlogs", incoming.GetID())
	}
	return nil
}

// materializeExternalRefreshSegment builds the SegmentInfo for a NEW segment out
// of the worker's result plus the collection context DataCoord owns. Every field
// the worker is not entitled to choose is set here — collection, partition,
// channel, state and level — so a malformed or malicious result cannot place a
// segment in another collection or resurrect it in a non-flushed state.
func materializeExternalRefreshSegment(
	incoming *datapb.SegmentInfo,
	collectionID int64,
	partitionID int64,
	insertChannel string,
) *datapb.SegmentInfo {
	return &datapb.SegmentInfo{
		ID:             incoming.GetID(),
		CollectionID:   collectionID,
		PartitionID:    partitionID,
		InsertChannel:  insertChannel,
		State:          commonpb.SegmentState_Flushed,
		Level:          datapb.SegmentLevel_L1,
		NumOfRows:      incoming.GetNumOfRows(),
		ManifestPath:   incoming.GetManifestPath(),
		SchemaVersion:  incoming.GetSchemaVersion(),
		StorageVersion: incoming.GetStorageVersion(),
		Binlogs:        cloneProtoFieldBinlogs(incoming.GetBinlogs()),
	}
}

func cloneProtoFieldBinlogs(src []*datapb.FieldBinlog) []*datapb.FieldBinlog {
	if len(src) == 0 {
		return nil
	}
	cloned := make([]*datapb.FieldBinlog, 0, len(src))
	for _, binlog := range src {
		if binlog == nil {
			continue
		}
		cloned = append(cloned, proto.Clone(binlog).(*datapb.FieldBinlog))
	}
	return cloned
}

func dispatchBaseManifests(segments []*datapb.SegmentInfo) map[int64]string {
	baseManifests := make(map[int64]string)
	for _, segment := range segments {
		if segment == nil || segment.GetID() <= 0 || segment.GetManifestPath() == "" {
			continue
		}
		baseManifests[segment.GetID()] = segment.GetManifestPath()
	}
	if len(baseManifests) == 0 {
		return nil
	}
	return baseManifests
}

func validateRefreshBaseManifestEcho(expected, echoed map[int64]string) error {
	for segmentID, echoedManifest := range echoed {
		expectedManifest, ok := expected[segmentID]
		if !ok {
			return merr.WrapErrServiceInternalMsg(
				"worker echoed a base manifest for segment %d outside the dispatch snapshot", segmentID)
		}
		if echoedManifest == "" || echoedManifest != expectedManifest {
			return merr.WrapErrServiceInternalMsg(
				"worker echoed base manifest %q for segment %d, expected %q",
				echoedManifest, segmentID, expectedManifest)
		}
	}
	return nil
}

func baseManifestsForUpdatedSegments(
	updatedSegments []*datapb.SegmentInfo,
	dispatchBaseManifests map[int64]string,
) map[int64]string {
	selected := make(map[int64]string)
	for _, segment := range updatedSegments {
		if segment == nil {
			continue
		}
		if baseManifest, ok := dispatchBaseManifests[segment.GetID()]; ok {
			selected[segment.GetID()] = baseManifest
		}
	}
	if len(selected) == 0 {
		return nil
	}
	return selected
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

func validateExternalRefreshBinlogRowCount(result *datapb.SegmentInfo, expectedRows int64) error {
	binlogRows := segmentutil.CalcRowCountFromBinLog(result)
	if binlogRows == -1 {
		return merr.WrapErrServiceInternalMsg("invalid binlog row count for segment %d", result.GetID())
	}
	if expectedRows > 0 && binlogRows != expectedRows {
		return merr.WrapErrServiceInternalMsg("binlog row count mismatch for segment %d: got %d, want %d",
			result.GetID(), binlogRows, expectedRows)
	}
	if binlogRows > 0 && binlogRows != result.GetNumOfRows() {
		return merr.WrapErrServiceInternalMsg("binlog row count mismatch for segment %d: got %d, segment rows %d",
			result.GetID(), binlogRows, result.GetNumOfRows())
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
	cloned.Binlogs = cloneProtoFieldBinlogs(incoming.GetBinlogs())
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
		baseManifestsForUpdatedSegments(resp.GetUpdatedSegments(), t.GetBaseManifests()),
		mlog.Int64("taskID", t.GetTaskId()),
	)
}

func (t *refreshExternalCollectionTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	timeout := paramtable.Get().DataCoordCfg.RequestTimeoutSeconds.GetAsDuration(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var err error
	workerMayExist := false
	defer func() {
		if err == nil {
			return
		}
		if workerMayExist {
			// Create may have reached the worker even when the RPC returned an
			// error. Do not make the task dispatchable (or terminal) until Drop
			// confirms the old attempt has completely exited.
			if dropErr := t.tryDropTaskOnWorker(cluster); dropErr != nil {
				mlog.Warn(context.TODO(), "failed to cancel refresh task after create error; keeping attempt in progress",
					mlog.Int64("taskID", t.GetTaskId()), mlog.Err(dropErr))
				return
			}
		}
		if errors.Is(err, errExternalRefreshJobTerminal) {
			reason := err.Error()
			if updateErr := t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, reason); updateErr != nil {
				mlog.Warn(context.TODO(), "failed to retire refresh task after owning job became terminal", mlog.Err(updateErr))
			}
			t.SetState(indexpb.JobState_JobStateFailed, reason)
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

	// Defense in depth against the narrow race after manager/inspector enqueue:
	// the owning job may become terminal while this task is waiting in the global
	// scheduler. Retire it before mutating attempt metadata or contacting a worker.
	if job := t.refreshMeta.GetJob(t.GetJobId()); job != nil && isTerminalJobState(job.GetState()) {
		reason := "owning job reached " + job.GetState().String()
		if job.GetFailReason() != "" {
			reason += ": " + job.GetFailReason()
		}
		if updateErr := t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, reason); updateErr != nil {
			mlog.Warn(context.TODO(), "failed to retire refresh task before worker dispatch", mlog.Err(updateErr))
		}
		// A superseded version fence may skip the persisted mutation; the local
		// wrapper must still become terminal so the scheduler does not requeue it.
		t.SetState(indexpb.JobState_JobStateFailed, reason)
		return
	}

	if t.mt == nil {
		err = merr.WrapErrServiceInternalMsg("meta is nil, cannot create task on worker")
		return
	}

	// Clear the prior attempt before re-dispatching: if this task carries a node
	// from a prior attempt (it was reset to Init after a stale-manifest rebuild
	// or a transient failure), drop the stale worker-side entry first. This is
	// load-bearing, not best-effort — the DataNode REJECTS a dispatch onto an
	// occupied taskID (ErrTaskDuplicate), so an un-dropped entry blocks the
	// re-dispatch entirely rather than being superseded by it. A transient drop
	// failure returns and is retried on the next tick (ErrNodeNotFound means the
	// node and its whole in-memory task map are gone, so proceed).
	if prevNode := t.GetNodeId(); prevNode != 0 {
		if dropErr := cluster.DropRefreshExternalCollectionTask(prevNode, t.GetTaskId()); dropErr != nil &&
			!errors.Is(dropErr, merr.ErrNodeNotFound) {
			err = dropErr
			return
		}
	}

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
	}
	targetSegmentBase := path.Join(
		req.GetStorageConfig().GetRootPath(),
		common.SegmentInsertLogPath,
		metautil.JoinIDPath(t.GetCollectionId(), partitionID),
	)

	// Reserve the exact attempt before contacting the worker. Version, node,
	// InProgress state, the coordinator-owned ID range/path, and every existing
	// segment manifest sent to the worker land in one task record. The CAS fence
	// therefore survives an ambiguous Create RPC, DataCoord restart, and an old
	// DataNode that returns only updatedSegments without echoing base_manifests.
	if err = t.refreshMeta.UpdateTaskVersion(
		t.GetTaskId(), nodeID, idRange, targetSegmentBase, dispatchBaseManifests(currentSegments)); err != nil {
		mlog.Warn(context.TODO(), "failed to reserve refresh task attempt", mlog.Err(err))
		return
	}

	// Re-read task from meta to sync the in-memory wrapper with the persisted
	// node/version/range fence used by later result adoption.
	updatedTask := t.refreshMeta.GetTask(t.GetTaskId())
	if updatedTask == nil {
		err = merr.WrapErrServiceInternalMsg("task %d not found after version update", t.GetTaskId())
		return
	}
	t.ExternalCollectionRefreshTask = updatedTask

	// Submit task to worker via unified task system
	workerMayExist = true
	err = cluster.CreateRefreshExternalCollectionTask(nodeID, req)
	if err != nil {
		mlog.Warn(context.TODO(), "failed to create refresh task on worker", mlog.Err(err))
		return
	}

	// Re-check the owning job after the RPC. UpdateTaskVersion already reserved
	// the attempt as InProgress before dispatch; this second write only closes the
	// race where the job became terminal while the worker RPC was in flight.
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
		if err := t.tryDropTaskOnWorker(cluster); err != nil {
			// Keep the task non-terminal so its persisted node/task identity remains
			// the cleanup owner. A later checker/query pass can retry the Drop.
			mlog.Warn(context.TODO(), "failed to drop refresh task on worker after job disappeared; retaining cleanup ownership",
				mlog.Int64("taskID", t.GetTaskId()), mlog.Int64("nodeID", t.GetNodeId()), mlog.Err(err))
			return
		}
		if err := t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, "job canceled"); err != nil {
			mlog.Warn(context.TODO(), "failed to persist Failed state after job cancellation", mlog.Err(err))
		}
		return
	}
	if job.GetState() == indexpb.JobState_JobStateFailed {
		mlog.Info(context.TODO(), "job has been marked as failed, canceling task",
			mlog.String("jobFailReason", job.GetFailReason()))
		if err := t.tryDropTaskOnWorker(cluster); err != nil {
			mlog.Warn(context.TODO(), "failed to drop refresh task on worker after job failed; retaining cleanup ownership",
				mlog.Int64("taskID", t.GetTaskId()), mlog.Int64("nodeID", t.GetNodeId()), mlog.Err(err))
			return
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
		// base_manifests in the response is optional for old workers and is never
		// the CAS source of truth. When present, verify it against the exact
		// DataCoord-owned dispatch snapshot so a buggy worker cannot misstate what
		// it built on without detection.
		if err := validateRefreshBaseManifestEcho(t.GetBaseManifests(), resp.GetBaseManifests()); err != nil {
			mlog.Warn(context.TODO(), "worker returned an invalid external refresh manifest echo", mlog.Err(err))
			t.dropAndResetTaskOnWorker(cluster, err.Error())
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
	if err := t.tryDropTaskOnWorker(cluster); err != nil {
		mlog.Warn(context.TODO(), "failed to drop refresh task on worker", mlog.Err(err))
	}
}

func (t *refreshExternalCollectionTask) tryDropTaskOnWorker(cluster session.Cluster) error {
	if t.GetNodeId() == 0 {
		return nil
	}
	err := cluster.DropRefreshExternalCollectionTask(t.GetNodeId(), t.GetTaskId())
	if err != nil && !errors.Is(err, merr.ErrNodeNotFound) {
		return err
	}
	mlog.Info(context.TODO(), "refresh task dropped successfully")
	return nil
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
	if err := t.tryDropTaskOnWorker(cluster); err != nil {
		mlog.Warn(context.TODO(), "failed to drop refresh task for retry, will retry drop next tick",
			mlog.Int64("taskID", t.GetTaskId()), mlog.Err(err))
		return
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
