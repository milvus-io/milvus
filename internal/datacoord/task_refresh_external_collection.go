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
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/time/rate"
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

// refreshQueryLogRate caps the per-poll logs at one line per second per
// process. Polls run on the scheduler round (100ms by default) and say the same
// thing every time; the transitions they lead to are logged separately.
const refreshQueryLogRate = rate.Limit(1)

// refreshExternalCollectionTask wraps ExternalCollectionRefreshTask for scheduling.
// This is used by the global task scheduler to dispatch refresh tasks to DataNodes.
type refreshExternalCollectionTask struct {
	*datapb.ExternalCollectionRefreshTask

	// stateGuard makes the embedded proto readable by the scheduler without the
	// per-task key lock. Callbacks replace the whole embedded pointer (meta
	// re-reads) and mutate State under that key lock, but the scheduler's phase
	// derivation and metrics pass read GetTaskID/GetTaskState/GetTaskVersion
	// lock-free on their own goroutines; see statsTask.stateGuard.
	stateGuard sync.RWMutex

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
	// beginExecution/endExecution form the manager-owned job lease around worker
	// callbacks. The lease closes the gap between a pending wrapper being popped
	// from the scheduler queue and acquiring the scheduler's per-task lock.
	beginExecution func(jobID int64) bool
	endExecution   func(jobID int64)
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
	t.stateGuard.RLock()
	defer t.stateGuard.RUnlock()
	return t.TaskId
}

func (t *refreshExternalCollectionTask) GetTaskType() taskcommon.Type {
	return taskcommon.RefreshExternalCollection
}

func (t *refreshExternalCollectionTask) GetTaskState() taskcommon.State {
	t.stateGuard.RLock()
	state := t.GetState()
	t.stateGuard.RUnlock()
	if state == indexpb.JobState_JobStateRetry {
		// Retry is durable replacement debt handled by the inspector. Expose the
		// old attempt as terminal to the global scheduler so it cannot dispatch
		// the same task ID again while a fresh replacement is being published.
		return taskcommon.Failed
	}
	// taskcommon.State is a type alias of indexpb.JobState, so this is type-safe.
	return state
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
	t.stateGuard.RLock()
	defer t.stateGuard.RUnlock()
	return t.GetVersion()
}

// swapTaskProto replaces the embedded proto after a meta re-read, under
// stateGuard so the scheduler's lock-free readers never observe a torn pointer.
func (t *refreshExternalCollectionTask) swapTaskProto(updated *datapb.ExternalCollectionRefreshTask) {
	t.stateGuard.Lock()
	t.ExternalCollectionRefreshTask = updated
	t.stateGuard.Unlock()
}

// retryWorkerFailure records a worker failure that another attempt could still
// get past, spending one of the task's attempts (issue #52445 tracked the
// worker-lost-the-task case: the worker now reports that as JobStateRetry with
// "task result not found", so it lands here and is re-dispatched rather than
// being mistaken for the work itself failing).
func (t *refreshExternalCollectionTask) retryWorkerFailure(reason string) error {
	maxRetryTimes := paramtable.Get().DataCoordCfg.ExternalCollectionMaxRetryTimes.GetAsInt64()
	if maxRetryTimes < 1 {
		maxRetryTimes = 1
	}
	return t.recordWorkerFailure(reason, maxRetryTimes)
}

// failWorkerPermanently ends the attempt on the first report.
//
// A worker failure that blames the request -- zero total rows in the source, a
// function field the schema does not have -- cannot be fixed by handing the
// same request to another node. Spending the retry budget on it only delays the
// RefreshFailed the caller is waiting for, which is what left permanent input
// errors sitting in RefreshInProgress. The worker draws the line by reporting
// Failed rather than Retry (see externalRefreshFailureState); a cap of one
// attempt is how that decision is honored here.
func (t *refreshExternalCollectionTask) failWorkerPermanently(reason string) error {
	return t.recordWorkerFailure(reason, 1)
}

func (t *refreshExternalCollectionTask) recordWorkerFailure(reason string, maxRetryTimes int64) error {
	updated, _, applied, err := t.refreshMeta.RecordTaskWorkerFailure(t.GetTaskId(), maxRetryTimes, reason)
	if err != nil {
		return err
	}
	if updated != nil {
		t.swapTaskProto(updated)
	}
	if applied && updated.GetState() == indexpb.JobState_JobStateFailed && t.processFinishedJob != nil {
		t.processFinishedJob(t.GetJobId())
	}
	return nil
}

func isTerminalExternalRefreshJob(job *datapb.ExternalCollectionRefreshJob) bool {
	if job == nil {
		return true
	}
	return job.GetState() == indexpb.JobState_JobStateFinished ||
		job.GetState() == indexpb.JobState_JobStateFailed
}

// cancelForTerminalJob closes the scheduler tail race where a pending wrapper
// has already been popped into an executor future just before GC drains the
// scheduler maps. A missing or terminal job no longer owns worker work, so the
// wrapper must become terminal locally even when its task metadata was already
// removed and the best-effort state persistence cannot succeed.
func (t *refreshExternalCollectionTask) cancelForTerminalJob(ctx context.Context, cluster session.Cluster) bool {
	job := t.refreshMeta.GetJob(t.GetJobId())
	if !isTerminalExternalRefreshJob(job) {
		return false
	}

	reason := "job canceled"
	if job != nil {
		reason = fmt.Sprintf("job canceled in state %s", job.GetState().String())
		if job.GetFailReason() != "" {
			reason += ": " + job.GetFailReason()
		}
	}
	if t.GetNodeId() != 0 {
		if err := cluster.DropRefreshExternalCollectionTask(t.GetNodeId(), t.GetTaskId()); err != nil {
			mlog.Warn(ctx, "failed to drop canceled external refresh task",
				mlog.FieldTaskID(t.GetTaskId()),
				mlog.Err(err))
		}
	}
	if err := t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, reason); err != nil {
		mlog.Warn(ctx, "failed to persist external refresh cancellation; terminating scheduler wrapper locally",
			mlog.FieldJobID(t.GetJobId()),
			mlog.FieldTaskID(t.GetTaskId()),
			mlog.Err(err))
		t.SetState(indexpb.JobState_JobStateFailed, reason)
	}
	return true
}

// terminateIfOwnershipGone prevents a popped scheduler future from requeueing
// itself when a concurrent composite DropJob won the metadata lock race after
// the future's initial job check.
func (t *refreshExternalCollectionTask) terminateIfOwnershipGone(reason string) {
	job := t.refreshMeta.GetJob(t.GetJobId())
	if isTerminalExternalRefreshJob(job) || t.refreshMeta.GetTask(t.GetTaskId()) == nil {
		t.SetState(indexpb.JobState_JobStateFailed, reason)
	}
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
	t.stateGuard.Lock()
	defer t.stateGuard.Unlock()
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
	alreadyAppliedNewSegments := make(map[int64]struct{})

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
				alreadyAppliedNewSegments[segmentID] = struct{}{}
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
			if _, ok := alreadyAppliedNewSegments[incoming.GetID()]; ok {
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

// externalRefreshNewSegmentAlreadyApplied recognizes an idempotent replay of a
// new, non-baseline segment. A manifest base path belongs to one segment, and
// its versions move forward as that segment gains later manifest updates. An
// equal or newer existing version therefore means the incoming refresh result
// has already been applied or superseded and must not be written again.
//
// Do not compare fake binlogs here: V3 catalog persistence intentionally strips
// them, so their in-memory representation does not survive DataCoord restart.
func externalRefreshNewSegmentAlreadyApplied(existing *SegmentInfo, incoming *datapb.SegmentInfo) bool {
	if existing == nil || incoming == nil {
		return false
	}
	if existing.GetID() != incoming.GetID() ||
		existing.GetCollectionID() != incoming.GetCollectionID() ||
		existing.GetPartitionID() != incoming.GetPartitionID() {
		return false
	}

	comparison, err := packed.CompareManifestPath(existing.GetManifestPath(), incoming.GetManifestPath())
	return err == nil && comparison >= 0
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
	if t.beginExecution != nil {
		if !t.beginExecution(t.GetJobId()) {
			t.SetState(indexpb.JobState_JobStateFailed, "job canceled before worker dispatch")
			return
		}
		if t.endExecution != nil {
			defer t.endExecution(t.GetJobId())
		}
	}
	// A replacement reuses the same immutable manifest range and owned segments,
	// so it cannot repair a malformed plan or invalid local metadata. Keep those
	// failures terminal and separate from transient failures that happen before
	// any worker RPC is sent.
	failPermanent := func(cause error) {
		log.Warn(ctx, "external refresh task is not dispatchable", mlog.Err(cause))
		if updateErr := t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, cause.Error()); updateErr != nil {
			log.Warn(ctx, "failed to persist Failed state for an invalid refresh task", mlog.Err(updateErr))
			t.terminateIfOwnershipGone(cause.Error())
		}
	}

	log.Info(ctx, "creating refresh task on worker",
		mlog.Int64("fileIndexBegin", t.GetFileIndexBegin()),
		mlog.Int64("fileIndexEnd", t.GetFileIndexEnd()),
		mlog.Int64("fileCount", t.GetFileIndexEnd()-t.GetFileIndexBegin()),
		mlog.Int("ownedSegments", len(t.GetOwnedSegmentIds())))
	if t.cancelForTerminalJob(ctx, cluster) {
		return
	}

	if t.mt == nil {
		failPermanent(merr.WrapErrServiceInternalMsg("meta is nil, cannot create task on worker"))
		return
	}
	if !isSupportedExternalRefreshOwnershipPlanVersion(t.GetOwnershipPlanVersion()) {
		failPermanent(merr.WrapErrServiceInternalMsg(
			"external refresh task %d has unsupported ownership plan version %d; retry refresh",
			t.GetTaskId(),
			t.GetOwnershipPlanVersion(),
		))
		return
	}

	// No worker has seen the task yet. A catalog failure here is safe to retry
	// with the same Init task ID; the global scheduler supplies exponential
	// backoff and no worker-failure attempt is consumed.
	if err := t.refreshMeta.UpdateTaskVersion(t.GetTaskId(), nodeID); err != nil {
		log.Warn(ctx, "failed to update task version; keeping task Init for retry", mlog.Err(err))
		// A concurrent job/task drop is not a transient catalog failure. End the
		// detached scheduler wrapper locally so it cannot retry work with no owner.
		t.terminateIfOwnershipGone(err.Error())
		return
	}

	// Re-read task from meta to sync in-memory state (nodeID and version)
	updatedTask := t.refreshMeta.GetTask(t.GetTaskId())
	if updatedTask == nil {
		failPermanent(merr.WrapErrServiceInternalMsg("task %d not found after version update", t.GetTaskId()))
		return
	}
	t.swapTaskProto(updatedTask)

	ownedSegmentIDs := t.GetOwnedSegmentIds()
	currentSegments := make([]*datapb.SegmentInfo, 0, len(ownedSegmentIDs))
	seenOwnedSegments := make(map[int64]struct{}, len(t.GetOwnedSegmentIds()))
	for _, segmentID := range ownedSegmentIDs {
		if _, ok := seenOwnedSegments[segmentID]; ok {
			failPermanent(merr.WrapErrServiceInternalMsg("task %d contains duplicate owned segment %d", t.GetTaskId(), segmentID))
			return
		}
		seenOwnedSegments[segmentID] = struct{}{}
	}

	segmentSnapshots := getExternalRefreshSegmentSnapshots(t.mt, ownedSegmentIDs)
	for i, segmentID := range ownedSegmentIDs {
		segment := segmentSnapshots[i]
		if segment == nil {
			failPermanent(merr.WrapErrServiceInternalMsg("owned segment %d not found for task %d", segmentID, t.GetTaskId()))
			return
		}
		if segment.GetCollectionID() != t.GetCollectionId() {
			failPermanent(merr.WrapErrServiceInternalMsg(
				"owned segment %d belongs to collection %d, expected %d",
				segmentID,
				segment.GetCollectionID(),
				t.GetCollectionId(),
			))
			return
		}
		if !isSegmentHealthy(segment) {
			failPermanent(merr.WrapErrServiceInternalMsg("owned segment %d is not active", segmentID))
			return
		}
		currentSegments = append(currentSegments, segment.SegmentInfo)
	}

	log.Info(ctx, "collected owned current segments", mlog.Int("segmentCount", len(currentSegments)))

	// Pre-allocate segment IDs for data mapping
	preAllocCount := paramtable.Get().DataCoordCfg.ExternalCollectionPreAllocSegments.GetAsInt64()

	idBegin, idEnd, err := t.allocator.AllocN(preAllocCount)
	if err != nil {
		// Allocation happens before dispatch. Retrying the same task is safe even
		// if an allocator consumed a range before returning an ambiguous error:
		// no worker can reference that range yet.
		log.Warn(ctx, "failed to batch allocate segment IDs; keeping task Init for retry", mlog.Err(err))
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
		failPermanent(merr.WrapErrServiceInternalMsg("collection %d not found in meta", t.GetCollectionId()))
		return
	}
	if len(collInfo.Partitions) != 1 {
		failPermanent(merr.WrapErrServiceInternalMsg("external collection %d expected exactly 1 partition, got %d", t.GetCollectionId(), len(collInfo.Partitions)))
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
	if workerErr := cluster.CreateRefreshExternalCollectionTask(nodeID, req); workerErr != nil {
		if errors.Is(workerErr, merr.ErrServiceTooManyRequests) {
			// Admission rejection is not an execution attempt.  Keep the same
			// task ID in Init so the global scheduler's per-task exponential
			// backoff remains attached and no worker-failure budget is consumed.
			log.Info(ctx, "external collection worker has no free slot; retrying with scheduler backoff",
				mlog.Err(workerErr))
			return
		}
		// The create RPC is an at-least-once dispatch boundary. Whether the
		// worker accepted it and lost the response or failed before acceptance, a
		// fresh dispatch is safe: only the result selected by DataCoord is
		// committed, and unreferenced output files are reclaimed by orphan GC.
		log.Warn(ctx, "failed to create refresh task on worker, recording worker failure", mlog.Err(workerErr))
		if updateErr := t.retryWorkerFailure(workerErr.Error()); updateErr != nil {
			log.Warn(ctx, "failed to persist refresh task state after create error", mlog.Err(updateErr))
			t.terminateIfOwnershipGone(workerErr.Error())
		}
		return
	}

	// Create succeeded, so this is now a real worker attempt even if persisting
	// InProgress fails. Record exactly one failure through the same durable
	// Retry/Failed transition used for RPC-unknown and worker-reported failures.
	// RecordTaskWorkerFailure increments its process-local counter only after the
	// state write succeeds; if the catalog remains unavailable the wrapper stays
	// Init and the scheduler safely retries without inventing a consumed attempt.
	if persistErr := t.UpdateStateWithMeta(indexpb.JobState_JobStateInProgress, ""); persistErr != nil {
		attemptErr := merr.Wrap(persistErr, "worker accepted external refresh task but persisting InProgress failed")
		log.Warn(ctx, "worker accepted refresh task but its InProgress state was not persisted", mlog.Err(attemptErr))
		if updateErr := t.retryWorkerFailure(attemptErr.Error()); updateErr != nil {
			log.Warn(ctx, "failed to persist refresh task retry after InProgress persistence failure", mlog.Err(updateErr))
			t.terminateIfOwnershipGone(attemptErr.Error())
		}
		return
	}

	log.Info(ctx, "refresh task submitted successfully")
}

func (t *refreshExternalCollectionTask) QueryTaskOnWorker(cluster session.Cluster) {
	if t.beginExecution != nil {
		if !t.beginExecution(t.GetJobId()) {
			t.SetState(indexpb.JobState_JobStateFailed, "job canceled before worker query")
			return
		}
		if t.endExecution != nil {
			defer t.endExecution(t.GetJobId())
		}
	}

	// Check if job has been canceled/superseded before querying worker.
	if t.cancelForTerminalJob(context.TODO(), cluster) {
		return
	}

	// Every log below names the task: these are lifecycle-terminal events, and
	// an event without an identity cannot be correlated with anything.
	log := mlog.With(
		mlog.FieldJobID(t.GetJobId()),
		mlog.FieldTaskID(t.GetTaskId()),
		mlog.FieldNodeID(t.GetNodeId()))

	// Query task status from worker
	resp, err := cluster.QueryRefreshExternalCollectionTask(t.GetNodeId(), t.GetTaskId())
	if err != nil {
		log.Warn(context.TODO(), "query refresh task result failed", mlog.Err(err))
		if updateErr := t.retryWorkerFailure(fmt.Sprintf("query task failed: %v", err)); updateErr != nil {
			log.Warn(context.TODO(), "failed to persist refresh task state after query error", mlog.Err(updateErr))
		}
		return
	}

	state := resp.GetState()
	failReason := resp.GetFailReason()

	// Rated: this fires on every poll of every running task, and the poll runs
	// on the scheduler round (100ms by default). The state changes below are
	// logged at Info/Warn on their own; this one only reports that a poll
	// happened.
	mlog.RatedInfo(context.TODO(), refreshQueryLogRate, "queried refresh task status",
		mlog.FieldTaskID(t.GetTaskId()),
		mlog.String("state", state.String()),
		mlog.String("failReason", failReason))

	// Handle different task states
	switch state {
	case indexpb.JobState_JobStateFinished:
		// Validate source before processing - check if task has been superseded
		if err := t.validateSource(); err != nil {
			log.Warn(context.TODO(), "task validation failed, task has been superseded", mlog.Err(err))
			if updateErr := t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, err.Error()); updateErr != nil {
				log.Warn(context.TODO(), "failed to persist superseded task state", mlog.Err(updateErr))
			}
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
			log.Warn(context.TODO(), "failed to update task state to Finished", mlog.Err(err))
			return
		}
		log.Info(context.TODO(), "refresh task completed successfully")

	case indexpb.JobState_JobStateFailed:
		// The worker calls this one permanent: the request itself is what it
		// could not satisfy. Retrying it would only spend the budget that
		// transient faults need.
		if err := t.failWorkerPermanently(failReason); err != nil {
			log.Warn(context.TODO(), "failed to persist refresh task state after worker failure", mlog.Err(err))
			return
		}
		log.Warn(context.TODO(), "refresh task failed permanently on worker", mlog.String("reason", failReason))

	case indexpb.JobState_JobStateInProgress, indexpb.JobState_JobStateNone, indexpb.JobState_JobStateInit:
		// Task still in progress or not yet picked up by scheduler, no action needed
		mlog.RatedInfo(context.TODO(), refreshQueryLogRate, "refresh task still in progress",
			mlog.FieldTaskID(t.GetTaskId()),
			mlog.String("state", state.String()))

	case indexpb.JobState_JobStateRetry:
		// A transient fault on the worker. Spend an attempt.
		if err := t.retryWorkerFailure(failReason); err != nil {
			log.Warn(context.TODO(), "failed to persist refresh task state requested by worker", mlog.Err(err))
		}

	default:
		mlog.Warn(context.TODO(), "refresh task in unknown state",
			mlog.String("state", state.String()))
	}
}

func (t *refreshExternalCollectionTask) DropTaskOnWorker(cluster session.Cluster) {
	// A task that was never dispatched has no worker to tell. NullNodeID (-1)
	// marks unassigned; legacy records may carry the proto default 0.
	if t.GetNodeId() == NullNodeID || t.GetNodeId() == 0 {
		return
	}
	// Drop task on worker to cancel execution and clean up resources
	err := cluster.DropRefreshExternalCollectionTask(t.GetNodeId(), t.GetTaskId())
	if err != nil {
		mlog.Warn(context.TODO(), "failed to drop refresh task on worker",
			mlog.FieldTaskID(t.GetTaskId()), mlog.FieldNodeID(t.GetNodeId()), mlog.Err(err))
		return
	}

	mlog.Info(context.TODO(), "refresh task dropped successfully",
		mlog.FieldTaskID(t.GetTaskId()), mlog.FieldNodeID(t.GetNodeId()))
}
