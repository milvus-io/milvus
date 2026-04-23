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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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
		return fmt.Errorf("job %d not found", t.GetJobId())
	}

	currentSource := job.GetExternalSource()
	currentSpec := job.GetExternalSpec()

	taskSource := t.GetExternalSource()
	taskSpec := t.GetExternalSpec()

	if currentSource != taskSource || currentSpec != taskSpec {
		return fmt.Errorf(
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
		log.Warn("update refresh task state failed",
			zap.Int64("taskID", t.GetTaskId()),
			zap.String("state", state.String()),
			zap.String("failReason", failReason),
			zap.Error(err))
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
		log.Warn("update refresh task progress failed",
			zap.Int64("taskID", t.GetTaskId()),
			zap.Int64("progress", progress),
			zap.Error(err))
		return err
	}
	t.Progress = progress
	return nil
}

// SetJobInfo processes the task response and updates segment information atomically
func (t *refreshExternalCollectionTask) SetJobInfo(ctx context.Context, resp *datapb.RefreshExternalCollectionTaskResponse) error {
	if t.mt == nil {
		return fmt.Errorf("meta is nil, cannot update segments")
	}

	log := log.Ctx(ctx).With(
		zap.Int64("taskID", t.GetTaskId()),
		zap.Int64("collectionID", t.GetCollectionId()),
	)

	keptSegmentIDs := resp.GetKeptSegments()
	updatedSegments := resp.GetUpdatedSegments()

	log.Info("processing external collection update response",
		zap.Int("keptSegments", len(keptSegmentIDs)),
		zap.Int("updatedSegments", len(updatedSegments)))

	// Build kept segments map for fast lookup
	keptSegmentMap := make(map[int64]bool)
	for _, segID := range keptSegmentIDs {
		keptSegmentMap[segID] = true
	}

	// Safety validation: count current active segments and segments to be dropped
	currentSegments := t.mt.SelectSegments(ctx, CollectionFilter(t.GetCollectionId()))
	activeSegmentCount := 0
	segmentsToDrop := make([]int64, 0)
	for _, seg := range currentSegments {
		if seg.GetState() != commonpb.SegmentState_Dropped {
			activeSegmentCount++
			if !keptSegmentMap[seg.GetID()] {
				segmentsToDrop = append(segmentsToDrop, seg.GetID())
			}
		}
	}

	// Calculate the final segment count after operation
	finalSegmentCount := len(keptSegmentIDs) + len(updatedSegments)

	log.Info("segment update safety check",
		zap.Int("currentActiveSegments", activeSegmentCount),
		zap.Int("segmentsToDrop", len(segmentsToDrop)),
		zap.Int("keptSegments", len(keptSegmentIDs)),
		zap.Int("newSegments", len(updatedSegments)),
		zap.Int("finalSegmentCount", finalSegmentCount))

	// Safety check: reject if dropping all segments without adding new ones
	// This prevents accidental data loss from malformed worker responses
	if activeSegmentCount > 0 && finalSegmentCount == 0 {
		log.Error("safety check failed: refusing to drop all segments without replacement",
			zap.Int("activeSegmentCount", activeSegmentCount),
			zap.Int("keptSegments", len(keptSegmentIDs)),
			zap.Int("updatedSegments", len(updatedSegments)))
		return fmt.Errorf("safety check failed: refusing to drop all %d segments without replacement (keptSegments=%d, updatedSegments=%d)",
			activeSegmentCount, len(keptSegmentIDs), len(updatedSegments))
	}

	// Safety check: warn if dropping more than configured ratio of segments
	if activeSegmentCount > 0 && len(segmentsToDrop) > 0 {
		dropRatio := float64(len(segmentsToDrop)) / float64(activeSegmentCount)
		threshold := paramtable.Get().DataCoordCfg.ExternalCollectionDropRatioWarn.GetAsFloat()
		if threshold <= 0 {
			threshold = 0.9
		}
		if dropRatio > threshold {
			log.Warn("high segment drop ratio detected",
				zap.Float64("dropRatio", dropRatio),
				zap.Float64("threshold", threshold),
				zap.Int64s("segmentsToDrop", segmentsToDrop),
				zap.Int("activeSegmentCount", activeSegmentCount))
		}
	}

	// DataNode already used pre-allocated segment IDs and wrote manifests to final paths.
	// Just set the segment state to Flushed — no second ID allocation needed.
	// Also populate InsertChannel and PartitionID which DataNode doesn't set for external segments.
	// These are required for QueryCoord to include segments in its loading target.
	collInfo := t.mt.GetCollection(t.GetCollectionId())
	if collInfo == nil {
		return fmt.Errorf("collection %d not found in meta", t.GetCollectionId())
	}
	// External collections are single-shard, single-partition (enforced at creation).
	// Assert exactly-one here to catch any invariant violation from data corruption or legacy data.
	if len(collInfo.VChannelNames) != 1 {
		return fmt.Errorf("external collection %d expected exactly 1 VChannel, got %d", t.GetCollectionId(), len(collInfo.VChannelNames))
	}
	if len(collInfo.Partitions) != 1 {
		return fmt.Errorf("external collection %d expected exactly 1 partition, got %d", t.GetCollectionId(), len(collInfo.Partitions))
	}
	insertChannel := collInfo.VChannelNames[0]
	partitionID := collInfo.Partitions[0]
	for _, seg := range updatedSegments {
		seg.State = commonpb.SegmentState_Flushed
		if seg.InsertChannel == "" {
			seg.InsertChannel = insertChannel
		}
		if seg.PartitionID == 0 {
			seg.PartitionID = partitionID
		}
	}

	// Build update operators
	var operators []UpdateOperator

	// Operator 1: Drop segments not in kept list
	dropOperator := func(modPack *updateSegmentPack) bool {
		currentSegments := modPack.meta.segments.GetSegments()
		for _, seg := range currentSegments {
			// Skip segments not in this collection
			if seg.GetCollectionID() != t.GetCollectionId() {
				continue
			}

			// Skip segments that are already dropped
			if seg.GetState() == commonpb.SegmentState_Dropped {
				continue
			}

			// Drop segment if not in kept list
			if !keptSegmentMap[seg.GetID()] {
				segment := modPack.Get(seg.GetID())
				if segment != nil {
					updateSegStateAndPrepareMetrics(segment, commonpb.SegmentState_Dropped, modPack.metricMutation)
					segment.DroppedAt = uint64(time.Now().UnixNano())
					modPack.segments[seg.GetID()] = segment
					log.Info("marking segment as dropped",
						zap.Int64("segmentID", seg.GetID()),
						zap.Int64("numRows", seg.GetNumOfRows()))
				}
			}
		}
		return true
	}
	operators = append(operators, dropOperator)

	// Operator 2: Add new segments
	for _, seg := range updatedSegments {
		newSeg := seg // capture for closure
		addOperator := func(modPack *updateSegmentPack) bool {
			segInfo := NewSegmentInfo(newSeg)
			modPack.segments[newSeg.GetID()] = segInfo

			// Add binlogs increment
			modPack.increments[newSeg.GetID()] = metastore.BinlogsIncrement{
				Segment: newSeg,
			}

			// Update metrics
			modPack.metricMutation.addNewSeg(
				commonpb.SegmentState_Flushed,
				newSeg.GetLevel(),
				newSeg.GetIsSorted(),
				newSeg.GetStorageVersion(),
				newSeg.GetNumOfRows(),
			)

			log.Info("adding new segment",
				zap.Int64("segmentID", newSeg.GetID()),
				zap.Int64("numRows", newSeg.GetNumOfRows()))
			return true
		}
		operators = append(operators, addOperator)
	}

	// Execute all operators atomically
	if err := t.mt.UpdateSegmentsInfo(ctx, operators...); err != nil {
		log.Warn("failed to update segments atomically", zap.Error(err))
		return err
	}

	log.Info("external collection segments updated successfully",
		zap.Int("updatedSegments", len(updatedSegments)),
		zap.Int("keptSegments", len(keptSegmentIDs)))

	return nil
}

func (t *refreshExternalCollectionTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	timeout := paramtable.Get().DataCoordCfg.RequestTimeoutSeconds.GetAsDuration(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	log := log.Ctx(ctx).With(
		zap.Int64("taskID", t.GetTaskId()),
		zap.Int64("collectionID", t.GetCollectionId()),
		zap.Int64("nodeID", nodeID),
	)

	var err error
	defer func() {
		if err != nil {
			log.Warn("failed to create refresh task on worker", zap.Error(err))
			if updateErr := t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, err.Error()); updateErr != nil {
				log.Warn("failed to persist Failed state after create error", zap.Error(updateErr))
			}
		}
	}()

	log.Info("creating refresh task on worker")

	if t.mt == nil {
		err = fmt.Errorf("meta is nil, cannot create task on worker")
		return
	}

	// Persist task version and nodeID before dispatching to worker
	if err = t.refreshMeta.UpdateTaskVersion(t.GetTaskId(), nodeID); err != nil {
		log.Warn("failed to update task version", zap.Error(err))
		return
	}

	// Re-read task from meta to sync in-memory state (nodeID and version)
	updatedTask := t.refreshMeta.GetTask(t.GetTaskId())
	if updatedTask == nil {
		err = fmt.Errorf("task %d not found after version update", t.GetTaskId())
		return
	}
	t.ExternalCollectionRefreshTask = updatedTask

	// Get current segments for the collection
	segments := t.mt.SelectSegments(ctx, CollectionFilter(t.GetCollectionId()))

	currentSegments := make([]*datapb.SegmentInfo, 0, len(segments))
	for _, seg := range segments {
		currentSegments = append(currentSegments, seg.SegmentInfo)
	}

	log.Info("collected current segments", zap.Int("segmentCount", len(currentSegments)))

	// Pre-allocate segment IDs for data mapping
	preAllocCount := paramtable.Get().DataCoordCfg.ExternalCollectionPreAllocSegments.GetAsInt64()

	idBegin, idEnd, err := t.allocator.AllocN(preAllocCount)
	if err != nil {
		log.Warn("failed to batch allocate segment IDs", zap.Error(err))
		return
	}

	idRange := &datapb.IDRange{
		Begin: idBegin,
		End:   idEnd,
	}

	log.Info("Pre-allocated segment IDs for external task",
		zap.Int64("idBegin", idBegin),
		zap.Int64("idEnd", idEnd),
		zap.Int64("count", idEnd-idBegin))

	// Get collection schema for column mapping
	collInfo := t.mt.GetCollection(t.GetCollectionId())
	if collInfo == nil {
		err = fmt.Errorf("collection %d not found in meta", t.GetCollectionId())
		return
	}

	req := &datapb.RefreshExternalCollectionTaskRequest{
		CollectionID:           t.GetCollectionId(),
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
		log.Warn("failed to create refresh task on worker", zap.Error(err))
		return
	}

	// Mark task as in progress - QueryTaskOnWorker will check completion
	if err = t.UpdateStateWithMeta(indexpb.JobState_JobStateInProgress, ""); err != nil {
		log.Warn("failed to update task state to InProgress", zap.Error(err))
		return
	}

	log.Info("refresh task submitted successfully")
}

func (t *refreshExternalCollectionTask) QueryTaskOnWorker(cluster session.Cluster) {
	timeout := paramtable.Get().DataCoordCfg.RequestTimeoutSeconds.GetAsDuration(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	log := log.Ctx(ctx).With(
		zap.Int64("taskID", t.GetTaskId()),
		zap.Int64("collectionID", t.GetCollectionId()),
		zap.Int64("nodeID", t.GetNodeId()),
	)

	// Check if job has been canceled/superseded before querying worker
	job := t.refreshMeta.GetJob(t.GetJobId())
	if job == nil {
		log.Info("job not found, task has been canceled")
		// Best-effort cleanup: try to drop task on worker if it was assigned
		if t.GetNodeId() != 0 {
			_ = cluster.DropRefreshExternalCollectionTask(t.GetNodeId(), t.GetTaskId())
		}
		if err := t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, "job canceled"); err != nil {
			log.Warn("failed to persist Failed state after job cancellation", zap.Error(err))
		}
		return
	}
	if job.GetState() == indexpb.JobState_JobStateFailed {
		log.Info("job has been marked as failed, canceling task",
			zap.String("jobFailReason", job.GetFailReason()))
		// Best-effort cleanup: try to drop task on worker if it was assigned
		if t.GetNodeId() != 0 {
			_ = cluster.DropRefreshExternalCollectionTask(t.GetNodeId(), t.GetTaskId())
		}
		if err := t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, "job canceled: "+job.GetFailReason()); err != nil {
			log.Warn("failed to persist Failed state after job cancellation", zap.Error(err))
		}
		return
	}

	// Query task status from worker
	resp, err := cluster.QueryRefreshExternalCollectionTask(t.GetNodeId(), t.GetTaskId())
	if err != nil {
		log.Warn("query refresh task result failed", zap.Error(err))
		// If query fails, mark task as failed
		if updateErr := t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, fmt.Sprintf("query task failed: %v", err)); updateErr != nil {
			log.Warn("failed to persist Failed state after query error", zap.Error(updateErr))
		}
		return
	}

	state := resp.GetState()
	failReason := resp.GetFailReason()

	log.Info("queried refresh task status",
		zap.String("state", state.String()),
		zap.String("failReason", failReason))

	// Handle different task states
	switch state {
	case indexpb.JobState_JobStateFinished:
		// Validate source before processing - check if task has been superseded
		if err := t.validateSource(); err != nil {
			log.Warn("task validation failed, task has been superseded", zap.Error(err))
			t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, err.Error())
			return
		}

		// Process the response and update segment info
		if err := t.SetJobInfo(ctx, resp); err != nil {
			log.Warn("failed to process job info", zap.Error(err))
			t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, fmt.Sprintf("failed to process job info: %v", err))
			return
		}

		// Task completed successfully
		if err := t.UpdateStateWithMeta(state, ""); err != nil {
			log.Warn("failed to update task state to Finished", zap.Error(err))
			return
		}
		log.Info("refresh task completed successfully")

	case indexpb.JobState_JobStateFailed:
		// Task failed
		if err := t.UpdateStateWithMeta(state, failReason); err != nil {
			log.Warn("failed to update task state to Failed", zap.Error(err))
			return
		}
		log.Warn("refresh task failed", zap.String("reason", failReason))

	case indexpb.JobState_JobStateInProgress, indexpb.JobState_JobStateNone, indexpb.JobState_JobStateInit:
		// Task still in progress or not yet picked up by scheduler, no action needed
		log.Info("refresh task still in progress",
			zap.String("state", state.String()))

	case indexpb.JobState_JobStateRetry:
		// Task needs retry - mark as failed
		log.Warn("refresh task in unexpected state, marking as failed",
			zap.String("state", state.String()))
		if err := t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, fmt.Sprintf("task in unexpected state: %s", state.String())); err != nil {
			log.Warn("failed to persist Failed state for retry branch", zap.Error(err))
		}

	default:
		log.Warn("refresh task in unknown state",
			zap.String("state", state.String()))
	}
}

func (t *refreshExternalCollectionTask) DropTaskOnWorker(cluster session.Cluster) {
	timeout := paramtable.Get().DataCoordCfg.RequestTimeoutSeconds.GetAsDuration(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	log := log.Ctx(ctx).With(
		zap.Int64("taskID", t.GetTaskId()),
		zap.Int64("collectionID", t.GetCollectionId()),
		zap.Int64("nodeID", t.GetNodeId()),
	)

	// Drop task on worker to cancel execution and clean up resources
	err := cluster.DropRefreshExternalCollectionTask(t.GetNodeId(), t.GetTaskId())
	if err != nil {
		log.Warn("failed to drop refresh task on worker", zap.Error(err))
		return
	}

	log.Info("refresh task dropped successfully")
}
