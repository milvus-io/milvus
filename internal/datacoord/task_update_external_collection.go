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
)

type updateExternalCollectionTask struct {
	*indexpb.UpdateExternalCollectionTask

	times *taskcommon.Times

	meta      *meta
	allocator allocator.Allocator
}

var _ globalTask.Task = (*updateExternalCollectionTask)(nil)

func newUpdateExternalCollectionTask(t *indexpb.UpdateExternalCollectionTask, mt *meta, alloc allocator.Allocator) *updateExternalCollectionTask {
	return &updateExternalCollectionTask{
		UpdateExternalCollectionTask: t,
		times:                        taskcommon.NewTimes(),
		meta:                         mt,
		allocator:                    alloc,
	}
}

func (t *updateExternalCollectionTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *updateExternalCollectionTask) GetTaskType() taskcommon.Type {
	// Reuse Stats type for now, or we could add a new type
	return taskcommon.Stats
}

func (t *updateExternalCollectionTask) GetTaskState() taskcommon.State {
	return t.GetState()
}

func (t *updateExternalCollectionTask) GetTaskSlot() int64 {
	// External collection tasks are lightweight, use 1 slot
	return 1
}

func (t *updateExternalCollectionTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	t.times.SetTaskTime(timeType, time)
}

func (t *updateExternalCollectionTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(t.times)
}

func (t *updateExternalCollectionTask) GetTaskVersion() int64 {
	return t.GetVersion()
}

// validateSource checks if this task's external source matches the current collection source
// Returns error if task has been superseded
func (t *updateExternalCollectionTask) validateSource() error {
	collection := t.meta.GetCollection(t.GetCollectionID())
	if collection == nil {
		return fmt.Errorf("collection %d not found", t.GetCollectionID())
	}

	currentSource := collection.Schema.GetExternalSource()
	currentSpec := collection.Schema.GetExternalSpec()

	taskSource := t.GetExternalSource()
	taskSpec := t.GetExternalSpec()

	if currentSource != taskSource || currentSpec != taskSpec {
		return fmt.Errorf("task source mismatch: task source=%s/%s, current source=%s/%s (task has been superseded)",
			taskSource, taskSpec, currentSource, currentSpec)
	}

	return nil
}

func (t *updateExternalCollectionTask) SetState(state indexpb.JobState, failReason string) {
	// If transitioning to finished state, validate source first
	if state == indexpb.JobState_JobStateFinished {
		if err := t.validateSource(); err != nil {
			log.Warn("Task source validation failed, marking as failed instead",
				zap.Int64("taskID", t.GetTaskID()),
				zap.Int64("collectionID", t.GetCollectionID()),
				zap.Error(err))
			t.State = indexpb.JobState_JobStateFailed
			t.FailReason = fmt.Sprintf("source mismatch: %s", err.Error())
			return
		}
	}

	t.State = state
	t.FailReason = failReason
}

func (t *updateExternalCollectionTask) UpdateStateWithMeta(state indexpb.JobState, failReason string) error {
	if err := t.meta.externalCollectionTaskMeta.UpdateTaskState(t.GetTaskID(), state, failReason); err != nil {
		log.Warn("update external collection task state failed",
			zap.Int64("taskID", t.GetTaskID()),
			zap.String("state", state.String()),
			zap.String("failReason", failReason),
			zap.Error(err))
		return err
	}
	t.SetState(state, failReason)
	return nil
}

// SetJobInfo processes the task response and updates segment information atomically
func (t *updateExternalCollectionTask) SetJobInfo(ctx context.Context, resp *datapb.UpdateExternalCollectionResponse) error {
	log := log.Ctx(ctx).With(
		zap.Int64("taskID", t.GetTaskID()),
		zap.Int64("collectionID", t.GetCollectionID()),
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

	// Allocate new IDs and update updatedSegments directly
	for _, seg := range updatedSegments {
		newSegmentID, err := t.allocator.AllocID(ctx)
		if err != nil {
			log.Warn("failed to allocate segment ID", zap.Error(err))
			return err
		}
		log.Info("allocated new segment ID",
			zap.Int64("oldID", seg.GetID()),
			zap.Int64("newID", newSegmentID))
		seg.ID = newSegmentID
		seg.State = commonpb.SegmentState_Flushed
	}

	// Build update operators
	var operators []UpdateOperator

	// Operator 1: Drop segments not in kept list
	dropOperator := func(modPack *updateSegmentPack) bool {
		currentSegments := modPack.meta.segments.GetSegments()
		for _, seg := range currentSegments {
			// Skip segments not in this collection
			if seg.GetCollectionID() != t.GetCollectionID() {
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
	if err := t.meta.UpdateSegmentsInfo(ctx, operators...); err != nil {
		log.Warn("failed to update segments atomically", zap.Error(err))
		return err
	}

	log.Info("external collection segments updated successfully",
		zap.Int("updatedSegments", len(updatedSegments)),
		zap.Int("keptSegments", len(keptSegmentIDs)))

	return nil
}

func (t *updateExternalCollectionTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	ctx := context.Background()
	log := log.Ctx(ctx).With(
		zap.Int64("taskID", t.GetTaskID()),
		zap.Int64("collectionID", t.GetCollectionID()),
		zap.Int64("nodeID", nodeID),
	)

	var err error
	defer func() {
		if err != nil {
			log.Warn("failed to create external collection update task on worker", zap.Error(err))
			t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, err.Error())
		}
	}()

	log.Info("creating external collection update task on worker")

	// Set node ID for this task
	t.NodeID = nodeID

	// Get current segments for the collection
	segments := t.meta.SelectSegments(ctx, CollectionFilter(t.GetCollectionID()))

	currentSegments := make([]*datapb.SegmentInfo, 0, len(segments))
	for _, seg := range segments {
		currentSegments = append(currentSegments, seg.SegmentInfo)
	}

	log.Info("collected current segments", zap.Int("segmentCount", len(currentSegments)))

	// Build request
	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID:    t.GetCollectionID(),
		TaskID:          t.GetTaskID(),
		CurrentSegments: currentSegments,
		ExternalSource:  t.GetExternalSource(),
		ExternalSpec:    t.GetExternalSpec(),
	}

	// Submit task to worker via unified task system
	// Task will execute asynchronously in worker's goroutine pool
	err = cluster.CreateExternalCollectionTask(nodeID, req)
	if err != nil {
		log.Warn("failed to create external collection task on worker", zap.Error(err))
		return
	}

	// Mark task as in progress - QueryTaskOnWorker will check completion
	if err = t.UpdateStateWithMeta(indexpb.JobState_JobStateInProgress, ""); err != nil {
		log.Warn("failed to update task state to InProgress", zap.Error(err))
		return
	}

	log.Info("external collection update task submitted successfully")
}

func (t *updateExternalCollectionTask) QueryTaskOnWorker(cluster session.Cluster) {
	ctx := context.Background()
	log := log.Ctx(ctx).With(
		zap.Int64("taskID", t.GetTaskID()),
		zap.Int64("collectionID", t.GetCollectionID()),
		zap.Int64("nodeID", t.GetNodeID()),
	)

	// Query task status from worker
	resp, err := cluster.QueryExternalCollectionTask(t.GetNodeID(), t.GetTaskID())
	if err != nil {
		log.Warn("query external collection task result failed", zap.Error(err))
		// If query fails, mark task as failed
		t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, fmt.Sprintf("query task failed: %v", err))
		return
	}

	state := resp.GetState()
	failReason := resp.GetFailReason()

	log.Info("queried external collection task status",
		zap.String("state", state.String()),
		zap.String("failReason", failReason))

	// Handle different task states
	switch state {
	case indexpb.JobState_JobStateFinished:
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
		log.Info("external collection task completed successfully")

	case indexpb.JobState_JobStateFailed:
		// Task failed
		if err := t.UpdateStateWithMeta(state, failReason); err != nil {
			log.Warn("failed to update task state to Failed", zap.Error(err))
			return
		}
		log.Warn("external collection task failed", zap.String("reason", failReason))

	case indexpb.JobState_JobStateInProgress:
		// Task still in progress, no action needed
		log.Info("external collection task still in progress")

	case indexpb.JobState_JobStateNone, indexpb.JobState_JobStateRetry:
		// Task not found or needs retry - mark as failed
		log.Warn("external collection task in unexpected state, marking as failed",
			zap.String("state", state.String()))
		t.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, fmt.Sprintf("task in unexpected state: %s", state.String()))

	default:
		log.Warn("external collection task in unknown state",
			zap.String("state", state.String()))
	}
}

func (t *updateExternalCollectionTask) DropTaskOnWorker(cluster session.Cluster) {
	ctx := context.Background()
	log := log.Ctx(ctx).With(
		zap.Int64("taskID", t.GetTaskID()),
		zap.Int64("collectionID", t.GetCollectionID()),
		zap.Int64("nodeID", t.GetNodeID()),
	)

	// Drop task on worker to cancel execution and clean up resources
	err := cluster.DropExternalCollectionTask(t.GetNodeID(), t.GetTaskID())
	if err != nil {
		log.Warn("failed to drop external collection task on worker", zap.Error(err))
		return
	}

	log.Info("external collection task dropped successfully")
}
