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
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"golang.org/x/exp/slices"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type analyzeTask struct {
	*indexpb.AnalyzeTask

	// stateGuard makes the fields below readable by the scheduler without the
	// per-task key lock; see statsTask.stateGuard.
	stateGuard sync.RWMutex

	times *taskcommon.Times

	meta    *meta
	handler Handler
}

var _ globalTask.Task = (*analyzeTask)(nil)

func newAnalyzeTask(t *indexpb.AnalyzeTask, meta *meta, handler Handler) *analyzeTask {
	return &analyzeTask{
		AnalyzeTask: t,
		times:       taskcommon.NewTimes(),
		meta:        meta,
		handler:     handler,
	}
}

func (at *analyzeTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	at.times.SetTaskTime(timeType, time)
}

func (at *analyzeTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(at.times)
}

func (at *analyzeTask) GetTaskVersion() int64 {
	// Analyze retries replace the whole compaction plan. Version remains a
	// persisted result-path marker and is not an attempt counter.
	return 0
}

func (at *analyzeTask) GetTaskType() taskcommon.Type {
	return taskcommon.Analyze
}

func (at *analyzeTask) GetTaskState() taskcommon.State {
	at.stateGuard.RLock()
	defer at.stateGuard.RUnlock()
	return at.State
}

func (at *analyzeTask) GetTaskSlot() int64 {
	return Params.DataCoordCfg.AnalyzeTaskSlotUsage.GetAsInt64()
}

func (at *analyzeTask) SetState(state indexpb.JobState, failReason string) {
	at.stateGuard.Lock()
	defer at.stateGuard.Unlock()
	at.State = state
	at.FailReason = failReason
}

func (at *analyzeTask) UpdateStateWithMeta(state indexpb.JobState, failReason string) error {
	if err := at.meta.analyzeMeta.UpdateState(at.GetTaskID(), state, failReason); err != nil {
		return err
	}
	at.SetState(state, failReason)
	return nil
}

func (at *analyzeTask) assignTask(nodeID int64) error {
	if err := at.meta.analyzeMeta.AssignTask(at.GetTaskID(), nodeID); err != nil {
		return err
	}
	at.stateGuard.Lock()
	at.Version = 1
	at.NodeID = nodeID
	at.State = indexpb.JobState_JobStateInProgress
	at.FailReason = ""
	at.stateGuard.Unlock()
	return nil
}

func (at *analyzeTask) setJobInfo(result *workerpb.AnalyzeResult) error {
	if err := at.meta.analyzeMeta.FinishTask(at.GetTaskID(), result); err != nil {
		return err
	}
	at.SetState(result.GetState(), result.GetFailReason())
	return nil
}

func (at *analyzeTask) retryTask(reason string) {
	if err := at.UpdateStateWithMeta(indexpb.JobState_JobStateRetry, reason); err != nil {
		// Release the scheduler-owned wrapper even if the catalog is unavailable.
		// The analyze inspector retries the authoritative record on its interval.
		at.SetState(indexpb.JobState_JobStateRetry, reason)
	}
}

func (at *analyzeTask) dropAndRetryTaskOnWorker(cluster session.Cluster, reason string) {
	// Drop is best effort. A retry ends this clustering-compaction attempt;
	// its existing replan path creates a fresh planID and AnalyzeTaskID, so the
	// old worker has no result-commit path even if cancellation is delayed.
	_ = at.tryDropTaskOnWorker(cluster)
	at.retryTask(reason)
}

func (at *analyzeTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	ctx := context.TODO()
	log := mlog.With(mlog.FieldTaskID(at.GetTaskID()))

	// Check if task still exists in meta
	task := at.meta.analyzeMeta.GetTask(at.GetTaskID())
	if task == nil {
		log.Info(ctx, "analyze task has not exist in meta table, remove task")
		at.SetState(indexpb.JobState_JobStateNone, "analyze task has not exist in meta table")
		return
	}
	if at.handler == nil {
		if err := at.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, "collection getter is not configured"); err != nil {
			log.Warn(ctx, "failed to persist analyze task configuration failure", mlog.Err(err))
		}
		return
	}
	coll, err := at.handler.GetCollection(ctx, task.CollectionID)
	if err != nil {
		if errors.Is(err, merr.ErrCollectionNotFound) {
			if updateErr := at.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, err.Error()); updateErr != nil {
				log.Warn(ctx, "failed to persist missing collection failure", mlog.Err(updateErr))
			}
			return
		}
		// Keep Init unchanged. The scheduler releases this wrapper and the
		// inspector re-enqueues the persisted task on its next interval.
		log.Warn(ctx, "failed to get collection for analyze dispatch; retry later", mlog.Err(err))
		return
	}
	if coll == nil || coll.Schema == nil {
		if updateErr := at.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, "collection schema is unavailable"); updateErr != nil {
			log.Warn(ctx, "failed to persist unavailable collection schema", mlog.Err(updateErr))
		}
		return
	}
	schema := coll.Schema

	req := &workerpb.AnalyzeRequest{
		ClusterID:    Params.CommonCfg.ClusterPrefix.GetValue(),
		TaskID:       at.GetTaskID(),
		CollectionID: task.CollectionID,
		PartitionID:  task.PartitionID,
		FieldID:      task.FieldID,
		FieldName:    task.FieldName,
		FieldType:    task.FieldType,
		Dim:          task.Dim,
		SegmentStats: make(map[int64]*indexpb.SegmentStats),
		// Version is retained on the wire for old workers and as the legacy
		// analyze-result path/completion marker. It is constant per fresh task
		// identity and is no longer an attempt fence.
		Version:       1,
		StorageConfig: createStorageConfig(),
	}
	// Populate SegmentStats with binlog IDs and row counts from segment metadata.
	segments := at.meta.SelectSegments(ctx, SegmentFilterFunc(func(info *SegmentInfo) bool {
		return isSegmentHealthy(info) && slices.Contains(task.SegmentIDs, info.ID)
	}))
	segmentsMap := lo.SliceToMap(segments, func(t *SegmentInfo) (int64, *SegmentInfo) {
		return t.ID, t
	})

	totalSegmentsRows := int64(0)
	for _, segID := range task.SegmentIDs {
		info := segmentsMap[segID]
		if info == nil {
			log.Warn(ctx, "analyze task is processing, but segment is nil, fail the task",
				mlog.FieldSegmentID(segID))
			if err := at.UpdateStateWithMeta(indexpb.JobState_JobStateFailed,
				fmt.Sprintf("segmentInfo with ID: %d is nil", segID)); err != nil {
				// State is left untouched, so the scheduler re-enqueues the task and retries.
				log.Warn(ctx, "failed to persist the failed state of the analyze task", mlog.Err(err))
			}
			return
		}
		totalSegmentsRows += info.GetNumOfRows()
		stats := &indexpb.SegmentStats{
			ID:      segID,
			NumRows: info.GetNumOfRows(),
		}
		// StorageV3 segments are read via manifest; StorageV2 segments use logIDs.
		// Exactly one representation is sent for each segment.
		if manifest := info.GetManifestPath(); manifest != "" {
			stats.ManifestPath = manifest
		} else {
			stats.LogIDs = getBinLogIDs(info, task.FieldID)
		}
		req.SegmentStats[segID] = stats
	}

	// Extract dim from schema field TypeParams for vector clustering key.
	if schema != nil {
		for _, f := range schema.Fields {
			if f.FieldID == task.FieldID {
				dim, err := storage.GetDimFromParams(f.TypeParams)
				if err != nil {
					if updateErr := at.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, err.Error()); updateErr != nil {
						log.Warn(ctx, "failed to persist invalid analyze dimension", mlog.Err(updateErr))
					}
					return
				}
				req.Dim = int64(dim)

				// Calculate the number of clusters based on total data size.
				totalSegmentsRawDataSize := float64(totalSegmentsRows) * float64(dim) * typeutil.VectorTypeSize(task.FieldType)
				numClusters := int64(math.Ceil(totalSegmentsRawDataSize / (Params.DataCoordCfg.SegmentMaxSize.GetAsFloat() * 1024 * 1024 * Params.DataCoordCfg.ClusteringCompactionMaxSegmentSizeRatio.GetAsFloat())))
				if numClusters < Params.DataCoordCfg.ClusteringCompactionMinCentroidsNum.GetAsInt64() {
					log.Info(ctx, "data size is too small, skip analyze task",
						mlog.Float64("raw data size", totalSegmentsRawDataSize),
						mlog.Int64("num clusters", numClusters),
						mlog.Int64("minimum num clusters required", Params.DataCoordCfg.ClusteringCompactionMinCentroidsNum.GetAsInt64()))
					if err := at.UpdateStateWithMeta(indexpb.JobState_JobStateFinished, ""); err != nil {
						// State is left untouched, so the scheduler re-enqueues the task and retries.
						log.Warn(ctx, "failed to persist the finished state of the analyze task", mlog.Err(err))
					}
					return
				}
				if numClusters > Params.DataCoordCfg.ClusteringCompactionMaxCentroidsNum.GetAsInt64() {
					numClusters = Params.DataCoordCfg.ClusteringCompactionMaxCentroidsNum.GetAsInt64()
				}
				req.NumClusters = numClusters
				break
			}
		}
	}

	req.MaxTrainSizeRatio = Params.DataCoordCfg.ClusteringCompactionMaxTrainSizeRatio.GetAsFloat()
	req.MinClusterSizeRatio = Params.DataCoordCfg.ClusteringCompactionMinClusterSizeRatio.GetAsFloat()
	req.MaxClusterSizeRatio = Params.DataCoordCfg.ClusteringCompactionMaxClusterSizeRatio.GetAsFloat()
	req.MaxClusterSize = Params.DataCoordCfg.ClusteringCompactionMaxClusterSize.GetAsSize()
	req.TaskSlot = Params.DataCoordCfg.AnalyzeTaskSlotUsage.GetAsInt64()

	WrapPluginContext(task.CollectionID, schema.GetProperties(), req)

	// Persist the worker assignment before Create. An error response is
	// ambiguous, so fail-stop and let restart recover the authoritative Init or
	// InProgress record before any Create is retried.
	if err := at.assignTask(nodeID); err != nil {
		if at.meta.ctx == nil || at.meta.ctx.Err() == nil {
			mlog.Fatal(ctx, "failed to persist analyze task assignment; terminating process",
				mlog.FieldTaskID(at.GetTaskID()), mlog.FieldNodeID(nodeID), mlog.Err(err))
		}
		log.Warn(ctx, "failed to persist analyze task assignment", mlog.Err(err))
		return
	}

	err = cluster.CreateAnalyze(nodeID, req)
	if err != nil {
		log.Warn(ctx, "assign analyze task to worker failed", mlog.Err(err))
		at.dropAndRetryTaskOnWorker(cluster, err.Error())
		return
	}

	log.Info(ctx, "analyze task assigned successfully")
}

func (at *analyzeTask) QueryTaskOnWorker(cluster session.Cluster) {
	log := mlog.With(
		mlog.FieldTaskID(at.GetTaskID()),
		mlog.FieldNodeID(at.NodeID),
	)

	resp, err := cluster.QueryAnalyze(at.NodeID, &workerpb.QueryJobsRequest{
		ClusterID: Params.CommonCfg.ClusterPrefix.GetValue(),
		TaskIDs:   []int64{at.GetTaskID()},
	})
	if err != nil {
		log.Warn(context.TODO(), "query analyze task result from worker failed", mlog.Err(err))
		at.dropAndRetryTaskOnWorker(cluster, err.Error())
		return
	}

	// Process query results
	for _, result := range resp.GetResults() {
		if result.GetTaskID() != at.GetTaskID() {
			continue
		}

		state := result.GetState()
		// Handle different task states
		switch state {
		case indexpb.JobState_JobStateFinished, indexpb.JobState_JobStateFailed:
			log.Info(context.TODO(), "query analyze task result success",
				mlog.String("state", state.String()),
				mlog.String("failReason", result.GetFailReason()))
			if err := at.setJobInfo(result); err != nil {
				log.Warn(context.TODO(), "failed to persist analyze task result", mlog.Err(err))
				// Keep the assigned attempt InProgress locally. The worker retains
				// this terminal result, so the next poll retries only the metadata
				// write instead of running analysis again.
			}
		case indexpb.JobState_JobStateRetry, indexpb.JobState_JobStateNone:
			log.Info(context.TODO(), "query analyze task result success",
				mlog.String("state", state.String()),
				mlog.String("failReason", result.GetFailReason()))
			at.dropAndRetryTaskOnWorker(cluster, result.GetFailReason())
		}
		// Otherwise (inProgress or unissued/init), keep current state
		return
	}

	log.Warn(context.TODO(), "query analyze task info failed, worker does not have task info")
	at.dropAndRetryTaskOnWorker(cluster, "analyze result is not in info response")
}

func (at *analyzeTask) tryDropTaskOnWorker(cluster session.Cluster) error {
	log := mlog.With(
		mlog.FieldTaskID(at.GetTaskID()),
		mlog.FieldNodeID(at.NodeID),
	)
	if at.NodeID <= 0 {
		return nil
	}

	if err := cluster.DropAnalyze(at.NodeID, at.GetTaskID()); err != nil && !errors.Is(err, merr.ErrNodeNotFound) {
		log.Warn(context.TODO(), "failed to drop analyze task on worker", mlog.Err(err))
		return err
	}

	log.Info(context.TODO(), "dropped analyze task on worker successfully")
	return nil
}

func (at *analyzeTask) DropTaskOnWorker(cluster session.Cluster) {
	at.tryDropTaskOnWorker(cluster)
}
