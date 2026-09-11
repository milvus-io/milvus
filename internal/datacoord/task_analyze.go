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
	"time"

	"github.com/samber/lo"
	"golang.org/x/exp/slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type analyzeTask struct {
	*indexpb.AnalyzeTask

	times *taskcommon.Times

	schema *schemapb.CollectionSchema
	meta   *meta

	pricing requirementCache
}

// TaskResources prices this analyze task before a node is picked for it.
//
// EstimateAnalyze is a BOUND on what the task allocates rather than a model of
// what it needs: TrainSize is whole-node memory x the ratio regardless of how
// much data there is, so the charge is min(dataset, that). The dataset is
// recomputed from Dim x rows because AnalyzeTask carries row counts, not bytes.
func (at *analyzeTask) TaskResources() *datapb.TaskResources {
	return at.pricing.get(func() (taskresource.Requirement, bool) {
		var totalRows int64
		complete := true
		for _, segID := range at.GetSegmentIDs() {
			segment := at.meta.GetHealthySegment(context.TODO(), segID)
			if segment == nil {
				complete = false
				continue
			}
			totalRows += segment.GetNumOfRows()
		}

		var dataType schemapb.DataType
		for _, f := range typeutil.GetAllFieldSchemas(at.schema) {
			if f.GetFieldID() == at.GetFieldID() {
				dataType = f.GetDataType()
				break
			}
		}

		req := taskresource.EstimateAnalyze(
			taskresource.VectorFieldByteSize(dataType, at.GetDim(), totalRows),
			Params.DataCoordCfg.ClusteringCompactionMaxTrainSizeRatio.GetAsFloat(),
		)
		return req, complete
	}).ToProto()
}

var _ globalTask.Task = (*analyzeTask)(nil)

func newAnalyzeTask(t *indexpb.AnalyzeTask, meta *meta) *analyzeTask {
	task := &analyzeTask{
		AnalyzeTask: t,
		times:       taskcommon.NewTimes(),
		meta:        meta,
	}
	coll := meta.GetCollection(t.CollectionID)
	if coll != nil {
		task.schema = coll.Schema
	}

	return task
}

func (at *analyzeTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	at.times.SetTaskTime(timeType, time)
}

func (at *analyzeTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(at.times)
}

func (at *analyzeTask) GetTaskVersion() int64 {
	return at.GetVersion()
}

func (at *analyzeTask) GetTaskType() taskcommon.Type {
	return taskcommon.Analyze
}

func (at *analyzeTask) GetTaskState() taskcommon.State {
	return at.State
}

func (at *analyzeTask) GetTaskSlot() int64 {
	return Params.DataCoordCfg.AnalyzeTaskSlotUsage.GetAsInt64()
}

func (at *analyzeTask) SetState(state indexpb.JobState, failReason string) {
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

func (at *analyzeTask) UpdateVersion(nodeID int64) error {
	if err := at.meta.analyzeMeta.UpdateVersion(at.GetTaskID(), nodeID); err != nil {
		return err
	}
	at.Version++
	at.NodeID = nodeID
	return nil
}

func (at *analyzeTask) setJobInfo(result *workerpb.AnalyzeResult) error {
	if err := at.meta.analyzeMeta.FinishTask(at.GetTaskID(), result); err != nil {
		return err
	}
	at.SetState(result.GetState(), result.GetFailReason())
	return nil
}

func (at *analyzeTask) resetTask(reason string) {
	at.UpdateStateWithMeta(indexpb.JobState_JobStateInit, reason)
}

func (at *analyzeTask) dropAndResetTaskOnWorker(cluster session.Cluster, reason string) {
	if err := at.tryDropTaskOnWorker(cluster); err != nil {
		return
	}
	at.resetTask(reason)
}

func (at *analyzeTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	ctx := context.TODO()
	log := mlog.With(mlog.FieldTaskID(at.GetTaskID()))

	// Check if task still exists in meta
	task := at.meta.analyzeMeta.GetTask(at.GetTaskID())
	if task == nil {
		log.Info(context.TODO(), "analyze task has not exist in meta table, remove task")
		at.SetState(indexpb.JobState_JobStateNone, "analyze task has not exist in meta table")
		return
	}

	// Update task version
	if err := at.UpdateVersion(nodeID); err != nil {
		log.Warn(context.TODO(), "failed to update task version", mlog.Err(err))
		return
	}
	req := &workerpb.AnalyzeRequest{
		ClusterID:     Params.CommonCfg.ClusterPrefix.GetValue(),
		TaskID:        at.GetTaskID(),
		CollectionID:  task.CollectionID,
		PartitionID:   task.PartitionID,
		FieldID:       task.FieldID,
		FieldName:     task.FieldName,
		FieldType:     task.FieldType,
		Dim:           task.Dim,
		SegmentStats:  make(map[int64]*indexpb.SegmentStats),
		Version:       task.Version + 1,
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
			log.Warn(context.TODO(), "analyze task is processing, but segment is nil, fail the task",
				mlog.FieldSegmentID(segID))
			if err := at.UpdateStateWithMeta(indexpb.JobState_JobStateFailed,
				fmt.Sprintf("segmentInfo with ID: %d is nil", segID)); err != nil {
				// State is left untouched, so the scheduler re-enqueues the task and retries.
				log.Warn(context.TODO(), "failed to persist the failed state of the analyze task", mlog.Err(err))
			}
			return
		}
		totalSegmentsRows += info.GetNumOfRows()
		stats := &indexpb.SegmentStats{
			ID:      segID,
			NumRows: info.GetNumOfRows(),
		}
		// StorageV3 segments are read via manifest; V1 via logIDs. Exactly one.
		if manifest := info.GetManifestPath(); manifest != "" {
			stats.ManifestPath = manifest
		} else {
			stats.LogIDs = getBinLogIDs(info, task.FieldID)
		}
		req.SegmentStats[segID] = stats
	}

	// Extract dim from schema field TypeParams for vector clustering key.
	if at.schema != nil {
		for _, f := range at.schema.Fields {
			if f.FieldID == task.FieldID {
				dim, err := storage.GetDimFromParams(f.TypeParams)
				if err != nil {
					at.SetState(indexpb.JobState_JobStateInit, err.Error())
					return
				}
				req.Dim = int64(dim)

				// Calculate the number of clusters based on total data size.
				totalSegmentsRawDataSize := float64(totalSegmentsRows) * float64(dim) * typeutil.VectorTypeSize(task.FieldType)
				numClusters := int64(math.Ceil(totalSegmentsRawDataSize / (Params.DataCoordCfg.SegmentMaxSize.GetAsFloat() * 1024 * 1024 * Params.DataCoordCfg.ClusteringCompactionMaxSegmentSizeRatio.GetAsFloat())))
				if numClusters < Params.DataCoordCfg.ClusteringCompactionMinCentroidsNum.GetAsInt64() {
					log.Info(context.TODO(), "data size is too small, skip analyze task",
						mlog.Float64("raw data size", totalSegmentsRawDataSize),
						mlog.Int64("num clusters", numClusters),
						mlog.Int64("minimum num clusters required", Params.DataCoordCfg.ClusteringCompactionMinCentroidsNum.GetAsInt64()))
					if err := at.UpdateStateWithMeta(indexpb.JobState_JobStateFinished, ""); err != nil {
						// State is left untouched, so the scheduler re-enqueues the task and retries.
						log.Warn(context.TODO(), "failed to persist the finished state of the analyze task", mlog.Err(err))
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
	// The same figure the scheduler placed this task on.
	req.TaskResources = at.pricing.fill(taskresource.EstimateAnalyze(
		taskresource.VectorFieldByteSize(task.FieldType, task.Dim, totalSegmentsRows),
		req.GetMaxTrainSizeRatio(),
	)).ToProto()

	WrapPluginContext(task.CollectionID, at.schema.GetProperties(), req)

	var err error
	defer func() {
		if err != nil {
			log.Warn(context.TODO(), "assign analyze task to worker failed, try drop task on worker", mlog.Err(err))
			at.tryDropTaskOnWorker(cluster)
		}
	}()

	err = cluster.CreateAnalyze(nodeID, req)
	if err != nil {
		log.Warn(context.TODO(), "assign analyze task to worker failed", mlog.Err(err))
		return
	}

	log.Info(context.TODO(), "analyze task assigned successfully")
	if err = at.UpdateStateWithMeta(indexpb.JobState_JobStateInProgress, ""); err != nil {
		log.Warn(context.TODO(), "failed to update task state to inProgress", mlog.Err(err))
	}
	log.Info(context.TODO(), "update task state to inProgress successfully")
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
		at.dropAndResetTaskOnWorker(cluster, err.Error())
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
			at.setJobInfo(result)
		case indexpb.JobState_JobStateRetry, indexpb.JobState_JobStateNone:
			log.Info(context.TODO(), "query analyze task result success",
				mlog.String("state", state.String()),
				mlog.String("failReason", result.GetFailReason()))
			at.dropAndResetTaskOnWorker(cluster, result.GetFailReason())
		}
		// Otherwise (inProgress or unissued/init), keep current state
		return
	}

	log.Warn(context.TODO(), "query analyze task info failed, worker does not have task info")
	at.UpdateStateWithMeta(indexpb.JobState_JobStateInit, "analyze result is not in info response")
}

func (at *analyzeTask) tryDropTaskOnWorker(cluster session.Cluster) error {
	log := mlog.With(
		mlog.FieldTaskID(at.GetTaskID()),
		mlog.FieldNodeID(at.NodeID),
	)

	if err := cluster.DropAnalyze(at.NodeID, at.GetTaskID()); err != nil {
		log.Warn(context.TODO(), "failed to drop analyze task on worker", mlog.Err(err))
		return err
	}

	log.Info(context.TODO(), "dropped analyze task on worker successfully")
	return nil
}

func (at *analyzeTask) DropTaskOnWorker(cluster session.Cluster) {
	at.tryDropTaskOnWorker(cluster)
}
