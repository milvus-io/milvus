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
	"strings"
	"time"

	"github.com/samber/lo"
	"golang.org/x/exp/slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
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
	at.UpdateStateWithMeta(indexpb.JobState_JobStateNone, reason)
}

func (at *analyzeTask) dropAndResetTaskOnWorker(cluster session.Cluster, reason string) {
	if err := at.tryDropTaskOnWorker(cluster); err != nil {
		return
	}
	at.resetTask(reason)
}

func (at *analyzeTask) updateAnalyzeInfo(req *workerpb.AnalyzeRequest) error {
	// When data analyze occurs, segments must not be discarded. Such as compaction, GC, etc.
	ctx := context.TODO()
	filters := []SegmentFilter{
		SegmentFilterFunc(func(segment *SegmentInfo) bool {
			return isSegmentHealthy(segment) && slices.Contains(at.SegmentIDs, segment.ID)
		}),
	}
	filters = append(filters, WithCollection(at.CollectionID))
	segments := at.meta.SelectSegments(ctx, filters...)
	segmentsMap := lo.SliceToMap(segments, func(t *SegmentInfo) (int64, *SegmentInfo) {
		return t.ID, t
	})

	totalSegmentsRows := int64(0)
	for _, segID := range at.SegmentIDs {
		info := segmentsMap[segID]
		if info == nil {
			log.Ctx(ctx).Warn("analyze stats task is processing, but segment is nil, delete the task",
				zap.Int64("taskID", at.GetTaskID()), zap.Int64("segmentID", segID))
			at.SetState(indexpb.JobState_JobStateFailed, fmt.Sprintf("segmentInfo with ID: %d is nil", segID))
			return fmt.Errorf("segmentInfo with ID: %d is nil", segID)
		}

		totalSegmentsRows += info.GetNumOfRows()
		// get binlogIDs
		binlogIDs := getBinLogIDs(info, at.FieldID)
		req.SegmentStats[segID] = &indexpb.SegmentStats{
			ID:      segID,
			NumRows: info.GetNumOfRows(),
			LogIDs:  binlogIDs,
		}
		req.InsertFiles[segID] = &workerpb.FieldBinLogs{
			BinLogs: info.GetBinlogs(),
		}
	}

	collInfo := at.meta.GetCollection(segments[0].GetCollectionID())
	if collInfo == nil {
		err := fmt.Errorf("analyze task get collection %d info failed", segments[0].GetCollectionID())
		log.Ctx(ctx).Warn("analyze task get collection info failed", zap.Int64("collectionID",
			segments[0].GetCollectionID()), zap.Error(err))
		at.SetState(indexpb.JobState_JobStateInit, err.Error())
		return err
	}

	schema := collInfo.Schema
	var field *schemapb.FieldSchema

	for _, f := range schema.Fields {
		if f.FieldID == at.FieldID {
			field = f
			break
		}
	}
	dim, err := storage.GetDimFromParams(field.TypeParams)
	if err != nil {
		err := fmt.Errorf("analyze task get collection %d info failed", segments[0].GetCollectionID())
		log.Ctx(ctx).Warn("analyze task get dim failed", zap.Int64("collectionID",
			segments[0].GetCollectionID()), zap.Error(err))
		at.SetState(indexpb.JobState_JobStateInit, err.Error())
		return err
	}
	req.Dim = int64(dim)

	totalSegmentsRawDataSize := float64(totalSegmentsRows) * float64(req.Dim) * typeutil.VectorTypeSize(at.FieldType) // Byte
	requiredSegmentSize := Params.DataCoordCfg.SegmentMaxSize.GetAsFloat() * 1024 * 1024 * Params.DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.GetAsFloat()
	maxSegmentSize := totalSegmentsRawDataSize / Params.DataCoordCfg.ClusteringCompactionMinCentroidsNum.GetAsFloat()
	segmentSize := math.Min(requiredSegmentSize, maxSegmentSize)
	numClusters := int64(math.Ceil(totalSegmentsRawDataSize / segmentSize))
	maxCentroids := Params.DataCoordCfg.ClusteringCompactionMaxCentroidsPerSegment.GetAsInt64()
	numClusters *= maxCentroids
	if segmentSize < 1.0 || numClusters < Params.DataCoordCfg.ClusteringCompactionMinCentroidsNum.GetAsInt64() {
		err := fmt.Errorf("the number of clusters %d is lower than minimum %d", numClusters, Params.DataCoordCfg.ClusteringCompactionMinCentroidsNum.GetAsInt64())
		log.Ctx(ctx).Info("data size is too small, skip analyze task", zap.Float64("raw data size", totalSegmentsRawDataSize), zap.Int64("num clusters", numClusters), zap.Int64("minimum num clusters required", Params.DataCoordCfg.ClusteringCompactionMinCentroidsNum.GetAsInt64()))
		at.SetState(indexpb.JobState_JobStateFinished, "")
		return err
	}
	if numClusters > Params.DataCoordCfg.ClusteringCompactionMaxCentroidsNum.GetAsInt64() {
		numClusters = Params.DataCoordCfg.ClusteringCompactionMaxCentroidsNum.GetAsInt64()
	}
	req.NumClusters = numClusters
	req.MaxTrainSizeRatio = Params.DataCoordCfg.ClusteringCompactionMaxTrainSizeRatio.GetAsFloat() // control clustering train data size
	// config to detect data skewness
	req.MinClusterSizeRatio = Params.DataCoordCfg.ClusteringCompactionMinClusterSizeRatio.GetAsFloat()
	req.MaxClusterSizeRatio = Params.DataCoordCfg.ClusteringCompactionMaxClusterSizeRatio.GetAsFloat()
	req.MaxClusterSize = Params.DataCoordCfg.ClusteringCompactionMaxClusterSize.GetAsSize()
	req.TrainBufferSize = Params.DataCoordCfg.ClusteringCompactionMaxTrainBufferSize.GetAsSize()
	req.AssignBufferSize = Params.DataCoordCfg.ClusteringCompactionMaxAssignBufferSize.GetAsSize()

	taskSlot := Params.DataCoordCfg.AnalyzeTaskSlotUsage.GetAsInt64()
	req.TaskSlot = taskSlot
	req.StorageVersion = segments[0].StorageVersion

	return nil
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
	at.SegmentIDs = task.SegmentIDs
	at.FieldID = task.FieldID
	at.FieldType = task.FieldType
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
		InsertFiles:   make(map[int64]*workerpb.FieldBinLogs),
	}

	WrapPluginContext(task.CollectionID, at.schema.GetProperties(), req)

	var err error
	err = at.updateAnalyzeInfo(req)
	if err != nil {
		at.dropAndResetTaskOnWorker(cluster, err.Error())
		return
	}
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
		log.Warn("failed to drop analyze task", zap.Error(err))
		if !strings.Contains(err.Error(), "node not found") && !strings.Contains(err.Error(), "task not found") {
			log.Warn("failed to drop analyze task on worker", zap.Error(err))
			return err
		}
	}

	log.Info(context.TODO(), "dropped analyze task on worker successfully")
	return nil
}

func (at *analyzeTask) DropTaskOnWorker(cluster session.Cluster) {
	at.tryDropTaskOnWorker(cluster)
}
