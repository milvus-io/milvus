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
	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"path"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/task"
	"github.com/milvus-io/milvus/pkg/v2/util/indexparams"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type indexBuildTask struct {
	*model.SegmentIndex

	taskSlot int64

	queueTime time.Time
	startTime time.Time
	endTime   time.Time

	meta                      *meta
	handler                   Handler
	chunkManager              storage.ChunkManager
	indexEngineVersionManager IndexEngineVersionManager
}

var _ globalTask.Task = (*indexBuildTask)(nil)

func newIndexBuildTask(segIndex *model.SegmentIndex,
	taskSlot int64,
	meta *meta,
	handler Handler,
	chunkManager storage.ChunkManager,
	indexEngineVersionManager IndexEngineVersionManager,
) *indexBuildTask {
	return &indexBuildTask{
		SegmentIndex:              segIndex,
		taskSlot:                  taskSlot,
		meta:                      meta,
		handler:                   handler,
		chunkManager:              chunkManager,
		indexEngineVersionManager: indexEngineVersionManager,
	}
}

func (it *indexBuildTask) GetTaskID() int64 {
	return it.BuildID
}

func (it *indexBuildTask) GetTaskSlot() int64 {
	return it.taskSlot
}

func (it *indexBuildTask) GetTaskState() task.State {
	return task.State(it.IndexState)
}

func (it *indexBuildTask) SetQueueTime(t time.Time) {
	it.queueTime = t
}

func (it *indexBuildTask) GetQueueTime() time.Time {
	return it.queueTime
}

func (it *indexBuildTask) SetStartTime(t time.Time) {
	it.startTime = t
}

func (it *indexBuildTask) GetStartTime() time.Time {
	return it.startTime
}

func (it *indexBuildTask) SetEndTime(t time.Time) {
	it.endTime = t
}

func (it *indexBuildTask) GetEndTime() time.Time {
	return it.endTime
}

func (it *indexBuildTask) GetTaskType() task.Type {
	return task.Index
}

func (it *indexBuildTask) SetState(state indexpb.JobState, failReason string) {
	it.IndexState = commonpb.IndexState(state)
	it.FailReason = failReason
}

func (it *indexBuildTask) UpdateStateWithMeta(state indexpb.JobState, failReason string) error {
	if err := it.meta.indexMeta.UpdateIndexState(it.BuildID, commonpb.IndexState(state), failReason); err != nil {
		return err
	}
	it.SetState(state, failReason)
	return nil
}

func (it *indexBuildTask) UpdateTaskVersion(nodeID int64) error {
	if err := it.meta.indexMeta.UpdateVersion(it.BuildID, nodeID); err != nil {
		return err
	}
	it.IndexVersion++
	it.NodeID = nodeID
	return nil
}

func (it *indexBuildTask) setJobInfo(result *workerpb.IndexTaskInfo) error {
	if err := it.meta.indexMeta.FinishTask(result); err != nil {
		return err
	}
	it.SetState(indexpb.JobState(result.GetState()), result.GetFailReason())
	return nil
}

func (it *indexBuildTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	ctx := context.TODO()
	log := log.Ctx(ctx).With(zap.Int64("taskID", it.BuildID), zap.Int64("segmentID", it.SegmentID))

	// Check if task exists in meta
	segIndex, exist := it.meta.indexMeta.GetIndexJob(it.BuildID)
	if !exist || segIndex == nil {
		log.Info("index task has not exist in meta table, removing task")
		it.SetState(indexpb.JobState_JobStateNone, "index task has not exist in meta table")
		return
	}

	// Check segment health and index existence
	segment := it.meta.GetSegment(ctx, segIndex.SegmentID)
	if !isSegmentHealthy(segment) || !it.meta.indexMeta.IsIndexExist(segIndex.CollectionID, segIndex.IndexID) {
		log.Info("task is no need to build index, removing it")
		it.SetState(indexpb.JobState_JobStateNone, "task is no need to build index")
		return
	}

	// Handle special cases for certain index types or small segments
	indexParams := it.meta.indexMeta.GetIndexParams(segIndex.CollectionID, segIndex.IndexID)
	indexType := GetIndexType(indexParams)
	if isNoTrainIndex(indexType) || segIndex.NumRows < Params.DataCoordCfg.MinSegmentNumRowsToEnableIndex.GetAsInt64() {
		log.Info("segment does not need index really, marking as finished", zap.Int64("numRows", segIndex.NumRows))
		now := time.Now()
		it.SetStartTime(now)
		it.SetEndTime(now)
		it.UpdateStateWithMeta(indexpb.JobState_JobStateFinished, "fake finished index success")
		return
	}

	// Create job request
	req, err := it.prepareJobRequest(ctx, segment, segIndex, indexParams, indexType)
	if err != nil {
		log.Warn("failed to prepare job request", zap.Error(err))
		return
	}

	// Update task version
	if err := it.UpdateTaskVersion(nodeID); err != nil {
		log.Warn("failed to update task version", zap.Error(err))
		return
	}

	defer func() {
		if err != nil {
			it.DropTaskOnWorker(cluster)
		}
	}()

	// Send request to worker
	if err = cluster.CreateIndex(nodeID, req); err != nil {
		log.Warn("failed to send job to worker", zap.Error(err))
		return
	}

	// Update state to in progress
	if err = it.UpdateStateWithMeta(indexpb.JobState_JobStateInProgress, ""); err != nil {
		log.Warn("failed to update task state", zap.Error(err))
		return
	}

	log.Info("index task assigned successfully")
}

// Helper method to prepare job request
func (it *indexBuildTask) prepareJobRequest(ctx context.Context, segment *SegmentInfo, segIndex *model.SegmentIndex,
	indexParams []*commonpb.KeyValuePair, indexType string,
) (*workerpb.CreateJobRequest, error) {
	log := log.Ctx(ctx).With(zap.Int64("taskID", it.BuildID), zap.Int64("segmentID", segment.GetID()))

	typeParams := it.meta.indexMeta.GetTypeParams(segIndex.CollectionID, segIndex.IndexID)
	fieldID := it.meta.indexMeta.GetFieldIDByIndexID(segIndex.CollectionID, segIndex.IndexID)

	binlogIDs := getBinLogIDs(segment, fieldID)
	totalRows := getTotalBinlogRows(segment, fieldID)

	// Update index parameters as needed
	params := indexParams
	if vecindexmgr.GetVecIndexMgrInstance().IsVecIndex(indexType) && Params.KnowhereConfig.Enable.GetAsBool() {
		var err error
		params, err = Params.KnowhereConfig.UpdateIndexParams(GetIndexType(params), paramtable.BuildStage, params)
		if err != nil {
			return nil, fmt.Errorf("failed to update index build params: %w", err)
		}
	}

	if isDiskANNIndex(GetIndexType(params)) {
		var err error
		params, err = indexparams.UpdateDiskIndexBuildParams(Params, params)
		if err != nil {
			return nil, fmt.Errorf("failed to append index build params: %w", err)
		}
	}

	// Get collection info and field
	collectionInfo, err := it.handler.GetCollection(ctx, segment.GetCollectionID())
	if err != nil {
		return nil, fmt.Errorf("failed to get collection info: %w", err)
	}

	schema := collectionInfo.Schema
	var field *schemapb.FieldSchema

	for _, f := range schema.Fields {
		if f.FieldID == fieldID {
			field = f
			break
		}
	}

	if field == nil {
		return nil, fmt.Errorf("field not found with ID %d", fieldID)
	}

	// Extract dim only for vector types to avoid unnecessary warnings
	dim := -1
	if typeutil.IsFixDimVectorType(field.GetDataType()) {
		if dimVal, err := storage.GetDimFromParams(field.GetTypeParams()); err != nil {
			log.Warn("failed to get dim from field type params",
				zap.String("field type", field.GetDataType().String()), zap.Error(err))
		} else {
			dim = dimVal
		}
	}

	// Prepare optional fields for vector index
	optionalFields, partitionKeyIsolation := it.prepareOptionalFields(ctx, collectionInfo, segment, schema, indexType, field)

	// Create the job request
	req := &workerpb.CreateJobRequest{
		ClusterID:                 Params.CommonCfg.ClusterPrefix.GetValue(),
		IndexFilePrefix:           path.Join(it.chunkManager.RootPath(), common.SegmentIndexPath),
		BuildID:                   it.BuildID,
		IndexVersion:              segIndex.IndexVersion + 1,
		StorageConfig:             createStorageConfig(),
		IndexParams:               params,
		TypeParams:                typeParams,
		NumRows:                   segIndex.NumRows,
		CurrentIndexVersion:       it.indexEngineVersionManager.GetCurrentIndexEngineVersion(),
		CurrentScalarIndexVersion: it.indexEngineVersionManager.GetCurrentScalarIndexEngineVersion(),
		CollectionID:              segment.GetCollectionID(),
		PartitionID:               segment.GetPartitionID(),
		SegmentID:                 segment.GetID(),
		FieldID:                   fieldID,
		FieldName:                 field.GetName(),
		FieldType:                 field.GetDataType(),
		Dim:                       int64(dim),
		DataIds:                   binlogIDs,
		OptionalScalarFields:      optionalFields,
		Field:                     field,
		PartitionKeyIsolation:     partitionKeyIsolation,
		StorageVersion:            segment.StorageVersion,
		TaskSlot:                  it.taskSlot,
		LackBinlogRows:            segIndex.NumRows - totalRows,
	}

	return req, nil
}

// Helper method to prepare optional fields
func (it *indexBuildTask) prepareOptionalFields(ctx context.Context, collectionInfo *collectionInfo,
	segment *SegmentInfo, schema *schemapb.CollectionSchema, indexType string, field *schemapb.FieldSchema,
) ([]*indexpb.OptionalFieldInfo, bool) {
	optionalFields := make([]*indexpb.OptionalFieldInfo, 0)
	partitionKeyIsolation := false

	isVectorTypeSupported := typeutil.IsDenseFloatVectorType(field.DataType) || typeutil.IsBinaryVectorType(field.DataType)
	if Params.CommonCfg.EnableMaterializedView.GetAsBool() && isVectorTypeSupported && isMvSupported(indexType) {
		partitionKeyField, _ := typeutil.GetPartitionKeyFieldSchema(schema)
		if partitionKeyField != nil && typeutil.IsFieldDataTypeSupportMaterializedView(partitionKeyField) {
			optionalFields = append(optionalFields, &indexpb.OptionalFieldInfo{
				FieldID:   partitionKeyField.FieldID,
				FieldName: partitionKeyField.Name,
				FieldType: int32(partitionKeyField.DataType),
				DataIds:   getBinLogIDs(segment, partitionKeyField.FieldID),
			})

			iso, isoErr := common.IsPartitionKeyIsolationPropEnabled(collectionInfo.Properties)
			if isoErr != nil {
				log.Ctx(ctx).Warn("failed to parse partition key isolation", zap.Error(isoErr))
			}
			if iso {
				partitionKeyIsolation = true
			}
		}
	}

	return optionalFields, partitionKeyIsolation
}

func (it *indexBuildTask) QueryTaskOnWorker(cluster session.Cluster) {
	log := log.Ctx(context.TODO()).With(zap.Int64("taskID", it.BuildID), zap.Int64("segmentID", it.SegmentID), zap.Int64("nodeID", it.NodeID))

	results, err := cluster.QueryIndex(it.NodeID, &workerpb.QueryJobsRequest{
		ClusterID: Params.CommonCfg.ClusterPrefix.GetValue(),
		TaskIDs:   []UniqueID{it.BuildID},
	})
	if err != nil {
		log.Warn("query index task result from worker failed", zap.Error(err))
		if errors.Is(err, merr.ErrNodeNotFound) {
			// try to set task to init
			it.UpdateStateWithMeta(indexpb.JobState_JobStateInit, err.Error())
		}
		return
	}

	// indexInfos length is always one.
	for _, info := range results.GetResults() {
		if info.GetBuildID() == it.BuildID {
			switch info.GetState() {
			case commonpb.IndexState_Finished, commonpb.IndexState_Failed:
				log.Info("query task index info successfully",
					zap.Int64("taskID", it.BuildID), zap.String("result state", info.GetState().String()),
					zap.String("failReason", info.GetFailReason()))
				it.setJobInfo(info)
			case commonpb.IndexState_Retry:
				log.Info("query task index info successfully",
					zap.Int64("taskID", it.BuildID), zap.String("result state", info.GetState().String()),
					zap.String("failReason", info.GetFailReason()))
				// try to drop task on worker
				it.DropTaskOnWorker(cluster)
				// reset task state to init
				it.UpdateStateWithMeta(indexpb.JobState_JobStateInit, info.GetFailReason())
			case commonpb.IndexState_IndexStateNone:
				log.Info("query task index info successfully",
					zap.Int64("taskID", it.BuildID), zap.String("result state", info.GetState().String()),
					zap.String("failReason", info.GetFailReason()))
				// reset task state to init
				it.UpdateStateWithMeta(indexpb.JobState_JobStateInit, info.GetFailReason())
			}
			// inProgress or unissued, keep InProgress state
			return
		}
	}
	it.UpdateStateWithMeta(indexpb.JobState_JobStateInit, "index is not in info response")
}

func (it *indexBuildTask) DropTaskOnWorker(cluster session.Cluster) {
	log := log.Ctx(context.TODO()).With(zap.Int64("taskID", it.BuildID), zap.Int64("segmentID", it.SegmentID), zap.Int64("nodeID", it.NodeID))

	if err := cluster.DropIndex(it.NodeID, &workerpb.DropJobsRequest{
		ClusterID: Params.CommonCfg.ClusterPrefix.GetValue(),
		TaskIDs:   []UniqueID{it.BuildID},
	}); err != nil {
		log.Warn("notify worker drop the index task failed", zap.Error(err))
		return
	}

	log.Info("index task dropped successfully")
}
