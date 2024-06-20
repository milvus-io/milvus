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
	"path"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	itypeutil "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/indexparams"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type indexBuildTask struct {
	buildID  int64
	nodeID   int64
	taskInfo *indexpb.IndexTaskInfo
}

func (it *indexBuildTask) GetTaskID() int64 {
	return it.buildID
}

func (it *indexBuildTask) GetNodeID() int64 {
	return it.nodeID
}

func (it *indexBuildTask) ResetNodeID() {
	it.nodeID = 0
}

func (it *indexBuildTask) CheckTaskHealthy(mt *meta) bool {
	_, exist := mt.indexMeta.GetIndexJob(it.GetTaskID())
	return exist
}

func (it *indexBuildTask) SetState(state indexpb.JobState, failReason string) {
	it.taskInfo.State = commonpb.IndexState(state)
	it.taskInfo.FailReason = failReason
}

func (it *indexBuildTask) GetState() indexpb.JobState {
	return indexpb.JobState(it.taskInfo.GetState())
}

func (it *indexBuildTask) GetFailReason() string {
	return it.taskInfo.FailReason
}

func (it *indexBuildTask) UpdateVersion(ctx context.Context, meta *meta) error {
	return meta.indexMeta.UpdateVersion(it.buildID)
}

func (it *indexBuildTask) UpdateMetaBuildingState(nodeID int64, meta *meta) error {
	it.nodeID = nodeID
	return meta.indexMeta.BuildIndex(it.buildID, nodeID)
}

func (it *indexBuildTask) AssignTask(ctx context.Context, client types.IndexNodeClient, dependency *taskScheduler) (bool, bool) {
	segIndex, exist := dependency.meta.indexMeta.GetIndexJob(it.buildID)
	if !exist || segIndex == nil {
		log.Ctx(ctx).Info("index task has not exist in meta table, remove task", zap.Int64("buildID", it.buildID))
		it.SetState(indexpb.JobState_JobStateNone, "index task has not exist in meta table")
		return false, false
	}

	segment := dependency.meta.GetSegment(segIndex.SegmentID)
	if !isSegmentHealthy(segment) || !dependency.meta.indexMeta.IsIndexExist(segIndex.CollectionID, segIndex.IndexID) {
		log.Ctx(ctx).Info("task is no need to build index, remove it", zap.Int64("buildID", it.buildID))
		it.SetState(indexpb.JobState_JobStateNone, "task is no need to build index")
		return false, false
	}
	indexParams := dependency.meta.indexMeta.GetIndexParams(segIndex.CollectionID, segIndex.IndexID)
	indexType := GetIndexType(indexParams)
	if isFlatIndex(indexType) || segIndex.NumRows < Params.DataCoordCfg.MinSegmentNumRowsToEnableIndex.GetAsInt64() {
		log.Ctx(ctx).Info("segment does not need index really", zap.Int64("buildID", it.buildID),
			zap.Int64("segmentID", segIndex.SegmentID), zap.Int64("num rows", segIndex.NumRows))
		it.SetState(indexpb.JobState_JobStateFinished, "fake finished index success")
		return true, true
	}
	// vector index build needs information of optional scalar fields data
	optionalFields := make([]*indexpb.OptionalFieldInfo, 0)
	if Params.CommonCfg.EnableMaterializedView.GetAsBool() && isOptionalScalarFieldSupported(indexType) {
		collInfo, err := dependency.handler.GetCollection(ctx, segIndex.CollectionID)
		if err != nil || collInfo == nil {
			log.Ctx(ctx).Warn("get collection failed", zap.Int64("collID", segIndex.CollectionID), zap.Error(err))
			it.SetState(indexpb.JobState_JobStateInit, err.Error())
			return false, false
		}
		colSchema := collInfo.Schema
		partitionKeyField, err := typeutil.GetPartitionKeyFieldSchema(colSchema)
		if partitionKeyField == nil || err != nil {
			log.Ctx(ctx).Warn("index builder get partition key field failed", zap.Int64("buildID", it.buildID), zap.Error(err))
		} else {
			optionalFields = append(optionalFields, &indexpb.OptionalFieldInfo{
				FieldID:   partitionKeyField.FieldID,
				FieldName: partitionKeyField.Name,
				FieldType: int32(partitionKeyField.DataType),
				DataIds:   getBinLogIDs(segment, partitionKeyField.FieldID),
			})
		}
	}

	typeParams := dependency.meta.indexMeta.GetTypeParams(segIndex.CollectionID, segIndex.IndexID)

	var storageConfig *indexpb.StorageConfig
	if Params.CommonCfg.StorageType.GetValue() == "local" {
		storageConfig = &indexpb.StorageConfig{
			RootPath:    Params.LocalStorageCfg.Path.GetValue(),
			StorageType: Params.CommonCfg.StorageType.GetValue(),
		}
	} else {
		storageConfig = &indexpb.StorageConfig{
			Address:          Params.MinioCfg.Address.GetValue(),
			AccessKeyID:      Params.MinioCfg.AccessKeyID.GetValue(),
			SecretAccessKey:  Params.MinioCfg.SecretAccessKey.GetValue(),
			UseSSL:           Params.MinioCfg.UseSSL.GetAsBool(),
			SslCACert:        Params.MinioCfg.SslCACert.GetValue(),
			BucketName:       Params.MinioCfg.BucketName.GetValue(),
			RootPath:         Params.MinioCfg.RootPath.GetValue(),
			UseIAM:           Params.MinioCfg.UseIAM.GetAsBool(),
			IAMEndpoint:      Params.MinioCfg.IAMEndpoint.GetValue(),
			StorageType:      Params.CommonCfg.StorageType.GetValue(),
			Region:           Params.MinioCfg.Region.GetValue(),
			UseVirtualHost:   Params.MinioCfg.UseVirtualHost.GetAsBool(),
			CloudProvider:    Params.MinioCfg.CloudProvider.GetValue(),
			RequestTimeoutMs: Params.MinioCfg.RequestTimeoutMs.GetAsInt64(),
		}
	}

	fieldID := dependency.meta.indexMeta.GetFieldIDByIndexID(segIndex.CollectionID, segIndex.IndexID)
	binlogIDs := getBinLogIDs(segment, fieldID)
	if isDiskANNIndex(GetIndexType(indexParams)) {
		var err error
		indexParams, err = indexparams.UpdateDiskIndexBuildParams(Params, indexParams)
		if err != nil {
			log.Ctx(ctx).Warn("failed to append index build params", zap.Int64("buildID", it.buildID), zap.Error(err))
			it.SetState(indexpb.JobState_JobStateInit, err.Error())
			return false, false
		}
	}
	var req *indexpb.CreateJobRequest
	collectionInfo, err := dependency.handler.GetCollection(ctx, segment.GetCollectionID())
	if err != nil {
		log.Ctx(ctx).Info("index builder get collection info failed", zap.Int64("collectionID", segment.GetCollectionID()), zap.Error(err))
		return false, false
	}

	schema := collectionInfo.Schema
	var field *schemapb.FieldSchema

	for _, f := range schema.Fields {
		if f.FieldID == fieldID {
			field = f
			break
		}
	}

	dim, err := storage.GetDimFromParams(field.TypeParams)
	if err != nil {
		log.Ctx(ctx).Warn("failed to get dim from field type params",
			zap.String("field type", field.GetDataType().String()), zap.Error(err))
		// don't return, maybe field is scalar field or sparseFloatVector
	}

	if Params.CommonCfg.EnableStorageV2.GetAsBool() {
		storePath, err := itypeutil.GetStorageURI(params.Params.CommonCfg.StorageScheme.GetValue(), params.Params.CommonCfg.StoragePathPrefix.GetValue(), segment.GetID())
		if err != nil {
			log.Ctx(ctx).Warn("failed to get storage uri", zap.Error(err))
			it.SetState(indexpb.JobState_JobStateInit, err.Error())
			return false, false
		}
		indexStorePath, err := itypeutil.GetStorageURI(params.Params.CommonCfg.StorageScheme.GetValue(), params.Params.CommonCfg.StoragePathPrefix.GetValue()+"/index", segment.GetID())
		if err != nil {
			log.Ctx(ctx).Warn("failed to get storage uri", zap.Error(err))
			it.SetState(indexpb.JobState_JobStateInit, err.Error())
			return false, false
		}

		req = &indexpb.CreateJobRequest{
			ClusterID:            Params.CommonCfg.ClusterPrefix.GetValue(),
			IndexFilePrefix:      path.Join(dependency.chunkManager.RootPath(), common.SegmentIndexPath),
			BuildID:              it.buildID,
			IndexVersion:         segIndex.IndexVersion,
			StorageConfig:        storageConfig,
			IndexParams:          indexParams,
			TypeParams:           typeParams,
			NumRows:              segIndex.NumRows,
			CollectionID:         segment.GetCollectionID(),
			PartitionID:          segment.GetPartitionID(),
			SegmentID:            segment.GetID(),
			FieldID:              fieldID,
			FieldName:            field.Name,
			FieldType:            field.DataType,
			StorePath:            storePath,
			StoreVersion:         segment.GetStorageVersion(),
			IndexStorePath:       indexStorePath,
			Dim:                  int64(dim),
			CurrentIndexVersion:  dependency.indexEngineVersionManager.GetCurrentIndexEngineVersion(),
			DataIds:              binlogIDs,
			OptionalScalarFields: optionalFields,
			Field:                field,
		}
	} else {
		req = &indexpb.CreateJobRequest{
			ClusterID:            Params.CommonCfg.ClusterPrefix.GetValue(),
			IndexFilePrefix:      path.Join(dependency.chunkManager.RootPath(), common.SegmentIndexPath),
			BuildID:              it.buildID,
			IndexVersion:         segIndex.IndexVersion,
			StorageConfig:        storageConfig,
			IndexParams:          indexParams,
			TypeParams:           typeParams,
			NumRows:              segIndex.NumRows,
			CurrentIndexVersion:  dependency.indexEngineVersionManager.GetCurrentIndexEngineVersion(),
			DataIds:              binlogIDs,
			CollectionID:         segment.GetCollectionID(),
			PartitionID:          segment.GetPartitionID(),
			SegmentID:            segment.GetID(),
			FieldID:              fieldID,
			OptionalScalarFields: optionalFields,
			Dim:                  int64(dim),
			Field:                field,
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()
	resp, err := client.CreateJobV2(ctx, &indexpb.CreateJobV2Request{
		ClusterID: req.GetClusterID(),
		TaskID:    req.GetBuildID(),
		JobType:   indexpb.JobType_JobTypeIndexJob,
		Request: &indexpb.CreateJobV2Request_IndexRequest{
			IndexRequest: req,
		},
	})
	if err == nil {
		err = merr.Error(resp)
	}
	if err != nil {
		log.Ctx(ctx).Warn("assign index task to indexNode failed", zap.Int64("buildID", it.buildID), zap.Error(err))
		it.SetState(indexpb.JobState_JobStateRetry, err.Error())
		return false, true
	}

	log.Ctx(ctx).Info("index task assigned successfully", zap.Int64("buildID", it.buildID),
		zap.Int64("segmentID", segIndex.SegmentID))
	it.SetState(indexpb.JobState_JobStateInProgress, "")
	return true, false
}

func (it *indexBuildTask) setResult(info *indexpb.IndexTaskInfo) {
	it.taskInfo = info
}

func (it *indexBuildTask) QueryResult(ctx context.Context, node types.IndexNodeClient) {
	resp, err := node.QueryJobsV2(ctx, &indexpb.QueryJobsV2Request{
		ClusterID: Params.CommonCfg.ClusterPrefix.GetValue(),
		TaskIDs:   []UniqueID{it.GetTaskID()},
		JobType:   indexpb.JobType_JobTypeIndexJob,
	})
	if err == nil {
		err = merr.Error(resp.GetStatus())
	}
	if err != nil {
		log.Ctx(ctx).Warn("get jobs info from IndexNode failed", zap.Int64("buildID", it.GetTaskID()),
			zap.Int64("nodeID", it.GetNodeID()), zap.Error(err))
		it.SetState(indexpb.JobState_JobStateRetry, err.Error())
		return
	}

	// indexInfos length is always one.
	for _, info := range resp.GetIndexJobResults().GetResults() {
		if info.GetBuildID() == it.GetTaskID() {
			log.Ctx(ctx).Info("query task index info successfully",
				zap.Int64("taskID", it.GetTaskID()), zap.String("result state", info.GetState().String()),
				zap.String("failReason", info.GetFailReason()))
			if info.GetState() == commonpb.IndexState_Finished || info.GetState() == commonpb.IndexState_Failed ||
				info.GetState() == commonpb.IndexState_Retry {
				// state is retry or finished or failed
				it.setResult(info)
			} else if info.GetState() == commonpb.IndexState_IndexStateNone {
				it.SetState(indexpb.JobState_JobStateRetry, "index state is none in info response")
			}
			// inProgress or unissued, keep InProgress state
			return
		}
	}
	it.SetState(indexpb.JobState_JobStateRetry, "index is not in info response")
}

func (it *indexBuildTask) DropTaskOnWorker(ctx context.Context, client types.IndexNodeClient) bool {
	resp, err := client.DropJobsV2(ctx, &indexpb.DropJobsV2Request{
		ClusterID: Params.CommonCfg.ClusterPrefix.GetValue(),
		TaskIDs:   []UniqueID{it.GetTaskID()},
		JobType:   indexpb.JobType_JobTypeIndexJob,
	})
	if err == nil {
		err = merr.Error(resp)
	}
	if err != nil {
		log.Ctx(ctx).Warn("notify worker drop the index task fail", zap.Int64("taskID", it.GetTaskID()),
			zap.Int64("nodeID", it.GetNodeID()), zap.Error(err))
		return false
	}
	log.Ctx(ctx).Info("drop index task on worker success", zap.Int64("taskID", it.GetTaskID()),
		zap.Int64("nodeID", it.GetNodeID()))
	return true
}

func (it *indexBuildTask) SetJobInfo(meta *meta) error {
	return meta.indexMeta.FinishTask(it.taskInfo)
}
