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
	"sync"
	"time"

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
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type indexTaskState int32

const (
	// when we receive a index task
	indexTaskInit indexTaskState = iota
	// we've sent index task to scheduler, and wait for building index.
	indexTaskInProgress
	// task done, wait to be cleaned
	indexTaskDone
	// index task need to retry.
	indexTaskRetry

	reqTimeoutInterval = time.Second * 10
)

var TaskStateNames = map[indexTaskState]string{
	0: "Init",
	1: "InProgress",
	2: "Done",
	3: "Retry",
}

func (x indexTaskState) String() string {
	ret, ok := TaskStateNames[x]
	if !ok {
		return "None"
	}
	return ret
}

type indexBuilder struct {
	ctx    context.Context
	cancel context.CancelFunc

	wg               sync.WaitGroup
	taskMutex        lock.RWMutex
	scheduleDuration time.Duration

	// TODO @xiaocai2333: use priority queue
	tasks      map[int64]indexTaskState
	notifyChan chan struct{}

	meta *meta

	policy                    buildIndexPolicy
	nodeManager               *IndexNodeManager
	chunkManager              storage.ChunkManager
	indexEngineVersionManager IndexEngineVersionManager
	handler                   Handler
}

func newIndexBuilder(
	ctx context.Context,
	metaTable *meta, nodeManager *IndexNodeManager,
	chunkManager storage.ChunkManager,
	indexEngineVersionManager IndexEngineVersionManager,
	handler Handler,
) *indexBuilder {
	ctx, cancel := context.WithCancel(ctx)

	ib := &indexBuilder{
		ctx:                       ctx,
		cancel:                    cancel,
		meta:                      metaTable,
		tasks:                     make(map[int64]indexTaskState),
		notifyChan:                make(chan struct{}, 1),
		scheduleDuration:          Params.DataCoordCfg.IndexTaskSchedulerInterval.GetAsDuration(time.Millisecond),
		policy:                    defaultBuildIndexPolicy,
		nodeManager:               nodeManager,
		chunkManager:              chunkManager,
		handler:                   handler,
		indexEngineVersionManager: indexEngineVersionManager,
	}
	ib.reloadFromKV()
	return ib
}

func (ib *indexBuilder) Start() {
	ib.wg.Add(1)
	go ib.schedule()
}

func (ib *indexBuilder) Stop() {
	ib.cancel()
	ib.wg.Wait()
}

func (ib *indexBuilder) reloadFromKV() {
	segments := ib.meta.GetAllSegmentsUnsafe()
	for _, segment := range segments {
		for _, segIndex := range ib.meta.indexMeta.getSegmentIndexes(segment.ID) {
			if segIndex.IsDeleted {
				continue
			}
			if segIndex.IndexState == commonpb.IndexState_Unissued {
				ib.tasks[segIndex.BuildID] = indexTaskInit
			} else if segIndex.IndexState == commonpb.IndexState_InProgress {
				ib.tasks[segIndex.BuildID] = indexTaskInProgress
			}
		}
	}
}

// notify is an unblocked notify function
func (ib *indexBuilder) notify() {
	select {
	case ib.notifyChan <- struct{}{}:
	default:
	}
}

func (ib *indexBuilder) enqueue(buildID UniqueID) {
	defer ib.notify()

	ib.taskMutex.Lock()
	defer ib.taskMutex.Unlock()
	if _, ok := ib.tasks[buildID]; !ok {
		ib.tasks[buildID] = indexTaskInit
	}
	log.Info("indexBuilder enqueue task", zap.Int64("buildID", buildID))
}

func (ib *indexBuilder) schedule() {
	// receive notifyChan
	// time ticker
	log.Ctx(ib.ctx).Info("index builder schedule loop start")
	defer ib.wg.Done()
	ticker := time.NewTicker(ib.scheduleDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ib.ctx.Done():
			log.Ctx(ib.ctx).Warn("index builder ctx done")
			return
		case _, ok := <-ib.notifyChan:
			if ok {
				ib.run()
			}
			// !ok means indexBuild is closed.
		case <-ticker.C:
			ib.run()
		}
	}
}

func (ib *indexBuilder) run() {
	ib.taskMutex.RLock()
	buildIDs := make([]UniqueID, 0, len(ib.tasks))
	for tID := range ib.tasks {
		buildIDs = append(buildIDs, tID)
	}
	ib.taskMutex.RUnlock()
	if len(buildIDs) > 0 {
		log.Ctx(ib.ctx).Info("index builder task schedule", zap.Int("task num", len(buildIDs)))
	}

	ib.policy(buildIDs)

	for _, buildID := range buildIDs {
		ok := ib.process(buildID)
		if !ok {
			log.Ctx(ib.ctx).Info("there is no idle indexing node, wait a minute...")
			break
		}
	}
}

func getBinLogIDs(segment *SegmentInfo, fieldID int64) []int64 {
	binlogIDs := make([]int64, 0)
	for _, fieldBinLog := range segment.GetBinlogs() {
		if fieldBinLog.GetFieldID() == fieldID {
			for _, binLog := range fieldBinLog.GetBinlogs() {
				binlogIDs = append(binlogIDs, binLog.GetLogID())
			}
			break
		}
	}
	return binlogIDs
}

func (ib *indexBuilder) process(buildID UniqueID) bool {
	ib.taskMutex.RLock()
	state := ib.tasks[buildID]
	ib.taskMutex.RUnlock()

	updateStateFunc := func(buildID UniqueID, state indexTaskState) {
		ib.taskMutex.Lock()
		defer ib.taskMutex.Unlock()
		ib.tasks[buildID] = state
	}

	deleteFunc := func(buildID UniqueID) {
		ib.taskMutex.Lock()
		defer ib.taskMutex.Unlock()
		delete(ib.tasks, buildID)
	}

	meta, exist := ib.meta.indexMeta.GetIndexJob(buildID)
	if !exist {
		log.Ctx(ib.ctx).Debug("index task has not exist in meta table, remove task", zap.Int64("buildID", buildID))
		deleteFunc(buildID)
		return true
	}

	switch state {
	case indexTaskInit:
		segment := ib.meta.GetSegment(meta.SegmentID)
		if !isSegmentHealthy(segment) || !ib.meta.indexMeta.IsIndexExist(meta.CollectionID, meta.IndexID) {
			log.Ctx(ib.ctx).Info("task is no need to build index, remove it", zap.Int64("buildID", buildID))
			if err := ib.meta.indexMeta.DeleteTask(buildID); err != nil {
				log.Ctx(ib.ctx).Warn("IndexCoord delete index failed", zap.Int64("buildID", buildID), zap.Error(err))
				return false
			}
			deleteFunc(buildID)
			return true
		}
		indexParams := ib.meta.indexMeta.GetIndexParams(meta.CollectionID, meta.IndexID)
		indexType := GetIndexType(indexParams)
		if isFlatIndex(indexType) || meta.NumRows < Params.DataCoordCfg.MinSegmentNumRowsToEnableIndex.GetAsInt64() {
			log.Ctx(ib.ctx).Info("segment does not need index really", zap.Int64("buildID", buildID),
				zap.Int64("segmentID", meta.SegmentID), zap.Int64("num rows", meta.NumRows))
			if err := ib.meta.indexMeta.FinishTask(&indexpb.IndexTaskInfo{
				BuildID:        buildID,
				State:          commonpb.IndexState_Finished,
				IndexFileKeys:  nil,
				SerializedSize: 0,
				FailReason:     "",
			}); err != nil {
				log.Ctx(ib.ctx).Warn("IndexCoord update index state fail", zap.Int64("buildID", buildID), zap.Error(err))
				return false
			}
			updateStateFunc(buildID, indexTaskDone)
			return true
		}
		// peek client
		// if all IndexNodes are executing task, wait for one of them to finish the task.
		nodeID, client := ib.nodeManager.PeekClient(meta)
		if client == nil {
			log.Ctx(ib.ctx).WithRateGroup("dc.indexBuilder", 1, 60).RatedInfo(5, "index builder peek client error, there is no available")
			return false
		}
		// update version and set nodeID
		if err := ib.meta.indexMeta.UpdateVersion(buildID, nodeID); err != nil {
			log.Ctx(ib.ctx).Warn("index builder update index version failed", zap.Int64("build", buildID), zap.Error(err))
			return false
		}

		// vector index build needs information of optional scalar fields data
		optionalFields := make([]*indexpb.OptionalFieldInfo, 0)
		if Params.CommonCfg.EnableMaterializedView.GetAsBool() {
			colSchema := ib.meta.GetCollection(meta.CollectionID).Schema
			if colSchema != nil {
				hasPartitionKey := typeutil.HasPartitionKey(colSchema)
				if hasPartitionKey {
					partitionKeyField, err := typeutil.GetPartitionKeyFieldSchema(colSchema)
					if partitionKeyField == nil || err != nil {
						log.Ctx(ib.ctx).Warn("index builder get partition key field failed", zap.Int64("build", buildID), zap.Error(err))
					} else {
						if typeutil.IsFieldDataTypeSupportMaterializedView(partitionKeyField) {
							optionalFields = append(optionalFields, &indexpb.OptionalFieldInfo{
								FieldID:   partitionKeyField.FieldID,
								FieldName: partitionKeyField.Name,
								FieldType: int32(partitionKeyField.DataType),
								DataIds:   getBinLogIDs(segment, partitionKeyField.FieldID),
							})
						}
					}
				}
			}
		}

		typeParams := ib.meta.indexMeta.GetTypeParams(meta.CollectionID, meta.IndexID)

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

		fieldID := ib.meta.indexMeta.GetFieldIDByIndexID(meta.CollectionID, meta.IndexID)
		binlogIDs := getBinLogIDs(segment, fieldID)
		if isDiskANNIndex(GetIndexType(indexParams)) {
			var err error
			indexParams, err = indexparams.UpdateDiskIndexBuildParams(Params, indexParams)
			if err != nil {
				log.Ctx(ib.ctx).Warn("failed to append index build params", zap.Int64("buildID", buildID),
					zap.Int64("nodeID", nodeID), zap.Error(err))
			}
		}
		var req *indexpb.CreateJobRequest
		collectionInfo, err := ib.handler.GetCollection(ib.ctx, segment.GetCollectionID())
		if err != nil {
			log.Info("index builder get collection info failed", zap.Int64("collectionID", segment.GetCollectionID()), zap.Error(err))
			return false
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
			log.Ctx(ib.ctx).Warn("failed to get dim from field type params",
				zap.String("field type", field.GetDataType().String()), zap.Error(err))
			// don't return, maybe field is scalar field or sparseFloatVector
		}
		if Params.CommonCfg.EnableStorageV2.GetAsBool() {
			storePath, err := itypeutil.GetStorageURI(params.Params.CommonCfg.StorageScheme.GetValue(), params.Params.CommonCfg.StoragePathPrefix.GetValue(), segment.GetID())
			if err != nil {
				log.Ctx(ib.ctx).Warn("failed to get storage uri", zap.Error(err))
				return false
			}
			indexStorePath, err := itypeutil.GetStorageURI(params.Params.CommonCfg.StorageScheme.GetValue(), params.Params.CommonCfg.StoragePathPrefix.GetValue()+"/index", segment.GetID())
			if err != nil {
				log.Ctx(ib.ctx).Warn("failed to get storage uri", zap.Error(err))
				return false
			}

			req = &indexpb.CreateJobRequest{
				ClusterID:            Params.CommonCfg.ClusterPrefix.GetValue(),
				IndexFilePrefix:      path.Join(ib.chunkManager.RootPath(), common.SegmentIndexPath),
				BuildID:              buildID,
				IndexVersion:         meta.IndexVersion + 1,
				StorageConfig:        storageConfig,
				IndexParams:          indexParams,
				TypeParams:           typeParams,
				NumRows:              meta.NumRows,
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
				CurrentIndexVersion:  ib.indexEngineVersionManager.GetCurrentIndexEngineVersion(),
				DataIds:              binlogIDs,
				OptionalScalarFields: optionalFields,
				Field:                field,
			}
		} else {
			req = &indexpb.CreateJobRequest{
				ClusterID:            Params.CommonCfg.ClusterPrefix.GetValue(),
				IndexFilePrefix:      path.Join(ib.chunkManager.RootPath(), common.SegmentIndexPath),
				BuildID:              buildID,
				IndexVersion:         meta.IndexVersion + 1,
				StorageConfig:        storageConfig,
				IndexParams:          indexParams,
				TypeParams:           typeParams,
				NumRows:              meta.NumRows,
				CurrentIndexVersion:  ib.indexEngineVersionManager.GetCurrentIndexEngineVersion(),
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

		if err := ib.assignTask(client, req); err != nil {
			// need to release lock then reassign, so set task state to retry
			log.Ctx(ib.ctx).Warn("index builder assign task to IndexNode failed", zap.Int64("buildID", buildID),
				zap.Int64("nodeID", nodeID), zap.Error(err))
			updateStateFunc(buildID, indexTaskRetry)
			return false
		}
		log.Ctx(ib.ctx).Info("index task assigned successfully", zap.Int64("buildID", buildID),
			zap.Int64("segmentID", meta.SegmentID), zap.Int64("nodeID", nodeID))
		// update index meta state to InProgress
		if err := ib.meta.indexMeta.BuildIndex(buildID); err != nil {
			// need to release lock then reassign, so set task state to retry
			log.Ctx(ib.ctx).Warn("index builder update index meta to InProgress failed", zap.Int64("buildID", buildID),
				zap.Int64("nodeID", nodeID), zap.Error(err))
			updateStateFunc(buildID, indexTaskRetry)
			return false
		}
		updateStateFunc(buildID, indexTaskInProgress)

	case indexTaskDone:
		if !ib.dropIndexTask(buildID, meta.NodeID) {
			return true
		}
		deleteFunc(buildID)
	case indexTaskRetry:
		if !ib.dropIndexTask(buildID, meta.NodeID) {
			return true
		}
		updateStateFunc(buildID, indexTaskInit)

	default:
		// state: in_progress
		updateStateFunc(buildID, ib.getTaskState(buildID, meta.NodeID))
	}
	return true
}

func (ib *indexBuilder) getTaskState(buildID, nodeID UniqueID) indexTaskState {
	client, exist := ib.nodeManager.GetClientByID(nodeID)
	if exist {
		ctx1, cancel := context.WithTimeout(ib.ctx, reqTimeoutInterval)
		defer cancel()
		response, err := client.QueryJobs(ctx1, &indexpb.QueryJobsRequest{
			ClusterID: Params.CommonCfg.ClusterPrefix.GetValue(),
			BuildIDs:  []int64{buildID},
		})
		if err != nil {
			log.Ctx(ib.ctx).Warn("IndexCoord get jobs info from IndexNode fail", zap.Int64("nodeID", nodeID),
				zap.Error(err))
			return indexTaskRetry
		}
		if response.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			log.Ctx(ib.ctx).Warn("IndexCoord get jobs info from IndexNode fail", zap.Int64("nodeID", nodeID),
				zap.Int64("buildID", buildID), zap.String("fail reason", response.GetStatus().GetReason()))
			return indexTaskRetry
		}

		// indexInfos length is always one.
		for _, info := range response.GetIndexInfos() {
			if info.GetBuildID() == buildID {
				if info.GetState() == commonpb.IndexState_Failed || info.GetState() == commonpb.IndexState_Finished {
					log.Ctx(ib.ctx).Info("this task has been finished", zap.Int64("buildID", info.GetBuildID()),
						zap.String("index state", info.GetState().String()))
					if err := ib.meta.indexMeta.FinishTask(info); err != nil {
						log.Ctx(ib.ctx).Warn("IndexCoord update index state fail", zap.Int64("buildID", info.GetBuildID()),
							zap.String("index state", info.GetState().String()), zap.Error(err))
						return indexTaskInProgress
					}
					return indexTaskDone
				} else if info.GetState() == commonpb.IndexState_Retry || info.GetState() == commonpb.IndexState_IndexStateNone {
					log.Ctx(ib.ctx).Info("this task should be retry", zap.Int64("buildID", buildID), zap.String("fail reason", info.GetFailReason()))
					return indexTaskRetry
				}
				return indexTaskInProgress
			}
		}
		log.Ctx(ib.ctx).Info("this task should be retry, indexNode does not have this task", zap.Int64("buildID", buildID),
			zap.Int64("nodeID", nodeID))
		return indexTaskRetry
	}
	// !exist --> node down
	log.Ctx(ib.ctx).Info("this task should be retry, indexNode is no longer exist", zap.Int64("buildID", buildID),
		zap.Int64("nodeID", nodeID))
	return indexTaskRetry
}

func (ib *indexBuilder) dropIndexTask(buildID, nodeID UniqueID) bool {
	client, exist := ib.nodeManager.GetClientByID(nodeID)
	if exist {
		ctx1, cancel := context.WithTimeout(ib.ctx, reqTimeoutInterval)
		defer cancel()
		status, err := client.DropJobs(ctx1, &indexpb.DropJobsRequest{
			ClusterID: Params.CommonCfg.ClusterPrefix.GetValue(),
			BuildIDs:  []UniqueID{buildID},
		})
		if err != nil {
			log.Ctx(ib.ctx).Warn("IndexCoord notify IndexNode drop the index task fail", zap.Int64("buildID", buildID),
				zap.Int64("nodeID", nodeID), zap.Error(err))
			return false
		}
		if status.GetErrorCode() != commonpb.ErrorCode_Success {
			log.Ctx(ib.ctx).Warn("IndexCoord notify IndexNode drop the index task fail", zap.Int64("buildID", buildID),
				zap.Int64("nodeID", nodeID), zap.String("fail reason", status.GetReason()))
			return false
		}
		log.Ctx(ib.ctx).Info("IndexCoord notify IndexNode drop the index task success",
			zap.Int64("buildID", buildID), zap.Int64("nodeID", nodeID))
		return true
	}
	log.Ctx(ib.ctx).Info("IndexNode no longer exist, no need to drop index task",
		zap.Int64("buildID", buildID), zap.Int64("nodeID", nodeID))
	return true
}

// assignTask sends the index task to the IndexNode, it has a timeout interval, if the IndexNode doesn't respond within
// the interval, it is considered that the task sending failed.
func (ib *indexBuilder) assignTask(builderClient types.IndexNodeClient, req *indexpb.CreateJobRequest) error {
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()
	resp, err := builderClient.CreateJob(ctx, req)
	if err == nil {
		err = merr.Error(resp)
	}
	if err != nil {
		log.Error("IndexCoord assignmentTasksLoop builderClient.CreateIndex failed", zap.Error(err))
		return err
	}

	return nil
}

func (ib *indexBuilder) nodeDown(nodeID UniqueID) {
	defer ib.notify()

	metas := ib.meta.indexMeta.GetMetasByNodeID(nodeID)

	ib.taskMutex.Lock()
	defer ib.taskMutex.Unlock()

	for _, meta := range metas {
		if ib.tasks[meta.BuildID] != indexTaskDone {
			ib.tasks[meta.BuildID] = indexTaskRetry
		}
	}
}
