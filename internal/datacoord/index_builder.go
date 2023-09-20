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
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
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
	scheduleDuration time.Duration

	notifyChan chan struct{}

	meta *meta

	tasksScheduler    schedulePolicy
	runningTasksQueue *runningTasksQueue

	policy                    buildIndexPolicy
	nodeManager               *IndexNodeManager
	chunkManager              storage.ChunkManager
	indexEngineVersionManager *IndexEngineVersionManager
}

type indexBuildTask struct {
	collectionID UniqueID
	buildID      UniqueID
	state        indexTaskState
	failReason   string
}

func newIndexBuilder(ctx context.Context,
	metaTable *meta,
	nodeManager *IndexNodeManager,
	chunkManager storage.ChunkManager,
	indexEngineVersionManager *IndexEngineVersionManager,
) *indexBuilder {
	ctx, cancel := context.WithCancel(ctx)

	ib := &indexBuilder{
		ctx:                       ctx,
		cancel:                    cancel,
		meta:                      metaTable,
		notifyChan:                make(chan struct{}, 1),
		scheduleDuration:          Params.DataCoordCfg.IndexTaskSchedulerInterval.GetAsDuration(time.Millisecond),
		policy:                    defaultBuildIndexPolicy,
		nodeManager:               nodeManager,
		chunkManager:              chunkManager,
		tasksScheduler:            newFairQueuePolicy(),
		runningTasksQueue:         newRunningTasksQueue(),
		indexEngineVersionManager: indexEngineVersionManager,
	}
	ib.reloadFromKV()
	return ib
}

func (ib *indexBuilder) Start() {
	ib.wg.Add(1)
	go ib.schedulerLoop()
}

func (ib *indexBuilder) Stop() {
	ib.cancel()
	ib.wg.Wait()
}

func (ib *indexBuilder) notify() {
	select {
	case ib.notifyChan <- struct{}{}:
	default:
	}
}

func (ib *indexBuilder) reloadFromKV() {
	segments := ib.meta.GetAllSegmentsUnsafe()
	for _, segment := range segments {
		for _, segIndex := range segment.segmentIndexes {
			if segIndex.IsDeleted {
				continue
			}
			if segIndex.IndexState == commonpb.IndexState_Unissued {
				ib.tasksScheduler.Push(&indexBuildTask{
					collectionID: segment.GetCollectionID(),
					buildID:      segIndex.BuildID,
					state:        indexTaskInit,
					failReason:   "",
				})
			} else if segIndex.IndexState == commonpb.IndexState_InProgress {
				ib.runningTasksQueue.push(&indexBuildTask{
					collectionID: segment.GetCollectionID(),
					buildID:      segIndex.BuildID,
					state:        indexTaskInProgress,
					failReason:   "",
				})
			}
		}
	}
}

func (ib *indexBuilder) enqueue(collectionID, buildID UniqueID) {
	defer ib.notify()

	task := &indexBuildTask{
		collectionID: collectionID,
		buildID:      buildID,
		state:        indexTaskInit,
		failReason:   "",
	}
	if ib.tasksScheduler.Exist(task) {
		log.Warn("index task already exist, check it", zap.Int64("collectionID", collectionID),
			zap.Int64("buildID", buildID))
		return
	}
	ib.tasksScheduler.Push(task)
	log.Info("task enqueue success", zap.Int64("collectionID", collectionID),
		zap.Int64("buildID", buildID))
}

func (ib *indexBuilder) schedulerLoop() {
	// receive notify
	// time ticker
	log.Info("index builder schedule loop start")
	defer ib.wg.Done()
	ticker := time.NewTicker(ib.scheduleDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ib.ctx.Done():
			log.Warn("index builder ctx done")
			return
		case _, ok := <-ib.notifyChan:
			if !ok {
				return
			}
			ib.run()
		// !ok means indexBuilder is closed.
		case <-ticker.C:
			ib.run()
		}
	}
}

func (ib *indexBuilder) run() {
	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()

		if ib.tasksScheduler.TaskCount() > 0 {
			log.Info("schedule index tasks", zap.Int("collection num", ib.tasksScheduler.NumRequesters()),
				zap.Int("task num", ib.tasksScheduler.TaskCount()))
			for {
				if ok := ib.scheduleTask(); !ok {
					break
				}
			}
		}
	}()

	go func() {
		defer wg.Done()
		if ib.runningTasksQueue.taskCount() > 0 {
			log.Info("schedule running index tasks", zap.Int("task num", ib.runningTasksQueue.taskCount()))
			tasks := ib.runningTasksQueue.getAllTasks()
			for _, task := range tasks {
				ib.checkProcessingTask(task)
			}
		}
	}()
	wg.Wait()
}

func (ib *indexBuilder) scheduleTask() bool {
	t := ib.tasksScheduler.Pop()
	if t == nil {
		return false
	}
	log.Info("index task will be assigned", zap.Int64("collectionID", t.collectionID),
		zap.Int64("buildID", t.buildID))
	meta, exist := ib.meta.GetIndexJob(t.buildID)
	if !exist {
		log.Ctx(ib.ctx).Debug("index task has not exist in meta table, remove task", zap.Int64("buildID", t.buildID))
		return true
	}
	segment := ib.meta.GetSegment(meta.SegmentID)
	if !isSegmentHealthy(segment) || !ib.meta.IsIndexExist(meta.CollectionID, meta.IndexID) {
		log.Ctx(ib.ctx).Info("task is no need to build index, remove it", zap.Int64("buildID", t.buildID))
		if err := ib.meta.DeleteTask(t.buildID); err != nil {
			log.Ctx(ib.ctx).Warn("IndexCoord delete index task fail, reEnqueue it", zap.Int64("buildID", t.buildID), zap.Error(err))
			ib.enqueue(t.collectionID, t.buildID)
			return false
		}
		return true
	}
	indexParams := ib.meta.GetIndexParams(meta.CollectionID, meta.IndexID)
	if isFlatIndex(getIndexType(indexParams)) || meta.NumRows < Params.DataCoordCfg.MinSegmentNumRowsToEnableIndex.GetAsInt64() {
		log.Ctx(ib.ctx).Debug("segment does not need index really", zap.Int64("buildID", t.buildID),
			zap.Int64("segmentID", meta.SegmentID), zap.Int64("num rows", meta.NumRows))
		if err := ib.meta.FinishTask(&indexpb.IndexTaskInfo{
			BuildID:        t.buildID,
			State:          commonpb.IndexState_Finished,
			IndexFileKeys:  nil,
			SerializedSize: 0,
			FailReason:     "",
		}); err != nil {
			log.Ctx(ib.ctx).Warn("IndexCoord update index state fail, reEnqueue it", zap.Int64("buildID", t.buildID), zap.Error(err))
			ib.enqueue(t.collectionID, t.buildID)
			return false
		}
		return true
	}
	// peek client
	// if all IndexNodes are executing task, wait for one of them to finish the task.
	nodeID, client := ib.nodeManager.PeekClient()
	if client == nil {
		log.Ctx(ib.ctx).WithRateGroup("dc.indexBuilder", 1, 60).RatedInfo(5, "index builder peek client error, there is no available. reEnqueue it")
		ib.enqueue(t.collectionID, t.buildID)
		return false
	}
	// update version and set nodeID
	if err := ib.meta.UpdateVersion(t.buildID, nodeID); err != nil {
		log.Ctx(ib.ctx).Warn("index builder update index version fail, reEnqueue it", zap.Int64("build", t.buildID), zap.Error(err))
		ib.enqueue(t.collectionID, t.buildID)
		return false
	}

	binLogs := make([]string, 0)
	fieldID := ib.meta.GetFieldIDByIndexID(meta.CollectionID, meta.IndexID)
	for _, fieldBinLog := range segment.GetBinlogs() {
		if fieldBinLog.GetFieldID() == fieldID {
			for _, binLog := range fieldBinLog.GetBinlogs() {
				binLogs = append(binLogs, binLog.LogPath)
			}
			break
		}
	}

	typeParams := ib.meta.GetTypeParams(meta.CollectionID, meta.IndexID)

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
	req := &indexpb.CreateJobRequest{
		ClusterID:           Params.CommonCfg.ClusterPrefix.GetValue(),
		IndexFilePrefix:     path.Join(ib.chunkManager.RootPath(), common.SegmentIndexPath),
		BuildID:             t.buildID,
		DataPaths:           binLogs,
		IndexVersion:        meta.IndexVersion + 1,
		StorageConfig:       storageConfig,
		IndexParams:         indexParams,
		TypeParams:          typeParams,
		NumRows:             meta.NumRows,
		CurrentIndexVersion: ib.indexEngineVersionManager.GetCurrentIndexEngineVersion(),
	}
	if err := ib.assignTask(client, req); err != nil {
		// need to release lock then reassign, so set task state to retry
		log.Ctx(ib.ctx).Warn("index builder assign task to IndexNode failed", zap.Int64("buildID", t.buildID),
			zap.Int64("nodeID", nodeID), zap.Error(err))
		ib.runningTasksQueue.push(&indexBuildTask{
			collectionID: t.collectionID,
			buildID:      t.buildID,
			state:        indexTaskRetry,
			failReason:   err.Error(),
		})
		return false
	}
	log.Ctx(ib.ctx).Info("index task assigned successfully", zap.Int64("buildID", t.buildID),
		zap.Int64("segmentID", meta.SegmentID), zap.Int64("nodeID", nodeID))
	// update index meta state to InProgress
	if err := ib.meta.BuildIndex(t.buildID); err != nil {
		// need to release lock then reassign, so set task state to retry
		log.Ctx(ib.ctx).Warn("index builder update index meta to InProgress failed", zap.Int64("buildID", t.buildID),
			zap.Int64("nodeID", nodeID), zap.Error(err))
		ib.runningTasksQueue.push(&indexBuildTask{
			collectionID: t.collectionID,
			buildID:      t.buildID,
			state:        indexTaskRetry,
			failReason:   err.Error(),
		})
		return false
	}
	ib.runningTasksQueue.push(&indexBuildTask{
		collectionID: t.collectionID,
		buildID:      t.buildID,
		state:        indexTaskInProgress,
		failReason:   "",
	})
	return true
}

func (ib *indexBuilder) checkProcessingTask(t *indexBuildTask) {
	if t == nil {
		log.Ctx(ib.ctx).Warn("get invalid index task")
		return
	}
	meta, exist := ib.meta.GetIndexJob(t.buildID)
	if !exist {
		log.Ctx(ib.ctx).Info("index task has not exist in meta table, remove task", zap.Int64("buildID", t.buildID))
		ib.runningTasksQueue.pop(t.collectionID, t.buildID)
		return
	}
	log.Ctx(ib.ctx).Info("check processing index task", zap.Int64("ClusterID", t.collectionID),
		zap.Int64("buildID", t.buildID), zap.String("task state", t.state.String()))

	switch t.state {
	case indexTaskDone:
		if !ib.dropIndexTask(t.buildID, meta.NodeID) {
			return
		}
		ib.runningTasksQueue.pop(t.collectionID, t.buildID)
	case indexTaskRetry:
		if !ib.dropIndexTask(t.buildID, meta.NodeID) {
			return
		}
		ib.runningTasksQueue.pop(t.collectionID, t.buildID)
		ib.enqueue(t.collectionID, t.buildID)

	case indexTaskInProgress:
		ib.runningTasksQueue.updateTaskState(t.collectionID, t.buildID, ib.getTaskState(t.buildID, meta.NodeID))
	default:
		log.Ctx(ib.ctx).Warn("index task state invalid", zap.Int64("ClusterID", t.collectionID),
			zap.Int64("buildID", t.buildID), zap.String("task state", t.state.String()))
		ib.runningTasksQueue.pop(t.collectionID, t.buildID)
	}
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
			return indexTaskInProgress
		}
		if response.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			log.Ctx(ib.ctx).Warn("IndexCoord get jobs info from IndexNode fail", zap.Int64("nodeID", nodeID),
				zap.Int64("buildID", buildID), zap.String("fail reason", response.GetStatus().GetReason()))
			return indexTaskInProgress
		}

		// indexInfos length is always one.
		for _, info := range response.GetIndexInfos() {
			if info.GetBuildID() == buildID {
				if info.GetState() == commonpb.IndexState_Failed || info.GetState() == commonpb.IndexState_Finished {
					log.Ctx(ib.ctx).Info("this task has been finished", zap.Int64("buildID", info.GetBuildID()),
						zap.String("index state", info.GetState().String()))
					if err := ib.meta.FinishTask(info); err != nil {
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

func (ib *indexBuilder) nodeDown(nodeID UniqueID) {
	defer ib.notify()

	metas := ib.meta.GetMetasByNodeID(nodeID)

	for _, meta := range metas {
		if ib.runningTasksQueue.getTaskState(meta.CollectionID, meta.BuildID) != indexTaskDone {
			ib.runningTasksQueue.updateTaskState(meta.CollectionID, meta.BuildID, indexTaskRetry)
		}
	}
}
