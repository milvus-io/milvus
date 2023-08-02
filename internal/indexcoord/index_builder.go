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

package indexcoord

import (
	"context"
	"errors"
	"path"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
)

type indexBuilder struct {
	ctx    context.Context
	cancel context.CancelFunc

	wg               sync.WaitGroup
	taskMutex        sync.RWMutex
	scheduleDuration time.Duration

	// TODO @xiaocai2333: use priority queue
	tasks      map[int64]indexTaskState
	notifyChan chan struct{}

	ic *IndexCoord

	meta *metaTable
}

func newIndexBuilder(ctx context.Context, ic *IndexCoord, metaTable *metaTable, aliveNodes []UniqueID) *indexBuilder {
	ctx, cancel := context.WithCancel(ctx)

	ib := &indexBuilder{
		ctx:              ctx,
		cancel:           cancel,
		meta:             metaTable,
		ic:               ic,
		tasks:            make(map[int64]indexTaskState),
		notifyChan:       make(chan struct{}, 1),
		scheduleDuration: Params.IndexCoordCfg.SchedulerInterval,
	}
	ib.reloadFromKV(aliveNodes)
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

func (ib *indexBuilder) reloadFromKV(aliveNodes []UniqueID) {
	metas := ib.meta.GetAllIndexMeta()
	for build, indexMeta := range metas {
		// deleted, need to release lock and clean meta
		if indexMeta.IsDeleted || ib.meta.IsIndexDeleted(indexMeta.CollectionID, indexMeta.IndexID) {
			if indexMeta.NodeID != 0 {
				ib.tasks[build] = indexTaskDeleted
			}
		} else if indexMeta.IndexState == commonpb.IndexState_Unissued && indexMeta.NodeID == 0 {
			// unissued, need to acquire lock and assign task
			ib.tasks[build] = indexTaskInit
		} else if indexMeta.IndexState == commonpb.IndexState_Unissued && indexMeta.NodeID != 0 {
			// retry, need to release lock and reassign task
			// need to release reference lock
			ib.tasks[build] = indexTaskRetry
		} else if indexMeta.IndexState == commonpb.IndexState_InProgress {
			// need to check IndexNode is still alive.
			alive := false
			for _, nodeID := range aliveNodes {
				if nodeID == indexMeta.NodeID {
					alive = true
					break
				}
			}
			if !alive {
				// IndexNode is down, need to retry
				ib.tasks[build] = indexTaskRetry
			} else {
				// in_progress, nothing to do
				ib.tasks[build] = indexTaskInProgress
			}
		} else if indexMeta.IndexState == commonpb.IndexState_Finished || indexMeta.IndexState == commonpb.IndexState_Failed {
			if indexMeta.NodeID != 0 {
				// task is done, but the lock has not been released, need to release.
				ib.tasks[build] = indexTaskDone
			}
			// else: task is done, and lock has been released, no need to add to index builder.
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

	sort.Slice(buildIDs, func(i, j int) bool {
		return buildIDs[i] < buildIDs[j]
	})
	if len(buildIDs) > 0 {
		log.Ctx(ib.ctx).Info("index builder task schedule", zap.Int("task num", len(buildIDs)))
	}
	for _, buildID := range buildIDs {
		ok := ib.process(buildID)
		if !ok {
			log.Ctx(ib.ctx).Info("there is no IndexNode available or etcd is not serviceable, wait a minute...")
			break
		}
	}
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

	meta, exist := ib.meta.GetMeta(buildID)
	if !exist {
		log.Ctx(ib.ctx).RatedDebug(5, "index task has not exist in meta table, remove task", zap.Int64("buildID", buildID))
		deleteFunc(buildID)
		return true
	}

	switch state {
	case indexTaskInit:
		if !ib.meta.NeedIndex(meta.CollectionID, meta.IndexID) {
			log.Ctx(ib.ctx).RatedDebug(5, "task is no need to build index, remove it", zap.Int64("buildID", buildID))
			deleteFunc(buildID)
			return true
		}
		indexParams := ib.meta.GetIndexParams(meta.CollectionID, meta.IndexID)
		if isFlatIndex(getIndexType(indexParams)) || meta.NumRows < Params.IndexCoordCfg.MinSegmentNumRowsToEnableIndex {
			log.Ctx(ib.ctx).Debug("segment does not need index really", zap.Int64("buildID", buildID),
				zap.Int64("segID", meta.SegmentID), zap.Int64("num rows", meta.NumRows))
			if err := ib.meta.FinishTask(&indexpb.IndexTaskInfo{
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
		nodeID, client := ib.ic.nodeManager.PeekClient(meta)
		if client == nil {
			log.Ctx(ib.ctx).RatedInfo(5, "index builder peek client error, there is no available")
			return false
		}
		// update version and set nodeID
		if err := ib.meta.UpdateVersion(buildID, nodeID); err != nil {
			log.Ctx(ib.ctx).Warn("index builder update index version failed", zap.Int64("build", buildID), zap.Error(err))
			return false
		}

		// acquire lock
		if err := ib.ic.tryAcquireSegmentReferLock(ib.ctx, buildID, nodeID, []UniqueID{meta.SegmentID}); err != nil {
			log.Ctx(ib.ctx).Warn("index builder acquire segment reference lock failed", zap.Int64("buildID", buildID),
				zap.Int64("nodeID", nodeID), zap.Error(err))
			updateStateFunc(buildID, indexTaskRetry)
			return false
		}
		info, err := ib.ic.pullSegmentInfo(ib.ctx, meta.SegmentID)
		if err != nil {
			log.Ctx(ib.ctx).Warn("IndexCoord get segment info from DataCoord fail", zap.Int64("segID", meta.SegmentID),
				zap.Int64("buildID", buildID), zap.Error(err))
			if errors.Is(err, ErrSegmentNotFound) {
				updateStateFunc(buildID, indexTaskDeleted)
				return true
			}
			updateStateFunc(buildID, indexTaskRetry)
			return false
		}
		binLogs := make([]string, 0)
		fieldID := ib.meta.GetFieldIDByIndexID(meta.CollectionID, meta.IndexID)
		for _, fieldBinLog := range info.GetBinlogs() {
			if fieldBinLog.GetFieldID() == fieldID {
				for _, binLog := range fieldBinLog.GetBinlogs() {
					binLogs = append(binLogs, binLog.LogPath)
				}
				break
			}
		}

		typeParams := ib.meta.GetTypeParams(meta.CollectionID, meta.IndexID)

		var storageConfig *indexpb.StorageConfig
		if Params.CommonCfg.StorageType == "local" {
			storageConfig = &indexpb.StorageConfig{
				RootPath:    Params.LocalStorageCfg.Path,
				StorageType: Params.CommonCfg.StorageType,
			}
		} else {
			storageConfig = &indexpb.StorageConfig{
				Address:         Params.MinioCfg.Address,
				AccessKeyID:     Params.MinioCfg.AccessKeyID,
				SecretAccessKey: Params.MinioCfg.SecretAccessKey,
				UseSSL:          Params.MinioCfg.UseSSL,
				BucketName:      Params.MinioCfg.BucketName,
				RootPath:        Params.MinioCfg.RootPath,
				UseIAM:          Params.MinioCfg.UseIAM,
				IAMEndpoint:     Params.MinioCfg.IAMEndpoint,
				StorageType:     Params.CommonCfg.StorageType,
				Region:          Params.MinioCfg.Region,
				UseVirtualHost:  Params.MinioCfg.UseVirtualHost,
			}
		}
		req := &indexpb.CreateJobRequest{
			ClusterID:       Params.CommonCfg.GetClusterPrefix(),
			IndexFilePrefix: path.Join(ib.ic.chunkManager.RootPath(), common.SegmentIndexPath),
			BuildID:         buildID,
			DataPaths:       binLogs,
			IndexVersion:    meta.IndexVersion + 1,
			StorageConfig:   storageConfig,
			IndexParams:     indexParams,
			TypeParams:      typeParams,
			NumRows:         meta.NumRows,
		}
		if err := ib.ic.assignTask(client, req); err != nil {
			// need to release lock then reassign, so set task state to retry
			log.Ctx(ib.ctx).RatedWarn(10, "index builder assign task to IndexNode failed", zap.Int64("buildID", buildID),
				zap.Int64("nodeID", nodeID), zap.Error(err))
			updateStateFunc(buildID, indexTaskRetry)
			return false
		}
		log.Ctx(ib.ctx).Info("index task assigned successfully", zap.Int64("buildID", buildID),
			zap.Int64("segID", meta.SegmentID), zap.Int64("nodeID", nodeID))
		// update index meta state to InProgress
		if err := ib.meta.BuildIndex(buildID); err != nil {
			// need to release lock then reassign, so set task state to retry
			log.Ctx(ib.ctx).Warn("index builder update index meta to InProgress failed", zap.Int64("buildID", buildID),
				zap.Int64("nodeID", nodeID), zap.Error(err))
			updateStateFunc(buildID, indexTaskRetry)
			return false
		}
		updateStateFunc(buildID, indexTaskInProgress)

	case indexTaskDone:
		if !ib.meta.NeedIndex(meta.CollectionID, meta.IndexID) {
			log.Ctx(ib.ctx).Info("task is no need to build index, remove it", zap.Int64("buildID", buildID),
				zap.Int64("segID", meta.SegmentID))
			updateStateFunc(buildID, indexTaskDeleted)
			return true
		}

		if !ib.dropIndexTask(buildID, meta.NodeID) {
			return true
		}
		if err := ib.releaseLockAndResetNode(buildID, meta.NodeID); err != nil {
			// release lock failed, no need to modify state, wait to retry
			return false
		}
		deleteFunc(buildID)
	case indexTaskRetry:
		if !ib.meta.NeedIndex(meta.CollectionID, meta.IndexID) {
			log.Ctx(ib.ctx).Info("task is no need to build index, remove it", zap.Int64("buildID", buildID))
			updateStateFunc(buildID, indexTaskDeleted)
			return true
		}
		if !ib.dropIndexTask(buildID, meta.NodeID) {
			return true
		}
		if err := ib.releaseLockAndResetTask(buildID, meta.NodeID); err != nil {
			// release lock failed, no need to modify state, wait to retry
			return false
		}
		updateStateFunc(buildID, indexTaskInit)

	case indexTaskDeleted:
		log.Ctx(ib.ctx).Debug("index task state is deleted, try to release reference lock", zap.Int64("buildID", buildID))
		if err := ib.meta.MarkSegmentsIndexAsDeletedByBuildID([]int64{buildID}); err != nil {
			return false
		}
		if meta.NodeID != 0 {
			if !ib.dropIndexTask(buildID, meta.NodeID) {
				log.Ctx(ib.ctx).Warn("index task state is deleted and drop index job for node fail", zap.Int64("build", buildID),
					zap.Int64("nodeID", meta.NodeID))
				return true
			}
			if err := ib.releaseLockAndResetNode(buildID, meta.NodeID); err != nil {
				// release lock failed, no need to modify state, wait to retry
				return false
			}
		}
		// reset nodeID success, remove task.
		deleteFunc(buildID)

	default:
		// state: in_progress
		if !ib.meta.NeedIndex(meta.CollectionID, meta.IndexID) {
			log.Ctx(ib.ctx).Debug("task is no need to build index, remove it", zap.Int64("buildID", buildID))
			updateStateFunc(buildID, indexTaskDeleted)
			return true
		}
		updateStateFunc(buildID, ib.getTaskState(buildID, meta.NodeID))
	}
	return true
}

func (ib *indexBuilder) getTaskState(buildID, nodeID UniqueID) indexTaskState {
	client, exist := ib.ic.nodeManager.GetClientByID(nodeID)
	if exist {
		ctx1, cancel := context.WithTimeout(ib.ctx, reqTimeoutInterval)
		defer cancel()
		response, err := client.QueryJobs(ctx1, &indexpb.QueryJobsRequest{
			ClusterID: Params.CommonCfg.GetClusterPrefix(),
			BuildIDs:  []int64{buildID},
		})
		if err != nil {
			log.Ctx(ib.ctx).Warn("IndexCoord get jobs info from IndexNode fail", zap.Int64("nodeID", nodeID),
				zap.Error(err))
			return indexTaskInProgress
		}
		if response.Status.ErrorCode != commonpb.ErrorCode_Success {
			log.Ctx(ib.ctx).Warn("IndexCoord get jobs info from IndexNode fail", zap.Int64("nodeID", nodeID),
				zap.Int64("buildID", buildID), zap.String("fail reason", response.Status.Reason))
			return indexTaskInProgress
		}

		// indexInfos length is always one.
		for _, info := range response.IndexInfos {
			if info.BuildID == buildID {
				if info.State == commonpb.IndexState_Failed || info.State == commonpb.IndexState_Finished {
					log.Ctx(ib.ctx).Info("this task has been finished", zap.Int64("buildID", info.BuildID),
						zap.String("index state", info.State.String()))
					if err := ib.meta.FinishTask(info); err != nil {
						log.Ctx(ib.ctx).Warn("IndexCoord update index state fail", zap.Int64("buildID", info.BuildID),
							zap.String("index state", info.State.String()), zap.Error(err))
						return indexTaskInProgress
					}
					return indexTaskDone
				} else if info.State == commonpb.IndexState_Retry || info.State == commonpb.IndexState_IndexStateNone {
					log.Ctx(ib.ctx).Info("this task should be retry", zap.Int64("buildID", buildID), zap.String("fail reason", info.FailReason))
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
	client, exist := ib.ic.nodeManager.GetClientByID(nodeID)
	if exist {
		ctx1, cancel := context.WithTimeout(ib.ctx, reqTimeoutInterval)
		defer cancel()
		status, err := client.DropJobs(ctx1, &indexpb.DropJobsRequest{
			ClusterID: Params.CommonCfg.GetClusterPrefix(),
			BuildIDs:  []UniqueID{buildID},
		})
		if err != nil {
			log.Ctx(ib.ctx).Warn("IndexCoord notify IndexNode drop the index task fail", zap.Int64("buildID", buildID),
				zap.Int64("nodeID", nodeID), zap.Error(err))
			return false
		}
		if status.ErrorCode != commonpb.ErrorCode_Success {
			log.Ctx(ib.ctx).Warn("IndexCoord notify IndexNode drop the index task fail", zap.Int64("buildID", buildID),
				zap.Int64("nodeID", nodeID), zap.String("fail reason", status.Reason))
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

func (ib *indexBuilder) releaseLockAndResetNode(buildID UniqueID, nodeID UniqueID) error {
	if err := ib.ic.tryReleaseSegmentReferLock(ib.ctx, buildID, nodeID); err != nil {
		// release lock failed, no need to modify state, wait to retry
		log.Ctx(ib.ctx).Warn("index builder try to release reference lock failed", zap.Int64("buildID", buildID),
			zap.Int64("nodeID", nodeID), zap.Error(err))
		return err
	}
	if err := ib.meta.ResetNodeID(buildID); err != nil {
		log.Ctx(ib.ctx).Warn("index builder try to reset nodeID failed", zap.Error(err))
		return err
	}
	log.Ctx(ib.ctx).Info("release segment reference lock and reset nodeID success", zap.Int64("buildID", buildID),
		zap.Int64("nodeID", nodeID))
	return nil
}

func (ib *indexBuilder) releaseLockAndResetTask(buildID UniqueID, nodeID UniqueID) error {
	if nodeID != 0 {
		if err := ib.ic.tryReleaseSegmentReferLock(ib.ctx, buildID, nodeID); err != nil {
			// release lock failed, no need to modify state, wait to retry
			log.Ctx(ib.ctx).Warn("index builder try to release reference lock failed", zap.Error(err))
			return err
		}
	}
	if err := ib.meta.ResetMeta(buildID); err != nil {
		log.Ctx(ib.ctx).Warn("index builder try to reset task failed", zap.Error(err))
		return err
	}
	log.Ctx(ib.ctx).Info("release segment reference lock and reset task success", zap.Int64("buildID", buildID),
		zap.Int64("nodeID", nodeID))
	return nil
}

func (ib *indexBuilder) nodeDown(nodeID UniqueID) {
	defer ib.notify()

	metas := ib.meta.GetMetasByNodeID(nodeID)

	ib.taskMutex.Lock()
	defer ib.taskMutex.Unlock()

	for _, meta := range metas {
		if ib.tasks[meta.BuildID] != indexTaskDone {
			ib.tasks[meta.BuildID] = indexTaskRetry
		}
	}
}
