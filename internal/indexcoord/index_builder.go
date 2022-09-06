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
	"sync"
	"time"

	queryPb "github.com/milvus-io/milvus/internal/proto/querypb"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/datapb"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
)

type indexBuilder struct {
	ctx    context.Context
	cancel context.CancelFunc

	wg               sync.WaitGroup
	taskMutex        sync.RWMutex
	scheduleDuration time.Duration

	// TODO @xiaocai2333: use priority queue
	tasks  map[int64]*indexTask
	notify chan struct{}

	ic *IndexCoord

	meta *metaTable
}

type indexTask struct {
	buildID     UniqueID
	state       indexTaskState
	segmentInfo *datapb.SegmentInfo
}

func newIndexBuilder(ctx context.Context, ic *IndexCoord, metaTable *metaTable, aliveNodes []UniqueID) *indexBuilder {
	ctx, cancel := context.WithCancel(ctx)

	ib := &indexBuilder{
		ctx:              ctx,
		cancel:           cancel,
		meta:             metaTable,
		ic:               ic,
		tasks:            make(map[int64]*indexTask, 1024),
		notify:           make(chan struct{}, 1),
		scheduleDuration: time.Second,
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
	close(ib.notify)
	ib.wg.Wait()
}

func (ib *indexBuilder) reloadFromKV(aliveNodes []UniqueID) {
	metas := ib.meta.GetAllIndexMeta()
	for buildID, indexMeta := range metas {
		// deleted, need to release lock and clean meta
		if indexMeta.IsDeleted || ib.meta.IsIndexDeleted(indexMeta.CollectionID, indexMeta.IndexID) {
			if indexMeta.NodeID != 0 {
				ib.tasks[buildID] = &indexTask{
					buildID:     buildID,
					state:       indexTaskDeleted,
					segmentInfo: nil,
				}
			}
		} else if indexMeta.IndexState == commonpb.IndexState_Unissued && indexMeta.NodeID == 0 {
			// unissued, need to acquire lock and assign task
			ib.tasks[buildID] = &indexTask{
				buildID:     buildID,
				state:       indexTaskInit,
				segmentInfo: nil,
			}
		} else if indexMeta.IndexState == commonpb.IndexState_Unissued && indexMeta.NodeID != 0 {
			// retry, need to release lock and reassign task
			// need to release reference lock
			ib.tasks[buildID] = &indexTask{
				buildID:     buildID,
				state:       indexTaskRetry,
				segmentInfo: nil,
			}
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
				ib.tasks[buildID] = &indexTask{
					buildID:     buildID,
					state:       indexTaskRetry,
					segmentInfo: nil,
				}
			} else {
				// in_progress, nothing to do
				ib.tasks[buildID] = &indexTask{
					buildID:     buildID,
					state:       indexTaskInProgress,
					segmentInfo: nil,
				}
			}
		} else if indexMeta.IndexState == commonpb.IndexState_Finished || indexMeta.IndexState == commonpb.IndexState_Failed {
			if indexMeta.NodeID != 0 {
				// task is done, but the lock has not been released, need to release.
				ib.tasks[buildID] = &indexTask{
					buildID:     buildID,
					state:       indexTaskDone,
					segmentInfo: nil,
				}
			} else if !indexMeta.NotifyHandoff {
				ib.tasks[buildID] = &indexTask{
					buildID:     buildID,
					state:       indexTaskHandoff,
					segmentInfo: nil,
				}
			}
			// else: task is done, and lock has been released, no need to add to index builder.
		}
	}
}

func (ib *indexBuilder) enqueue(buildID UniqueID) {
	// notify
	ib.taskMutex.Lock()
	defer ib.taskMutex.Unlock()
	ib.tasks[buildID] = &indexTask{
		buildID:     buildID,
		state:       indexTaskInit,
		segmentInfo: nil,
	}
	select {
	case ib.notify <- struct{}{}:
	default:
	}
}

func (ib *indexBuilder) schedule() {
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
		case _, ok := <-ib.notify:
			if ok {
				ib.taskMutex.Lock()
				if len(ib.tasks) > 0 {
					log.Info("index builder task schedule", zap.Int("task num", len(ib.tasks)))
					for _, t := range ib.tasks {
						ib.process(t)
					}
				}
				ib.taskMutex.Unlock()
			}
		// !ok means indexBuilder is closed.
		case <-ticker.C:
			ib.taskMutex.Lock()
			if len(ib.tasks) > 0 {
				log.Info("index builder task schedule", zap.Int("task num", len(ib.tasks)))
				for _, t := range ib.tasks {
					ib.process(t)
				}
			}
			ib.taskMutex.Unlock()
		}
	}
}

func (ib *indexBuilder) process(t *indexTask) {
	state := t.state
	log.Info("index task is processing", zap.Int64("buildID", t.buildID), zap.String("task state", state.String()))
	meta, exist := ib.meta.GetMeta(t.buildID)
	if !exist {
		delete(ib.tasks, t.buildID)
		return
	}
	if err := ib.getSegmentInfo(t.buildID, meta.SegmentID); err != nil {
		log.Error("IndexCoord get segmentInfo from dataCoord fail", zap.Int64("buildID", t.buildID),
			zap.Int64("segmentID", meta.SegmentID), zap.Error(err))
		return
	}

	switch state {
	case indexTaskInit:
		if !ib.meta.NeedIndex(meta.CollectionID, meta.IndexID) {
			delete(ib.tasks, t.buildID)
			return
		}
		log.Debug("task state is init, build index ...", zap.Int64("buildID", t.buildID))
		if t.segmentInfo.NumOfRows < Params.IndexCoordCfg.MinSegmentNumRowsToEnableIndex {
			log.Debug("index task no need to build index, too few rows", zap.Int64("buildID", t.buildID),
				zap.Int64("segID", t.segmentInfo.ID), zap.Int64("num rows", t.segmentInfo.NumOfRows))
			ib.tasks[t.buildID].state = indexTaskHandoff
			return
		}
		// peek client
		// if all IndexNodes are executing task, wait for one of them to finish the task.
		nodeID, client := ib.ic.nodeManager.PeekClient(meta)
		if client == nil {
			log.RatedDebug(30, "index builder peek client error, there is no available")
			return
		}
		// update version and set nodeID
		if err := ib.meta.UpdateVersion(t.buildID, nodeID); err != nil {
			log.Error("index builder update index version failed", zap.Int64("build", t.buildID), zap.Error(err))
			return
		}

		// acquire lock
		if err := ib.ic.tryAcquireSegmentReferLock(ib.ctx, t.buildID, nodeID, []UniqueID{meta.SegmentID}); err != nil {
			log.Error("index builder acquire segment reference lock failed", zap.Int64("buildID", t.buildID),
				zap.Int64("nodeID", nodeID), zap.Error(err))
			ib.tasks[t.buildID].state = indexTaskRetry
			return
		}

		binLogs := make([]string, 0)
		fieldID := ib.meta.GetFieldIDByIndexID(meta.CollectionID, meta.IndexID)
		for _, fieldBinLog := range t.segmentInfo.GetBinlogs() {
			if fieldBinLog.GetFieldID() == fieldID {
				for _, binLog := range fieldBinLog.GetBinlogs() {
					binLogs = append(binLogs, binLog.LogPath)
				}
				break
			}
		}

		typeParams := ib.meta.GetTypeParams(meta.CollectionID, meta.IndexID)
		indexParams := ib.meta.GetIndexParams(meta.CollectionID, meta.IndexID)

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
			}
		}
		req := &indexpb.CreateJobRequest{
			ClusterID:       Params.CommonCfg.ClusterPrefix,
			IndexFilePrefix: path.Join(ib.ic.chunkManager.RootPath(), common.SegmentIndexPath),
			BuildID:         t.buildID,
			DataPaths:       binLogs,
			IndexVersion:    meta.IndexVersion + 1,
			StorageConfig:   storageConfig,
			IndexParams:     indexParams,
			TypeParams:      typeParams,
			NumRows:         meta.NumRows,
		}
		log.Debug("assign task to indexNode", zap.Int64("buildID", t.buildID), zap.Int64("nodeID", nodeID))
		if err := ib.ic.assignTask(client, req); err != nil {
			// need to release lock then reassign, so set task state to retry
			log.Error("index builder assign task to IndexNode failed", zap.Int64("buildID", t.buildID),
				zap.Int64("nodeID", nodeID), zap.Error(err))
			ib.tasks[t.buildID].state = indexTaskRetry
			return
		}
		// update index meta state to InProgress
		if err := ib.meta.BuildIndex(t.buildID); err != nil {
			// need to release lock then reassign, so set task state to retry
			log.Error("index builder update index meta to InProgress failed", zap.Int64("buildID", t.buildID),
				zap.Int64("nodeID", nodeID), zap.Error(err))
			ib.tasks[t.buildID].state = indexTaskRetry
			return
		}
		log.Debug("index task assigned success", zap.Int64("buildID", t.buildID), zap.Int64("nodeID", nodeID))
		ib.tasks[t.buildID].state = indexTaskInProgress

	case indexTaskDone:
		log.Debug("index task has done", zap.Int64("buildID", t.buildID))
		if !ib.meta.NeedIndex(meta.CollectionID, meta.IndexID) {
			ib.tasks[t.buildID].state = indexTaskDeleted
			return
		}

		if !ib.dropIndexTask(t.buildID, meta.NodeID) {
			return
		}
		if err := ib.releaseLockAndResetNode(t.buildID, meta.NodeID); err != nil {
			// release lock failed, no need to modify state, wait to retry
			log.Error("index builder try to release reference lock failed", zap.Error(err))
			return
		}
		log.Debug("index builder complete index task, wait to write handoff event", zap.Int64("buildID", t.buildID))
		ib.tasks[t.buildID].state = indexTaskHandoff
	case indexTaskRetry:
		log.Debug("index task state is retry, try to release reference lock", zap.Int64("buildID", t.buildID))
		if !ib.meta.NeedIndex(meta.CollectionID, meta.IndexID) {
			ib.tasks[t.buildID].state = indexTaskDeleted
			return
		}
		if err := ib.releaseLockAndResetTask(t.buildID, meta.NodeID); err != nil {
			// release lock failed, no need to modify state, wait to retry
			log.Error("index builder try to release reference lock failed", zap.Error(err))
			return
		}
		ib.tasks[t.buildID].state = indexTaskInit

	case indexTaskDeleted:
		log.Debug("index task state is deleted, try to release reference lock", zap.Int64("buildID", t.buildID))

		if meta.NodeID != 0 {
			if !ib.dropIndexTask(t.buildID, meta.NodeID) {
				return
			}
			if err := ib.releaseLockAndResetNode(t.buildID, meta.NodeID); err != nil {
				// release lock failed, no need to modify state, wait to retry
				log.Error("index builder try to release reference lock failed", zap.Error(err))
				return
			}
		}
		// reset nodeID success, remove task.
		delete(ib.tasks, t.buildID)
	case indexTaskHandoff:
		if meta.NumRows < Params.IndexCoordCfg.MinSegmentNumRowsToEnableIndex {
			handoffInfo := &queryPb.SegmentInfo{
				SegmentID:           t.segmentInfo.ID,
				CollectionID:        t.segmentInfo.CollectionID,
				PartitionID:         t.segmentInfo.PartitionID,
				NumRows:             t.segmentInfo.NumOfRows,
				CompactionFrom:      t.segmentInfo.CompactionFrom,
				CreatedByCompaction: t.segmentInfo.CreatedByCompaction,
				SegmentState:        t.segmentInfo.State,
				EnableIndex:         false,
			}
			if err := ib.ic.writeHandoffSegment(handoffInfo); err != nil {
				log.Error("index builder write handoff task fail", zap.Int64("segID", t.segmentInfo.ID),
					zap.Int64("buildID", t.buildID), zap.Error(err))
				return
			}
			delete(ib.tasks, t.buildID)
			return
		}
		filePathInfo, err := ib.meta.GetIndexFilePathInfo(meta.SegmentID, meta.IndexID)
		if err != nil {
			log.Warn("IndexCoord get index file path fail", zap.Int64("collID", t.segmentInfo.CollectionID),
				zap.Int64("partID", t.segmentInfo.PartitionID), zap.Int64("segID", t.segmentInfo.ID), zap.Error(err))
			return
		}
		handoffInfo := &queryPb.SegmentInfo{
			SegmentID:           t.segmentInfo.ID,
			CollectionID:        t.segmentInfo.CollectionID,
			PartitionID:         t.segmentInfo.PartitionID,
			NumRows:             t.segmentInfo.NumOfRows,
			IndexName:           ib.meta.GetIndexNameByID(meta.CollectionID, meta.IndexID),
			IndexID:             meta.IndexID,
			CompactionFrom:      t.segmentInfo.CompactionFrom,
			CreatedByCompaction: t.segmentInfo.CreatedByCompaction,
			SegmentState:        t.segmentInfo.State,
			IndexInfos: []*queryPb.FieldIndexInfo{
				{
					FieldID:        ib.meta.GetFieldIDByIndexID(t.segmentInfo.CollectionID, meta.IndexID),
					EnableIndex:    true,
					IndexName:      ib.meta.GetIndexNameByID(meta.CollectionID, meta.IndexID),
					IndexID:        meta.IndexID,
					BuildID:        t.buildID,
					IndexParams:    ib.meta.GetIndexParams(meta.CollectionID, meta.IndexID),
					IndexFilePaths: filePathInfo.IndexFilePaths[:],
					IndexSize:      int64(filePathInfo.SerializedSize),
				},
			},
			EnableIndex: true,
		}
		if err = ib.ic.writeHandoffSegment(handoffInfo); err != nil {
			log.Error("index builder write handoff task fail", zap.Int64("segID", t.segmentInfo.ID),
				zap.Int64("buildID", t.buildID), zap.Error(err))
			return
		}
		delete(ib.tasks, t.buildID)
		return
	default:
		log.Debug("index task is in progress", zap.Int64("buildID", t.buildID),
			zap.String("state", meta.IndexState.String()))
		if !exist || !ib.meta.NeedIndex(meta.CollectionID, meta.IndexID) {
			ib.tasks[t.buildID].state = indexTaskDeleted
			return
		}
		ib.tasks[t.buildID].state = ib.getTaskState(t.buildID, meta.NodeID)
	}
}

func (ib *indexBuilder) getTaskState(buildID, nodeID UniqueID) indexTaskState {
	log.Info("IndexCoord indexBuilder get index task state", zap.Int64("buildID", buildID), zap.Int64("nodeID", nodeID))
	client, exist := ib.ic.nodeManager.GetClientByID(nodeID)
	if exist {
		response, err := client.QueryJobs(ib.ctx, &indexpb.QueryJobsRequest{
			ClusterID: Params.CommonCfg.ClusterPrefix,
			BuildIDs:  []int64{buildID},
		})
		if err != nil {
			log.Error("IndexCoord get jobs info from IndexNode fail", zap.Int64("nodeID", nodeID),
				zap.Error(err))
			return indexTaskInProgress
		}
		if response.Status.ErrorCode != commonpb.ErrorCode_Success {
			log.Error("IndexCoord get jobs info from IndexNode fail", zap.Int64("nodeID", nodeID),
				zap.Int64("buildID", buildID), zap.String("fail reason", response.Status.Reason))
			return indexTaskInProgress
		}

		// indexInfos length is always one.
		for _, info := range response.IndexInfos {
			if info.State == commonpb.IndexState_Failed || info.State == commonpb.IndexState_Finished {
				log.Info("this task has been finished", zap.Int64("buildID", info.BuildID),
					zap.String("index state", info.State.String()))
				if err := ib.meta.FinishTask(info); err != nil {
					log.Error("IndexCoord update index state fail", zap.Int64("buildID", info.BuildID),
						zap.String("index state", info.State.String()), zap.Error(err))
					return indexTaskInProgress
				}
				return indexTaskDone
			} else if info.State == commonpb.IndexState_Retry {
				log.Info("this task should be retry", zap.Int64("buildID", buildID))
				return indexTaskRetry
			}
			return indexTaskInProgress
		}
	}
	// !exist --> node down
	return indexTaskInProgress
}

func (ib *indexBuilder) getSegmentInfo(buildID, segmentID UniqueID) error {
	if ib.tasks[buildID].segmentInfo != nil {
		return nil
	}
	segmentsInfo, err := ib.ic.dataCoordClient.GetSegmentInfo(ib.ctx, &datapb.GetSegmentInfoRequest{
		SegmentIDs:       []UniqueID{segmentID},
		IncludeUnHealthy: false,
	})

	if err != nil {
		return err
	}
	if segmentsInfo.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(segmentsInfo.Status.GetReason())
	}
	for _, segInfo := range segmentsInfo.Infos {
		if segInfo.ID == segmentID {
			ib.tasks[buildID].segmentInfo = segInfo
			break
		}
	}
	return nil
}

func (ib *indexBuilder) dropIndexTask(buildID, nodeID UniqueID) bool {
	log.Info("IndexCoord notify IndexNode drop the index task", zap.Int64("buildID", buildID), zap.Int64("nodeID", nodeID))
	client, exist := ib.ic.nodeManager.GetClientByID(nodeID)
	if exist {
		status, err := client.DropJobs(ib.ctx, &indexpb.DropJobsRequest{
			ClusterID: Params.CommonCfg.ClusterPrefix,
			BuildIDs:  []UniqueID{buildID},
		})
		if err != nil {
			log.Warn("IndexCoord notify IndexNode drop the index task fail", zap.Int64("buildID", buildID),
				zap.Int64("nodeID", nodeID), zap.Error(err))
			return false
		}
		if status.ErrorCode != commonpb.ErrorCode_Success {
			log.Warn("IndexCoord notify IndexNode drop the index task fail", zap.Int64("buildID", buildID),
				zap.Int64("nodeID", nodeID), zap.String("fail reason", status.Reason))
			return false
		}
		return true
	}
	return true
}

func (ib *indexBuilder) releaseLockAndResetNode(buildID UniqueID, nodeID UniqueID) error {
	log.Info("release segment reference lock and reset nodeID", zap.Int64("buildID", buildID),
		zap.Int64("nodeID", nodeID))
	if err := ib.ic.tryReleaseSegmentReferLock(ib.ctx, buildID, nodeID); err != nil {
		// release lock failed, no need to modify state, wait to retry
		log.Error("index builder try to release reference lock failed", zap.Error(err))
		return err
	}
	if err := ib.meta.ResetNodeID(buildID); err != nil {
		log.Error("index builder try to reset nodeID failed", zap.Error(err))
		return err
	}
	log.Info("release segment reference lock and reset nodeID success", zap.Int64("buildID", buildID),
		zap.Int64("nodeID", nodeID))
	return nil
}

func (ib *indexBuilder) releaseLockAndResetTask(buildID UniqueID, nodeID UniqueID) error {
	log.Info("release segment reference lock and reset task", zap.Int64("buildID", buildID),
		zap.Int64("nodeID", nodeID))
	if nodeID != 0 {
		if err := ib.ic.tryReleaseSegmentReferLock(ib.ctx, buildID, nodeID); err != nil {
			// release lock failed, no need to modify state, wait to retry
			log.Error("index builder try to release reference lock failed", zap.Error(err))
			return err
		}
	}
	if err := ib.meta.ResetMeta(buildID); err != nil {
		log.Error("index builder try to reset task failed", zap.Error(err))
		return err
	}
	log.Info("release segment reference lock and reset task success", zap.Int64("buildID", buildID),
		zap.Int64("nodeID", nodeID))
	return nil
}

func (ib *indexBuilder) markTasksAsDeleted(buildIDs []UniqueID) {
	ib.taskMutex.Lock()
	defer ib.taskMutex.Unlock()

	for _, buildID := range buildIDs {
		if _, ok := ib.tasks[buildID]; ok {
			ib.tasks[buildID].state = indexTaskDeleted
			log.Debug("index task has been deleted", zap.Int64("buildID", buildID))
		}
	}
}

func (ib *indexBuilder) nodeDown(nodeID UniqueID) {
	ib.taskMutex.Lock()
	defer ib.taskMutex.Unlock()
	metas := ib.meta.GetMetasByNodeID(nodeID)

	for _, meta := range metas {
		if ib.tasks[meta.BuildID].state != indexTaskDone {
			ib.tasks[meta.BuildID].state = indexTaskRetry
		}
	}
}

func (ib *indexBuilder) hasTask(buildID UniqueID) bool {
	ib.taskMutex.RLock()
	defer ib.taskMutex.RUnlock()

	_, ok := ib.tasks[buildID]
	return ok
}
