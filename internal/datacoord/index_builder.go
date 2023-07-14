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

	"github.com/cockroachdb/errors"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
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
	// task has been deleted.
	indexTaskDeleted
	// task needs to prepare segment info on IndexNode
	indexTaskPrepare

	reqTimeoutInterval = time.Second * 10
)

var TaskStateNames = map[indexTaskState]string{
	0: "Init",
	1: "InProgress",
	2: "Done",
	3: "Retry",
	4: "Deleted",
	5: "Prepare",
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
	taskMutex        sync.RWMutex
	scheduleDuration time.Duration

	// TODO @xiaocai2333: use priority queue
	tasks      map[int64]indexTaskState
	notifyChan chan struct{}

	meta *meta

	policy       buildIndexPolicy
	nodeManager  *IndexNodeManager
	chunkManager storage.ChunkManager
}

func newIndexBuilder(ctx context.Context, metaTable *meta, nodeManager *IndexNodeManager, chunkManager storage.ChunkManager) *indexBuilder {
	ctx, cancel := context.WithCancel(ctx)

	ib := &indexBuilder{
		ctx:              ctx,
		cancel:           cancel,
		meta:             metaTable,
		tasks:            make(map[int64]indexTaskState),
		notifyChan:       make(chan struct{}, 1),
		scheduleDuration: Params.DataCoordCfg.IndexTaskSchedulerInterval.GetAsDuration(time.Millisecond),
		policy:           defaultBuildIndexPolicy,
		nodeManager:      nodeManager,
		chunkManager:     chunkManager,
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
		for _, segIndex := range segment.segmentIndexes {
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

	meta, exist := ib.meta.GetIndexJob(buildID)
	if !exist {
		log.Ctx(ib.ctx).Debug("index task has not exist in meta table, remove task", zap.Int64("buildID", buildID))
		deleteFunc(buildID)
		return true
	}

	switch state {
	case indexTaskInit:
		segment := ib.meta.GetSegment(meta.SegmentID)
		if !isSegmentHealthy(segment) || !ib.meta.IsIndexExist(meta.CollectionID, meta.IndexID) {
			log.Ctx(ib.ctx).Info("task is no need to build index, remove it", zap.Int64("buildID", buildID))
			deleteFunc(buildID)
			return true
		}
		indexParams := ib.meta.GetIndexParams(meta.CollectionID, meta.IndexID)
		if isFlatIndex(getIndexType(indexParams)) || meta.NumRows < Params.DataCoordCfg.MinSegmentNumRowsToEnableIndex.GetAsInt64() {
			log.Ctx(ib.ctx).Debug("segment does not need index really", zap.Int64("buildID", buildID),
				zap.Int64("segmentID", meta.SegmentID), zap.Int64("num rows", meta.NumRows))
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
		nodeID, client := ib.nodeManager.PeekClient(meta)
		if client == nil {
			log.Ctx(ib.ctx).WithRateGroup("dc.indexBuilder", 1, 60).RatedInfo(5, "index builder peek client error, there is no available")
			return false
		}
		// update version and set nodeID
		if err := ib.meta.UpdateVersion(buildID, nodeID); err != nil {
			log.Ctx(ib.ctx).Warn("index builder update index version failed", zap.Int64("build", buildID), zap.Error(err))
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
				Address:         Params.MinioCfg.Address.GetValue(),
				AccessKeyID:     Params.MinioCfg.AccessKeyID.GetValue(),
				SecretAccessKey: Params.MinioCfg.SecretAccessKey.GetValue(),
				UseSSL:          Params.MinioCfg.UseSSL.GetAsBool(),
				BucketName:      Params.MinioCfg.BucketName.GetValue(),
				RootPath:        Params.MinioCfg.RootPath.GetValue(),
				UseIAM:          Params.MinioCfg.UseIAM.GetAsBool(),
				IAMEndpoint:     Params.MinioCfg.IAMEndpoint.GetValue(),
				StorageType:     Params.CommonCfg.StorageType.GetValue(),
			}
		}
		req := &indexpb.CreateJobRequest{
			ClusterID:       Params.CommonCfg.ClusterPrefix.GetValue(),
			IndexFilePrefix: path.Join(ib.chunkManager.RootPath(), common.SegmentIndexPath),
			BuildID:         buildID,
			DataPaths:       binLogs,
			IndexVersion:    meta.IndexVersion + 1,
			StorageConfig:   storageConfig,
			IndexParams:     indexParams,
			TypeParams:      typeParams,
			NumRows:         meta.NumRows,
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
		if err := ib.meta.BuildIndex(buildID); err != nil {
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

	case indexTaskDeleted:
		deleteFunc(buildID)

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

// assignTask sends the index task to the IndexNode, it has a timeout interval, if the IndexNode doesn't respond within
// the interval, it is considered that the task sending failed.
func (ib *indexBuilder) assignTask(builderClient types.IndexNode, req *indexpb.CreateJobRequest) error {
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()
	resp, err := builderClient.CreateJob(ctx, req)
	if err != nil {
		log.Error("IndexCoord assignmentTasksLoop builderClient.CreateIndex failed", zap.Error(err))
		return err
	}

	if resp.ErrorCode != commonpb.ErrorCode_Success {
		log.Error("IndexCoord assignmentTasksLoop builderClient.CreateIndex failed", zap.String("Reason", resp.Reason))
		return errors.New(resp.Reason)
	}
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
