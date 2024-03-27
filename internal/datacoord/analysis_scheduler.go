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
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type analysisTaskScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc

	wg               sync.WaitGroup
	lock             sync.RWMutex
	scheduleDuration time.Duration

	meta         *meta
	analysisMeta *analysisMeta

	tasks       map[int64]taskState
	notifyChan  chan struct{}
	nodeManager WorkerManager
}

func newAnalysisTaskScheduler(
	ctx context.Context,
	metaTable *meta, at *analysisMeta, nodeManager WorkerManager,
) *analysisTaskScheduler {
	ctx, cancel := context.WithCancel(ctx)

	ats := &analysisTaskScheduler{
		ctx:              ctx,
		cancel:           cancel,
		meta:             metaTable,
		analysisMeta:     at,
		tasks:            make(map[int64]taskState),
		notifyChan:       make(chan struct{}, 1),
		scheduleDuration: Params.DataCoordCfg.IndexTaskSchedulerInterval.GetAsDuration(time.Millisecond),
		nodeManager:      nodeManager,
	}
	ats.reloadFromMeta()
	log.Info("new analysis task scheduler success")
	return ats
}

func (ats *analysisTaskScheduler) reloadFromMeta() {
	ats.lock.Lock()
	defer ats.lock.Unlock()

	allTasks := ats.analysisMeta.GetAllTasks()
	for taskID, t := range allTasks {
		if t.State == commonpb.IndexState_Finished || t.State == commonpb.IndexState_Failed {
			continue
		} else if t.State == commonpb.IndexState_InProgress {
			ats.tasks[taskID] = taskInProgress
		} else {
			ats.tasks[taskID] = taskInit
		}
	}
}

func (ats *analysisTaskScheduler) Start() {
	ats.wg.Add(1)
	go ats.Schedule()
}

func (ats *analysisTaskScheduler) Stop() {
	ats.cancel()
	ats.wg.Wait()
}

func (ats *analysisTaskScheduler) Schedule() {
	log.Ctx(ats.ctx).Info("analysis task scheduler loop start")
	defer ats.wg.Done()
	ticker := time.NewTicker(ats.scheduleDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ats.ctx.Done():
			log.Ctx(ats.ctx).Warn("analysis task scheduler ctx done")
			return
		case _, ok := <-ats.notifyChan:
			if ok {
				ats.run()
			}
		case <-ticker.C:
			ats.run()
		}
	}
}

func (ats *analysisTaskScheduler) enqueue(taskID int64) {
	defer func() {
		ats.notifyChan <- struct{}{}
	}()

	ats.lock.Lock()
	defer ats.lock.Unlock()
	if _, ok := ats.tasks[taskID]; !ok {
		ats.tasks[taskID] = taskInit
	}
	log.Info("analyze task enqueue successfully", zap.Int64("taskID", taskID))
}

func (ats *analysisTaskScheduler) run() {
	ats.lock.RLock()
	taskIDs := lo.Keys(ats.tasks)
	ats.lock.RUnlock()

	if len(taskIDs) > 0 {
		log.Ctx(ats.ctx).Info("analyze task scheduler start", zap.Int("analyze task num", len(taskIDs)))
	}
	for _, taskID := range taskIDs {
		if !ats.process(taskID) {
			log.Ctx(ats.ctx).Info("there is no idle indexing node, wait a minute...")
			break
		}
	}
}

func (ats *analysisTaskScheduler) getTaskState(taskID int64) taskState {
	ats.lock.RLock()
	defer ats.lock.RUnlock()

	return ats.tasks[taskID]
}

func (ats *analysisTaskScheduler) updateTaskState(taskID int64, state taskState) {
	ats.lock.Lock()
	defer ats.lock.Unlock()

	ats.tasks[taskID] = state
}

func (ats *analysisTaskScheduler) deleteTask(taskID int64) {
	ats.lock.Lock()
	defer ats.lock.Unlock()

	delete(ats.tasks, taskID)
}

func (ats *analysisTaskScheduler) process(taskID int64) bool {
	state := ats.getTaskState(taskID)
	t := ats.analysisMeta.GetTask(taskID)
	if t == nil {
		log.Ctx(ats.ctx).Info("task is nil, delete it", zap.Int64("taskID", taskID))
		ats.deleteTask(taskID)
		return true
	}
	log.Ctx(ats.ctx).Info("process task", zap.Int64("taskID", taskID), zap.String("state", state.String()))

	switch state {
	case taskInit:
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
		req := &indexpb.AnalysisRequest{
			ClusterID:     Params.CommonCfg.ClusterPrefix.GetValue(),
			TaskID:        taskID,
			CollectionID:  t.CollectionID,
			PartitionID:   t.PartitionID,
			FieldID:       t.FieldID,
			FieldName:     t.FieldName,
			FieldType:     t.FieldType,
			Dim:           t.Dim,
			SegmentStats:  make(map[int64]*indexpb.SegmentStats),
			Version:       t.Version,
			StorageConfig: storageConfig,
		}

		// When data analysis occurs, segments must not be discarded. Such as compaction, GC, etc.
		segments := ats.meta.SelectSegments(func(info *SegmentInfo) bool {
			return isSegmentHealthy(info) && slices.Contains(t.SegmentIDs, info.ID)
		})
		segmentsMap := lo.SliceToMap(segments, func(t *SegmentInfo) (int64, *SegmentInfo) {
			return t.ID, t
		})
		for _, segID := range t.SegmentIDs {
			info := segmentsMap[segID]
			if info == nil {
				log.Warn("analyze stats task is processing, but segment is nil, delete the task", zap.Int64("taskID", taskID),
					zap.Int64("segmentID", segID))
				ats.deleteTask(taskID)
				return true
			}

			// get binlogIDs
			binlogIDs := getBinLogIds(info, t.FieldID)

			req.SegmentStats[segID] = &indexpb.SegmentStats{
				ID:      segID,
				NumRows: info.GetNumOfRows(),
				LogIDs:  binlogIDs,
			}
		}

		// 1. update task version
		if err := ats.analysisMeta.UpdateVersion(taskID); err != nil {
			log.Warn("update task version failed", zap.Int64("taskID", taskID), zap.Error(err))
			return false
		}

		assignFunc := func(nodeID int64, client types.IndexNodeClient) error {
			if err := ats.analysisMeta.BuildingTask(taskID, nodeID); err != nil {
				log.Warn("set task building state failed", zap.Int64("taskID", taskID), zap.Int64("nodeID", nodeID),
					zap.Error(err))
				return err
			}
			if err := ats.assignTask(client, req); err != nil {
				log.Ctx(ats.ctx).Warn("assign analysis task to indexNode failed", zap.Int64("taskID", taskID),
					zap.Int64("nodeID", nodeID), zap.Error(err))
				ats.updateTaskState(taskID, taskRetry)
				return err
			}
			log.Ctx(ats.ctx).Info("analysis task assigned successfully", zap.Int64("taskID", taskID),
				zap.Int64("nodeID", nodeID))
			return nil
		}

		if err := ats.nodeManager.SelectNodeAndAssignTask(assignFunc); err != nil {
			log.Ctx(ats.ctx).Info("nodeManager select node or assign task failed", zap.Error(err))
			return false
		}
		ats.updateTaskState(taskID, taskInProgress)
	case taskRetry:
		if !ats.dropIndexTask(taskID, t.NodeID) {
			return true
		}
		ats.updateTaskState(taskID, taskInit)
	case taskDone:
		if !ats.dropIndexTask(taskID, t.NodeID) {
			return true
		}
		ats.deleteTask(taskID)
	default:
		// taskInProgress
		ats.updateTaskState(taskID, ats.getTaskResult(taskID, t.NodeID))
	}
	return true
}

func (ats *analysisTaskScheduler) assignTask(builderClient types.IndexNodeClient, req *indexpb.AnalysisRequest) error {
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()
	resp, err := builderClient.Analysis(ctx, req)
	if err == nil {
		err = merr.Error(resp)
	}

	return err
}

func (ats *analysisTaskScheduler) dropIndexTask(taskID, nodeID UniqueID) bool {
	client, exist := ats.nodeManager.GetClientByID(nodeID)
	if exist {
		ctx1, cancel := context.WithTimeout(ats.ctx, reqTimeoutInterval)
		defer cancel()
		status, err := client.DropAnalysisTasks(ctx1, &indexpb.DropAnalysisTasksRequest{
			ClusterID: Params.CommonCfg.ClusterPrefix.GetValue(),
			TaskIDs:   []UniqueID{taskID},
		})
		if err == nil {
			err = merr.Error(status)
		}
		if err != nil {
			log.Ctx(ats.ctx).Warn("indexNode drop the analysis  task failed",
				zap.Int64("taskID", taskID), zap.Int64("nodeID", nodeID), zap.Error(err))
			return false
		}
		log.Ctx(ats.ctx).Info("indexNode drop the analysis task success",
			zap.Int64("taskID", taskID), zap.Int64("nodeID", nodeID))
		return true
	}
	log.Ctx(ats.ctx).Info("IndexNode no longer exist, no need to drop analysis task",
		zap.Int64("taskID", taskID), zap.Int64("nodeID", nodeID))
	return true
}

func (ats *analysisTaskScheduler) getTaskResult(taskID, nodeID int64) taskState {
	client, exist := ats.nodeManager.GetClientByID(nodeID)
	if exist {
		ctx1, cancel := context.WithTimeout(ats.ctx, reqTimeoutInterval)
		defer cancel()
		response, err := client.QueryAnalysisResult(ctx1, &indexpb.QueryAnalysisResultRequest{
			ClusterID: Params.CommonCfg.ClusterPrefix.GetValue(),
			TaskIDs:   []int64{taskID},
		})
		if err == nil {
			err = merr.Error(response.GetStatus())
		}
		if err != nil {
			log.Ctx(ats.ctx).Warn("get analysis task result from IndexNode failed",
				zap.Int64("taskID", taskID), zap.Int64("nodeID", nodeID), zap.Error(err))
			return taskRetry
		}

		// indexInfos length is always one.
		result, ok := response.GetResults()[taskID]
		if !ok {
			log.Ctx(ats.ctx).Info("this analysis task should be retry, indexNode does not have this task",
				zap.Int64("taskID", taskID), zap.Int64("nodeID", nodeID))
			return taskRetry
		}
		if result.GetState() == commonpb.IndexState_Finished || result.GetState() == commonpb.IndexState_Failed {
			log.Ctx(ats.ctx).Info("this analysis task has been finished",
				zap.Int64("taskID", taskID), zap.String("state", result.GetState().String()))
			if err := ats.analysisMeta.FinishTask(taskID, result); err != nil {
				log.Ctx(ats.ctx).Warn("update analysis task state fail",
					zap.Int64("taskID", taskID),
					zap.String("state", result.GetState().String()), zap.Error(err))
				return taskInProgress
			}
			return taskDone
		} else if result.GetState() == commonpb.IndexState_Retry || result.GetState() == commonpb.IndexState_IndexStateNone {
			log.Ctx(ats.ctx).Info("this analysis task should be retry", zap.Int64("taskID", taskID),
				zap.String("state", result.GetState().String()), zap.String("fail reason", result.GetFailReason()))
			return taskRetry
		}
		return taskInProgress
	}
	// !exist --> node down
	log.Ctx(ats.ctx).Info("this analysis task should be retry, indexNode is no longer exist",
		zap.Int64("taskID", taskID), zap.Int64("nodeID", nodeID))
	return taskRetry
}
