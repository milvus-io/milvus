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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
)

const (
	reqTimeoutInterval = time.Second * 10
)

type taskScheduler struct {
	sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	scheduleDuration time.Duration

	// TODO @xiaocai2333: use priority queue
	tasks      map[int64]Task
	notifyChan chan struct{}

	meta *meta

	policy                    buildIndexPolicy
	nodeManager               WorkerManager
	chunkManager              storage.ChunkManager
	indexEngineVersionManager IndexEngineVersionManager
	handler                   Handler
}

func newTaskScheduler(
	ctx context.Context,
	metaTable *meta, nodeManager WorkerManager,
	chunkManager storage.ChunkManager,
	indexEngineVersionManager IndexEngineVersionManager,
	handler Handler,
) *taskScheduler {
	ctx, cancel := context.WithCancel(ctx)

	ts := &taskScheduler{
		ctx:                       ctx,
		cancel:                    cancel,
		meta:                      metaTable,
		tasks:                     make(map[int64]Task),
		notifyChan:                make(chan struct{}, 1),
		scheduleDuration:          Params.DataCoordCfg.IndexTaskSchedulerInterval.GetAsDuration(time.Millisecond),
		policy:                    defaultBuildIndexPolicy,
		nodeManager:               nodeManager,
		chunkManager:              chunkManager,
		handler:                   handler,
		indexEngineVersionManager: indexEngineVersionManager,
	}
	ts.reloadFromKV()
	return ts
}

func (s *taskScheduler) Start() {
	s.wg.Add(1)
	go s.schedule()
}

func (s *taskScheduler) Stop() {
	s.cancel()
	s.wg.Wait()
}

func (s *taskScheduler) reloadFromKV() {
	segments := s.meta.GetAllSegmentsUnsafe()
	for _, segment := range segments {
		for _, segIndex := range s.meta.indexMeta.getSegmentIndexes(segment.ID) {
			if segIndex.IsDeleted {
				continue
			}
			if segIndex.IndexState != commonpb.IndexState_Finished && segIndex.IndexState != commonpb.IndexState_Failed {
				s.tasks[segIndex.BuildID] = &indexBuildTask{
					buildID: segIndex.BuildID,
					nodeID:  segIndex.NodeID,
					taskInfo: &indexpb.IndexTaskInfo{
						BuildID:    segIndex.BuildID,
						State:      segIndex.IndexState,
						FailReason: segIndex.FailReason,
					},
				}
			}
		}
	}

	allAnalyzeTasks := s.meta.analyzeMeta.GetAllTasks()
	for taskID, t := range allAnalyzeTasks {
		if t.State != indexpb.JobState_JobStateFinished && t.State != indexpb.JobState_JobStateFailed {
			s.tasks[taskID] = &analyzeTask{
				taskID: taskID,
				nodeID: t.NodeID,
				taskInfo: &indexpb.AnalyzeResult{
					TaskID:     taskID,
					State:      t.State,
					FailReason: t.FailReason,
				},
			}
		}
	}
}

// notify is an unblocked notify function
func (s *taskScheduler) notify() {
	select {
	case s.notifyChan <- struct{}{}:
	default:
	}
}

func (s *taskScheduler) enqueue(task Task) {
	defer s.notify()

	s.Lock()
	defer s.Unlock()
	taskID := task.GetTaskID()
	if _, ok := s.tasks[taskID]; !ok {
		s.tasks[taskID] = task
	}
	log.Info("taskScheduler enqueue task", zap.Int64("taskID", taskID))
}

func (s *taskScheduler) schedule() {
	// receive notifyChan
	// time ticker
	log.Ctx(s.ctx).Info("task scheduler loop start")
	defer s.wg.Done()
	ticker := time.NewTicker(s.scheduleDuration)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			log.Ctx(s.ctx).Warn("task scheduler ctx done")
			return
		case _, ok := <-s.notifyChan:
			if ok {
				s.run()
			}
			// !ok means indexBuild is closed.
		case <-ticker.C:
			s.run()
		}
	}
}

func (s *taskScheduler) getTask(taskID UniqueID) Task {
	s.RLock()
	defer s.RUnlock()

	return s.tasks[taskID]
}

func (s *taskScheduler) run() {
	// schedule policy
	s.RLock()
	taskIDs := make([]UniqueID, 0, len(s.tasks))
	for tID := range s.tasks {
		taskIDs = append(taskIDs, tID)
	}
	s.RUnlock()
	if len(taskIDs) > 0 {
		log.Ctx(s.ctx).Info("task scheduler", zap.Int("task num", len(taskIDs)))
	}

	s.policy(taskIDs)

	for _, taskID := range taskIDs {
		ok := s.process(taskID)
		if !ok {
			log.Ctx(s.ctx).Info("there is no idle indexing node, wait a minute...")
			break
		}
	}
}

func (s *taskScheduler) removeTask(taskID UniqueID) {
	s.Lock()
	defer s.Unlock()
	delete(s.tasks, taskID)
}

func (s *taskScheduler) process(taskID UniqueID) bool {
	task := s.getTask(taskID)

	if !task.CheckTaskHealthy(s.meta) {
		s.removeTask(taskID)
		return true
	}
	state := task.GetState()
	log.Ctx(s.ctx).Info("task is processing", zap.Int64("taskID", taskID),
		zap.String("state", state.String()))

	switch state {
	case indexpb.JobState_JobStateNone:
		s.removeTask(taskID)

	case indexpb.JobState_JobStateInit:
		// 1. pick an indexNode client
		nodeID, client := s.nodeManager.PickClient()
		if client == nil {
			log.Ctx(s.ctx).Debug("pick client failed")
			return false
		}
		log.Ctx(s.ctx).Info("pick client success", zap.Int64("taskID", taskID), zap.Int64("nodeID", nodeID))

		// 2. update version
		if err := task.UpdateVersion(s.ctx, s.meta); err != nil {
			log.Ctx(s.ctx).Warn("update task version failed", zap.Int64("taskID", taskID), zap.Error(err))
			return false
		}
		log.Ctx(s.ctx).Info("update task version success", zap.Int64("taskID", taskID))

		// 3. assign task to indexNode
		success, skip := task.AssignTask(s.ctx, client, s)
		if !success {
			log.Ctx(s.ctx).Warn("assign task to client failed", zap.Int64("taskID", taskID),
				zap.String("new state", task.GetState().String()), zap.String("fail reason", task.GetFailReason()))
			// If the problem is caused by the task itself, subsequent tasks will not be skipped.
			// If etcd fails or fails to send tasks to the node, the subsequent tasks will be skipped.
			return !skip
		}
		if skip {
			// create index for small segment(<1024), skip next steps.
			return true
		}
		log.Ctx(s.ctx).Info("assign task to client success", zap.Int64("taskID", taskID), zap.Int64("nodeID", nodeID))

		// 4. update meta state
		if err := task.UpdateMetaBuildingState(nodeID, s.meta); err != nil {
			log.Ctx(s.ctx).Warn("update meta building state failed", zap.Int64("taskID", taskID), zap.Error(err))
			task.SetState(indexpb.JobState_JobStateRetry, "update meta building state failed")
			return false
		}
		log.Ctx(s.ctx).Info("update task meta state to InProgress success", zap.Int64("taskID", taskID),
			zap.Int64("nodeID", nodeID))
	case indexpb.JobState_JobStateFinished, indexpb.JobState_JobStateFailed:
		if err := task.SetJobInfo(s.meta); err != nil {
			log.Ctx(s.ctx).Warn("update task info failed", zap.Error(err))
			return true
		}
		client, exist := s.nodeManager.GetClientByID(task.GetNodeID())
		if exist {
			if !task.DropTaskOnWorker(s.ctx, client) {
				return true
			}
		}
		s.removeTask(taskID)
	case indexpb.JobState_JobStateRetry:
		client, exist := s.nodeManager.GetClientByID(task.GetNodeID())
		if exist {
			if !task.DropTaskOnWorker(s.ctx, client) {
				return true
			}
		}
		task.SetState(indexpb.JobState_JobStateInit, "")
		task.ResetNodeID()

	default:
		// state: in_progress
		client, exist := s.nodeManager.GetClientByID(task.GetNodeID())
		if exist {
			task.QueryResult(s.ctx, client)
			return true
		}
		task.SetState(indexpb.JobState_JobStateRetry, "")
	}
	return true
}
