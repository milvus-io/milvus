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
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/lock"
)

const (
	reqTimeoutInterval = time.Second * 10
)

type taskScheduler struct {
	sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	scheduleDuration       time.Duration
	collectMetricsDuration time.Duration

	// TODO @xiaocai2333: use priority queue
	tasks      map[int64]Task
	notifyChan chan struct{}
	taskLock   *lock.KeyLock[int64]

	meta *meta

	policy                    buildIndexPolicy
	nodeManager               session.WorkerManager
	chunkManager              storage.ChunkManager
	indexEngineVersionManager IndexEngineVersionManager
	handler                   Handler
	allocator                 allocator.Allocator
}

func newTaskScheduler(
	ctx context.Context,
	metaTable *meta, nodeManager session.WorkerManager,
	chunkManager storage.ChunkManager,
	indexEngineVersionManager IndexEngineVersionManager,
	handler Handler,
	allocator allocator.Allocator,
) *taskScheduler {
	ctx, cancel := context.WithCancel(ctx)

	ts := &taskScheduler{
		ctx:                       ctx,
		cancel:                    cancel,
		meta:                      metaTable,
		tasks:                     make(map[int64]Task),
		notifyChan:                make(chan struct{}, 1),
		taskLock:                  lock.NewKeyLock[int64](),
		scheduleDuration:          Params.DataCoordCfg.IndexTaskSchedulerInterval.GetAsDuration(time.Millisecond),
		collectMetricsDuration:    time.Minute,
		policy:                    defaultBuildIndexPolicy,
		nodeManager:               nodeManager,
		chunkManager:              chunkManager,
		handler:                   handler,
		indexEngineVersionManager: indexEngineVersionManager,
		allocator:                 allocator,
	}
	ts.reloadFromKV()
	return ts
}

func (s *taskScheduler) Start() {
	s.wg.Add(2)
	go s.schedule()
	go s.collectTaskMetrics()
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
					taskID: segIndex.BuildID,
					nodeID: segIndex.NodeID,
					taskInfo: &workerpb.IndexTaskInfo{
						BuildID:    segIndex.BuildID,
						State:      segIndex.IndexState,
						FailReason: segIndex.FailReason,
					},
					queueTime: time.Now(),
					startTime: time.Now(),
					endTime:   time.Now(),
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
				taskInfo: &workerpb.AnalyzeResult{
					TaskID:     taskID,
					State:      t.State,
					FailReason: t.FailReason,
				},
				queueTime: time.Now(),
				startTime: time.Now(),
				endTime:   time.Now(),
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
		task.SetQueueTime(time.Now())
		log.Info("taskScheduler enqueue task", zap.Int64("taskID", taskID))
	}
}

func (s *taskScheduler) AbortTask(taskID int64) {
	s.RLock()
	task, ok := s.tasks[taskID]
	s.RUnlock()
	if ok {
		s.taskLock.Lock(taskID)
		task.SetState(indexpb.JobState_JobStateFailed, "canceled")
		s.taskLock.Unlock(taskID)
	}
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
		s.taskLock.Lock(taskID)
		ok := s.process(taskID)
		if !ok {
			s.taskLock.Unlock(taskID)
			log.Ctx(s.ctx).Info("there is no idle indexing node, wait a minute...")
			break
		}
		s.taskLock.Unlock(taskID)
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
		zap.String("task type", task.GetTaskType()), zap.String("state", state.String()))

	switch state {
	case indexpb.JobState_JobStateNone:
		s.removeTask(taskID)

	case indexpb.JobState_JobStateInit:
		return s.processInit(task)
	case indexpb.JobState_JobStateFinished, indexpb.JobState_JobStateFailed:
		return s.processFinished(task)
	case indexpb.JobState_JobStateRetry:
		return s.processRetry(task)
	default:
		// state: in_progress
		return s.processInProgress(task)
	}
	return true
}

func (s *taskScheduler) collectTaskMetrics() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.collectMetricsDuration)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			log.Warn("task scheduler context done")
			return
		case <-ticker.C:
			s.RLock()
			taskIDs := make([]UniqueID, 0, len(s.tasks))
			for tID := range s.tasks {
				taskIDs = append(taskIDs, tID)
			}
			s.RUnlock()

			maxTaskQueueingTime := make(map[string]int64)
			maxTaskRunningTime := make(map[string]int64)

			collectMetricsFunc := func(taskID int64) {
				s.taskLock.Lock(taskID)
				defer s.taskLock.Unlock(taskID)

				task := s.getTask(taskID)
				if task == nil {
					return
				}

				state := task.GetState()
				switch state {
				case indexpb.JobState_JobStateNone:
					return
				case indexpb.JobState_JobStateInit:
					queueingTime := time.Since(task.GetQueueTime())
					if queueingTime > Params.DataCoordCfg.TaskSlowThreshold.GetAsDuration(time.Second) {
						log.Warn("task queueing time is too long", zap.Int64("taskID", taskID),
							zap.Int64("queueing time(ms)", queueingTime.Milliseconds()))
					}

					maxQueueingTime, ok := maxTaskQueueingTime[task.GetTaskType()]
					if !ok || maxQueueingTime < queueingTime.Milliseconds() {
						maxTaskQueueingTime[task.GetTaskType()] = queueingTime.Milliseconds()
					}
				case indexpb.JobState_JobStateInProgress:
					runningTime := time.Since(task.GetStartTime())
					if runningTime > Params.DataCoordCfg.TaskSlowThreshold.GetAsDuration(time.Second) {
						log.Warn("task running time is too long", zap.Int64("taskID", taskID),
							zap.Int64("running time(ms)", runningTime.Milliseconds()))
					}

					maxRunningTime, ok := maxTaskRunningTime[task.GetTaskType()]
					if !ok || maxRunningTime < runningTime.Milliseconds() {
						maxTaskRunningTime[task.GetTaskType()] = runningTime.Milliseconds()
					}
				}
			}

			for _, taskID := range taskIDs {
				collectMetricsFunc(taskID)
			}

			for taskType, queueingTime := range maxTaskQueueingTime {
				metrics.DataCoordTaskExecuteLatency.
					WithLabelValues(taskType, metrics.Pending).Observe(float64(queueingTime))
			}

			for taskType, runningTime := range maxTaskRunningTime {
				metrics.DataCoordTaskExecuteLatency.
					WithLabelValues(taskType, metrics.Executing).Observe(float64(runningTime))
			}
		}
	}
}

func (s *taskScheduler) processInit(task Task) bool {
	// 0. pre check task
	// Determine whether the task can be performed or if it is truly necessary.
	// for example: flat index doesn't need to actually build. checkPass is false.
	checkPass := task.PreCheck(s.ctx, s)
	if !checkPass {
		return true
	}

	// 1. pick an indexNode client
	nodeID, client := s.nodeManager.PickClient()
	if client == nil {
		log.Ctx(s.ctx).Debug("pick client failed")
		return false
	}
	log.Ctx(s.ctx).Info("pick client success", zap.Int64("taskID", task.GetTaskID()), zap.Int64("nodeID", nodeID))

	// 2. update version
	if err := task.UpdateVersion(s.ctx, s.meta); err != nil {
		log.Ctx(s.ctx).Warn("update task version failed", zap.Int64("taskID", task.GetTaskID()), zap.Error(err))
		return false
	}
	log.Ctx(s.ctx).Info("update task version success", zap.Int64("taskID", task.GetTaskID()))

	// 3. assign task to indexNode
	success := task.AssignTask(s.ctx, client)
	if !success {
		log.Ctx(s.ctx).Warn("assign task to client failed", zap.Int64("taskID", task.GetTaskID()),
			zap.String("new state", task.GetState().String()), zap.String("fail reason", task.GetFailReason()))
		// If the problem is caused by the task itself, subsequent tasks will not be skipped.
		// If etcd fails or fails to send tasks to the node, the subsequent tasks will be skipped.
		return false
	}
	log.Ctx(s.ctx).Info("assign task to client success", zap.Int64("taskID", task.GetTaskID()), zap.Int64("nodeID", nodeID))

	// 4. update meta state
	if err := task.UpdateMetaBuildingState(nodeID, s.meta); err != nil {
		log.Ctx(s.ctx).Warn("update meta building state failed", zap.Int64("taskID", task.GetTaskID()), zap.Error(err))
		task.SetState(indexpb.JobState_JobStateRetry, "update meta building state failed")
		return false
	}
	task.SetStartTime(time.Now())
	queueingTime := task.GetStartTime().Sub(task.GetQueueTime())
	if queueingTime > Params.DataCoordCfg.TaskSlowThreshold.GetAsDuration(time.Second) {
		log.Warn("task queueing time is too long", zap.Int64("taskID", task.GetTaskID()),
			zap.Int64("queueing time(ms)", queueingTime.Milliseconds()))
	}
	metrics.DataCoordTaskExecuteLatency.
		WithLabelValues(task.GetTaskType(), metrics.Pending).Observe(float64(queueingTime.Milliseconds()))
	log.Ctx(s.ctx).Info("update task meta state to InProgress success", zap.Int64("taskID", task.GetTaskID()),
		zap.Int64("nodeID", nodeID))
	return s.processInProgress(task)
}

func (s *taskScheduler) processFinished(task Task) bool {
	if err := task.SetJobInfo(s.meta); err != nil {
		log.Ctx(s.ctx).Warn("update task info failed", zap.Error(err))
		return true
	}
	task.SetEndTime(time.Now())
	runningTime := task.GetEndTime().Sub(task.GetStartTime())
	if runningTime > Params.DataCoordCfg.TaskSlowThreshold.GetAsDuration(time.Second) {
		log.Warn("task running time is too long", zap.Int64("taskID", task.GetTaskID()),
			zap.Int64("running time(ms)", runningTime.Milliseconds()))
	}
	metrics.DataCoordTaskExecuteLatency.
		WithLabelValues(task.GetTaskType(), metrics.Executing).Observe(float64(runningTime.Milliseconds()))
	client, exist := s.nodeManager.GetClientByID(task.GetNodeID())
	if exist {
		if !task.DropTaskOnWorker(s.ctx, client) {
			return true
		}
	}
	s.removeTask(task.GetTaskID())
	return true
}

func (s *taskScheduler) processRetry(task Task) bool {
	client, exist := s.nodeManager.GetClientByID(task.GetNodeID())
	if exist {
		if !task.DropTaskOnWorker(s.ctx, client) {
			return true
		}
	}
	task.SetState(indexpb.JobState_JobStateInit, "")
	task.ResetTask(s.meta)
	return true
}

func (s *taskScheduler) processInProgress(task Task) bool {
	client, exist := s.nodeManager.GetClientByID(task.GetNodeID())
	if exist {
		task.QueryResult(s.ctx, client)
		if task.GetState() == indexpb.JobState_JobStateFinished || task.GetState() == indexpb.JobState_JobStateFailed {
			return s.processFinished(task)
		}
		return true
	}
	task.SetState(indexpb.JobState_JobStateRetry, "node does not exist")
	return true
}
