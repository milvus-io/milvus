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

	"github.com/hashicorp/golang-lru/v2/expirable"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type taskScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	scheduleDuration       time.Duration
	collectMetricsDuration time.Duration

	pendingTasks schedulePolicy
	runningTasks *typeutil.ConcurrentMap[UniqueID, Task]

	taskLock *lock.KeyLock[int64]

	notifyChan chan struct{}

	meta *meta

	policy                    buildIndexPolicy
	nodeManager               session.WorkerManager
	chunkManager              storage.ChunkManager
	indexEngineVersionManager IndexEngineVersionManager
	handler                   Handler
	allocator                 allocator.Allocator
	compactionHandler         compactionPlanContext

	slotsMutex sync.RWMutex

	taskStats *expirable.LRU[UniqueID, Task]
}

func newTaskScheduler(
	ctx context.Context,
	metaTable *meta, nodeManager session.WorkerManager,
	chunkManager storage.ChunkManager,
	indexEngineVersionManager IndexEngineVersionManager,
	handler Handler,
	allocator allocator.Allocator,
	compactionHandler compactionPlanContext,
) *taskScheduler {
	ctx, cancel := context.WithCancel(ctx)

	ts := &taskScheduler{
		ctx:                       ctx,
		cancel:                    cancel,
		meta:                      metaTable,
		pendingTasks:              newPriorityQueuePolicy(),
		runningTasks:              typeutil.NewConcurrentMap[UniqueID, Task](),
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
		taskStats:                 expirable.NewLRU[UniqueID, Task](512, nil, time.Minute*15),
		compactionHandler:         compactionHandler,
	}
	ts.reloadFromMeta()
	return ts
}

func (s *taskScheduler) Start() {
	s.wg.Add(3)
	go s.schedule()
	go s.collectTaskMetrics()
	go s.checkProcessingTasksLoop()
}

func (s *taskScheduler) Stop() {
	s.cancel()
	s.wg.Wait()
}

func (s *taskScheduler) reloadFromMeta() {
	segments := s.meta.GetAllSegmentsUnsafe()
	for _, segment := range segments {
		for _, segIndex := range s.meta.indexMeta.GetSegmentIndexes(segment.GetCollectionID(), segment.ID) {
			if segIndex.IsDeleted {
				continue
			}
			indexParams := s.meta.indexMeta.GetIndexParams(segment.CollectionID, segIndex.IndexID)
			indexType := GetIndexType(indexParams)
			isVectorIndex := vecindexmgr.GetVecIndexMgrInstance().IsVecIndex(indexType)
			fieldID := s.meta.indexMeta.GetFieldIDByIndexID(segment.CollectionID, segIndex.IndexID)
			taskSlot := calculateIndexTaskSlot(segment.getFieldBinlogSize(fieldID), isVectorIndex)
			task := &indexBuildTask{
				taskID: segIndex.BuildID,
				nodeID: segIndex.NodeID,
				taskInfo: &workerpb.IndexTaskInfo{
					BuildID:    segIndex.BuildID,
					State:      segIndex.IndexState,
					FailReason: segIndex.FailReason,
				},
				taskSlot: taskSlot,
				req: &workerpb.CreateJobRequest{
					ClusterID: Params.CommonCfg.ClusterPrefix.GetValue(),
					BuildID:   segIndex.BuildID,
				},
				queueTime: time.Now(),
				startTime: time.Now(),
				endTime:   time.Now(),
			}
			switch segIndex.IndexState {
			case commonpb.IndexState_IndexStateNone, commonpb.IndexState_Unissued:
				s.pendingTasks.Push(task)
			case commonpb.IndexState_InProgress, commonpb.IndexState_Retry:
				s.runningTasks.Insert(segIndex.BuildID, task)
			}
		}
	}

	allAnalyzeTasks := s.meta.analyzeMeta.GetAllTasks()
	for taskID, t := range allAnalyzeTasks {
		task := &analyzeTask{
			taskID: taskID,
			nodeID: t.NodeID,
			taskInfo: &workerpb.AnalyzeResult{
				TaskID:     taskID,
				State:      t.State,
				FailReason: t.FailReason,
			},
			req: &workerpb.AnalyzeRequest{
				ClusterID: Params.CommonCfg.ClusterPrefix.GetValue(),
				TaskID:    taskID,
			},
			queueTime: time.Now(),
			startTime: time.Now(),
			endTime:   time.Now(),
		}
		switch t.State {
		case indexpb.JobState_JobStateNone, indexpb.JobState_JobStateInit:
			s.pendingTasks.Push(task)
		case indexpb.JobState_JobStateInProgress, indexpb.JobState_JobStateRetry:
			s.runningTasks.Insert(taskID, task)
		}
	}

	allStatsTasks := s.meta.statsTaskMeta.GetAllTasks()
	for taskID, t := range allStatsTasks {
		segment := s.meta.GetHealthySegment(s.ctx, t.GetSegmentID())
		taskSlot := int64(0)
		if segment != nil {
			taskSlot = calculateStatsTaskSlot(segment.getSegmentSize())
		}
		task := &statsTask{
			taskID:          taskID,
			segmentID:       t.GetSegmentID(),
			targetSegmentID: t.GetTargetSegmentID(),
			nodeID:          t.NodeID,
			taskInfo: &workerpb.StatsResult{
				TaskID:     taskID,
				State:      t.GetState(),
				FailReason: t.GetFailReason(),
			},
			req: &workerpb.CreateStatsRequest{
				ClusterID: Params.CommonCfg.ClusterPrefix.GetValue(),
				TaskID:    taskID,
			},
			taskSlot:   taskSlot,
			queueTime:  time.Now(),
			startTime:  time.Now(),
			endTime:    time.Now(),
			subJobType: t.GetSubJobType(),
		}
		switch t.GetState() {
		case indexpb.JobState_JobStateNone, indexpb.JobState_JobStateInit:
			s.pendingTasks.Push(task)
		case indexpb.JobState_JobStateInProgress, indexpb.JobState_JobStateRetry:
			if t.GetSubJobType() == indexpb.StatsSubJob_Sort {
				exist, canDo := s.meta.CheckAndSetSegmentsCompacting(context.TODO(), []UniqueID{t.GetSegmentID()})
				if !exist || !canDo {
					log.Ctx(s.ctx).Warn("segment is not exist or is compacting, skip stats, but this should not have happened, try to remove the stats task",
						zap.Int64("taskID", taskID), zap.Bool("exist", exist), zap.Bool("canDo", canDo))
					err := s.meta.statsTaskMeta.DropStatsTask(t.GetTaskID())
					if err == nil {
						continue
					}
					log.Ctx(s.ctx).Warn("remove stats task failed, set to failed", zap.Int64("taskID", taskID), zap.Error(err))
					task.taskInfo.State = indexpb.JobState_JobStateFailed
					task.taskInfo.FailReason = "segment is not exist or is compacting"
				} else {
					if !s.compactionHandler.checkAndSetSegmentStating(t.GetInsertChannel(), t.GetSegmentID()) {
						s.meta.SetSegmentsCompacting(context.TODO(), []UniqueID{t.GetSegmentID()}, false)
						err := s.meta.statsTaskMeta.DropStatsTask(t.GetTaskID())
						if err == nil {
							continue
						}
						log.Ctx(s.ctx).Warn("remove stats task failed, set to failed", zap.Int64("taskID", taskID), zap.Error(err))
						task.taskInfo.State = indexpb.JobState_JobStateFailed
						task.taskInfo.FailReason = "segment is not exist or is l0 compacting"
					}
				}
			}
			s.runningTasks.Insert(taskID, task)
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

func (s *taskScheduler) exist(taskID UniqueID) bool {
	exist := s.pendingTasks.Exist(taskID)
	if exist {
		return true
	}
	_, ok := s.runningTasks.Get(taskID)
	return ok
}

func (s *taskScheduler) enqueue(task Task) {
	defer s.notify()
	taskID := task.GetTaskID()
	_, ok := s.runningTasks.Get(taskID)
	if !ok {
		task.SetQueueTime(time.Now())
		s.pendingTasks.Push(task)
		log.Ctx(s.ctx).Info("taskScheduler enqueue task", zap.Int64("taskID", taskID))
	}
}

func (s *taskScheduler) AbortTask(taskID int64) {
	log.Ctx(s.ctx).Info("task scheduler receive abort task request", zap.Int64("taskID", taskID))
	s.taskLock.Lock(taskID)
	defer s.taskLock.Unlock(taskID)

	task := s.pendingTasks.Get(taskID)
	if task != nil {
		task.SetState(indexpb.JobState_JobStateFailed, "canceled")
		s.runningTasks.Insert(taskID, task)
		s.pendingTasks.Remove(taskID)
		return
	}

	if runningTask, ok := s.runningTasks.Get(taskID); ok {
		runningTask.SetState(indexpb.JobState_JobStateFailed, "canceled")
		s.runningTasks.Insert(taskID, runningTask)
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
			log.Ctx(s.ctx).Warn("task scheduler ctx done, exit schedule")
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

func (s *taskScheduler) checkProcessingTasksLoop() {
	log.Ctx(s.ctx).Info("taskScheduler checkProcessingTasks loop start")
	defer s.wg.Done()
	ticker := time.NewTicker(s.scheduleDuration)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			log.Ctx(s.ctx).Warn("task scheduler ctx done, exit checkProcessingTasksLoop")
			return
		case <-ticker.C:
			s.checkProcessingTasks()
		}
	}
}

func (s *taskScheduler) checkProcessingTasks() {
	if s.runningTasks.Len() <= 0 {
		return
	}
	log.Ctx(s.ctx).Info("check running tasks", zap.Int("runningTask num", s.runningTasks.Len()))

	allRunningTasks := s.runningTasks.Values()
	var wg sync.WaitGroup
	sem := make(chan struct{}, 100)
	for _, task := range allRunningTasks {
		wg.Add(1)
		sem <- struct{}{}
		go func(task Task) {
			defer wg.Done()
			defer func() {
				<-sem
			}()
			s.taskLock.Lock(task.GetTaskID())
			suc := s.checkProcessingTask(task)
			s.taskLock.Unlock(task.GetTaskID())
			if suc {
				s.runningTasks.Remove(task.GetTaskID())
			}
		}(task)
	}
	wg.Wait()
}

func (s *taskScheduler) checkProcessingTask(task Task) bool {
	switch task.GetState() {
	case indexpb.JobState_JobStateInProgress:
		return s.processInProgress(task)
	case indexpb.JobState_JobStateRetry:
		return s.processRetry(task)
	case indexpb.JobState_JobStateFinished, indexpb.JobState_JobStateFailed:
		return s.processFinished(task)
	default:
		log.Ctx(s.ctx).Error("invalid task state in running queue", zap.Int64("taskID", task.GetTaskID()), zap.String("state", task.GetState().String()))
	}
	return false
}

func (s *taskScheduler) run() {
	// schedule policy
	pendingTaskNum := s.pendingTasks.TaskCount()
	if pendingTaskNum == 0 {
		return
	}

	workerSlots := s.nodeManager.QuerySlots()
	// Build the node-slot max-heap once per round and reuse it across all picks,
	// so each task lands on the currently least-loaded DataNode (water-filling).
	// Entries hold pointers into workerSlots, so hasAvailableSlots on the map
	// still observes the decrements applied through the heap.
	slotHeap := newNodeSlotHeap(workerSlots)

	var totalAvailable int64
	for _, ws := range workerSlots {
		totalAvailable += ws.AvailableSlots
	}
	log.Ctx(s.ctx).Info("task scheduler round starting",
		zap.Int("pendingTasks", pendingTaskNum),
		zap.Int("nodes", len(workerSlots)),
		zap.Int64("totalAvailableSlots", totalAvailable),
		zap.Any("workerSlots", workerSlots))

	var wg sync.WaitGroup
	for {
		if !s.hasAvailableSlots(workerSlots) {
			break
		}

		task := s.pendingTasks.Pop()
		if task == nil {
			break
		}

		taskSlot := task.GetTaskSlot()
		nodeID := s.pickNode(slotHeap, taskSlot)

		wg.Add(1)
		go func(task Task, nodeID UniqueID) {
			defer wg.Done()

			if nodeID != -1 {
				s.taskLock.Lock(task.GetTaskID())
				s.process(task, nodeID)
				s.taskLock.Unlock(task.GetTaskID())
			}

			switch task.GetState() {
			case indexpb.JobState_JobStateNone:
				if !s.processNone(task) {
					s.pendingTasks.Push(task)
				}
			case indexpb.JobState_JobStateInit:
				s.pendingTasks.Push(task)
			default:
				s.runningTasks.Insert(task.GetTaskID(), task)
			}
		}(task, nodeID)
	}
	wg.Wait()
}

func (s *taskScheduler) process(task Task, nodeID int64) bool {
	log.Ctx(s.ctx).Info("task is processing", zap.Int64("taskID", task.GetTaskID()),
		zap.Int64("nodeID", nodeID), zap.String("task type", task.GetTaskType()),
		zap.String("state", task.GetState().String()))

	switch task.GetState() {
	case indexpb.JobState_JobStateNone:
		return s.processNone(task)
	case indexpb.JobState_JobStateInit:
		return s.processInit(task, nodeID)
	default:
		log.Ctx(s.ctx).Error("invalid task state in pending queue", zap.Int64("taskID", task.GetTaskID()), zap.String("state", task.GetState().String()))
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
			log.Warn("task scheduler context done, exit collectTaskMetrics")
			return
		case <-ticker.C:
			maxTaskQueueingTime := make(map[string]int64)
			maxTaskRunningTime := make(map[string]int64)

			collectPendingMetricsFunc := func(taskID int64) {
				task := s.pendingTasks.Get(taskID)
				if task == nil {
					return
				}

				s.taskLock.Lock(taskID)
				defer s.taskLock.Unlock(taskID)

				switch task.GetState() {
				case indexpb.JobState_JobStateInit:
					queueingTime := time.Since(task.GetQueueTime())
					if queueingTime > Params.DataCoordCfg.TaskSlowThreshold.GetAsDuration(time.Second) {
						log.Ctx(s.ctx).Warn("task queueing time is too long", zap.Int64("taskID", taskID),
							zap.Int64("queueing time(ms)", queueingTime.Milliseconds()))
					}

					maxQueueingTime, ok := maxTaskQueueingTime[task.GetTaskType()]
					if !ok || maxQueueingTime < queueingTime.Milliseconds() {
						maxTaskQueueingTime[task.GetTaskType()] = queueingTime.Milliseconds()
					}
				}
			}

			collectRunningMetricsFunc := func(task Task) {
				s.taskLock.Lock(task.GetTaskID())
				defer s.taskLock.Unlock(task.GetTaskID())

				switch task.GetState() {
				case indexpb.JobState_JobStateInProgress:
					runningTime := time.Since(task.GetStartTime())
					if runningTime > Params.DataCoordCfg.TaskSlowThreshold.GetAsDuration(time.Second) {
						log.Ctx(s.ctx).Warn("task running time is too long", zap.Int64("taskID", task.GetTaskID()),
							zap.Int64("running time(ms)", runningTime.Milliseconds()))
					}

					maxRunningTime, ok := maxTaskRunningTime[task.GetTaskType()]
					if !ok || maxRunningTime < runningTime.Milliseconds() {
						maxTaskRunningTime[task.GetTaskType()] = runningTime.Milliseconds()
					}
				}
			}

			taskIDs := s.pendingTasks.Keys()

			for _, taskID := range taskIDs {
				collectPendingMetricsFunc(taskID)
			}

			allRunningTasks := s.runningTasks.Values()
			for _, task := range allRunningTasks {
				collectRunningMetricsFunc(task)
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

func (s *taskScheduler) processInit(task Task, nodeID int64) bool {
	// 0. pre check task
	// Determine whether the task can be performed or if it is truly necessary.
	// for example: flat index doesn't need to actually build. checkPass is false.
	checkPass := task.PreCheck(s.ctx, s)
	if !checkPass {
		return true
	}
	client, exist := s.nodeManager.GetClientByID(nodeID)
	if !exist || client == nil {
		log.Ctx(s.ctx).Debug("get indexnode client failed", zap.Int64("nodeID", nodeID))
		return false
	}
	log.Ctx(s.ctx).Info("pick client success", zap.Int64("taskID", task.GetTaskID()), zap.Int64("nodeID", nodeID))

	// 2. update version
	if err := task.UpdateVersion(s.ctx, nodeID, s.meta, s.compactionHandler); err != nil {
		log.Ctx(s.ctx).Warn("update task version failed", zap.Int64("taskID", task.GetTaskID()), zap.Error(err))
		return false
	}
	log.Ctx(s.ctx).Info("update task version success", zap.Int64("taskID", task.GetTaskID()))

	// 3. assign task to indexNode
	success := task.AssignTask(s.ctx, client, s.meta)
	if !success {
		log.Ctx(s.ctx).Warn("assign task to client failed", zap.Int64("taskID", task.GetTaskID()),
			zap.String("new state", task.GetState().String()), zap.String("fail reason", task.GetFailReason()))
		// If the problem is caused by the task itself, subsequent tasks will not be skipped.
		// If etcd fails or fails to send tasks to the node, the subsequent tasks will be skipped.
		return false
	}
	log.Ctx(s.ctx).Info("assign task to client success", zap.Int64("taskID", task.GetTaskID()), zap.Int64("nodeID", nodeID))

	// 4. update meta state
	if err := task.UpdateMetaBuildingState(s.meta); err != nil {
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
	return true
}

func (s *taskScheduler) processNone(task Task) bool {
	if err := task.DropTaskMeta(s.ctx, s.meta); err != nil {
		log.Ctx(s.ctx).Warn("set job info failed", zap.Error(err))
		return false
	}
	return true
}

func (s *taskScheduler) processFinished(task Task) bool {
	if err := task.SetJobInfo(s.meta); err != nil {
		log.Ctx(s.ctx).Warn("update task info failed", zap.Error(err))
		return false
	}
	task.SetEndTime(time.Now())
	runningTime := task.GetEndTime().Sub(task.GetStartTime())
	if runningTime > Params.DataCoordCfg.TaskSlowThreshold.GetAsDuration(time.Second) {
		log.Ctx(s.ctx).Warn("task running time is too long", zap.Int64("taskID", task.GetTaskID()),
			zap.Int64("running time(ms)", runningTime.Milliseconds()))
	}
	metrics.DataCoordTaskExecuteLatency.
		WithLabelValues(task.GetTaskType(), metrics.Executing).Observe(float64(runningTime.Milliseconds()))
	client, exist := s.nodeManager.GetClientByID(task.GetNodeID())
	if exist {
		if !task.DropTaskOnWorker(s.ctx, client) {
			log.Ctx(s.ctx).Warn("drop task on worker failed, but ignore it",
				zap.Int64("taskID", task.GetTaskID()), zap.Int64("nodeID", task.GetNodeID()))
			return true
		}
	}
	log.Ctx(s.ctx).Info("task has been finished", zap.Int64("taskID", task.GetTaskID()),
		zap.Int64("queueing time(ms)", task.GetStartTime().Sub(task.GetQueueTime()).Milliseconds()),
		zap.Int64("running time(ms)", runningTime.Milliseconds()),
		zap.Int64("total time(ms)", task.GetEndTime().Sub(task.GetQueueTime()).Milliseconds()))
	return true
}

func (s *taskScheduler) processRetry(task Task) bool {
	client, exist := s.nodeManager.GetClientByID(task.GetNodeID())
	if exist {
		if !task.DropTaskOnWorker(s.ctx, client) {
			return false
		}
	}
	task.SetState(indexpb.JobState_JobStateInit, "")
	task.ResetTask(s.meta)

	log.Ctx(s.ctx).Info("processRetry success, set task to pending queue", zap.Int64("taskID", task.GetTaskID()),
		zap.String("state", task.GetState().String()))

	s.pendingTasks.Push(task)
	return true
}

func (s *taskScheduler) processInProgress(task Task) bool {
	client, exist := s.nodeManager.GetClientByID(task.GetNodeID())
	if exist {
		task.QueryResult(s.ctx, client)
		if task.GetState() == indexpb.JobState_JobStateFinished || task.GetState() == indexpb.JobState_JobStateFailed {
			return s.processFinished(task)
		}
		return false
	}
	log.Ctx(s.ctx).Info("node does not exist, set task state to retry", zap.Int64("taskID", task.GetTaskID()))
	task.SetState(indexpb.JobState_JobStateRetry, "node does not exist")
	return false
}

// nodeSlotEntry pairs a DataNode ID with a pointer to its live WorkerSlots so
// that mutations made while an entry sits outside the heap are visible to
// hasAvailableSlots (which walks the same map).
type nodeSlotEntry struct {
	nodeID int64
	slots  *session.WorkerSlots
}

// newNodeSlotHeap builds a max-heap ordered by AvailableSlots. The heap MUST
// NOT be mutated while an entry is still inside it, or heap order will break;
// callers Pop, mutate, then Push.
func newNodeSlotHeap(workerSlots map[int64]*session.WorkerSlots) typeutil.Heap[*nodeSlotEntry] {
	entries := make([]*nodeSlotEntry, 0, len(workerSlots))
	for nodeID, ws := range workerSlots {
		entries = append(entries, &nodeSlotEntry{nodeID: nodeID, slots: ws})
	}
	return typeutil.NewObjectArrayBasedMaximumHeap(entries, func(e *nodeSlotEntry) int64 {
		return e.slots.AvailableSlots
	})
}

// pickNode returns the currently least-loaded DataNode (the one with the most
// available slots). Always assigning to the most-available node spreads tasks
// evenly across DataNodes instead of hot-spotting whichever node happened to
// come first in the previous map-iteration-based first-fit.
//
// Fallback behavior when no node fully satisfies taskSlot is preserved: the
// most-available node is chosen and its remaining slots are drained to 0.
// Returns -1 when the heap is empty or the top-of-heap has no capacity.
func (s *taskScheduler) pickNode(slotHeap typeutil.Heap[*nodeSlotEntry], taskSlot int64) int64 {
	s.slotsMutex.Lock()
	defer s.slotsMutex.Unlock()

	if slotHeap.Len() == 0 {
		return -1
	}

	entry := slotHeap.Pop()
	before := entry.slots.AvailableSlots

	// Non-positive slot tasks (e.g., cleanup) dispatch without consuming capacity.
	if taskSlot <= 0 {
		slotHeap.Push(entry)
		log.Ctx(s.ctx).Debug("pickNode assigned zero-slot task",
			zap.Int64("nodeID", entry.nodeID),
			zap.Int64("availableSlots", entry.slots.AvailableSlots))
		return entry.nodeID
	}

	if entry.slots.AvailableSlots <= 0 {
		slotHeap.Push(entry)
		return -1
	}

	if entry.slots.AvailableSlots >= taskSlot {
		entry.slots.AvailableSlots -= taskSlot
	} else {
		// Fallback: drain the most-available node when nothing fully fits.
		entry.slots.AvailableSlots = 0
	}
	slotHeap.Push(entry)

	log.Ctx(s.ctx).Debug("pickNode assigned task",
		zap.Int64("nodeID", entry.nodeID),
		zap.Int64("taskSlot", taskSlot),
		zap.Int64("slotsBefore", before),
		zap.Int64("slotsAfter", entry.slots.AvailableSlots))
	return entry.nodeID
}

func (s *taskScheduler) hasAvailableSlots(workerSlots map[int64]*session.WorkerSlots) bool {
	s.slotsMutex.RLock()
	defer s.slotsMutex.RUnlock()

	for _, ws := range workerSlots {
		if ws.AvailableSlots > 0 {
			return true
		}
	}
	return false
}
