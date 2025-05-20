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

package task

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	taskcommon "github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const NullNodeID = -1

type GlobalScheduler interface {
	Enqueue(task Task)
	AbortAndRemoveTask(taskID int64)

	Start()
	Stop()
}

var _ GlobalScheduler = (*globalTaskScheduler)(nil)

type globalTaskScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu           *lock.KeyLock[int64]
	pendingTasks FIFOQueue
	runningTasks *typeutil.ConcurrentMap[int64, Task]
	execPool     *conc.Pool[struct{}]
	checkPool    *conc.Pool[struct{}]
	cluster      session.Cluster
}

func (s *globalTaskScheduler) Enqueue(task Task) {
	if s.pendingTasks.Get(task.GetTaskID()) != nil {
		return
	}
	if s.runningTasks.Contain(task.GetTaskID()) {
		return
	}
	switch task.GetTaskState() {
	case taskcommon.Init:
		task.SetTaskTime(taskcommon.TimeQueue, time.Now())
		s.pendingTasks.Push(task)
	case taskcommon.InProgress:
		task.SetTaskTime(taskcommon.TimeStart, time.Now())
		s.runningTasks.Insert(task.GetTaskID(), task)
	}
}

func (s *globalTaskScheduler) AbortAndRemoveTask(taskID int64) {
	s.mu.Lock(taskID)
	defer s.mu.Unlock(taskID)
	if task, ok := s.runningTasks.GetAndRemove(taskID); ok {
		task.DropTaskOnWorker(s.cluster)
	}
	if task := s.pendingTasks.Get(taskID); task != nil {
		task.DropTaskOnWorker(s.cluster)
		s.pendingTasks.Remove(taskID)
	}
}

func (s *globalTaskScheduler) Start() {
	dur := paramtable.Get().DataCoordCfg.TaskScheduleInterval.GetAsDuration(time.Millisecond)
	s.wg.Add(3)
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(dur)
		defer t.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-t.C:
				s.schedule()
			}
		}
	}()
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(dur)
		defer t.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-t.C:
				s.check()
			}
		}
	}()
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-t.C:
				s.updateTaskTimeMetrics()
			}
		}
	}()
}

func (s *globalTaskScheduler) Stop() {
	s.cancel()
	s.wg.Wait()
}

func (s *globalTaskScheduler) pickNode(workerSlots map[int64]*session.WorkerSlots, taskSlot int64) int64 {
	var fallbackNodeID int64 = NullNodeID
	var maxAvailable int64 = -1

	for nodeID, ws := range workerSlots {
		if ws.AvailableSlots >= taskSlot {
			ws.AvailableSlots -= taskSlot
			return nodeID
		}
		if ws.AvailableSlots > maxAvailable && ws.AvailableSlots > 0 {
			maxAvailable = ws.AvailableSlots
			fallbackNodeID = nodeID
		}
	}

	if fallbackNodeID != NullNodeID {
		workerSlots[fallbackNodeID].AvailableSlots = 0
		return fallbackNodeID
	}
	return NullNodeID
}

func (s *globalTaskScheduler) schedule() {
	pendingNum := len(s.pendingTasks.TaskIDs())
	if pendingNum == 0 {
		return
	}
	nodeSlots := s.cluster.QuerySlot()
	log.Ctx(s.ctx).Info("scheduling pending tasks...", zap.Int("num", pendingNum), zap.Any("nodeSlots", nodeSlots))

	futures := make([]*conc.Future[struct{}], 0)
	for {
		task := s.pendingTasks.Pop()
		if task == nil {
			break
		}
		taskSlot := task.GetTaskSlot()
		nodeID := s.pickNode(nodeSlots, taskSlot)
		if nodeID == NullNodeID {
			s.pendingTasks.Push(task)
			break
		}
		future := s.execPool.Submit(func() (struct{}, error) {
			s.mu.RLock(task.GetTaskID())
			defer s.mu.RUnlock(task.GetTaskID())
			log.Ctx(s.ctx).Info("processing task...", WrapTaskLog(task)...)
			if task.GetTaskState() == taskcommon.Init {
				task.CreateTaskOnWorker(nodeID, s.cluster)
				switch task.GetTaskState() {
				case taskcommon.Init, taskcommon.Retry:
					s.pendingTasks.Push(task)
				case taskcommon.InProgress:
					task.SetTaskTime(taskcommon.TimeStart, time.Now())
					s.runningTasks.Insert(task.GetTaskID(), task)
				}
			}
			return struct{}{}, nil
		})
		futures = append(futures, future)
	}
	_ = conc.AwaitAll(futures...)
}

func (s *globalTaskScheduler) check() {
	if s.runningTasks.Len() <= 0 {
		return
	}
	log.Ctx(s.ctx).Info("check running tasks", zap.Int("num", s.runningTasks.Len()))

	tasks := s.runningTasks.Values()
	futures := make([]*conc.Future[struct{}], 0, len(tasks))
	for _, task := range tasks {
		future := s.checkPool.Submit(func() (struct{}, error) {
			s.mu.RLock(task.GetTaskID())
			defer s.mu.RUnlock(task.GetTaskID())
			task.QueryTaskOnWorker(s.cluster)
			switch task.GetTaskState() {
			case taskcommon.Init, taskcommon.Retry:
				s.runningTasks.Remove(task.GetTaskID())
				s.pendingTasks.Push(task)
			case taskcommon.Finished, taskcommon.Failed:
				task.SetTaskTime(taskcommon.TimeEnd, time.Now())
				task.DropTaskOnWorker(s.cluster)
				s.runningTasks.Remove(task.GetTaskID())
			}
			return struct{}{}, nil
		})
		futures = append(futures, future)
	}
	_ = conc.AwaitAll(futures...)
}

func (s *globalTaskScheduler) updateTaskTimeMetrics() {
	maxTaskQueueingTime := make(map[string]int64)
	maxTaskRunningTime := make(map[string]int64)

	collectPendingMetricsFunc := func(taskID int64) {
		task := s.pendingTasks.Get(taskID)
		if task == nil {
			return
		}

		s.mu.Lock(taskID)
		defer s.mu.Unlock(taskID)

		queueingTime := time.Since(task.GetTaskTime(taskcommon.TimeQueue))
		if queueingTime > paramtable.Get().DataCoordCfg.TaskSlowThreshold.GetAsDuration(time.Second) {
			log.Ctx(s.ctx).Warn("task queueing time is too long", zap.Int64("taskID", taskID),
				zap.Int64("queueing time(ms)", queueingTime.Milliseconds()))
		}

		maxQueueingTime, ok := maxTaskQueueingTime[task.GetTaskType()]
		if !ok || maxQueueingTime < queueingTime.Milliseconds() {
			maxTaskQueueingTime[task.GetTaskType()] = queueingTime.Milliseconds()
		}
	}

	collectRunningMetricsFunc := func(task Task) {
		s.mu.Lock(task.GetTaskID())
		defer s.mu.Unlock(task.GetTaskID())

		runningTime := time.Since(task.GetTaskTime(taskcommon.TimeStart))
		if runningTime > paramtable.Get().DataCoordCfg.TaskSlowThreshold.GetAsDuration(time.Second) {
			log.Ctx(s.ctx).Warn("task running time is too long", zap.Int64("taskID", task.GetTaskID()),
				zap.Int64("running time(ms)", runningTime.Milliseconds()))
		}

		maxRunningTime, ok := maxTaskRunningTime[task.GetTaskType()]
		if !ok || maxRunningTime < runningTime.Milliseconds() {
			maxTaskRunningTime[task.GetTaskType()] = runningTime.Milliseconds()
		}
	}

	taskIDs := s.pendingTasks.TaskIDs()

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

func NewGlobalTaskScheduler(ctx context.Context, cluster session.Cluster) GlobalScheduler {
	execPool := conc.NewPool[struct{}](128)
	checkPool := conc.NewPool[struct{}](128)
	ctx1, cancel := context.WithCancel(ctx)
	return &globalTaskScheduler{
		ctx:          ctx1,
		cancel:       cancel,
		wg:           sync.WaitGroup{},
		mu:           lock.NewKeyLock[int64](),
		pendingTasks: NewFIFOQueue(),
		runningTasks: typeutil.NewConcurrentMap[int64, Task](),
		execPool:     execPool,
		checkPool:    checkPool,
		cluster:      cluster,
	}
}
