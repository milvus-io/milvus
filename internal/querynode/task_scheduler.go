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

package querynode

import (
	"container/list"
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
)

const (
	maxExecuteReadChanLen = 1024 * 10
)

type taskScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc

	// for search and query start
	unsolvedReadTasks *list.List
	readyReadTasks    *list.List

	receiveReadTaskChan chan readTask
	executeReadTaskChan chan readTask

	notifyChan chan struct{}

	// tSafeReplica
	tSafeReplica TSafeReplicaInterface

	schedule scheduleReadTaskPolicy
	// for search and query end

	cpuUsage        int32 // 1200 means 1200% 12 cores
	readConcurrency int32 // 1200 means 1200% 12 cores

	// for other tasks
	queue       taskQueue
	maxCPUUsage int32

	wg sync.WaitGroup
}

func getNumCPU() int {
	cur := runtime.GOMAXPROCS(0)
	if cur <= 0 {
		cur = runtime.NumCPU()
	}
	return cur
}

func newTaskScheduler(ctx context.Context, tSafeReplica TSafeReplicaInterface) *taskScheduler {
	ctx1, cancel := context.WithCancel(ctx)
	s := &taskScheduler{
		ctx:                 ctx1,
		cancel:              cancel,
		unsolvedReadTasks:   list.New(),
		readyReadTasks:      list.New(),
		receiveReadTaskChan: make(chan readTask, Params.QueryNodeCfg.MaxReceiveChanSize),
		executeReadTaskChan: make(chan readTask, maxExecuteReadChanLen),
		notifyChan:          make(chan struct{}, 1),
		tSafeReplica:        tSafeReplica,
		maxCPUUsage:         int32(getNumCPU() * 100),
		schedule:            defaultScheduleReadPolicy,
	}
	s.queue = newQueryNodeTaskQueue(s)
	return s
}

func (s *taskScheduler) processTask(t task, q taskQueue) {
	// TODO: ctx?
	err := t.PreExecute(s.ctx)

	defer func() {
		t.Notify(err)
	}()
	if err != nil {
		log.Warn(err.Error())
		return
	}

	q.AddActiveTask(t)
	defer func() {
		q.PopActiveTask(t.ID())
	}()

	err = t.Execute(s.ctx)
	if err != nil {
		log.Warn(err.Error())
		return
	}
	err = t.PostExecute(s.ctx)
}

func (s *taskScheduler) taskLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.queue.utChan():
			if !s.queue.utEmpty() {
				t := s.queue.PopUnissuedTask()
				s.processTask(t, s.queue)
			}
		}
	}
}

func (s *taskScheduler) Start() {
	s.wg.Add(1)
	go s.taskLoop()

	s.wg.Add(1)
	go s.scheduleReadTasks()

	s.wg.Add(1)
	go s.executeReadTasks()
}

func (s *taskScheduler) tryEvictUnsolvedReadTask(headCount int) {
	after := headCount + s.unsolvedReadTasks.Len()
	diff := int32(after) - Params.QueryNodeCfg.MaxUnsolvedQueueSize
	if diff <= 0 {
		return
	}
	timeoutErr := fmt.Errorf("deadline exceed")
	var next *list.Element
	for e := s.unsolvedReadTasks.Front(); e != nil; e = next {
		next = e.Next()
		t, ok := e.Value.(readTask)
		if !ok {
			s.unsolvedReadTasks.Remove(e)
			diff--
			continue
		}
		if t.Timeout() {
			s.unsolvedReadTasks.Remove(e)
			t.Notify(timeoutErr)
			diff--
		}
	}
	if diff <= 0 {
		return
	}
	metrics.QueryNodeEvictedReadReqCount.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Add(float64(diff))
	busyErr := fmt.Errorf("server is busy")
	for e := s.unsolvedReadTasks.Front(); e != nil && diff > 0; e = next {
		next = e.Next()
		diff--
		s.unsolvedReadTasks.Remove(e)
		t, ok := e.Value.(readTask)
		if ok {
			t.Notify(busyErr)
		}
	}
}

func (s *taskScheduler) scheduleReadTasks() {
	defer s.wg.Done()
	l := s.tSafeReplica.Watch()
	defer l.Unregister()
	for {
		select {
		case <-s.ctx.Done():
			log.Warn("QueryNode sop schedulerReadTasks")
			return

		case <-s.notifyChan:
			s.tryMergeReadTasks()
			s.popAndAddToExecute()

		case t, ok := <-s.receiveReadTaskChan:
			if ok {
				pendingTaskLen := len(s.receiveReadTaskChan)
				s.tryEvictUnsolvedReadTask(pendingTaskLen + 1)
				if t != nil {
					s.unsolvedReadTasks.PushBack(t)
				}
				for i := 0; i < pendingTaskLen; i++ {
					t := <-s.receiveReadTaskChan
					if t != nil {
						s.unsolvedReadTasks.PushBack(t)
					}
				}
				s.tryMergeReadTasks()
				s.popAndAddToExecute()
			} else {
				errMsg := "taskScheduler receiveReadTaskChan has been closed"
				log.Warn(errMsg)
				return
			}
		case <-l.On():
			s.tryMergeReadTasks()
			s.popAndAddToExecute()
		}
	}
}

func (s *taskScheduler) AddReadTask(ctx context.Context, t readTask) error {
	t.SetMaxCPUUsage(s.maxCPUUsage)
	t.OnEnqueue()
	select {
	case <-ctx.Done():
		return fmt.Errorf("taskScheduler AddReadTask context is done")
	case <-s.ctx.Done():
		return fmt.Errorf("taskScheduler stoped")
	case s.receiveReadTaskChan <- t:
		return nil
	}
}

func (s *taskScheduler) popAndAddToExecute() {
	readConcurrency := atomic.LoadInt32(&s.readConcurrency)
	metrics.QueryNodeReadTaskConcurrency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Set(float64(readConcurrency))
	if s.readyReadTasks.Len() == 0 {
		return
	}
	curUsage := atomic.LoadInt32(&s.cpuUsage)
	if curUsage < 0 {
		curUsage = 0
	}
	metrics.QueryNodeEstimateCPUUsage.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Set(float64(curUsage))
	targetUsage := s.maxCPUUsage - curUsage
	if targetUsage <= 0 {
		return
	}

	remain := Params.QueryNodeCfg.MaxReadConcurrency - readConcurrency
	if remain <= 0 {
		return
	}

	tasks, deltaUsage := s.schedule(s.readyReadTasks, targetUsage, remain)
	atomic.AddInt32(&s.cpuUsage, deltaUsage)
	for _, t := range tasks {
		s.executeReadTaskChan <- t
	}
}

func (s *taskScheduler) executeReadTasks() {
	defer s.wg.Done()
	var taskWg sync.WaitGroup
	defer taskWg.Wait()
	timeoutErr := fmt.Errorf("deadline exceed")

	executeFunc := func(t readTask) {
		defer taskWg.Done()
		if t.Timeout() {
			t.Notify(timeoutErr)
		} else {
			s.processReadTask(t)
		}
		cpu := t.CPUUsage()
		atomic.AddInt32(&s.readConcurrency, -1)
		atomic.AddInt32(&s.cpuUsage, -cpu)
		select {
		case s.notifyChan <- struct{}{}:
		default:
		}
	}

	for {
		select {
		case <-s.ctx.Done():
			log.Debug("QueryNode stop executeReadTasks", zap.Int64("NodeID", Params.QueryNodeCfg.GetNodeID()))
			return
		case t, ok := <-s.executeReadTaskChan:
			if ok {
				pendingTaskLen := len(s.executeReadTaskChan)
				taskWg.Add(1)
				atomic.AddInt32(&s.readConcurrency, int32(pendingTaskLen+1))
				go executeFunc(t)

				for i := 0; i < pendingTaskLen; i++ {
					taskWg.Add(1)
					t := <-s.executeReadTaskChan
					go executeFunc(t)
				}
				//log.Debug("QueryNode taskScheduler executeReadTasks process tasks done", zap.Int("numOfTasks", pendingTaskLen+1))
			} else {
				errMsg := "taskScheduler executeReadTaskChan has been closed"
				log.Warn(errMsg)
				return
			}
		}
	}
}

func (s *taskScheduler) processReadTask(t readTask) {
	err := t.PreExecute(t.Ctx())

	defer func() {
		t.Notify(err)
	}()
	if err != nil {
		log.Warn(err.Error())
		return
	}

	err = t.Execute(s.ctx)
	if err != nil {
		log.Warn(err.Error())
		return
	}
	err = t.PostExecute(s.ctx)
}

func (s *taskScheduler) Close() {
	s.cancel()
	s.wg.Wait()
}

func (s *taskScheduler) tryMergeReadTasks() {
	var next *list.Element
	for e := s.unsolvedReadTasks.Front(); e != nil; e = next {
		next = e.Next()
		t, ok := e.Value.(readTask)
		if !ok {
			s.unsolvedReadTasks.Remove(e)
			continue
		}
		ready, err := t.Ready()
		if err != nil {
			s.unsolvedReadTasks.Remove(e)
			t.Notify(err)
			continue
		}
		if ready {
			if !Params.QueryNodeCfg.GroupEnabled {
				s.readyReadTasks.PushBack(t)
			} else {
				merged := false
				for m := s.readyReadTasks.Back(); m != nil; m = m.Prev() {
					mTask, ok := m.Value.(readTask)
					if !ok {
						continue
					}
					if mTask.CanMergeWith(t) {
						mTask.Merge(t)
						merged = true
						break
					}
				}
				if !merged {
					s.readyReadTasks.PushBack(t)
				}
			}
			s.unsolvedReadTasks.Remove(e)
		}
	}
	metrics.QueryNodeReadTaskUnsolveLen.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Set(float64(s.unsolvedReadTasks.Len()))
	metrics.QueryNodeReadTaskReadyLen.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Set(float64(s.readyReadTasks.Len()))
}
