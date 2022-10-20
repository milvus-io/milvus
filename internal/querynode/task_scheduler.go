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
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"

	"github.com/milvus-io/milvus/internal/util/concurrency"
)

const (
	maxExecuteReadChanLen = 1024 * 10
)

type taskScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc

	unsolveLock sync.Mutex
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

	readConcurrency int32

	// for other tasks
	queue taskQueue

	wg sync.WaitGroup

	pool *concurrency.Pool
}

func newTaskScheduler(ctx context.Context, tSafeReplica TSafeReplicaInterface, pool *concurrency.Pool) *taskScheduler {
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
		schedule:            defaultScheduleReadPolicy,
		pool:                pool,
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
				select {
				case <-t.Ctx().Done():
					t.Notify(context.Canceled)
					continue
				default:
					s.processTask(t, s.queue)
				}
			}
		}
	}
}

func (s *taskScheduler) Start() {
	s.wg.Add(1)
	go s.taskLoop()

	s.wg.Add(1)
	go s.scheduleReadTasks()

}

func (s *taskScheduler) scheduleReadTasks() {
	defer s.wg.Done()
	l := s.tSafeReplica.Watch()
	defer l.Unregister()
	for {
		select {
		case <-s.ctx.Done():
			log.Warn("QueryNode stop schedulerReadTasks")
			return
		case <-s.pool.SignalChan():
			s.tryMergeReadTasks()
			s.popAndAddToExecute()
		case <-s.notifyChan:
			s.tryMergeReadTasks()
			s.popAndAddToExecute()
		case <-l.On():
			s.tryMergeReadTasks()
			s.popAndAddToExecute()
		}
	}
}

func (s *taskScheduler) AddReadTask(ctx context.Context, t readTask) error {
	t.OnEnqueue()
	s.unsolveLock.Lock()
	s.unsolvedReadTasks.PushBack(t)
	s.unsolveLock.Unlock()

	select {
	case <-ctx.Done():
		return fmt.Errorf("taskScheduler AddReadTask context is done")
	case <-s.ctx.Done():
		return fmt.Errorf("taskScheduler stoped")
	case s.notifyChan <- struct{}{}:
		rateCol.rtCounter.add(t, receiveQueueType)
	default:
	}
	return nil
}

func (s *taskScheduler) popAndAddToExecute() {
	readConcurrency := atomic.LoadInt32(&s.readConcurrency)
	metrics.QueryNodeReadTaskConcurrency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Set(float64(readConcurrency))

	jobNum := s.pool.JobNum()
	metrics.QueryNodePoolJobNum.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Set(float64(jobNum))
	if s.readyReadTasks.Len() == 0 {
		return
	}

	remain := Params.QueryNodeCfg.MaxReadConcurrency - int32(jobNum)
	if remain <= 0 {
		return
	}

	tasks, _ := s.schedule(s.readyReadTasks, remain, Params.QueryNodeCfg.MaxReadConcurrency)

	atomic.AddInt32(&s.readConcurrency, int32(len(tasks)))
	for _, t := range tasks {
		rateCol.rtCounter.add(t, executeQueueType)
		go func(t readTask) {
			s.processReadTask(t)
			atomic.AddInt32(&s.readConcurrency, -1)
			select {
			case s.notifyChan <- struct{}{}:
			default:
			}
		}(t)
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
	s.unsolveLock.Lock()
	defer s.unsolveLock.Unlock()
	var next *list.Element
	for e := s.unsolvedReadTasks.Front(); e != nil; e = next {
		next = e.Next()
		t, ok := e.Value.(readTask)
		if !ok {
			s.unsolvedReadTasks.Remove(e)
			rateCol.rtCounter.sub(t, unsolvedQueueType)
			continue
		}
		ready, err := t.Ready()
		if err != nil {
			s.unsolvedReadTasks.Remove(e)
			rateCol.rtCounter.sub(t, unsolvedQueueType)
			t.Notify(err)
			continue
		}
		if ready {
			if !Params.QueryNodeCfg.GroupEnabled {
				s.readyReadTasks.PushBack(t)
				rateCol.rtCounter.add(t, readyQueueType)
			} else {
				merged := false
				var prev *list.Element
				for m := s.readyReadTasks.Back(); m != nil; m = prev {
					prev = m.Prev()
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
					rateCol.rtCounter.add(t, readyQueueType)
				}
			}
			s.unsolvedReadTasks.Remove(e)
			rateCol.rtCounter.sub(t, unsolvedQueueType)
		}
	}

	metrics.QueryNodeReadTaskUnsolveLen.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Set(float64(s.unsolvedReadTasks.Len()))
	metrics.QueryNodeReadTaskReadyLen.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Set(float64(s.readyReadTasks.Len()))
}
