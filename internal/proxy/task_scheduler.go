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

package proxy

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/opentracing/opentracing-go"
	oplog "github.com/opentracing/opentracing-go/log"
)

type taskQueue interface {
	utChan() <-chan int
	utEmpty() bool
	utFull() bool
	addUnissuedTask(t task) error
	FrontUnissuedTask() task
	PopUnissuedTask() task
	AddActiveTask(t task)
	PopActiveTask(taskID UniqueID) task
	getTaskByReqID(reqID UniqueID) task
	Enqueue(t task) error
	setMaxTaskNum(num int64)
	getMaxTaskNum() int64
}

// make sure baseTaskQueue implements taskQueue.
var _ taskQueue = (*baseTaskQueue)(nil)

// baseTaskQueue implements taskQueue.
type baseTaskQueue struct {
	unissuedTasks *list.List
	activeTasks   map[UniqueID]task
	utLock        sync.RWMutex
	atLock        sync.RWMutex

	// maxTaskNum should keep still
	maxTaskNum    int64
	maxTaskNumMtx sync.RWMutex

	utBufChan chan int // to block scheduler

	tsoAllocatorIns tsoAllocator
	idAllocatorIns  idAllocatorInterface
}

func (queue *baseTaskQueue) utChan() <-chan int {
	return queue.utBufChan
}

func (queue *baseTaskQueue) utEmpty() bool {
	queue.utLock.RLock()
	defer queue.utLock.RUnlock()
	return queue.unissuedTasks.Len() == 0
}

func (queue *baseTaskQueue) utFull() bool {
	return int64(queue.unissuedTasks.Len()) >= queue.getMaxTaskNum()
}

func (queue *baseTaskQueue) addUnissuedTask(t task) error {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	if queue.utFull() {
		return errors.New("task queue is full")
	}
	queue.unissuedTasks.PushBack(t)
	queue.utBufChan <- 1
	return nil
}

func (queue *baseTaskQueue) FrontUnissuedTask() task {
	queue.utLock.RLock()
	defer queue.utLock.RUnlock()

	if queue.unissuedTasks.Len() <= 0 {
		return nil
	}

	return queue.unissuedTasks.Front().Value.(task)
}

func (queue *baseTaskQueue) PopUnissuedTask() task {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	if queue.unissuedTasks.Len() <= 0 {
		return nil
	}

	ft := queue.unissuedTasks.Front()
	queue.unissuedTasks.Remove(ft)

	return ft.Value.(task)
}

func (queue *baseTaskQueue) AddActiveTask(t task) {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()
	tID := t.ID()
	_, ok := queue.activeTasks[tID]
	if ok {
		log.Warn("Proxy task with tID already in active task list!", zap.Any("ID", tID))
	}

	queue.activeTasks[tID] = t
}

func (queue *baseTaskQueue) PopActiveTask(taskID UniqueID) task {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()
	t, ok := queue.activeTasks[taskID]
	if ok {
		delete(queue.activeTasks, taskID)
		return t
	}

	log.Warn("Proxy task not in active task list! ts", zap.Any("taskID", taskID))
	return t
}

func (queue *baseTaskQueue) getTaskByReqID(reqID UniqueID) task {
	queue.utLock.RLock()
	for e := queue.unissuedTasks.Front(); e != nil; e = e.Next() {
		if e.Value.(task).ID() == reqID {
			queue.utLock.RUnlock()
			return e.Value.(task)
		}
	}
	queue.utLock.RUnlock()

	queue.atLock.RLock()
	for tID, t := range queue.activeTasks {
		if tID == reqID {
			queue.atLock.RUnlock()
			return t
		}
	}
	queue.atLock.RUnlock()
	return nil
}

func (queue *baseTaskQueue) Enqueue(t task) error {
	err := t.OnEnqueue()
	if err != nil {
		return err
	}

	ts, err := queue.tsoAllocatorIns.AllocOne()
	if err != nil {
		return err
	}
	t.SetTs(ts)

	reqID, err := queue.idAllocatorIns.AllocOne()
	if err != nil {
		return err
	}
	t.SetID(reqID)

	return queue.addUnissuedTask(t)
}

func (queue *baseTaskQueue) setMaxTaskNum(num int64) {
	queue.maxTaskNumMtx.Lock()
	defer queue.maxTaskNumMtx.Unlock()

	queue.maxTaskNum = num
}

func (queue *baseTaskQueue) getMaxTaskNum() int64 {
	queue.maxTaskNumMtx.RLock()
	defer queue.maxTaskNumMtx.RUnlock()

	return queue.maxTaskNum
}

func newBaseTaskQueue(tsoAllocatorIns tsoAllocator, idAllocatorIns idAllocatorInterface) *baseTaskQueue {
	return &baseTaskQueue{
		unissuedTasks:   list.New(),
		activeTasks:     make(map[UniqueID]task),
		utLock:          sync.RWMutex{},
		atLock:          sync.RWMutex{},
		maxTaskNum:      Params.ProxyCfg.MaxTaskNum,
		utBufChan:       make(chan int, Params.ProxyCfg.MaxTaskNum),
		tsoAllocatorIns: tsoAllocatorIns,
		idAllocatorIns:  idAllocatorIns,
	}
}

type ddTaskQueue struct {
	*baseTaskQueue
	lock sync.Mutex
}

type pChanStatInfo struct {
	pChanStatistics
	tsSet map[Timestamp]struct{}
}

type dmTaskQueue struct {
	*baseTaskQueue
	lock sync.Mutex

	statsLock            sync.RWMutex
	pChanStatisticsInfos map[pChan]*pChanStatInfo
}

func (queue *dmTaskQueue) Enqueue(t task) error {
	queue.statsLock.Lock()
	defer queue.statsLock.Unlock()
	err := queue.baseTaskQueue.Enqueue(t)
	if err != nil {
		return err
	}
	err = queue.addPChanStats(t)
	if err != nil {
		return err
	}

	return nil
}

func (queue *dmTaskQueue) PopActiveTask(taskID UniqueID) task {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()
	t, ok := queue.activeTasks[taskID]
	if ok {
		queue.statsLock.Lock()
		defer queue.statsLock.Unlock()

		delete(queue.activeTasks, taskID)
		log.Debug("Proxy dmTaskQueue popPChanStats", zap.Any("taskID", t.ID()))
		queue.popPChanStats(t)
	} else {
		log.Info("Proxy task not in active task list!", zap.Any("taskID", taskID))
	}
	return t
}

func (queue *dmTaskQueue) addPChanStats(t task) error {
	if dmT, ok := t.(dmlTask); ok {
		stats, err := dmT.getPChanStats()
		if err != nil {
			log.Warn("Proxy dmTaskQueue addPChanStats", zap.Any("tID", t.ID()),
				zap.Any("stats", stats), zap.Error(err))
			return err
		}
		for cName, stat := range stats {
			info, ok := queue.pChanStatisticsInfos[cName]
			if !ok {
				info = &pChanStatInfo{
					pChanStatistics: stat,
					tsSet: map[Timestamp]struct{}{
						stat.minTs: {},
					},
				}
				queue.pChanStatisticsInfos[cName] = info
			} else {
				if info.minTs > stat.minTs {
					queue.pChanStatisticsInfos[cName].minTs = stat.minTs
				}
				if info.maxTs < stat.maxTs {
					queue.pChanStatisticsInfos[cName].maxTs = stat.maxTs
				}
				queue.pChanStatisticsInfos[cName].tsSet[info.minTs] = struct{}{}
			}
		}
	} else {
		return fmt.Errorf("proxy addUnissuedTask reflect to dmlTask failed, tID:%v", t.ID())
	}
	return nil
}

func (queue *dmTaskQueue) popPChanStats(t task) error {
	if dmT, ok := t.(dmlTask); ok {
		channels, err := dmT.getChannels()
		if err != nil {
			return err
		}
		for _, cName := range channels {
			info, ok := queue.pChanStatisticsInfos[cName]
			if ok {
				delete(queue.pChanStatisticsInfos[cName].tsSet, info.minTs)
				if len(queue.pChanStatisticsInfos[cName].tsSet) <= 0 {
					delete(queue.pChanStatisticsInfos, cName)
				} else if queue.pChanStatisticsInfos[cName].minTs == info.minTs {
					minTs := info.maxTs
					for ts := range queue.pChanStatisticsInfos[cName].tsSet {
						if ts < minTs {
							minTs = ts
						}
					}
					queue.pChanStatisticsInfos[cName].minTs = minTs
				}
			}
		}
	} else {
		return fmt.Errorf("proxy dmTaskQueue popPChanStats reflect to dmlTask failed, tID:%v", t.ID())
	}
	return nil
}

func (queue *dmTaskQueue) getPChanStatsInfo() (map[pChan]*pChanStatistics, error) {

	ret := make(map[pChan]*pChanStatistics)
	queue.statsLock.RLock()
	defer queue.statsLock.RUnlock()
	for cName, info := range queue.pChanStatisticsInfos {
		ret[cName] = &pChanStatistics{
			minTs: info.minTs,
			maxTs: info.maxTs,
		}
	}
	return ret, nil
}

type dqTaskQueue struct {
	*baseTaskQueue
}

func (queue *ddTaskQueue) Enqueue(t task) error {
	queue.lock.Lock()
	defer queue.lock.Unlock()
	return queue.baseTaskQueue.Enqueue(t)
}

func newDdTaskQueue(tsoAllocatorIns tsoAllocator, idAllocatorIns idAllocatorInterface) *ddTaskQueue {
	return &ddTaskQueue{
		baseTaskQueue: newBaseTaskQueue(tsoAllocatorIns, idAllocatorIns),
	}
}

func newDmTaskQueue(tsoAllocatorIns tsoAllocator, idAllocatorIns idAllocatorInterface) *dmTaskQueue {
	return &dmTaskQueue{
		baseTaskQueue:        newBaseTaskQueue(tsoAllocatorIns, idAllocatorIns),
		pChanStatisticsInfos: make(map[pChan]*pChanStatInfo),
	}
}

func newDqTaskQueue(tsoAllocatorIns tsoAllocator, idAllocatorIns idAllocatorInterface) *dqTaskQueue {
	return &dqTaskQueue{
		baseTaskQueue: newBaseTaskQueue(tsoAllocatorIns, idAllocatorIns),
	}
}

// taskScheduler schedules the gRPC tasks.
type taskScheduler struct {
	ddQueue *ddTaskQueue
	dmQueue *dmTaskQueue
	dqQueue *dqTaskQueue

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	msFactory msgstream.Factory
}

type schedOpt func(*taskScheduler)

func newTaskScheduler(ctx context.Context,
	idAllocatorIns idAllocatorInterface,
	tsoAllocatorIns tsoAllocator,
	factory msgstream.Factory,
	opts ...schedOpt,
) (*taskScheduler, error) {
	ctx1, cancel := context.WithCancel(ctx)
	s := &taskScheduler{
		ctx:       ctx1,
		cancel:    cancel,
		msFactory: factory,
	}
	s.ddQueue = newDdTaskQueue(tsoAllocatorIns, idAllocatorIns)
	s.dmQueue = newDmTaskQueue(tsoAllocatorIns, idAllocatorIns)
	s.dqQueue = newDqTaskQueue(tsoAllocatorIns, idAllocatorIns)

	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}

func (sched *taskScheduler) scheduleDdTask() task {
	return sched.ddQueue.PopUnissuedTask()
}

func (sched *taskScheduler) scheduleDmTask() task {
	return sched.dmQueue.PopUnissuedTask()
}

func (sched *taskScheduler) scheduleDqTask() task {
	return sched.dqQueue.PopUnissuedTask()
}

func (sched *taskScheduler) getTaskByReqID(reqID UniqueID) task {
	if t := sched.ddQueue.getTaskByReqID(reqID); t != nil {
		return t
	}
	if t := sched.dmQueue.getTaskByReqID(reqID); t != nil {
		return t
	}
	if t := sched.dqQueue.getTaskByReqID(reqID); t != nil {
		return t
	}
	return nil
}

func (sched *taskScheduler) processTask(t task, q taskQueue) {
	span, ctx := trace.StartSpanFromContext(t.TraceCtx(),
		opentracing.Tags{
			"Type": t.Name(),
			"ID":   t.ID(),
		})
	defer span.Finish()
	traceID, _, _ := trace.InfoFromSpan(span)

	span.LogFields(oplog.Int64("scheduler process AddActiveTask", t.ID()))
	q.AddActiveTask(t)

	defer func() {
		span.LogFields(oplog.Int64("scheduler process PopActiveTask", t.ID()))
		q.PopActiveTask(t.ID())
	}()
	span.LogFields(oplog.Int64("scheduler process PreExecute", t.ID()))

	err := t.PreExecute(ctx)

	defer func() {
		t.Notify(err)
	}()
	if err != nil {
		trace.LogError(span, err)
		log.Warn("Failed to pre-execute task: ",
			zap.String("traceID", traceID), zap.Error(err))
		return
	}

	span.LogFields(oplog.Int64("scheduler process Execute", t.ID()))
	err = t.Execute(ctx)
	if err != nil {
		trace.LogError(span, err)
		log.Warn("Failed to execute task: ",
			zap.String("traceID", traceID), zap.Error(err))
		return
	}

	span.LogFields(oplog.Int64("scheduler process PostExecute", t.ID()))
	err = t.PostExecute(ctx)

	if err != nil {
		trace.LogError(span, err)
		log.Warn("Failed to post-execute task: ",
			zap.String("traceID", traceID), zap.Error(err))
		return
	}
}

// definitionLoop schedules the ddl tasks.
func (sched *taskScheduler) definitionLoop() {
	defer sched.wg.Done()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.ddQueue.utChan():
			if !sched.ddQueue.utEmpty() {
				t := sched.scheduleDdTask()
				sched.processTask(t, sched.ddQueue)
			}
		}
	}
}

func (sched *taskScheduler) manipulationLoop() {
	defer sched.wg.Done()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.dmQueue.utChan():
			if !sched.dmQueue.utEmpty() {
				t := sched.scheduleDmTask()
				go sched.processTask(t, sched.dmQueue)
			}
		}
	}
}

func (sched *taskScheduler) queryLoop() {
	defer sched.wg.Done()

	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.dqQueue.utChan():
			if !sched.dqQueue.utEmpty() {
				t := sched.scheduleDqTask()
				go sched.processTask(t, sched.dqQueue)
			}
		}
	}
}

func (sched *taskScheduler) Start() error {
	sched.wg.Add(1)
	go sched.definitionLoop()

	sched.wg.Add(1)
	go sched.manipulationLoop()

	sched.wg.Add(1)
	go sched.queryLoop()

	return nil
}

func (sched *taskScheduler) Close() {
	sched.cancel()
	sched.wg.Wait()
}

func (sched *taskScheduler) getPChanStatistics() (map[pChan]*pChanStatistics, error) {
	return sched.dmQueue.getPChanStatsInfo()
}
