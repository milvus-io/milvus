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
	"sync"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
		log.Warn("Proxy task with tID already in active task list!", zap.Int64("ID", tID))
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

	log.Warn("Proxy task not in active task list! ts", zap.Int64("taskID", taskID))
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

	ts, err := queue.tsoAllocatorIns.AllocOne(t.TraceCtx())
	if err != nil {
		return err
	}
	t.SetTs(ts)

	// we always use same msg id and ts for now.
	t.SetID(UniqueID(ts))

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

func newBaseTaskQueue(tsoAllocatorIns tsoAllocator) *baseTaskQueue {
	return &baseTaskQueue{
		unissuedTasks:   list.New(),
		activeTasks:     make(map[UniqueID]task),
		utLock:          sync.RWMutex{},
		atLock:          sync.RWMutex{},
		maxTaskNum:      Params.ProxyCfg.MaxTaskNum.GetAsInt64(),
		utBufChan:       make(chan int, Params.ProxyCfg.MaxTaskNum.GetAsInt()),
		tsoAllocatorIns: tsoAllocatorIns,
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

	statsLock            sync.RWMutex
	pChanStatisticsInfos map[pChan]*pChanStatInfo
}

func (queue *dmTaskQueue) Enqueue(t task) error {
	// This statsLock has two functions:
	//	1) Protect member pChanStatisticsInfos
	//	2) Serialize the timestamp allocation for dml tasks

	//1. set the current pChannels for this dmTask
	dmt := t.(dmlTask)
	err := dmt.setChannels()
	if err != nil {
		log.Warn("setChannels failed when Enqueue", zap.Int64("taskID", t.ID()), zap.Error(err))
		return err
	}

	//2. enqueue dml task
	queue.statsLock.Lock()
	defer queue.statsLock.Unlock()
	err = queue.baseTaskQueue.Enqueue(t)
	if err != nil {
		return err
	}
	//3. commit will use pChannels got previously when preAdding and will definitely succeed
	pChannels := dmt.getChannels()
	queue.commitPChanStats(dmt, pChannels)
	//there's indeed a possibility that the collection info cache was expired after preAddPChanStats
	//but considering root coord knows everything about meta modification, invalid stats appended after the meta changed
	//will be discarded by root coord and will not lead to inconsistent state
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
		log.Warn("Proxy task not in active task list!", zap.Any("taskID", taskID))
	}
	return t
}

func (queue *dmTaskQueue) commitPChanStats(dmt dmlTask, pChannels []pChan) {
	//1. prepare new stat for all pChannels
	newStats := make(map[pChan]pChanStatistics)
	beginTs := dmt.BeginTs()
	endTs := dmt.EndTs()
	for _, channel := range pChannels {
		newStats[channel] = pChanStatistics{
			minTs: beginTs,
			maxTs: endTs,
		}
	}
	//2. update stats for all pChannels
	for cName, newStat := range newStats {
		currentStat, ok := queue.pChanStatisticsInfos[cName]
		if !ok {
			currentStat = &pChanStatInfo{
				pChanStatistics: newStat,
				tsSet: map[Timestamp]struct{}{
					newStat.minTs: {},
				},
			}
			queue.pChanStatisticsInfos[cName] = currentStat
		} else {
			if currentStat.minTs > newStat.minTs {
				currentStat.minTs = newStat.minTs
			}
			if currentStat.maxTs < newStat.maxTs {
				currentStat.maxTs = newStat.maxTs
			}
			currentStat.tsSet[newStat.minTs] = struct{}{}
		}
	}
}

func (queue *dmTaskQueue) popPChanStats(t task) {
	channels := t.(dmlTask).getChannels()
	taskTs := t.BeginTs()
	for _, cName := range channels {
		info, ok := queue.pChanStatisticsInfos[cName]
		if ok {
			delete(info.tsSet, taskTs)
			if len(info.tsSet) <= 0 {
				delete(queue.pChanStatisticsInfos, cName)
			} else {
				newMinTs := info.maxTs
				for ts := range info.tsSet {
					if newMinTs > ts {
						newMinTs = ts
					}
				}
				info.minTs = newMinTs
			}
		}
	}
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

func newDdTaskQueue(tsoAllocatorIns tsoAllocator) *ddTaskQueue {
	return &ddTaskQueue{
		baseTaskQueue: newBaseTaskQueue(tsoAllocatorIns),
	}
}

func newDmTaskQueue(tsoAllocatorIns tsoAllocator) *dmTaskQueue {
	return &dmTaskQueue{
		baseTaskQueue:        newBaseTaskQueue(tsoAllocatorIns),
		pChanStatisticsInfos: make(map[pChan]*pChanStatInfo),
	}
}

func newDqTaskQueue(tsoAllocatorIns tsoAllocator) *dqTaskQueue {
	return &dqTaskQueue{
		baseTaskQueue: newBaseTaskQueue(tsoAllocatorIns),
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
	s.ddQueue = newDdTaskQueue(tsoAllocatorIns)
	s.dmQueue = newDmTaskQueue(tsoAllocatorIns)
	s.dqQueue = newDqTaskQueue(tsoAllocatorIns)

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
	ctx, span := otel.Tracer(typeutil.ProxyRole).Start(t.TraceCtx(), t.Name())
	defer span.End()

	span.AddEvent("scheduler process AddActiveTask")
	q.AddActiveTask(t)

	defer func() {
		span.AddEvent("scheduler process PopActiveTask")
		q.PopActiveTask(t.ID())
	}()
	span.AddEvent("scheduler process PreExecute")

	err := t.PreExecute(ctx)

	defer func() {
		t.Notify(err)
	}()
	if err != nil {
		span.RecordError(err)
		log.Ctx(ctx).Error("Failed to pre-execute task: " + err.Error())
		return
	}

	span.AddEvent("scheduler process Execute")
	err = t.Execute(ctx)
	if err != nil {
		span.RecordError(err)
		log.Ctx(ctx).Error("Failed to execute task: ", zap.Error(err))
		return
	}

	span.AddEvent("scheduler process PostExecute")
	err = t.PostExecute(ctx)

	if err != nil {
		span.RecordError(err)
		log.Ctx(ctx).Error("Failed to post-execute task: ", zap.Error(err))
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
			} else {
				log.Debug("query queue is empty ...")
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
