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
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
		return merr.WrapErrTooManyRequests(int32(queue.getMaxTaskNum()))
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
		log.Ctx(t.TraceCtx()).Warn("Proxy task with tID already in active task list!", zap.Int64("ID", tID))
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

	log.Ctx(context.TODO()).Warn("Proxy task not in active task list! ts", zap.Int64("taskID", taskID))
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

	var ts Timestamp
	var id UniqueID
	if t.CanSkipAllocTimestamp() {
		ts = tsoutil.ComposeTS(time.Now().UnixMilli(), 0)
		id, err = globalMetaCache.AllocID(t.TraceCtx())
		if err != nil {
			return err
		}
	} else {
		ts, err = queue.tsoAllocatorIns.AllocOne(t.TraceCtx())
		if err != nil {
			return err
		}
		// we always use same msg id and ts for now.
		id = UniqueID(ts)
	}
	t.SetTs(ts)
	t.SetID(id)

	t.SetOnEnqueueTime()
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

// ddTaskQueue represents queue for DDL task such as createCollection/createPartition/dropCollection/dropPartition/hasCollection/hasPartition
type ddTaskQueue struct {
	*baseTaskQueue
	lock sync.Mutex
}

func (queue *ddTaskQueue) updateMetrics() {
	queue.utLock.RLock()
	unissuedTasksNum := queue.unissuedTasks.Len()
	queue.utLock.RUnlock()
	queue.atLock.RLock()
	activateTaskNum := len(queue.activeTasks)
	queue.atLock.RUnlock()

	metrics.ProxyQueueTaskNum.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "ddl", metrics.UnissuedIndexTaskLabel).Set(float64(unissuedTasksNum))
	metrics.ProxyQueueTaskNum.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "ddl", metrics.InProgressIndexTaskLabel).Set(float64(activateTaskNum))
}

type pChanStatInfo struct {
	pChanStatistics
	tsSet map[Timestamp]struct{}
}

// dmTaskQueue represents queue for DML task such as insert/delete/upsert
type dmTaskQueue struct {
	*baseTaskQueue

	statsLock            sync.RWMutex
	pChanStatisticsInfos map[pChan]*pChanStatInfo
}

func (queue *dmTaskQueue) updateMetrics() {
	queue.utLock.RLock()
	unissuedTasksNum := queue.unissuedTasks.Len()
	queue.utLock.RUnlock()
	queue.atLock.RLock()
	activateTaskNum := len(queue.activeTasks)
	queue.atLock.RUnlock()

	metrics.ProxyQueueTaskNum.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "dml", metrics.UnissuedIndexTaskLabel).Set(float64(unissuedTasksNum))
	metrics.ProxyQueueTaskNum.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "dml", metrics.InProgressIndexTaskLabel).Set(float64(activateTaskNum))
}

func (queue *dmTaskQueue) Enqueue(t task) error {
	// This statsLock has two functions:
	//	1) Protect member pChanStatisticsInfos
	//	2) Serialize the timestamp allocation for dml tasks

	// 1. set the current pChannels for this dmTask
	dmt := t.(dmlTask)
	err := dmt.setChannels()
	if err != nil {
		log.Ctx(t.TraceCtx()).Warn("setChannels failed when Enqueue", zap.Int64("taskID", t.ID()), zap.Error(err))
		return err
	}

	// 2. enqueue dml task
	queue.statsLock.Lock()
	defer queue.statsLock.Unlock()
	err = queue.baseTaskQueue.Enqueue(t)
	if err != nil {
		return err
	}
	// 3. commit will use pChannels got previously when preAdding and will definitely succeed
	pChannels := dmt.getChannels()
	queue.commitPChanStats(dmt, pChannels)
	// there's indeed a possibility that the collection info cache was expired after preAddPChanStats
	// but considering root coord knows everything about meta modification, invalid stats appended after the meta changed
	// will be discarded by root coord and will not lead to inconsistent state
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
		log.Ctx(t.TraceCtx()).Debug("Proxy dmTaskQueue popPChanStats", zap.Int64("taskID", t.ID()))
		queue.popPChanStats(t)
	} else {
		log.Ctx(context.TODO()).Warn("Proxy task not in active task list!", zap.Int64("taskID", taskID))
	}
	return t
}

func (queue *dmTaskQueue) commitPChanStats(dmt dmlTask, pChannels []pChan) {
	// 1. prepare new stat for all pChannels
	newStats := make(map[pChan]pChanStatistics)
	beginTs := dmt.BeginTs()
	endTs := dmt.EndTs()
	for _, channel := range pChannels {
		newStats[channel] = pChanStatistics{
			minTs: beginTs,
			maxTs: endTs,
		}
	}
	// 2. update stats for all pChannels
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

// dqTaskQueue represents queue for DQL task such as search/query
type dqTaskQueue struct {
	*baseTaskQueue
}

func (queue *dqTaskQueue) updateMetrics() {
	queue.utLock.RLock()
	unissuedTasksNum := queue.unissuedTasks.Len()
	queue.utLock.RUnlock()
	queue.atLock.RLock()
	activateTaskNum := len(queue.activeTasks)
	queue.atLock.RUnlock()

	metrics.ProxyQueueTaskNum.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "dql", metrics.UnissuedIndexTaskLabel).Set(float64(unissuedTasksNum))
	metrics.ProxyQueueTaskNum.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "dql", metrics.InProgressIndexTaskLabel).Set(float64(activateTaskNum))
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

	// data control queue, use for such as flush operation, which control the data status
	dcQueue *ddTaskQueue

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

	s.dcQueue = newDdTaskQueue(tsoAllocatorIns)

	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}

func (sched *taskScheduler) scheduleDdTask() task {
	return sched.ddQueue.PopUnissuedTask()
}

func (sched *taskScheduler) scheduleDcTask() task {
	return sched.dcQueue.PopUnissuedTask()
}

func (sched *taskScheduler) scheduleDmTask() task {
	return sched.dmQueue.PopUnissuedTask()
}

func (sched *taskScheduler) scheduleDqTask() task {
	return sched.dqQueue.PopUnissuedTask()
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

	waitDuration := t.GetDurationInQueue()
	metrics.ProxyReqInQueueLatency.
		WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), t.Type().String()).
		Observe(float64(waitDuration.Milliseconds()))

	err := t.PreExecute(ctx)

	defer func() {
		t.Notify(err)
	}()
	if err != nil {
		span.RecordError(err)
		log.Ctx(ctx).Warn("Failed to pre-execute task: " + err.Error())
		return
	}

	span.AddEvent("scheduler process Execute")
	err = t.Execute(ctx)
	if err != nil {
		span.RecordError(err)
		log.Ctx(ctx).Warn("Failed to execute task: ", zap.Error(err))
		return
	}

	span.AddEvent("scheduler process PostExecute")
	err = t.PostExecute(ctx)
	if err != nil {
		span.RecordError(err)
		log.Ctx(ctx).Warn("Failed to post-execute task: ", zap.Error(err))
		return
	}
}

// definitionLoop schedules the ddl tasks.
func (sched *taskScheduler) definitionLoop() {
	defer sched.wg.Done()

	pool := conc.NewPool[struct{}](paramtable.Get().ProxyCfg.DDLConcurrency.GetAsInt(), conc.WithExpiryDuration(time.Minute))
	defer pool.Release()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.ddQueue.utChan():
			if !sched.ddQueue.utEmpty() {
				t := sched.scheduleDdTask()
				pool.Submit(func() (struct{}, error) {
					sched.processTask(t, sched.ddQueue)
					return struct{}{}, nil
				})
			}
			sched.ddQueue.updateMetrics()
		}
	}
}

// controlLoop schedule the data control operation, such as flush
func (sched *taskScheduler) controlLoop() {
	defer sched.wg.Done()

	pool := conc.NewPool[struct{}](paramtable.Get().ProxyCfg.DCLConcurrency.GetAsInt(), conc.WithExpiryDuration(time.Minute))
	defer pool.Release()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.dcQueue.utChan():
			if !sched.dcQueue.utEmpty() {
				t := sched.scheduleDcTask()
				pool.Submit(func() (struct{}, error) {
					sched.processTask(t, sched.dcQueue)
					return struct{}{}, nil
				})
			}
			sched.dcQueue.updateMetrics()
		}
	}
}

func (sched *taskScheduler) manipulationLoop() {
	defer sched.wg.Done()
	pool := conc.NewPool[struct{}](paramtable.Get().ProxyCfg.MaxTaskNum.GetAsInt())
	defer pool.Release()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.dmQueue.utChan():
			if !sched.dmQueue.utEmpty() {
				t := sched.scheduleDmTask()
				pool.Submit(func() (struct{}, error) {
					sched.processTask(t, sched.dmQueue)
					return struct{}{}, nil
				})
			}
			sched.dmQueue.updateMetrics()
		}
	}
}

func (sched *taskScheduler) queryLoop() {
	defer sched.wg.Done()

	poolSize := paramtable.Get().ProxyCfg.MaxTaskNum.GetAsInt()
	pool := conc.NewPool[struct{}](poolSize, conc.WithExpiryDuration(time.Minute))
	subTaskPool := conc.NewPool[struct{}](poolSize, conc.WithExpiryDuration(time.Minute))
	defer pool.Release()
	defer subTaskPool.Release()

	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.dqQueue.utChan():
			if !sched.dqQueue.utEmpty() {
				t := sched.scheduleDqTask()
				p := pool
				// if task is sub task spawned by another, use sub task pool in case of deadlock
				if t.IsSubTask() {
					p = subTaskPool
				}
				p.Submit(func() (struct{}, error) {
					sched.processTask(t, sched.dqQueue)
					return struct{}{}, nil
				})
			} else {
				log.Ctx(context.TODO()).Debug("query queue is empty ...")
			}
			sched.dqQueue.updateMetrics()
		}
	}
}

func (sched *taskScheduler) Start() error {
	sched.wg.Add(1)
	go sched.definitionLoop()

	sched.wg.Add(1)
	go sched.controlLoop()

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
