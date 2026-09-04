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

package scheduler

import (
	"container/list"
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"

	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type TaskQueue interface {
	utChan() <-chan int
	utEmpty() bool
	utFull() bool
	addUnissuedTask(t taskmodel.Task) error
	FrontUnissuedTask() taskmodel.Task
	PopUnissuedTask() taskmodel.Task
	AddActiveTask(t taskmodel.Task)
	PopActiveTask(taskID taskmodel.UniqueID) taskmodel.Task
	getTaskByReqID(reqID taskmodel.UniqueID) taskmodel.Task
	Enqueue(t taskmodel.Task) error
	SetMaxTaskNum(num int64)
	GetMaxTaskNum() int64
}

// make sure BaseTaskQueue implements TaskQueue.
var _ TaskQueue = (*BaseTaskQueue)(nil)

// BaseTaskQueue implements TaskQueue.
type BaseTaskQueue struct {
	unissuedTasks *list.List
	activeTasks   map[taskmodel.UniqueID]taskmodel.Task
	utLock        sync.RWMutex
	atLock        sync.RWMutex

	// maxTaskNum should keep still
	maxTaskNum    int64
	maxTaskNumMtx sync.RWMutex

	utBufChan chan int // to block scheduler

	tsoAllocatorIns taskmodel.TsoAllocator
}

func (queue *BaseTaskQueue) utChan() <-chan int {
	return queue.utBufChan
}

func (queue *BaseTaskQueue) utEmpty() bool {
	queue.utLock.RLock()
	defer queue.utLock.RUnlock()
	return queue.unissuedTasks.Len() == 0
}

func (queue *BaseTaskQueue) utFull() bool {
	return int64(queue.unissuedTasks.Len()) >= queue.GetMaxTaskNum()
}

// IsFull is the lock-acquiring counterpart of utFull; utFull assumes the
// caller already holds utLock.
func (queue *BaseTaskQueue) IsFull() bool {
	queue.utLock.RLock()
	defer queue.utLock.RUnlock()
	return queue.utFull()
}

func (queue *BaseTaskQueue) addUnissuedTask(t taskmodel.Task) error {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	if queue.utFull() {
		return merr.WrapErrTooManyRequests(int32(queue.GetMaxTaskNum()))
	}
	queue.unissuedTasks.PushBack(t)
	// utBufChan is an edge-triggered, capacity-1 notifier: a pending token
	// means "the unissued list is non-empty, wake the scheduler". Concurrent
	// sends coalesce; the scheduler drains the list on each wake.
	select {
	case queue.utBufChan <- 1:
	default:
	}
	return nil
}

func (queue *BaseTaskQueue) FrontUnissuedTask() taskmodel.Task {
	queue.utLock.RLock()
	defer queue.utLock.RUnlock()

	if queue.unissuedTasks.Len() <= 0 {
		return nil
	}

	return queue.unissuedTasks.Front().Value.(taskmodel.Task)
}

func (queue *BaseTaskQueue) PopUnissuedTask() taskmodel.Task {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	if queue.unissuedTasks.Len() <= 0 {
		return nil
	}

	ft := queue.unissuedTasks.Front()
	queue.unissuedTasks.Remove(ft)

	return ft.Value.(taskmodel.Task)
}

func (queue *BaseTaskQueue) popUnissuedTasks(filter func(taskmodel.Task) bool) []taskmodel.Task {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	removed := make([]taskmodel.Task, 0)
	for e := queue.unissuedTasks.Front(); e != nil; {
		next := e.Next()
		t := e.Value.(taskmodel.Task)
		if filter == nil || filter(t) {
			queue.unissuedTasks.Remove(e)
			removed = append(removed, t)
		}
		e = next
	}
	return removed
}

func (queue *BaseTaskQueue) AddActiveTask(t taskmodel.Task) {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()
	tID := t.ID()
	_, ok := queue.activeTasks[tID]
	if ok {
		mlog.Warn(t.TraceCtx(), "Proxy task with tID already in active task list!", mlog.Int64("ID", tID))
	}

	queue.activeTasks[tID] = t
	t.SetExecutingTime()
}

func (queue *BaseTaskQueue) PopActiveTask(taskID taskmodel.UniqueID) taskmodel.Task {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()
	t, ok := queue.activeTasks[taskID]
	if ok {
		delete(queue.activeTasks, taskID)
		return t
	}
	mlog.Warn(context.TODO(), "Proxy task not in active task list! ts", mlog.FieldTaskID(taskID))
	return t
}

func (queue *BaseTaskQueue) getTaskByReqID(reqID taskmodel.UniqueID) taskmodel.Task {
	queue.utLock.RLock()
	for e := queue.unissuedTasks.Front(); e != nil; e = e.Next() {
		if e.Value.(taskmodel.Task).ID() == reqID {
			queue.utLock.RUnlock()
			return e.Value.(taskmodel.Task)
		}
	}
	queue.utLock.RUnlock()

	queue.atLock.RLock()
	t, ok := queue.activeTasks[reqID]
	queue.atLock.RUnlock()
	if ok {
		return t
	}
	return nil
}

func (queue *BaseTaskQueue) Enqueue(t taskmodel.Task) error {
	err := t.OnEnqueue()
	if err != nil {
		return err
	}

	// Fast-fail when the queue is already full, before any potentially-blocking
	// allocation. The authoritative check remains in addUnissuedTask; this
	// snapshot only prevents a rejected request from queuing behind a slow
	// TSO/ID allocator (#49223).
	queue.utLock.RLock()
	full := queue.utFull()
	queue.utLock.RUnlock()
	if full {
		return merr.WrapErrTooManyRequests(int32(queue.GetMaxTaskNum()))
	}

	var ts taskmodel.Timestamp
	var id taskmodel.UniqueID
	if t.CanSkipAllocTimestamp() {
		ts = tsoutil.ComposeTS(time.Now().UnixMilli(), 0)
		id, err = t.GetMetaCache().AllocID(t.TraceCtx())
		if err != nil {
			return err
		}
	} else {
		ts, err = queue.tsoAllocatorIns.AllocOne(t.TraceCtx())
		if err != nil {
			return err
		}
		// we always use same msg id and ts for now.
		id = taskmodel.UniqueID(ts)
	}
	t.SetTs(ts)
	t.SetID(id)

	t.SetOnEnqueueTime()
	return queue.addUnissuedTask(t)
}

func (queue *BaseTaskQueue) SetMaxTaskNum(num int64) {
	queue.maxTaskNumMtx.Lock()
	defer queue.maxTaskNumMtx.Unlock()

	queue.maxTaskNum = num
}

func (queue *BaseTaskQueue) GetMaxTaskNum() int64 {
	queue.maxTaskNumMtx.RLock()
	defer queue.maxTaskNumMtx.RUnlock()

	return queue.maxTaskNum
}

func newBaseTaskQueue(tsoAllocatorIns taskmodel.TsoAllocator) *BaseTaskQueue {
	return &BaseTaskQueue{
		unissuedTasks:   list.New(),
		activeTasks:     make(map[taskmodel.UniqueID]taskmodel.Task),
		utLock:          sync.RWMutex{},
		atLock:          sync.RWMutex{},
		maxTaskNum:      paramtable.Get().ProxyCfg.MaxTaskNum.GetAsInt64(),
		utBufChan:       make(chan int, 1),
		tsoAllocatorIns: tsoAllocatorIns,
	}
}

// DdTaskQueue represents queue for DDL task such as createCollection/createPartition/dropCollection/dropPartition/hasCollection/hasPartition
type DdTaskQueue struct {
	*BaseTaskQueue
	lock sync.Mutex
}

func (queue *DdTaskQueue) updateMetrics() {
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
	taskmodel.PChanStatistics
	tsSet map[taskmodel.Timestamp]struct{}
}

// DmTaskQueue represents queue for DML task such as insert/delete/upsert
type DmTaskQueue struct {
	*BaseTaskQueue

	statsLock            sync.RWMutex
	pChanStatisticsInfos map[taskmodel.PChan]*pChanStatInfo
}

func (queue *DmTaskQueue) updateMetrics() {
	queue.utLock.RLock()
	unissuedTasksNum := queue.unissuedTasks.Len()
	queue.utLock.RUnlock()
	queue.atLock.RLock()
	activateTaskNum := len(queue.activeTasks)
	queue.atLock.RUnlock()

	metrics.ProxyQueueTaskNum.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "dml", metrics.UnissuedIndexTaskLabel).Set(float64(unissuedTasksNum))
	metrics.ProxyQueueTaskNum.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "dml", metrics.InProgressIndexTaskLabel).Set(float64(activateTaskNum))
}

func (queue *DmTaskQueue) Enqueue(t taskmodel.Task) error {
	// This statsLock has two functions:
	//	1) Protect member pChanStatisticsInfos
	//	2) Serialize the timestamp allocation for dml tasks

	// 1. set the current pChannels for this dmTask
	dmt := t.(taskmodel.DMLTask)
	err := dmt.SetChannels()
	if err != nil {
		mlog.Warn(t.TraceCtx(), "setChannels failed when Enqueue", mlog.FieldTaskID(t.ID()), mlog.Err(err))
		return err
	}

	// 2. enqueue dml task
	queue.statsLock.Lock()
	defer queue.statsLock.Unlock()
	err = queue.BaseTaskQueue.Enqueue(t)
	if err != nil {
		return err
	}
	// 3. commit will use pChannels got previously when preAdding and will definitely succeed
	pChannels := dmt.GetChannels()
	queue.commitPChanStats(dmt, pChannels)
	// there's indeed a possibility that the collection info cache was expired after preAddPChanStats
	// but considering root coord knows everything about meta modification, invalid stats appended after the meta changed
	// will be discarded by root coord and will not lead to inconsistent state
	return nil
}

func (queue *DmTaskQueue) PopActiveTask(taskID taskmodel.UniqueID) taskmodel.Task {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()
	t, ok := queue.activeTasks[taskID]
	if ok {
		queue.statsLock.Lock()
		defer queue.statsLock.Unlock()

		delete(queue.activeTasks, taskID)
		mlog.Debug(t.TraceCtx(), "Proxy DmTaskQueue popPChanStats", mlog.FieldTaskID(t.ID()))
		queue.popPChanStats(t)
	} else {
		mlog.Warn(context.TODO(), "Proxy task not in active task list!", mlog.FieldTaskID(taskID))
	}
	return t
}

func (queue *DmTaskQueue) commitPChanStats(dmt taskmodel.DMLTask, pChannels []taskmodel.PChan) {
	// 1. prepare new stat for all pChannels
	newStats := make(map[taskmodel.PChan]taskmodel.PChanStatistics)
	beginTs := dmt.BeginTs()
	endTs := dmt.EndTs()
	for _, channel := range pChannels {
		newStats[channel] = taskmodel.PChanStatistics{
			MinTs: beginTs,
			MaxTs: endTs,
		}
	}
	// 2. update stats for all pChannels
	for cName, newStat := range newStats {
		currentStat, ok := queue.pChanStatisticsInfos[cName]
		if !ok {
			currentStat = &pChanStatInfo{
				PChanStatistics: newStat,
				tsSet: map[taskmodel.Timestamp]struct{}{
					newStat.MinTs: {},
				},
			}
			queue.pChanStatisticsInfos[cName] = currentStat
		} else {
			if currentStat.MinTs > newStat.MinTs {
				currentStat.MinTs = newStat.MinTs
			}
			if currentStat.MaxTs < newStat.MaxTs {
				currentStat.MaxTs = newStat.MaxTs
			}
			currentStat.tsSet[newStat.MinTs] = struct{}{}
		}
	}
}

func (queue *DmTaskQueue) popPChanStats(t taskmodel.Task) {
	channels := t.(taskmodel.DMLTask).GetChannels()
	taskTs := t.BeginTs()
	for _, cName := range channels {
		info, ok := queue.pChanStatisticsInfos[cName]
		if ok {
			delete(info.tsSet, taskTs)
			if len(info.tsSet) <= 0 {
				delete(queue.pChanStatisticsInfos, cName)
			} else {
				newMinTs := info.MaxTs
				for ts := range info.tsSet {
					if newMinTs > ts {
						newMinTs = ts
					}
				}
				info.MinTs = newMinTs
			}
		}
	}
}

func (queue *DmTaskQueue) getPChanStatsInfo() (map[taskmodel.PChan]*taskmodel.PChanStatistics, error) {
	ret := make(map[taskmodel.PChan]*taskmodel.PChanStatistics)
	queue.statsLock.RLock()
	defer queue.statsLock.RUnlock()
	for cName, info := range queue.pChanStatisticsInfos {
		ret[cName] = &taskmodel.PChanStatistics{
			MinTs: info.MinTs,
			MaxTs: info.MaxTs,
		}
	}
	return ret, nil
}

// DqTaskQueue represents queue for DQL task such as search/query
type DqTaskQueue struct {
	*BaseTaskQueue
}

type ClearTaskQueueResult struct {
	QueuedCleared int64
}

func isDQLTaskMatched(t taskmodel.Task, taskType string) bool {
	switch taskType {
	case "", "all":
		return true
	case "search":
		return t.Name() == taskmodel.SearchTaskName
	case "query":
		return t.Name() == taskmodel.QueryTaskName
	default:
		return false
	}
}

func clearTaskQueueError(reason string) error {
	if reason == "" {
		return errors.Wrap(context.Canceled, "read task queue cleared by admin")
	}
	return errors.Wrap(context.Canceled, fmt.Sprintf("read task queue cleared by admin: %s", reason))
}

func (queue *DqTaskQueue) clearQueuedTasks(taskType string, reason string) ClearTaskQueueResult {
	removed := queue.popUnissuedTasks(func(t taskmodel.Task) bool {
		return isDQLTaskMatched(t, taskType)
	})
	if len(removed) == 0 {
		queue.updateMetrics()
		return ClearTaskQueueResult{}
	}

	clearErr := clearTaskQueueError(reason)
	for _, task := range removed {
		task.Notify(clearErr)
	}
	queue.updateMetrics()
	return ClearTaskQueueResult{QueuedCleared: int64(len(removed))}
}

func (queue *DqTaskQueue) updateMetrics() {
	queue.utLock.RLock()
	unissuedTasksNum := queue.unissuedTasks.Len()
	queue.utLock.RUnlock()
	queue.atLock.RLock()
	activateTaskNum := len(queue.activeTasks)
	queue.atLock.RUnlock()

	metrics.ProxyQueueTaskNum.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "dql", metrics.UnissuedIndexTaskLabel).Set(float64(unissuedTasksNum))
	metrics.ProxyQueueTaskNum.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "dql", metrics.InProgressIndexTaskLabel).Set(float64(activateTaskNum))
}

func (queue *DdTaskQueue) Enqueue(t taskmodel.Task) error {
	queue.lock.Lock()
	defer queue.lock.Unlock()
	return queue.BaseTaskQueue.Enqueue(t)
}

func newDdTaskQueue(tsoAllocatorIns taskmodel.TsoAllocator) *DdTaskQueue {
	return &DdTaskQueue{
		BaseTaskQueue: newBaseTaskQueue(tsoAllocatorIns),
	}
}

func newDmTaskQueue(tsoAllocatorIns taskmodel.TsoAllocator) *DmTaskQueue {
	return &DmTaskQueue{
		BaseTaskQueue:        newBaseTaskQueue(tsoAllocatorIns),
		pChanStatisticsInfos: make(map[taskmodel.PChan]*pChanStatInfo),
	}
}

func newDqTaskQueue(tsoAllocatorIns taskmodel.TsoAllocator) *DqTaskQueue {
	return &DqTaskQueue{
		BaseTaskQueue: newBaseTaskQueue(tsoAllocatorIns),
	}
}

// TaskScheduler schedules the gRPC tasks.
type TaskScheduler struct {
	DdQueue *DdTaskQueue
	DmQueue *DmTaskQueue
	DqQueue *DqTaskQueue

	// data control queue, use for such as flush operation, which control the data status
	DcQueue *DdTaskQueue

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

type SchedOpt func(*TaskScheduler)

func NewTaskScheduler(ctx context.Context,
	tsoAllocatorIns taskmodel.TsoAllocator,
	opts ...SchedOpt,
) (*TaskScheduler, error) {
	ctx1, cancel := context.WithCancel(ctx)
	s := &TaskScheduler{
		ctx:    ctx1,
		cancel: cancel,
	}
	s.DdQueue = newDdTaskQueue(tsoAllocatorIns)
	s.DmQueue = newDmTaskQueue(tsoAllocatorIns)
	s.DqQueue = newDqTaskQueue(tsoAllocatorIns)

	s.DcQueue = newDdTaskQueue(tsoAllocatorIns)

	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}

func (sched *TaskScheduler) scheduleDdTask() taskmodel.Task {
	return sched.DdQueue.PopUnissuedTask()
}

func (sched *TaskScheduler) scheduleDcTask() taskmodel.Task {
	return sched.DcQueue.PopUnissuedTask()
}

func (sched *TaskScheduler) scheduleDmTask() taskmodel.Task {
	return sched.DmQueue.PopUnissuedTask()
}

func (sched *TaskScheduler) scheduleDqTask() taskmodel.Task {
	return sched.DqQueue.PopUnissuedTask()
}

func (sched *TaskScheduler) ClearDQLQueue(taskType string, reason string) ClearTaskQueueResult {
	return sched.DqQueue.clearQueuedTasks(taskType, reason)
}

func (sched *TaskScheduler) processTask(t taskmodel.Task, q TaskQueue) {
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
		Observe(float64(waitDuration.Microseconds()) / 1000.0)

	err := t.PreExecute(ctx)

	defer func() {
		t.Notify(err)
	}()
	if err != nil {
		span.RecordError(err)
		mlog.Warn(ctx, "Failed to pre-execute task: "+err.Error())
		return
	}

	span.AddEvent("scheduler process Execute")
	err = t.Execute(ctx)
	if err != nil {
		span.RecordError(err)
		mlog.Warn(ctx, "Failed to execute task: ", mlog.Err(err))
		return
	}

	span.AddEvent("scheduler process PostExecute")
	err = t.PostExecute(ctx)
	if err != nil {
		span.RecordError(err)
		mlog.Warn(ctx, "Failed to post-execute task: ", mlog.Err(err))
		return
	}
}

// definitionLoop schedules the ddl tasks.
func (sched *TaskScheduler) definitionLoop() {
	defer sched.wg.Done()

	pool := conc.NewPool[struct{}](paramtable.Get().ProxyCfg.DDLConcurrency.GetAsInt(), conc.WithExpiryDuration(time.Minute))
	defer pool.Release()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.DdQueue.utChan():
			for t := sched.scheduleDdTask(); t != nil; t = sched.scheduleDdTask() {
				task := t
				pool.Submit(func() (struct{}, error) {
					sched.processTask(task, sched.DdQueue)
					return struct{}{}, nil
				})
			}
			sched.DdQueue.updateMetrics()
		}
	}
}

// controlLoop schedule the data control operation, such as flush
func (sched *TaskScheduler) controlLoop() {
	defer sched.wg.Done()

	pool := conc.NewPool[struct{}](paramtable.Get().ProxyCfg.DCLConcurrency.GetAsInt(), conc.WithExpiryDuration(time.Minute))
	defer pool.Release()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.DcQueue.utChan():
			for t := sched.scheduleDcTask(); t != nil; t = sched.scheduleDcTask() {
				task := t
				pool.Submit(func() (struct{}, error) {
					sched.processTask(task, sched.DcQueue)
					return struct{}{}, nil
				})
			}
			sched.DcQueue.updateMetrics()
		}
	}
}

func (sched *TaskScheduler) manipulationLoop() {
	defer sched.wg.Done()
	pool := conc.NewPool[struct{}](paramtable.Get().ProxyCfg.MaxTaskNum.GetAsInt())
	defer pool.Release()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.DmQueue.utChan():
			for t := sched.scheduleDmTask(); t != nil; t = sched.scheduleDmTask() {
				task := t
				pool.Submit(func() (struct{}, error) {
					sched.processTask(task, sched.DmQueue)
					return struct{}{}, nil
				})
			}
			sched.DmQueue.updateMetrics()
		}
	}
}

func (sched *TaskScheduler) queryLoop() {
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
		case <-sched.DqQueue.utChan():
			for t := sched.scheduleDqTask(); t != nil; t = sched.scheduleDqTask() {
				task := t
				p := pool
				// if task is sub task spawned by another, use sub task pool in case of deadlock
				if task.IsSubTask() {
					p = subTaskPool
				}
				p.Submit(func() (struct{}, error) {
					sched.processTask(task, sched.DqQueue)
					return struct{}{}, nil
				})
			}
			sched.DqQueue.updateMetrics()
		}
	}
}

func (sched *TaskScheduler) Start() error {
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

func (sched *TaskScheduler) Close() {
	sched.cancel()
	sched.wg.Wait()
}

func (sched *TaskScheduler) GetPChanStatistics() (map[taskmodel.PChan]*taskmodel.PChanStatistics, error) {
	return sched.DmQueue.getPChanStatsInfo()
}

func (sched *TaskScheduler) getTaskQueueMetrics(queue *BaseTaskQueue, queueType string) metricsinfo.TaskQueueMetrics {
	pendingTaskStats := make(map[string]*TaskStatsTracker, 0)
	executingTaskStats := make(map[string]*TaskStatsTracker, 0)
	queue.atLock.RLock()
	atNum := len(queue.activeTasks)
	for _, task := range queue.activeTasks {
		taskType := task.Name()
		executingTime := task.GetDurationInExecuting().Milliseconds()

		tracker, ok := executingTaskStats[taskType]
		if !ok {
			tracker = NewTaskStatsTracker(taskType)
			executingTaskStats[taskType] = tracker
		}
		tracker.AddSample(executingTime)
	}
	executingTaskMetrics := make([]metricsinfo.TaskMetrics, 0, len(executingTaskStats))
	for _, tracker := range executingTaskStats {
		executingTaskMetrics = append(executingTaskMetrics, metricsinfo.TaskMetrics{
			Type:         tracker.TaskType,
			MaxQueueTime: tracker.MaxQueueTime,
			MinQueueTime: tracker.MinQueueTime,
			AvgQueueTime: tracker.AvgQueueTime(),
			Count:        tracker.Count,
		})
	}
	queue.atLock.RUnlock()

	queue.utLock.RLock()
	defer queue.utLock.RUnlock()
	utNum := queue.unissuedTasks.Len()

	for e := queue.unissuedTasks.Front(); e != nil; e = e.Next() {
		task := e.Value.(taskmodel.Task)
		taskType := task.Name()
		queueTimeMs := task.GetDurationInQueue().Milliseconds()

		tracker, ok := pendingTaskStats[taskType]
		if !ok {
			tracker = NewTaskStatsTracker(taskType)
			pendingTaskStats[taskType] = tracker
		}

		tracker.AddSample(queueTimeMs)
	}

	pendingTaskMetrics := make([]metricsinfo.TaskMetrics, 0, len(pendingTaskStats))
	for _, tracker := range pendingTaskStats {
		pendingTaskMetrics = append(pendingTaskMetrics, metricsinfo.TaskMetrics{
			Type:         tracker.TaskType,
			MaxQueueTime: tracker.MaxQueueTime,
			MinQueueTime: tracker.MinQueueTime,
			AvgQueueTime: tracker.AvgQueueTime(),
			Count:        tracker.Count,
		})
	}

	return metricsinfo.TaskQueueMetrics{
		Type:           queueType,
		PendingCount:   int64(utNum),
		ExecutingCount: int64(atNum),
		PendingTasks:   pendingTaskMetrics,
		ExecutingTasks: executingTaskMetrics,
	}
}

type TaskStatsTracker struct {
	TaskType       string
	MaxQueueTime   int64
	MinQueueTime   int64
	TotalQueueTime int64
	Count          int64
}

func NewTaskStatsTracker(taskType string) *TaskStatsTracker {
	return &TaskStatsTracker{
		TaskType:       taskType,
		MaxQueueTime:   0,
		MinQueueTime:   math.MaxInt64,
		TotalQueueTime: 0,
		Count:          0,
	}
}

func (t *TaskStatsTracker) AddSample(queueTimeMs int64) {
	t.MaxQueueTime = max(t.MaxQueueTime, queueTimeMs)
	t.MinQueueTime = min(t.MinQueueTime, queueTimeMs)
	t.TotalQueueTime += queueTimeMs
	t.Count++
}

func (t *TaskStatsTracker) AvgQueueTime() int64 {
	if t.Count == 0 {
		return 0
	}
	return t.TotalQueueTime / t.Count
}

func (sched *TaskScheduler) GetMetrics() []metricsinfo.TaskQueueMetrics {
	dmlQueueMetrics := sched.getTaskQueueMetrics(sched.DmQueue.BaseTaskQueue, "dml")
	ddlQueueMetrics := sched.getTaskQueueMetrics(sched.DdQueue.BaseTaskQueue, "ddl")
	dqlQueueMetrics := sched.getTaskQueueMetrics(sched.DqQueue.BaseTaskQueue, "dql")
	dcQueueMetrics := sched.getTaskQueueMetrics(sched.DcQueue.BaseTaskQueue, "dc")
	return []metricsinfo.TaskQueueMetrics{
		dmlQueueMetrics,
		ddlQueueMetrics,
		dqlQueueMetrics,
		dcQueueMetrics,
	}
}
