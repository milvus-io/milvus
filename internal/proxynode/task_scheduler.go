// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package proxynode

import (
	"container/list"
	"context"
	"errors"
	"strconv"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/opentracing/opentracing-go"
	oplog "github.com/opentracing/opentracing-go/log"
)

type TaskQueue interface {
	utChan() <-chan int
	utEmpty() bool
	utFull() bool
	addUnissuedTask(t task) error
	FrontUnissuedTask() task
	PopUnissuedTask() task
	AddActiveTask(t task)
	PopActiveTask(ts Timestamp) task
	getTaskByReqID(reqID UniqueID) task
	TaskDoneTest(ts Timestamp) bool
	Enqueue(t task) error
}

type BaseTaskQueue struct {
	unissuedTasks *list.List
	activeTasks   map[Timestamp]task
	utLock        sync.Mutex
	atLock        sync.Mutex

	// maxTaskNum should keep still
	maxTaskNum int64

	utBufChan chan int // to block scheduler

	sched *TaskScheduler
}

func (queue *BaseTaskQueue) utChan() <-chan int {
	return queue.utBufChan
}

func (queue *BaseTaskQueue) utEmpty() bool {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()
	return queue.unissuedTasks.Len() == 0
}

func (queue *BaseTaskQueue) utFull() bool {
	return int64(queue.unissuedTasks.Len()) >= queue.maxTaskNum
}

func (queue *BaseTaskQueue) addUnissuedTask(t task) error {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	if queue.utFull() {
		return errors.New("task queue is full")
	}
	queue.unissuedTasks.PushBack(t)
	queue.utBufChan <- 1
	return nil
}

func (queue *BaseTaskQueue) FrontUnissuedTask() task {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	if queue.unissuedTasks.Len() <= 0 {
		log.Warn("sorry, but the unissued task list is empty!")
		return nil
	}

	return queue.unissuedTasks.Front().Value.(task)
}

func (queue *BaseTaskQueue) PopUnissuedTask() task {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	if queue.unissuedTasks.Len() <= 0 {
		log.Warn("sorry, but the unissued task list is empty!")
		return nil
	}

	ft := queue.unissuedTasks.Front()
	queue.unissuedTasks.Remove(ft)

	return ft.Value.(task)
}

func (queue *BaseTaskQueue) AddActiveTask(t task) {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()

	ts := t.EndTs()
	_, ok := queue.activeTasks[ts]
	if ok {
		log.Debug("proxynode", zap.Uint64("task with timestamp ts already in active task list! ts:", ts))
	}

	queue.activeTasks[ts] = t
}

func (queue *BaseTaskQueue) PopActiveTask(ts Timestamp) task {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()

	t, ok := queue.activeTasks[ts]
	if ok {
		delete(queue.activeTasks, ts)
		return t
	}

	log.Debug("proxynode", zap.Uint64("task with timestamp ts already in active task list! ts:", ts))
	return nil
}

func (queue *BaseTaskQueue) getTaskByReqID(reqID UniqueID) task {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()
	for e := queue.unissuedTasks.Front(); e != nil; e = e.Next() {
		if e.Value.(task).ID() == reqID {
			return e.Value.(task)
		}
	}

	queue.atLock.Lock()
	defer queue.atLock.Unlock()
	for ats := range queue.activeTasks {
		if queue.activeTasks[ats].ID() == reqID {
			return queue.activeTasks[ats]
		}
	}

	return nil
}

func (queue *BaseTaskQueue) TaskDoneTest(ts Timestamp) bool {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()
	for e := queue.unissuedTasks.Front(); e != nil; e = e.Next() {
		if e.Value.(task).EndTs() < ts {
			return false
		}
	}

	queue.atLock.Lock()
	defer queue.atLock.Unlock()
	for ats := range queue.activeTasks {
		if ats < ts {
			return false
		}
	}

	return true
}

func (queue *BaseTaskQueue) Enqueue(t task) error {
	err := t.OnEnqueue()
	if err != nil {
		return err
	}

	ts, err := queue.sched.tsoAllocator.AllocOne()
	if err != nil {
		return err
	}
	t.SetTs(ts)

	reqID, err := queue.sched.idAllocator.AllocOne()
	if err != nil {
		return err
	}
	t.SetID(reqID)

	return queue.addUnissuedTask(t)
}

type DdTaskQueue struct {
	BaseTaskQueue
	lock sync.Mutex
}

type DmTaskQueue struct {
	BaseTaskQueue
}

type DqTaskQueue struct {
	BaseTaskQueue
}

func (queue *DdTaskQueue) Enqueue(t task) error {
	queue.lock.Lock()
	defer queue.lock.Unlock()
	return queue.BaseTaskQueue.Enqueue(t)
}

func NewDdTaskQueue(sched *TaskScheduler) *DdTaskQueue {
	return &DdTaskQueue{
		BaseTaskQueue: BaseTaskQueue{
			unissuedTasks: list.New(),
			activeTasks:   make(map[Timestamp]task),
			maxTaskNum:    1024,
			utBufChan:     make(chan int, 1024),
			sched:         sched,
		},
	}
}

func NewDmTaskQueue(sched *TaskScheduler) *DmTaskQueue {
	return &DmTaskQueue{
		BaseTaskQueue: BaseTaskQueue{
			unissuedTasks: list.New(),
			activeTasks:   make(map[Timestamp]task),
			maxTaskNum:    1024,
			utBufChan:     make(chan int, 1024),
			sched:         sched,
		},
	}
}

func NewDqTaskQueue(sched *TaskScheduler) *DqTaskQueue {
	return &DqTaskQueue{
		BaseTaskQueue: BaseTaskQueue{
			unissuedTasks: list.New(),
			activeTasks:   make(map[Timestamp]task),
			maxTaskNum:    1024,
			utBufChan:     make(chan int, 1024),
			sched:         sched,
		},
	}
}

type TaskScheduler struct {
	DdQueue TaskQueue
	DmQueue TaskQueue
	DqQueue TaskQueue

	idAllocator  *allocator.IDAllocator
	tsoAllocator *TimestampAllocator

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	msFactory msgstream.Factory
}

func NewTaskScheduler(ctx context.Context,
	idAllocator *allocator.IDAllocator,
	tsoAllocator *TimestampAllocator,
	factory msgstream.Factory) (*TaskScheduler, error) {
	ctx1, cancel := context.WithCancel(ctx)
	s := &TaskScheduler{
		idAllocator:  idAllocator,
		tsoAllocator: tsoAllocator,
		ctx:          ctx1,
		cancel:       cancel,
		msFactory:    factory,
	}
	s.DdQueue = NewDdTaskQueue(s)
	s.DmQueue = NewDmTaskQueue(s)
	s.DqQueue = NewDqTaskQueue(s)

	return s, nil
}

func (sched *TaskScheduler) scheduleDdTask() task {
	return sched.DdQueue.PopUnissuedTask()
}

func (sched *TaskScheduler) scheduleDmTask() task {
	return sched.DmQueue.PopUnissuedTask()
}

func (sched *TaskScheduler) scheduleDqTask() task {
	return sched.DqQueue.PopUnissuedTask()
}

func (sched *TaskScheduler) getTaskByReqID(collMeta UniqueID) task {
	if t := sched.DdQueue.getTaskByReqID(collMeta); t != nil {
		return t
	}
	if t := sched.DmQueue.getTaskByReqID(collMeta); t != nil {
		return t
	}
	if t := sched.DqQueue.getTaskByReqID(collMeta); t != nil {
		return t
	}
	return nil
}

func (sched *TaskScheduler) processTask(t task, q TaskQueue) {
	span, ctx := trace.StartSpanFromContext(t.TraceCtx(),
		opentracing.Tags{
			"Type": t.Name(),
			"ID":   t.ID(),
		})
	defer span.Finish()
	span.LogFields(oplog.Int64("scheduler process PreExecute", t.ID()))
	err := t.PreExecute(ctx)

	defer func() {
		t.Notify(err)
	}()
	if err != nil {
		trace.LogError(span, err)
		return
	}

	span.LogFields(oplog.Int64("scheduler process AddActiveTask", t.ID()))
	q.AddActiveTask(t)

	defer func() {
		span.LogFields(oplog.Int64("scheduler process PopActiveTask", t.ID()))
		q.PopActiveTask(t.EndTs())
	}()
	span.LogFields(oplog.Int64("scheduler process Execute", t.ID()))
	err = t.Execute(ctx)
	if err != nil {
		trace.LogError(span, err)
		return
	}
	span.LogFields(oplog.Int64("scheduler process PostExecute", t.ID()))
	err = t.PostExecute(ctx)
}

func (sched *TaskScheduler) definitionLoop() {
	defer sched.wg.Done()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.DdQueue.utChan():
			if !sched.DdQueue.utEmpty() {
				t := sched.scheduleDdTask()
				sched.processTask(t, sched.DdQueue)
			}
		}
	}
}

func (sched *TaskScheduler) manipulationLoop() {
	defer sched.wg.Done()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.DmQueue.utChan():
			if !sched.DmQueue.utEmpty() {
				t := sched.scheduleDmTask()
				go sched.processTask(t, sched.DmQueue)
			}
		}
	}
}

func (sched *TaskScheduler) queryLoop() {
	defer sched.wg.Done()

	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.DqQueue.utChan():
			if !sched.DqQueue.utEmpty() {
				t := sched.scheduleDqTask()
				go sched.processTask(t, sched.DqQueue)
			} else {
				log.Debug("query queue is empty ...")
			}
		}
	}
}

func (sched *TaskScheduler) queryResultLoop() {
	defer sched.wg.Done()

	queryResultMsgStream, _ := sched.msFactory.NewQueryMsgStream(sched.ctx)
	queryResultMsgStream.AsConsumer(Params.SearchResultChannelNames, Params.ProxySubName)
	log.Debug("proxynode", zap.Strings("search result channel names", Params.SearchResultChannelNames))
	queryResultMsgStream.AsConsumer(Params.RetrieveResultChannelNames, Params.ProxySubName)
	log.Debug("proxynode", zap.Strings("Retrieve result channel names", Params.RetrieveResultChannelNames))
	log.Debug("proxynode", zap.String("proxySubName", Params.ProxySubName))

	queryNodeNum := Params.QueryNodeNum

	queryResultMsgStream.Start()
	defer queryResultMsgStream.Close()

	queryResultBuf := make(map[UniqueID][]*internalpb.SearchResults)
	retrieveResultBuf := make(map[UniqueID][]*internalpb.RetrieveResults)

	for {
		select {
		case msgPack, ok := <-queryResultMsgStream.Chan():
			if !ok {
				log.Debug("buf chan closed")
				return
			}
			if msgPack == nil {
				continue
			}
			for _, tsMsg := range msgPack.Msgs {
				sp, ctx := trace.StartSpanFromContext(tsMsg.TraceCtx())
				tsMsg.SetTraceCtx(ctx)
				if searchResultMsg, srOk := tsMsg.(*msgstream.SearchResultMsg); srOk {
					reqID := searchResultMsg.Base.MsgID
					reqIDStr := strconv.FormatInt(reqID, 10)
					t := sched.getTaskByReqID(reqID)
					if t == nil {
						log.Debug("proxynode", zap.String("QueryResult GetTaskByReqID failed, reqID = ", reqIDStr))
						delete(queryResultBuf, reqID)
						continue
					}

					_, ok = queryResultBuf[reqID]
					if !ok {
						queryResultBuf[reqID] = make([]*internalpb.SearchResults, 0)
					}
					queryResultBuf[reqID] = append(queryResultBuf[reqID], &searchResultMsg.SearchResults)

					//t := sched.getTaskByReqID(reqID)
					{
						colName := t.(*SearchTask).query.CollectionName
						log.Debug("Getcollection", zap.String("collection name", colName), zap.String("reqID", reqIDStr), zap.Int("answer cnt", len(queryResultBuf[reqID])))
					}
					if len(queryResultBuf[reqID]) == queryNodeNum {
						t := sched.getTaskByReqID(reqID)
						if t != nil {
							qt, ok := t.(*SearchTask)
							if ok {
								qt.resultBuf <- queryResultBuf[reqID]
								delete(queryResultBuf, reqID)
							}
						} else {

							// log.Printf("task with reqID %v is nil", reqID)
						}
					}
					sp.Finish()
				}
				if retrieveResultMsg, rtOk := tsMsg.(*msgstream.RetrieveResultMsg); rtOk {
					reqID := retrieveResultMsg.Base.MsgID
					reqIDStr := strconv.FormatInt(reqID, 10)
					t := sched.getTaskByReqID(reqID)
					if t == nil {
						log.Debug("proxynode", zap.String("RetrieveResult GetTaskByReqID failed, reqID = ", reqIDStr))
						delete(retrieveResultBuf, reqID)
						continue
					}

					_, ok = retrieveResultBuf[reqID]
					if !ok {
						retrieveResultBuf[reqID] = make([]*internalpb.RetrieveResults, 0)
					}
					retrieveResultBuf[reqID] = append(retrieveResultBuf[reqID], &retrieveResultMsg.RetrieveResults)

					{
						colName := t.(*RetrieveTask).retrieve.CollectionName
						log.Debug("Getcollection", zap.String("collection name", colName), zap.String("reqID", reqIDStr), zap.Int("answer cnt", len(retrieveResultBuf[reqID])))
					}
					if len(retrieveResultBuf[reqID]) == queryNodeNum {
						t := sched.getTaskByReqID(reqID)
						if t != nil {
							rt, ok := t.(*RetrieveTask)
							if ok {
								rt.resultBuf <- retrieveResultBuf[reqID]
								delete(retrieveResultBuf, reqID)
							}
						} else {
						}
					}
					sp.Finish()
				}
			}
		case <-sched.ctx.Done():
			log.Debug("proxynode server is closed ...")
			return
		}
	}
}

func (sched *TaskScheduler) Start() error {
	sched.wg.Add(1)
	go sched.definitionLoop()

	sched.wg.Add(1)
	go sched.manipulationLoop()

	sched.wg.Add(1)
	go sched.queryLoop()

	sched.wg.Add(1)
	go sched.queryResultLoop()

	return nil
}

func (sched *TaskScheduler) Close() {
	sched.cancel()
	sched.wg.Wait()
}

func (sched *TaskScheduler) TaskDoneTest(ts Timestamp) bool {
	ddTaskDone := sched.DdQueue.TaskDoneTest(ts)
	dmTaskDone := sched.DmQueue.TaskDoneTest(ts)
	//dqTaskDone := sched.DqQueue.TaskDoneTest(ts)
	return ddTaskDone && dmTaskDone && true
}
