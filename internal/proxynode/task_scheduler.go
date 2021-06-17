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
	"fmt"
	"strconv"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
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
	PopActiveTask(tID UniqueID) task
	getTaskByReqID(reqID UniqueID) task
	TaskDoneTest(ts Timestamp) bool
	Enqueue(t task) error
}

type BaseTaskQueue struct {
	unissuedTasks *list.List
	activeTasks   map[UniqueID]task
	utLock        sync.RWMutex
	atLock        sync.RWMutex

	// maxTaskNum should keep still
	maxTaskNum int64

	utBufChan chan int // to block scheduler

	sched *TaskScheduler
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
	queue.utLock.RLock()
	defer queue.utLock.RUnlock()

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
	tID := t.ID()
	_, ok := queue.activeTasks[tID]
	if ok {
		log.Debug("ProxyNode task with tID already in active task list!", zap.Any("ID", tID))
	}

	queue.activeTasks[tID] = t
}

func (queue *BaseTaskQueue) PopActiveTask(tID UniqueID) task {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()
	t, ok := queue.activeTasks[tID]
	if ok {
		delete(queue.activeTasks, tID)
		return t
	}

	log.Debug("ProxyNode task not in active task list! ts", zap.Any("tID", tID))
	return t
}

func (queue *BaseTaskQueue) getTaskByReqID(reqID UniqueID) task {
	queue.utLock.RLock()
	defer queue.utLock.RUnlock()
	for e := queue.unissuedTasks.Front(); e != nil; e = e.Next() {
		if e.Value.(task).ID() == reqID {
			return e.Value.(task)
		}
	}

	queue.atLock.RLock()
	defer queue.atLock.RUnlock()
	for tID := range queue.activeTasks {
		if tID == reqID {
			return queue.activeTasks[tID]
		}
	}

	return nil
}

func (queue *BaseTaskQueue) TaskDoneTest(ts Timestamp) bool {
	queue.utLock.RLock()
	defer queue.utLock.RUnlock()
	for e := queue.unissuedTasks.Front(); e != nil; e = e.Next() {
		if e.Value.(task).EndTs() < ts {
			return false
		}
	}

	queue.atLock.RLock()
	defer queue.atLock.RUnlock()
	for _, task := range queue.activeTasks {
		if task.BeginTs() < ts {
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

type pChanStatInfo struct {
	pChanStatistics
	refCnt int
}

type DmTaskQueue struct {
	BaseTaskQueue
	statsLock            sync.RWMutex
	pChanStatisticsInfos map[pChan]*pChanStatInfo
}

func (queue *DmTaskQueue) Enqueue(t task) error {
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

func (queue *DmTaskQueue) addUnissuedTask(t task) error {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	if queue.utFull() {
		return errors.New("task queue is full")
	}
	queue.unissuedTasks.PushBack(t)
	queue.addPChanStats(t)
	queue.utBufChan <- 1
	return nil
}

func (queue *DmTaskQueue) PopActiveTask(tID UniqueID) task {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()
	t, ok := queue.activeTasks[tID]
	if ok {
		delete(queue.activeTasks, tID)
		log.Debug("ProxyNode DmTaskQueue popPChanStats", zap.Any("tID", t.ID()))
		queue.popPChanStats(t)
	} else {
		log.Debug("ProxyNode task not in active task list!", zap.Any("tID", tID))
	}
	return t
}

func (queue *DmTaskQueue) addPChanStats(t task) error {
	if dmT, ok := t.(dmlTask); ok {
		stats, err := dmT.getPChanStats()
		if err != nil {
			return err
		}
		log.Debug("ProxyNode DmTaskQueue addPChanStats", zap.Any("tID", t.ID()),
			zap.Any("stats", stats))
		queue.statsLock.Lock()
		for cName, stat := range stats {
			info, ok := queue.pChanStatisticsInfos[cName]
			if !ok {
				info = &pChanStatInfo{
					pChanStatistics: stat,
					refCnt:          1,
				}
				queue.pChanStatisticsInfos[cName] = info
			} else {
				if info.minTs > stat.minTs {
					info.minTs = stat.minTs
				}
				if info.maxTs < stat.maxTs {
					info.maxTs = stat.maxTs
				}
				info.refCnt++
			}
		}
		queue.statsLock.Unlock()
	} else {
		return fmt.Errorf("ProxyNode addUnissuedTask reflect to dmlTask failed, tID:%v", t.ID())
	}
	return nil
}

func (queue *DmTaskQueue) popPChanStats(t task) error {
	if dmT, ok := t.(dmlTask); ok {
		channels, err := dmT.getChannels()
		if err != nil {
			return err
		}
		queue.statsLock.Lock()
		for _, cName := range channels {
			info, ok := queue.pChanStatisticsInfos[cName]
			if ok {
				info.refCnt--
				if info.refCnt <= 0 {
					delete(queue.pChanStatisticsInfos, cName)
				}
			}
		}
		queue.statsLock.Unlock()
	} else {
		return fmt.Errorf("ProxyNode DmTaskQueue popPChanStats reflect to dmlTask failed, tID:%v", t.ID())
	}
	return nil
}

func (queue *DmTaskQueue) getPChanStatsInfo() (map[pChan]*pChanStatistics, error) {

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
			activeTasks:   make(map[UniqueID]task),
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
			activeTasks:   make(map[UniqueID]task),
			maxTaskNum:    1024,
			utBufChan:     make(chan int, 1024),
			sched:         sched,
		},
		pChanStatisticsInfos: make(map[pChan]*pChanStatInfo),
	}
}

func NewDqTaskQueue(sched *TaskScheduler) *DqTaskQueue {
	return &DqTaskQueue{
		BaseTaskQueue: BaseTaskQueue{
			unissuedTasks: list.New(),
			activeTasks:   make(map[UniqueID]task),
			maxTaskNum:    1024,
			utBufChan:     make(chan int, 1024),
			sched:         sched,
		},
	}
}

type TaskScheduler struct {
	DdQueue TaskQueue
	DmQueue *DmTaskQueue
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
		q.PopActiveTask(t.ID())
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

type resultBufHeader struct {
	usedVChans                  map[interface{}]struct{} // set of vChan
	usedChans                   map[interface{}]struct{} // set of Chan todo
	receivedVChansSet           map[interface{}]struct{} // set of vChan
	receivedSealedSegmentIDsSet map[interface{}]struct{} // set of UniqueID
	receivedGlobalSegmentIDsSet map[interface{}]struct{} // set of UniqueID
	haveError                   bool
}

type searchResultBuf struct {
	resultBufHeader
	resultBuf []*internalpb.SearchResults
}

type queryResultBuf struct {
	resultBufHeader
	resultBuf []*internalpb.RetrieveResults
}

func newSearchResultBuf() *searchResultBuf {
	return &searchResultBuf{
		resultBufHeader: resultBufHeader{
			usedVChans:                  make(map[interface{}]struct{}),
			usedChans:                   make(map[interface{}]struct{}),
			receivedVChansSet:           make(map[interface{}]struct{}),
			receivedSealedSegmentIDsSet: make(map[interface{}]struct{}),
			receivedGlobalSegmentIDsSet: make(map[interface{}]struct{}),
			haveError:                   false,
		},
		resultBuf: make([]*internalpb.SearchResults, 0),
	}
}

func newQueryResultBuf() *queryResultBuf {
	return &queryResultBuf{
		resultBufHeader: resultBufHeader{
			usedVChans:                  make(map[interface{}]struct{}),
			usedChans:                   make(map[interface{}]struct{}),
			receivedVChansSet:           make(map[interface{}]struct{}),
			receivedSealedSegmentIDsSet: make(map[interface{}]struct{}),
			receivedGlobalSegmentIDsSet: make(map[interface{}]struct{}),
			haveError:                   false,
		},
		resultBuf: make([]*internalpb.RetrieveResults, 0),
	}
}

func setContain(m1, m2 map[interface{}]struct{}) bool {
	log.Debug("ProxyNode task_scheduler setContain", zap.Any("len(m1)", len(m1)),
		zap.Any("len(m2)", len(m2)))
	if len(m1) < len(m2) {
		return false
	}

	for k2 := range m2 {
		_, ok := m1[k2]
		log.Debug("ProxyNode task_scheduler setContain", zap.Any("k2", fmt.Sprintf("%v", k2)),
			zap.Any("ok", ok))
		if !ok {
			return false
		}
	}

	return true
}

func (sr *resultBufHeader) readyToReduce() bool {
	if sr.haveError {
		log.Debug("ProxyNode searchResultBuf readyToReduce", zap.Any("haveError", true))
		return true
	}

	usedChansSetStrMap := make(map[string]int)
	for x := range sr.usedChans {
		usedChansSetStrMap[x.(string)] = 1
	}

	receivedVChansSetStrMap := make(map[string]int)

	for x := range sr.receivedVChansSet {
		receivedVChansSetStrMap[x.(string)] = 1
	}

	usedVChansSetStrMap := make(map[string]int)
	for x := range sr.usedVChans {
		usedVChansSetStrMap[x.(string)] = 1
	}

	sealedSegmentIDsStrMap := make(map[int64]int)

	for x := range sr.receivedSealedSegmentIDsSet {
		sealedSegmentIDsStrMap[x.(int64)] = 1
	}

	sealedGlobalSegmentIDsStrMap := make(map[int64]int)
	for x := range sr.receivedGlobalSegmentIDsSet {
		sealedGlobalSegmentIDsStrMap[x.(int64)] = 1
	}

	ret1 := setContain(sr.receivedVChansSet, sr.usedVChans)
	ret2 := setContain(sr.receivedVChansSet, sr.usedChans)
	log.Debug("ProxyNode searchResultBuf readyToReduce", zap.Any("receivedVChansSet", receivedVChansSetStrMap),
		zap.Any("usedVChans", usedVChansSetStrMap),
		zap.Any("usedChans", usedChansSetStrMap),
		zap.Any("receivedSealedSegmentIDsSet", sealedSegmentIDsStrMap),
		zap.Any("receivedGlobalSegmentIDsSet", sealedGlobalSegmentIDsStrMap),
		zap.Any("ret1", ret1),
		zap.Any("ret2", ret2))
	if !ret1 && !ret2 {
		return false
	}
	ret := setContain(sr.receivedSealedSegmentIDsSet, sr.receivedGlobalSegmentIDsSet)
	log.Debug("ProxyNode searchResultBuf readyToReduce", zap.Any("ret", ret))
	return ret
}

func (sr *resultBufHeader) addPartialResult(vchans []vChan, searchSegIDs, globalSegIDs []UniqueID) {

	for _, vchan := range vchans {
		sr.receivedVChansSet[vchan] = struct{}{}
	}

	for _, sealedSegment := range searchSegIDs {
		sr.receivedSealedSegmentIDsSet[sealedSegment] = struct{}{}
	}

	for _, globalSegment := range globalSegIDs {
		sr.receivedGlobalSegmentIDsSet[globalSegment] = struct{}{}
	}
}

func (sr *searchResultBuf) addPartialResult(result *internalpb.SearchResults) {
	sr.resultBuf = append(sr.resultBuf, result)
	if result.Status.ErrorCode != commonpb.ErrorCode_Success {
		sr.haveError = true
		return
	}
	sr.resultBufHeader.addPartialResult(result.ChannelIDsSearched, result.SealedSegmentIDsSearched,
		result.GlobalSealedSegmentIDs)
}

func (qr *queryResultBuf) addPartialResult(result *internalpb.RetrieveResults) {
	qr.resultBuf = append(qr.resultBuf, result)
	if result.Status.ErrorCode != commonpb.ErrorCode_Success {
		qr.haveError = true
		return
	}
	qr.resultBufHeader.addPartialResult(result.ChannelIDsRetrieved, result.SealedSegmentIDsRetrieved,
		result.GlobalSealedSegmentIDs)
}

func (sched *TaskScheduler) collectResultLoop() {
	defer sched.wg.Done()

	queryResultMsgStream, _ := sched.msFactory.NewQueryMsgStream(sched.ctx)
	queryResultMsgStream.AsConsumer(Params.SearchResultChannelNames, Params.ProxySubName)
	log.Debug("ProxyNode", zap.Strings("SearchResultChannelNames", Params.SearchResultChannelNames),
		zap.Any("ProxySubName", Params.ProxySubName))

	queryResultMsgStream.Start()
	defer queryResultMsgStream.Close()

	searchResultBufs := make(map[UniqueID]*searchResultBuf)
	searchResultBufFlags := make(map[UniqueID]bool) // if value is true, we can ignore queryResult
	queryResultBufs := make(map[UniqueID]*queryResultBuf)
	queryResultBufFlags := make(map[UniqueID]bool) // if value is true, we can ignore queryResult

	for {
		select {
		case msgPack, ok := <-queryResultMsgStream.Chan():
			if !ok {
				log.Debug("ProxyNode collectResultLoop exit Chan closed")
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
					ignoreThisResult, ok := searchResultBufFlags[reqID]
					if !ok {
						searchResultBufFlags[reqID] = false
						ignoreThisResult = false
					}
					if ignoreThisResult {
						log.Debug("ProxyNode collectResultLoop Got a SearchResultMsg, but we should ignore", zap.Any("ReqID", reqID))
						continue
					}
					t := sched.getTaskByReqID(reqID)
					log.Debug("ProxyNode collectResultLoop Got a SearchResultMsg", zap.Any("ReqID", reqID), zap.Any("t", t))
					if t == nil {
						log.Debug("ProxyNode collectResultLoop GetTaskByReqID failed", zap.String("reqID", reqIDStr))
						delete(searchResultBufs, reqID)
						searchResultBufFlags[reqID] = true
						continue
					}

					st, ok := t.(*SearchTask)
					if !ok {
						log.Debug("ProxyNode collectResultLoop type assert t as SearchTask failed", zap.Any("t", t))
						delete(searchResultBufs, reqID)
						searchResultBufFlags[reqID] = true
						continue
					}

					resultBuf, ok := searchResultBufs[reqID]
					if !ok {
						resultBuf = newSearchResultBuf()
						vchans, err := st.getVChannels()
						log.Debug("ProxyNode collectResultLoop, first receive", zap.Any("reqID", reqID), zap.Any("vchans", vchans),
							zap.Error(err))
						if err != nil {
							delete(searchResultBufs, reqID)
							continue
						}
						for _, vchan := range vchans {
							resultBuf.usedVChans[vchan] = struct{}{}
						}
						pchans, err := st.getChannels()
						log.Debug("ProxyNode collectResultLoop, first receive", zap.Any("reqID", reqID), zap.Any("pchans", pchans),
							zap.Error(err))
						if err != nil {
							delete(searchResultBufs, reqID)
							continue
						}
						for _, pchan := range pchans {
							resultBuf.usedChans[pchan] = struct{}{}
						}
						searchResultBufs[reqID] = resultBuf
					}
					resultBuf.addPartialResult(&searchResultMsg.SearchResults)

					//t := sched.getTaskByReqID(reqID)
					{
						colName := t.(*SearchTask).query.CollectionName
						log.Debug("ProxyNode collectResultLoop", zap.String("collection name", colName), zap.String("reqID", reqIDStr), zap.Int("answer cnt", len(searchResultBufs[reqID].resultBuf)))
					}

					if resultBuf.readyToReduce() {
						log.Debug("ProxyNode collectResultLoop readyToReduce and assign to reduce")
						searchResultBufFlags[reqID] = true
						st.resultBuf <- resultBuf.resultBuf
						delete(searchResultBufs, reqID)
					}

					sp.Finish()
				}
				if queryResultMsg, rtOk := tsMsg.(*msgstream.RetrieveResultMsg); rtOk {
					//reqID := retrieveResultMsg.Base.MsgID
					//reqIDStr := strconv.FormatInt(reqID, 10)
					//t := sched.getTaskByReqID(reqID)
					//if t == nil {
					//	log.Debug("proxynode", zap.String("RetrieveResult GetTaskByReqID failed, reqID = ", reqIDStr))
					//	delete(queryResultBufs, reqID)
					//	continue
					//}
					//
					//_, ok = queryResultBufs[reqID]
					//if !ok {
					//	queryResultBufs[reqID] = make([]*internalpb.RetrieveResults, 0)
					//}
					//queryResultBufs[reqID] = append(queryResultBufs[reqID], &retrieveResultMsg.RetrieveResults)
					//
					//{
					//	colName := t.(*RetrieveTask).retrieve.CollectionName
					//	log.Debug("Getcollection", zap.String("collection name", colName), zap.String("reqID", reqIDStr), zap.Int("answer cnt", len(queryResultBufs[reqID])))
					//}
					//if len(queryResultBufs[reqID]) == queryNodeNum {
					//	t := sched.getTaskByReqID(reqID)
					//	if t != nil {
					//		rt, ok := t.(*RetrieveTask)
					//		if ok {
					//			rt.resultBuf <- queryResultBufs[reqID]
					//			delete(queryResultBufs, reqID)
					//		}
					//	} else {
					//	}
					//}

					reqID := queryResultMsg.Base.MsgID
					reqIDStr := strconv.FormatInt(reqID, 10)
					ignoreThisResult, ok := queryResultBufFlags[reqID]
					if !ok {
						queryResultBufFlags[reqID] = false
						ignoreThisResult = false
					}
					if ignoreThisResult {
						log.Debug("ProxyNode collectResultLoop Got a queryResultMsg, but we should ignore", zap.Any("ReqID", reqID))
						continue
					}
					t := sched.getTaskByReqID(reqID)
					log.Debug("ProxyNode collectResultLoop Got a queryResultMsg", zap.Any("ReqID", reqID), zap.Any("t", t))
					if t == nil {
						log.Debug("ProxyNode collectResultLoop GetTaskByReqID failed", zap.String("reqID", reqIDStr))
						delete(queryResultBufs, reqID)
						queryResultBufFlags[reqID] = true
						continue
					}

					st, ok := t.(*RetrieveTask)
					if !ok {
						log.Debug("ProxyNode collectResultLoop type assert t as RetrieveTask failed", zap.Any("t", t))
						delete(queryResultBufs, reqID)
						queryResultBufFlags[reqID] = true
						continue
					}

					resultBuf, ok := queryResultBufs[reqID]
					if !ok {
						resultBuf = newQueryResultBuf()
						vchans, err := st.getVChannels()
						log.Debug("ProxyNode collectResultLoop, first receive", zap.Any("reqID", reqID), zap.Any("vchans", vchans),
							zap.Error(err))
						if err != nil {
							delete(queryResultBufs, reqID)
							continue
						}
						for _, vchan := range vchans {
							resultBuf.usedVChans[vchan] = struct{}{}
						}
						pchans, err := st.getChannels()
						log.Debug("ProxyNode collectResultLoop, first receive", zap.Any("reqID", reqID), zap.Any("pchans", pchans),
							zap.Error(err))
						if err != nil {
							delete(queryResultBufs, reqID)
							continue
						}
						for _, pchan := range pchans {
							resultBuf.usedChans[pchan] = struct{}{}
						}
						queryResultBufs[reqID] = resultBuf
					}
					resultBuf.addPartialResult(&queryResultMsg.RetrieveResults)

					//t := sched.getTaskByReqID(reqID)
					{
						colName := t.(*RetrieveTask).retrieve.CollectionName
						log.Debug("ProxyNode collectResultLoop", zap.String("collection name", colName), zap.String("reqID", reqIDStr), zap.Int("answer cnt", len(queryResultBufs[reqID].resultBuf)))
					}

					if resultBuf.readyToReduce() {
						log.Debug("ProxyNode collectResultLoop readyToReduce and assign to reduce")
						queryResultBufFlags[reqID] = true
						st.resultBuf <- resultBuf.resultBuf
						delete(queryResultBufs, reqID)
					}
					sp.Finish()
				}
			}
		case <-sched.ctx.Done():
			log.Debug("ProxyNode collectResultLoop is closed ...")
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
	go sched.collectResultLoop()

	return nil
}

func (sched *TaskScheduler) Close() {
	sched.cancel()
	sched.wg.Wait()
}

func (sched *TaskScheduler) TaskDoneTest(ts Timestamp) bool {
	ddTaskDone := sched.DdQueue.TaskDoneTest(ts)
	dmTaskDone := sched.DmQueue.TaskDoneTest(ts)
	return ddTaskDone && dmTaskDone
}

func (sched *TaskScheduler) getPChanStatistics() (map[pChan]*pChanStatistics, error) {
	return sched.DmQueue.getPChanStatsInfo()
}
