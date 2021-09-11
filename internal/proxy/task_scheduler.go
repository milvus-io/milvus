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

package proxy

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
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
	PopActiveTask(tID UniqueID) task
	getTaskByReqID(reqID UniqueID) task
	TaskDoneTest(ts Timestamp) bool
	Enqueue(t task) error
}

// TODO(dragondriver): load from config
const maxTaskNum = 1024

type baseTaskQueue struct {
	unissuedTasks *list.List
	activeTasks   map[UniqueID]task
	utLock        sync.RWMutex
	atLock        sync.RWMutex

	// maxTaskNum should keep still
	maxTaskNum int64

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
	return int64(queue.unissuedTasks.Len()) >= queue.maxTaskNum
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
		log.Warn("sorry, but the unissued task list is empty!")
		return nil
	}

	return queue.unissuedTasks.Front().Value.(task)
}

func (queue *baseTaskQueue) PopUnissuedTask() task {
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

func (queue *baseTaskQueue) AddActiveTask(t task) {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()
	tID := t.ID()
	_, ok := queue.activeTasks[tID]
	if ok {
		log.Debug("Proxy task with tID already in active task list!", zap.Any("ID", tID))
	}

	queue.activeTasks[tID] = t
}

func (queue *baseTaskQueue) PopActiveTask(tID UniqueID) task {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()
	t, ok := queue.activeTasks[tID]
	if ok {
		delete(queue.activeTasks, tID)
		return t
	}

	log.Debug("Proxy task not in active task list! ts", zap.Any("tID", tID))
	return t
}

func (queue *baseTaskQueue) getTaskByReqID(reqID UniqueID) task {
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

func (queue *baseTaskQueue) TaskDoneTest(ts Timestamp) bool {
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

func newBaseTaskQueue(tsoAllocatorIns tsoAllocator, idAllocatorIns idAllocatorInterface) *baseTaskQueue {
	return &baseTaskQueue{
		unissuedTasks:   list.New(),
		activeTasks:     make(map[UniqueID]task),
		utLock:          sync.RWMutex{},
		atLock:          sync.RWMutex{},
		maxTaskNum:      maxTaskNum,
		utBufChan:       make(chan int, maxTaskNum),
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
	queue.lock.Lock()
	defer queue.lock.Unlock()

	err := queue.baseTaskQueue.Enqueue(t)
	if err != nil {
		return err
	}
	_ = queue.addPChanStats(t)

	return nil
}

func (queue *dmTaskQueue) PopActiveTask(tID UniqueID) task {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()
	t, ok := queue.activeTasks[tID]
	if ok {
		delete(queue.activeTasks, tID)
		log.Debug("Proxy dmTaskQueue popPChanStats", zap.Any("tID", t.ID()))
		queue.popPChanStats(t)
	} else {
		log.Debug("Proxy task not in active task list!", zap.Any("tID", tID))
	}
	return t
}

func (queue *dmTaskQueue) addPChanStats(t task) error {
	if dmT, ok := t.(dmlTask); ok {
		stats, err := dmT.getPChanStats()
		if err != nil {
			log.Debug("Proxy dmTaskQueue addPChanStats", zap.Any("tID", t.ID()),
				zap.Any("stats", stats), zap.Error(err))
			return err
		}
		queue.statsLock.Lock()
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
		queue.statsLock.Unlock()
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
		queue.statsLock.Lock()
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
		queue.statsLock.Unlock()
	} else {
		return fmt.Errorf("Proxy dmTaskQueue popPChanStats reflect to dmlTask failed, tID:%v", t.ID())
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

type taskScheduler struct {
	ddQueue taskQueue
	dmQueue *dmTaskQueue
	dqQueue taskQueue

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	msFactory msgstream.Factory
}

func newTaskScheduler(ctx context.Context,
	idAllocatorIns idAllocatorInterface,
	tsoAllocatorIns tsoAllocator,
	factory msgstream.Factory) (*taskScheduler, error) {
	ctx1, cancel := context.WithCancel(ctx)
	s := &taskScheduler{
		ctx:       ctx1,
		cancel:    cancel,
		msFactory: factory,
	}
	s.ddQueue = newDdTaskQueue(tsoAllocatorIns, idAllocatorIns)
	s.dmQueue = newDmTaskQueue(tsoAllocatorIns, idAllocatorIns)
	s.dqQueue = newDqTaskQueue(tsoAllocatorIns, idAllocatorIns)

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

func (sched *taskScheduler) getTaskByReqID(collMeta UniqueID) task {
	if t := sched.ddQueue.getTaskByReqID(collMeta); t != nil {
		return t
	}
	if t := sched.dmQueue.getTaskByReqID(collMeta); t != nil {
		return t
	}
	if t := sched.dqQueue.getTaskByReqID(collMeta); t != nil {
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
		return
	}

	span.LogFields(oplog.Int64("scheduler process Execute", t.ID()))
	err = t.Execute(ctx)
	if err != nil {
		trace.LogError(span, err)
		return
	}

	span.LogFields(oplog.Int64("scheduler process PostExecute", t.ID()))
	err = t.PostExecute(ctx)
}

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

type resultBufHeader struct {
	usedVChans                  map[interface{}]struct{} // set of vChan
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
			receivedVChansSet:           make(map[interface{}]struct{}),
			receivedSealedSegmentIDsSet: make(map[interface{}]struct{}),
			receivedGlobalSegmentIDsSet: make(map[interface{}]struct{}),
			haveError:                   false,
		},
		resultBuf: make([]*internalpb.RetrieveResults, 0),
	}
}

func (sr *resultBufHeader) readyToReduce() bool {
	if sr.haveError {
		log.Debug("Proxy searchResultBuf readyToReduce", zap.Any("haveError", true))
		return true
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

	ret1 := funcutil.SetContain(sr.receivedVChansSet, sr.usedVChans)
	log.Debug("Proxy searchResultBuf readyToReduce", zap.Any("receivedVChansSet", receivedVChansSetStrMap),
		zap.Any("usedVChans", usedVChansSetStrMap),
		zap.Any("receivedSealedSegmentIDsSet", sealedSegmentIDsStrMap),
		zap.Any("receivedGlobalSegmentIDsSet", sealedGlobalSegmentIDsStrMap),
		zap.Any("ret1", ret1),
	)
	if !ret1 {
		return false
	}
	ret := funcutil.SetContain(sr.receivedSealedSegmentIDsSet, sr.receivedGlobalSegmentIDsSet)
	log.Debug("Proxy searchResultBuf readyToReduce", zap.Any("ret", ret))
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

func (sched *taskScheduler) collectResultLoop() {
	defer sched.wg.Done()

	queryResultMsgStream, _ := sched.msFactory.NewQueryMsgStream(sched.ctx)
	queryResultMsgStream.AsConsumer(Params.SearchResultChannelNames, Params.ProxySubName)
	log.Debug("Proxy", zap.Strings("SearchResultChannelNames", Params.SearchResultChannelNames),
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
				log.Debug("Proxy collectResultLoop exit Chan closed")
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
						log.Debug("Proxy collectResultLoop Got a SearchResultMsg, but we should ignore", zap.Any("ReqID", reqID))
						continue
					}
					t := sched.getTaskByReqID(reqID)
					log.Debug("Proxy collectResultLoop Got a SearchResultMsg", zap.Any("ReqID", reqID))
					if t == nil {
						log.Debug("Proxy collectResultLoop GetTaskByReqID failed", zap.String("reqID", reqIDStr))
						delete(searchResultBufs, reqID)
						searchResultBufFlags[reqID] = true
						continue
					}

					st, ok := t.(*searchTask)
					if !ok {
						log.Debug("Proxy collectResultLoop type assert t as searchTask failed", zap.Any("ReqID", reqID))
						delete(searchResultBufs, reqID)
						searchResultBufFlags[reqID] = true
						continue
					}

					resultBuf, ok := searchResultBufs[reqID]
					if !ok {
						resultBuf = newSearchResultBuf()
						vchans, err := st.getVChannels()
						log.Debug("Proxy collectResultLoop, first receive", zap.Any("reqID", reqID), zap.Any("vchans", vchans),
							zap.Error(err))
						if err != nil {
							delete(searchResultBufs, reqID)
							continue
						}
						for _, vchan := range vchans {
							resultBuf.usedVChans[vchan] = struct{}{}
						}
						pchans, err := st.getChannels()
						log.Debug("Proxy collectResultLoop, first receive", zap.Any("reqID", reqID), zap.Any("pchans", pchans),
							zap.Error(err))
						if err != nil {
							delete(searchResultBufs, reqID)
							continue
						}
						searchResultBufs[reqID] = resultBuf
					}
					resultBuf.addPartialResult(&searchResultMsg.SearchResults)

					//t := sched.getTaskByReqID(reqID)
					{
						colName := t.(*searchTask).query.CollectionName
						log.Debug("Proxy collectResultLoop", zap.String("collection name", colName), zap.String("reqID", reqIDStr), zap.Int("answer cnt", len(searchResultBufs[reqID].resultBuf)))
					}

					if resultBuf.readyToReduce() {
						log.Debug("Proxy collectResultLoop readyToReduce and assign to reduce")
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
					//	log.Debug("proxy", zap.String("RetrieveResult GetTaskByReqID failed, reqID = ", reqIDStr))
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
						log.Debug("Proxy collectResultLoop Got a queryResultMsg, but we should ignore", zap.Any("ReqID", reqID))
						continue
					}
					t := sched.getTaskByReqID(reqID)
					log.Debug("Proxy collectResultLoop Got a queryResultMsg", zap.Any("ReqID", reqID))
					if t == nil {
						log.Debug("Proxy collectResultLoop GetTaskByReqID failed", zap.String("reqID", reqIDStr))
						delete(queryResultBufs, reqID)
						queryResultBufFlags[reqID] = true
						continue
					}

					st, ok := t.(*queryTask)
					if !ok {
						log.Debug("Proxy collectResultLoop type assert t as queryTask failed")
						delete(queryResultBufs, reqID)
						queryResultBufFlags[reqID] = true
						continue
					}

					resultBuf, ok := queryResultBufs[reqID]
					if !ok {
						resultBuf = newQueryResultBuf()
						vchans, err := st.getVChannels()
						log.Debug("Proxy collectResultLoop, first receive", zap.Any("reqID", reqID), zap.Any("vchans", vchans),
							zap.Error(err))
						if err != nil {
							delete(queryResultBufs, reqID)
							continue
						}
						for _, vchan := range vchans {
							resultBuf.usedVChans[vchan] = struct{}{}
						}
						pchans, err := st.getChannels()
						log.Debug("Proxy collectResultLoop, first receive", zap.Any("reqID", reqID), zap.Any("pchans", pchans),
							zap.Error(err))
						if err != nil {
							delete(queryResultBufs, reqID)
							continue
						}
						queryResultBufs[reqID] = resultBuf
					}
					resultBuf.addPartialResult(&queryResultMsg.RetrieveResults)

					//t := sched.getTaskByReqID(reqID)
					{
						colName := t.(*queryTask).query.CollectionName
						log.Debug("Proxy collectResultLoop", zap.String("collection name", colName), zap.String("reqID", reqIDStr), zap.Int("answer cnt", len(queryResultBufs[reqID].resultBuf)))
					}

					if resultBuf.readyToReduce() {
						log.Debug("Proxy collectResultLoop readyToReduce and assign to reduce")
						queryResultBufFlags[reqID] = true
						st.resultBuf <- resultBuf.resultBuf
						delete(queryResultBufs, reqID)
					}
					sp.Finish()
				}
			}
		case <-sched.ctx.Done():
			log.Debug("Proxy collectResultLoop is closed ...")
			return
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

	sched.wg.Add(1)
	go sched.collectResultLoop()

	return nil
}

func (sched *taskScheduler) Close() {
	sched.cancel()
	sched.wg.Wait()
}

func (sched *taskScheduler) TaskDoneTest(ts Timestamp) bool {
	ddTaskDone := sched.ddQueue.TaskDoneTest(ts)
	dmTaskDone := sched.dmQueue.TaskDoneTest(ts)
	return ddTaskDone && dmTaskDone
}

func (sched *taskScheduler) getPChanStatistics() (map[pChan]*pChanStatistics, error) {
	return sched.dmQueue.getPChanStatsInfo()
}
