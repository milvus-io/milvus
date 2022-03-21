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

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
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
		log.Debug("Proxy task with tID already in active task list!", zap.Any("ID", tID))
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

	log.Debug("Proxy task not in active task list! ts", zap.Any("taskID", taskID))
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
		log.Debug("Proxy task not in active task list!", zap.Any("taskID", taskID))
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

	searchResultCh   chan *internalpb.SearchResults
	retrieveResultCh chan *internalpb.RetrieveResults
}

type schedOpt func(*taskScheduler)

func schedOptWithSearchResultCh(ch chan *internalpb.SearchResults) schedOpt {
	return func(sched *taskScheduler) {
		sched.searchResultCh = ch
	}
}

func schedOptWithRetrieveResultCh(ch chan *internalpb.RetrieveResults) schedOpt {
	return func(sched *taskScheduler) {
		sched.retrieveResultCh = ch
	}
}

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
		log.Error("Failed to pre-execute task: "+err.Error(),
			zap.String("traceID", traceID))
		return
	}

	span.LogFields(oplog.Int64("scheduler process Execute", t.ID()))
	err = t.Execute(ctx)
	if err != nil {
		trace.LogError(span, err)
		log.Error("Failed to execute task: "+err.Error(),
			zap.String("traceID", traceID))
		return
	}

	span.LogFields(oplog.Int64("scheduler process PostExecute", t.ID()))
	err = t.PostExecute(ctx)

	if err != nil {
		trace.LogError(span, err)
		log.Error("Failed to post-execute task: "+err.Error(),
			zap.String("traceID", traceID))
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

type resultBufHeader struct {
	msgID                       UniqueID
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

func newSearchResultBuf(msgID UniqueID) *searchResultBuf {
	return &searchResultBuf{
		resultBufHeader: resultBufHeader{
			usedVChans:                  make(map[interface{}]struct{}),
			receivedVChansSet:           make(map[interface{}]struct{}),
			receivedSealedSegmentIDsSet: make(map[interface{}]struct{}),
			receivedGlobalSegmentIDsSet: make(map[interface{}]struct{}),
			haveError:                   false,
			msgID:                       msgID,
		},
		resultBuf: make([]*internalpb.SearchResults, 0),
	}
}

func newQueryResultBuf(msgID UniqueID) *queryResultBuf {
	return &queryResultBuf{
		resultBufHeader: resultBufHeader{
			usedVChans:                  make(map[interface{}]struct{}),
			receivedVChansSet:           make(map[interface{}]struct{}),
			receivedSealedSegmentIDsSet: make(map[interface{}]struct{}),
			receivedGlobalSegmentIDsSet: make(map[interface{}]struct{}),
			haveError:                   false,
			msgID:                       msgID,
		},
		resultBuf: make([]*internalpb.RetrieveResults, 0),
	}
}

func (sr *resultBufHeader) readyToReduce() bool {
	if sr.haveError {
		log.Debug("Proxy searchResultBuf readyToReduce", zap.Any("haveError", true))
		return true
	}

	log.Debug("check if result buf is ready to reduce",
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", sr.msgID),
		zap.Any("receivedVChansSet", funcutil.SetToSlice(sr.receivedVChansSet)),
		zap.Any("usedVChans", funcutil.SetToSlice(sr.usedVChans)),
		zap.Any("receivedSealedSegmentIDsSet", funcutil.SetToSlice(sr.receivedSealedSegmentIDsSet)),
		zap.Any("receivedGlobalSegmentIDsSet", funcutil.SetToSlice(sr.receivedGlobalSegmentIDsSet)))

	ret1 := funcutil.SetContain(sr.receivedVChansSet, sr.usedVChans)
	if !ret1 {
		return false
	}

	return funcutil.SetContain(sr.receivedSealedSegmentIDsSet, sr.receivedGlobalSegmentIDsSet)
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

	searchResultBufs := make(map[UniqueID]*searchResultBuf)
	searchResultBufFlags := newIDCache(Params.ProxyCfg.BufFlagExpireTime, Params.ProxyCfg.BufFlagCleanupInterval) // if value is true, we can ignore searchResult
	queryResultBufs := make(map[UniqueID]*queryResultBuf)
	queryResultBufFlags := newIDCache(Params.ProxyCfg.BufFlagExpireTime, Params.ProxyCfg.BufFlagCleanupInterval) // if value is true, we can ignore queryResult

	processSearchResult := func(results *internalpb.SearchResults) error {
		reqID := results.GetReqID()

		ignoreThisResult, ok := searchResultBufFlags.Get(reqID)
		if !ok {
			searchResultBufFlags.Set(reqID, false)
			ignoreThisResult = false
		}
		if ignoreThisResult {
			log.Debug("got a search result, but we should ignore", zap.String("role", typeutil.ProxyRole), zap.Int64("ReqID", reqID))
			return nil
		}

		log.Debug("got a search result", zap.String("role", typeutil.ProxyRole), zap.Int64("ReqID", reqID))

		t := sched.getTaskByReqID(reqID)

		if t == nil {
			log.Debug("got a search result, but not in task scheduler", zap.String("role", typeutil.ProxyRole), zap.Int64("ReqID", reqID))
			delete(searchResultBufs, reqID)
			searchResultBufFlags.Set(reqID, true)
		}

		st, ok := t.(*searchTask)
		if !ok {
			log.Debug("got a search result, but the related task is not of search task", zap.String("role", typeutil.ProxyRole), zap.Int64("ReqID", reqID))
			delete(searchResultBufs, reqID)
			searchResultBufFlags.Set(reqID, true)
			return nil
		}

		resultBuf, ok := searchResultBufs[reqID]
		if !ok {
			log.Debug("first receive search result of this task", zap.String("role", typeutil.ProxyRole), zap.Int64("reqID", reqID))
			resultBuf = newSearchResultBuf(reqID)
			vchans, err := st.getVChannels()
			if err != nil {
				delete(searchResultBufs, reqID)
				log.Warn("failed to get virtual channels", zap.String("role", typeutil.ProxyRole), zap.Error(err), zap.Int64("reqID", reqID))
				return err
			}
			for _, vchan := range vchans {
				resultBuf.usedVChans[vchan] = struct{}{}
			}
			searchResultBufs[reqID] = resultBuf
		}
		resultBuf.addPartialResult(results)

		colName := t.(*searchTask).query.CollectionName
		log.Debug("process search result", zap.String("role", typeutil.ProxyRole), zap.String("collection", colName), zap.Int64("reqID", reqID), zap.Int("answer cnt", len(searchResultBufs[reqID].resultBuf)))

		if resultBuf.readyToReduce() {
			log.Debug("process search result, ready to reduce", zap.String("role", typeutil.ProxyRole), zap.Int64("reqID", reqID))
			searchResultBufFlags.Set(reqID, true)
			st.resultBuf <- resultBuf.resultBuf
			delete(searchResultBufs, reqID)
		}

		return nil
	}

	processRetrieveResult := func(results *internalpb.RetrieveResults) error {
		reqID := results.GetReqID()

		ignoreThisResult, ok := queryResultBufFlags.Get(reqID)
		if !ok {
			queryResultBufFlags.Set(reqID, false)
			ignoreThisResult = false
		}
		if ignoreThisResult {
			log.Debug("got a retrieve result, but we should ignore", zap.String("role", typeutil.ProxyRole), zap.Int64("ReqID", reqID))
			return nil
		}

		log.Debug("got a retrieve result", zap.String("role", typeutil.ProxyRole), zap.Int64("ReqID", reqID))

		t := sched.getTaskByReqID(reqID)

		if t == nil {
			log.Debug("got a retrieve result, but not in task scheduler", zap.String("role", typeutil.ProxyRole), zap.Int64("ReqID", reqID))
			delete(queryResultBufs, reqID)
			queryResultBufFlags.Set(reqID, true)
		}

		st, ok := t.(*queryTask)
		if !ok {
			log.Debug("got a retrieve result, but the related task is not of retrieve task", zap.String("role", typeutil.ProxyRole), zap.Int64("ReqID", reqID))
			delete(queryResultBufs, reqID)
			queryResultBufFlags.Set(reqID, true)
			return nil
		}

		resultBuf, ok := queryResultBufs[reqID]
		if !ok {
			log.Debug("first receive retrieve result of this task", zap.String("role", typeutil.ProxyRole), zap.Int64("reqID", reqID))
			resultBuf = newQueryResultBuf(reqID)
			vchans, err := st.getVChannels()
			if err != nil {
				delete(queryResultBufs, reqID)
				log.Warn("failed to get virtual channels", zap.String("role", typeutil.ProxyRole), zap.Error(err), zap.Int64("reqID", reqID))
				return err
			}
			for _, vchan := range vchans {
				resultBuf.usedVChans[vchan] = struct{}{}
			}
			queryResultBufs[reqID] = resultBuf
		}
		resultBuf.addPartialResult(results)

		colName := t.(*queryTask).query.CollectionName
		log.Debug("process retrieve result", zap.String("role", typeutil.ProxyRole), zap.String("collection", colName), zap.Int64("reqID", reqID), zap.Int("answer cnt", len(queryResultBufs[reqID].resultBuf)))

		if resultBuf.readyToReduce() {
			log.Debug("process retrieve result, ready to reduce", zap.String("role", typeutil.ProxyRole), zap.Int64("reqID", reqID))
			queryResultBufFlags.Set(reqID, true)
			st.resultBuf <- resultBuf.resultBuf
			delete(queryResultBufs, reqID)
		}

		return nil
	}

	for {
		select {
		case <-sched.ctx.Done():
			log.Info("task scheduler's result loop of Proxy exit", zap.String("reason", "context done"))
			return
		case sr, ok := <-sched.searchResultCh:
			if !ok {
				log.Info("task scheduler's result loop of Proxy exit", zap.String("reason", "search result channel closed"))
				return
			}
			if err := processSearchResult(sr); err != nil {
				log.Warn("failed to process search result", zap.Error(err))
			}
		case rr, ok := <-sched.retrieveResultCh:
			if !ok {
				log.Info("task scheduler's result loop of Proxy exit", zap.String("reason", "retrieve result channel closed"))
				return
			}
			if err := processRetrieveResult(rr); err != nil {
				log.Warn("failed to process retrieve result", zap.Error(err))
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

	sched.wg.Add(1)
	go sched.collectResultLoop()

	return nil
}

func (sched *taskScheduler) Close() {
	sched.cancel()
	sched.wg.Wait()
}

func (sched *taskScheduler) getPChanStatistics() (map[pChan]*pChanStatistics, error) {
	return sched.dmQueue.getPChanStatsInfo()
}
