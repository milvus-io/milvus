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
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/internal/util/errorutil"

	oplog "github.com/opentracing/opentracing-go/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// queryCollection manages and executes the retrieve and search tasks, it can be created
// by LoadCollection or LoadPartition, but can only be destroyed by ReleaseCollection.
// Currently query behaviors are defined below, if:
// 1. LoadCollection --> ReleaseCollection: Query would be failed in proxy because collection is not loaded;
// 2. LoadCollection --> ReleasePartition: Not allowed, release would failed;
// 3. LoadPartition --> ReleaseCollection: Query would be failed in proxy because collection is not loaded;
// 4. LoadPartition --> ReleasePartition: Query in collection should return empty results, and query in partition should return notLoaded error.
type queryCollection struct {
	releaseCtx context.Context
	cancel     context.CancelFunc

	collectionID UniqueID
	historical   *historical
	streaming    *streaming

	unsolvedMsgMu sync.Mutex // guards unsolvedMsg
	unsolvedMsg   []queryMsg

	tSafeWatchersMu sync.RWMutex // guards tSafeWatchers
	tSafeWatchers   map[Channel]*tSafeWatcher
	tSafeUpdate     bool
	watcherCond     *sync.Cond

	serviceableTimeMutex sync.RWMutex // guards serviceableTime
	serviceableTime      Timestamp

	queryMsgStream msgstream.MsgStream
	sessionManager *SessionManager

	localChunkManager  storage.ChunkManager
	remoteChunkManager storage.ChunkManager
	vectorChunkManager *storage.VectorChunkManager
	localCacheEnabled  bool

	globalSegmentManager *globalSealedSegmentManager

	mergedMsgs           []queryMsg
	needWaitNewTsafeMsgs []queryMsg

	executeChan     chan queryMsg
	notifyChan      chan bool
	receiveMsgChan  chan bool
	tsafeUpdateChan chan bool
	publishChan     chan *pubResult
	executeNQNum    atomic.Value
	wg              sync.WaitGroup
	mergeMsgs       bool
}

const (
	maxExecuteReqs = 1
	maxExecuteNQ   = 5000
)

type qcOpt func(*queryCollection)

func qcOptWithSessionManager(s *SessionManager) qcOpt {
	return func(qc *queryCollection) {
		qc.sessionManager = s
	}
}

func newQueryCollection(ctx context.Context,
	collectionID UniqueID,
	historical *historical,
	streaming *streaming,
	factory msgstream.Factory,
	localChunkManager storage.ChunkManager,
	remoteChunkManager storage.ChunkManager,
	localCacheEnabled bool,
	opts ...qcOpt,
) (*queryCollection, error) {

	unsolvedMsg := make([]queryMsg, 0)
	ctx1, cancel := context.WithCancel(ctx)
	queryStream, _ := factory.NewQueryMsgStream(ctx1)

	condMu := sync.Mutex{}

	qc := &queryCollection{
		releaseCtx: ctx1,
		cancel:     cancel,

		collectionID: collectionID,
		historical:   historical,
		streaming:    streaming,

		tSafeWatchers: make(map[Channel]*tSafeWatcher),
		tSafeUpdate:   false,
		watcherCond:   sync.NewCond(&condMu),

		unsolvedMsg: unsolvedMsg,

		queryMsgStream: queryStream,

		localChunkManager:    localChunkManager,
		remoteChunkManager:   remoteChunkManager,
		localCacheEnabled:    localCacheEnabled,
		globalSegmentManager: newGlobalSealedSegmentManager(collectionID),

		executeChan:     make(chan queryMsg, 1024),
		publishChan:     make(chan *pubResult, 1024),
		notifyChan:      make(chan bool, 1),
		receiveMsgChan:  make(chan bool, 1),
		tsafeUpdateChan: make(chan bool, 1),
		executeNQNum:    atomic.Value{},
		mergeMsgs:       Params.QueryNodeCfg.MergeSearchReqs,
	}
	qc.executeNQNum.Store(int64(0))

	for _, opt := range opts {
		opt(qc)
	}

	err := qc.registerCollectionTSafe()
	if err != nil {
		return nil, err
	}
	return qc, nil
}

func (q *queryCollection) start() {
	go q.queryMsgStream.Start()

	q.wg.Add(1)
	go q.consumeQuery()

	q.wg.Add(1)
	go q.schedulerSearchMsgs()

	q.wg.Add(1)
	go q.executeSearchMsgs()

	q.wg.Add(1)
	go q.publishResultLoop()
}

func (q *queryCollection) close() {
	if q.queryMsgStream != nil {
		q.queryMsgStream.Close()
	}
	q.globalSegmentManager.close()
	if q.vectorChunkManager != nil {
		err := q.vectorChunkManager.Close()
		if err != nil {
			log.Warn("close vector chunk manager error occurs", zap.Error(err))
		}
	}
	q.cancel()
	q.wg.Wait()
}

// registerCollectionTSafe registers tSafe watcher if vChannels exists
func (q *queryCollection) registerCollectionTSafe() error {
	streamingCollection, err := q.streaming.replica.getCollectionByID(q.collectionID)
	if err != nil {
		return err
	}
	for _, channel := range streamingCollection.getVChannels() {
		err := q.addTSafeWatcher(channel)
		if err != nil {
			return err
		}
	}
	log.Debug("register tSafe watcher and init watcher select case",
		zap.Any("collectionID", streamingCollection.ID()),
		zap.Any("dml channels", streamingCollection.getVChannels()))

	historicalCollection, err := q.historical.replica.getCollectionByID(q.collectionID)
	if err != nil {
		return err
	}
	for _, channel := range historicalCollection.getVDeltaChannels() {
		err := q.addTSafeWatcher(channel)
		if err != nil {
			return err
		}
	}
	log.Debug("register tSafe watcher and init watcher select case",
		zap.Any("collectionID", historicalCollection.ID()),
		zap.Any("delta channels", historicalCollection.getVDeltaChannels()))

	return nil
}

func (q *queryCollection) addTSafeWatcher(vChannel Channel) error {
	q.tSafeWatchersMu.Lock()
	defer q.tSafeWatchersMu.Unlock()
	if _, ok := q.tSafeWatchers[vChannel]; ok {
		err := errors.New(fmt.Sprintln("tSafeWatcher of queryCollection has been exists, ",
			"collectionID = ", q.collectionID, ", ",
			"channel = ", vChannel))
		log.Warn(err.Error())
		return nil
	}
	q.tSafeWatchers[vChannel] = newTSafeWatcher()
	err := q.streaming.tSafeReplica.registerTSafeWatcher(vChannel, q.tSafeWatchers[vChannel])
	if err != nil {
		return err
	}
	log.Debug("add tSafeWatcher to queryCollection",
		zap.Any("collectionID", q.collectionID),
		zap.Any("channel", vChannel),
	)
	go q.startWatcher(q.tSafeWatchers[vChannel].watcherChan(), q.tSafeWatchers[vChannel].closeCh)
	return nil
}

func (q *queryCollection) removeTSafeWatcher(channel Channel) error {
	q.tSafeWatchersMu.Lock()
	defer q.tSafeWatchersMu.Unlock()
	if _, ok := q.tSafeWatchers[channel]; !ok {
		err := errors.New(fmt.Sprintln("tSafeWatcher of queryCollection not exists, ",
			"collectionID = ", q.collectionID, ", ",
			"channel = ", channel))
		return err
	}
	q.tSafeWatchers[channel].close()
	delete(q.tSafeWatchers, channel)
	log.Debug("remove tSafeWatcher from queryCollection",
		zap.Any("collectionID", q.collectionID),
		zap.Any("channel", channel),
	)
	return nil
}

//czsa
func (q *queryCollection) startWatcher(channel <-chan bool, closeCh <-chan struct{}) {
	for {
		select {
		case <-q.releaseCtx.Done():
			log.Debug("stop queryCollection watcher because queryCollection ctx done", zap.Any("collectionID", q.collectionID))
			return
		case <-closeCh:
			log.Debug("stop queryCollection watcher because watcher closed", zap.Any("collectionID", q.collectionID))
			return
		case <-channel:
			// TODO: check if channel is closed
			//q.watcherCond.L.Lock()
			//q.tSafeUpdate = true
			//q.watcherCond.Broadcast()
			//q.tSafeUpdate = false
			//q.watcherCond.L.Unlock()
			_ = q.updateServiceableTime()
		}
	}
}

func (q *queryCollection) addToUnsolvedMsg(msg queryMsg) {
	q.unsolvedMsgMu.Lock()
	defer q.unsolvedMsgMu.Unlock()
	q.unsolvedMsg = append(q.unsolvedMsg, msg)
}

func (q *queryCollection) popAllUnsolvedMsg() []queryMsg {
	q.unsolvedMsgMu.Lock()
	defer q.unsolvedMsgMu.Unlock()
	ret := make([]queryMsg, 0, len(q.unsolvedMsg))
	ret = append(ret, q.unsolvedMsg...)
	q.unsolvedMsg = q.unsolvedMsg[:0]
	return ret
}

func (q *queryCollection) getServiceableTime() Timestamp {
	gracefulTimeInMilliSecond := Params.QueryNodeCfg.GracefulTime
	gracefulTime := typeutil.ZeroTimestamp
	if gracefulTimeInMilliSecond > 0 {
		gracefulTime = tsoutil.ComposeTS(gracefulTimeInMilliSecond, 0)
	}
	q.serviceableTimeMutex.RLock()
	defer q.serviceableTimeMutex.RUnlock()
	return q.serviceableTime + gracefulTime
}

func (q *queryCollection) setServiceableTime(t Timestamp) {
	q.serviceableTimeMutex.Lock()
	defer q.serviceableTimeMutex.Unlock()

	if t < q.serviceableTime {
		return
	}
	q.serviceableTime = t

	ps, _ := tsoutil.ParseHybridTs(t)
	metrics.QueryNodeServiceTime.WithLabelValues(fmt.Sprint(q.collectionID), fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Set(float64(ps))
}

func (q *queryCollection) updateServiceableTime() error {
	t := Timestamp(math.MaxInt64)
	for channel := range q.tSafeWatchers {
		ts, err := q.streaming.tSafeReplica.getTSafe(channel)
		if err != nil {
			return err
		}
		if ts <= t {
			t = ts
		}
	}
	q.setServiceableTime(t)

	if len(q.tsafeUpdateChan) == 0 {
		q.tsafeUpdateChan <- true
	}

	return nil
}

func (q *queryCollection) checkTimeout(msg queryMsg) bool {
	curTime := tsoutil.GetCurrentTime()
	curTimePhysical, _ := tsoutil.ParseTS(curTime)
	timeoutTsPhysical, _ := tsoutil.ParseTS(msg.TimeoutTs())
	log.Debug("check if query timeout",
		zap.Int64("collectionID", q.collectionID),
		zap.Int64("msgID", msg.ID()),
		zap.Uint64("TimeoutTs", msg.TimeoutTs()),
		zap.Uint64("curTime", curTime),
		zap.Time("timeoutTsPhysical", timeoutTsPhysical),
		zap.Time("curTimePhysical", curTimePhysical),
	)
	return msg.TimeoutTs() > typeutil.ZeroTimestamp && curTime >= msg.TimeoutTs()
}

func (q *queryCollection) consumeQuery() {
	defer q.wg.Done()
	for {
		select {
		case <-q.releaseCtx.Done():
			log.Debug("stop queryCollection's receiveQueryMsg", zap.Int64("collectionID", q.collectionID))
			return
		default:
			msgPack := q.queryMsgStream.Consume()
			if msgPack == nil || len(msgPack.Msgs) <= 0 {
				//msgPackNil := msgPack == nil
				//msgPackEmpty := true
				//if msgPack != nil {
				//	msgPackEmpty = len(msgPack.Msgs) <= 0
				//}
				//log.Debug("consume query message failed", zap.Any("msgPack is Nil", msgPackNil),
				//	zap.Any("msgPackEmpty", msgPackEmpty))
				continue
			}
			for _, msg := range msgPack.Msgs {
				switch sm := msg.(type) {
				case *msgstream.SearchMsg:
					newMsg := convertSearchMsg(sm)
					err := q.receiveQueryMsg(newMsg)
					if err != nil {
						log.Warn(err.Error())
						pubRet := &pubResult{
							Msg: newMsg,
							Err: err,
						}
						q.addToPublishChan(pubRet)
					}
				case *msgstream.RetrieveMsg:
					newMsg := convertToRetrieveMsg(sm)
					err := q.receiveQueryMsg(newMsg)
					if err != nil {
						log.Warn(err.Error())
						pubRet := &pubResult{
							Msg: newMsg,
							Err: err,
						}
						q.addToPublishChan(pubRet)
					}
				case *msgstream.SealedSegmentsChangeInfoMsg:
					q.adjustByChangeInfo(sm)
				default:
					log.Warn("unsupported msg type in search channel", zap.Any("msg", sm))
				}
			}
		}
	}
}

func (q *queryCollection) publishResultLoop() {
	defer q.wg.Done()
	for {
		select {
		case <-q.releaseCtx.Done():
			log.Debug("stop Collection's publishSearchResultLoop", zap.Int64("collectionID", q.collectionID))
			return
		case msg, ok := <-q.publishChan:
			if ok {
				if msg.Err != nil {
					publishErr := q.publishFailedQueryResult(msg.Msg, msg.Err)
					if publishErr != nil {
						log.Warn(publishErr.Error())
					}
				} else {
					switch msg.Type() {
					case commonpb.MsgType_Search:
						var el errorutil.ErrorList
						for _, ret := range msg.SearchRet.results {
							publishErr := q.publishSearchResult(ret, ret.Base.SourceID)
							if publishErr != nil {
								el = append(el, publishErr)
							}
							publishResultDuration := msg.Msg.GetTimeRecorder().Record(fmt.Sprintf("publish search result, msgID = %d", msg.Msg.ID()))
							log.Debug(log.BenchmarkRoot, zap.String(log.BenchmarkRole, typeutil.QueryNodeRole), zap.String(log.BenchmarkStep, "PublishSearchResult"),
								zap.Int64(log.BenchmarkCollectionID, msg.Msg.GetCollectionID()),
								zap.Int64(log.BenchmarkMsgID, msg.Msg.ID()), zap.Int64(log.BenchmarkDuration, publishResultDuration.Microseconds()))
						}
						msg.SearchRet.Close()
						if el != nil {
							log.Warn("publishSearchResult failed", zap.Error(el))
						}
						metrics.QueryNodeSearch.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Set(float64(msg.Msg.GetTimeRecorder().ElapseSpan().Microseconds()))
					case commonpb.MsgType_Retrieve:
						err := q.publishRetrieveResult(msg.RetrieveRet, msg.Msg.SourceID())
						if err != nil {
							log.Warn("publishSearchResult failed", zap.Error(err))
						}
					default:
						log.Error("receive an indeterminate type query request.")
					}
				}
			} else {
				log.Warn("canDoSearchRequest channel has been closed")
				panic("queryCollection canDoSearchRequest channel has been closed.")
			}
		}
	}
}

func (q *queryCollection) addToPublishChan(ret *pubResult) {
	select {
	case <-q.releaseCtx.Done():
		log.Debug("stop Collection's executeSearchMsg", zap.Int64("collectionID", q.collectionID))
		return
	case q.publishChan <- ret:
		return
	}
}

func (q *queryCollection) executeSearchMsgs() {
	defer q.wg.Done()
	for {
		select {
		case <-q.releaseCtx.Done():
			log.Debug("stop Collection's executeSearchMsg", zap.Int64("collectionID", q.collectionID))
			return
		case msg, ok := <-q.executeChan:
			if ok {
				log.Debug("receive a query msg", zap.Int64("msgID", msg.ID()), zap.Any("msg type", msg.Type()))
				switch msg.Type() {
				case commonpb.MsgType_Search:
					ret, err := q.search(msg)
					pubRet := &pubResult{
						Msg:       msg,
						Err:       err,
						SearchRet: ret,
					}
					q.addToPublishChan(pubRet)
					q.executeNQNum.Store(q.executeNQNum.Load().(int64) - msg.(*searchMsg).NQ)
					metrics.QueryNodeExecuteReqs.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Add(float64(len(msg.(*searchMsg).ReqIDs)))
					if len(q.notifyChan) == 0 {
						q.notifyChan <- true
					}
				case commonpb.MsgType_Retrieve:
					ret, err := q.retrieve(msg)
					pubRet := &pubResult{
						Msg:         msg,
						Err:         err,
						RetrieveRet: ret,
					}
					q.addToPublishChan(pubRet)
				default:
					log.Error("receive an indeterminate type query request.")
				}
			} else {
				log.Warn("canDoSearchRequest channel has been closed")
				panic("queryCollection canDoSearchRequest channel has been closed.")
			}
		}
	}
}

func (q *queryCollection) adjustByChangeInfo(msg *msgstream.SealedSegmentsChangeInfoMsg) {
	for _, info := range msg.Infos {
		// precheck collection id, if not the same collection, skip
		for _, segment := range info.OnlineSegments {
			if segment.CollectionID != q.collectionID {
				return
			}
		}

		for _, segment := range info.OfflineSegments {
			if segment.CollectionID != q.collectionID {
				return
			}
		}

		// for OnlineSegments:
		for _, segment := range info.OnlineSegments {
			// 1. update global sealed segments
			q.globalSegmentManager.addGlobalSegmentInfo(segment)
			// 2. update excluded segment, cluster have been loaded sealed segments,
			// so we need to avoid getting growing segment from flow graph.
			q.streaming.replica.addExcludedSegments(segment.CollectionID, []*datapb.SegmentInfo{
				{
					ID:            segment.SegmentID,
					CollectionID:  segment.CollectionID,
					PartitionID:   segment.PartitionID,
					InsertChannel: segment.DmChannel,
					NumOfRows:     segment.NumRows,
					// TODO: add status, remove query pb segment status, use common pb segment status?
					DmlPosition: &internalpb.MsgPosition{
						// use max timestamp to filter out dm messages
						Timestamp: typeutil.MaxTimestamp,
					},
				},
			})
		}

		// for OfflineSegments:
		for _, segment := range info.OfflineSegments {
			// 1. update global sealed segments
			q.globalSegmentManager.removeGlobalSealedSegmentInfo(segment.SegmentID)
		}

		log.Info("Successfully changed global sealed segment info ",
			zap.Int64("collection ", q.collectionID),
			zap.Any("online segments ", info.OnlineSegments),
			zap.Any("offline segments ", info.OfflineSegments))
	}
}

func (q *queryCollection) receiveQueryMsg(msg queryMsg) error {
	msg.SetTimeRecorder()
	var collectionID = msg.GetCollectionID()
	if collectionID != q.collectionID {
		return nil
	}

	switch msg.Type() {
	case commonpb.MsgType_Retrieve, commonpb.MsgType_Search:
		log.Debug(log.BenchmarkRoot, zap.String(log.BenchmarkRole, typeutil.QueryNodeRole), zap.String(log.BenchmarkStep, "QueryNode-Receive"),
			zap.Int64(log.BenchmarkCollectionID, q.collectionID),
			zap.Int64(log.BenchmarkMsgID, msg.ID()), zap.Int64(log.BenchmarkDuration, time.Now().UnixNano()))

	default:
		err := fmt.Errorf("receive invalid msgType = %d", msg.Type())
		return err
	}

	sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	msg.SetTraceCtx(ctx)
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("receiveQueryMsg %d", msg.ID()))

	if q.checkTimeout(msg) {
		err := fmt.Errorf("do query failed in receiveQueryMsg because timeout, "+
			"collectionID = %d, msgID = %d, timeoutTS = %d", q.collectionID, msg.ID(), msg.TimeoutTs())
		return err
	}

	q.addToUnsolvedMsg(msg)
	if len(q.receiveMsgChan) == 0 {
		q.receiveMsgChan <- true
	}
	metrics.QueryNodeReceiveReqs.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Inc()
	sp.Finish()
	tr.Record("add msg to unSolved queue")
	return nil
}

func (q *queryCollection) checkSearchCanDo(msg queryMsg) (bool, error) {
	// check if collection has been released
	collection, err := q.historical.replica.getCollectionByID(q.collectionID)
	if err != nil {
		return false, err
	}
	guaranteeTs := msg.GuaranteeTs()
	if guaranteeTs >= collection.getReleaseTime() {
		err = fmt.Errorf("retrieve failed, collection has been released, msgID = %d, collectionID = %d", msg.ID(), q.collectionID)
		return false, err
	}

	serviceTime := q.getServiceableTime()
	gt, _ := tsoutil.ParseTS(guaranteeTs)
	st, _ := tsoutil.ParseTS(serviceTime)
	if guaranteeTs > serviceTime && (len(collection.getVChannels()) > 0 || len(collection.getVDeltaChannels()) > 0) {
		log.Debug("query msg can't do",
			zap.Any("collectionID", q.collectionID),
			zap.Any("sm.GuaranteeTimestamp", gt),
			zap.Any("serviceTime", st),
			zap.Any("delta seconds", (guaranteeTs-serviceTime)/(1000*1000*1000)),
			zap.Any("msgID", msg.ID()),
			zap.Any("msgType", msg.Type()),
		)
		msg.GetTimeRecorder().RecordSpan()
		return false, nil
	}
	return true, nil
}

func (q *queryCollection) popAndAddToQuery() {
	if len(q.executeChan) < maxExecuteReqs && q.executeNQNum.Load().(int64) < maxExecuteNQ {
		if len(q.mergedMsgs) > 0 {
			msg := q.mergedMsgs[0]
			q.executeChan <- msg
			q.mergedMsgs = q.mergedMsgs[1:]
			sMsg, ok := msg.(*searchMsg)
			if ok {
				q.executeNQNum.Store(q.executeNQNum.Load().(int64) + sMsg.NQ)
			}
		}
	}
}

func (q *queryCollection) schedulerSearchMsgs() {
	defer q.wg.Done()
	log.Debug("starting doUnsolvedMsg...", zap.Any("collectionID", q.collectionID))

	for {
		select {
		case <-q.releaseCtx.Done():
			log.Debug("stop Collection's schedulerSearchMsgs", zap.Int64("collectionID", q.collectionID))
			return
		case <-q.notifyChan:
			q.needWaitNewTsafeMsgs = append(q.needWaitNewTsafeMsgs, q.popAllUnsolvedMsg()...)
			if len(q.needWaitNewTsafeMsgs) == 0 && len(q.mergedMsgs) == 0 {
				continue
			}
			q.mergedMsgs, q.needWaitNewTsafeMsgs = q.mergeSearchReqsByMaxNQ(q.mergedMsgs, q.needWaitNewTsafeMsgs)
			q.popAndAddToQuery()

		case <-q.receiveMsgChan:
			q.needWaitNewTsafeMsgs = append(q.needWaitNewTsafeMsgs, q.popAllUnsolvedMsg()...)
			q.mergedMsgs, q.needWaitNewTsafeMsgs = q.mergeSearchReqsByMaxNQ(q.mergedMsgs, q.needWaitNewTsafeMsgs)
			q.popAndAddToQuery()

		case <-q.tsafeUpdateChan:
			q.needWaitNewTsafeMsgs = append(q.needWaitNewTsafeMsgs, q.popAllUnsolvedMsg()...)
			if len(q.needWaitNewTsafeMsgs) == 0 && len(q.mergedMsgs) == 0 {
				continue
			}
			q.mergedMsgs, q.needWaitNewTsafeMsgs = q.mergeSearchReqsByMaxNQ(q.mergedMsgs, q.needWaitNewTsafeMsgs)
			q.popAndAddToQuery()
			//runtime.GC()
		}
	}
}

func (q *queryCollection) retrieve(msg queryMsg) (*pubRetrieveResults, error) {
	// TODO(yukun)
	// step 1: get retrieve object and defer destruction
	// step 2: for each segment, call retrieve to get ids proto buffer
	// step 3: merge all proto in go
	// step 4: publish results
	// retrieveProtoBlob, err := proto.Marshal(&retrieveMsg.RetrieveRequest)
	msg.SetTimeRecorder()
	retrieveMsg := msg.(*retrieveMsg)
	sp, ctx := trace.StartSpanFromContext(retrieveMsg.TraceCtx())
	defer sp.Finish()
	retrieveMsg.SetTraceCtx(ctx)
	timestamp := retrieveMsg.TravelTimestamp

	collectionID := retrieveMsg.CollectionID
	collection, err := q.streaming.replica.getCollectionByID(collectionID)
	if err != nil {
		return nil, err
	}

	expr := retrieveMsg.SerializedExprPlan
	plan, err := createRetrievePlanByExpr(collection, expr, timestamp)
	if err != nil {
		return nil, err
	}
	defer plan.delete()

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("retrieve %d", retrieveMsg.CollectionID))

	var globalSealedSegments []UniqueID
	if len(retrieveMsg.PartitionIDs) > 0 {
		globalSealedSegments = q.globalSegmentManager.getGlobalSegmentIDsByPartitionIds(retrieveMsg.PartitionIDs)
	} else {
		globalSealedSegments = q.globalSegmentManager.getGlobalSegmentIDs()
	}

	var mergeList []*segcorepb.RetrieveResults

	if q.vectorChunkManager == nil {
		if q.localChunkManager == nil {
			return nil, fmt.Errorf("can not create vector chunk manager for local chunk manager is nil, msgID = %d", retrieveMsg.ID())
		}
		if q.remoteChunkManager == nil {
			return nil, fmt.Errorf("can not create vector chunk manager for remote chunk manager is nil, msgID = %d", retrieveMsg.ID())
		}
		q.vectorChunkManager = storage.NewVectorChunkManager(q.localChunkManager, q.remoteChunkManager,
			&etcdpb.CollectionMeta{
				ID:     collection.id,
				Schema: collection.schema,
			}, q.localCacheEnabled)
	}

	// historical retrieve
	log.Debug("historical retrieve start", zap.Int64("msgID", retrieveMsg.ID()))
	hisRetrieveResults, sealedSegmentRetrieved, sealedPartitionRetrieved, err := q.historical.retrieve(collectionID, retrieveMsg.PartitionIDs, q.vectorChunkManager, plan)
	if err != nil {
		return nil, err
	}
	mergeList = append(mergeList, hisRetrieveResults...)
	log.Debug("historical retrieve", zap.Int64("msgID", retrieveMsg.ID()), zap.Int64("collectionID", collectionID), zap.Int64s("retrieve partitionIDs", sealedPartitionRetrieved), zap.Int64s("retrieve segmentIDs", sealedSegmentRetrieved))
	tr.Record(fmt.Sprintf("historical retrieve done, msgID = %d", retrieveMsg.ID()))

	// streaming retrieve
	log.Debug("streaming retrieve start", zap.Int64("msgID", retrieveMsg.ID()))
	strRetrieveResults, streamingSegmentRetrived, streamingPartitionRetrived, err := q.streaming.retrieve(collectionID, retrieveMsg.PartitionIDs, plan)
	if err != nil {
		return nil, err
	}
	mergeList = append(mergeList, strRetrieveResults...)
	log.Debug("streaming retrieve", zap.Int64("msgID", retrieveMsg.ID()), zap.Int64("collectionID", collectionID), zap.Int64s("retrieve partitionIDs", streamingPartitionRetrived), zap.Int64s("retrieve segmentIDs", streamingSegmentRetrived))
	tr.Record(fmt.Sprintf("streaming retrieve done, msgID = %d", retrieveMsg.ID()))

	result, err := mergeRetrieveResults(mergeList)
	if err != nil {
		return nil, err
	}
	reduceDuration := tr.Record(fmt.Sprintf("merge result done, msgID = %d", retrieveMsg.ID()))
	metrics.QueryNodeReduceLatency.WithLabelValues(metrics.QueryLabel, fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Observe(float64(reduceDuration.Milliseconds()))

	retrieveResults := &internalpb.RetrieveResults{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_RetrieveResult,
			MsgID:    retrieveMsg.Base.MsgID,
			SourceID: retrieveMsg.Base.SourceID,
		},
		ReqID:                     retrieveMsg.ReqID,
		Status:                    &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		Ids:                       result.Ids,
		FieldsData:                result.FieldsData,
		SealedSegmentIDsRetrieved: sealedSegmentRetrieved,
		ChannelIDsRetrieved:       collection.getVChannels(),
		GlobalSealedSegmentIDs:    globalSealedSegments,
	}

	metrics.QueryNodeSQCount.WithLabelValues(metrics.SuccessLabel, metrics.QueryLabel, fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Inc()
	metrics.QueryNodeSQReqLatency.WithLabelValues(metrics.QueryLabel, fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Observe(float64(msg.GetTimeRecorder().ElapseSpan().Milliseconds()))

	//log.Debug("QueryNode publish RetrieveResultMsg",
	//	zap.Int64("msgID", retrieveMsg.ID()),
	//	zap.Any("vChannels", collection.getVChannels()),
	//	zap.Any("collectionID", collection.ID()),
	//	zap.Any("sealedSegmentRetrieved", sealedSegmentRetrieved),
	//)
	//tr.Elapse(fmt.Sprintf("all done, msgID = %d", retrieveMsg.ID()))
	return retrieveResults, nil
}

func mergeRetrieveResults(retrieveResults []*segcorepb.RetrieveResults) (*segcorepb.RetrieveResults, error) {
	var ret *segcorepb.RetrieveResults
	var skipDupCnt int64
	var idSet = make(map[int64]struct{})

	// merge results and remove duplicates
	for _, rr := range retrieveResults {
		// skip empty result, it will break merge result
		if rr == nil || len(rr.Offset) == 0 {
			continue
		}

		if ret == nil {
			ret = &segcorepb.RetrieveResults{
				Ids: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: []int64{},
						},
					},
				},
				FieldsData: make([]*schemapb.FieldData, len(rr.FieldsData)),
			}
		}

		if len(ret.FieldsData) != len(rr.FieldsData) {
			return nil, fmt.Errorf("mismatch FieldData in RetrieveResults")
		}

		dstIds := ret.Ids.GetIntId()
		for i, id := range rr.Ids.GetIntId().GetData() {
			if _, ok := idSet[id]; !ok {
				dstIds.Data = append(dstIds.Data, id)
				typeutil.AppendFieldData(ret.FieldsData, rr.FieldsData, int64(i))
				idSet[id] = struct{}{}
			} else {
				// primary keys duplicate
				skipDupCnt++
			}
		}
	}
	log.Debug("skip duplicated query result", zap.Int64("count", skipDupCnt))

	// not found, return default values indicating not result found
	if ret == nil {
		ret = &segcorepb.RetrieveResults{
			Ids:        &schemapb.IDs{},
			FieldsData: []*schemapb.FieldData{},
		}
	}

	return ret, nil
}

func (q *queryCollection) publishSearchResult(result *internalpb.SearchResults, nodeID UniqueID) error {
	metrics.QueryNodeSQCount.WithLabelValues(metrics.TotalLabel, metrics.SearchLabel, fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Inc()
	return q.sessionManager.SendSearchResult(q.releaseCtx, nodeID, result)
}

func (q *queryCollection) publishRetrieveResult(result *internalpb.RetrieveResults, nodeID UniqueID) error {
	metrics.QueryNodeSQCount.WithLabelValues(metrics.TotalLabel, metrics.QueryLabel, fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Inc()
	return q.sessionManager.SendRetrieveResult(q.releaseCtx, nodeID, result)
}

func (q *queryCollection) publishFailedQueryResult(msg queryMsg, err error) error {
	msgType := msg.Type()
	span, traceCtx := trace.StartSpanFromContext(msg.TraceCtx())
	defer span.Finish()
	msg.SetTraceCtx(traceCtx)

	switch msgType {
	case commonpb.MsgType_Retrieve:
		baseResult := &commonpb.MsgBase{
			MsgID:     msg.ID(),
			Timestamp: msg.BeginTs(),
			SourceID:  msg.SourceID(),
		}
		retrieveMsg := msg.(*retrieveMsg)
		baseResult.MsgType = commonpb.MsgType_RetrieveResult
		metrics.QueryNodeSQCount.WithLabelValues(metrics.FailLabel, metrics.QueryLabel, fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Inc()
		return q.publishRetrieveResult(&internalpb.RetrieveResults{
			Base:       baseResult,
			Status:     &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: err.Error()},
			ReqID:      retrieveMsg.ReqID,
			Ids:        nil,
			FieldsData: nil,
		}, msg.SourceID())
	case commonpb.MsgType_Search:
		searchMsg := msg.(*searchMsg)
		pubRet := generateErrPubSearchResults(searchMsg, err)
		var el errorutil.ErrorList
		for _, ret := range pubRet.results {
			err := q.publishSearchResult(ret, ret.Base.SourceID)
			if err != nil {
				el = append(el, err)
			}
		}
		if el != nil {
			log.Warn("publishFailedSearchResult failed", zap.Error(el))
			return el
		}

	default:
		return fmt.Errorf("publish invalid msgType %d", msgType)
	}
	return nil
}

func (q *queryCollection) search(qMsg queryMsg) (*pubSearchResults, error) {
	searchMsg, ok := qMsg.(*searchMsg)
	if !ok {
		panic("Unexpected error: qMsg is not of type searchMsg")
	}
	searchMsg.CombinePlaceHolderGroups()
	q.streaming.replica.queryRLock()
	q.historical.replica.queryRLock()
	defer q.historical.replica.queryRUnlock()
	defer q.streaming.replica.queryRUnlock()

	collectionID := searchMsg.CollectionID

	searchTime := searchMsg.tr.ElapseSpan().Microseconds()
	metrics.QueryNodeSearchNQ.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Set(float64(searchMsg.NQ))
	//time.Sleep(200 * time.Millisecond)
	for _, msgID := range searchMsg.ReqIDs {
		log.Info(log.BenchmarkRoot, zap.String(log.BenchmarkRole, typeutil.QueryNodeRole), zap.String(log.BenchmarkStep, "start search"),
			zap.Int64(log.BenchmarkCollectionID, collectionID),
			zap.Int64(log.BenchmarkMsgID, msgID), zap.Int64(log.BenchmarkDuration, searchTime))
	}
	sp, ctx := trace.StartSpanFromContext(searchMsg.TraceCtx())
	defer sp.Finish()
	searchMsg.SetTraceCtx(ctx)

	searchTimestamp := searchMsg.BeginTs()
	travelTimestamp := searchMsg.TravelTimestamp
	if travelTimestamp == 0 {
		travelTimestamp = typeutil.MaxTimestamp
	}

	collection, err := q.streaming.replica.getCollectionByID(searchMsg.CollectionID)
	if err != nil {
		return nil, err
	}

	var plan *SearchPlan
	if searchMsg.DslType == commonpb.DslType_BoolExprV1 {
		expr := searchMsg.SerializedExprPlan
		plan, err = createSearchPlanByExpr(collection, expr)
		if err != nil {
			return nil, err
		}
	} else {
		dsl := searchMsg.Dsl
		plan, err = createSearchPlan(collection, dsl)
		if err != nil {
			return nil, err
		}
	}

	defer plan.delete()

	topK := plan.getTopK()
	if topK == 0 {
		return nil, fmt.Errorf("limit must be greater than 0, msgID = %d", searchMsg.ID())
	}
	if topK >= 16385 {
		return nil, fmt.Errorf("limit %d is too large, msgID = %d", topK, searchMsg.ID())
	}
	searchRequestBlob := searchMsg.PlaceholderGroup
	searchReq, err := parseSearchRequest(plan, searchRequestBlob)
	if err != nil {
		return nil, err
	}
	defer searchReq.delete()

	nq := searchReq.getNumOfQuery()

	if searchMsg.DslType == commonpb.DslType_BoolExprV1 {
		sp.LogFields(oplog.String("statistical time", "stats start"),
			oplog.Object("nq", nq),
			oplog.Object("expr", searchMsg.SerializedExprPlan))
	} else {
		sp.LogFields(oplog.String("statistical time", "stats start"),
			oplog.Object("nq", nq),
			oplog.Object("dsl", searchMsg.Dsl))
	}

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("search %d(nq=%d, k=%d), msgID = %d", searchMsg.CollectionID, nq, topK, searchMsg.ID()))

	// get global sealed segments
	var globalSealedSegments []UniqueID
	if len(searchMsg.PartitionIDs) > 0 {
		globalSealedSegments = q.globalSegmentManager.getGlobalSegmentIDsByPartitionIds(searchMsg.PartitionIDs)
	} else {
		globalSealedSegments = q.globalSegmentManager.getGlobalSegmentIDs()
	}

	searchResults := make([]*SearchResult, 0)
	defer func() {
		deleteSearchResults(searchResults)
	}()
	// historical search
	trHis := timerecord.NewTimeRecorder("hisRecorder")
	log.Debug("historical search start", zap.Int64("msgID", searchMsg.ID()))
	var hisSearchResults []*SearchResult
	var sealedSegmentSearched []UniqueID
	var sealedPartitionSearched []UniqueID
	if q.historical.replica.getSegmentNum() != 0 {
		hisSearchResults, sealedSegmentSearched, sealedPartitionSearched, err = q.historical.search(searchReq, collection.id, searchMsg.PartitionIDs, plan, travelTimestamp)
		if err != nil {
			return nil, err
		}
		searchResults = append(searchResults, hisSearchResults...)
	}
	metrics.QueryNodeSearchHistorical.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Set(float64(trHis.ElapseSpan().Microseconds()))

	log.Debug("historical search", zap.Int64("msgID", searchMsg.ID()), zap.Int64("collectionID", collectionID), zap.Int64s("searched partitionIDs", sealedPartitionSearched), zap.Int64s("searched segmentIDs", sealedSegmentSearched))
	hisSearchDur := tr.Record(fmt.Sprintf("historical search done, msgID = %d", searchMsg.ID())).Microseconds()
	for _, msgID := range searchMsg.ReqIDs {
		log.Info(log.BenchmarkRoot, zap.String(log.BenchmarkRole, typeutil.QueryNodeRole), zap.String(log.BenchmarkStep, "HistoricalSearch"),
			zap.Int64(log.BenchmarkCollectionID, collectionID),
			zap.Int64(log.BenchmarkMsgID, msgID), zap.Int64(log.BenchmarkDuration, hisSearchDur))
	}
	// streaming search
	log.Debug("streaming search start", zap.Int64("msgID", searchMsg.ID()))
	if q.streaming.replica.getSegmentNum() != 0 {
		for _, channel := range collection.getVChannels() {
			strSearchResults, growingSegmentSearched, growingPartitionSearched, err := q.streaming.search(searchReq, collection.id, searchMsg.PartitionIDs, channel, plan, travelTimestamp)
			if err != nil {
				return nil, err
			}
			searchResults = append(searchResults, strSearchResults...)
			log.Debug("streaming search", zap.Int64("msgID", searchMsg.ID()), zap.Int64("collectionID", collectionID), zap.String("searched dmChannel", channel), zap.Int64s("searched partitionIDs", growingPartitionSearched), zap.Int64s("searched segmentIDs", growingSegmentSearched))
		}
	}
	streamingSearchDuration := tr.Record(fmt.Sprintf("streaming search done, msgID = %d", searchMsg.ID())).Microseconds()
	for _, msgID := range searchMsg.ReqIDs {
		log.Info(log.BenchmarkRoot, zap.String(log.BenchmarkRole, typeutil.QueryNodeRole), zap.String(log.BenchmarkStep, "StreamingSearch"),
			zap.Int64(log.BenchmarkCollectionID, collectionID),
			zap.Int64(log.BenchmarkMsgID, msgID), zap.Int64(log.BenchmarkDuration, streamingSearchDuration))
	}
	sp.LogFields(oplog.String("statistical time", "segment search end"))
	if len(searchResults) <= 0 {
		pubRet := &pubSearchResults{}
		for i, reqID := range searchMsg.ReqIDs {
			ret := &internalpb.SearchResults{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_SearchResult,
					MsgID:     searchMsg.Base.MsgID,
					Timestamp: searchMsg.BeginTs(),
					SourceID:  searchMsg.SourceIDs[i],
				},
				Status:                   &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
				ReqID:                    reqID,
				NumQueries:               searchMsg.OrigNQs[i],
				MetricType:               plan.getMetricType(),
				TopK:                     topK,
				SealedSegmentIDsSearched: sealedSegmentSearched,
				ChannelIDsSearched:       collection.getVChannels(),
				GlobalSealedSegmentIDs:   globalSealedSegments,
			}
			pubRet.results = append(pubRet.results, ret)
		}
		log.Debug("QueryNode Empty SearchResultMsg",
			zap.Any("collectionID", collection.id),
			zap.Any("msgID", searchMsg.ID()),
			zap.Any("vChannels", collection.getVChannels()),
			zap.Any("sealedSegmentSearched", sealedSegmentSearched),
		)
		metrics.QueryNodeSQReqLatency.WithLabelValues(metrics.SearchLabel, fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Observe(float64(searchMsg.tr.ElapseSpan().Milliseconds()))
		metrics.QueryNodeSQCount.WithLabelValues(metrics.SuccessLabel, metrics.SearchLabel, fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Inc()

		return pubRet, nil
	}

	numSegment := int64(len(searchResults))

	log.Debug("QueryNode reduce data", zap.Int64("msgID", searchMsg.ID()), zap.Int64("numSegment", numSegment))
	tr.RecordSpan()
	err = reduceSearchResultsAndFillData(plan, searchResults, numSegment)
	log.Debug("QueryNode reduce data finished", zap.Int64("msgID", searchMsg.ID()))
	sp.LogFields(oplog.String("statistical time", "reduceSearchResults end"))
	if err != nil {
		log.Error("QueryNode reduce data failed", zap.Int64("msgID", searchMsg.ID()), zap.Error(err))
		return nil, err
	}

	sInfo := parseSliceInfo(searchMsg.OrigNQs, searchMsg.NQ, searchMsg.ReqIDs, searchMsg.SourceIDs)

	// TODO: move sliceTopKs to sliceInfo
	sliceTopKs := make([]int32, 0)
	for i := 0; i < len(sInfo.slices); i++ {
		sliceTopKs = append(sliceTopKs, int32(topK))
	}
	blobs, err := marshal(collectionID, searchMsg.ID(), searchResults, int(numSegment), sInfo.slices, sliceTopKs)
	sp.LogFields(oplog.String("statistical time", "reorganizeSearchResults end"))
	if err != nil {
		deleteSearchResultDataBlobs(blobs)
		return nil, err
	}

	pubRet := &pubSearchResults{}

	for i, reqID := range sInfo.reqIDs {
		blob, err := getSearchResultDataBlob(blobs, i)
		if err != nil {
			deleteSearchResultDataBlobs(blobs)
			return nil, err
		}

		reduceTime := tr.RecordSpan()
		log.Debug(log.BenchmarkRoot, zap.String(log.BenchmarkRole, typeutil.QueryNodeRole), zap.String(log.BenchmarkStep, "QNReduceSearchResults"),
			zap.Int64(log.BenchmarkCollectionID, collectionID),
			zap.Int64(log.BenchmarkMsgID, reqID), zap.Int64(log.BenchmarkDuration, reduceTime.Microseconds()))
		metrics.QueryNodeReduceLatency.WithLabelValues(metrics.SearchLabel, fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Observe(float64(reduceTime.Milliseconds()))
		metrics.QueryNodeSearchReduce.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Set(float64(reduceTime.Microseconds()))

		ret := &internalpb.SearchResults{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_SearchResult,
				MsgID:     searchMsg.Base.MsgID,
				Timestamp: searchTimestamp,
				SourceID:  searchMsg.SourceIDs[i],
			},
			ReqID:                    reqID,
			Status:                   &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			MetricType:               plan.getMetricType(),
			NumQueries:               searchMsg.OrigNQs[i],
			TopK:                     searchMsg.OrigTopKs[i],
			SlicedBlob:               blob,
			SlicedOffset:             sInfo.getSliceOffset(i),
			SlicedNumCount:           sInfo.getSliceNum(i),
			SealedSegmentIDsSearched: sealedSegmentSearched,
			ChannelIDsSearched:       collection.getVChannels(),
			GlobalSealedSegmentIDs:   globalSealedSegments,
		}
		pubRet.results = append(pubRet.results, ret)

		log.Debug("QueryNode SearchResultMsg",
			zap.Any("collectionID", collection.id),
			zap.Any("msgID", sInfo.reqIDs[i]),
			zap.Any("vChannels", collection.getVChannels()),
			zap.Any("sealedSegmentSearched", sealedSegmentSearched),
		)
		metrics.QueryNodeSQReqLatency.WithLabelValues(metrics.SearchLabel,
			fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Observe(float64(searchMsg.tr.ElapseSpan().Milliseconds()))
		metrics.QueryNodeSQCount.WithLabelValues(metrics.SuccessLabel,
			metrics.SearchLabel,
			fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Inc()
		//publishResultDuration := tr.Record(fmt.Sprintf("publish search result, msgID = %d", searchMsg.ID()))
		//log.Debug(log.BenchmarkRoot, zap.String(log.BenchmarkRole, typeutil.QueryNodeRole), zap.String(log.BenchmarkStep, "PublishSearchResult"),
		//	zap.Int64(log.BenchmarkCollectionID, collectionID),
		//	zap.Int64(log.BenchmarkMsgID, searchMsg.ID()), zap.Int64(log.BenchmarkDuration, publishResultDuration.Microseconds()))
	}
	sp.LogFields(oplog.String("statistical time", "stats done"))
	tr.Elapse(fmt.Sprintf("all done, msgID = %d", searchMsg.ID()))
	pubRet.Blobs = blobs
	return pubRet, nil
}

func generateErrPubSearchResults(msg *searchMsg, err error) *pubSearchResults {
	searchTimestamp := msg.BeginTs()
	pubRet := pubSearchResults{}
	for i, reqID := range msg.ReqIDs {
		ret := &internalpb.SearchResults{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_SearchResult,
				MsgID:     msg.Base.MsgID,
				Timestamp: searchTimestamp,
				SourceID:  msg.SourceIDs[i],
			},
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			ReqID:      reqID,
			NumQueries: msg.OrigNQs[i],
		}
		pubRet.results = append(pubRet.results, ret)
	}
	return &pubRet
}

// mergeSearchReqsByMaxNQ uses a greedy way to merge query requests, the NQ has an upper limit.
// 1. check if msg can do.
// 2. check if these requests that whose can do can be merged.
// 3. Merged these requests that whose can be merged.
// TODO @xiaocai2333: break away queryCollection
func (q *queryCollection) mergeSearchReqsByMaxNQ(mergedMsgs []queryMsg, queryMsgs []queryMsg) ([]queryMsg, []queryMsg) {
	if len(queryMsgs) == 0 {
		return mergedMsgs, queryMsgs
	}
	canNotDoMsg := make([]queryMsg, 0)
	log.Debug("merge reqs", zap.Int("merged msgs", len(mergedMsgs)), zap.Int("query msgs", len(queryMsgs)))
	metrics.QueryNodeWaitForMergeReqs.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Set(float64(len(queryMsgs)))
	for i := 0; i < len(queryMsgs); i++ {
		msg := queryMsgs[i]
		ok, err := q.checkSearchCanDo(msg)
		log.Debug("judge if msg can do", zap.Int64("msgID", msg.ID()), zap.Int64("collectionID", q.collectionID),
			zap.Bool("ok", ok), zap.Error(err), zap.Bool("merge search requests", q.mergeMsgs))

		if err != nil {
			pubRet := &pubResult{
				Msg: msg,
				Err: err,
			}
			q.addToPublishChan(pubRet)
			log.Error("search request fast fail", zap.Int64("msgID", msg.ID()), zap.Error(err))
			continue
		}
		if ok {
			if q.mergeMsgs {
				merge := false
				for j, mergedMsg := range mergedMsgs {
					if canMerge(mergedMsg, msg) {
						mergedMsgs[j] = mergeSearchMsg(mergedMsg, msg)
						merge = true
						log.Info("Merge search message", zap.Int("num", mergedMsg.GetNumMerged()),
							zap.Int64("mergedMsgID", mergedMsg.ID()), zap.Int64("msgID", msg.ID()),
							zap.Int("merged msg nq", msg.GetNumMerged()))
						break
					}
				}
				if !merge {
					mergedMsgs = append(mergedMsgs, msg)
				}
			} else {
				mergedMsgs = append(mergedMsgs, msg)
			}

		} else {
			canNotDoMsg = append(canNotDoMsg, msg)
		}
	}
	metrics.QueryNodeWaitForExecuteReqs.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Set(float64(len(mergedMsgs)))
	return mergedMsgs, canNotDoMsg
}
