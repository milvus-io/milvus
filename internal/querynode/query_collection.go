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
	"time"
	"unsafe"

	"github.com/golang/protobuf/proto"
	oplog "github.com/opentracing/opentracing-go/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type queryMsg interface {
	msgstream.TsMsg
	GuaranteeTs() Timestamp
	TravelTs() Timestamp
	TimeoutTs() Timestamp
	SetTimeRecorder()
	ElapseSpan() time.Duration
	RecordSpan() time.Duration
}

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
	// queryResultMsgStream msgstream.MsgStream
	sessionManager *SessionManager

	localChunkManager  storage.ChunkManager
	remoteChunkManager storage.ChunkManager
	vectorChunkManager *storage.VectorChunkManager
	localCacheEnabled  bool

	globalSegmentManager *globalSealedSegmentManager
}

type qcOpt func(*queryCollection)

func qcOptWithSessionManager(s *SessionManager) qcOpt {
	return func(qc *queryCollection) {
		qc.sessionManager = s
	}
}

func newQueryCollection(releaseCtx context.Context,
	cancel context.CancelFunc,
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

	queryStream, _ := factory.NewQueryMsgStream(releaseCtx)
	// queryResultStream, _ := factory.NewQueryMsgStream(releaseCtx)

	condMu := sync.Mutex{}

	qc := &queryCollection{
		releaseCtx: releaseCtx,
		cancel:     cancel,

		collectionID: collectionID,
		historical:   historical,
		streaming:    streaming,

		tSafeWatchers: make(map[Channel]*tSafeWatcher),
		tSafeUpdate:   false,
		watcherCond:   sync.NewCond(&condMu),

		unsolvedMsg: unsolvedMsg,

		queryMsgStream: queryStream,
		// queryResultMsgStream: queryResultStream,

		localChunkManager:    localChunkManager,
		remoteChunkManager:   remoteChunkManager,
		localCacheEnabled:    localCacheEnabled,
		globalSegmentManager: newGlobalSealedSegmentManager(collectionID),
	}

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
	// go q.queryResultMsgStream.Start()
	go q.consumeQuery()
	go q.doUnsolvedQueryMsg()
}

func (q *queryCollection) close() {
	if q.queryMsgStream != nil {
		q.queryMsgStream.Close()
	}
	// if q.queryResultMsgStream != nil {
	// 	q.queryResultMsgStream.Close()
	// }
	q.globalSegmentManager.close()
	if q.vectorChunkManager != nil {
		err := q.vectorChunkManager.Close()
		if err != nil {
			log.Warn("close vector chunk manager error occurs", zap.Error(err))
		}
	}
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
			q.watcherCond.L.Lock()
			q.tSafeUpdate = true
			q.watcherCond.Broadcast()
			q.watcherCond.L.Unlock()
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

func (q *queryCollection) waitNewTSafe() (Timestamp, error) {
	q.watcherCond.L.Lock()
	for !q.tSafeUpdate {
		q.watcherCond.Wait()
		err := q.releaseCtx.Err()
		if err != nil {
			return 0, err
		}
	}
	q.tSafeUpdate = false
	q.watcherCond.Broadcast()
	q.watcherCond.L.Unlock()
	q.tSafeWatchersMu.RLock()
	defer q.tSafeWatchersMu.RUnlock()
	t := Timestamp(math.MaxInt64)
	for channel := range q.tSafeWatchers {
		ts, err := q.streaming.tSafeReplica.getTSafe(channel)
		if err != nil {
			return 0, err
		}
		if ts <= t {
			t = ts
		}
	}
	//p, _ := tsoutil.ParseTS(t)
	//log.Debug("waitNewTSafe",
	//	zap.Any("collectionID", q.collectionID),
	//	zap.Any("tSafe", t),
	//	zap.Any("tSafe_p", p),
	//)
	return t, nil
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
	metrics.QueryNodeServiceTime.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Set(float64(ps))
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
	for {
		select {
		case <-q.releaseCtx.Done():
			log.Debug("stop queryCollection's receiveQueryMsg", zap.Int64("collectionID", q.collectionID))
			return
		case msgPack, ok := <-q.queryMsgStream.Chan():
			if !ok {
				log.Warn("Receive Query Msg from chan failed", zap.Int64("collectionID", q.collectionID))
				return
			}
			if !ok || msgPack == nil || len(msgPack.Msgs) == 0 {
				continue
			}

			for _, msg := range msgPack.Msgs {
				switch sm := msg.(type) {
				case *msgstream.SearchMsg:
					err := q.receiveQueryMsg(sm)
					if err != nil {
						log.Warn(err.Error())
					}
				case *msgstream.RetrieveMsg:
					err := q.receiveQueryMsg(sm)
					if err != nil {
						log.Warn(err.Error())
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
	msgType := msg.Type()
	var collectionID UniqueID
	var msgTypeStr string

	msg.SetTimeRecorder()

	switch msgType {
	case commonpb.MsgType_Retrieve:
		collectionID = msg.(*msgstream.RetrieveMsg).CollectionID
		msgTypeStr = "retrieve"
		//log.Debug("consume retrieve message",
		//	zap.Any("collectionID", collectionID),
		//	zap.Int64("msgID", msg.ID()),
		//)
	case commonpb.MsgType_Search:
		collectionID = msg.(*msgstream.SearchMsg).CollectionID
		msgTypeStr = "search"
		//log.Debug("consume search message",
		//	zap.Any("collectionID", collectionID),
		//	zap.Int64("msgID", msg.ID()),
		//)
	default:
		err := fmt.Errorf("receive invalid msgType = %d", msgType)
		return err
	}
	if collectionID != q.collectionID {
		//log.Warn("not target collection query request",
		//	zap.Any("collectionID", q.collectionID),
		//	zap.Int64("target collectionID", collectionID),
		//	zap.Int64("msgID", msg.ID()),
		//)
		//err := fmt.Errorf("not target collection query request, collectionID = %d, targetCollectionID = %d, msgID = %d", q.collectionID, collectionID, msg.ID())
		return nil
	}

	sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	msg.SetTraceCtx(ctx)
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("receiveQueryMsg %d", msg.ID()))

	if q.checkTimeout(msg) {
		err := fmt.Errorf("do query failed in receiveQueryMsg because timeout, "+
			"collectionID = %d, msgID = %d, timeoutTS = %d", collectionID, msg.ID(), msg.TimeoutTs())
		publishErr := q.publishFailedQueryResult(msg, err.Error())
		if publishErr != nil {
			return fmt.Errorf("first err = %s, second err = %s", err, publishErr)
		}
		return err
	}

	// check if collection has been released
	collection, err := q.historical.replica.getCollectionByID(collectionID)
	if err != nil {
		publishErr := q.publishFailedQueryResult(msg, err.Error())
		if publishErr != nil {
			finalErr := fmt.Errorf("first err = %s, second err = %s", err, publishErr)
			return finalErr
		}
		log.Debug("do query failed in receiveQueryMsg, publish failed query result",
			zap.Int64("collectionID", collectionID),
			zap.Int64("msgID", msg.ID()),
			zap.String("msgType", msgTypeStr),
		)
		return err
	}
	guaranteeTs := msg.GuaranteeTs()
	if guaranteeTs >= collection.getReleaseTime() {
		err = fmt.Errorf("retrieve failed, collection has been released, msgID = %d, collectionID = %d", msg.ID(), collectionID)
		publishErr := q.publishFailedQueryResult(msg, err.Error())
		if publishErr != nil {
			finalErr := fmt.Errorf("first err = %s, second err = %s", err, publishErr)
			return finalErr
		}
		log.Debug("do query failed in receiveQueryMsg, publish failed query result",
			zap.Int64("collectionID", collectionID),
			zap.Int64("msgID", msg.ID()),
			zap.String("msgType", msgTypeStr),
		)
		return err
	}

	serviceTime := q.getServiceableTime()
	gt, _ := tsoutil.ParseTS(guaranteeTs)
	st, _ := tsoutil.ParseTS(serviceTime)
	if guaranteeTs > serviceTime && (len(collection.getVChannels()) > 0 || len(collection.getVDeltaChannels()) > 0) {
		log.Debug("query node::receiveQueryMsg: add to unsolvedMsg",
			zap.Any("collectionID", q.collectionID),
			zap.Any("sm.GuaranteeTimestamp", gt),
			zap.Any("serviceTime", st),
			zap.Any("delta seconds", (guaranteeTs-serviceTime)/(1000*1000*1000)),
			zap.Any("msgID", msg.ID()),
			zap.String("msgType", msgTypeStr),
		)
		msg.RecordSpan()
		q.addToUnsolvedMsg(msg)
		sp.LogFields(
			oplog.String("send to unsolved buffer", "send to unsolved buffer"),
			oplog.Object("guarantee ts", gt),
			oplog.Object("serviceTime", st),
			oplog.Float64("delta seconds", float64(guaranteeTs-serviceTime)/(1000.0*1000.0*1000.0)),
		)
		sp.Finish()
		return nil
	}
	tr.Record("get searchable time done")

	log.Debug("doing query in receiveQueryMsg...",
		zap.Int64("collectionID", collectionID),
		zap.Any("sm.GuaranteeTimestamp", gt),
		zap.Any("serviceTime", st),
		zap.Any("delta seconds", (guaranteeTs-serviceTime)/(1000*1000*1000)),
		zap.Int64("msgID", msg.ID()),
		zap.String("msgType", msgTypeStr),
	)
	switch msgType {
	case commonpb.MsgType_Retrieve:
		err = q.retrieve(msg)
	case commonpb.MsgType_Search:
		err = q.search(msg)
	default:
		err = fmt.Errorf("receive invalid msgType = %d", msgType)
		return err
	}
	tr.Record("operation done")

	if err != nil {
		publishErr := q.publishFailedQueryResult(msg, err.Error())
		if publishErr != nil {
			finalErr := fmt.Errorf("first err = %s, second err = %s", err, publishErr)
			return finalErr
		}
		log.Debug("do query failed in receiveQueryMsg, publish failed query result",
			zap.Int64("collectionID", collectionID),
			zap.Int64("msgID", msg.ID()),
			zap.String("msgType", msgTypeStr),
		)
		return err
	}
	log.Debug("do query done in receiveQueryMsg",
		zap.Int64("collectionID", collectionID),
		zap.Int64("msgID", msg.ID()),
		zap.String("msgType", msgTypeStr),
	)
	tr.Elapse("all done")
	sp.Finish()
	return nil
}

func (q *queryCollection) doUnsolvedQueryMsg() {
	log.Debug("starting doUnsolvedMsg...", zap.Any("collectionID", q.collectionID))
	for {
		select {
		case <-q.releaseCtx.Done():
			log.Debug("stop Collection's doUnsolvedMsg", zap.Int64("collectionID", q.collectionID))
			return
		default:
			//time.Sleep(10 * time.Millisecond)
			serviceTime, err := q.waitNewTSafe()
			if err != nil {
				if err == q.releaseCtx.Err() {
					continue
				}
				log.Error("[should not happen!] stop doUnsolvedMsg, err = " + err.Error())
				return
			}
			//st, _ := tsoutil.ParseTS(serviceTime)
			//log.Debug("get tSafe from flow graph",
			//	zap.Int64("collectionID", q.collectionID),
			//	zap.Any("tSafe", st))

			q.setServiceableTime(serviceTime)
			//log.Debug("query node::doUnsolvedMsg: setServiceableTime", zap.Any("serviceTime", st))

			unSolvedMsg := make([]queryMsg, 0)
			tempMsg := q.popAllUnsolvedMsg()

			for _, m := range tempMsg {
				guaranteeTs := m.GuaranteeTs()
				gt, _ := tsoutil.ParseTS(guaranteeTs)
				st, _ := tsoutil.ParseTS(serviceTime)
				log.Debug("get query message from unsolvedMsg",
					zap.Int64("collectionID", q.collectionID),
					zap.Int64("msgID", m.ID()),
					zap.Any("reqTime_p", gt),
					zap.Any("serviceTime_p", st),
					zap.Any("guaranteeTime_l", guaranteeTs),
					zap.Any("serviceTime_l", serviceTime),
				)

				if q.checkTimeout(m) {
					err := errors.New(fmt.Sprintln("do query failed in doUnsolvedQueryMsg because timeout"+
						", collectionID = ", q.collectionID,
						", msgID = ", m.ID()))
					log.Warn(err.Error())
					publishErr := q.publishFailedQueryResult(m, err.Error())
					if publishErr != nil {
						log.Error(publishErr.Error())
					}
					continue
				}

				if guaranteeTs <= q.getServiceableTime() {
					unSolvedMsg = append(unSolvedMsg, m)
					continue
				}
				log.Debug("query node::doUnsolvedMsg: add to unsolvedMsg",
					zap.Any("collectionID", q.collectionID),
					zap.Any("sm.BeginTs", gt),
					zap.Any("serviceTime", st),
					zap.Any("delta seconds", (guaranteeTs-serviceTime)/(1000*1000*1000)),
					zap.Any("msgID", m.ID()),
				)
				q.addToUnsolvedMsg(m)
			}

			if len(unSolvedMsg) <= 0 {
				continue
			}
			for _, m := range unSolvedMsg {
				msgType := m.Type()
				var err error
				sp, ctx := trace.StartSpanFromContext(m.TraceCtx())
				m.SetTraceCtx(ctx)
				log.Debug("doing search in doUnsolvedMsg...",
					zap.Int64("collectionID", q.collectionID),
					zap.Int64("msgID", m.ID()),
				)
				switch msgType {
				case commonpb.MsgType_Retrieve:
					metrics.QueryNodeSQLatencyInQueue.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID),
						metrics.QueryLabel).Observe(float64(m.RecordSpan().Milliseconds()))
					err = q.retrieve(m)
				case commonpb.MsgType_Search:
					metrics.QueryNodeSQLatencyInQueue.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID),
						metrics.SearchLabel).Observe(float64(m.RecordSpan().Milliseconds()))
					err = q.search(m)
				default:
					err := fmt.Errorf("receive invalid msgType = %d", msgType)
					log.Warn(err.Error())
					return
				}

				if err != nil {
					log.Warn(err.Error())
					err = q.publishFailedQueryResult(m, err.Error())
					if err != nil {
						log.Warn(err.Error())
					} else {
						log.Debug("do query failed in doUnsolvedMsg, publish failed query result",
							zap.Int64("collectionID", q.collectionID),
							zap.Int64("msgID", m.ID()),
						)
					}
				}
				sp.Finish()
				log.Debug("do query done in doUnsolvedMsg",
					zap.Int64("collectionID", q.collectionID),
					zap.Int64("msgID", m.ID()),
				)
			}
			log.Debug("doUnsolvedMsg: do query done", zap.Int("num of query msg", len(unSolvedMsg)))
		}
	}
}

func translateHits(schema *typeutil.SchemaHelper, fieldIDs []int64, rawHits [][]byte) (*schemapb.SearchResultData, error) {
	tr := timerecord.NewTimeRecorder("translateHitsDuration")
	log.Debug("translateHits:", zap.Any("lenOfFieldIDs", len(fieldIDs)), zap.Any("lenOfRawHits", len(rawHits)))
	if len(rawHits) == 0 {
		return nil, fmt.Errorf("empty results")
	}

	var hits []*milvuspb.Hits
	for _, rawHit := range rawHits {
		var hit milvuspb.Hits
		err := proto.Unmarshal(rawHit, &hit)
		if err != nil {
			return nil, err
		}
		hits = append(hits, &hit)
	}

	blobOffset := 0
	// skip id
	numQueries := len(rawHits)
	pbHits := &milvuspb.Hits{}
	err := proto.Unmarshal(rawHits[0], pbHits)
	if err != nil {
		return nil, err
	}
	topK := len(pbHits.IDs)

	blobOffset += 8
	var ids []int64
	var scores []float32
	for _, hit := range hits {
		ids = append(ids, hit.IDs...)
		scores = append(scores, hit.Scores...)
	}

	finalResult := &schemapb.SearchResultData{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: ids,
				},
			},
		},
		Scores:     scores,
		TopK:       int64(topK),
		NumQueries: int64(numQueries),
	}

	for _, fieldID := range fieldIDs {
		fieldMeta, err := schema.GetFieldFromID(fieldID)
		if err != nil {
			return nil, err
		}
		switch fieldMeta.DataType {
		case schemapb.DataType_Bool:
			blobLen := 1
			var colData []bool
			for _, hit := range hits {
				for _, row := range hit.RowData {
					dataBlob := row[blobOffset : blobOffset+blobLen]
					data := dataBlob[0]
					colData = append(colData, data != 0)
				}
			}
			newCol := &schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{
								Data: colData,
							},
						},
					},
				},
			}
			finalResult.FieldsData = append(finalResult.FieldsData, newCol)
			blobOffset += blobLen
		case schemapb.DataType_Int8:
			blobLen := 1
			var colData []int32
			for _, hit := range hits {
				for _, row := range hit.RowData {
					dataBlob := row[blobOffset : blobOffset+blobLen]
					data := int32(dataBlob[0])
					colData = append(colData, data)
				}
			}
			newCol := &schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: colData,
							},
						},
					},
				},
			}
			finalResult.FieldsData = append(finalResult.FieldsData, newCol)
			blobOffset += blobLen
		case schemapb.DataType_Int16:
			blobLen := 2
			var colData []int32
			for _, hit := range hits {
				for _, row := range hit.RowData {
					dataBlob := row[blobOffset : blobOffset+blobLen]
					data := int32(int16(common.Endian.Uint16(dataBlob)))
					colData = append(colData, data)
				}
			}
			newCol := &schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: colData,
							},
						},
					},
				},
			}
			finalResult.FieldsData = append(finalResult.FieldsData, newCol)
			blobOffset += blobLen
		case schemapb.DataType_Int32:
			blobLen := 4
			var colData []int32
			for _, hit := range hits {
				for _, row := range hit.RowData {
					dataBlob := row[blobOffset : blobOffset+blobLen]
					data := int32(common.Endian.Uint32(dataBlob))
					colData = append(colData, data)
				}
			}
			newCol := &schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: colData,
							},
						},
					},
				},
			}
			finalResult.FieldsData = append(finalResult.FieldsData, newCol)
			blobOffset += blobLen
		case schemapb.DataType_Int64:
			blobLen := 8
			var colData []int64
			for _, hit := range hits {
				for _, row := range hit.RowData {
					dataBlob := row[blobOffset : blobOffset+blobLen]
					data := int64(common.Endian.Uint64(dataBlob))
					colData = append(colData, data)
				}
			}
			newCol := &schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: colData,
							},
						},
					},
				},
			}
			finalResult.FieldsData = append(finalResult.FieldsData, newCol)
			blobOffset += blobLen
		case schemapb.DataType_Float:
			blobLen := 4
			var colData []float32
			for _, hit := range hits {
				for _, row := range hit.RowData {
					dataBlob := row[blobOffset : blobOffset+blobLen]
					data := math.Float32frombits(common.Endian.Uint32(dataBlob))
					colData = append(colData, data)
				}
			}
			newCol := &schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{
								Data: colData,
							},
						},
					},
				},
			}
			finalResult.FieldsData = append(finalResult.FieldsData, newCol)
			blobOffset += blobLen
		case schemapb.DataType_Double:
			blobLen := 8
			var colData []float64
			for _, hit := range hits {
				for _, row := range hit.RowData {
					dataBlob := row[blobOffset : blobOffset+blobLen]
					data := math.Float64frombits(common.Endian.Uint64(dataBlob))
					colData = append(colData, data)
				}
			}
			newCol := &schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{
								Data: colData,
							},
						},
					},
				},
			}
			finalResult.FieldsData = append(finalResult.FieldsData, newCol)
			blobOffset += blobLen
		case schemapb.DataType_FloatVector:
			dim, err := schema.GetVectorDimFromID(fieldID)
			if err != nil {
				return nil, err
			}
			blobLen := dim * 4
			var colData []float32
			for _, hit := range hits {
				for _, row := range hit.RowData {
					dataBlob := row[blobOffset : blobOffset+blobLen]
					//ref https://github.com/golang/go/wiki/cgo#turning-c-arrays-into-go-slices
					/* #nosec G103 */
					ptr := unsafe.Pointer(&dataBlob[0])
					farray := (*[1 << 28]float32)(ptr)
					colData = append(colData, farray[:dim:dim]...)
				}
			}
			newCol := &schemapb.FieldData{
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: int64(dim),
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{
								Data: colData,
							},
						},
					},
				},
			}
			finalResult.FieldsData = append(finalResult.FieldsData, newCol)
			blobOffset += blobLen
		case schemapb.DataType_BinaryVector:
			dim, err := schema.GetVectorDimFromID(fieldID)
			if err != nil {
				return nil, err
			}
			blobLen := dim / 8
			var colData []byte
			for _, hit := range hits {
				for _, row := range hit.RowData {
					dataBlob := row[blobOffset : blobOffset+blobLen]
					colData = append(colData, dataBlob...)
				}
			}
			newCol := &schemapb.FieldData{
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: int64(dim),
						Data: &schemapb.VectorField_BinaryVector{
							BinaryVector: colData,
						},
					},
				},
			}
			finalResult.FieldsData = append(finalResult.FieldsData, newCol)
			blobOffset += blobLen
		default:
			return nil, fmt.Errorf("unsupported data type %s", schemapb.DataType_name[int32(fieldMeta.DataType)])
		}
	}

	metrics.QueryNodeTranslateHitsLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return finalResult, nil
}

// TODO:: cache map[dsl]plan
// TODO: reBatched search requests
func (q *queryCollection) search(msg queryMsg) error {
	q.streaming.replica.queryRLock()
	q.historical.replica.queryRLock()
	defer q.historical.replica.queryRUnlock()
	defer q.streaming.replica.queryRUnlock()

	searchMsg := msg.(*msgstream.SearchMsg)
	collectionID := searchMsg.CollectionID
	sp, ctx := trace.StartSpanFromContext(searchMsg.TraceCtx())
	defer sp.Finish()
	searchMsg.SetTraceCtx(ctx)
	searchTimestamp := searchMsg.BeginTs()
	travelTimestamp := searchMsg.TravelTimestamp

	collection, err := q.streaming.replica.getCollectionByID(searchMsg.CollectionID)
	if err != nil {
		return err
	}

	schema, err := typeutil.CreateSchemaHelper(collection.schema)
	if err != nil {
		return err
	}

	var plan *SearchPlan
	if searchMsg.GetDslType() == commonpb.DslType_BoolExprV1 {
		expr := searchMsg.SerializedExprPlan
		plan, err = createSearchPlanByExpr(collection, expr)
		if err != nil {
			return err
		}
	} else {
		dsl := searchMsg.Dsl
		plan, err = createSearchPlan(collection, dsl)
		if err != nil {
			return err
		}
	}

	defer plan.delete()

	topK := plan.getTopK()
	if topK == 0 {
		return fmt.Errorf("limit must be greater than 0, msgID = %d", searchMsg.ID())
	}
	if topK >= 16385 {
		return fmt.Errorf("limit %d is too large, msgID = %d", topK, searchMsg.ID())
	}
	searchRequestBlob := searchMsg.PlaceholderGroup
	searchReq, err := parseSearchRequest(plan, searchRequestBlob)
	if err != nil {
		return err
	}
	defer searchReq.delete()

	queryNum := searchReq.getNumOfQuery()
	searchRequests := make([]*searchRequest, 0)
	searchRequests = append(searchRequests, searchReq)

	if searchMsg.GetDslType() == commonpb.DslType_BoolExprV1 {
		sp.LogFields(oplog.String("statistical time", "stats start"),
			oplog.Object("nq", queryNum),
			oplog.Object("expr", searchMsg.SerializedExprPlan))
	} else {
		sp.LogFields(oplog.String("statistical time", "stats start"),
			oplog.Object("nq", queryNum),
			oplog.Object("dsl", searchMsg.Dsl))
	}

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("search %d(nq=%d, k=%d), msgID = %d", searchMsg.CollectionID, queryNum, topK, searchMsg.ID()))

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
	log.Debug("historical search start", zap.Int64("msgID", searchMsg.ID()))
	hisSearchResults, sealedSegmentSearched, sealedPartitionSearched, err := q.historical.search(searchRequests, collection.id, searchMsg.PartitionIDs, plan, travelTimestamp)
	if err != nil {
		return err
	}
	searchResults = append(searchResults, hisSearchResults...)
	log.Debug("historical search", zap.Int64("msgID", searchMsg.ID()), zap.Int64("collectionID", collectionID), zap.Int64s("searched partitionIDs", sealedPartitionSearched), zap.Int64s("searched segmentIDs", sealedSegmentSearched))
	tr.Record(fmt.Sprintf("historical search done, msgID = %d", searchMsg.ID()))

	log.Debug("streaming search start", zap.Int64("msgID", searchMsg.ID()))
	for _, channel := range collection.getVChannels() {
		strSearchResults, growingSegmentSearched, growingPartitionSearched, err := q.streaming.search(searchRequests, collection.id, searchMsg.PartitionIDs, channel, plan, travelTimestamp)
		if err != nil {
			return err
		}
		searchResults = append(searchResults, strSearchResults...)
		log.Debug("streaming search", zap.Int64("msgID", searchMsg.ID()), zap.Int64("collectionID", collectionID), zap.String("searched dmChannel", channel), zap.Int64s("searched partitionIDs", growingPartitionSearched), zap.Int64s("searched segmentIDs", growingSegmentSearched))
	}
	tr.Record(fmt.Sprintf("streaming search done, msgID = %d", searchMsg.ID()))

	sp.LogFields(oplog.String("statistical time", "segment search end"))
	if len(searchResults) <= 0 {
		for range searchRequests {
			resultChannelInt := 0
			searchResultMsg := &msgstream.SearchResultMsg{
				BaseMsg: msgstream.BaseMsg{Ctx: searchMsg.Ctx, HashValues: []uint32{uint32(resultChannelInt)}},
				SearchResults: internalpb.SearchResults{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_SearchResult,
						MsgID:     searchMsg.Base.MsgID,
						Timestamp: searchTimestamp,
						SourceID:  searchMsg.Base.SourceID,
					},
					Status:                   &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
					ResultChannelID:          searchMsg.ResultChannelID,
					MetricType:               plan.getMetricType(),
					NumQueries:               queryNum,
					TopK:                     topK,
					SlicedBlob:               nil,
					SlicedOffset:             1,
					SlicedNumCount:           1,
					SealedSegmentIDsSearched: sealedSegmentSearched,
					ChannelIDsSearched:       collection.getVChannels(),
					GlobalSealedSegmentIDs:   globalSealedSegments,
				},
			}
			log.Debug("QueryNode Empty SearchResultMsg",
				zap.Any("collectionID", collection.id),
				zap.Any("msgID", searchMsg.ID()),
				zap.Any("vChannels", collection.getVChannels()),
				zap.Any("sealedSegmentSearched", sealedSegmentSearched),
			)
			err = q.publishSearchResult(&searchResultMsg.SearchResults, searchMsg.Base.SourceID)
			if err != nil {
				return err
			}
			metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID), metrics.SearchLabel).Observe(float64(msg.ElapseSpan().Milliseconds()))
			metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID), metrics.SearchLabel, metrics.SuccessLabel).Inc()

			tr.Record(fmt.Sprintf("publish empty search result done, msgID = %d", searchMsg.ID()))
			tr.Elapse(fmt.Sprintf("all done, msgID = %d", searchMsg.ID()))
			return nil
		}
	}

	numSegment := int64(len(searchResults))
	var marshaledHits *MarshaledHits
	log.Debug("QueryNode reduce data", zap.Int64("msgID", searchMsg.ID()), zap.Int64("numSegment", numSegment))
	tr.RecordSpan()
	err = reduceSearchResultsAndFillData(plan, searchResults, numSegment)
	log.Debug("QueryNode reduce data finished", zap.Int64("msgID", searchMsg.ID()))
	sp.LogFields(oplog.String("statistical time", "reduceSearchResults end"))
	if err != nil {
		log.Error("QueryNode reduce data failed", zap.Int64("msgID", searchMsg.ID()), zap.Error(err))
		return err
	}
	marshaledHits, err = reorganizeSearchResults(searchResults, numSegment)
	sp.LogFields(oplog.String("statistical time", "reorganizeSearchResults end"))
	if err != nil {
		return err
	}
	defer deleteMarshaledHits(marshaledHits)

	hitsBlob, err := marshaledHits.getHitsBlob()
	sp.LogFields(oplog.String("statistical time", "getHitsBlob end"))
	if err != nil {
		return err
	}
	metrics.QueryNodeReduceLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID), metrics.SearchLabel).Observe(float64(tr.RecordSpan().Milliseconds()))

	var offset int64
	for index := range searchRequests {
		hitBlobSizePeerQuery, err := marshaledHits.hitBlobSizeInGroup(int64(index))
		if err != nil {
			return err
		}
		hits := make([][]byte, len(hitBlobSizePeerQuery))
		for i, len := range hitBlobSizePeerQuery {
			hits[i] = hitsBlob[offset : offset+len]
			//test code to checkout marshaled hits
			//marshaledHit := hitsBlob[offset:offset+len]
			//unMarshaledHit := milvuspb.Hits{}
			//err = proto.Unmarshal(marshaledHit, &unMarshaledHit)
			//if err != nil {
			//	return err
			//}
			//log.Debug("hits msg  = ", unMarshaledHit)
			offset += len
		}

		// TODO: remove inefficient code in cgo and use SearchResultData directly
		// TODO: Currently add a translate layer from hits to SearchResultData
		// TODO: hits marshal and unmarshal is likely bottleneck

		transformed, err := translateHits(schema, searchMsg.OutputFieldsId, hits)
		if err != nil {
			return err
		}
		byteBlobs, err := proto.Marshal(transformed)
		if err != nil {
			return err
		}

		resultChannelInt := 0
		searchResultMsg := &msgstream.SearchResultMsg{
			BaseMsg: msgstream.BaseMsg{Ctx: searchMsg.Ctx, HashValues: []uint32{uint32(resultChannelInt)}},
			SearchResults: internalpb.SearchResults{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_SearchResult,
					MsgID:     searchMsg.Base.MsgID,
					Timestamp: searchTimestamp,
					SourceID:  searchMsg.Base.SourceID,
				},
				Status:                   &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
				ResultChannelID:          searchMsg.ResultChannelID,
				MetricType:               plan.getMetricType(),
				NumQueries:               queryNum,
				TopK:                     topK,
				SlicedBlob:               byteBlobs,
				SlicedOffset:             1,
				SlicedNumCount:           1,
				SealedSegmentIDsSearched: sealedSegmentSearched,
				ChannelIDsSearched:       collection.getVChannels(),
				GlobalSealedSegmentIDs:   globalSealedSegments,
			},
		}
		log.Debug("QueryNode SearchResultMsg",
			zap.Any("collectionID", collection.id),
			zap.Any("msgID", searchMsg.ID()),
			zap.Any("vChannels", collection.getVChannels()),
			zap.Any("sealedSegmentSearched", sealedSegmentSearched),
		)

		// For debugging, please don't delete.
		//fmt.Println("==================== search result ======================")
		//for i := 0; i < len(hits); i++ {
		//	testHits := milvuspb.Hits{}
		//	err := proto.Unmarshal(hits[i], &testHits)
		//	if err != nil {
		//		panic(err)
		//	}
		//	fmt.Println(testHits.IDs)
		//	fmt.Println(testHits.Scores)
		//}
		err = q.publishSearchResult(&searchResultMsg.SearchResults, searchMsg.Base.SourceID)
		if err != nil {
			return err
		}
		metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID),
			metrics.SearchLabel).Observe(float64(msg.ElapseSpan().Milliseconds()))
		metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID),
			metrics.SearchLabel,
			metrics.SuccessLabel).Inc()
		tr.Record(fmt.Sprintf("publish search result, msgID = %d", searchMsg.ID()))
	}
	sp.LogFields(oplog.String("statistical time", "stats done"))
	tr.Elapse(fmt.Sprintf("all done, msgID = %d", searchMsg.ID()))
	return nil
}

func (q *queryCollection) retrieve(msg queryMsg) error {
	// TODO(yukun)
	// step 1: get retrieve object and defer destruction
	// step 2: for each segment, call retrieve to get ids proto buffer
	// step 3: merge all proto in go
	// step 4: publish results
	// retrieveProtoBlob, err := proto.Marshal(&retrieveMsg.RetrieveRequest)
	retrieveMsg := msg.(*msgstream.RetrieveMsg)
	sp, ctx := trace.StartSpanFromContext(retrieveMsg.TraceCtx())
	defer sp.Finish()
	retrieveMsg.SetTraceCtx(ctx)
	timestamp := retrieveMsg.RetrieveRequest.TravelTimestamp

	collectionID := retrieveMsg.CollectionID
	collection, err := q.streaming.replica.getCollectionByID(collectionID)
	if err != nil {
		return err
	}

	expr := retrieveMsg.SerializedExprPlan
	plan, err := createRetrievePlanByExpr(collection, expr, timestamp)
	if err != nil {
		return err
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
			return fmt.Errorf("can not create vector chunk manager for local chunk manager is nil, msgID = %d", retrieveMsg.ID())
		}
		if q.remoteChunkManager == nil {
			return fmt.Errorf("can not create vector chunk manager for remote chunk manager is nil, msgID = %d", retrieveMsg.ID())
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
		return err
	}
	mergeList = append(mergeList, hisRetrieveResults...)
	log.Debug("historical retrieve", zap.Int64("msgID", retrieveMsg.ID()), zap.Int64("collectionID", collectionID), zap.Int64s("retrieve partitionIDs", sealedPartitionRetrieved), zap.Int64s("retrieve segmentIDs", sealedSegmentRetrieved))
	tr.Record(fmt.Sprintf("historical retrieve done, msgID = %d", retrieveMsg.ID()))

	// streaming retrieve
	log.Debug("streaming retrieve start", zap.Int64("msgID", retrieveMsg.ID()))
	strRetrieveResults, streamingSegmentRetrived, streamingPartitionRetrived, err := q.streaming.retrieve(collectionID, retrieveMsg.PartitionIDs, plan)
	if err != nil {
		return err
	}
	mergeList = append(mergeList, strRetrieveResults...)
	log.Debug("streaming retrieve", zap.Int64("msgID", retrieveMsg.ID()), zap.Int64("collectionID", collectionID), zap.Int64s("retrieve partitionIDs", streamingPartitionRetrived), zap.Int64s("retrieve segmentIDs", streamingSegmentRetrived))
	tr.Record(fmt.Sprintf("streaming retrieve done, msgID = %d", retrieveMsg.ID()))

	result, err := mergeRetrieveResults(mergeList)
	if err != nil {
		return err
	}
	reduceDuration := tr.Record(fmt.Sprintf("merge result done, msgID = %d", retrieveMsg.ID()))
	metrics.QueryNodeReduceLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID), metrics.QueryLabel).Observe(float64(reduceDuration.Milliseconds()))

	resultChannelInt := 0
	retrieveResultMsg := &msgstream.RetrieveResultMsg{
		BaseMsg: msgstream.BaseMsg{Ctx: retrieveMsg.Ctx, HashValues: []uint32{uint32(resultChannelInt)}},
		RetrieveResults: internalpb.RetrieveResults{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_RetrieveResult,
				MsgID:    retrieveMsg.Base.MsgID,
				SourceID: retrieveMsg.Base.SourceID,
			},
			Status:                    &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			Ids:                       result.Ids,
			FieldsData:                result.FieldsData,
			ResultChannelID:           retrieveMsg.ResultChannelID,
			SealedSegmentIDsRetrieved: sealedSegmentRetrieved,
			ChannelIDsRetrieved:       collection.getVChannels(),
			GlobalSealedSegmentIDs:    globalSealedSegments,
		},
	}

	err = q.publishRetrieveResult(&retrieveResultMsg.RetrieveResults, retrieveMsg.Base.SourceID)
	if err != nil {
		return err
	}
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID), metrics.QueryLabel, metrics.SuccessLabel).Inc()
	metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID), metrics.QueryLabel).Observe(float64(msg.ElapseSpan().Milliseconds()))

	log.Debug("QueryNode publish RetrieveResultMsg",
		zap.Int64("msgID", retrieveMsg.ID()),
		zap.Any("vChannels", collection.getVChannels()),
		zap.Any("collectionID", collection.ID()),
		zap.Any("sealedSegmentRetrieved", sealedSegmentRetrieved),
	)
	tr.Elapse(fmt.Sprintf("all done, msgID = %d", retrieveMsg.ID()))
	return nil
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

func (q *queryCollection) publishSearchResultWithCtx(ctx context.Context, result *internalpb.SearchResults, nodeID UniqueID) error {
	return q.sessionManager.SendSearchResult(ctx, nodeID, result)
}

func (q *queryCollection) publishSearchResult(result *internalpb.SearchResults, nodeID UniqueID) error {
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID), metrics.SearchLabel, metrics.TotalLabel).Inc()
	return q.publishSearchResultWithCtx(q.releaseCtx, result, nodeID)
}

func (q *queryCollection) publishRetrieveResultWithCtx(ctx context.Context, result *internalpb.RetrieveResults, nodeID UniqueID) error {
	return q.sessionManager.SendRetrieveResult(ctx, nodeID, result)
}

func (q *queryCollection) publishRetrieveResult(result *internalpb.RetrieveResults, nodeID UniqueID) error {
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID), metrics.QueryLabel, metrics.TotalLabel).Inc()
	return q.publishRetrieveResultWithCtx(q.releaseCtx, result, nodeID)
}

func (q *queryCollection) publishFailedQueryResultWithCtx(ctx context.Context, msg msgstream.TsMsg, errMsg string) error {
	msgType := msg.Type()
	span, traceCtx := trace.StartSpanFromContext(msg.TraceCtx())
	defer span.Finish()
	msg.SetTraceCtx(traceCtx)

	baseResult := &commonpb.MsgBase{
		MsgID:     msg.ID(),
		Timestamp: msg.BeginTs(),
		SourceID:  msg.SourceID(),
	}

	switch msgType {
	case commonpb.MsgType_Retrieve:
		retrieveMsg := msg.(*msgstream.RetrieveMsg)
		baseResult.MsgType = commonpb.MsgType_RetrieveResult
		metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID), metrics.QueryLabel, metrics.FailLabel).Inc()
		return q.publishRetrieveResult(&internalpb.RetrieveResults{
			Base:            baseResult,
			Status:          &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: errMsg},
			ResultChannelID: retrieveMsg.ResultChannelID,
			Ids:             nil,
			FieldsData:      nil,
		}, msg.SourceID())
	case commonpb.MsgType_Search:
		searchMsg := msg.(*msgstream.SearchMsg)
		baseResult.MsgType = commonpb.MsgType_SearchResult
		metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID), metrics.SearchLabel, metrics.FailLabel).Inc()
		return q.publishSearchResultWithCtx(ctx, &internalpb.SearchResults{
			Base:            baseResult,
			Status:          &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: errMsg},
			ResultChannelID: searchMsg.ResultChannelID,
		}, msg.SourceID())
	default:
		return fmt.Errorf("publish invalid msgType %d", msgType)
	}
}

func (q *queryCollection) publishFailedQueryResult(msg msgstream.TsMsg, errMsg string) error {
	return q.publishFailedQueryResultWithCtx(q.releaseCtx, msg, errMsg)
}

// func (q *queryCollection) publishQueryResult(msg msgstream.TsMsg, collectionID UniqueID) error {
// 	span, ctx := trace.StartSpanFromContext(msg.TraceCtx())
// 	defer span.Finish()
// 	msg.SetTraceCtx(ctx)
// 	msgPack := msgstream.MsgPack{}
// 	msgPack.Msgs = append(msgPack.Msgs, msg)
// 	err := q.queryResultMsgStream.Produce(&msgPack)
// 	if err != nil {
// 		log.Error(err.Error())
// 	}
//
// 	return err
// }

// func (q *queryCollection) publishFailedQueryResult(msg msgstream.TsMsg, errMsg string) error {
// 	msgType := msg.Type()
// 	span, ctx := trace.StartSpanFromContext(msg.TraceCtx())
// 	defer span.Finish()
// 	msg.SetTraceCtx(ctx)
// 	msgPack := msgstream.MsgPack{}
//
// 	resultChannelInt := 0
// 	baseMsg := msgstream.BaseMsg{
// 		HashValues: []uint32{uint32(resultChannelInt)},
// 	}
// 	baseResult := &commonpb.MsgBase{
// 		MsgID:     msg.ID(),
// 		Timestamp: msg.BeginTs(),
// 		SourceID:  msg.SourceID(),
// 	}
//
// 	switch msgType {
// 	case commonpb.MsgType_Retrieve:
// 		retrieveMsg := msg.(*msgstream.RetrieveMsg)
// 		baseResult.MsgType = commonpb.MsgType_RetrieveResult
// 		retrieveResultMsg := &msgstream.RetrieveResultMsg{
// 			BaseMsg: baseMsg,
// 			RetrieveResults: internalpb.RetrieveResults{
// 				Base:            baseResult,
// 				Status:          &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: errMsg},
// 				ResultChannelID: retrieveMsg.ResultChannelID,
// 				Ids:             nil,
// 				FieldsData:      nil,
// 			},
// 		}
// 		msgPack.Msgs = append(msgPack.Msgs, retrieveResultMsg)
// 	case commonpb.MsgType_Search:
// 		searchMsg := msg.(*msgstream.SearchMsg)
// 		baseResult.MsgType = commonpb.MsgType_SearchResult
// 		searchResultMsg := &msgstream.SearchResultMsg{
// 			BaseMsg: baseMsg,
// 			SearchResults: internalpb.SearchResults{
// 				Base:            baseResult,
// 				Status:          &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: errMsg},
// 				ResultChannelID: searchMsg.ResultChannelID,
// 			},
// 		}
// 		msgPack.Msgs = append(msgPack.Msgs, searchResultMsg)
// 	default:
// 		return fmt.Errorf("publish invalid msgType %d", msgType)
// 	}
//
// 	return q.queryResultMsgStream.Produce(&msgPack)
// }
//
