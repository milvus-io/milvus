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

package querynode

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"

	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	oplog "github.com/opentracing/opentracing-go/log"
	"go.uber.org/zap"
)

type queryCollection struct {
	releaseCtx context.Context
	cancel     context.CancelFunc

	collectionID UniqueID
	historical   *historical
	streaming    *streaming

	unsolvedMsgMu       sync.Mutex // guards unsolvedMsg
	unsolvedMsg         []*msgstream.SearchMsg
	unsolvedRetrieveMsg []*msgstream.RetrieveMsg

	tSafeWatchers     map[Channel]*tSafeWatcher
	watcherSelectCase []reflect.SelectCase

	serviceableTimeMutex sync.Mutex // guards serviceableTime
	serviceableTime      Timestamp

	queryMsgStream       msgstream.MsgStream
	queryResultMsgStream msgstream.MsgStream
}

type ResultEntityIds []UniqueID

func newQueryCollection(releaseCtx context.Context,
	cancel context.CancelFunc,
	collectionID UniqueID,
	historical *historical,
	streaming *streaming,
	factory msgstream.Factory) *queryCollection {

	unsolvedMsg := make([]*msgstream.SearchMsg, 0)

	queryStream, _ := factory.NewQueryMsgStream(releaseCtx)
	queryResultStream, _ := factory.NewQueryMsgStream(releaseCtx)

	qc := &queryCollection{
		releaseCtx: releaseCtx,
		cancel:     cancel,

		collectionID: collectionID,
		historical:   historical,
		streaming:    streaming,

		tSafeWatchers: make(map[Channel]*tSafeWatcher),

		unsolvedMsg: unsolvedMsg,

		queryMsgStream:       queryStream,
		queryResultMsgStream: queryResultStream,
	}

	qc.register()
	return qc
}

func (q *queryCollection) start() {
	go q.queryMsgStream.Start()
	go q.queryResultMsgStream.Start()
	go q.consumeQuery()
	go q.doUnsolvedMsgSearch()
	go q.doUnsolvedMsgRetrieve()
}

func (q *queryCollection) close() {
	if q.queryMsgStream != nil {
		q.queryMsgStream.Close()
	}
	if q.queryResultMsgStream != nil {
		q.queryResultMsgStream.Close()
	}
}

func (q *queryCollection) register() {
	collection, err := q.streaming.replica.getCollectionByID(q.collectionID)
	if err != nil {
		log.Error(err.Error())
		return
	}

	q.watcherSelectCase = make([]reflect.SelectCase, 0)
	log.Debug("register tSafe watcher and init watcher select case",
		zap.Any("collectionID", collection.ID()),
		zap.Any("dml channels", collection.getVChannels()),
	)
	for _, channel := range collection.getVChannels() {
		q.tSafeWatchers[channel] = newTSafeWatcher()
		q.streaming.tSafeReplica.registerTSafeWatcher(channel, q.tSafeWatchers[channel])
		q.watcherSelectCase = append(q.watcherSelectCase, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(q.tSafeWatchers[channel].watcherChan()),
		})
	}
}

func (q *queryCollection) addToUnsolvedMsg(msg *msgstream.SearchMsg) {
	q.unsolvedMsgMu.Lock()
	defer q.unsolvedMsgMu.Unlock()
	q.unsolvedMsg = append(q.unsolvedMsg, msg)
}

func (q *queryCollection) addToUnsolvedRetrieveMsg(msg *msgstream.RetrieveMsg) {
	q.unsolvedMsgMu.Lock()
	defer q.unsolvedMsgMu.Unlock()
	q.unsolvedRetrieveMsg = append(q.unsolvedRetrieveMsg, msg)
}

func (q *queryCollection) popAllUnsolvedMsg() []*msgstream.SearchMsg {
	q.unsolvedMsgMu.Lock()
	defer q.unsolvedMsgMu.Unlock()
	tmp := q.unsolvedMsg
	q.unsolvedMsg = q.unsolvedMsg[:0]
	return tmp
}

func (q *queryCollection) popAllUnsolvedRetrieveMsg() []*msgstream.RetrieveMsg {
	q.unsolvedMsgMu.Lock()
	defer q.unsolvedMsgMu.Unlock()
	tmp := q.unsolvedRetrieveMsg
	q.unsolvedRetrieveMsg = q.unsolvedRetrieveMsg[:0]
	return tmp
}

func (q *queryCollection) waitNewTSafe() Timestamp {
	// block until any vChannel updating tSafe
	_, _, recvOK := reflect.Select(q.watcherSelectCase)
	if !recvOK {
		log.Error("tSafe has been closed", zap.Any("collectionID", q.collectionID))
		return Timestamp(math.MaxInt64)
	}
	//log.Debug("wait new tSafe", zap.Any("collectionID", s.collectionID))
	t := Timestamp(math.MaxInt64)
	for channel := range q.tSafeWatchers {
		ts := q.streaming.tSafeReplica.getTSafe(channel)
		if ts <= t {
			t = ts
		}
	}
	return t
}

func (q *queryCollection) getServiceableTime() Timestamp {
	q.serviceableTimeMutex.Lock()
	defer q.serviceableTimeMutex.Unlock()
	return q.serviceableTime
}

func (q *queryCollection) setServiceableTime(t Timestamp) {
	q.serviceableTimeMutex.Lock()
	defer q.serviceableTimeMutex.Unlock()

	if t < q.serviceableTime {
		return
	}

	gracefulTimeInMilliSecond := Params.GracefulTime
	if gracefulTimeInMilliSecond > 0 {
		gracefulTime := tsoutil.ComposeTS(gracefulTimeInMilliSecond, 0)
		q.serviceableTime = t + gracefulTime
	} else {
		q.serviceableTime = t
	}
}

func (q *queryCollection) emptySearch(searchMsg *msgstream.SearchMsg) {
	sp, ctx := trace.StartSpanFromContext(searchMsg.TraceCtx())
	defer sp.Finish()
	searchMsg.SetTraceCtx(ctx)
	err := q.search(searchMsg)
	if err != nil {
		log.Error(err.Error())
		q.publishFailedSearchResult(searchMsg, err.Error())
	}
}

func (q *queryCollection) consumeQuery() {
	for {
		select {
		case <-q.releaseCtx.Done():
			log.Debug("stop queryCollection's receiveQueryMsg", zap.Int64("collectionID", q.collectionID))
			return
		default:
			msgPack := q.queryMsgStream.Consume()
			if msgPack == nil || len(msgPack.Msgs) <= 0 {
				msgPackNil := msgPack == nil
				msgPackEmpty := true
				if msgPack != nil {
					msgPackEmpty = len(msgPack.Msgs) <= 0
				}
				log.Debug("consume query message failed", zap.Any("msgPack is Nil", msgPackNil),
					zap.Any("msgPackEmpty", msgPackEmpty))
				continue
			}
			for _, msg := range msgPack.Msgs {
				switch sm := msg.(type) {
				case *msgstream.SearchMsg:
					q.receiveSearch(sm)
				case *msgstream.LoadBalanceSegmentsMsg:
					q.loadBalance(sm)
				case *msgstream.RetrieveMsg:
					q.receiveRetrieve(sm)
				default:
					log.Warn("unsupported msg type in search channel", zap.Any("msg", sm))
				}
			}
		}
	}
}

func (q *queryCollection) loadBalance(msg *msgstream.LoadBalanceSegmentsMsg) {
	//TODO:: get loadBalance info from etcd
	//log.Debug("consume load balance message",
	//	zap.Int64("msgID", msg.ID()))
	//nodeID := Params.QueryNodeID
	//for _, info := range msg.Infos {
	//	segmentID := info.SegmentID
	//	if nodeID == info.SourceNodeID {
	//		err := s.historical.replica.removeSegment(segmentID)
	//		if err != nil {
	//			log.Error("loadBalance failed when remove segment",
	//				zap.Error(err),
	//				zap.Any("segmentID", segmentID))
	//		}
	//	}
	//	if nodeID == info.DstNodeID {
	//		segment, err := s.historical.replica.getSegmentByID(segmentID)
	//		if err != nil {
	//			log.Error("loadBalance failed when making segment on service",
	//				zap.Error(err),
	//				zap.Any("segmentID", segmentID))
	//			continue // not return, try to load balance all segment
	//		}
	//		segment.setOnService(true)
	//	}
	//}
	//log.Debug("load balance done",
	//	zap.Int64("msgID", msg.ID()),
	//	zap.Int("num of segment", len(msg.Infos)))
}

func (q *queryCollection) receiveRetrieve(msg *msgstream.RetrieveMsg) {
	if msg.CollectionID != q.collectionID {
		log.Debug("not target collection retrieve request",
			zap.Any("collectionID", msg.CollectionID),
			zap.Int64("msgID", msg.ID()),
		)
		return
	}

	log.Debug("consume retrieve message",
		zap.Any("collectionID", msg.CollectionID),
		zap.Int64("msgID", msg.ID()),
	)
	sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	msg.SetTraceCtx(ctx)

	// check if collection has been released
	collection, err := q.historical.replica.getCollectionByID(msg.CollectionID)
	if err != nil {
		log.Error(err.Error())
		q.publishFailedRetrieveResult(msg, err.Error())
		return
	}
	if msg.BeginTs() >= collection.getReleaseTime() {
		err := errors.New("retrieve failed, collection has been released, msgID = " +
			fmt.Sprintln(msg.ID()) +
			", collectionID = " +
			fmt.Sprintln(msg.CollectionID))
		log.Error(err.Error())
		q.publishFailedRetrieveResult(msg, err.Error())
		return
	}

	serviceTime := q.getServiceableTime()
	if msg.BeginTs() > serviceTime {
		bt, _ := tsoutil.ParseTS(msg.BeginTs())
		st, _ := tsoutil.ParseTS(serviceTime)
		log.Debug("query node::receiveRetrieveMsg: add to unsolvedMsg",
			zap.Any("collectionID", q.collectionID),
			zap.Any("sm.BeginTs", bt),
			zap.Any("serviceTime", st),
			zap.Any("delta seconds", (msg.BeginTs()-serviceTime)/(1000*1000*1000)),
			zap.Any("msgID", msg.ID()),
		)
		q.addToUnsolvedRetrieveMsg(msg)
		sp.LogFields(
			oplog.String("send to unsolved buffer", "send to unsolved buffer"),
			oplog.Object("begin ts", bt),
			oplog.Object("serviceTime", st),
			oplog.Float64("delta seconds", float64(msg.BeginTs()-serviceTime)/(1000.0*1000.0*1000.0)),
		)
		sp.Finish()
		return
	}
	log.Debug("doing retrieve in receiveRetrieveMsg...",
		zap.Int64("collectionID", msg.CollectionID),
		zap.Int64("msgID", msg.ID()),
	)
	err = q.retrieve(msg)
	if err != nil {
		log.Error(err.Error())
		log.Debug("do retrieve failed in receiveRetrieveMsg, prepare to publish failed retrieve result",
			zap.Int64("collectionID", msg.CollectionID),
			zap.Int64("msgID", msg.ID()),
		)
		q.publishFailedRetrieveResult(msg, err.Error())
	}
	log.Debug("do retrieve done in receiveRetrieve",
		zap.Int64("collectionID", msg.CollectionID),
		zap.Int64("msgID", msg.ID()),
	)
	sp.Finish()
}

func (q *queryCollection) receiveSearch(msg *msgstream.SearchMsg) {
	if msg.CollectionID != q.collectionID {
		log.Debug("not target collection search request",
			zap.Any("collectionID", msg.CollectionID),
			zap.Int64("msgID", msg.ID()),
		)
		return
	}

	log.Debug("consume search message",
		zap.Any("collectionID", msg.CollectionID),
		zap.Int64("msgID", msg.ID()),
	)
	sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	msg.SetTraceCtx(ctx)

	// check if collection has been released
	collection, err := q.historical.replica.getCollectionByID(msg.CollectionID)
	if err != nil {
		log.Error(err.Error())
		q.publishFailedSearchResult(msg, err.Error())
		return
	}
	if msg.BeginTs() >= collection.getReleaseTime() {
		err := errors.New("search failed, collection has been released, msgID = " +
			fmt.Sprintln(msg.ID()) +
			", collectionID = " +
			fmt.Sprintln(msg.CollectionID))
		log.Error(err.Error())
		q.publishFailedSearchResult(msg, err.Error())
		return
	}

	serviceTime := q.getServiceableTime()
	if msg.BeginTs() > serviceTime {
		bt, _ := tsoutil.ParseTS(msg.BeginTs())
		st, _ := tsoutil.ParseTS(serviceTime)
		log.Debug("query node::receiveSearchMsg: add to unsolvedMsg",
			zap.Any("collectionID", q.collectionID),
			zap.Any("sm.BeginTs", bt),
			zap.Any("serviceTime", st),
			zap.Any("delta seconds", (msg.BeginTs()-serviceTime)/(1000*1000*1000)),
			zap.Any("msgID", msg.ID()),
		)
		q.addToUnsolvedMsg(msg)
		sp.LogFields(
			oplog.String("send to unsolved buffer", "send to unsolved buffer"),
			oplog.Object("begin ts", bt),
			oplog.Object("serviceTime", st),
			oplog.Float64("delta seconds", float64(msg.BeginTs()-serviceTime)/(1000.0*1000.0*1000.0)),
		)
		sp.Finish()
		return
	}
	log.Debug("doing search in receiveSearchMsg...",
		zap.Int64("collectionID", msg.CollectionID),
		zap.Int64("msgID", msg.ID()),
	)
	err = q.search(msg)
	if err != nil {
		log.Error(err.Error())
		log.Debug("do search failed in receiveSearchMsg, prepare to publish failed search result",
			zap.Int64("collectionID", msg.CollectionID),
			zap.Int64("msgID", msg.ID()),
		)
		q.publishFailedSearchResult(msg, err.Error())
	}
	log.Debug("do search done in receiveSearch",
		zap.Int64("collectionID", msg.CollectionID),
		zap.Int64("msgID", msg.ID()),
	)
	sp.Finish()
}

func (q *queryCollection) doUnsolvedMsgSearch() {
	log.Debug("starting doUnsolvedMsgSearch...", zap.Any("collectionID", q.collectionID))
	for {
		select {
		case <-q.releaseCtx.Done():
			log.Debug("stop searchCollection's doUnsolvedMsgSearch", zap.Int64("collectionID", q.collectionID))
			return
		default:
			//time.Sleep(10 * time.Millisecond)
			serviceTime := q.waitNewTSafe()
			st, _ := tsoutil.ParseTS(serviceTime)
			log.Debug("get tSafe from flow graph",
				zap.Int64("collectionID", q.collectionID),
				zap.Any("tSafe", st))

			q.setServiceableTime(serviceTime)
			//log.Debug("query node::doUnsolvedMsgSearch: setServiceableTime",
			//	zap.Any("serviceTime", st),
			//)

			searchMsg := make([]*msgstream.SearchMsg, 0)
			tempMsg := q.popAllUnsolvedMsg()

			for _, sm := range tempMsg {
				bt, _ := tsoutil.ParseTS(sm.EndTs())
				st, _ = tsoutil.ParseTS(serviceTime)
				log.Debug("get search message from unsolvedMsg",
					zap.Int64("collectionID", sm.CollectionID),
					zap.Int64("msgID", sm.ID()),
					zap.Any("reqTime_p", bt),
					zap.Any("serviceTime_p", st),
					zap.Any("reqTime_l", sm.EndTs()),
					zap.Any("serviceTime_l", serviceTime),
				)
				if sm.EndTs() <= serviceTime {
					searchMsg = append(searchMsg, sm)
					continue
				}
				log.Debug("query node::doUnsolvedMsgSearch: add to unsolvedMsg",
					zap.Any("collectionID", q.collectionID),
					zap.Any("sm.BeginTs", bt),
					zap.Any("serviceTime", st),
					zap.Any("delta seconds", (sm.BeginTs()-serviceTime)/(1000*1000*1000)),
					zap.Any("msgID", sm.ID()),
				)
				q.addToUnsolvedMsg(sm)
			}

			if len(searchMsg) <= 0 {
				continue
			}
			for _, sm := range searchMsg {
				sp, ctx := trace.StartSpanFromContext(sm.TraceCtx())
				sm.SetTraceCtx(ctx)
				log.Debug("doing search in doUnsolvedMsgSearch...",
					zap.Int64("collectionID", sm.CollectionID),
					zap.Int64("msgID", sm.ID()),
				)
				err := q.search(sm)
				if err != nil {
					log.Error(err.Error())
					log.Debug("do search failed in doUnsolvedMsgSearch, prepare to publish failed search result",
						zap.Int64("collectionID", sm.CollectionID),
						zap.Int64("msgID", sm.ID()),
					)
					q.publishFailedSearchResult(sm, err.Error())
				}
				sp.Finish()
				log.Debug("do search done in doUnsolvedMsgSearch",
					zap.Int64("collectionID", sm.CollectionID),
					zap.Int64("msgID", sm.ID()),
				)
			}
			log.Debug("doUnsolvedMsgSearch, do search done", zap.Int("num of searchMsg", len(searchMsg)))
		}
	}
}

func translateHits(schema *typeutil.SchemaHelper, fieldIDs []int64, rawHits [][]byte) (*schemapb.SearchResultData, error) {
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
	numQuereis := len(rawHits)
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
		NumQueries: int64(numQuereis),
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
					data := int32(int16(binary.LittleEndian.Uint16(dataBlob)))
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
					data := int32(binary.LittleEndian.Uint32(dataBlob))
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
					data := int64(binary.LittleEndian.Uint64(dataBlob))
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
					data := math.Float32frombits(binary.LittleEndian.Uint32(dataBlob))
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
					data := math.Float64frombits(binary.LittleEndian.Uint64(dataBlob))
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
		case schemapb.DataType_BinaryVector:
			return nil, fmt.Errorf("unsupported")
		default:
		}
	}
	return finalResult, nil
}

// TODO:: cache map[dsl]plan
// TODO: reBatched search requests
func (q *queryCollection) search(searchMsg *msgstream.SearchMsg) error {
	sp, ctx := trace.StartSpanFromContext(searchMsg.TraceCtx())
	defer sp.Finish()
	searchMsg.SetTraceCtx(ctx)
	searchTimestamp := searchMsg.SearchRequest.TravelTimestamp

	collectionID := searchMsg.CollectionID
	collection, err := q.streaming.replica.getCollectionByID(collectionID)
	if err != nil {
		return err
	}
	schema, err := typeutil.CreateSchemaHelper(collection.schema)
	if err != nil {
		return err
	}

	var plan *Plan
	if searchMsg.GetDslType() == commonpb.DslType_BoolExprV1 {
		expr := searchMsg.SerializedExprPlan
		plan, err = createPlanByExpr(collection, expr)
		if err != nil {
			return err
		}
	} else {
		dsl := searchMsg.Dsl
		plan, err = createPlan(collection, dsl)
		if err != nil {
			return err
		}
	}
	searchRequestBlob := searchMsg.PlaceholderGroup
	searchReq, err := parseSearchRequest(plan, searchRequestBlob)
	if err != nil {
		return err
	}
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

	searchResults := make([]*SearchResult, 0)
	matchedSegments := make([]*Segment, 0)
	sealedSegmentSearched := make([]UniqueID, 0)

	// historical search
	hisSearchResults, hisSegmentResults, err1 := q.historical.search(searchRequests, collectionID, searchMsg.PartitionIDs, plan, searchTimestamp)
	if err1 != nil {
		log.Error(err1.Error())
		return err1
	}
	searchResults = append(searchResults, hisSearchResults...)
	matchedSegments = append(matchedSegments, hisSegmentResults...)
	for _, seg := range hisSegmentResults {
		sealedSegmentSearched = append(sealedSegmentSearched, seg.segmentID)
	}

	// streaming search
	var err2 error
	for _, channel := range collection.getVChannels() {
		var strSearchResults []*SearchResult
		var strSegmentResults []*Segment
		strSearchResults, strSegmentResults, err2 = q.streaming.search(searchRequests, collectionID, searchMsg.PartitionIDs, channel, plan, searchTimestamp)
		if err2 != nil {
			log.Error(err2.Error())
			return err2
		}
		searchResults = append(searchResults, strSearchResults...)
		matchedSegments = append(matchedSegments, strSegmentResults...)
	}

	sp.LogFields(oplog.String("statistical time", "segment search end"))
	if len(searchResults) <= 0 {
		for _, group := range searchRequests {
			nq := group.getNumOfQuery()
			nilHits := make([][]byte, nq)
			hit := &milvuspb.Hits{}
			for i := 0; i < int(nq); i++ {
				bs, err := proto.Marshal(hit)
				if err != nil {
					return err
				}
				nilHits[i] = bs
			}

			// TODO: remove inefficient code in cgo and use SearchResultData directly
			// TODO: Currently add a translate layer from hits to SearchResultData
			// TODO: hits marshal and unmarshal is likely bottleneck

			transformed, err := translateHits(schema, searchMsg.OutputFieldsId, nilHits)
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
					Hits:                     nilHits,
					SlicedBlob:               byteBlobs,
					SlicedOffset:             1,
					SlicedNumCount:           1,
					MetricType:               plan.getMetricType(),
					SealedSegmentIDsSearched: sealedSegmentSearched,
					ChannelIDsSearched:       collection.getVChannels(),
					//TODO:: get global sealed segment from etcd
					GlobalSealedSegmentIDs: sealedSegmentSearched,
				},
			}
			log.Debug("QueryNode Empty SearchResultMsg",
				zap.Any("collectionID", collection.ID()),
				zap.Any("msgID", searchMsg.ID()),
				zap.Any("vChannels", collection.getVChannels()),
				zap.Any("sealedSegmentSearched", sealedSegmentSearched),
			)
			err = q.publishSearchResult(searchResultMsg, searchMsg.CollectionID)
			if err != nil {
				return err
			}
			return nil
		}
	}

	inReduced := make([]bool, len(searchResults))
	numSegment := int64(len(searchResults))
	var marshaledHits *MarshaledHits = nil
	if numSegment == 1 {
		inReduced[0] = true
		err = fillTargetEntry(plan, searchResults, matchedSegments, inReduced)
		sp.LogFields(oplog.String("statistical time", "fillTargetEntry end"))
		if err != nil {
			return err
		}
		marshaledHits, err = reorganizeSingleQueryResult(plan, searchRequests, searchResults[0])
		sp.LogFields(oplog.String("statistical time", "reorganizeSingleQueryResult end"))
		if err != nil {
			return err
		}
	} else {
		err = reduceSearchResults(searchResults, numSegment, inReduced)
		sp.LogFields(oplog.String("statistical time", "reduceSearchResults end"))
		if err != nil {
			return err
		}
		err = fillTargetEntry(plan, searchResults, matchedSegments, inReduced)
		sp.LogFields(oplog.String("statistical time", "fillTargetEntry end"))
		if err != nil {
			return err
		}
		marshaledHits, err = reorganizeQueryResults(plan, searchRequests, searchResults, numSegment, inReduced)
		sp.LogFields(oplog.String("statistical time", "reorganizeQueryResults end"))
		if err != nil {
			return err
		}
	}
	hitsBlob, err := marshaledHits.getHitsBlob()
	sp.LogFields(oplog.String("statistical time", "getHitsBlob end"))
	if err != nil {
		return err
	}

	var offset int64 = 0
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
				Hits:                     hits,
				SlicedBlob:               byteBlobs,
				SlicedOffset:             1,
				SlicedNumCount:           1,
				MetricType:               plan.getMetricType(),
				SealedSegmentIDsSearched: sealedSegmentSearched,
				ChannelIDsSearched:       collection.getVChannels(),
				//TODO:: get global sealed segment from etcd
				GlobalSealedSegmentIDs: sealedSegmentSearched,
			},
		}
		log.Debug("QueryNode SearchResultMsg",
			zap.Any("collectionID", collection.ID()),
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
		err = q.publishSearchResult(searchResultMsg, searchMsg.CollectionID)
		if err != nil {
			return err
		}
	}

	sp.LogFields(oplog.String("statistical time", "before free c++ memory"))
	deleteSearchResults(searchResults)
	deleteMarshaledHits(marshaledHits)
	sp.LogFields(oplog.String("statistical time", "stats done"))
	plan.delete()
	searchReq.delete()
	return nil
}

func (q *queryCollection) publishSearchResult(msg msgstream.TsMsg, collectionID UniqueID) error {
	log.Debug("publishing search result...",
		zap.Int64("collectionID", collectionID),
		zap.Int64("msgID", msg.ID()),
	)
	span, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	defer span.Finish()
	msg.SetTraceCtx(ctx)
	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, msg)
	err := q.queryResultMsgStream.Produce(&msgPack)
	if err != nil {
		log.Error("publishing search result failed, err = "+err.Error(),
			zap.Int64("collectionID", collectionID),
			zap.Int64("msgID", msg.ID()),
		)
	} else {
		log.Debug("publish search result done",
			zap.Int64("collectionID", collectionID),
			zap.Int64("msgID", msg.ID()),
		)
	}
	return err
}

func (q *queryCollection) publishFailedSearchResult(searchMsg *msgstream.SearchMsg, errMsg string) {
	span, ctx := trace.StartSpanFromContext(searchMsg.TraceCtx())
	defer span.Finish()
	searchMsg.SetTraceCtx(ctx)
	//log.Debug("Public fail SearchResult!")
	msgPack := msgstream.MsgPack{}

	resultChannelInt := 0
	searchResultMsg := &msgstream.SearchResultMsg{
		BaseMsg: msgstream.BaseMsg{HashValues: []uint32{uint32(resultChannelInt)}},
		SearchResults: internalpb.SearchResults{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_SearchResult,
				MsgID:     searchMsg.Base.MsgID,
				Timestamp: searchMsg.Base.Timestamp,
				SourceID:  searchMsg.Base.SourceID,
			},
			Status:          &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: errMsg},
			ResultChannelID: searchMsg.ResultChannelID,
		},
	}

	msgPack.Msgs = append(msgPack.Msgs, searchResultMsg)
	err := q.queryResultMsgStream.Produce(&msgPack)
	if err != nil {
		log.Error("publish FailedSearchResult failed" + err.Error())
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////

func (q *queryCollection) doUnsolvedMsgRetrieve() {
	log.Debug("starting doUnsolvedMsgRetrieve...", zap.Any("collectionID", q.collectionID))
	for {
		select {
		case <-q.releaseCtx.Done():
			log.Debug("stop retrieveCollection's doUnsolvedMsgRertieve", zap.Int64("collectionID", q.collectionID))
			return
		default:
			//time.Sleep(10 * time.Millisecond)
			serviceTime := q.waitNewTSafe()
			st, _ := tsoutil.ParseTS(serviceTime)
			log.Debug("get tSafe from flow graph",
				zap.Int64("collectionID", q.collectionID),
				zap.Any("tSafe", st))

			q.setServiceableTime(serviceTime)
			//log.Debug("query node::doUnsolvedMsgSearch: setServiceableTime",
			//	zap.Any("serviceTime", st),
			//)

			retrieveMsg := make([]*msgstream.RetrieveMsg, 0)
			tempMsg := q.popAllUnsolvedRetrieveMsg()

			for _, rm := range tempMsg {
				bt, _ := tsoutil.ParseTS(rm.EndTs())
				st, _ = tsoutil.ParseTS(serviceTime)
				log.Debug("get retrieve message from unsolvedMsg",
					zap.Int64("collectionID", rm.CollectionID),
					zap.Int64("msgID", rm.ID()),
					zap.Any("reqTime_p", bt),
					zap.Any("serviceTime_p", st),
					zap.Any("reqTime_l", rm.EndTs()),
					zap.Any("serviceTime_l", serviceTime),
				)
				if rm.EndTs() <= serviceTime {
					retrieveMsg = append(retrieveMsg, rm)
					continue
				}
				log.Debug("query node::doUnsolvedMsgRetrieve: add to unsolvedMsg",
					zap.Any("collectionID", q.collectionID),
					zap.Any("sm.BeginTs", bt),
					zap.Any("serviceTime", st),
					zap.Any("delta seconds", (rm.BeginTs()-serviceTime)/(1000*1000*1000)),
					zap.Any("msgID", rm.ID()),
				)
				q.addToUnsolvedRetrieveMsg(rm)
			}

			if len(retrieveMsg) <= 0 {
				continue
			}
			for _, rm := range retrieveMsg {
				sp, ctx := trace.StartSpanFromContext(rm.TraceCtx())
				rm.SetTraceCtx(ctx)
				log.Debug("doing search in doUnsolvedMsgRetrieve...",
					zap.Int64("collectionID", rm.CollectionID),
					zap.Int64("msgID", rm.ID()),
				)
				err := q.retrieve(rm)
				if err != nil {
					log.Error(err.Error())
					log.Debug("do retrieve failed in doUnsolvedMsgSearch, prepare to publish failed retrieve result",
						zap.Int64("collectionID", rm.CollectionID),
						zap.Int64("msgID", rm.ID()),
					)
					q.publishFailedRetrieveResult(rm, err.Error())
				}
				sp.Finish()
				log.Debug("do retrieve done in doUnsolvedMsgSearch",
					zap.Int64("collectionID", rm.CollectionID),
					zap.Int64("msgID", rm.ID()),
				)
			}
			log.Debug("doUnsolvedMsgRetrieve, do retrieve done", zap.Int("num of retrieveMsg", len(retrieveMsg)))
		}
	}
}

func (q *queryCollection) retrieve(retrieveMsg *msgstream.RetrieveMsg) error {
	// TODO(yukun)
	// step 1: get retrieve object and defer destruction
	// step 2: for each segment, call retrieve to get ids proto buffer
	// step 3: merge all proto in go
	// step 4: publish results
	// retrieveProtoBlob, err := proto.Marshal(&retrieveMsg.RetrieveRequest)
	sp, ctx := trace.StartSpanFromContext(retrieveMsg.TraceCtx())
	defer sp.Finish()
	retrieveMsg.SetTraceCtx(ctx)
	timestamp := retrieveMsg.RetrieveRequest.TravelTimestamp

	collectionID := retrieveMsg.CollectionID
	collection, err := q.streaming.replica.getCollectionByID(collectionID)
	if err != nil {
		return err
	}

	req := &segcorepb.RetrieveRequest{
		Ids:          retrieveMsg.Ids,
		OutputFields: retrieveMsg.OutputFields,
	}

	plan, err := createRetrievePlan(collection, req, timestamp)
	if err != nil {
		return err
	}
	defer plan.delete()

	var partitionIDsInHistorical []UniqueID
	var partitionIDsInStreaming []UniqueID
	partitionIDsInQuery := retrieveMsg.PartitionIDs
	if len(partitionIDsInQuery) == 0 {
		partitionIDsInHistoricalCol, err1 := q.historical.replica.getPartitionIDs(collectionID)
		partitionIDsInStreamingCol, err2 := q.streaming.replica.getPartitionIDs(collectionID)
		if err1 != nil && err2 != nil {
			return err2
		}
		if len(partitionIDsInHistoricalCol) == 0 {
			return errors.New("none of this collection's partition has been loaded")
		}
		partitionIDsInHistorical = partitionIDsInHistoricalCol
		partitionIDsInStreaming = partitionIDsInStreamingCol
	} else {
		for _, id := range partitionIDsInQuery {
			_, err1 := q.historical.replica.getPartitionByID(id)
			if err1 == nil {
				partitionIDsInHistorical = append(partitionIDsInHistorical, id)
			}
			_, err2 := q.streaming.replica.getPartitionByID(id)
			if err2 == nil {
				partitionIDsInStreaming = append(partitionIDsInStreaming, id)
			}
			if err1 != nil && err2 != nil {
				return err2
			}
		}
	}

	sealedSegmentRetrieved := make([]UniqueID, 0)
	var mergeList []*segcorepb.RetrieveResults
	for _, partitionID := range partitionIDsInHistorical {
		segmentIDs, err := q.historical.replica.getSegmentIDs(partitionID)
		if err != nil {
			return err
		}
		for _, segmentID := range segmentIDs {
			segment, err := q.historical.replica.getSegmentByID(segmentID)
			if err != nil {
				return err
			}
			result, err := segment.segmentGetEntityByIds(plan)
			if err != nil {
				return err
			}
			mergeList = append(mergeList, result)
			sealedSegmentRetrieved = append(sealedSegmentRetrieved, segmentID)
		}
	}

	for _, partitionID := range partitionIDsInStreaming {
		segmentIDs, err := q.streaming.replica.getSegmentIDs(partitionID)
		if err != nil {
			return err
		}
		for _, segmentID := range segmentIDs {
			segment, err := q.streaming.replica.getSegmentByID(segmentID)
			if err != nil {
				return err
			}
			result, err := segment.segmentGetEntityByIds(plan)
			if err != nil {
				return err
			}
			mergeList = append(mergeList, result)
		}
	}

	result, err := mergeRetrieveResults(mergeList)
	if err != nil {
		return err
	}

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
			//TODO(yukun):: get global sealed segment from etcd
			GlobalSealedSegmentIDs: sealedSegmentRetrieved,
		},
	}
	log.Debug("QueryNode RetrieveResultMsg",
		zap.Any("pChannels", collection.getPChannels()),
		zap.Any("collectionID", collection.ID()),
		zap.Any("sealedSegmentRetrieved", sealedSegmentRetrieved),
	)
	err3 := q.publishRetrieveResult(retrieveResultMsg, retrieveMsg.CollectionID)
	if err3 != nil {
		return err3
	}
	return nil
}

func mergeRetrieveResults(dataArr []*segcorepb.RetrieveResults) (*segcorepb.RetrieveResults, error) {
	var final *segcorepb.RetrieveResults
	for _, data := range dataArr {
		if data == nil {
			continue
		}

		if final == nil {
			final = proto.Clone(data).(*segcorepb.RetrieveResults)
			continue
		}

		proto.Merge(final.Ids, data.Ids)
		if len(final.FieldsData) != len(data.FieldsData) {
			return nil, fmt.Errorf("mismatch FieldData in RetrieveResults")
		}

		for i := range final.FieldsData {
			proto.Merge(final.FieldsData[i], data.FieldsData[i])
		}
	}

	// not found, return default values indicating not result found
	if final == nil {
		final = &segcorepb.RetrieveResults{
			Ids:        nil,
			FieldsData: []*schemapb.FieldData{},
		}
	}

	return final, nil
}

func (q *queryCollection) publishRetrieveResult(msg msgstream.TsMsg, collectionID UniqueID) error {
	log.Debug("publishing retrieve result...",
		zap.Int64("msgID", msg.ID()),
		zap.Int64("collectionID", collectionID))
	span, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	defer span.Finish()
	msg.SetTraceCtx(ctx)
	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, msg)
	err := q.queryResultMsgStream.Produce(&msgPack)
	if err != nil {
		log.Error(err.Error())
	} else {
		log.Debug("publish retrieve result done",
			zap.Int64("msgID", msg.ID()),
			zap.Int64("collectionID", collectionID))
	}
	return err
}

func (q *queryCollection) publishFailedRetrieveResult(retrieveMsg *msgstream.RetrieveMsg, errMsg string) error {
	span, ctx := trace.StartSpanFromContext(retrieveMsg.TraceCtx())
	defer span.Finish()
	retrieveMsg.SetTraceCtx(ctx)
	msgPack := msgstream.MsgPack{}

	resultChannelInt := 0
	retrieveResultMsg := &msgstream.RetrieveResultMsg{
		BaseMsg: msgstream.BaseMsg{HashValues: []uint32{uint32(resultChannelInt)}},
		RetrieveResults: internalpb.RetrieveResults{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_RetrieveResult,
				MsgID:     retrieveMsg.Base.MsgID,
				Timestamp: retrieveMsg.Base.Timestamp,
				SourceID:  retrieveMsg.Base.SourceID,
			},
			Status:          &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: errMsg},
			ResultChannelID: retrieveMsg.ResultChannelID,
			Ids:             nil,
			FieldsData:      nil,
		},
	}

	msgPack.Msgs = append(msgPack.Msgs, retrieveResultMsg)
	err := q.queryResultMsgStream.Produce(&msgPack)
	if err != nil {
		return err
	}

	return nil
}
