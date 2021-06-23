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

type searchCollection struct {
	releaseCtx context.Context
	cancel     context.CancelFunc

	collectionID UniqueID
	historical   *historical
	streaming    *streaming

	msgBuffer     chan *msgstream.SearchMsg
	unsolvedMsgMu sync.Mutex // guards unsolvedMsg
	unsolvedMsg   []*msgstream.SearchMsg

	tSafeWatchers     map[Channel]*tSafeWatcher
	watcherSelectCase []reflect.SelectCase

	serviceableTimeMutex sync.Mutex // guards serviceableTime
	serviceableTime      Timestamp

	searchMsgStream       msgstream.MsgStream
	searchResultMsgStream msgstream.MsgStream
}

type ResultEntityIds []UniqueID

func newSearchCollection(releaseCtx context.Context,
	cancel context.CancelFunc,
	collectionID UniqueID,
	historical *historical,
	streaming *streaming,
	factory msgstream.Factory) *searchCollection {

	receiveBufSize := Params.SearchReceiveBufSize
	msgBuffer := make(chan *msgstream.SearchMsg, receiveBufSize)
	unsolvedMsg := make([]*msgstream.SearchMsg, 0)

	searchStream, _ := factory.NewQueryMsgStream(releaseCtx)
	searchResultStream, _ := factory.NewQueryMsgStream(releaseCtx)

	sc := &searchCollection{
		releaseCtx: releaseCtx,
		cancel:     cancel,

		collectionID: collectionID,
		historical:   historical,
		streaming:    streaming,

		tSafeWatchers: make(map[Channel]*tSafeWatcher),

		msgBuffer:   msgBuffer,
		unsolvedMsg: unsolvedMsg,

		searchMsgStream:       searchStream,
		searchResultMsgStream: searchResultStream,
	}

	sc.register()
	return sc
}

func (s *searchCollection) start() {
	go s.searchMsgStream.Start()
	go s.searchResultMsgStream.Start()
	go s.consumeSearch()
	go s.doUnsolvedMsgSearch()
}

func (s *searchCollection) close() {
	if s.searchMsgStream != nil {
		s.searchMsgStream.Close()
	}
	if s.searchResultMsgStream != nil {
		s.searchResultMsgStream.Close()
	}
}

func (s *searchCollection) register() {
	collection, err := s.streaming.replica.getCollectionByID(s.collectionID)
	if err != nil {
		log.Error(err.Error())
		return
	}

	s.watcherSelectCase = make([]reflect.SelectCase, 0)
	log.Debug("register tSafe watcher and init watcher select case",
		zap.Any("collectionID", collection.ID()),
		zap.Any("dml channels", collection.getVChannels()),
	)
	for _, channel := range collection.getVChannels() {
		s.tSafeWatchers[channel] = newTSafeWatcher()
		s.streaming.tSafeReplica.registerTSafeWatcher(channel, s.tSafeWatchers[channel])
		s.watcherSelectCase = append(s.watcherSelectCase, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(s.tSafeWatchers[channel].watcherChan()),
		})
	}
}

func (s *searchCollection) addToUnsolvedMsg(msg *msgstream.SearchMsg) {
	s.unsolvedMsgMu.Lock()
	defer s.unsolvedMsgMu.Unlock()
	s.unsolvedMsg = append(s.unsolvedMsg, msg)
}

func (s *searchCollection) popAllUnsolvedMsg() []*msgstream.SearchMsg {
	s.unsolvedMsgMu.Lock()
	defer s.unsolvedMsgMu.Unlock()
	tmp := s.unsolvedMsg
	s.unsolvedMsg = s.unsolvedMsg[:0]
	return tmp
}

func (s *searchCollection) waitNewTSafe() Timestamp {
	// block until any vChannel updating tSafe
	_, _, recvOK := reflect.Select(s.watcherSelectCase)
	if !recvOK {
		log.Error("tSafe has been closed", zap.Any("collectionID", s.collectionID))
		return invalidTimestamp
	}
	//log.Debug("wait new tSafe", zap.Any("collectionID", s.collectionID))
	t := Timestamp(math.MaxInt64)
	for channel := range s.tSafeWatchers {
		ts := s.streaming.tSafeReplica.getTSafe(channel)
		if ts <= t {
			t = ts
		}
	}
	return t
}

func (s *searchCollection) getServiceableTime() Timestamp {
	s.serviceableTimeMutex.Lock()
	defer s.serviceableTimeMutex.Unlock()
	return s.serviceableTime
}

func (s *searchCollection) setServiceableTime(t Timestamp) {
	s.serviceableTimeMutex.Lock()
	defer s.serviceableTimeMutex.Unlock()

	if t < s.serviceableTime {
		return
	}

	gracefulTimeInMilliSecond := Params.GracefulTime
	if gracefulTimeInMilliSecond > 0 {
		gracefulTime := tsoutil.ComposeTS(gracefulTimeInMilliSecond, 0)
		s.serviceableTime = t + gracefulTime
	} else {
		s.serviceableTime = t
	}
}

func (s *searchCollection) emptySearch(searchMsg *msgstream.SearchMsg) {
	sp, ctx := trace.StartSpanFromContext(searchMsg.TraceCtx())
	defer sp.Finish()
	searchMsg.SetTraceCtx(ctx)
	err := s.search(searchMsg)
	if err != nil {
		log.Error(err.Error())
		s.publishFailedSearchResult(searchMsg, err.Error())
	}
}

func (s *searchCollection) consumeSearch() {
	for {
		select {
		case <-s.releaseCtx.Done():
			log.Debug("stop searchCollection's receiveSearchMsg", zap.Int64("collectionID", s.collectionID))
			return
		default:
			msgPack := s.searchMsgStream.Consume()
			if msgPack == nil || len(msgPack.Msgs) <= 0 {
				msgPackNil := msgPack == nil
				msgPackEmpty := true
				if msgPack != nil {
					msgPackEmpty = len(msgPack.Msgs) <= 0
				}
				log.Debug("consume search message failed", zap.Any("msgPack is Nil", msgPackNil),
					zap.Any("msgPackEmpty", msgPackEmpty))
				continue
			}
			for _, msg := range msgPack.Msgs {
				switch sm := msg.(type) {
				case *msgstream.SearchMsg:
					s.receiveSearch(sm)
				case *msgstream.LoadBalanceSegmentsMsg:
					s.loadBalance(sm)
				default:
					log.Warn("unsupported msg type in search channel", zap.Any("msg", sm))
				}
			}
		}
	}
}

func (s *searchCollection) loadBalance(msg *msgstream.LoadBalanceSegmentsMsg) {
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

func (s *searchCollection) receiveSearch(msg *msgstream.SearchMsg) {
	if msg.CollectionID != s.collectionID {
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
	collection, err := s.historical.replica.getCollectionByID(msg.CollectionID)
	if err != nil {
		log.Error(err.Error())
		s.publishFailedSearchResult(msg, err.Error())
		return
	}
	if msg.BeginTs() >= collection.getReleaseTime() {
		err := errors.New("search failed, collection has been released, msgID = " +
			fmt.Sprintln(msg.ID()) +
			", collectionID = " +
			fmt.Sprintln(msg.CollectionID))
		log.Error(err.Error())
		s.publishFailedSearchResult(msg, err.Error())
		return
	}

	serviceTime := s.getServiceableTime()
	if msg.BeginTs() > serviceTime {
		bt, _ := tsoutil.ParseTS(msg.BeginTs())
		st, _ := tsoutil.ParseTS(serviceTime)
		log.Debug("query node::receiveSearchMsg: add to unsolvedMsg",
			zap.Any("collectionID", s.collectionID),
			zap.Any("sm.BeginTs", bt),
			zap.Any("serviceTime", st),
			zap.Any("delta seconds", (msg.BeginTs()-serviceTime)/(1000*1000*1000)),
			zap.Any("msgID", msg.ID()),
		)
		s.addToUnsolvedMsg(msg)
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
	err = s.search(msg)
	if err != nil {
		log.Error(err.Error())
		log.Debug("do search failed in receiveSearchMsg, prepare to publish failed search result",
			zap.Int64("collectionID", msg.CollectionID),
			zap.Int64("msgID", msg.ID()),
		)
		s.publishFailedSearchResult(msg, err.Error())
	}
	log.Debug("do search done in receiveSearch",
		zap.Int64("collectionID", msg.CollectionID),
		zap.Int64("msgID", msg.ID()),
	)
	sp.Finish()
}

func (s *searchCollection) doUnsolvedMsgSearch() {
	log.Debug("starting doUnsolvedMsgSearch...", zap.Any("collectionID", s.collectionID))
	for {
		select {
		case <-s.releaseCtx.Done():
			log.Debug("stop searchCollection's doUnsolvedMsgSearch", zap.Int64("collectionID", s.collectionID))
			return
		default:
			//time.Sleep(10 * time.Millisecond)
			serviceTime := s.waitNewTSafe()
			st, _ := tsoutil.ParseTS(serviceTime)
			log.Debug("get tSafe from flow graph",
				zap.Int64("collectionID", s.collectionID),
				zap.Any("tSafe", st))

			s.setServiceableTime(serviceTime)
			//log.Debug("query node::doUnsolvedMsgSearch: setServiceableTime",
			//	zap.Any("serviceTime", st),
			//)

			searchMsg := make([]*msgstream.SearchMsg, 0)
			tempMsg := s.popAllUnsolvedMsg()

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
					zap.Any("collectionID", s.collectionID),
					zap.Any("sm.BeginTs", bt),
					zap.Any("serviceTime", st),
					zap.Any("delta seconds", (sm.BeginTs()-serviceTime)/(1000*1000*1000)),
					zap.Any("msgID", sm.ID()),
				)
				s.addToUnsolvedMsg(sm)
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
				err := s.search(sm)
				if err != nil {
					log.Error(err.Error())
					log.Debug("do search failed in doUnsolvedMsgSearch, prepare to publish failed search result",
						zap.Int64("collectionID", sm.CollectionID),
						zap.Int64("msgID", sm.ID()),
					)
					s.publishFailedSearchResult(sm, err.Error())
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
func (s *searchCollection) search(searchMsg *msgstream.SearchMsg) error {
	sp, ctx := trace.StartSpanFromContext(searchMsg.TraceCtx())
	defer sp.Finish()
	searchMsg.SetTraceCtx(ctx)
	searchTimestamp := searchMsg.SearchRequest.TravelTimestamp

	collectionID := searchMsg.CollectionID
	collection, err := s.streaming.replica.getCollectionByID(collectionID)
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
	hisSearchResults, hisSegmentResults, err1 := s.historical.search(searchRequests, collectionID, searchMsg.PartitionIDs, plan, searchTimestamp)
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
		strSearchResults, strSegmentResults, err2 = s.streaming.search(searchRequests, collectionID, searchMsg.PartitionIDs, channel, plan, searchTimestamp)
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
					ChannelIDsSearched:       collection.getPChannels(),
					//TODO:: get global sealed segment from etcd
					GlobalSealedSegmentIDs: sealedSegmentSearched,
				},
			}
			log.Debug("QueryNode Empty SearchResultMsg",
				zap.Any("collectionID", collection.ID()),
				zap.Any("msgID", searchMsg.ID()),
				zap.Any("pChannels", collection.getPChannels()),
				zap.Any("sealedSegmentSearched", sealedSegmentSearched),
			)
			err = s.publishSearchResult(searchResultMsg, searchMsg.CollectionID)
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
				ChannelIDsSearched:       collection.getPChannels(),
				//TODO:: get global sealed segment from etcd
				GlobalSealedSegmentIDs: sealedSegmentSearched,
			},
		}
		log.Debug("QueryNode SearchResultMsg",
			zap.Any("collectionID", collection.ID()),
			zap.Any("msgID", searchMsg.ID()),
			zap.Any("pChannels", collection.getPChannels()),
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
		err = s.publishSearchResult(searchResultMsg, searchMsg.CollectionID)
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

func (s *searchCollection) publishSearchResult(msg msgstream.TsMsg, collectionID UniqueID) error {
	log.Debug("publishing search result...",
		zap.Int64("collectionID", collectionID),
		zap.Int64("msgID", msg.ID()),
	)
	span, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	defer span.Finish()
	msg.SetTraceCtx(ctx)
	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, msg)
	err := s.searchResultMsgStream.Produce(&msgPack)
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

func (s *searchCollection) publishFailedSearchResult(searchMsg *msgstream.SearchMsg, errMsg string) {
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
	err := s.searchResultMsgStream.Produce(&msgPack)
	if err != nil {
		log.Error("publish FailedSearchResult failed" + err.Error())
	}
}
