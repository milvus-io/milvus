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
	"errors"
	"sync"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	oplog "github.com/opentracing/opentracing-go/log"
)

type searchCollection struct {
	releaseCtx context.Context
	cancel     context.CancelFunc

	collectionID      UniqueID
	historicalReplica ReplicaInterface
	streamingReplica  ReplicaInterface
	tSafeReplica      TSafeReplicaInterface

	msgBuffer     chan *msgstream.SearchMsg
	unsolvedMSgMu sync.Mutex // guards unsolvedMsg
	unsolvedMsg   []*msgstream.SearchMsg

	tSafeMutex   sync.Mutex
	tSafeWatcher *tSafeWatcher

	serviceableTimeMutex sync.Mutex // guards serviceableTime
	serviceableTime      Timestamp

	searchResultMsgStream msgstream.MsgStream
}

type ResultEntityIds []UniqueID

func newSearchCollection(releaseCtx context.Context,
	cancel context.CancelFunc,
	collectionID UniqueID,
	historicalReplica ReplicaInterface,
	streamingReplica ReplicaInterface,
	tSafeReplica TSafeReplicaInterface,
	searchResultStream msgstream.MsgStream) *searchCollection {
	receiveBufSize := Params.SearchReceiveBufSize
	msgBuffer := make(chan *msgstream.SearchMsg, receiveBufSize)
	unsolvedMsg := make([]*msgstream.SearchMsg, 0)

	sc := &searchCollection{
		releaseCtx: releaseCtx,
		cancel:     cancel,

		collectionID:      collectionID,
		historicalReplica: historicalReplica,
		streamingReplica:  streamingReplica,
		tSafeReplica:      tSafeReplica,

		msgBuffer:   msgBuffer,
		unsolvedMsg: unsolvedMsg,

		searchResultMsgStream: searchResultStream,
	}

	sc.register(collectionID)
	return sc
}

func (s *searchCollection) start() {
	go s.receiveSearchMsg()
	go s.doUnsolvedMsgSearch()
}

func (s *searchCollection) register(collectionID UniqueID) {
	// TODO: remove and use vChannel
	vChannel := collectionIDToChannel(collectionID)
	s.tSafeReplica.addTSafe(vChannel)
	s.tSafeMutex.Lock()
	s.tSafeWatcher = newTSafeWatcher()
	s.tSafeMutex.Unlock()
	s.tSafeReplica.registerTSafeWatcher(vChannel, s.tSafeWatcher)
}

func (s *searchCollection) addToUnsolvedMsg(msg *msgstream.SearchMsg) {
	s.unsolvedMSgMu.Lock()
	defer s.unsolvedMSgMu.Unlock()
	s.unsolvedMsg = append(s.unsolvedMsg, msg)
}

func (s *searchCollection) popAllUnsolvedMsg() []*msgstream.SearchMsg {
	s.unsolvedMSgMu.Lock()
	defer s.unsolvedMSgMu.Unlock()
	tmp := s.unsolvedMsg
	s.unsolvedMsg = s.unsolvedMsg[:0]
	return tmp
}

func (s *searchCollection) waitNewTSafe() Timestamp {
	// TODO: remove and use vChannel
	vChannel := collectionIDToChannel(s.collectionID)
	// block until dataSyncService updating tSafe
	s.tSafeWatcher.hasUpdate()
	ts := s.tSafeReplica.getTSafe(vChannel)
	return ts
}

func (s *searchCollection) getServiceableTime() Timestamp {
	s.serviceableTimeMutex.Lock()
	defer s.serviceableTimeMutex.Unlock()
	return s.serviceableTime
}

func (s *searchCollection) setServiceableTime(t Timestamp) {
	s.serviceableTimeMutex.Lock()
	gracefulTimeInMilliSecond := Params.GracefulTime
	if gracefulTimeInMilliSecond > 0 {
		gracefulTime := tsoutil.ComposeTS(gracefulTimeInMilliSecond, 0)
		s.serviceableTime = t + gracefulTime
	} else {
		s.serviceableTime = t
	}
	s.serviceableTimeMutex.Unlock()
}

func (s *searchCollection) emptySearch(searchMsg *msgstream.SearchMsg) {
	sp, ctx := trace.StartSpanFromContext(searchMsg.TraceCtx())
	defer sp.Finish()
	searchMsg.SetTraceCtx(ctx)
	err := s.search(searchMsg)
	if err != nil {
		log.Error(err.Error())
		err2 := s.publishFailedSearchResult(searchMsg, err.Error())
		if err2 != nil {
			log.Error("publish FailedSearchResult failed", zap.Error(err2))
		}
	}
}

func (s *searchCollection) receiveSearchMsg() {
	for {
		select {
		case <-s.releaseCtx.Done():
			log.Debug("stop searchCollection's receiveSearchMsg", zap.Int64("collectionID", s.collectionID))
			return
		case sm := <-s.msgBuffer:
			sp, ctx := trace.StartSpanFromContext(sm.TraceCtx())
			sm.SetTraceCtx(ctx)
			log.Debug("get search message from msgBuffer",
				zap.Int64("msgID", sm.ID()),
				zap.Int64("collectionID", sm.CollectionID))
			serviceTime := s.getServiceableTime()
			if sm.BeginTs() > serviceTime {
				bt, _ := tsoutil.ParseTS(sm.BeginTs())
				st, _ := tsoutil.ParseTS(serviceTime)
				log.Debug("querynode::receiveSearchMsg: add to unsolvedMsgs",
					zap.Any("sm.BeginTs", bt),
					zap.Any("serviceTime", st),
					zap.Any("delta seconds", (sm.BeginTs()-serviceTime)/(1000*1000*1000)),
					zap.Any("collectionID", s.collectionID),
				)
				s.addToUnsolvedMsg(sm)
				sp.LogFields(
					oplog.String("send to unsolved buffer", "send to unsolved buffer"),
					oplog.Object("begin ts", bt),
					oplog.Object("serviceTime", st),
					oplog.Float64("delta seconds", float64(sm.BeginTs()-serviceTime)/(1000.0*1000.0*1000.0)),
				)
				sp.Finish()
				continue
			}
			log.Debug("doing search in receiveSearchMsg...",
				zap.Int64("msgID", sm.ID()),
				zap.Int64("collectionID", sm.CollectionID))
			err := s.search(sm)
			if err != nil {
				log.Error(err.Error())
				log.Debug("do search failed in receiveSearchMsg, prepare to publish failed search result",
					zap.Int64("msgID", sm.ID()),
					zap.Int64("collectionID", sm.CollectionID))
				err2 := s.publishFailedSearchResult(sm, err.Error())
				if err2 != nil {
					log.Error("publish FailedSearchResult failed", zap.Error(err2))
				}
			}
			log.Debug("do search done in receiveSearchMsg",
				zap.Int64("msgID", sm.ID()),
				zap.Int64("collectionID", sm.CollectionID))
			sp.Finish()
		}
	}
}

func (s *searchCollection) doUnsolvedMsgSearch() {
	for {
		select {
		case <-s.releaseCtx.Done():
			log.Debug("stop searchCollection's doUnsolvedMsgSearch", zap.Int64("collectionID", s.collectionID))
			return
		default:
			serviceTime := s.waitNewTSafe()
			s.setServiceableTime(serviceTime)
			log.Debug("querynode::doUnsolvedMsgSearch: setServiceableTime",
				zap.Any("serviceTime", serviceTime),
			)
			log.Debug("get tSafe from flow graph",
				zap.Int64("collectionID", s.collectionID),
				zap.Uint64("tSafe", serviceTime))

			searchMsg := make([]*msgstream.SearchMsg, 0)
			tempMsg := s.popAllUnsolvedMsg()

			for _, sm := range tempMsg {
				log.Debug("get search message from unsolvedMsg",
					zap.Int64("msgID", sm.ID()),
					zap.Int64("collectionID", sm.CollectionID))
				if sm.EndTs() <= serviceTime {
					searchMsg = append(searchMsg, sm)
					continue
				}
				s.addToUnsolvedMsg(sm)
			}

			if len(searchMsg) <= 0 {
				continue
			}
			for _, sm := range searchMsg {
				sp, ctx := trace.StartSpanFromContext(sm.TraceCtx())
				sm.SetTraceCtx(ctx)
				log.Debug("doing search in doUnsolvedMsgSearch...",
					zap.Int64("msgID", sm.ID()),
					zap.Int64("collectionID", sm.CollectionID))
				err := s.search(sm)
				if err != nil {
					log.Error(err.Error())
					log.Debug("do search failed in doUnsolvedMsgSearch, prepare to publish failed search result",
						zap.Int64("msgID", sm.ID()),
						zap.Int64("collectionID", sm.CollectionID))
					err2 := s.publishFailedSearchResult(sm, err.Error())
					if err2 != nil {
						log.Error("publish FailedSearchResult failed", zap.Error(err2))
					}
				}
				sp.Finish()
				log.Debug("do search done in doUnsolvedMsgSearch",
					zap.Int64("msgID", sm.ID()),
					zap.Int64("collectionID", sm.CollectionID))
			}
			log.Debug("doUnsolvedMsgSearch, do search done", zap.Int("num of searchMsg", len(searchMsg)))
		}
	}
}

// TODO:: cache map[dsl]plan
// TODO: reBatched search requests
func (s *searchCollection) search(searchMsg *msgstream.SearchMsg) error {
	sp, ctx := trace.StartSpanFromContext(searchMsg.TraceCtx())
	defer sp.Finish()
	searchMsg.SetTraceCtx(ctx)
	searchTimestamp := searchMsg.Base.Timestamp

	collectionID := searchMsg.CollectionID
	collection, err := s.historicalReplica.getCollectionByID(collectionID)
	if err != nil {
		return err
	}
	var plan *Plan
	if searchMsg.GetDslType() == commonpb.DslType_BoolExprV1 {
		expr := searchMsg.SerializedExprPlan
		plan, err = createPlanByExpr(*collection, expr)
		if err != nil {
			return err
		}
	} else {
		dsl := searchMsg.Dsl
		plan, err = createPlan(*collection, dsl)
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

	searchResults := make([]*SearchResult, 0)
	matchedSegments := make([]*Segment, 0)

	//log.Debug("search msg's partitionID = ", partitionIDsInQuery)
	partitionIDsInHistoricalCol, err1 := s.historicalReplica.getPartitionIDs(collectionID)
	partitionIDsInStreamingCol, err2 := s.streamingReplica.getPartitionIDs(collectionID)

	if err1 != nil && err2 != nil {
		return err2
	}
	var searchPartitionIDsInHistorical []UniqueID
	var searchPartitionIDsInStreaming []UniqueID
	partitionIDsInQuery := searchMsg.PartitionIDs
	if len(partitionIDsInQuery) == 0 {
		if len(partitionIDsInHistoricalCol) == 0 {
			return errors.New("none of this collection's partition has been loaded")
		}
		searchPartitionIDsInHistorical = partitionIDsInHistoricalCol
		searchPartitionIDsInStreaming = partitionIDsInStreamingCol
	} else {
		for _, id := range partitionIDsInQuery {
			_, err1 = s.historicalReplica.getPartitionByID(id)
			if err1 == nil {
				searchPartitionIDsInHistorical = append(searchPartitionIDsInHistorical, id)
			}
			_, err2 = s.streamingReplica.getPartitionByID(id)
			if err2 == nil {
				searchPartitionIDsInStreaming = append(searchPartitionIDsInStreaming, id)
			}
			if err1 != nil && err2 != nil {
				return err2
			}
		}
	}

	if searchMsg.GetDslType() == commonpb.DslType_BoolExprV1 {
		sp.LogFields(oplog.String("statistical time", "stats start"),
			oplog.Object("nq", queryNum),
			oplog.Object("expr", searchMsg.SerializedExprPlan))
	} else {
		sp.LogFields(oplog.String("statistical time", "stats start"),
			oplog.Object("nq", queryNum),
			oplog.Object("dsl", searchMsg.Dsl))
	}

	sealedSegmentSearched := make([]UniqueID, 0)
	for _, partitionID := range searchPartitionIDsInHistorical {
		segmentIDs, err := s.historicalReplica.getSegmentIDs(partitionID)
		if err != nil {
			return err
		}
		for _, segmentID := range segmentIDs {
			segment, err := s.historicalReplica.getSegmentByID(segmentID)
			if err != nil {
				return err
			}
			searchResult, err := segment.segmentSearch(plan, searchRequests, []Timestamp{searchTimestamp})

			if err != nil {
				return err
			}
			searchResults = append(searchResults, searchResult)
			matchedSegments = append(matchedSegments, segment)
			sealedSegmentSearched = append(sealedSegmentSearched, segmentID)
		}
	}

	//TODO:: get searched channels
	for _, partitionID := range searchPartitionIDsInStreaming {
		segmentIDs, err := s.streamingReplica.getSegmentIDs(partitionID)
		if err != nil {
			return err
		}
		for _, segmentID := range segmentIDs {
			segment, err := s.streamingReplica.getSegmentByID(segmentID)
			if err != nil {
				return err
			}
			searchResult, err := segment.segmentSearch(plan, searchRequests, []Timestamp{searchTimestamp})

			if err != nil {
				return err
			}
			searchResults = append(searchResults, searchResult)
			matchedSegments = append(matchedSegments, segment)
		}
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
					Status:          &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
					ResultChannelID: searchMsg.ResultChannelID,
					Hits:            nilHits,
					MetricType:      plan.getMetricType(),
				},
			}
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
				MetricType:               plan.getMetricType(),
				SealedSegmentIDsSearched: sealedSegmentSearched,
				ChannelIDsSearched:       collection.getWatchedDmChannels(),
				//TODO:: get global sealed segment from etcd
				GlobalSealedSegmentIDs: sealedSegmentSearched,
			},
		}

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
		zap.Int64("msgID", msg.ID()),
		zap.Int64("collectionID", collectionID))
	span, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	defer span.Finish()
	msg.SetTraceCtx(ctx)
	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, msg)
	err := s.searchResultMsgStream.Produce(&msgPack)
	if err != nil {
		log.Error(err.Error())
	} else {
		log.Debug("publish search result done",
			zap.Int64("msgID", msg.ID()),
			zap.Int64("collectionID", collectionID))
	}
	return err
}

func (s *searchCollection) publishFailedSearchResult(searchMsg *msgstream.SearchMsg, errMsg string) error {
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
			Hits:            [][]byte{},
		},
	}

	msgPack.Msgs = append(msgPack.Msgs, searchResultMsg)
	err := s.searchResultMsgStream.Produce(&msgPack)
	if err != nil {
		return err
	}

	return nil
}
