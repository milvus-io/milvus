package querynode

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/util/trace"
)

type searchCollection struct {
	releaseCtx context.Context
	cancel     context.CancelFunc

	collectionID UniqueID
	replica      ReplicaInterface

	msgBuffer   chan *msgstream.SearchMsg
	unsolvedMsg []*msgstream.SearchMsg

	tSafeMutex   sync.Mutex
	tSafeWatcher *tSafeWatcher

	serviceableTimeMutex sync.Mutex // guards serviceableTime
	serviceableTime      Timestamp

	searchResultMsgStream msgstream.MsgStream
}

type ResultEntityIds []UniqueID

func newSearchCollection(releaseCtx context.Context, cancel context.CancelFunc, collectionID UniqueID, replica ReplicaInterface, searchResultStream msgstream.MsgStream) *searchCollection {
	receiveBufSize := Params.SearchReceiveBufSize
	msgBuffer := make(chan *msgstream.SearchMsg, receiveBufSize)
	unsolvedMsg := make([]*msgstream.SearchMsg, 0)

	sc := &searchCollection{
		releaseCtx: releaseCtx,
		cancel:     cancel,

		collectionID: collectionID,
		replica:      replica,

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
	s.replica.addTSafe(collectionID)
	tSafe := s.replica.getTSafe(collectionID)
	s.tSafeMutex.Lock()
	s.tSafeWatcher = newTSafeWatcher()
	s.tSafeMutex.Unlock()
	tSafe.registerTSafeWatcher(s.tSafeWatcher)
}

func (s *searchCollection) waitNewTSafe() (Timestamp, error) {
	// block until dataSyncService updating tSafe
	s.tSafeWatcher.hasUpdate()
	ts := s.replica.getTSafe(s.collectionID)
	if ts != nil {
		return ts.get(), nil
	}
	return 0, errors.New("tSafe closed, collectionID =" + fmt.Sprintln(s.collectionID))
}

func (s *searchCollection) getServiceableTime() Timestamp {
	s.serviceableTimeMutex.Lock()
	defer s.serviceableTimeMutex.Unlock()
	return s.serviceableTime
}

func (s *searchCollection) setServiceableTime(t Timestamp) {
	s.serviceableTimeMutex.Lock()
	// hard code graceful time to 1 second
	// TODO: use config to set graceful time
	s.serviceableTime = t + 1000*1000*1000
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
			log.Debug("stop receiveSearchMsg", zap.Int64("collectionID", s.collectionID))
			return
		case sm := <-s.msgBuffer:
			serviceTime := s.getServiceableTime()
			if sm.BeginTs() > serviceTime {
				s.unsolvedMsg = append(s.unsolvedMsg, sm)
				continue
			}
			err := s.search(sm)
			if err != nil {
				log.Error(err.Error())
				err2 := s.publishFailedSearchResult(sm, err.Error())
				if err2 != nil {
					log.Error("publish FailedSearchResult failed", zap.Error(err2))
				}
			}
			log.Debug("ReceiveSearchMsg, do search done, num of searchMsg = 1")
		}
	}
}

func (s *searchCollection) doUnsolvedMsgSearch() {
	for {
		select {
		case <-s.releaseCtx.Done():
			log.Debug("stop doUnsolvedMsgSearch", zap.Int64("collectionID", s.collectionID))
			return
		default:
			serviceTime, err := s.waitNewTSafe()
			s.setServiceableTime(serviceTime)
			if err != nil {
				// TODO: emptySearch or continue, note: collection has been released
				continue
			}

			searchMsg := make([]*msgstream.SearchMsg, 0)
			tempMsg := s.unsolvedMsg
			s.unsolvedMsg = s.unsolvedMsg[:0]

			for _, sm := range tempMsg {
				if sm.EndTs() <= serviceTime {
					searchMsg = append(searchMsg, sm)
					continue
				}
				s.unsolvedMsg = append(s.unsolvedMsg, sm)
			}

			if len(searchMsg) <= 0 {
				continue
			}
			for _, sm := range searchMsg {
				sp, ctx := trace.StartSpanFromContext(sm.TraceCtx())
				sm.SetTraceCtx(ctx)
				err := s.search(sm)
				if err != nil {
					log.Error(err.Error())
					err2 := s.publishFailedSearchResult(sm, err.Error())
					if err2 != nil {
						log.Error("publish FailedSearchResult failed", zap.Error(err2))
					}
				}
				sp.Finish()
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
	var queryBlob = searchMsg.Query.Value
	query := milvuspb.SearchRequest{}
	err := proto.Unmarshal(queryBlob, &query)
	if err != nil {
		return errors.New("unmarshal query failed")
	}
	collectionID := searchMsg.CollectionID
	collection, err := s.replica.getCollectionByID(collectionID)
	if err != nil {
		return err
	}
	dsl := query.Dsl
	plan, err := createPlan(*collection, dsl)
	if err != nil {
		return err
	}
	placeHolderGroupBlob := query.PlaceholderGroup
	placeholderGroup, err := parserPlaceholderGroup(plan, placeHolderGroupBlob)
	if err != nil {
		return err
	}
	placeholderGroups := make([]*PlaceholderGroup, 0)
	placeholderGroups = append(placeholderGroups, placeholderGroup)

	searchResults := make([]*SearchResult, 0)
	matchedSegments := make([]*Segment, 0)

	//log.Debug("search msg's partitionID = ", partitionIDsInQuery)
	partitionIDsInCol, err := s.replica.getPartitionIDs(collectionID)
	if err != nil {
		return err
	}
	var searchPartitionIDs []UniqueID
	partitionIDsInQuery := searchMsg.PartitionIDs
	if len(partitionIDsInQuery) == 0 {
		if len(partitionIDsInCol) == 0 {
			return errors.New("none of this collection's partition has been loaded")
		}
		searchPartitionIDs = partitionIDsInCol
	} else {
		for _, id := range partitionIDsInQuery {
			_, err2 := s.replica.getPartitionByID(id)
			if err2 != nil {
				return err2
			}
		}
		searchPartitionIDs = partitionIDsInQuery
	}

	for _, partitionID := range searchPartitionIDs {
		segmentIDs, err := s.replica.getSegmentIDs(partitionID)
		if err != nil {
			return err
		}
		for _, segmentID := range segmentIDs {
			//log.Debug("dsl = ", dsl)
			segment, err := s.replica.getSegmentByID(segmentID)
			if err != nil {
				return err
			}
			searchResult, err := segment.segmentSearch(plan, placeholderGroups, []Timestamp{searchTimestamp})

			if err != nil {
				return err
			}
			searchResults = append(searchResults, searchResult)
			matchedSegments = append(matchedSegments, segment)
		}
	}

	if len(searchResults) <= 0 {
		for _, group := range placeholderGroups {
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
			resultChannelInt, _ := strconv.ParseInt(searchMsg.ResultChannelID, 10, 64)
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
			err = s.publishSearchResult(searchResultMsg)
			if err != nil {
				return err
			}
			return nil
		}
	}

	inReduced := make([]bool, len(searchResults))
	numSegment := int64(len(searchResults))
	err2 := reduceSearchResults(searchResults, numSegment, inReduced)
	if err2 != nil {
		return err2
	}
	err = fillTargetEntry(plan, searchResults, matchedSegments, inReduced)
	if err != nil {
		return err
	}
	marshaledHits, err := reorganizeQueryResults(plan, placeholderGroups, searchResults, numSegment, inReduced)
	if err != nil {
		return err
	}
	hitsBlob, err := marshaledHits.getHitsBlob()
	if err != nil {
		return err
	}

	var offset int64 = 0
	for index := range placeholderGroups {
		hitBlobSizePeerQuery, err := marshaledHits.hitBlobSizeInGroup(int64(index))
		if err != nil {
			return err
		}
		hits := make([][]byte, 0)
		for _, len := range hitBlobSizePeerQuery {
			hits = append(hits, hitsBlob[offset:offset+len])
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
		resultChannelInt, _ := strconv.ParseInt(searchMsg.ResultChannelID, 10, 64)
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
				Hits:            hits,
				MetricType:      plan.getMetricType(),
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
		err = s.publishSearchResult(searchResultMsg)
		if err != nil {
			return err
		}
	}

	deleteSearchResults(searchResults)
	deleteMarshaledHits(marshaledHits)
	plan.delete()
	placeholderGroup.delete()
	return nil
}

func (s *searchCollection) publishSearchResult(msg msgstream.TsMsg) error {
	span, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	defer span.Finish()
	msg.SetTraceCtx(ctx)
	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, msg)
	err := s.searchResultMsgStream.Produce(&msgPack)
	return err
}

func (s *searchCollection) publishFailedSearchResult(searchMsg *msgstream.SearchMsg, errMsg string) error {
	span, ctx := trace.StartSpanFromContext(searchMsg.TraceCtx())
	defer span.Finish()
	searchMsg.SetTraceCtx(ctx)
	//log.Debug("Public fail SearchResult!")
	msgPack := msgstream.MsgPack{}

	resultChannelInt, _ := strconv.ParseInt(searchMsg.ResultChannelID, 10, 64)
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
