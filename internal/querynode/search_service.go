package querynode

import "C"
import (
	"context"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
)

type searchService struct {
	ctx    context.Context
	wait   sync.WaitGroup
	cancel context.CancelFunc

	replica      ReplicaInterface
	tSafeMutex   *sync.Mutex
	tSafeWatcher map[UniqueID]*tSafeWatcher

	serviceableTimeMutex sync.Mutex // guards serviceableTime
	serviceableTime      map[UniqueID]Timestamp

	msgBuffer             chan *msgstream.SearchMsg
	unsolvedMsg           []*msgstream.SearchMsg
	searchMsgStream       msgstream.MsgStream
	searchResultMsgStream msgstream.MsgStream
	queryNodeID           UniqueID
}

type ResultEntityIds []UniqueID

func newSearchService(ctx context.Context, replica ReplicaInterface, factory msgstream.Factory) *searchService {
	receiveBufSize := Params.SearchReceiveBufSize

	searchStream, _ := factory.NewQueryMsgStream(ctx)
	searchResultStream, _ := factory.NewQueryMsgStream(ctx)

	// query node doesn't need to consumer any search or search result channel actively.
	consumeChannels := Params.SearchChannelNames
	consumeSubName := Params.MsgChannelSubName
	searchStream.AsConsumer(consumeChannels, consumeSubName)
	log.Debug("querynode AsConsumer: " + strings.Join(consumeChannels, ", ") + " : " + consumeSubName)
	producerChannels := Params.SearchResultChannelNames
	searchResultStream.AsProducer(producerChannels)
	log.Debug("querynode AsProducer: " + strings.Join(producerChannels, ", "))

	searchServiceCtx, searchServiceCancel := context.WithCancel(ctx)
	msgBuffer := make(chan *msgstream.SearchMsg, receiveBufSize)
	unsolvedMsg := make([]*msgstream.SearchMsg, 0)
	return &searchService{
		ctx:             searchServiceCtx,
		cancel:          searchServiceCancel,
		serviceableTime: make(map[UniqueID]Timestamp),
		msgBuffer:       msgBuffer,
		unsolvedMsg:     unsolvedMsg,

		replica:      replica,
		tSafeMutex:   &sync.Mutex{},
		tSafeWatcher: make(map[UniqueID]*tSafeWatcher),

		searchMsgStream:       searchStream,
		searchResultMsgStream: searchResultStream,
		queryNodeID:           Params.QueryNodeID,
	}
}

func (ss *searchService) start() {
	ss.searchMsgStream.Start()
	ss.searchResultMsgStream.Start()
	ss.wait.Add(2)
	go ss.receiveSearchMsg()
	go ss.doUnsolvedMsgSearch()
	ss.wait.Wait()
}

func (ss *searchService) close() {
	if ss.searchMsgStream != nil {
		ss.searchMsgStream.Close()
	}
	if ss.searchResultMsgStream != nil {
		ss.searchResultMsgStream.Close()
	}
	ss.cancel()
}

func (ss *searchService) register(collectionID UniqueID) {
	tSafe := ss.replica.getTSafe(collectionID)
	ss.tSafeMutex.Lock()
	ss.tSafeWatcher[collectionID] = newTSafeWatcher()
	ss.tSafeMutex.Unlock()
	tSafe.registerTSafeWatcher(ss.tSafeWatcher[collectionID])
}

func (ss *searchService) waitNewTSafe(collectionID UniqueID) (Timestamp, error) {
	// block until dataSyncService updating tSafe
	ss.tSafeWatcher[collectionID].hasUpdate()
	ts := ss.replica.getTSafe(collectionID)
	if ts != nil {
		return ts.get(), nil
	}
	return 0, errors.New("tSafe closed, collectionID =" + fmt.Sprintln(collectionID))
}

func (ss *searchService) getServiceableTime(collectionID UniqueID) Timestamp {
	ss.serviceableTimeMutex.Lock()
	defer ss.serviceableTimeMutex.Unlock()
	//t, ok := ss.serviceableTime[collectionID]
	//if !ok {
	//	return 0, errors.New("cannot found")
	//}
	return ss.serviceableTime[collectionID]
}

func (ss *searchService) setServiceableTime(collectionID UniqueID, t Timestamp) {
	ss.serviceableTimeMutex.Lock()
	// hard code gracefultime to 1 second
	// TODO: use config to set gracefultime
	ss.serviceableTime[collectionID] = t + 1000*1000*1000
	ss.serviceableTimeMutex.Unlock()
}

func (ss *searchService) collectionCheck(collectionID UniqueID) error {
	// check if collection exists
	if _, ok := ss.tSafeWatcher[collectionID]; !ok {
		err := errors.New("no collection found, collectionID = " + strconv.FormatInt(collectionID, 10))
		log.Error(err.Error())
		return err
	}
	return nil
}

func (ss *searchService) emptySearch(searchMsg *msgstream.SearchMsg) {
	err := ss.search(searchMsg)
	if err != nil {
		log.Error(err.Error())
		err2 := ss.publishFailedSearchResult(searchMsg, err.Error())
		if err2 != nil {
			log.Error("publish FailedSearchResult failed", zap.Error(err2))
		}
	}
}

func (ss *searchService) receiveSearchMsg() {
	defer ss.wait.Done()
	for {
		select {
		case <-ss.ctx.Done():
			return
		default:
			msgPack, _ := ss.searchMsgStream.Consume()
			if msgPack == nil || len(msgPack.Msgs) <= 0 {
				continue
			}
			searchNum := 0
			for _, msg := range msgPack.Msgs {
				sm, ok := msg.(*msgstream.SearchMsg)
				if !ok {
					continue
				}
				err := ss.collectionCheck(sm.CollectionID)
				if err != nil {
					ss.emptySearch(sm)
					searchNum++
					continue
				}
				serviceTime := ss.getServiceableTime(sm.CollectionID)
				if msg.BeginTs() > serviceTime {
					ss.msgBuffer <- sm
					continue
				}
				err = ss.search(sm)
				if err != nil {
					log.Error(err.Error())
					err2 := ss.publishFailedSearchResult(sm, err.Error())
					if err2 != nil {
						log.Error("publish FailedSearchResult failed", zap.Error(err2))
					}
				}
				searchNum++
			}
			log.Debug("ReceiveSearchMsg, do search done", zap.Int("num of searchMsg", searchNum))
		}
	}
}

func (ss *searchService) doUnsolvedMsgSearch() {
	defer ss.wait.Done()
	for {
		select {
		case <-ss.ctx.Done():
			return
		default:
			searchMsg := make([]*msgstream.SearchMsg, 0)
			tempMsg := make([]*msgstream.SearchMsg, 0)
			tempMsg = append(tempMsg, ss.unsolvedMsg...)
			ss.unsolvedMsg = ss.unsolvedMsg[:0]

			serviceTimeTmpTable := make(map[UniqueID]Timestamp)

			searchNum := 0
			for _, sm := range tempMsg {
				err := ss.collectionCheck(sm.CollectionID)
				if err != nil {
					ss.emptySearch(sm)
					searchNum++
					continue
				}
				_, ok := serviceTimeTmpTable[sm.CollectionID]
				if !ok {
					serviceTime, err := ss.waitNewTSafe(sm.CollectionID)
					if err != nil {
						// TODO: emptySearch or continue, note: collection has been released
						continue
					}
					ss.setServiceableTime(sm.CollectionID, serviceTime)
					serviceTimeTmpTable[sm.CollectionID] = serviceTime
				}
				if sm.EndTs() <= serviceTimeTmpTable[sm.CollectionID] {
					searchMsg = append(searchMsg, sm)
					continue
				}
				ss.unsolvedMsg = append(ss.unsolvedMsg, sm)
			}

			for {
				msgBufferLength := len(ss.msgBuffer)
				if msgBufferLength <= 0 {
					break
				}
				sm := <-ss.msgBuffer
				err := ss.collectionCheck(sm.CollectionID)
				if err != nil {
					ss.emptySearch(sm)
					searchNum++
					continue
				}
				_, ok := serviceTimeTmpTable[sm.CollectionID]
				if !ok {
					serviceTime, err := ss.waitNewTSafe(sm.CollectionID)
					if err != nil {
						// TODO: emptySearch or continue, note: collection has been released
						continue
					}
					ss.setServiceableTime(sm.CollectionID, serviceTime)
					serviceTimeTmpTable[sm.CollectionID] = serviceTime
				}
				if sm.EndTs() <= serviceTimeTmpTable[sm.CollectionID] {
					searchMsg = append(searchMsg, sm)
					continue
				}
				ss.unsolvedMsg = append(ss.unsolvedMsg, sm)
			}

			if len(searchMsg) <= 0 {
				continue
			}
			for _, sm := range searchMsg {
				err := ss.search(sm)
				if err != nil {
					log.Error(err.Error())
					err2 := ss.publishFailedSearchResult(sm, err.Error())
					if err2 != nil {
						log.Error("publish FailedSearchResult failed", zap.Error(err2))
					}
				}
				searchNum++
			}
			log.Debug("doUnsolvedMsgSearch, do search done", zap.Int("num of searchMsg", searchNum))
		}
	}
}

// TODO:: cache map[dsl]plan
// TODO: reBatched search requests
func (ss *searchService) search(searchMsg *msgstream.SearchMsg) error {
	searchTimestamp := searchMsg.Base.Timestamp
	var queryBlob = searchMsg.Query.Value
	query := milvuspb.SearchRequest{}
	err := proto.Unmarshal(queryBlob, &query)
	if err != nil {
		return errors.New("unmarshal query failed")
	}
	collectionID := searchMsg.CollectionID
	collection, err := ss.replica.getCollectionByID(collectionID)
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
	partitionIDsInCol, err := ss.replica.getPartitionIDs(collectionID)
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
			_, err2 := ss.replica.getPartitionByID(id)
			if err2 != nil {
				return err2
			}
		}
		searchPartitionIDs = partitionIDsInQuery
	}

	for _, partitionID := range searchPartitionIDs {
		segmentIDs, err := ss.replica.getSegmentIDs(partitionID)
		if err != nil {
			return err
		}
		for _, segmentID := range segmentIDs {
			//log.Debug("dsl = ", dsl)
			segment, err := ss.replica.getSegmentByID(segmentID)
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
				BaseMsg: msgstream.BaseMsg{HashValues: []uint32{uint32(resultChannelInt)}},
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
			err = ss.publishSearchResult(searchResultMsg)
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
			BaseMsg: msgstream.BaseMsg{HashValues: []uint32{uint32(resultChannelInt)}},
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
		err = ss.publishSearchResult(searchResultMsg)
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

func (ss *searchService) publishSearchResult(msg msgstream.TsMsg) error {
	// span, ctx := opentracing.StartSpanFromContext(msg.GetMsgContext(), "publish search result")
	// defer span.Finish()
	// msg.SetMsgContext(ctx)
	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, msg)
	err := ss.searchResultMsgStream.Produce(context.TODO(), &msgPack)
	return err
}

func (ss *searchService) publishFailedSearchResult(searchMsg *msgstream.SearchMsg, errMsg string) error {
	// span, ctx := opentracing.StartSpanFromContext(msg.GetMsgContext(), "receive search msg")
	// defer span.Finish()
	// msg.SetMsgContext(ctx)
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
	err := ss.searchResultMsgStream.Produce(context.TODO(), &msgPack)
	if err != nil {
		return err
	}

	return nil
}
