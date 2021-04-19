package querynode

import "C"
import (
	"context"
	"errors"
	"fmt"
	"log"
	"regexp"
	"sync"

	"github.com/golang/protobuf/proto"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
)

type searchService struct {
	ctx    context.Context
	wait   sync.WaitGroup
	cancel context.CancelFunc

	replica      collectionReplica
	tSafeWatcher *tSafeWatcher

	serviceableTime      Timestamp
	serviceableTimeMutex sync.Mutex

	msgBuffer             chan msgstream.TsMsg
	unsolvedMsg           []msgstream.TsMsg
	searchMsgStream       msgstream.MsgStream
	searchResultMsgStream msgstream.MsgStream
	queryNodeID           UniqueID
}

type ResultEntityIds []UniqueID

func newSearchService(ctx context.Context, replica collectionReplica) *searchService {
	receiveBufSize := Params.SearchReceiveBufSize
	pulsarBufSize := Params.SearchPulsarBufSize

	msgStreamURL := Params.PulsarAddress

	consumeChannels := Params.SearchChannelNames
	consumeSubName := Params.MsgChannelSubName
	searchStream := msgstream.NewPulsarMsgStream(ctx, receiveBufSize)
	searchStream.SetPulsarClient(msgStreamURL)
	unmarshalDispatcher := msgstream.NewUnmarshalDispatcher()
	searchStream.CreatePulsarConsumers(consumeChannels, consumeSubName, unmarshalDispatcher, pulsarBufSize)
	var inputStream msgstream.MsgStream = searchStream

	producerChannels := Params.SearchResultChannelNames
	searchResultStream := msgstream.NewPulsarMsgStream(ctx, receiveBufSize)
	searchResultStream.SetPulsarClient(msgStreamURL)
	searchResultStream.CreatePulsarProducers(producerChannels)
	var outputStream msgstream.MsgStream = searchResultStream

	searchServiceCtx, searchServiceCancel := context.WithCancel(ctx)
	msgBuffer := make(chan msgstream.TsMsg, receiveBufSize)
	unsolvedMsg := make([]msgstream.TsMsg, 0)
	return &searchService{
		ctx:             searchServiceCtx,
		cancel:          searchServiceCancel,
		serviceableTime: Timestamp(0),
		msgBuffer:       msgBuffer,
		unsolvedMsg:     unsolvedMsg,

		replica:      replica,
		tSafeWatcher: newTSafeWatcher(),

		searchMsgStream:       inputStream,
		searchResultMsgStream: outputStream,
		queryNodeID:           Params.QueryNodeID,
	}
}

func (ss *searchService) start() {
	ss.searchMsgStream.Start()
	ss.searchResultMsgStream.Start()
	ss.register()
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

func (ss *searchService) register() {
	tSafe := ss.replica.getTSafe()
	tSafe.registerTSafeWatcher(ss.tSafeWatcher)
}

func (ss *searchService) waitNewTSafe() Timestamp {
	// block until dataSyncService updating tSafe
	ss.tSafeWatcher.hasUpdate()
	timestamp := ss.replica.getTSafe().get()
	return timestamp
}

func (ss *searchService) getServiceableTime() Timestamp {
	ss.serviceableTimeMutex.Lock()
	defer ss.serviceableTimeMutex.Unlock()
	return ss.serviceableTime
}

func (ss *searchService) setServiceableTime(t Timestamp) {
	ss.serviceableTimeMutex.Lock()
	// TODO:: add gracefulTime
	ss.serviceableTime = t
	ss.serviceableTimeMutex.Unlock()
}

func (ss *searchService) receiveSearchMsg() {
	defer ss.wait.Done()
	for {
		select {
		case <-ss.ctx.Done():
			return
		default:
			msgPack := ss.searchMsgStream.Consume()
			if msgPack == nil || len(msgPack.Msgs) <= 0 {
				continue
			}
			searchMsg := make([]msgstream.TsMsg, 0)
			serverTime := ss.getServiceableTime()
			for i := range msgPack.Msgs {
				if msgPack.Msgs[i].BeginTs() > serverTime {
					ss.msgBuffer <- msgPack.Msgs[i]
					continue
				}
				searchMsg = append(searchMsg, msgPack.Msgs[i])
			}
			for _, msg := range searchMsg {
				err := ss.search(msg)
				if err != nil {
					log.Println(err)
					err2 := ss.publishFailedSearchResult(msg, err.Error())
					if err2 != nil {
						log.Println("publish FailedSearchResult failed, error message: ", err2)
					}
				}
			}
			log.Println("ReceiveSearchMsg, do search done, num of searchMsg = ", len(searchMsg))
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
			serviceTime := ss.waitNewTSafe()
			ss.setServiceableTime(serviceTime)
			searchMsg := make([]msgstream.TsMsg, 0)
			tempMsg := make([]msgstream.TsMsg, 0)
			tempMsg = append(tempMsg, ss.unsolvedMsg...)
			ss.unsolvedMsg = ss.unsolvedMsg[:0]
			for _, msg := range tempMsg {
				if msg.EndTs() <= serviceTime {
					searchMsg = append(searchMsg, msg)
					continue
				}
				ss.unsolvedMsg = append(ss.unsolvedMsg, msg)
			}

			for {
				msgBufferLength := len(ss.msgBuffer)
				if msgBufferLength <= 0 {
					break
				}
				msg := <-ss.msgBuffer
				if msg.EndTs() <= serviceTime {
					searchMsg = append(searchMsg, msg)
					continue
				}
				ss.unsolvedMsg = append(ss.unsolvedMsg, msg)
			}

			if len(searchMsg) <= 0 {
				continue
			}
			for _, msg := range searchMsg {
				err := ss.search(msg)
				if err != nil {
					log.Println(err)
					err2 := ss.publishFailedSearchResult(msg, err.Error())
					if err2 != nil {
						log.Println("publish FailedSearchResult failed, error message: ", err2)
					}
				}
			}
			log.Println("doUnsolvedMsgSearch, do search done, num of searchMsg = ", len(searchMsg))
		}
	}
}

// TODO:: cache map[dsl]plan
// TODO: reBatched search requests
func (ss *searchService) search(msg msgstream.TsMsg) error {
	searchMsg, ok := msg.(*msgstream.SearchMsg)
	if !ok {
		return errors.New("invalid request type = " + string(msg.Type()))
	}

	searchTimestamp := searchMsg.Timestamp
	var queryBlob = searchMsg.Query.Value
	query := servicepb.Query{}
	err := proto.Unmarshal(queryBlob, &query)
	if err != nil {
		return errors.New("unmarshal query failed")
	}
	collectionName := query.CollectionName
	partitionTagsInQuery := query.PartitionTags
	collection, err := ss.replica.getCollectionByName(collectionName)
	if err != nil {
		return err
	}
	collectionID := collection.ID()
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

	fmt.Println("search msg's partitionTag = ", partitionTagsInQuery)

	var partitionTagsInCol []string
	for _, partition := range collection.partitions {
		partitionTag := partition.partitionTag
		partitionTagsInCol = append(partitionTagsInCol, partitionTag)
	}
	var searchPartitionTag []string
	if len(partitionTagsInQuery) == 0 {
		searchPartitionTag = partitionTagsInCol
	} else {
		for _, tag := range partitionTagsInCol {
			for _, toMatchTag := range partitionTagsInQuery {
				re := regexp.MustCompile("^" + toMatchTag + "$")
				if re.MatchString(tag) {
					searchPartitionTag = append(searchPartitionTag, tag)
				}
			}
		}
	}

	for _, partitionTag := range searchPartitionTag {
		partition, _ := ss.replica.getPartitionByTag(collectionID, partitionTag)
		for _, segment := range partition.segments {
			//fmt.Println("dsl = ", dsl)

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
			hit := &servicepb.Hits{}
			for i := 0; i < int(nq); i++ {
				bs, err := proto.Marshal(hit)
				if err != nil {
					return err
				}
				nilHits[i] = bs
			}
			var results = internalpb.SearchResult{
				MsgType:         internalpb.MsgType_kSearchResult,
				Status:          &commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS},
				ReqID:           searchMsg.ReqID,
				ProxyID:         searchMsg.ProxyID,
				QueryNodeID:     ss.queryNodeID,
				Timestamp:       searchTimestamp,
				ResultChannelID: searchMsg.ResultChannelID,
				Hits:            nilHits,
			}
			searchResultMsg := &msgstream.SearchResultMsg{
				BaseMsg:      msgstream.BaseMsg{HashValues: []uint32{uint32(searchMsg.ResultChannelID)}},
				SearchResult: results,
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
			//unMarshaledHit := servicepb.Hits{}
			//err = proto.Unmarshal(marshaledHit, &unMarshaledHit)
			//if err != nil {
			//	return err
			//}
			//fmt.Println("hits msg  = ", unMarshaledHit)
			offset += len
		}
		var results = internalpb.SearchResult{
			MsgType:         internalpb.MsgType_kSearchResult,
			Status:          &commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS},
			ReqID:           searchMsg.ReqID,
			ProxyID:         searchMsg.ProxyID,
			QueryNodeID:     searchMsg.ProxyID,
			Timestamp:       searchTimestamp,
			ResultChannelID: searchMsg.ResultChannelID,
			Hits:            hits,
			MetricType:      plan.getMetricType(),
		}
		searchResultMsg := &msgstream.SearchResultMsg{
			BaseMsg:      msgstream.BaseMsg{HashValues: []uint32{uint32(searchMsg.ResultChannelID)}},
			SearchResult: results,
		}
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
	fmt.Println("Public SearchResult", msg.HashKeys())
	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, msg)
	err := ss.searchResultMsgStream.Produce(&msgPack)
	return err
}

func (ss *searchService) publishFailedSearchResult(msg msgstream.TsMsg, errMsg string) error {
	fmt.Println("Public fail SearchResult!")
	msgPack := msgstream.MsgPack{}
	searchMsg, ok := msg.(*msgstream.SearchMsg)
	if !ok {
		return errors.New("invalid request type = " + string(msg.Type()))
	}
	var results = internalpb.SearchResult{
		MsgType:         internalpb.MsgType_kSearchResult,
		Status:          &commonpb.Status{ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR, Reason: errMsg},
		ReqID:           searchMsg.ReqID,
		ProxyID:         searchMsg.ProxyID,
		QueryNodeID:     searchMsg.ProxyID,
		Timestamp:       searchMsg.Timestamp,
		ResultChannelID: searchMsg.ResultChannelID,
		Hits:            [][]byte{},
	}

	tsMsg := &msgstream.SearchResultMsg{
		BaseMsg:      msgstream.BaseMsg{HashValues: []uint32{uint32(searchMsg.ResultChannelID)}},
		SearchResult: results,
	}
	msgPack.Msgs = append(msgPack.Msgs, tsMsg)
	err := ss.searchResultMsgStream.Produce(&msgPack)
	if err != nil {
		return err
	}

	return nil
}
