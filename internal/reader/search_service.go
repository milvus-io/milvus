package reader

import "C"
import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"sync"

	"github.com/golang/protobuf/proto"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
)

type searchService struct {
	ctx         context.Context
	wait        sync.WaitGroup
	cancel      context.CancelFunc
	msgBuffer   chan msgstream.TsMsg
	unsolvedMsg []msgstream.TsMsg

	replica      *collectionReplica
	tSafeWatcher *tSafeWatcher

	searchMsgStream       *msgstream.MsgStream
	searchResultMsgStream *msgstream.MsgStream
}

type ResultEntityIds []UniqueID

func newSearchService(ctx context.Context, replica *collectionReplica) *searchService {
	receiveBufSize := Params.searchReceiveBufSize()
	pulsarBufSize := Params.searchPulsarBufSize()

	msgStreamURL, err := Params.pulsarAddress()
	if err != nil {
		log.Fatal(err)
	}

	consumeChannels := Params.searchChannelNames()
	consumeSubName := "subSearch"
	searchStream := msgstream.NewPulsarMsgStream(ctx, receiveBufSize)
	searchStream.SetPulsarClient(msgStreamURL)
	unmarshalDispatcher := msgstream.NewUnmarshalDispatcher()
	searchStream.CreatePulsarConsumers(consumeChannels, consumeSubName, unmarshalDispatcher, pulsarBufSize)
	var inputStream msgstream.MsgStream = searchStream

	producerChannels := Params.searchResultChannelNames()
	searchResultStream := msgstream.NewPulsarMsgStream(ctx, receiveBufSize)
	searchResultStream.SetPulsarClient(msgStreamURL)
	searchResultStream.CreatePulsarProducers(producerChannels)
	var outputStream msgstream.MsgStream = searchResultStream

	searchServiceCtx, searchServiceCancel := context.WithCancel(ctx)
	msgBuffer := make(chan msgstream.TsMsg, receiveBufSize)
	unsolvedMsg := make([]msgstream.TsMsg, 0)
	return &searchService{
		ctx:         searchServiceCtx,
		cancel:      searchServiceCancel,
		msgBuffer:   msgBuffer,
		unsolvedMsg: unsolvedMsg,

		replica:      replica,
		tSafeWatcher: newTSafeWatcher(),

		searchMsgStream:       &inputStream,
		searchResultMsgStream: &outputStream,
	}
}

func (ss *searchService) start() {
	(*ss.searchMsgStream).Start()
	(*ss.searchResultMsgStream).Start()
	ss.wait.Add(2)
	go ss.receiveSearchMsg()
	go ss.startSearchService()
	ss.wait.Wait()
}

func (ss *searchService) close() {
	(*ss.searchMsgStream).Close()
	(*ss.searchResultMsgStream).Close()
	ss.cancel()
}

func (ss *searchService) register() {
	tSafe := (*(ss.replica)).getTSafe()
	(*tSafe).registerTSafeWatcher(ss.tSafeWatcher)
}

func (ss *searchService) waitNewTSafe() Timestamp {
	// block until dataSyncService updating tSafe
	ss.tSafeWatcher.hasUpdate()
	timestamp := (*(*ss.replica).getTSafe()).get()
	return timestamp
}

func (ss *searchService) receiveSearchMsg() {
	defer ss.wait.Done()
	for {
		select {
		case <-ss.ctx.Done():
			return
		default:
			msgPack := (*ss.searchMsgStream).Consume()
			if msgPack == nil || len(msgPack.Msgs) <= 0 {
				continue
			}
			for i := range msgPack.Msgs {
				ss.msgBuffer <- msgPack.Msgs[i]
				//fmt.Println("receive a search msg")
			}
		}
	}
}

func (ss *searchService) startSearchService() {
	defer ss.wait.Done()
	for {
		select {
		case <-ss.ctx.Done():
			return
		default:
			serviceTimestamp := (*(*ss.replica).getTSafe()).get()
			searchMsg := make([]msgstream.TsMsg, 0)
			tempMsg := make([]msgstream.TsMsg, 0)
			tempMsg = append(tempMsg, ss.unsolvedMsg...)
			ss.unsolvedMsg = ss.unsolvedMsg[:0]
			for _, msg := range tempMsg {
				if msg.BeginTs() > serviceTimestamp {
					searchMsg = append(searchMsg, msg)
					continue
				}
				ss.unsolvedMsg = append(ss.unsolvedMsg, msg)
			}

			msgBufferLength := len(ss.msgBuffer)
			for i := 0; i < msgBufferLength; i++ {
				msg := <-ss.msgBuffer
				if msg.BeginTs() > serviceTimestamp {
					searchMsg = append(searchMsg, msg)
					continue
				}
				ss.unsolvedMsg = append(ss.unsolvedMsg, msg)
			}
			if len(searchMsg) <= 0 {
				continue
			}
			err := ss.search(searchMsg)
			if err != nil {
				fmt.Println("search Failed")
				ss.publishFailedSearchResult()
			}
			fmt.Println("Do search done")
		}
	}
}

func (ss *searchService) search(searchMessages []msgstream.TsMsg) error {
	// TODO:: cache map[dsl]plan
	// TODO: reBatched search requests
	for _, msg := range searchMessages {
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
		partitionTags := query.PartitionTags
		collection, err := (*ss.replica).getCollectionByName(collectionName)
		if err != nil {
			return err
		}
		collectionID := collection.ID()
		dsl := query.Dsl
		plan := CreatePlan(*collection, dsl)
		topK := plan.GetTopK()
		placeHolderGroupBlob := query.PlaceholderGroup
		group := servicepb.PlaceholderGroup{}
		err = proto.Unmarshal(placeHolderGroupBlob, &group)
		if err != nil {
			return err
		}
		placeholderGroup := ParserPlaceholderGroup(plan, placeHolderGroupBlob)
		placeholderGroups := make([]*PlaceholderGroup, 0)
		placeholderGroups = append(placeholderGroups, placeholderGroup)

		// 2d slice for receiving multiple queries's results
		var numQueries int64 = 0
		for _, pg := range placeholderGroups {
			numQueries += pg.GetNumOfQuery()
		}

		resultIds := make([]IntPrimaryKey, topK*numQueries)
		resultDistances := make([]float32, topK*numQueries)
		for i := range resultDistances {
			resultDistances[i] = math.MaxFloat32
		}

		// 3. Do search in all segments
		for _, partitionTag := range partitionTags {
			partition, err := (*ss.replica).getPartitionByTag(collectionID, partitionTag)
			if err != nil {
				return err
			}
			for _, segment := range partition.segments {
				err := segment.segmentSearch(plan,
					placeholderGroups,
					[]Timestamp{searchTimestamp},
					resultIds,
					resultDistances,
					numQueries,
					topK)
				if err != nil {
					return err
				}
			}
		}

		// 4. return results
		hits := make([]*servicepb.Hits, 0)
		for i := int64(0); i < numQueries; i++ {
			hit := servicepb.Hits{}
			score := servicepb.Score{}
			for j := i * topK; j < (i+1)*topK; j++ {
				hit.IDs = append(hit.IDs, resultIds[j])
				score.Values = append(score.Values, resultDistances[j])
			}
			hit.Scores = append(hit.Scores, &score)
			hits = append(hits, &hit)
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
		}

		var tsMsg msgstream.TsMsg = &msgstream.SearchResultMsg{SearchResult: results}
		ss.publishSearchResult(tsMsg)
		plan.Delete()
		placeholderGroup.Delete()
	}

	return nil
}

func (ss *searchService) publishSearchResult(res msgstream.TsMsg) {
	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, res)
	(*ss.searchResultMsgStream).Produce(&msgPack)
}

func (ss *searchService) publishFailedSearchResult() {
	var errorResults = internalpb.SearchResult{
		MsgType: internalpb.MsgType_kSearchResult,
		Status:  &commonpb.Status{ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR},
	}

	var tsMsg msgstream.TsMsg = &msgstream.SearchResultMsg{SearchResult: errorResults}
	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, tsMsg)
	(*ss.searchResultMsgStream).Produce(&msgPack)
}
