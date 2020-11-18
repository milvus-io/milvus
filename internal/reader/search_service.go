package reader

import "C"
import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"

	"github.com/golang/protobuf/proto"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
)

type searchService struct {
	ctx    context.Context
	cancel context.CancelFunc

	replica      *collectionReplica
	tSafeWatcher *tSafeWatcher

	searchMsgStream       *msgstream.MsgStream
	searchResultMsgStream *msgstream.MsgStream
}

type ResultEntityIds []UniqueID

type SearchResult struct {
	ResultIds       []UniqueID
	ResultDistances []float32
}

func newSearchService(ctx context.Context, replica *collectionReplica) *searchService {
	receiveBufSize := Params.searchReceiveBufSize()
	pulsarBufSize := Params.searchPulsarBufSize()

	msgStreamURL, err := Params.pulsarAddress()
	if err != nil {
		log.Fatal(err)
	}

	consumeChannels := []string{"search"}
	consumeSubName := "subSearch"
	searchStream := msgstream.NewPulsarMsgStream(ctx, receiveBufSize)
	searchStream.SetPulsarCient(msgStreamURL)
	unmarshalDispatcher := msgstream.NewUnmarshalDispatcher()
	searchStream.CreatePulsarConsumers(consumeChannels, consumeSubName, unmarshalDispatcher, pulsarBufSize)
	var inputStream msgstream.MsgStream = searchStream

	producerChannels := []string{"searchResult"}
	searchResultStream := msgstream.NewPulsarMsgStream(ctx, receiveBufSize)
	searchResultStream.SetPulsarCient(msgStreamURL)
	searchResultStream.CreatePulsarProducers(producerChannels)
	var outputStream msgstream.MsgStream = searchResultStream

	searchServiceCtx, searchServiceCancel := context.WithCancel(ctx)
	return &searchService{
		ctx:    searchServiceCtx,
		cancel: searchServiceCancel,

		replica:      replica,
		tSafeWatcher: newTSafeWatcher(),

		searchMsgStream:       &inputStream,
		searchResultMsgStream: &outputStream,
	}
}

func (ss *searchService) start() {
	(*ss.searchMsgStream).Start()
	(*ss.searchResultMsgStream).Start()

	go func() {
		for {
			select {
			case <-ss.ctx.Done():
				return
			default:
				msgPack := (*ss.searchMsgStream).Consume()
				if msgPack == nil || len(msgPack.Msgs) <= 0 {
					continue
				}
				// TODO: add serviceTime check
				err := ss.search(msgPack.Msgs)
				if err != nil {
					fmt.Println("search Failed")
					ss.publishFailedSearchResult()
				}
				fmt.Println("Do search done")
			}
		}
	}()
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

func (ss *searchService) search(searchMessages []msgstream.TsMsg) error {

	type SearchResult struct {
		ResultID       int64
		ResultDistance float32
	}
	// TODO:: cache map[dsl]plan
	// TODO: reBatched search requests
	for _, msg := range searchMessages {
		searchMsg, ok := msg.(*msgstream.SearchMsg)
		if !ok {
			return errors.New("invalid request type = " + string(msg.Type()))
		}

		searchTimestamp := searchMsg.Timestamp

		// TODO:: add serviceable time
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
		var searchResults = make([][]SearchResult, numQueries)
		for i := 0; i < int(numQueries); i++ {
			searchResults[i] = make([]SearchResult, 0)
		}

		// 3. Do search in all segments
		for _, partitionTag := range partitionTags {
			partition, err := (*ss.replica).getPartitionByTag(collectionID, partitionTag)
			if err != nil {
				return err
			}
			for _, segment := range partition.segments {
				res, err := segment.segmentSearch(plan, placeholderGroups, []Timestamp{searchTimestamp}, numQueries, topK)
				if err != nil {
					return err
				}
				for i := 0; int64(i) < numQueries; i++ {
					for j := int64(i) * topK; j < int64(i+1)*topK; j++ {
						searchResults[i] = append(searchResults[i], SearchResult{
							ResultID:       res.ResultIds[j],
							ResultDistance: res.ResultDistances[j],
						})
					}
				}
			}
		}

		// 4. Reduce results
		// TODO::reduce in c++ merge_into func
		for _, temp := range searchResults {
			sort.Slice(temp, func(i, j int) bool {
				return temp[i].ResultDistance < temp[j].ResultDistance
			})
		}

		for i, tmp := range searchResults {
			if int64(len(tmp)) > topK {
				searchResults[i] = searchResults[i][:topK]
			}
		}

		hits := make([]*servicepb.Hits, 0)
		for _, value := range searchResults {
			hit := servicepb.Hits{}
			score := servicepb.Score{}
			for j := 0; int64(j) < topK; j++ {
				hit.IDs = append(hit.IDs, value[j].ResultID)
				score.Values = append(score.Values, value[j].ResultDistance)
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
