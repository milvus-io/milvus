package reader

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	servicePb "github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
)

type searchService struct {
	ctx       context.Context
	pulsarURL string

	node *QueryNode

	searchMsgStream       *msgstream.MsgStream
	searchResultMsgStream *msgstream.MsgStream
}

type queryInfo struct {
	NumQueries int64  `json:"num_queries"`
	TopK       int    `json:"topK"`
	FieldName  string `json:"field_name"`
}

type ResultEntityIds []UniqueID

type SearchResult struct {
	ResultIds       []UniqueID
	ResultDistances []float32
}

func newSearchService(ctx context.Context, node *QueryNode, pulsarURL string) *searchService {

	return &searchService{
		ctx:       ctx,
		pulsarURL: pulsarURL,

		node: node,

		searchMsgStream:       nil,
		searchResultMsgStream: nil,
	}
}

func (ss *searchService) start() {
	const (
		receiveBufSize = 1024
		pulsarBufSize  = 1024
	)

	consumeChannels := []string{"search"}
	consumeSubName := "searchSub"

	searchStream := msgstream.NewPulsarMsgStream(ss.ctx, receiveBufSize)
	searchStream.SetPulsarCient(ss.pulsarURL)
	unmarshalDispatcher := msgstream.NewUnmarshalDispatcher()
	searchStream.CreatePulsarConsumers(consumeChannels, consumeSubName, unmarshalDispatcher, pulsarBufSize)

	producerChannels := []string{"searchResult"}

	searchResultStream := msgstream.NewPulsarMsgStream(ss.ctx, receiveBufSize)
	searchResultStream.SetPulsarCient(ss.pulsarURL)
	searchResultStream.CreatePulsarProducers(producerChannels)

	var searchMsgStream msgstream.MsgStream = searchStream
	var searchResultMsgStream msgstream.MsgStream = searchResultStream

	ss.searchMsgStream = &searchMsgStream
	ss.searchResultMsgStream = &searchResultMsgStream

	(*ss.searchMsgStream).Start()

	for {
		select {
		case <-ss.ctx.Done():
			return
		default:
			msgPack := (*ss.searchMsgStream).Consume()
			// TODO: add serviceTime check
			err := ss.search(msgPack.Msgs)
			if err != nil {
				fmt.Println("search Failed")
				ss.publishFailedSearchResult()
			}
			fmt.Println("Do search done")
		}
	}
}

func (ss *searchService) search(searchMessages []*msgstream.TsMsg) error {

	//type SearchResultTmp struct {
	//	ResultID       int64
	//	ResultDistance float32
	//}
	//
	//for _, msg := range searchMessages {
	//	// preprocess
	//	// msg.dsl compare
	//
	//}
	//
	//// Traverse all messages in the current messageClient.
	//// TODO: Do not receive batched search requests
	//for _, msg := range searchMessages {
	//	searchMsg, ok := (*msg).(msgstream.SearchTask)
	//	if !ok {
	//		return errors.New("invalid request type = " + string((*msg).Type()))
	//	}
	//	var clientID = searchMsg.ProxyID
	//	var searchTimestamp = searchMsg.Timestamp
	//
	//	// ServiceTimeSync update by TimeSync, which is get from proxy.
	//	// Proxy send this timestamp per `conf.Config.Timesync.Interval` milliseconds.
	//	// However, timestamp of search request (searchTimestamp) is precision time.
	//	// So the ServiceTimeSync is always less than searchTimestamp.
	//	// Here, we manually make searchTimestamp's logic time minus `conf.Config.Timesync.Interval` milliseconds.
	//	// Which means `searchTimestamp.logicTime = searchTimestamp.logicTime - conf.Config.Timesync.Interval`.
	//	var logicTimestamp = searchTimestamp << 46 >> 46
	//	searchTimestamp = (searchTimestamp>>18-uint64(conf.Config.Timesync.Interval+600))<<18 + logicTimestamp
	//
	//	//var vector = searchMsg.Query
	//	// We now only the first Json is valid.
	//	var queryJSON = "searchMsg.SearchRequest.???"
	//
	//	// 1. Timestamp check
	//	// TODO: return or wait? Or adding graceful time
	//	if searchTimestamp > ss.node.queryNodeTime.ServiceTimeSync {
	//		errMsg := fmt.Sprint("Invalid query time, timestamp = ", searchTimestamp>>18, ", SearchTimeSync = ", ss.node.queryNodeTime.ServiceTimeSync>>18)
	//		fmt.Println(errMsg)
	//		return errors.New(errMsg)
	//	}
	//
	//	// 2. Get query information from query json
	//	query := ss.queryJSON2Info(&queryJSON)
	//	// 2d slice for receiving multiple queries's results
	//	var resultsTmp = make([][]SearchResultTmp, query.NumQueries)
	//	for i := 0; i < int(query.NumQueries); i++ {
	//		resultsTmp[i] = make([]SearchResultTmp, 0)
	//	}
	//
	//	// 3. Do search in all segments
	//	for _, segment := range ss.node.SegmentsMap {
	//		if segment.getRowCount() <= 0 {
	//			// Skip empty segment
	//			continue
	//		}
	//
	//		//fmt.Println("search in segment:", segment.SegmentID, ",segment rows:", segment.getRowCount())
	//		var res, err = segment.segmentSearch(query, searchTimestamp, nil)
	//		if err != nil {
	//			fmt.Println(err.Error())
	//			return err
	//		}
	//
	//		for i := 0; i < int(query.NumQueries); i++ {
	//			for j := i * query.TopK; j < (i+1)*query.TopK; j++ {
	//				resultsTmp[i] = append(resultsTmp[i], SearchResultTmp{
	//					ResultID:       res.ResultIds[j],
	//					ResultDistance: res.ResultDistances[j],
	//				})
	//			}
	//		}
	//	}
	//
	//	// 4. Reduce results
	//	for _, rTmp := range resultsTmp {
	//		sort.Slice(rTmp, func(i, j int) bool {
	//			return rTmp[i].ResultDistance < rTmp[j].ResultDistance
	//		})
	//	}
	//
	//	for _, rTmp := range resultsTmp {
	//		if len(rTmp) > query.TopK {
	//			rTmp = rTmp[:query.TopK]
	//		}
	//	}
	//
	//	var entities = servicePb.Entities{
	//		Ids: make([]int64, 0),
	//	}
	//	var results = servicePb.QueryResult{
	//		Status: &servicePb.Status{
	//			ErrorCode: 0,
	//		},
	//		Entities:  &entities,
	//		Distances: make([]float32, 0),
	//		QueryId:   searchMsg.ReqID,
	//		ProxyID:   clientID,
	//	}
	//	for _, rTmp := range resultsTmp {
	//		for _, res := range rTmp {
	//			results.Entities.Ids = append(results.Entities.Ids, res.ResultID)
	//			results.Distances = append(results.Distances, res.ResultDistance)
	//			results.Scores = append(results.Distances, float32(0))
	//		}
	//	}
	//	// Send numQueries to RowNum.
	//	results.RowNum = query.NumQueries
	//
	//	// 5. publish result to pulsar
	//	//fmt.Println(results.Entities.Ids)
	//	//fmt.Println(results.Distances)
	//	ss.publishSearchResult(&results)
	//}

	return nil
}

func (ss *searchService) publishSearchResult(res *servicePb.QueryResult) {
	//(*inputStream).Produce(&msgPack)
}

func (ss *searchService) publishFailedSearchResult() {

}

func (ss *searchService) queryJSON2Info(queryJSON *string) *queryInfo {
	var query queryInfo
	var err = json.Unmarshal([]byte(*queryJSON), &query)

	if err != nil {
		log.Fatal("Unmarshal query json failed")
		return nil
	}

	return &query
}
