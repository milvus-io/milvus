package reader

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"

	"github.com/zilliztech/milvus-distributed/internal/conf"
	msgPb "github.com/zilliztech/milvus-distributed/internal/proto/message"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type searchService struct {
	ctx       context.Context
	pulsarURL string
	node      *QueryNode
	msgStream *msgstream.PulsarMsgStream
}

type queryInfo struct {
	NumQueries int64  `json:"num_queries"`
	TopK       int    `json:"topK"`
	FieldName  string `json:"field_name"`
}

func newSearchService(ctx context.Context, node *QueryNode, pulsarURL string) *searchService {

	return &searchService{
		ctx:       ctx,
		pulsarURL: pulsarURL,
		node:      node,
		msgStream: nil,
	}
}

func (ss *searchService) start() {
	// TODO: init pulsar message stream

	for {
		select {
		case <-ss.ctx.Done():
			return
			//case msg := <-node.messageClient.GetSearchChan():
			//	ss.messageClient.SearchMsg = node.messageClient.SearchMsg[:0]
			//	ss.messageClient.SearchMsg = append(node.messageClient.SearchMsg, msg)
			//	// TODO: add serviceTime check
			//	var status = node.search(node.messageClient.SearchMsg)
			//	fmt.Println("Do search done")
			//	if status.ErrorCode != 0 {
			//		fmt.Println("search Failed")
			//		node.PublishFailedSearchResult()
			//	}
		}
	}
}

func (ss *searchService) search(searchMessages []*msgPb.SearchMsg) msgPb.Status {

	type SearchResultTmp struct {
		ResultID       int64
		ResultDistance float32
	}

	// Traverse all messages in the current messageClient.
	// TODO: Do not receive batched search requests
	for _, msg := range searchMessages {
		var clientID = msg.ClientId
		var searchTimestamp = msg.Timestamp

		// ServiceTimeSync update by TimeSync, which is get from proxy.
		// Proxy send this timestamp per `conf.Config.Timesync.Interval` milliseconds.
		// However, timestamp of search request (searchTimestamp) is precision time.
		// So the ServiceTimeSync is always less than searchTimestamp.
		// Here, we manually make searchTimestamp's logic time minus `conf.Config.Timesync.Interval` milliseconds.
		// Which means `searchTimestamp.logicTime = searchTimestamp.logicTime - conf.Config.Timesync.Interval`.
		var logicTimestamp = searchTimestamp << 46 >> 46
		searchTimestamp = (searchTimestamp>>18-uint64(conf.Config.Timesync.Interval+600))<<18 + logicTimestamp

		var vector = msg.Records
		// We now only the first Json is valid.
		var queryJSON = msg.Json[0]

		// 1. Timestamp check
		// TODO: return or wait? Or adding graceful time
		if searchTimestamp > ss.node.queryNodeTime.ServiceTimeSync {
			fmt.Println("Invalid query time, timestamp = ", searchTimestamp>>18, ", SearchTimeSync = ", ss.node.queryNodeTime.ServiceTimeSync>>18)
			return msgPb.Status{ErrorCode: 1}
		}

		// 2. Get query information from query json
		query := ss.queryJSON2Info(&queryJSON)
		// 2d slice for receiving multiple queries's results
		var resultsTmp = make([][]SearchResultTmp, query.NumQueries)
		for i := 0; i < int(query.NumQueries); i++ {
			resultsTmp[i] = make([]SearchResultTmp, 0)
		}

		// 3. Do search in all segments
		for _, segment := range ss.node.SegmentsMap {
			if segment.getRowCount() <= 0 {
				// Skip empty segment
				continue
			}

			//fmt.Println("search in segment:", segment.SegmentID, ",segment rows:", segment.getRowCount())
			var res, err = segment.segmentSearch(query, searchTimestamp, vector)
			if err != nil {
				fmt.Println(err.Error())
				return msgPb.Status{ErrorCode: 1}
			}

			for i := 0; i < int(query.NumQueries); i++ {
				for j := i * query.TopK; j < (i+1)*query.TopK; j++ {
					resultsTmp[i] = append(resultsTmp[i], SearchResultTmp{
						ResultID:       res.ResultIds[j],
						ResultDistance: res.ResultDistances[j],
					})
				}
			}
		}

		// 4. Reduce results
		for _, rTmp := range resultsTmp {
			sort.Slice(rTmp, func(i, j int) bool {
				return rTmp[i].ResultDistance < rTmp[j].ResultDistance
			})
		}

		for _, rTmp := range resultsTmp {
			if len(rTmp) > query.TopK {
				rTmp = rTmp[:query.TopK]
			}
		}

		var entities = msgPb.Entities{
			Ids: make([]int64, 0),
		}
		var results = msgPb.QueryResult{
			Status: &msgPb.Status{
				ErrorCode: 0,
			},
			Entities:  &entities,
			Distances: make([]float32, 0),
			QueryId:   msg.Uid,
			ProxyId:   clientID,
		}
		for _, rTmp := range resultsTmp {
			for _, res := range rTmp {
				results.Entities.Ids = append(results.Entities.Ids, res.ResultID)
				results.Distances = append(results.Distances, res.ResultDistance)
				results.Scores = append(results.Distances, float32(0))
			}
		}
		// Send numQueries to RowNum.
		results.RowNum = query.NumQueries

		// 5. publish result to pulsar
		//fmt.Println(results.Entities.Ids)
		//fmt.Println(results.Distances)
		ss.publishSearchResult(&results)
	}

	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}

func (ss *searchService) publishSearchResult(res *msgPb.QueryResult) {
	// TODO: add result publish
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
