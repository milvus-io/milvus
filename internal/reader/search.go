package reader

import (
	"fmt"
	"github.com/zilliztech/milvus-distributed/internal/conf"
	msgPb "github.com/zilliztech/milvus-distributed/internal/proto/message"
	"sort"
)

func (node *QueryNode) Search(searchMessages []*msgPb.SearchMsg) msgPb.Status {

	type SearchResultTmp struct {
		ResultId       int64
		ResultDistance float32
	}

	node.msgCounter.SearchCounter += int64(len(searchMessages))

	// Traverse all messages in the current messageClient.
	// TODO: Do not receive batched search requests
	for _, msg := range searchMessages {
		var clientId = msg.ClientId
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
		var queryJson = msg.Json[0]

		// 1. Timestamp check
		// TODO: return or wait? Or adding graceful time
		if searchTimestamp > node.queryNodeTimeSync.ServiceTimeSync {
			fmt.Println("Invalid query time, timestamp = ", searchTimestamp>>18, ", SearchTimeSync = ", node.queryNodeTimeSync.ServiceTimeSync>>18)
			return msgPb.Status{ErrorCode: 1}
		}

		// 2. Get query information from query json
		query := node.QueryJson2Info(&queryJson)
		// 2d slice for receiving multiple queries's results
		var resultsTmp = make([][]SearchResultTmp, query.NumQueries)
		for i := 0; i < int(query.NumQueries); i++ {
			resultsTmp[i] = make([]SearchResultTmp, 0)
		}

		// 3. Do search in all segments
		for _, segment := range node.SegmentsMap {
			if segment.GetRowCount() <= 0 {
				// Skip empty segment
				continue
			}

			//fmt.Println("Search in segment:", segment.SegmentId, ",segment rows:", segment.GetRowCount())
			var res, err = segment.SegmentSearch(query, searchTimestamp, vector)
			if err != nil {
				fmt.Println(err.Error())
				return msgPb.Status{ErrorCode: 1}
			}

			for i := 0; i < int(query.NumQueries); i++ {
				for j := i * query.TopK; j < (i+1)*query.TopK; j++ {
					resultsTmp[i] = append(resultsTmp[i], SearchResultTmp{
						ResultId:       res.ResultIds[j],
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
			ProxyId:   clientId,
		}
		for _, rTmp := range resultsTmp {
			for _, res := range rTmp {
				results.Entities.Ids = append(results.Entities.Ids, res.ResultId)
				results.Distances = append(results.Distances, res.ResultDistance)
				results.Scores = append(results.Distances, float32(0))
			}
		}
		// Send numQueries to RowNum.
		results.RowNum = query.NumQueries

		// 5. publish result to pulsar
		//fmt.Println(results.Entities.Ids)
		//fmt.Println(results.Distances)
		node.PublishSearchResult(&results)
	}

	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}
