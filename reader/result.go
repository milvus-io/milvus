package reader

import (
	"fmt"
	msgPb "github.com/czs007/suvlim/pkg/message"
	"strconv"
)

type ResultEntityIds []int64

type SearchResult struct {
	ResultIds 			[]int64
	ResultDistances 	[]float32
}

func getResultTopicByClientId(clientId int64) string {
	// TODO: Result topic?
	return "result-topic/partition-" + strconv.FormatInt(clientId, 10)
}

func publishResult(ids *ResultEntityIds, clientId int64) msgPb.Status {
	// TODO: Pulsar publish
	var resultTopic = getResultTopicByClientId(clientId)
	fmt.Println(resultTopic)
	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}

func publishSearchResult(searchResults *SearchResult, clientId int64) msgPb.Status {
	// TODO: Pulsar publish
	var resultTopic = getResultTopicByClientId(clientId)
	fmt.Println(resultTopic)
	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}

func publicStatistic(statisticTopic string) msgPb.Status {
	// TODO: get statistic info
	// getStatisticInfo()
	// var info = getStatisticInfo()
	// TODO: Pulsar publish
	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}
