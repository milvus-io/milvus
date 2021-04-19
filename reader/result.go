package reader

import (
	"context"
	"fmt"
	msgPb "github.com/czs007/suvlim/pkg/master/grpc/message"
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

func (node *QueryNode) PublishSearchResult(results *msgPb.QueryResult, clientId int64) msgPb.Status {
	var ctx = context.Background()

	var resultTopic = getResultTopicByClientId(clientId)
	node.messageClient.Send(ctx, *results)
	fmt.Println(resultTopic)
	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}

func (node *QueryNode) PublicStatistic(statisticTopic string) msgPb.Status {
	// TODO: get statistic info
	// getStatisticInfo()
	// var info = getStatisticInfo()
	// TODO: Pulsar publish
	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}
