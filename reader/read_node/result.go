package reader

import (
	"context"
	masterPb "github.com/czs007/suvlim/pkg/master/grpc/master"
	msgPb "github.com/czs007/suvlim/pkg/master/grpc/message"
)

type ResultEntityIds []int64

type SearchResult struct {
	ResultIds       []int64
	ResultDistances []float32
}

func (node *QueryNode) PublishSearchResult(results *msgPb.QueryResult) msgPb.Status {
	var ctx = context.Background()

	node.messageClient.SendResult(ctx, *results, results.ClientId)

	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}

func (node *QueryNode) PublishFailedSearchResult() msgPb.Status {
	var results = msgPb.QueryResult{
		Status: &msgPb.Status{
			ErrorCode: 1,
			Reason:    "Search Failed",
		},
	}

	var ctx = context.Background()

	node.messageClient.SendResult(ctx, results, results.ClientId)
	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}

func (node *QueryNode) PublicStatistic(statisticData *[]masterPb.SegmentStat) msgPb.Status {
	var ctx = context.Background()

	node.messageClient.SendSegmentsStatistic(ctx, statisticData)

	return msgPb.Status{ErrorCode: msgPb.ErrorCode_SUCCESS}
}
