package reader

type ResultEntityIds []UniqueID

type SearchResult struct {
	ResultIds       []UniqueID
	ResultDistances []float32
}

//
//func (node *QueryNode) PublishSearchResult(results *msgpb.QueryResult) commonpb.Status {
//	var ctx = context.Background()
//
//	node.messageClient.SendResult(ctx, *results, results.ProxyId)
//
//	return commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS}
//}
//
//func (node *QueryNode) PublishFailedSearchResult() commonpb.Status {
//	var results = msgpb.QueryResult{}
//
//	var ctx = context.Background()
//
//	node.messageClient.SendResult(ctx, results, results.ProxyId)
//	return commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS}
//}
//
//func (node *QueryNode) PublicStatistic(statisticData *[]internalpb.SegmentStatistics) commonpb.Status {
//	var ctx = context.Background()
//
//	node.messageClient.SendSegmentsStatistic(ctx, statisticData)
//
//	return commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS}
//}
