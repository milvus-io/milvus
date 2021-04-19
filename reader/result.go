package reader

import (
	"fmt"
	schema2 "suvlim/pulsar/client-go/schema"
)

type ResultEntityIds []int64

func getResultTopicByClientId(clientId int64) string {
	// TODO: Result topic?
	return "result-topic/partition-" + string(clientId)
}

func publishResult(ids *ResultEntityIds, clientId int64) schema2.Status {
	// TODO: Pulsar publish
	var resultTopic = getResultTopicByClientId(clientId)
	fmt.Println(resultTopic)
	return schema2.Status{Error_code: schema2.ErrorCode_SUCCESS}
}

func publicStatistic(statisticTopic string) schema2.Status {
	// TODO: get statistic info
	// getStatisticInfo()
	// var info = getStatisticInfo()
	// TODO: Pulsar publish
	return schema2.Status{Error_code: schema2.ErrorCode_SUCCESS}
}
