package reader

import (
	"fmt"
	"suvlim/pulsar/schema"
)

type ResultEntityIds []int64

func getResultTopicByClientId(clientId int64) string {
	// TODO: Result topic?
	return "result-topic/partition-" + string(clientId)
}

func publishResult(ids *ResultEntityIds, clientId int64) schema.Status {
	// TODO: Pulsar publish
	var resultTopic = getResultTopicByClientId(clientId)
	fmt.Println(resultTopic)
	return schema.Status{Error_code: schema.ErrorCode_SUCCESS}
}

func publicStatistic(statisticTopic string) schema.Status {
	// TODO: get statistic info
	// getStatisticInfo()
	// var info = getStatisticInfo()
	// TODO: Pulsar publish
	return schema.Status{Error_code: schema.ErrorCode_SUCCESS}
}
