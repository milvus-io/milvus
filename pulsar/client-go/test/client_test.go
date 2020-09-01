package test

import "sync"

var (
	wg sync.WaitGroup

	OriginMsgSchema = "{\"type\":\"record\",\"name\":\"suvlim\",\"namespace\":\"pulsar\",\"fields\":[" +
		"{\"name\":\"CollectionName\",\"type\":\"string\"}," +
		"{\"name\":\"Fields\",\"type\":\"[]*FieldValue\"}" +
		"{\"name\":\"EntityId\",\"type\":\"int64\"}" +
		"{\"name\":\"PartitionTag\",\"type\":\"string\"}" +
		"{\"name\":\"VectorParam\",\"type\":\"*VectorParam\"}" +
		"{\"name\":\"Segments\",\"type\":\"[]string\"}" +
		"{\"name\":\"Timestamp\",\"type\":\"int64\"}" +
		"{\"name\":\"ClientId\",\"type\":\"int64\"}" +
		"{\"name\":\"MsgType\",\"type\":\"OpType\"}" +
		"]}"
)