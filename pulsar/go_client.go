package pulsar

import "suvlim/pulsar/schema"

func Send(msg schema.Message) {

}

func BatchSend(msgs []schema.Message) {

}

var (
	InsertSchemaDef = "{\"type\":\"record\",\"name\":\"suvlim\",\"namespace\":\"pulsar\",\"fields\":[" +
		"{\"name\":\"CollectionName\",\"type\":\"string\"}," +
		"{\"name\":\"Fields\",\"type\":\"[]*FieldValue\"}" +
		"{\"name\":\"EntityId\",\"type\":\"int64\"}" +
		"{\"name\":\"PartitionTag\",\"type\":\"string\"}" +
		"{\"name\":\"Timestamp\",\"type\":\"int64\"}" +
		"{\"name\":\"ClientId\",\"type\":\"int64\"}" +
		"{\"name\":\"MsgType\",\"type\":\"OpType\"}" +
		"]}"
	DeleteSchemaDef = "{\"type\":\"record\",\"name\":\"suvlim\",\"namespace\":\"pulsar\",\"fields\":[" +
		"{\"name\":\"CollectionName\",\"type\":\"string\"}," +
		"{\"name\":\"EntityId\",\"type\":\"int64\"}" +
		"{\"name\":\"Timestamp\",\"type\":\"int64\"}" +
		"{\"name\":\"ClientId\",\"type\":\"int64\"}" +
		"{\"name\":\"MsgType\",\"type\":\"OpType\"}" +
		"]}"
	SearchSchemaDef = "{\"type\":\"record\",\"name\":\"suvlim\",\"namespace\":\"pulsar\",\"fields\":[" +
		"{\"name\":\"CollectionName\",\"type\":\"string\"}," +
		"{\"name\":\"PartitionTag\",\"type\":\"string\"}" +
		"{\"name\":\"VectorParam\",\"type\":\"*VectorParam\"}" +
		"{\"name\":\"Timestamp\",\"type\":\"int64\"}" +
		"{\"name\":\"ClientId\",\"type\":\"int64\"}" +
		"{\"name\":\"MsgType\",\"type\":\"OpType\"}" +
		"]}"
	TimeSyncSchemaDef = "{\"type\":\"record\",\"name\":\"suvlim\",\"namespace\":\"pulsar\",\"fields\":[" +
		"{\"name\":\"Timestamp\",\"type\":\"int64\"}" +
		"{\"name\":\"ClientId\",\"type\":\"int64\"}" +
		"{\"name\":\"MsgType\",\"type\":\"OpType\"}" +
		"]}"
)