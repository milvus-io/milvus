package pulsar

import (
	"context"
	"github.com/apache/pulsar/pulsar-client-go/pulsar"
	"log"
	"suvlim/pulsar/schema"
	"sync"
)

var (
	wg sync.WaitGroup
	//wgJob sync.WaitGroup
	//wgQuery sync.WaitGroup
	//wgWrite sync.WaitGroup

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
	SyncEofSchema = "{\"type\":\"record\",\"name\":\"suvlim\",\"namespace\":\"pulsar\",\"fields\":[" +
		"{\"name\":\"MsgType\",\"type\":\"OpType\"}," +
		"]}"
)

type MessageClient struct {

	//message channel
	insertChan chan *schema.InsertMsg
	deleteChan chan *schema.DeleteMsg
	searchChan chan *schema.SearchMsg
	timeSyncChan chan *schema.TimeSyncMsg
	key2SegChan chan *schema.Key2SegMsg

	// pulsar
	client pulsar.Client
	syncInsertProducer pulsar.Producer
	syncDeleteProducer pulsar.Producer
	key2segProducer pulsar.Producer
	consumer pulsar.Consumer

	// batch messages
	insertMsg []*schema.InsertMsg
	deleteMsg []*schema.DeleteMsg
	searchMsg []*schema.SearchMsg
	timeMsg []*schema.TimeSyncMsg
	key2segMsg []*schema.Key2SegMsg

}

func (mc *MessageClient) ReceiveMessage() {
	for {
		pulsarMessage := schema.PulsarMessage{}
		msg, err := mc.consumer.Receive(context.Background())
		err = msg.GetValue(&pulsarMessage)
		if err != nil {
			log.Fatal(err)
		}

		msgType := pulsarMessage.MsgType
		switch msgType {
		case schema.Insert:
			IMsgObj := schema.InsertMsg{}
			mc.insertChan <- &IMsgObj
		case schema.Delete:
			DMsgObj := schema.DeleteMsg{}
			mc.deleteChan <- &DMsgObj
		case schema.Search:
			SMsgObj := schema.SearchMsg{}
			mc.searchChan <- &SMsgObj
		case schema.TimeSync:
			TMsgObj := schema.TimeSyncMsg{}
			mc.timeSyncChan <- &TMsgObj
		case schema.Key2Seg:
			KMsgObj := schema.Key2SegMsg{}
			mc.key2SegChan <- &KMsgObj
		}
	}
}

func (mc *MessageClient) CreatProducer(schemaDef string, topicName string) pulsar.Producer{
	schema  := pulsar.NewProtoSchema(schemaDef, nil)
	producer, err := mc.client.CreateProducerWithSchema(pulsar.ProducerOptions{
		Topic: topicName,
	}, schema)
	defer producer.Close()
	if err != nil {
		log.Fatal(err)
	}
	return producer
}

func (mc *MessageClient) CreateConsumer(schemaDef string, topics []string) pulsar.Consumer {
	originMsgSchema := pulsar.NewProtoSchema(schemaDef, nil)
	consumer, err := mc.client.SubscribeWithSchema(pulsar.ConsumerOptions{
		Topics:           topics,
		SubscriptionName: "multi-topic-sub",
	}, originMsgSchema)
	defer consumer.Close()
	if err != nil {
		log.Fatal(err)
	}
	return consumer
}

func (mc *MessageClient) CreateClient(url string) pulsar.Client {
	// create client
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: url,
	})
	defer client.Close()
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func (mc *MessageClient) InitClient(url string,topics []string) {
	//create client
	mc.client = mc.CreateClient(url)

	//create producer
	mc.syncInsertProducer = mc.CreatProducer(SyncEofSchema, "insert")
	mc.syncDeleteProducer = mc.CreatProducer(SyncEofSchema, "delete")
	mc.key2segProducer = mc.CreatProducer(SyncEofSchema, "key2seg")

	//create consumer
	mc.consumer = mc.CreateConsumer(OriginMsgSchema, topics)

	// init channel
	mc.insertChan = make(chan *schema.InsertMsg, 1000)
	mc.deleteChan = make(chan *schema.DeleteMsg, 1000)
	mc.searchChan  = make(chan *schema.SearchMsg, 1000)
	mc.timeSyncChan = make(chan *schema.TimeSyncMsg, 1000)
	mc.key2SegChan = make(chan *schema.Key2SegMsg, 1000)
}

type JobType int
const (
	OpInQueryNode JobType = 0
	OpInWriteNode JobType = 1
)

func (mc *MessageClient) PrepareBatchMsg(jobType JobType) {
	// assume the channel not full
	mc.insertMsg = make([]*schema.InsertMsg, 1000)
	mc.deleteMsg = make([]*schema.DeleteMsg, 1000)
	mc.searchMsg = make([]*schema.SearchMsg, 1000)
	mc.timeMsg = make([]*schema.TimeSyncMsg, 1000)
	mc.key2segMsg = make([]*schema.Key2SegMsg, 1000)

	// get the length of every channel
	insertLen := len(mc.insertChan)
	deleteLen := len(mc.deleteChan)
	searchLen := len(mc.searchChan)
	timeLen := len(mc.timeSyncChan)
	key2segLen := len(mc.key2SegChan)


	// get message from channel to slice
	for i := 0; i < insertLen; i++ {
		msg := <- mc.insertChan
		mc.insertMsg[i] = msg
	}
	for i := 0; i < deleteLen; i++ {
		msg := <- mc.deleteChan
		mc.deleteMsg[i] = msg
	}
	for i := 0; i < timeLen; i++ {
		msg := <- mc.timeSyncChan
		mc.timeMsg[i] = msg
	}
	if jobType == OpInQueryNode {
		for i := 0; i < key2segLen; i++ {
			msg := <-mc.key2SegChan
			mc.key2segMsg[i] = msg
		}

		for i := 0; i < searchLen; i++ {
			msg := <-mc.searchChan
			mc.searchMsg[i] = msg
		}
	}
}
