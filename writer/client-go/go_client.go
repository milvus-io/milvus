package client_go

import (
	"context"
	"github.com/apache/pulsar/pulsar-client-go/pulsar"
	"github.com/golang/protobuf/proto"
	"log"
	"suvlim/pulsar/client-go/pb"
	"suvlim/pulsar/client-go/schema"
	"sync"
)

var (
	SyncEofSchema = "{\"type\":\"record\",\"name\":\"suvlim\",\"namespace\":\"pulsar\",\"fields\":[" +
		"{\"name\":\"MsgType\",\"type\":\"OpType\"}," +
		"]}"
)

type MessageClient struct {

	//message channel
	insertOrDeleteChan chan *pb.InsertOrDeleteMsg
	searchChan chan *pb.SearchMsg
	timeSyncChan chan *pb.TimeSyncMsg
	key2SegChan chan *pb.Key2SegMsg

	// pulsar
	client pulsar.Client
	key2segProducer pulsar.Producer
	writeSyncProducer pulsar.Producer
	insertOrDeleteConsumer pulsar.Consumer
	searchConsumer pulsar.Consumer
	timeSyncConsumer pulsar.Consumer

	// batch messages
	InsertOrDeleteMsg  []*pb.InsertOrDeleteMsg
	SearchMsg  []*pb.SearchMsg
	timeSyncMsg    []*pb.TimeSyncMsg
	key2segMsg []*pb.Key2SegMsg
}

func (mc *MessageClient)ReceiveInsertOrDeleteMsg() {
	for {
		insetOrDeleteMsg := pb.InsertOrDeleteMsg{}
		msg, err := mc.insertOrDeleteConsumer.Receive(context.Background())
		err = msg.GetValue(&insetOrDeleteMsg)
		if err != nil {
			log.Fatal(err)
		}
		mc.insertOrDeleteChan <- &insetOrDeleteMsg
	}
}

func (mc *MessageClient)ReceiveSearchMsg() {
	for {
		searchMsg := pb.SearchMsg{}
		msg, err := mc.insertOrDeleteConsumer.Receive(context.Background())
		err = msg.GetValue(&searchMsg)
		if err != nil {
			log.Fatal(err)
		}
		mc.searchChan <- &searchMsg
	}
}

func (mc *MessageClient)ReceiveTimeSyncMsg() {
	for {
		timeSyncMsg := pb.TimeSyncMsg{}
		msg, err := mc.insertOrDeleteConsumer.Receive(context.Background())
		err = msg.GetValue(&timeSyncMsg)
		if err != nil {
			log.Fatal(err)
		}
		mc.timeSyncChan <- &timeSyncMsg
	}
}

func (mc *MessageClient) ReceiveMessage() {
	go mc.ReceiveInsertOrDeleteMsg()
	go mc.ReceiveSearchMsg()
	go mc.ReceiveTimeSyncMsg()
}

func (mc *MessageClient) CreatProducer(opType pb.OpType, topicName string) pulsar.Producer{
	producer, err := mc.client.CreateProducer(pulsar.ProducerOptions{
		Topic: topicName,
	})
	defer producer.Close()
	if err != nil {
		log.Fatal(err)
	}
	proto.Marshal()
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

func (mc *MessageClient) InitClient(url string, topics []string, consumerMsgSchema string) {
	//create client
	mc.client = mc.CreateClient(url)

	//create producer
	for topicIndex := range topics {
		if topics[topicIndex] == "insert" {
			mc.key2segProducer = mc.CreatProducer(SyncEofSchema, "insert")
		}
		if topics[topicIndex] == "delete" {
			mc.syncDeleteProducer = mc.CreatProducer(SyncEofSchema, "delete")
		}
		if topics[topicIndex] == "key2seg" {
			mc.syncInsertProducer = mc.CreatProducer(SyncEofSchema, "key2seg")
		}

	}
	mc.syncInsertProducer = mc.CreatProducer(SyncEofSchema, "insert")
	mc.syncDeleteProducer = mc.CreatProducer(SyncEofSchema, "delete")
	mc.key2segProducer = mc.CreatProducer(SyncEofSchema, "key2seg")

	//create consumer
	mc.consumer = mc.CreateConsumer(consumerMsgSchema, topics)

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

func (mc *MessageClient) PrepareMsg(opType schema.OpType, msgLen int) {
	switch opType {
	case schema.Insert:
		for i := 0; i < msgLen; i++ {
			msg := <- mc.insertChan
			mc.InsertMsg[i] = msg
		}
	case schema.Delete:
		for i := 0; i < msgLen; i++ {
			msg := <- mc.deleteChan
			mc.DeleteMsg[i] = msg
		}
	case schema.Search:
		for i := 0; i < msgLen; i++ {
			msg := <-mc.searchChan
			mc.SearchMsg[i] = msg
		}
	case schema.TimeSync:
		for i := 0; i < msgLen; i++ {
			msg := <- mc.timeSyncChan
			mc.timeMsg[i] = msg
		}
	case schema.Key2Seg:
		for i := 0; i < msgLen; i++ {
			msg := <-mc.key2SegChan
			mc.key2segMsg[i] = msg
		}
	}
}

func (mc *MessageClient) PrepareBatchMsg(jobType JobType) {
	// assume the channel not full
	mc.InsertMsg = make([]*schema.InsertMsg, 1000)
	mc.DeleteMsg = make([]*schema.DeleteMsg, 1000)
	mc.SearchMsg = make([]*schema.SearchMsg, 1000)
	mc.timeMsg = make([]*schema.TimeSyncMsg, 1000)
	mc.key2segMsg = make([]*schema.Key2SegMsg, 1000)

	// ensure all messages before time in timeSyncTopic have been push into channel

	// get the length of every channel
	insertLen := len(mc.insertChan)
	deleteLen := len(mc.deleteChan)
	searchLen := len(mc.searchChan)
	timeLen := len(mc.timeSyncChan)
	key2segLen := len(mc.key2SegChan)

	// get message from channel to slice
	mc.PrepareMsg(schema.Insert, insertLen)
	mc.PrepareMsg(schema.Delete, deleteLen)
	mc.PrepareMsg(schema.TimeSync, timeLen)
	if jobType == OpInQueryNode {
		mc.PrepareMsg(schema.Key2Seg, key2segLen)
		mc.PrepareMsg(schema.Search, searchLen)
	}
}
