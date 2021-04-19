package message_client

import (
	"context"
	"github.com/apache/pulsar/pulsar-client-go/pulsar"
	"github.com/golang/protobuf/proto"
	"log"
	"writer/pb"
)


type MessageClient struct {

	//message channel
	insertOrDeleteChan chan *pb.InsertOrDeleteMsg
	searchChan chan *pb.SearchMsg
	timeSyncChan chan *pb.TimeSyncMsg

	// pulsar
	client pulsar.Client
	key2segProducer pulsar.Producer
	writeSyncProducer pulsar.Producer
	insertOrDeleteConsumer pulsar.Consumer
	searchConsumer pulsar.Consumer
	timeSyncConsumer pulsar.Consumer

	// batch messages
	InsertMsg  []*pb.InsertOrDeleteMsg
	DeleteMsg  []*pb.InsertOrDeleteMsg
	SearchMsg  []*pb.SearchMsg
	timeSyncMsg    []*pb.TimeSyncMsg
}

func (mc *MessageClient)ReceiveInsertOrDeleteMsg() {
	for {
		insetOrDeleteMsg := pb.InsertOrDeleteMsg{}
		msg, err := mc.insertOrDeleteConsumer.Receive(context.Background())
		err = proto.Unmarshal(msg.Payload(), &insetOrDeleteMsg)
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
		err = proto.Unmarshal(msg.Payload(), &searchMsg)
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
		err = proto.Unmarshal(msg.Payload(), &timeSyncMsg)
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

func (mc *MessageClient) CreatProducer(topicName string) pulsar.Producer{
	producer, err := mc.client.CreateProducer(pulsar.ProducerOptions{
		Topic: topicName,
	})

	if err != nil {
		log.Fatal(err)
	}
	return producer
}

func (mc *MessageClient) CreateConsumer(topicName string) pulsar.Consumer {
	consumer, err := mc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:           topicName,
		SubscriptionName: "multi-topic-sub",
	})

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

	if err != nil {
		log.Fatal(err)
	}
	return client
}

func (mc *MessageClient) InitClient(url string) {
	//create client
	mc.client = mc.CreateClient(url)

	//create producer
	mc.key2segProducer = mc.CreatProducer("Key2Seg")
	mc.writeSyncProducer = mc.CreatProducer("TimeSync")

	//create consumer
	mc.insertOrDeleteConsumer = mc.CreateConsumer("InsertOrDelete")
	mc.searchConsumer = mc.CreateConsumer("Search")
	mc.timeSyncConsumer = mc.CreateConsumer("TimeSync")

	// init channel
	mc.insertOrDeleteChan = make(chan *pb.InsertOrDeleteMsg, 1000)
	mc.searchChan  = make(chan *pb.SearchMsg, 1000)
	mc.timeSyncChan = make(chan *pb.TimeSyncMsg, 1000)
}

func (mc *MessageClient) Close() {
	defer mc.client.Close()
	defer mc.key2segProducer.Close()
	defer mc.writeSyncProducer.Close()
	defer mc.insertOrDeleteConsumer.Close()
	defer mc.searchConsumer.Close()
	defer mc.timeSyncConsumer.Close()
}

type JobType int
const (
	OpInQueryNode JobType = 0
	OpInWriteNode JobType = 1
)

type MessageType int
const (
	InsertOrDelete MessageType = 0
	Delete         MessageType = 1
	Search         MessageType = 2
	TimeSync       MessageType = 3
	Key2Seg        MessageType = 4
	Statistics     MessageType = 5
)

func (mc *MessageClient) PrepareMsg(messageType MessageType, msgLen int) {
	switch messageType {
	case InsertOrDelete:
		for i := 0; i < msgLen; i++ {
			msg := <-mc.insertOrDeleteChan
			if msg.Op == pb.OpType_INSERT {
				mc.InsertMsg[i] = msg
			} else {
				mc.DeleteMsg[i] = msg
			}
		}
	case Search:
		for i := 0; i < msgLen; i++ {
			msg := <-mc.searchChan
			mc.SearchMsg[i] = msg
		}
	case TimeSync:
		for i := 0; i < msgLen; i++ {
			msg := <-mc.timeSyncChan
			mc.timeSyncMsg[i] = msg
		}
	}
}

func (mc *MessageClient)PrepareBatchMsg() []int{
	// assume the channel not full
	mc.InsertMsg = make([]*pb.InsertOrDeleteMsg, 1000)
	mc.DeleteMsg = make([]*pb.InsertOrDeleteMsg, 1000)
	mc.SearchMsg = make([]*pb.SearchMsg, 1000)
	mc.timeSyncMsg = make([]*pb.TimeSyncMsg, 1000)

	// get the length of every channel
	insertOrDeleteLen := len(mc.insertOrDeleteChan)
	searchLen := len(mc.searchChan)
	timeLen := len(mc.timeSyncChan)

	// get message from channel to slice
	mc.PrepareMsg(InsertOrDelete, insertOrDeleteLen)
	mc.PrepareMsg(Search, searchLen)
	mc.PrepareMsg(TimeSync, timeLen)

	return []int{insertOrDeleteLen, searchLen, timeLen}
}