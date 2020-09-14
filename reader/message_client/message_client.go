package message_client

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	msgpb "github.com/czs007/suvlim/pkg/master/grpc/message"
	"github.com/golang/protobuf/proto"
	"log"
	"time"
)

type MessageClient struct {

	//message channel
	insertOrDeleteChan chan *msgpb.InsertOrDeleteMsg
	searchChan         chan *msgpb.SearchMsg
	timeSyncChan       chan *msgpb.TimeSyncMsg
	key2SegChan        chan *msgpb.Key2SegMsg

	// pulsar
	client                 pulsar.Client
	searchResultProducer   pulsar.Producer
	insertOrDeleteConsumer pulsar.Consumer
	searchConsumer         pulsar.Consumer
	timeSyncConsumer       pulsar.Consumer
	key2segConsumer        pulsar.Consumer

	// batch messages
	InsertOrDeleteMsg []*msgpb.InsertOrDeleteMsg
	SearchMsg         []*msgpb.SearchMsg
	TimeSyncMsg       []*msgpb.TimeSyncMsg
	Key2SegMsg        []*msgpb.Key2SegMsg
}

func (mc *MessageClient) Send(ctx context.Context, msg msgpb.QueryResult) {
	var msgBuffer, _ = proto.Marshal(&msg)
	if _, err := mc.searchResultProducer.Send(ctx, &pulsar.ProducerMessage{
		Payload: msgBuffer,
	}); err != nil {
		log.Fatal(err)
	}
}

func (mc *MessageClient) GetSearchChan() chan *msgpb.SearchMsg {
	return mc.searchChan
}

func (mc *MessageClient) ReceiveInsertOrDeleteMsg() {
	var count = 0
	var start time.Time
	for {
		insetOrDeleteMsg := msgpb.InsertOrDeleteMsg{}
		msg, err := mc.insertOrDeleteConsumer.Receive(context.Background())
		err = proto.Unmarshal(msg.Payload(), &insetOrDeleteMsg)
		if err != nil {
			log.Fatal(err)
		}
		if count == 0 {
			start = time.Now()
		}
		count++
		mc.insertOrDeleteChan <- &insetOrDeleteMsg
		mc.insertOrDeleteConsumer.Ack(msg)
		if count == 100000 - 1 {
			elapsed := time.Since(start)
			fmt.Println("Query node ReceiveInsertOrDeleteMsg time:", elapsed)
		}
	}
}

func (mc *MessageClient) ReceiveSearchMsg() {
	for {
		searchMsg := msgpb.SearchMsg{}
		msg, err := mc.searchConsumer.Receive(context.Background())
		err = proto.Unmarshal(msg.Payload(), &searchMsg)
		if err != nil {
			log.Fatal(err)
		}
		mc.searchChan <- &searchMsg
		mc.searchConsumer.Ack(msg)
	}
}

func (mc *MessageClient) ReceiveTimeSyncMsg() {
	for {
		timeSyncMsg := msgpb.TimeSyncMsg{}
		msg, err := mc.timeSyncConsumer.Receive(context.Background())
		err = proto.Unmarshal(msg.Payload(), &timeSyncMsg)
		if err != nil {
			log.Fatal(err)
		}
		mc.timeSyncChan <- &timeSyncMsg
		mc.timeSyncConsumer.Ack(msg)
	}
}

func (mc *MessageClient) ReceiveKey2SegMsg() {
	for {
		key2SegMsg := msgpb.Key2SegMsg{}
		msg, err := mc.key2segConsumer.Receive(context.Background())
		err = proto.Unmarshal(msg.Payload(), &key2SegMsg)
		if err != nil {
			log.Fatal(err)
		}
		mc.key2SegChan <- &key2SegMsg
		mc.key2segConsumer.Ack(msg)
	}
}

func (mc *MessageClient) ReceiveMessage() {
	go mc.ReceiveInsertOrDeleteMsg()
	go mc.ReceiveSearchMsg()
	go mc.ReceiveTimeSyncMsg()
	go mc.ReceiveKey2SegMsg()
}

func (mc *MessageClient) CreatProducer(topicName string) pulsar.Producer {
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
		Topic:            topicName,
		SubscriptionName: "reader",
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
	mc.searchResultProducer = mc.CreatProducer("SearchResult")

	//create consumer
	mc.insertOrDeleteConsumer = mc.CreateConsumer("InsertOrDelete")
	mc.searchConsumer = mc.CreateConsumer("Search")
	mc.timeSyncConsumer = mc.CreateConsumer("TimeSync")
	mc.key2segConsumer = mc.CreateConsumer("Key2Seg")

	// init channel
	mc.insertOrDeleteChan = make(chan *msgpb.InsertOrDeleteMsg, 1000)
	mc.searchChan = make(chan *msgpb.SearchMsg, 1000)
	mc.timeSyncChan = make(chan *msgpb.TimeSyncMsg, 1000)
	mc.key2SegChan = make(chan *msgpb.Key2SegMsg, 1000)

	mc.InsertOrDeleteMsg = make([]*msgpb.InsertOrDeleteMsg, 1000)
	mc.SearchMsg = make([]*msgpb.SearchMsg, 1000)
	mc.TimeSyncMsg = make([]*msgpb.TimeSyncMsg, 1000)
	mc.Key2SegMsg = make([]*msgpb.Key2SegMsg, 1000)
}

func (mc *MessageClient) Close() {
	defer mc.client.Close()
	defer mc.searchResultProducer.Close()
	defer mc.insertOrDeleteConsumer.Close()
	defer mc.searchConsumer.Close()
	defer mc.timeSyncConsumer.Close()
	defer mc.key2segConsumer.Close()
}

type MessageType int

const (
	InsertOrDelete MessageType = 0
	Search         MessageType = 1
	SearchById     MessageType = 2
	TimeSync       MessageType = 3
	Key2Seg        MessageType = 4
	Statistics     MessageType = 5
)

func (mc *MessageClient) PrepareMsg(messageType MessageType, msgLen int) {
	switch messageType {
	case InsertOrDelete:
		for i := 0; i < msgLen; i++ {
			msg := <-mc.insertOrDeleteChan
			mc.InsertOrDeleteMsg = append(mc.InsertOrDeleteMsg, msg)
		}
	case Search:
		for i := 0; i < msgLen; i++ {
			msg := <-mc.searchChan
			mc.SearchMsg = append(mc.SearchMsg, msg)
		}
	case TimeSync:
		for i := 0; i < msgLen; i++ {
			msg := <-mc.timeSyncChan
			mc.TimeSyncMsg = append(mc.TimeSyncMsg, msg)
		}
	}
}

func (mc *MessageClient) PrepareKey2SegmentMsg() {
	mc.Key2SegMsg = mc.Key2SegMsg[:0]
	msgLen := len(mc.key2SegChan)
	for i := 0; i < msgLen; i++ {
		msg := <-mc.key2SegChan
		mc.Key2SegMsg = append(mc.Key2SegMsg, msg)
	}
}

func (mc *MessageClient) PrepareBatchMsg() []int {
	// assume the channel not full
	mc.InsertOrDeleteMsg = mc.InsertOrDeleteMsg[:0]
	//mc.SearchMsg = mc.SearchMsg[:0]
	mc.TimeSyncMsg = mc.TimeSyncMsg[:0]

	// get the length of every channel
	insertOrDeleteLen := len(mc.insertOrDeleteChan)
	//searchLen := len(mc.searchChan)
	timeLen := len(mc.timeSyncChan)

	// get message from channel to slice
	mc.PrepareMsg(InsertOrDelete, insertOrDeleteLen)
	//mc.PrepareMsg(Search, searchLen)
	mc.PrepareMsg(TimeSync, timeLen)

	return []int{insertOrDeleteLen}
}
