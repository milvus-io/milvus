package message_client

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	masterPb "github.com/czs007/suvlim/pkg/master/grpc/master"
	msgpb "github.com/czs007/suvlim/pkg/master/grpc/message"
	timesync "github.com/czs007/suvlim/timesync"
	"github.com/golang/protobuf/proto"
	"log"
	"strconv"
)

type MessageClient struct {
	// timesync
	timeSyncCfg *timesync.ReaderTimeSyncCfg

	// message channel
	searchChan  chan *msgpb.SearchMsg
	key2SegChan chan *msgpb.Key2SegMsg

	// pulsar
	client                   pulsar.Client
	searchResultProducer     pulsar.Producer
	segmentsStatisticProducer pulsar.Producer
	searchConsumer           pulsar.Consumer
	key2segConsumer          pulsar.Consumer

	// batch messages
	InsertOrDeleteMsg   []*msgpb.InsertOrDeleteMsg
	Key2SegMsg          []*msgpb.Key2SegMsg
	SearchMsg           []*msgpb.SearchMsg
	timestampBatchStart uint64
	timestampBatchEnd   uint64
	batchIDLen          int

	//
	MessageClientID int
}

func (mc *MessageClient) TimeSyncStart() uint64 {
	return mc.timestampBatchStart
}

func (mc *MessageClient) TimeSyncEnd() uint64 {
	return mc.timestampBatchEnd
}

func (mc *MessageClient) SendResult(ctx context.Context, msg msgpb.QueryResult) {
	var msgBuffer, _ = proto.Marshal(&msg)
	if _, err := mc.searchResultProducer.Send(ctx, &pulsar.ProducerMessage{
		Payload: msgBuffer,
	}); err != nil {
		log.Fatal(err)
	}
}

func (mc *MessageClient) SendSegmentsStatistic(ctx context.Context, statisticData *[]masterPb.SegmentStat) {
	for _, data := range *statisticData {
		var stat, _ = proto.Marshal(&data)
		if _, err := mc.segmentsStatisticProducer.Send(ctx, &pulsar.ProducerMessage{
			Payload: stat,
		}); err != nil {
			log.Fatal(err)
		}
	}
}

func (mc *MessageClient) GetSearchChan() <-chan *msgpb.SearchMsg {
	return mc.searchChan
}

func (mc *MessageClient) receiveSearchMsg() {
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

func (mc *MessageClient) receiveKey2SegMsg() {
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

	err := mc.timeSyncCfg.Start()
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}
	go mc.receiveSearchMsg()
	go mc.receiveKey2SegMsg()
}

func (mc *MessageClient) creatProducer(topicName string) pulsar.Producer {
	producer, err := mc.client.CreateProducer(pulsar.ProducerOptions{
		Topic: topicName,
	})

	if err != nil {
		log.Fatal(err)
	}
	return producer
}

func (mc *MessageClient) createConsumer(topicName string) pulsar.Consumer {
	consumer, err := mc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "reader" + strconv.Itoa(mc.MessageClientID),
	})

	if err != nil {
		log.Fatal(err)
	}
	return consumer
}

func (mc *MessageClient) createClient(url string) pulsar.Client {
	// create client
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: url,
	})

	if err != nil {
		log.Fatal(err)
	}
	return client
}

func (mc *MessageClient) InitClient(url string, numOfQueryNode int) {
	const ChannelNum = 1024

	//create client
	mc.client = mc.createClient(url)

	//create producer
	mc.searchResultProducer = mc.creatProducer("SearchResult")
	mc.segmentsStatisticProducer = mc.creatProducer("SegmentsStatistic")

	//create consumer
	mc.searchConsumer = mc.createConsumer("Search")
	mc.key2segConsumer = mc.createConsumer("Key2Seg")

	// init channel
	mc.searchChan = make(chan *msgpb.SearchMsg, 10000)
	mc.key2SegChan = make(chan *msgpb.Key2SegMsg, 10000)

	mc.InsertOrDeleteMsg = make([]*msgpb.InsertOrDeleteMsg, 0)
	mc.Key2SegMsg = make([]*msgpb.Key2SegMsg, 0)

	//init timesync
	URL := "pulsar://localhost:6650"
	timeSyncTopic := "TimeSync"
	timeSyncSubName := "reader" + strconv.Itoa(mc.MessageClientID)
	readTopics := make([]string, 0, ChannelNum)
	for i := ChannelNum / numOfQueryNode * mc.MessageClientID; i < ChannelNum/numOfQueryNode*(mc.MessageClientID+1); i++ {
		str := "InsertOrDelete-partition-"
		str = str + strconv.Itoa(i)
		readTopics = append(readTopics, str)
	}
	readSubName := "reader" + strconv.Itoa(mc.MessageClientID)
	proxyIdList := []int64{0}
	timeSync, err := timesync.NewReaderTimeSync(URL, timeSyncTopic, timeSyncSubName, readTopics, readSubName, proxyIdList, 400, -2, timesync.WithReaderQueueSize(ChannelNum))
	if err != nil {
		log.Fatal(err)
	}
	mc.timeSyncCfg = timeSync.(*timesync.ReaderTimeSyncCfg)

	mc.timestampBatchStart = 0
	mc.timestampBatchEnd = 0
	mc.batchIDLen = 0
}

func (mc *MessageClient) Close() {
	mc.client.Close()
	mc.searchResultProducer.Close()
	mc.segmentsStatisticProducer.Close()
	mc.searchConsumer.Close()
	mc.key2segConsumer.Close()
	mc.timeSyncCfg.Close()
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

func (mc *MessageClient) prepareMsg(messageType MessageType, msgLen int) {
	switch messageType {
	case InsertOrDelete:
		for i := 0; i < msgLen; i++ {
			msg := <-mc.timeSyncCfg.InsertOrDelete()
			mc.InsertOrDeleteMsg = append(mc.InsertOrDeleteMsg, msg)
		}
	case TimeSync:
		mc.timestampBatchStart = mc.timestampBatchEnd
		mc.batchIDLen = 0
		for i := 0; i < msgLen; i++ {
			msg, ok := <-mc.timeSyncCfg.TimeSync()
			if !ok {
				fmt.Println("cnn't get data from timesync chan")
			}
			if i == msgLen-1 {
				mc.timestampBatchEnd = msg.Timestamp
			}
			mc.batchIDLen += int(msg.NumRecorders)
		}
	}
}

func (mc *MessageClient) PrepareBatchMsg() []int {
	// assume the channel not full
	mc.InsertOrDeleteMsg = mc.InsertOrDeleteMsg[:0]
	mc.batchIDLen = 0

	// get the length of every channel
	timeLen := mc.timeSyncCfg.TimeSyncChanLen()

	// get message from channel to slice
	if timeLen > 0 {
		mc.prepareMsg(TimeSync, timeLen)
		mc.prepareMsg(InsertOrDelete, mc.batchIDLen)
	}
	return []int{mc.batchIDLen, timeLen}
}

func (mc *MessageClient) PrepareKey2SegmentMsg() {
	mc.Key2SegMsg = mc.Key2SegMsg[:0]
	msgLen := len(mc.key2SegChan)
	for i := 0; i < msgLen; i++ {
		msg := <-mc.key2SegChan
		mc.Key2SegMsg = append(mc.Key2SegMsg, msg)
	}
}
