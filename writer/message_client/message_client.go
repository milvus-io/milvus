package message_client

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/czs007/suvlim/conf"
	msgpb "github.com/czs007/suvlim/pkg/master/grpc/message"
	timesync "github.com/czs007/suvlim/timesync"
	"github.com/golang/protobuf/proto"
	"log"
	"strconv"
)

type MessageClient struct {
	// timesync
	timeSyncCfg *timesync.ReaderTimeSyncCfg

	//message channel
	searchByIdChan chan *msgpb.EntityIdentity

	// pulsar
	client          pulsar.Client
	key2segProducer pulsar.Producer
	searchByIdConsumer pulsar.Consumer

	// batch messages
	InsertMsg           []*msgpb.InsertOrDeleteMsg
	DeleteMsg           []*msgpb.InsertOrDeleteMsg
	timestampBatchStart uint64
	timestampBatchEnd   uint64
	batchIDLen          int

	//client id
	MessageClientID int
}

func (mc *MessageClient) Send(ctx context.Context, msg msgpb.Key2SegMsg) {
	var msgBuffer, _ = proto.Marshal(&msg)
	if _, err := mc.key2segProducer.Send(ctx, &pulsar.ProducerMessage{
		Payload: msgBuffer,
	}); err != nil {
		log.Fatal(err)
	}
}

func (mc *MessageClient) TimeSync() uint64 {
	return mc.timestampBatchEnd
}

func (mc *MessageClient) SearchByIdChan() chan *msgpb.EntityIdentity {
	return mc.searchByIdChan
}

func (mc *MessageClient) receiveSearchByIdMsg() {
	for {
		searchByIdMsg := msgpb.EntityIdentity{}
		msg, err := mc.searchByIdConsumer.Receive(context.Background())
		err = proto.Unmarshal(msg.Payload(), &searchByIdMsg)
		if err != nil {
			log.Fatal(err)
		}
		mc.searchByIdChan <- &searchByIdMsg
		mc.searchByIdConsumer.Ack(msg)
	}
}


func (mc *MessageClient) ReceiveMessage() {
	err := mc.timeSyncCfg.Start()
	if err != nil {
		log.Fatal(err)
	}
	go mc.receiveSearchByIdMsg()
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
		SubscriptionName: "writer",
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

func (mc *MessageClient) InitClient(url string) {
	//create client
	mc.client = mc.createClient(url)
	mc.MessageClientID = conf.Config.Writer.ClientId

	//create producer
	mc.key2segProducer = mc.creatProducer("Key2Seg")

	//create consumer
	mc.searchByIdConsumer = mc.createConsumer("SearchById")

	//init channel
	mc.searchByIdChan = make(chan *msgpb.EntityIdentity, conf.Config.Writer.SearchByIdChanSize)

	//init msg slice
	mc.InsertMsg = make([]*msgpb.InsertOrDeleteMsg, 0)
	mc.DeleteMsg = make([]*msgpb.InsertOrDeleteMsg, 0)

	//init timesync
	timeSyncTopic := "TimeSync"
	timeSyncSubName := "writer" + strconv.Itoa(mc.MessageClientID)
	readTopics := make([]string, 0)
	for i := conf.Config.Writer.TopicStart; i < conf.Config.Writer.TopicEnd; i++ {
		str := "InsertOrDelete-"
		str = str + strconv.Itoa(i)
		readTopics = append(readTopics, str)
	}
	readSubName := "writer" + strconv.Itoa(mc.MessageClientID)
	proxyIdList := conf.Config.Master.ProxyIdList
	readerQueueSize := timesync.WithReaderQueueSize(conf.Config.Reader.ReaderQueueSize)
	timeSync, err := timesync.NewReaderTimeSync(timeSyncTopic,
		timeSyncSubName,
		readTopics,
		readSubName,
		proxyIdList,
		conf.Config.Writer.StopFlag,
		readerQueueSize)
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
	mc.key2segProducer.Close()
	mc.searchByIdConsumer.Close()
	mc.timeSyncCfg.Close()
}

type MessageType int

const (
	InsertOrDelete MessageType = 0
	Delete         MessageType = 1
	SearchById     MessageType = 2
	TimeSync       MessageType = 3
	Key2Seg        MessageType = 4
	Statistics     MessageType = 5
)

func (mc *MessageClient) PrepareMsg(messageType MessageType, msgLen int) {
	switch messageType {
	case InsertOrDelete:
		for i := 0; i < msgLen; i++ {
			msg := <-mc.timeSyncCfg.InsertOrDelete()
			if msg.Op == msgpb.OpType_INSERT {
				mc.InsertMsg = append(mc.InsertMsg, msg)
			} else {
				mc.DeleteMsg = append(mc.DeleteMsg, msg)
			}
		}
	case TimeSync:
		mc.timestampBatchStart = mc.timestampBatchEnd
		mc.batchIDLen = 0
		for i := 0; i < msgLen; i++ {
			msg := <-mc.timeSyncCfg.TimeSync()
			if i == msgLen-1 {
				mc.timestampBatchEnd = msg.Timestamp
			}
			mc.batchIDLen += int(msg.NumRecorders)
		}
	}
}

func (mc *MessageClient) PrepareBatchMsg() int {
	// assume the channel not full
	mc.InsertMsg = mc.InsertMsg[:0]
	mc.DeleteMsg = mc.DeleteMsg[:0]
	mc.batchIDLen = 0

	// get the length of every channel
	timeLen := len(mc.timeSyncCfg.TimeSync())

	// get message from channel to slice
	if timeLen > 0 {
		mc.PrepareMsg(TimeSync, timeLen)
		mc.PrepareMsg(InsertOrDelete, mc.batchIDLen)
	}
	//return []int{insertOrDeleteLen, searchLen, timeLen}
	return mc.batchIDLen
}
