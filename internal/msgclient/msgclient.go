package msgclient

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	msgpb "github.com/zilliztech/milvus-distributed/internal/proto/message"
	"github.com/zilliztech/milvus-distributed/internal/timesync"
)

type MessageType int

const (
	InsertOrDelete MessageType = 0
	Search         MessageType = 1
	SearchById     MessageType = 2
	TimeSync       MessageType = 3
	Key2Seg        MessageType = 4
	Statistics     MessageType = 5
)

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

type ReaderMessageClient struct {
	// context
	ctx context.Context

	// timesync
	timeSyncCfg *timesync.TimeSyncCfg

	// message channel
	searchChan  chan *msgpb.SearchMsg
	Key2SegChan chan *msgpb.Key2SegMsg

	// pulsar
	client pulsar.Client
	//searchResultProducer     pulsar.Producer
	searchResultProducers     map[UniqueID]pulsar.Producer
	segmentsStatisticProducer pulsar.Producer
	searchConsumer            pulsar.Consumer
	key2segConsumer           pulsar.Consumer

	// batch messages
	InsertOrDeleteMsg   []*msgpb.InsertOrDeleteMsg
	Key2SegMsg          []*msgpb.Key2SegMsg
	SearchMsg           []*msgpb.SearchMsg
	timestampBatchStart Timestamp
	timestampBatchEnd   Timestamp
	batchIDLen          int

	//client id
	MessageClientID int
}

func (mc *ReaderMessageClient) GetTimeNow() Timestamp {
	return mc.timestampBatchEnd
}

func (mc *ReaderMessageClient) TimeSyncStart() Timestamp {
	return mc.timestampBatchStart
}

func (mc *ReaderMessageClient) TimeSyncEnd() Timestamp {
	return mc.timestampBatchEnd
}

func (mc *ReaderMessageClient) SendResult(ctx context.Context, msg msgpb.QueryResult, producerKey UniqueID) {
	var msgBuffer, _ = proto.Marshal(&msg)
	if _, err := mc.searchResultProducers[producerKey].Send(ctx, &pulsar.ProducerMessage{
		Payload: msgBuffer,
	}); err != nil {
		log.Fatal(err)
	}
}

func (mc *ReaderMessageClient) SendSegmentsStatistic(ctx context.Context, statisticData *[]internalpb.SegmentStatistics) {
	for _, data := range *statisticData {
		var stat, _ = proto.Marshal(&data)
		if _, err := mc.segmentsStatisticProducer.Send(ctx, &pulsar.ProducerMessage{
			Payload: stat,
		}); err != nil {
			log.Fatal(err)
		}
	}
}

func (mc *ReaderMessageClient) GetSearchChan() <-chan *msgpb.SearchMsg {
	return mc.searchChan
}

func (mc *ReaderMessageClient) receiveSearchMsg() {
	for {
		select {
		case <-mc.ctx.Done():
			return
		default:
			searchMsg := msgpb.SearchMsg{}
			msg, err := mc.searchConsumer.Receive(mc.ctx)
			if err != nil {
				log.Println(err)
				continue
			}
			err = proto.Unmarshal(msg.Payload(), &searchMsg)
			if err != nil {
				log.Fatal(err)
			}
			mc.searchChan <- &searchMsg
			mc.searchConsumer.Ack(msg)
		}
	}
}

func (mc *ReaderMessageClient) receiveKey2SegMsg() {
	for {
		select {
		case <-mc.ctx.Done():
			return
		default:
			key2SegMsg := msgpb.Key2SegMsg{}
			msg, err := mc.key2segConsumer.Receive(mc.ctx)
			if err != nil {
				log.Println(err)
				continue
			}
			err = proto.Unmarshal(msg.Payload(), &key2SegMsg)
			if err != nil {
				log.Fatal(err)
			}
			mc.Key2SegChan <- &key2SegMsg
			mc.key2segConsumer.Ack(msg)
		}
	}
}

func (mc *ReaderMessageClient) ReceiveMessage() {

	err := mc.timeSyncCfg.Start()
	if err != nil {
		log.Println(err)
	}
	go mc.receiveSearchMsg()
	go mc.receiveKey2SegMsg()
}

func (mc *ReaderMessageClient) creatProducer(topicName string) pulsar.Producer {
	producer, err := mc.client.CreateProducer(pulsar.ProducerOptions{
		Topic: topicName,
	})

	if err != nil {
		log.Fatal(err)
	}
	return producer
}

func (mc *ReaderMessageClient) createConsumer(topicName string) pulsar.Consumer {
	consumer, err := mc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "reader" + strconv.Itoa(mc.MessageClientID),
	})

	if err != nil {
		log.Fatal(err)
	}
	return consumer
}

func (mc *ReaderMessageClient) createClient(url string) pulsar.Client {
	if conf.Config.Pulsar.Authentication {
		// create client with Authentication
		client, err := pulsar.NewClient(pulsar.ClientOptions{
			URL:            url,
			Authentication: pulsar.NewAuthenticationToken(conf.Config.Pulsar.Token),
		})

		if err != nil {
			log.Fatal(err)
		}
		return client
	}

	// create client without Authentication
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: url,
	})

	if err != nil {
		log.Fatal(err)
	}
	return client
}

func (mc *ReaderMessageClient) InitClient(ctx context.Context, url string) {
	// init context
	mc.ctx = ctx

	//create client
	mc.client = mc.createClient(url)
	mc.MessageClientID = conf.Config.Reader.ClientId

	//create producer
	mc.searchResultProducers = make(map[UniqueID]pulsar.Producer)
	proxyIdList := conf.Config.Master.ProxyIdList

	searchResultTopicName := "SearchResult-"
	searchTopicName := "search"
	key2SegTopicName := "Key2Seg"
	timeSyncTopicName := "TimeSync"
	insertOrDeleteTopicName := "InsertOrDelete-"

	if conf.Config.Pulsar.Authentication {
		searchResultTopicName = "SearchResult-" + conf.Config.Pulsar.User + "-"
		searchTopicName = "search-" + conf.Config.Pulsar.User
		key2SegTopicName = "Key2Seg-" + conf.Config.Pulsar.User
		// timeSyncTopicName = "TimeSync-" + conf.Config.Pulsar.User
		insertOrDeleteTopicName = "InsertOrDelete-" + conf.Config.Pulsar.User + "-"
	}

	for _, key := range proxyIdList {
		topic := searchResultTopicName
		topic = topic + strconv.Itoa(int(key))
		mc.searchResultProducers[key] = mc.creatProducer(topic)
	}
	//mc.searchResultProducer = mc.creatProducer("SearchResult")
	SegmentsStatisticTopicName := conf.Config.Master.PulsarTopic
	mc.segmentsStatisticProducer = mc.creatProducer(SegmentsStatisticTopicName)

	//create consumer
	mc.searchConsumer = mc.createConsumer(searchTopicName)
	mc.key2segConsumer = mc.createConsumer(key2SegTopicName)

	// init channel
	mc.searchChan = make(chan *msgpb.SearchMsg, conf.Config.Reader.SearchChanSize)
	mc.Key2SegChan = make(chan *msgpb.Key2SegMsg, conf.Config.Reader.Key2SegChanSize)

	mc.InsertOrDeleteMsg = make([]*msgpb.InsertOrDeleteMsg, 0)
	mc.Key2SegMsg = make([]*msgpb.Key2SegMsg, 0)

	//init timesync
	timeSyncTopic := timeSyncTopicName
	timeSyncSubName := "reader" + strconv.Itoa(mc.MessageClientID)
	readTopics := make([]string, 0)
	for i := conf.Config.Reader.TopicStart; i < conf.Config.Reader.TopicEnd; i++ {
		str := insertOrDeleteTopicName
		str = str + strconv.Itoa(i)
		readTopics = append(readTopics, str)
	}

	readSubName := "reader" + strconv.Itoa(mc.MessageClientID)
	readerQueueSize := timesync.WithReaderQueueSize(conf.Config.Reader.ReaderQueueSize)
	timeSync, err := timesync.NewTimeSync(ctx,
		timeSyncTopic,
		timeSyncSubName,
		readTopics,
		readSubName,
		proxyIdList,
		conf.Config.Reader.StopFlag,
		readerQueueSize)
	if err != nil {
		log.Fatal(err)
	}
	mc.timeSyncCfg = timeSync.(*timesync.TimeSyncCfg)
	mc.timeSyncCfg.RoleType = timesync.Reader

	mc.timestampBatchStart = 0
	mc.timestampBatchEnd = 0
	mc.batchIDLen = 0
}

func (mc *ReaderMessageClient) Close() {
	if mc.client != nil {
		mc.client.Close()
	}
	for key, _ := range mc.searchResultProducers {
		if mc.searchResultProducers[key] != nil {
			mc.searchResultProducers[key].Close()
		}
	}
	if mc.segmentsStatisticProducer != nil {
		mc.segmentsStatisticProducer.Close()
	}
	if mc.searchConsumer != nil {
		mc.searchConsumer.Close()
	}
	if mc.key2segConsumer != nil {
		mc.key2segConsumer.Close()
	}
	if mc.timeSyncCfg != nil {
		mc.timeSyncCfg.Close()
	}
}

func (mc *ReaderMessageClient) prepareMsg(messageType MessageType, msgLen int) {
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

func (mc *ReaderMessageClient) PrepareBatchMsg() []int {
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

func (mc *ReaderMessageClient) PrepareKey2SegmentMsg() {
	mc.Key2SegMsg = mc.Key2SegMsg[:0]
	msgLen := len(mc.Key2SegChan)
	for i := 0; i < msgLen; i++ {
		msg := <-mc.Key2SegChan
		mc.Key2SegMsg = append(mc.Key2SegMsg, msg)
	}
}

type WriterMessageClient struct {
	// timesync
	timeSyncCfg *timesync.TimeSyncCfg

	//message channel
	searchByIdChan chan *msgpb.EntityIdentity

	// pulsar
	client             pulsar.Client
	key2segProducer    pulsar.Producer
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

func (mc *WriterMessageClient) Send(ctx context.Context, msg msgpb.Key2SegMsg) {
	var msgBuffer, _ = proto.Marshal(&msg)
	if _, err := mc.key2segProducer.Send(ctx, &pulsar.ProducerMessage{
		Payload: msgBuffer,
	}); err != nil {
		log.Fatal(err)
	}
}

func (mc *WriterMessageClient) TimeSync() uint64 {
	return mc.timestampBatchEnd
}

func (mc *WriterMessageClient) SearchByIdChan() chan *msgpb.EntityIdentity {
	return mc.searchByIdChan
}

func (mc *WriterMessageClient) receiveSearchByIdMsg() {
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

func (mc *WriterMessageClient) ReceiveMessage() {
	err := mc.timeSyncCfg.Start()
	if err != nil {
		log.Fatal(err)
	}
	go mc.receiveSearchByIdMsg()
}

func (mc *WriterMessageClient) creatProducer(topicName string) pulsar.Producer {
	producer, err := mc.client.CreateProducer(pulsar.ProducerOptions{
		Topic: topicName,
	})

	if err != nil {
		log.Fatal(err)
	}
	return producer
}

func (mc *WriterMessageClient) createConsumer(topicName string) pulsar.Consumer {
	consumer, err := mc.client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "writer" + strconv.Itoa(mc.MessageClientID),
	})

	if err != nil {
		log.Fatal(err)
	}
	return consumer
}

func (mc *WriterMessageClient) createClient(url string) pulsar.Client {
	if conf.Config.Pulsar.Authentication {
		// create client with Authentication
		client, err := pulsar.NewClient(pulsar.ClientOptions{
			URL:            url,
			Authentication: pulsar.NewAuthenticationToken(conf.Config.Pulsar.Token),
		})

		if err != nil {
			log.Fatal(err)
		}
		return client
	}

	// create client without Authentication
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: url,
	})

	if err != nil {
		log.Fatal(err)
	}
	return client
}

func (mc *WriterMessageClient) InitClient(url string) {
	//create client
	mc.client = mc.createClient(url)
	mc.MessageClientID = conf.Config.Writer.ClientId

	key2SegTopicName := "Key2Seg"
	searchByIdTopicName := "SearchById"
	timeSyncTopicName := "TimeSync"
	insertOrDeleteTopicName := "InsertOrDelete-"

	if conf.Config.Pulsar.Authentication {
		key2SegTopicName = "Key2Seg-" + conf.Config.Pulsar.User
		searchByIdTopicName = "search-" + conf.Config.Pulsar.User
		// timeSyncTopicName = "TimeSync-" + conf.Config.Pulsar.User
		insertOrDeleteTopicName = "InsertOrDelete-" + conf.Config.Pulsar.User + "-"
	}

	//create producer
	mc.key2segProducer = mc.creatProducer(key2SegTopicName)

	//create consumer
	mc.searchByIdConsumer = mc.createConsumer(searchByIdTopicName)

	//init channel
	mc.searchByIdChan = make(chan *msgpb.EntityIdentity, conf.Config.Writer.SearchByIdChanSize)

	//init msg slice
	mc.InsertMsg = make([]*msgpb.InsertOrDeleteMsg, 0)
	mc.DeleteMsg = make([]*msgpb.InsertOrDeleteMsg, 0)

	//init timesync
	timeSyncTopic := timeSyncTopicName
	timeSyncSubName := "writer" + strconv.Itoa(mc.MessageClientID)
	readTopics := make([]string, 0)
	for i := conf.Config.Writer.TopicStart; i < conf.Config.Writer.TopicEnd; i++ {
		str := insertOrDeleteTopicName
		str = str + strconv.Itoa(i)
		readTopics = append(readTopics, str)
	}
	readSubName := "writer" + strconv.Itoa(mc.MessageClientID)
	proxyIdList := conf.Config.Master.ProxyIdList
	readerQueueSize := timesync.WithReaderQueueSize(conf.Config.Reader.ReaderQueueSize)
	timeSync, err := timesync.NewTimeSync(context.Background(),
		timeSyncTopic,
		timeSyncSubName,
		readTopics,
		readSubName,
		proxyIdList,
		conf.Config.Writer.StopFlag,
		readerQueueSize)
	if err != nil {
		log.Fatal(err)
	}
	mc.timeSyncCfg = timeSync.(*timesync.TimeSyncCfg)
	mc.timeSyncCfg.RoleType = timesync.Writer

	mc.timestampBatchStart = 0
	mc.timestampBatchEnd = 0
	mc.batchIDLen = 0
}

func (mc *WriterMessageClient) Close() {
	mc.client.Close()
	mc.key2segProducer.Close()
	mc.searchByIdConsumer.Close()
	mc.timeSyncCfg.Close()
}

func (mc *WriterMessageClient) PrepareMsg(messageType MessageType, msgLen int) {
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

func (mc *WriterMessageClient) PrepareBatchMsg() int {
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
