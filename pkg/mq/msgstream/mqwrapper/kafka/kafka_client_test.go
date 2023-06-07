package kafka

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var Params = paramtable.Get()

func TestMain(m *testing.M) {
	Params.Init()
	mockCluster, err := kafka.NewMockCluster(1)
	defer mockCluster.Close()
	if err != nil {
		// nolint
		fmt.Printf("Failed to create MockCluster: %s\n", err)
		os.Exit(1)
	}

	broker := mockCluster.BootstrapServers()
	Params.Save("kafka.brokerList", broker)

	exitCode := m.Run()
	os.Exit(exitCode)
}

func getKafkaBrokerList() string {
	brokerList := Params.Get("kafka.brokerList")
	log.Info("get kafka broker list.", zap.String("address", brokerList))
	return brokerList
}

func IntToBytes(n int) []byte {
	tmp := int32(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, common.Endian, tmp)
	return bytesBuffer.Bytes()
}

func BytesToInt(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)
	var tmp int32
	binary.Read(bytesBuffer, common.Endian, &tmp)
	return int(tmp)
}

// Consume1 will consume random messages and record the last MessageID it received
func Consume1(ctx context.Context, t *testing.T, kc *kafkaClient, topic string, subName string, c chan mqwrapper.MessageID, total *int) {
	consumer, err := kc.Subscribe(mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		BufSize:                     1024,
		SubscriptionInitialPosition: mqwrapper.SubscriptionPositionEarliest,
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	// get random number between 1 ~ 5
	rand.Seed(time.Now().UnixNano())
	cnt := 1 + rand.Int()%5

	log.Info("Consume1 start")
	var msg mqwrapper.Message
	for i := 0; i < cnt; i++ {
		select {
		case <-ctx.Done():
			log.Info("Consume1 channel closed")
			return
		case msg = <-consumer.Chan():
			if msg == nil {
				return
			}

			log.Info("Consume1 RECV", zap.Any("v", BytesToInt(msg.Payload())))
			consumer.Ack(msg)
			(*total)++
		}
	}

	c <- msg.ID()
	log.Info("Consume1 randomly RECV", zap.Any("number", cnt))
	log.Info("Consume1 done")
}

// Consume2 will consume messages from specified MessageID
func Consume2(ctx context.Context, t *testing.T, kc *kafkaClient, topic string, subName string, msgID mqwrapper.MessageID, total *int) {
	consumer, err := kc.Subscribe(mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		BufSize:                     1024,
		SubscriptionInitialPosition: mqwrapper.SubscriptionPositionUnknown,
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	err = consumer.Seek(msgID, true)
	assert.NoError(t, err)

	mm := <-consumer.Chan()
	consumer.Ack(mm)
	log.Info("skip the last received message", zap.Any("skip msg", mm.ID()))

	log.Info("Consume2 start")
	for {
		select {
		case <-ctx.Done():
			log.Info("Consume2 channel closed")
			return
		case msg, ok := <-consumer.Chan():
			if msg == nil || !ok {
				return
			}

			log.Info("Consume2 RECV", zap.Any("v", BytesToInt(msg.Payload())))
			consumer.Ack(msg)
			(*total)++
		}
	}
}

func Consume3(ctx context.Context, t *testing.T, kc *kafkaClient, topic string, subName string, total *int) {
	consumer, err := kc.Subscribe(mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		BufSize:                     1024,
		SubscriptionInitialPosition: mqwrapper.SubscriptionPositionEarliest,
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	log.Info("Consume3 start")
	for {
		select {
		case <-ctx.Done():
			log.Info("Consume3 channel closed")
			return
		case msg, ok := <-consumer.Chan():
			if msg == nil || !ok {
				return
			}

			consumer.Ack(msg)
			(*total)++
			log.Info("Consume3 RECV", zap.Any("v", BytesToInt(msg.Payload())), zap.Int("total", *total))
		}
	}
}

func TestKafkaClient_ConsumeWithAck(t *testing.T) {
	kc := createKafkaClient(t)
	defer kc.Close()
	assert.NotNil(t, kc)

	rand.Seed(time.Now().UnixNano())
	topic := fmt.Sprintf("test-topic-%d", rand.Int())
	subName := fmt.Sprintf("test-subname-%d", rand.Int())
	arr1 := []int{111, 222, 333, 444, 555, 666, 777}
	arr2 := []string{"111", "222", "333", "444", "555", "666", "777"}

	c := make(chan mqwrapper.MessageID, 1)

	ctx, cancel := context.WithCancel(context.Background())

	var total1 int
	var total2 int
	var total3 int

	producer := createProducer(t, kc, topic)
	defer producer.Close()
	produceData(ctx, t, producer, arr1, arr2)
	time.Sleep(100 * time.Millisecond)

	ctx1, cancel1 := context.WithTimeout(ctx, 5*time.Second)
	defer cancel1()
	Consume1(ctx1, t, kc, topic, subName, c, &total1)

	lastMsgID := <-c
	log.Info("lastMsgID", zap.Any("lastMsgID", lastMsgID.(*kafkaID).messageID))

	ctx2, cancel2 := context.WithTimeout(ctx, 3*time.Second)
	Consume2(ctx2, t, kc, topic, subName, lastMsgID, &total2)
	cancel2()

	time.Sleep(5 * time.Second)
	ctx3, cancel3 := context.WithTimeout(ctx, 3*time.Second)
	Consume3(ctx3, t, kc, topic, subName, &total3)
	cancel3()

	cancel()
	assert.Equal(t, len(arr1), total1+total2)

	assert.Equal(t, len(arr1), total3)
}

func TestKafkaClient_SeekPosition(t *testing.T) {
	kc := createKafkaClient(t)
	defer kc.Close()

	rand.Seed(time.Now().UnixNano())
	ctx := context.Background()
	topic := fmt.Sprintf("test-topic-%d", rand.Int())
	subName := fmt.Sprintf("test-subname-%d", rand.Int())

	producer := createProducer(t, kc, topic)
	defer producer.Close()

	data1 := []int{1, 2, 3}
	data2 := []string{"1", "2", "3"}
	ids := produceData(ctx, t, producer, data1, data2)

	consumer := createConsumer(t, kc, topic, subName, mqwrapper.SubscriptionPositionUnknown)
	defer consumer.Close()

	err := consumer.Seek(ids[2], true)
	assert.NoError(t, err)

	select {
	case msg := <-consumer.Chan():
		consumer.Ack(msg)
		assert.Equal(t, 3, BytesToInt(msg.Payload()))
		assert.Equal(t, "3", msg.Properties()[common.TraceIDKey])
	case <-time.After(10 * time.Second):
		assert.FailNow(t, "should not wait")
	}
}

func TestKafkaClient_ConsumeFromLatest(t *testing.T) {
	kc := createKafkaClient(t)
	defer kc.Close()

	rand.Seed(time.Now().UnixNano())
	ctx := context.Background()
	topic := fmt.Sprintf("test-topic-%d", rand.Int())
	subName := fmt.Sprintf("test-subname-%d", rand.Int())

	producer := createProducer(t, kc, topic)
	defer producer.Close()

	data1 := []int{1, 2}
	data2 := []string{"1", "2"}
	produceData(ctx, t, producer, data1, data2)

	consumer := createConsumer(t, kc, topic, subName, mqwrapper.SubscriptionPositionLatest)
	defer consumer.Close()

	go func() {
		time.Sleep(time.Second * 2)
		data1 := []int{3}
		data2 := []string{"3"}
		produceData(ctx, t, producer, data1, data2)
	}()

	select {
	case msg := <-consumer.Chan():
		consumer.Ack(msg)
		assert.Equal(t, 3, BytesToInt(msg.Payload()))
		assert.Equal(t, "3", msg.Properties()[common.TraceIDKey])
	case <-time.After(5 * time.Second):
		assert.FailNow(t, "should not wait")
	}
}

func TestKafkaClient_EarliestMessageID(t *testing.T) {
	kafkaAddress := getKafkaBrokerList()
	kc := NewKafkaClientInstance(kafkaAddress)
	defer kc.Close()

	mid := kc.EarliestMessageID()
	assert.NotNil(t, mid)
}

func TestKafkaClient_MsgSerializAndDeserialize(t *testing.T) {
	kafkaAddress := getKafkaBrokerList()
	kc := NewKafkaClientInstance(kafkaAddress)
	defer kc.Close()

	mid := kc.EarliestMessageID()
	msgID, err := kc.BytesToMsgID(mid.Serialize())
	assert.NoError(t, err)
	assert.True(t, msgID.AtEarliestPosition())

	msgID, err = kc.StringToMsgID("1")
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	msgID, err = kc.StringToMsgID("1.0")
	assert.Error(t, err)
	assert.Nil(t, msgID)
}

func createParamItem(v string) paramtable.ParamItem {
	item := paramtable.ParamItem{
		Formatter: func(originValue string) string { return v },
	}
	item.Init(&config.Manager{})
	return item
}

func TestKafkaClient_NewKafkaClientInstanceWithConfig(t *testing.T) {
	config1 := &paramtable.KafkaConfig{
		Address:      createParamItem("addr"),
		SaslPassword: createParamItem("password"),
	}
	assert.Panics(t, func() { NewKafkaClientInstanceWithConfig(config1) })

	config2 := &paramtable.KafkaConfig{
		Address:      createParamItem("addr"),
		SaslUsername: createParamItem("username"),
	}
	assert.Panics(t, func() { NewKafkaClientInstanceWithConfig(config2) })

	producerConfig := make(map[string]string)
	producerConfig["client.id"] = "dc1"
	consumerConfig := make(map[string]string)
	consumerConfig["client.id"] = "dc"

	config := &paramtable.KafkaConfig{
		Address:             createParamItem("addr"),
		SaslUsername:        createParamItem("username"),
		SaslPassword:        createParamItem("password"),
		SaslMechanisms:      createParamItem("sasl"),
		SecurityProtocol:    createParamItem("plain"),
		ConsumerExtraConfig: paramtable.ParamGroup{GetFunc: func() map[string]string { return consumerConfig }},
		ProducerExtraConfig: paramtable.ParamGroup{GetFunc: func() map[string]string { return producerConfig }},
	}
	client := NewKafkaClientInstanceWithConfig(config)
	assert.NotNil(t, client)
	assert.NotNil(t, client.basicConfig)

	assert.Equal(t, "dc", client.consumerConfig["client.id"])
	newConsumerConfig := client.newConsumerConfig("test", 0)
	clientID, err := newConsumerConfig.Get("client.id", "")
	assert.NoError(t, err)
	assert.Equal(t, "dc", clientID)

	assert.Equal(t, "dc1", client.producerConfig["client.id"])
	newProducerConfig := client.newProducerConfig()
	pClientID, err := newProducerConfig.Get("client.id", "")
	assert.NoError(t, err)
	assert.Equal(t, pClientID, "dc1")
}

func createKafkaClient(t *testing.T) *kafkaClient {
	kafkaAddress := getKafkaBrokerList()
	kc := NewKafkaClientInstance(kafkaAddress)
	assert.NotNil(t, kc)
	return kc
}

func createConsumer(t *testing.T,
	kc *kafkaClient,
	topic string,
	groupID string,
	initPosition mqwrapper.SubscriptionInitialPosition) mqwrapper.Consumer {
	consumer, err := kc.Subscribe(mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            groupID,
		BufSize:                     1024,
		SubscriptionInitialPosition: initPosition,
	})
	assert.NoError(t, err)
	return consumer
}

func createProducer(t *testing.T, kc *kafkaClient, topic string) mqwrapper.Producer {
	producer, err := kc.CreateProducer(mqwrapper.ProducerOptions{Topic: topic})
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	return producer
}

func produceData(ctx context.Context, t *testing.T, producer mqwrapper.Producer, arr []int, pArr []string) []mqwrapper.MessageID {
	var msgIDs []mqwrapper.MessageID
	for k, v := range arr {
		msg := &mqwrapper.ProducerMessage{
			Payload: IntToBytes(v),
			Properties: map[string]string{
				common.TraceIDKey: pArr[k],
			},
		}
		msgID, err := producer.Send(ctx, msg)
		msgIDs = append(msgIDs, msgID)
		assert.NoError(t, err)
	}

	producer.(*kafkaProducer).p.Flush(500)
	return msgIDs
}
