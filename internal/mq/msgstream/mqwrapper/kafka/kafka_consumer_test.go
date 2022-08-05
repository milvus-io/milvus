package kafka

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

func TestKafkaConsumer_Subscription(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	groupID := fmt.Sprintf("test-groupid-%d", rand.Int())
	topic := fmt.Sprintf("test-topicName-%d", rand.Int())

	config := createConfig(groupID)
	kc, err := newKafkaConsumer(config, topic, groupID)
	assert.NoError(t, err)
	defer kc.Close()
	assert.Equal(t, kc.Subscription(), groupID)
}

func TestKafkaConsumer_Chan(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	groupID := fmt.Sprintf("test-groupid-%d", rand.Int())
	topic := fmt.Sprintf("test-topicName-%d", rand.Int())

	config := createConfig(groupID)
	consumer, err := newKafkaConsumer(config, topic, groupID)
	assert.NoError(t, err)
	defer consumer.Close()

	data := []int{111, 222, 333}
	testKafkaConsumerProduceData(t, topic, data)

	msgID := &kafkaID{messageID: 1}
	err = consumer.Seek(msgID, false)
	assert.Nil(t, err)

	msg := <-consumer.Chan()
	assert.Equal(t, 333, BytesToInt(msg.Payload()))
	assert.Equal(t, int64(2), msg.ID().(*kafkaID).messageID)
	assert.Equal(t, topic, msg.Topic())
	assert.True(t, len(msg.Properties()) == 0)
}

func TestKafkaConsumer_GetSeek(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	groupID := fmt.Sprintf("test-groupid-%d", rand.Int())
	topic := fmt.Sprintf("test-topicName-%d", rand.Int())

	config := createConfig(groupID)
	consumer, err := newKafkaConsumer(config, topic, groupID)
	assert.NoError(t, err)
	defer consumer.Close()

	msgID := &kafkaID{messageID: 0}
	err = consumer.Seek(msgID, false)
	assert.Nil(t, err)

	assert.Error(t, consumer.Seek(msgID, false))
}

func TestKafkaConsumer_SeekAfterChan(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	groupID := fmt.Sprintf("test-groupid-%d", rand.Int())
	topic := fmt.Sprintf("test-topicName-%d", rand.Int())

	config := createConfig(groupID)
	consumer, err := newKafkaConsumer(config, topic, groupID)
	assert.NoError(t, err)
	defer consumer.Close()

	data := []int{111}
	testKafkaConsumerProduceData(t, topic, data)
	msg := <-consumer.Chan()
	assert.Equal(t, 111, BytesToInt(msg.Payload()))

	assert.Panics(t, func() {
		consumer.Seek(nil, false)
	})
}

func TestKafkaConsumer_GetLatestMsgID(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	groupID := fmt.Sprintf("test-groupid-%d", rand.Int())
	topic := fmt.Sprintf("test-topicName-%d", rand.Int())

	config := createConfig(groupID)
	consumer, err := newKafkaConsumer(config, topic, groupID)
	assert.NoError(t, err)
	defer consumer.Close()

	latestMsgID, err := consumer.GetLatestMsgID()
	assert.Equal(t, int64(0), latestMsgID.(*kafkaID).messageID)
	assert.Nil(t, err)

	data := []int{111, 222, 333}
	testKafkaConsumerProduceData(t, topic, data)

	latestMsgID, err = consumer.GetLatestMsgID()
	assert.Equal(t, int64(2), latestMsgID.(*kafkaID).messageID)
	assert.Nil(t, err)
}

func TestKafkaConsumer_ConsumeFromLatest(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	groupID := fmt.Sprintf("test-groupid-%d", rand.Int())
	topic := fmt.Sprintf("test-topicName-%d", rand.Int())

	data := []int{111, 222, 333}
	testKafkaConsumerProduceData(t, topic, data)

	config := createConfig(groupID)
	config.SetKey("auto.offset.reset", "latest")
	consumer, err := newKafkaConsumer(config, topic, groupID)
	assert.NoError(t, err)
	defer consumer.Close()

	data = []int{444, 555}
	testKafkaConsumerProduceData(t, topic, data)

	msg := <-consumer.Chan()
	assert.Equal(t, 444, BytesToInt(msg.Payload()))
	msg = <-consumer.Chan()
	assert.Equal(t, 555, BytesToInt(msg.Payload()))
}

func TestKafkaConsumer_createKafkaConsumer(t *testing.T) {
	consumer := &Consumer{config: &kafka.ConfigMap{}}
	err := consumer.createKafkaConsumer()
	assert.NotNil(t, err)
}

func testKafkaConsumerProduceData(t *testing.T, topic string, data []int) {
	ctx := context.Background()
	kc := createKafkaClient(t)
	defer kc.Close()
	producer := createProducer(t, kc, topic)
	defer producer.Close()

	produceData(ctx, t, producer, data)

	producer.(*kafkaProducer).p.Flush(500)
}

func createConfig(groupID string) *kafka.ConfigMap {
	kafkaAddress := getKafkaBrokerList()
	return &kafka.ConfigMap{
		"bootstrap.servers":        kafkaAddress,
		"group.id":                 groupID,
		"auto.offset.reset":        "earliest",
		"api.version.request":      "true",
		"go.events.channel.enable": true,
	}
}
