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
	kc := newKafkaConsumer(config, topic, groupID)
	defer kc.Close()
	assert.Equal(t, kc.Subscription(), groupID)
}

func TestKafkaConsumer_Chan(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	groupID := fmt.Sprintf("test-groupid-%d", rand.Int())
	topic := fmt.Sprintf("test-topicName-%d", rand.Int())

	config := createConfig(groupID)
	consumer := newKafkaConsumer(config, topic, groupID)
	defer consumer.Close()

	data := []int{111, 222, 333}
	testKafkaConsumerProduceData(t, topic, data)

	msgID := &kafkaID{messageID: 1}
	err := consumer.Seek(msgID, false)
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
	consumer := newKafkaConsumer(config, topic, groupID)
	defer consumer.Close()

	msgID := &kafkaID{messageID: 0}
	err := consumer.Seek(msgID, false)
	assert.Nil(t, err)
}

func TestKafkaConsumer_GetLatestMsgID(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	groupID := fmt.Sprintf("test-groupid-%d", rand.Int())
	topic := fmt.Sprintf("test-topicName-%d", rand.Int())

	config := createConfig(groupID)
	consumer := newKafkaConsumer(config, topic, groupID)
	defer consumer.Close()

	latestMsgID, err := consumer.GetLatestMsgID()
	assert.Nil(t, latestMsgID)
	assert.NotNil(t, err)

	data := []int{111, 222, 333}
	testKafkaConsumerProduceData(t, topic, data)

	latestMsgID, err = consumer.GetLatestMsgID()
	assert.Equal(t, int64(2), latestMsgID.(*kafkaID).messageID)
	assert.Nil(t, err)
}

func testKafkaConsumerProduceData(t *testing.T, topic string, data []int) {
	ctx := context.Background()
	kc := createKafkaClient(t)
	defer kc.Close()
	producer := createProducer(t, kc, topic)
	defer producer.Close()

	produceData(ctx, t, producer, data)
}

func createConfig(groupID string) *kafka.ConfigMap {
	kafkaAddress, _ := Params.Load("_KafkaBrokerList")
	return &kafka.ConfigMap{
		"bootstrap.servers":        kafkaAddress,
		"group.id":                 groupID,
		"auto.offset.reset":        "earliest",
		"api.version.request":      "true",
		"go.events.channel.enable": true,
	}
}
