package kafka

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

func TestKafkaConsumer_Subscription(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	groupID := fmt.Sprintf("test-groupid-%d", rand.Int())
	topic := fmt.Sprintf("test-topicName-%d", rand.Int())

	config := createConfig(groupID)
	kc, err := newKafkaConsumer(config, topic, groupID, mqwrapper.SubscriptionPositionUnknown)
	assert.NoError(t, err)
	defer kc.Close()
	assert.Equal(t, kc.Subscription(), groupID)
}

func TestKafkaConsumer_SeekExclusive(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	groupID := fmt.Sprintf("test-groupid-%d", rand.Int())
	topic := fmt.Sprintf("test-topicName-%d", rand.Int())

	config := createConfig(groupID)
	consumer, err := newKafkaConsumer(config, topic, groupID, mqwrapper.SubscriptionPositionUnknown)
	assert.NoError(t, err)
	defer consumer.Close()

	data1 := []int{111, 222, 333}
	data2 := []string{"111", "222", "333"}
	testKafkaConsumerProduceData(t, topic, data1, data2)

	msgID := &kafkaID{messageID: 1}
	err = consumer.Seek(msgID, false)
	assert.NoError(t, err)

	msg := <-consumer.Chan()
	assert.Equal(t, 333, BytesToInt(msg.Payload()))
	assert.Equal(t, "333", msg.Properties()[common.TraceIDKey])
	assert.Equal(t, int64(2), msg.ID().(*kafkaID).messageID)
	assert.Equal(t, topic, msg.Topic())
	assert.True(t, len(msg.Properties()) == 1)
}

func TestKafkaConsumer_SeekInclusive(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	groupID := fmt.Sprintf("test-groupid-%d", rand.Int())
	topic := fmt.Sprintf("test-topicName-%d", rand.Int())

	config := createConfig(groupID)
	consumer, err := newKafkaConsumer(config, topic, groupID, mqwrapper.SubscriptionPositionUnknown)
	assert.NoError(t, err)
	defer consumer.Close()

	data1 := []int{111, 222, 333}
	data2 := []string{"111", "222", "333"}
	testKafkaConsumerProduceData(t, topic, data1, data2)

	msgID := &kafkaID{messageID: 1}
	err = consumer.Seek(msgID, true)
	assert.NoError(t, err)

	msg := <-consumer.Chan()
	assert.Equal(t, 222, BytesToInt(msg.Payload()))
	assert.Equal(t, "222", msg.Properties()[common.TraceIDKey])
	assert.Equal(t, int64(1), msg.ID().(*kafkaID).messageID)
	assert.Equal(t, topic, msg.Topic())
	assert.True(t, len(msg.Properties()) == 1)
}

func TestKafkaConsumer_GetSeek(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	groupID := fmt.Sprintf("test-groupid-%d", rand.Int())
	topic := fmt.Sprintf("test-topicName-%d", rand.Int())

	config := createConfig(groupID)
	consumer, err := newKafkaConsumer(config, topic, groupID, mqwrapper.SubscriptionPositionUnknown)
	assert.NoError(t, err)
	defer consumer.Close()

	msgID := &kafkaID{messageID: 0}
	err = consumer.Seek(msgID, false)
	assert.NoError(t, err)

	assert.Error(t, consumer.Seek(msgID, false))
}

func TestKafkaConsumer_ChanWithNoAssign(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	groupID := fmt.Sprintf("test-groupid-%d", rand.Int())
	topic := fmt.Sprintf("test-topicName-%d", rand.Int())

	config := createConfig(groupID)
	consumer, err := newKafkaConsumer(config, topic, groupID, mqwrapper.SubscriptionPositionUnknown)
	assert.NoError(t, err)
	defer consumer.Close()

	data1 := []int{111}
	data2 := []string{"111"}
	testKafkaConsumerProduceData(t, topic, data1, data2)
	assert.Panics(t, func() {
		<-consumer.Chan()
	})
}

type mockMsgID struct {
}

func (m2 mockMsgID) AtEarliestPosition() bool {
	return false
}

func (m2 mockMsgID) LessOrEqualThan(msgID []byte) (bool, error) {
	return false, nil
}

func (m2 mockMsgID) Equal(msgID []byte) (bool, error) {
	return false, nil
}

func (m2 mockMsgID) Serialize() []byte {
	return nil
}

func TestKafkaConsumer_SeekAfterChan(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	groupID := fmt.Sprintf("test-groupid-%d", rand.Int())
	topic := fmt.Sprintf("test-topicName-%d", rand.Int())

	config := createConfig(groupID)
	consumer, err := newKafkaConsumer(config, topic, groupID, mqwrapper.SubscriptionPositionEarliest)
	assert.NoError(t, err)
	defer consumer.Close()

	data1 := []int{111}
	data2 := []string{"111"}
	testKafkaConsumerProduceData(t, topic, data1, data2)
	msg := <-consumer.Chan()
	assert.Equal(t, 111, BytesToInt(msg.Payload()))
	assert.Equal(t, "111", msg.Properties()[common.TraceIDKey])

	err = consumer.Seek(mockMsgID{}, false)
	assert.Error(t, err)
}

func TestKafkaConsumer_GetLatestMsgID(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	groupID := fmt.Sprintf("test-groupid-%d", rand.Int())
	topic := fmt.Sprintf("test-topicName-%d", rand.Int())

	config := createConfig(groupID)
	consumer, err := newKafkaConsumer(config, topic, groupID, mqwrapper.SubscriptionPositionUnknown)
	assert.NoError(t, err)
	defer consumer.Close()

	latestMsgID, err := consumer.GetLatestMsgID()
	assert.Equal(t, int64(0), latestMsgID.(*kafkaID).messageID)
	assert.NoError(t, err)

	data1 := []int{111, 222, 333}
	data2 := []string{"111", "222", "333"}
	testKafkaConsumerProduceData(t, topic, data1, data2)

	latestMsgID, err = consumer.GetLatestMsgID()
	assert.Equal(t, int64(2), latestMsgID.(*kafkaID).messageID)
	assert.NoError(t, err)
}

func TestKafkaConsumer_ConsumeFromLatest(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	groupID := fmt.Sprintf("test-groupid-%d", rand.Int())
	topic := fmt.Sprintf("test-topicName-%d", rand.Int())

	data1 := []int{111, 222, 333}
	data2 := []string{"111", "222", "333"}
	testKafkaConsumerProduceData(t, topic, data1, data2)

	config := createConfig(groupID)
	consumer, err := newKafkaConsumer(config, topic, groupID, mqwrapper.SubscriptionPositionLatest)
	assert.NoError(t, err)
	defer consumer.Close()
	data1 = []int{444, 555}
	data2 = []string{"444", "555"}
	testKafkaConsumerProduceData(t, topic, data1, data2)

	msg := <-consumer.Chan()
	assert.Equal(t, 444, BytesToInt(msg.Payload()))
	assert.Equal(t, "444", msg.Properties()[common.TraceIDKey])
	msg = <-consumer.Chan()
	assert.Equal(t, 555, BytesToInt(msg.Payload()))
	assert.Equal(t, "555", msg.Properties()[common.TraceIDKey])
}

func TestKafkaConsumer_ConsumeFromEarliest(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	groupID := fmt.Sprintf("test-groupid-%d", rand.Int())
	topic := fmt.Sprintf("test-topicName-%d", rand.Int())

	data1 := []int{111, 222, 333}
	data2 := []string{"111", "222", "333"}
	testKafkaConsumerProduceData(t, topic, data1, data2)

	config := createConfig(groupID)
	consumer, err := newKafkaConsumer(config, topic, groupID, mqwrapper.SubscriptionPositionEarliest)
	assert.NoError(t, err)
	msg := <-consumer.Chan()
	assert.Equal(t, 111, BytesToInt(msg.Payload()))
	assert.Equal(t, "111", msg.Properties()[common.TraceIDKey])
	consumer.Ack(msg)
	defer consumer.Close()

	config = createConfig(groupID)
	consumer2, err := newKafkaConsumer(config, topic, groupID, mqwrapper.SubscriptionPositionEarliest)
	assert.NoError(t, err)
	msg = <-consumer2.Chan()
	assert.Equal(t, 111, BytesToInt(msg.Payload()))
	assert.Equal(t, "111", msg.Properties()[common.TraceIDKey])
	consumer2.Ack(msg)
	defer consumer2.Close()
}

func TestKafkaConsumer_createKafkaConsumer(t *testing.T) {
	consumer := &Consumer{config: &kafka.ConfigMap{}}
	err := consumer.createKafkaConsumer()
	assert.Error(t, err)
}

func testKafkaConsumerProduceData(t *testing.T, topic string, data []int, arr []string) {
	ctx := context.Background()
	kc := createKafkaClient(t)
	defer kc.Close()
	producer := createProducer(t, kc, topic)
	defer producer.Close()

	produceData(ctx, t, producer, data, arr)

	producer.(*kafkaProducer).p.Flush(500)
}

func createConfig(groupID string) *kafka.ConfigMap {
	kafkaAddress := getKafkaBrokerList()
	return &kafka.ConfigMap{
		"bootstrap.servers":   kafkaAddress,
		"group.id":            groupID,
		"api.version.request": "true",
	}
}

func TestKafkaConsumer_CheckPreTopicValid(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	groupID := fmt.Sprintf("test-groupid-%d", rand.Int())
	topic := fmt.Sprintf("test-topicName-%d", rand.Int())

	config := createConfig(groupID)
	consumer, err := newKafkaConsumer(config, topic, groupID, mqwrapper.SubscriptionPositionEarliest)
	assert.NoError(t, err)
	defer consumer.Close()

	err = consumer.CheckTopicValid(topic)
	assert.NoError(t, err)
}
