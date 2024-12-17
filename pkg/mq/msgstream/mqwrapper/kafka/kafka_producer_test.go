package kafka

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/mq/common"
)

func TestKafkaProducer_SendSuccess(t *testing.T) {
	kafkaAddress := getKafkaBrokerList()
	kc := NewKafkaClientInstance(kafkaAddress)
	defer kc.Close()
	assert.NotNil(t, kc)

	rand.Seed(time.Now().UnixNano())
	topic := fmt.Sprintf("test-topic-%d", rand.Int())

	producer, err := kc.CreateProducer(common.ProducerOptions{Topic: topic})
	assert.NoError(t, err)
	assert.NotNil(t, producer)

	kafkaProd := producer.(*kafkaProducer)
	assert.Equal(t, kafkaProd.Topic(), topic)

	msg2 := &common.ProducerMessage{
		Payload:    []byte{},
		Properties: map[string]string{},
	}
	msgID, err := producer.Send(context.TODO(), msg2)
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	producer.Close()
}

func TestKafkaProducer_SendFail(t *testing.T) {
	kafkaAddress := getKafkaBrokerList()
	{
		rand.Seed(time.Now().UnixNano())
		topic := fmt.Sprintf("test-topic-%d", rand.Int())

		pp, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaAddress})
		assert.NoError(t, err)
		producer := &kafkaProducer{p: pp, stopCh: make(chan struct{}), topic: topic}
		close(producer.stopCh)

		msg := &common.ProducerMessage{
			Payload:    []byte{1},
			Properties: map[string]string{},
		}
		ret, err := producer.Send(context.TODO(), msg)
		assert.Nil(t, ret)
		assert.Error(t, err)
	}
}

func TestKafkaProducer_SendFailAfterClose(t *testing.T) {
	kafkaAddress := getKafkaBrokerList()
	kc := NewKafkaClientInstance(kafkaAddress)
	defer kc.Close()
	assert.NotNil(t, kc)

	rand.Seed(time.Now().UnixNano())
	topic := fmt.Sprintf("test-topic-%d", rand.Int())

	producer, err := kc.CreateProducer(common.ProducerOptions{Topic: topic})
	assert.Nil(t, err)
	assert.NotNil(t, producer)

	producer.Close()

	kafkaProd := producer.(*kafkaProducer)
	assert.Equal(t, kafkaProd.Topic(), topic)

	msg2 := &common.ProducerMessage{
		Payload:    []byte{},
		Properties: map[string]string{},
	}
	_, err = producer.Send(context.TODO(), msg2)
	time.Sleep(10 * time.Second)
	assert.NotNil(t, err)
}
