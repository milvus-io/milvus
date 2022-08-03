package kafka

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"github.com/stretchr/testify/assert"
)

func TestKafkaProducer_SendSuccess(t *testing.T) {
	kafkaAddress := getKafkaBrokerList()
	kc := NewKafkaClientInstance(kafkaAddress)
	defer kc.Close()
	assert.NotNil(t, kc)

	rand.Seed(time.Now().UnixNano())
	topic := fmt.Sprintf("test-topic-%d", rand.Int())

	producer, err := kc.CreateProducer(mqwrapper.ProducerOptions{Topic: topic})
	assert.Nil(t, err)
	assert.NotNil(t, producer)

	kafkaProd := producer.(*kafkaProducer)
	assert.Equal(t, kafkaProd.Topic(), topic)

	msg2 := &mqwrapper.ProducerMessage{
		Payload:    []byte{},
		Properties: map[string]string{},
	}
	msgID, err := producer.Send(context.TODO(), msg2)
	assert.Nil(t, err)
	assert.NotNil(t, msgID)

	producer.Close()
}

func TestKafkaProducer_SendFail(t *testing.T) {
	kafkaAddress := getKafkaBrokerList()
	{

		deliveryChan := make(chan kafka.Event, 1)
		rand.Seed(time.Now().UnixNano())
		topic := fmt.Sprintf("test-topic-%d", rand.Int())

		pp, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaAddress})
		assert.Nil(t, err)
		producer := &kafkaProducer{p: pp, deliveryChan: deliveryChan, topic: topic}

		msg := &mqwrapper.ProducerMessage{
			Payload:    []byte{1},
			Properties: map[string]string{},
		}
		var resultMsg kafka.Event = &kafka.Message{TopicPartition: kafka.TopicPartition{Error: errors.New("error")}}
		deliveryChan <- resultMsg

		ret, err := producer.Send(context.TODO(), msg)
		assert.Nil(t, ret)
		assert.NotNil(t, err)

		producer.Close()
	}
}
