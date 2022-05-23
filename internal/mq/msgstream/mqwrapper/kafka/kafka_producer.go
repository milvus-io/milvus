package kafka

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
)

type kafkaProducer struct {
	p            *kafka.Producer
	topic        string
	deliveryChan chan kafka.Event
	closeOnce    sync.Once
}

func (kp *kafkaProducer) Topic() string {
	return kp.topic
}

func (kp *kafkaProducer) Send(ctx context.Context, message *mqwrapper.ProducerMessage) (mqwrapper.MessageID, error) {
	err := kp.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kp.topic, Partition: mqwrapper.DefaultPartitionIdx},
		Value:          message.Payload,
	}, kp.deliveryChan)

	if err != nil {
		return nil, err
	}

	e, ok := <-kp.deliveryChan
	if !ok {
		log.Error("kafka produce message fail because of delivery chan is closed", zap.String("topic", kp.topic))
		return nil, errors.New("delivery chan of kafka producer is closed")
	}

	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		return nil, m.TopicPartition.Error
	}

	return &kafkaID{messageID: int64(m.TopicPartition.Offset)}, nil
}

func (kp *kafkaProducer) Close() {
	kp.closeOnce.Do(func() {
		start := time.Now()
		//flush in-flight msg within queue.
		kp.p.Flush(10000)

		close(kp.deliveryChan)

		cost := time.Since(start).Milliseconds()
		if cost > 500 {
			log.Debug("kafka producer is closed", zap.Any("topic", kp.topic), zap.Int64("time cost(ms)", cost))
		}
	})
}
