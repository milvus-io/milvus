package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

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
	var err error
	maxAttempt := 3

	// In order to avoid https://github.com/confluentinc/confluent-kafka-go/issues/769,
	// just retry produce again when getting a nil from delivery chan.
	for i := 0; i < maxAttempt; i++ {
		err = kp.p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kp.topic, Partition: mqwrapper.DefaultPartitionIdx},
			Value:          message.Payload,
		}, kp.deliveryChan)

		if err != nil {
			break
		}

		e := <-kp.deliveryChan
		if e == nil {
			errMsg := "produce message arise exception, delivery Chan return a nil value"
			err = errors.New(errMsg)
			log.Warn(errMsg, zap.String("topic", kp.topic), zap.ByteString("msg", message.Payload), zap.Int("retries", i))
			continue
		}

		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			return nil, m.TopicPartition.Error
		}

		return &kafkaID{messageID: int64(m.TopicPartition.Offset)}, nil
	}

	return nil, err
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
