package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
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
	headers := make([]kafka.Header, 0, len(message.Properties))
	for key, value := range message.Properties {
		header := kafka.Header{Key: key, Value: []byte(value)}
		headers = append(headers, header)
	}
	err := kp.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kp.topic, Partition: mqwrapper.DefaultPartitionIdx},
		Value:          message.Payload,
		Headers:        headers,
	}, kp.deliveryChan)

	if err != nil {
		return nil, err
	}

	e, ok := <-kp.deliveryChan
	if !ok {
		log.Error("kafka produce message fail because of delivery chan is closed", zap.String("topic", kp.topic))
		return nil, common.NewIgnorableError(fmt.Errorf("delivery chan of kafka producer is closed"))
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

		kp.p.Close()
	})
}
