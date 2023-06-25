package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"go.uber.org/zap"
)

type kafkaProducer struct {
	p            *kafka.Producer
	topic        string
	deliveryChan chan kafka.Event
	closeOnce    sync.Once
	isClosed     bool
}

func (kp *kafkaProducer) Topic() string {
	return kp.topic
}

func (kp *kafkaProducer) Send(ctx context.Context, message *mqwrapper.ProducerMessage) (mqwrapper.MessageID, error) {
	start := timerecord.NewTimeRecorder("send msg to stream")
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.SendMsgLabel, metrics.TotalLabel).Inc()

	if kp.isClosed {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.SendMsgLabel, metrics.FailLabel).Inc()
		log.Error("kafka produce message fail because the producer has been closed", zap.String("topic", kp.topic))
		return nil, common.NewIgnorableError(fmt.Errorf("kafka producer is closed"))
	}

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
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.SendMsgLabel, metrics.FailLabel).Inc()
		return nil, err
	}

	e, ok := <-kp.deliveryChan
	if !ok {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.SendMsgLabel, metrics.FailLabel).Inc()
		log.Error("kafka produce message fail because of delivery chan is closed", zap.String("topic", kp.topic))
		return nil, common.NewIgnorableError(fmt.Errorf("delivery chan of kafka producer is closed"))
	}

	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.SendMsgLabel, metrics.FailLabel).Inc()
		return nil, m.TopicPartition.Error
	}

	elapsed := start.ElapseSpan()
	metrics.MsgStreamRequestLatency.WithLabelValues(metrics.SendMsgLabel).Observe(float64(elapsed.Milliseconds()))
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.SendMsgLabel, metrics.SuccessLabel).Inc()

	return &kafkaID{messageID: int64(m.TopicPartition.Offset)}, nil
}

func (kp *kafkaProducer) Close() {
	kp.closeOnce.Do(func() {
		kp.isClosed = true

		start := time.Now()
		//flush in-flight msg within queue.
		i := kp.p.Flush(10000)
		if i > 0 {
			log.Warn("There are still un-flushed outstanding events", zap.Int("event_num", i), zap.Any("topic", kp.topic))
		}

		close(kp.deliveryChan)

		cost := time.Since(start).Milliseconds()
		if cost > 500 {
			log.Debug("kafka producer is closed", zap.Any("topic", kp.topic), zap.Int64("time cost(ms)", cost))
		}
	})
}
