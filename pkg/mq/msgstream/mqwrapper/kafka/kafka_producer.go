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
	"github.com/milvus-io/milvus/pkg/metrics"
	mqcommon "github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

type kafkaProducer struct {
	p         *kafka.Producer
	topic     string
	closeOnce sync.Once
	isClosed  bool
	stopCh    chan struct{}
}

func (kp *kafkaProducer) Topic() string {
	return kp.topic
}

func (kp *kafkaProducer) Send(ctx context.Context, message *mqcommon.ProducerMessage) (mqcommon.MessageID, error) {
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

	resultCh := make(chan kafka.Event, 1)
	err := kp.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kp.topic, Partition: mqwrapper.DefaultPartitionIdx},
		Value:          message.Payload,
		Headers:        headers,
	}, resultCh)
	if err != nil {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.SendMsgLabel, metrics.FailLabel).Inc()
		return nil, err
	}

	var m *kafka.Message
	select {
	case <-kp.stopCh:
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.SendMsgLabel, metrics.FailLabel).Inc()
		log.Error("kafka produce message fail because of kafka producer is closed", zap.String("topic", kp.topic))
		return nil, common.NewIgnorableError(fmt.Errorf("kafka producer is closed"))
	case e := <-resultCh:
		m = e.(*kafka.Message)
	}

	if m.TopicPartition.Error != nil {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.SendMsgLabel, metrics.FailLabel).Inc()
		return nil, m.TopicPartition.Error
	}

	elapsed := start.ElapseSpan()
	metrics.MsgStreamRequestLatency.WithLabelValues(metrics.SendMsgLabel).Observe(float64(elapsed.Milliseconds()))
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.SendMsgLabel, metrics.SuccessLabel).Inc()

	return &KafkaID{MessageID: int64(m.TopicPartition.Offset)}, nil
}

func (kp *kafkaProducer) Close() {
	log := log.Ctx(context.TODO())
	kp.closeOnce.Do(func() {
		kp.isClosed = true

		start := time.Now()
		// flush in-flight msg within queue.
		i := kp.p.Flush(10000)
		if i > 0 {
			log.Warn("There are still un-flushed outstanding events", zap.Int("event_num", i), zap.String("topic", kp.topic))
		}

		close(kp.stopCh)
		cost := time.Since(start).Milliseconds()
		if cost > 500 {
			log.Debug("kafka producer is closed", zap.String("topic", kp.topic), zap.Int64("time cost(ms)", cost))
		}
	})
}
