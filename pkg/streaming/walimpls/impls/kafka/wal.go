package kafka

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/helper"
)

var _ walimpls.WALImpls = (*walImpl)(nil)

type walImpl struct {
	*helper.WALHelper
	p              *kafka.Producer
	consumerConfig kafka.ConfigMap
}

func (w *walImpl) WALName() string {
	return walName
}

func (w *walImpl) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	properties := msg.Properties().ToRawMap()
	headers := make([]kafka.Header, 0, len(properties))
	for key, value := range properties {
		header := kafka.Header{Key: key, Value: []byte(value)}
		headers = append(headers, header)
	}
	ch := make(chan kafka.Event, 1)
	topic := w.Channel().Name

	if err := w.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
		Value:          msg.Payload(),
		Headers:        headers,
	}, ch); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case event := <-ch:
		relatedMsg := event.(*kafka.Message)
		if relatedMsg.TopicPartition.Error != nil {
			return nil, relatedMsg.TopicPartition.Error
		}
		return kafkaID(relatedMsg.TopicPartition.Offset), nil
	}
}

func (w *walImpl) Read(ctx context.Context, opt walimpls.ReadOption) (s walimpls.ScannerImpls, err error) {
	// The scanner is stateless, so we can create a scanner with an anonymous consumer.
	// and there's no commit opeartions.
	consumerConfig := cloneKafkaConfig(w.consumerConfig)
	consumerConfig.SetKey("group.id", opt.Name)
	c, err := kafka.NewConsumer(&consumerConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create kafka consumer")
	}

	topic := w.Channel().Name
	seekPosition := kafka.TopicPartition{
		Topic:     &topic,
		Partition: 0,
	}
	var exclude *kafkaID
	switch t := opt.DeliverPolicy.GetPolicy().(type) {
	case *streamingpb.DeliverPolicy_All:
		seekPosition.Offset = kafka.OffsetBeginning
	case *streamingpb.DeliverPolicy_Latest:
		seekPosition.Offset = kafka.OffsetEnd
	case *streamingpb.DeliverPolicy_StartFrom:
		id, err := unmarshalMessageID(t.StartFrom.GetId())
		if err != nil {
			return nil, err
		}
		seekPosition.Offset = kafka.Offset(id)
	case *streamingpb.DeliverPolicy_StartAfter:
		id, err := unmarshalMessageID(t.StartAfter.GetId())
		if err != nil {
			return nil, err
		}
		seekPosition.Offset = kafka.Offset(id)
		exclude = &id
	default:
		panic("unknown deliver policy")
	}

	if err := c.Assign([]kafka.TopicPartition{seekPosition}); err != nil {
		return nil, errors.Wrap(err, "failed to assign kafka consumer")
	}
	return newScanner(opt.Name, exclude, c), nil
}

func (w *walImpl) Close() {
	// The lifetime control of the producer is delegated to the wal adaptor.
	// So we just make resource cleanup here.
	// But kafka producer is not topic level, so we don't close it here.
}
