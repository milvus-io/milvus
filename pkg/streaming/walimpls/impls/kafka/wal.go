package kafka

import (
	"context"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var _ walimpls.WALImpls = (*walImpl)(nil)

type walImpl struct {
	*helper.WALHelper
	p              *kafka.Producer
	consumerConfig kafka.ConfigMap
}

func (w *walImpl) WALName() message.WALName {
	return message.WALNameKafka
}

func (w *walImpl) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	if w.Channel().AccessMode != types.AccessModeRW {
		panic("write on a wal that is not in read-write mode")
	}

	pb := msg.IntoMessageProto()
	properties := pb.Properties
	headers := make([]kafka.Header, 0, len(properties))
	for key, value := range properties {
		header := kafka.Header{Key: key, Value: []byte(value)}
		headers = append(headers, header)
	}
	ch := make(chan kafka.Event, 1)
	topic := w.Channel().Name

	if err := w.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
		Value:          pb.Payload,
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
	if err := consumerConfig.SetKey("group.id", opt.Name); err != nil {
		return nil, merr.WrapErrMqInternal(err, "failed to configure kafka reader group")
	}
	if w.Channel().AccessMode == types.AccessModeRO {
		// A read-only reader must never create a missing topic.
		if err := consumerConfig.SetKey("allow.auto.create.topics", false); err != nil {
			return nil, merr.WrapErrMqInternal(err, "failed to disable kafka topic auto creation")
		}
		// A read-only scan must fail instead of silently resetting to earliest
		// when its requested offset has been removed by retention.
		if err := consumerConfig.SetKey("auto.offset.reset", "error"); err != nil {
			return nil, merr.WrapErrMqInternal(err, "failed to disable kafka offset auto reset")
		}
	}
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
	var requestedOffset *kafka.Offset
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
		requestedOffset = &seekPosition.Offset
	case *streamingpb.DeliverPolicy_StartAfter:
		id, err := unmarshalMessageID(t.StartAfter.GetId())
		if err != nil {
			return nil, err
		}
		seekPosition.Offset = kafka.Offset(id)
		requestedOffset = &seekPosition.Offset
		exclude = &id
	default:
		panic("unknown deliver policy")
	}
	if w.Channel().AccessMode == types.AccessModeRO {
		const topicValidationTimeout = 3000
		low, high, err := c.QueryWatermarkOffsets(topic, 0, topicValidationTimeout)
		if err != nil {
			_ = c.Close()
			return nil, mapKafkaReadError(topic, err)
		}
		if requestedOffset != nil {
			if err := validateKafkaReadOffset(topic, int64(*requestedOffset), low, high); err != nil {
				_ = c.Close()
				return nil, err
			}
		}
	}

	if err := c.Assign([]kafka.TopicPartition{seekPosition}); err != nil {
		_ = c.Close()
		return nil, errors.Wrap(err, "failed to assign kafka consumer")
	}
	return newScanner(opt.Name, topic, exclude, c), nil
}

func mapKafkaReadError(topic string, err error) error {
	var kafkaErr kafka.Error
	if errors.As(err, &kafkaErr) {
		switch kafkaErr.Code() {
		case kafka.ErrUnknownTopic, kafka.ErrUnknownPartition, kafka.ErrUnknownTopicOrPart, kafka.ErrOffsetOutOfRange:
			return merr.WrapErrMqTopicNotFound(topic, err.Error())
		}
	}
	return err
}

func validateKafkaReadOffset(topic string, offset, low, high int64) error {
	if kafka.Offset(offset) == kafka.OffsetBeginning {
		// OffsetBeginning is the persisted sentinel used by the migration
		// checkpoint for an earliest-position Kafka reader.
		return nil
	}
	if offset >= low && offset < high {
		return nil
	}
	return merr.WrapErrMqTopicNotFound(topic,
		"requested offset "+strconv.FormatInt(offset, 10)+
			" is outside retained range ["+strconv.FormatInt(low, 10)+", "+strconv.FormatInt(high, 10)+")")
}

func (w *walImpl) Truncate(ctx context.Context, id message.MessageID) error {
	if w.Channel().AccessMode != types.AccessModeRW {
		panic("truncate on a wal that is not in read-write mode")
	}
	return nil
}

func (w *walImpl) Close() {
	// The lifetime control of the producer is delegated to the wal adaptor.
	// So we just make resource cleanup here.
	// But kafka producer is not topic level, so we don't close it here.
}
