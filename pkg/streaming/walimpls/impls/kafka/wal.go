package kafka

import (
	"context"
	"time"

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

const kafkaTopicMetadataTimeout = time.Second

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
	consumerConfig := w.consumerConfigForRead()
	consumerConfig.SetKey("group.id", opt.Name)
	c, err := kafka.NewConsumer(&consumerConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create kafka consumer")
	}
	consumerOwnedByScanner := false
	defer func() {
		if !consumerOwnedByScanner {
			c.Close()
		}
	}()

	topic := w.Channel().Name
	if w.Channel().AccessMode == types.AccessModeRO {
		if err := validateKafkaTopicExists(ctx, c, topic); err != nil {
			return nil, err
		}
	}
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
		return nil, errors.Wrap(convertKafkaReadError(err, topic), "failed to assign kafka consumer")
	}
	consumerOwnedByScanner = true
	return newScanner(opt.Name, topic, exclude, c), nil
}

func validateKafkaTopicExists(ctx context.Context, consumer *kafka.Consumer, topic string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	timeout := kafkaTopicMetadataTimeout
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return ctx.Err()
		}
		if remaining < timeout {
			timeout = remaining
		}
	}
	timeoutMillis := max(int(timeout.Milliseconds()), 1)
	// Request a complete metadata snapshot without naming the missing topic.
	// This avoids triggering topic creation and keeps transient runtime
	// UNKNOWN_TOPIC_OR_PARTITION errors separate from an absent topic.
	metadata, err := consumer.GetMetadata(nil, true, timeoutMillis)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.Wrap(err, "failed to get kafka topic metadata")
	}
	return validateKafkaTopicMetadata(metadata, topic)
}

func validateKafkaTopicMetadata(metadata *kafka.Metadata, topic string) error {
	if metadata == nil {
		return merr.WrapErrMqInternalMsg("kafka returned nil topic metadata")
	}
	topicMetadata, ok := metadata.Topics[topic]
	if !ok {
		return merr.WrapErrMqTopicNotFound(topic)
	}
	if topicMetadata.Error.Code() != kafka.ErrNoError {
		return convertKafkaReadError(topicMetadata.Error, topic)
	}
	return nil
}

func (w *walImpl) consumerConfigForRead() kafka.ConfigMap {
	consumerConfig := cloneKafkaConfig(w.consumerConfig)
	if w.Channel().AccessMode == types.AccessModeRO {
		// A historical reader must never recreate a deleted topic as an empty
		// stream, otherwise it can wait forever for an AlterWAL boundary.
		consumerConfig.SetKey("allow.auto.create.topics", false)
	}
	return consumerConfig
}

func convertKafkaReadError(err error, topic string) error {
	var kafkaErr kafka.Error
	if errors.As(err, &kafkaErr) && kafkaErr.IsFatal() {
		switch kafkaErr.Code() {
		case kafka.ErrUnknownTopic, kafka.ErrUnknownTopicOrPart:
			return merr.WrapErrMqTopicNotFound(topic, kafkaErr.Error())
		}
	}
	return err
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
