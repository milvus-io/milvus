package kafka

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jackc/puddle/v2"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"go.uber.org/zap"
)

type Consumer struct {
	resource   *puddle.Resource[*kafka.Consumer]
	msgChannel chan mqwrapper.Message
	tp         []kafka.TopicPartition
	hasAssign  bool
	skipMsg    bool
	topic      string
	groupID    string
	chanOnce   sync.Once
	closeOnce  sync.Once
	closeCh    chan struct{}
}

const timeout = 3000

func newKafkaConsumer(topic string, groupID string, position mqwrapper.SubscriptionInitialPosition) (*Consumer, error) {
	msgChannel := make(chan mqwrapper.Message, 256)
	kc := &Consumer{
		msgChannel: msgChannel,
		topic:      topic,
		groupID:    groupID,
		closeCh:    make(chan struct{}),
	}

	err := kc.createKafkaConsumer()
	if err != nil {
		return nil, err
	}

	// if it's unknown, we leave the assign to seek
	if position != mqwrapper.SubscriptionPositionUnknown {
		var offset kafka.Offset
		if position == mqwrapper.SubscriptionPositionEarliest {
			offset, err = kafka.NewOffset("earliest")
			if err != nil {
				return nil, err
			}
		} else {
			latestMsgID, err := kc.GetLatestMsgID()

			if err != nil {
				switch v := err.(type) {
				case kafka.Error:
					if v.Code() == kafka.ErrUnknownTopic || v.Code() == kafka.ErrUnknownPartition || v.Code() == kafka.ErrUnknownTopicOrPart {
						log.Warn("get latest msg ID failed, topic or partition does not exists!",
							zap.String("topic", kc.topic),
							zap.String("err msg", v.String()))
						offset, err = kafka.NewOffset("earliest")
						if err != nil {
							return nil, err
						}
					}
				default:
					log.Error("kafka get latest msg ID failed", zap.String("topic", kc.topic), zap.Error(err))
					return nil, err
				}
			} else {
				offset = kafka.Offset(latestMsgID.(*kafkaID).messageID)
				kc.skipMsg = true
			}
		}

		start := time.Now()
		kc.tp = []kafka.TopicPartition{{Topic: &topic, Partition: mqwrapper.DefaultPartitionIdx, Offset: offset}}
		err = kc.resource.Value().Assign(kc.tp)
		if err != nil {
			log.Error("kafka consumer assign failed ", zap.String("topic name", topic), zap.Any("Msg position", position), zap.Error(err))
			return nil, err
		}
		cost := time.Since(start).Milliseconds()
		if cost > 200 {
			log.Warn("kafka consumer assign take too long!", zap.String("topic name", topic), zap.Any("Msg position", position), zap.Int64("time cost(ms)", cost))
		}

		kc.hasAssign = true
	}

	return kc, nil
}

func (kc *Consumer) createKafkaConsumer() error {
	var err error
	kc.resource, err = ConsumerConnectionPool.Acquire(context.Background())
	if err != nil {
		log.Error("get kafka consumer failed from consumer connection pool", zap.String("topic", kc.topic), zap.Error(err))
		return err
	}

	stats := ConsumerConnectionPool.Stat()
	log.Debug("kafka consumer connection pool stats", zap.Int32("total resource", stats.TotalResources()),
		zap.Int32("acquired resource", stats.AcquiredResources()),
		zap.Int32("idle resource", stats.IdleResources()),
		zap.Int32("construction resource", stats.ConstructingResources()),
		zap.Int32("max resource", stats.MaxResources()),
		zap.Int64("acquired resource count", stats.AcquireCount()),
		zap.Any("acquired duration", stats.AcquireDuration()),
		zap.Int64("canceled acquire resource count", stats.CanceledAcquireCount()),
		zap.Int64("empty acquire resource count", stats.EmptyAcquireCount()),
	)
	return nil
}

func (kc *Consumer) Subscription() string {
	return kc.groupID
}

// Chan provides a channel to read consumed message.
// confluent-kafka-go recommend us to use function-based consumer,
// channel-based consumer API had already deprecated, see more details
// https://github.com/confluentinc/confluent-kafka-go.
func (kc *Consumer) Chan() <-chan mqwrapper.Message {
	if !kc.hasAssign {
		log.Error("can not chan with not assigned channel", zap.String("topic", kc.topic), zap.String("groupID", kc.groupID))
		panic("failed to chan a kafka consumer without assign")
	}
	kc.chanOnce.Do(func() {
		go func() {
			for {
				select {
				case <-kc.closeCh:
					log.Info("close consumer ", zap.String("topic", kc.topic), zap.String("groupID", kc.groupID))
					err := kc.resource.Value().Unassign()
					if err != nil {
						log.Error("kafka unassign consumer fail", zap.String("topic", kc.topic), zap.String("groupID", kc.groupID), zap.Error(err))
						// try to destroy directly it if unassign fail.
						kc.resource.Destroy()
						return
					}

					kc.resource.Release()
					if kc.msgChannel != nil {
						close(kc.msgChannel)
					}
					return
				default:
					e, err := kc.resource.Value().ReadMessage(30 * time.Second)
					if err != nil {
						// if we failed to read message in 30 Seconds, print out a warn message since there should always be a tt
						log.Warn("consume msg failed", zap.Any("topic", kc.topic), zap.String("groupID", kc.groupID), zap.Error(err))
					} else {
						if kc.skipMsg {
							kc.skipMsg = false
							continue
						}
						kc.msgChannel <- &kafkaMessage{msg: e}
					}
				}
			}
		}()
	})

	return kc.msgChannel
}

func (kc *Consumer) Seek(id mqwrapper.MessageID, inclusive bool) error {
	if kc.hasAssign {
		return errors.New("kafka consumer is already assigned, can not seek again")
	}

	offset := kafka.Offset(id.(*kafkaID).messageID)
	return kc.internalSeek(offset, inclusive)
}

func (kc *Consumer) internalSeek(offset kafka.Offset, inclusive bool) error {
	log.Info("kafka consumer seek start", zap.String("topic name", kc.topic),
		zap.Any("Msg offset", offset), zap.Bool("inclusive", inclusive))

	start := time.Now()
	kc.tp = []kafka.TopicPartition{{Topic: &kc.topic, Partition: mqwrapper.DefaultPartitionIdx, Offset: offset}}
	err := kc.resource.Value().Assign(kc.tp)
	if err != nil {
		log.Warn("kafka consumer assign failed ", zap.String("topic name", kc.topic), zap.Any("Msg offset", offset), zap.Error(err))
		return err
	}

	cost := time.Since(start).Milliseconds()
	if cost > 200 {
		log.Warn("kafka consumer assign take too long!", zap.String("topic name", kc.topic),
			zap.Any("Msg offset", offset), zap.Bool("inclusive", inclusive), zap.Int64("time cost(ms)", cost))
	}

	// If seek timeout is not 0 the call twice will return error isStarted RD_KAFKA_RESP_ERR__STATE.
	// if the timeout is 0 it will initiate the seek  but return immediately without any error reporting
	kc.skipMsg = !inclusive
	if err := kc.resource.Value().Seek(kafka.TopicPartition{
		Topic:     &kc.topic,
		Partition: mqwrapper.DefaultPartitionIdx,
		Offset:    offset}, timeout); err != nil {
		return err
	}
	cost = time.Since(start).Milliseconds()
	log.Info("kafka consumer seek finished", zap.String("topic name", kc.topic),
		zap.Any("Msg offset", offset), zap.Bool("inclusive", inclusive), zap.Int64("time cost(ms)", cost))

	kc.hasAssign = true
	return nil
}

func (kc *Consumer) Ack(message mqwrapper.Message) {
	// Do nothing
	// Kafka retention mechanism only depends on retention configuration,
	// it does not relate to the commit with consumer's offsets.
}

func (kc *Consumer) GetLatestMsgID() (mqwrapper.MessageID, error) {
	low, high, err := kc.resource.Value().QueryWatermarkOffsets(kc.topic, mqwrapper.DefaultPartitionIdx, timeout)
	if err != nil {
		return nil, err
	}

	// Current high value is next offset of the latest message ID, in order to keep
	// semantics consistency with the latest message ID, the high value need to move forward.
	if high > 0 {
		high = high - 1
	}

	log.Info("get latest msg ID ", zap.Any("topic", kc.topic), zap.Int64("oldest offset", low), zap.Int64("latest offset", high))
	return &kafkaID{messageID: high}, nil
}

func (kc *Consumer) Close() {
	kc.closeOnce.Do(func() {
		close(kc.closeCh)
	})
}
