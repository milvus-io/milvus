package kafka

import (
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type Consumer struct {
	c          *kafka.Consumer
	config     *kafka.ConfigMap
	msgChannel chan mqwrapper.Message
	hasAssign  bool
	skipMsg    bool
	topic      string
	groupID    string
	chanOnce   sync.Once
	closeOnce  sync.Once
	closeCh    chan struct{}
	wg         sync.WaitGroup
}

const timeout = 3000

func newKafkaConsumer(config *kafka.ConfigMap, topic string, groupID string, position mqwrapper.SubscriptionInitialPosition) (*Consumer, error) {
	msgChannel := make(chan mqwrapper.Message, 256)
	kc := &Consumer{
		config:     config,
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
		topicPartition := []kafka.TopicPartition{{Topic: &topic, Partition: mqwrapper.DefaultPartitionIdx, Offset: offset}}
		err = kc.c.Assign(topicPartition)
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
	kc.c, err = kafka.NewConsumer(kc.config)
	if err != nil {
		log.Error("create kafka consumer failed", zap.String("topic", kc.topic), zap.Error(err))
		return err
	}
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
		kc.wg.Add(1)
		go func() {
			defer kc.wg.Done()
			for {
				select {
				case <-kc.closeCh:
					log.Info("close consumer ", zap.String("topic", kc.topic), zap.String("groupID", kc.groupID))
					start := time.Now()
					err := kc.c.Close()
					if err != nil {
						log.Warn("failed to close ", zap.String("topic", kc.topic), zap.Error(err))
					}
					cost := time.Since(start).Milliseconds()
					if cost > 200 {
						log.Warn("close consumer costs too long time", zap.Any("topic", kc.topic), zap.String("groupID", kc.groupID), zap.Int64("time(ms)", cost))
					}
					if kc.msgChannel != nil {
						close(kc.msgChannel)
					}
					return
				default:
					e, err := kc.c.ReadMessage(30 * time.Second)
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
	err := kc.c.Assign([]kafka.TopicPartition{{Topic: &kc.topic, Partition: mqwrapper.DefaultPartitionIdx, Offset: offset}})
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
	if err := kc.c.Seek(kafka.TopicPartition{
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
	low, high, err := kc.c.QueryWatermarkOffsets(kc.topic, mqwrapper.DefaultPartitionIdx, timeout)
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

func (kc *Consumer) CheckTopicValid(topic string) error {
	latestMsgID, err := kc.GetLatestMsgID()
	log.With(zap.String("topic", kc.topic))
	// check topic is existed
	if err != nil {
		switch v := err.(type) {
		case kafka.Error:
			if v.Code() == kafka.ErrUnknownTopic || v.Code() == kafka.ErrUnknownPartition || v.Code() == kafka.ErrUnknownTopicOrPart {
				return merr.WrapErrTopicNotFound(topic, "topic get latest msg ID failed, topic or partition does not exists")
			}
		default:
			return err
		}
	}

	// check topic is empty
	if !latestMsgID.AtEarliestPosition() {
		return merr.WrapErrTopicNotEmpty(topic, "topic is not empty")
	}
	log.Info("created topic is empty")

	return nil
}

func (kc *Consumer) Close() {
	kc.closeOnce.Do(func() {
		close(kc.closeCh)
		kc.wg.Wait() // wait worker exist and close the client
	})
}
