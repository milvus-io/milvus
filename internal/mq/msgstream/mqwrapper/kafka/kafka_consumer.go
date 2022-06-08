package kafka

import (
	"errors"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"go.uber.org/zap"
)

type Consumer struct {
	c               *kafka.Consumer
	config          *kafka.ConfigMap
	msgChannel      chan mqwrapper.Message
	hasSeek         bool
	hasConsume      bool
	skipMsg         bool
	latestMsgOffset kafka.Offset
	topic           string
	groupID         string
	closeCh         chan struct{}
	chanOnce        sync.Once
	closeOnce       sync.Once
}

func newKafkaConsumer(config *kafka.ConfigMap, topic string, groupID string) (*Consumer, error) {
	closeCh := make(chan struct{})
	msgChannel := make(chan mqwrapper.Message, 256)

	kafkaConsumer := &Consumer{
		config:     config,
		msgChannel: msgChannel,
		topic:      topic,
		groupID:    groupID,
		closeCh:    closeCh,
	}

	err := kafkaConsumer.createKafkaConsumer()
	return kafkaConsumer, err
}

func (kc *Consumer) createKafkaConsumer() error {
	var err error
	kc.c, err = kafka.NewConsumer(kc.config)
	if err != nil {
		log.Error("create kafka consumer failed", zap.String("topic", kc.topic), zap.Error(err))
		return err
	}

	latestMsgID, err := kc.GetLatestMsgID()
	if err != nil {
		switch v := err.(type) {
		case kafka.Error:
			if v.Code() == kafka.ErrUnknownTopic || v.Code() == kafka.ErrUnknownPartition || v.Code() == kafka.ErrUnknownTopicOrPart {
				log.Warn("get latest msg ID failed, topic or partition does not exists!",
					zap.String("topic", kc.topic),
					zap.String("err msg", v.String()))
				kc.latestMsgOffset = kafka.OffsetBeginning
			}
		default:
			log.Error("get latest msg ID failed", zap.String("topic", kc.topic), zap.Error(err))
			return err
		}
	} else {
		kc.latestMsgOffset = kafka.Offset(latestMsgID.(*kafkaID).messageID)
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
	kc.chanOnce.Do(func() {
		if !kc.hasSeek {
			offsetStr, err := kc.config.Get("auto.offset.reset", "earliest")
			if err != nil {
				log.Error("get auto.offset.reset config fail in kafka consumer", zap.String("topic name", kc.topic), zap.Error(err))
				panic(err)
			}

			offset, err := kafka.NewOffset(offsetStr)
			if err != nil {
				log.Error("Invalid kafka offset", zap.String("topic name", kc.topic), zap.Error(err))
				panic(err)
			}

			// we assume that case is Chan starting before producing message with auto create topic config,
			// consuming messages will fail that error is 'Subscribed topic not available'
			// if invoke Subscribe method of kafka, so we use Assign instead of Subscribe.
			var tps []kafka.TopicPartition
			if offset == kafka.OffsetEnd && kc.latestMsgOffset != kafka.OffsetBeginning {
				// kafka consumer will start when assign invoked, in order to guarantee the latest message
				// position is same with created consumer time, there will use a seek to the latest to
				// replace consuming from the latest position.
				if err := kc.internalSeek(kc.latestMsgOffset, false); err != nil {
					log.Error("kafka consumer subscribe failed ",
						zap.String("topic name", kc.topic),
						zap.Any("latestMsgOffset", kc.latestMsgOffset),
						zap.Any("offset", offset),
						zap.Error(err))
					panic(err)
				}
			} else {
				tps = []kafka.TopicPartition{{Topic: &kc.topic, Partition: mqwrapper.DefaultPartitionIdx, Offset: offset}}
				if err := kc.c.Assign(tps); err != nil {
					log.Error("kafka consumer subscribe failed ",
						zap.String("topic name", kc.topic),
						zap.Any("latestMsgOffset", kc.latestMsgOffset),
						zap.Any("offset", offset),
						zap.Error(err))
					panic(err)
				}
			}

			log.Debug("starting kafka consume",
				zap.String("topic name", kc.topic),
				zap.Any("latestMsgOffset", kc.latestMsgOffset),
				zap.Any("offset", offset))
		}

		go func() {
			// loop end if consumer is closed
			for ev := range kc.c.Events() {
				switch e := ev.(type) {
				case *kafka.Message:
					if kc.skipMsg {
						kc.skipMsg = false
						continue
					}

					kc.msgChannel <- &kafkaMessage{msg: e}
				case kafka.Error:
					log.Error("consume msg failed", zap.Any("topic", kc.topic), zap.String("groupID", kc.groupID), zap.Error(e))
					if ev.(kafka.Error).IsFatal() {
						panic(e)
					}
				}
			}

			if kc.msgChannel != nil {
				close(kc.msgChannel)
			}
		}()

		kc.hasConsume = true
	})

	return kc.msgChannel
}

func (kc *Consumer) Seek(id mqwrapper.MessageID, inclusive bool) error {
	offset := kafka.Offset(id.(*kafkaID).messageID)
	return kc.internalSeek(offset, inclusive)
}

func (kc *Consumer) internalSeek(offset kafka.Offset, inclusive bool) error {
	if kc.hasSeek {
		return errors.New("unsupported multiple seek with the same kafka consumer")
	}

	if kc.hasConsume {
		return errors.New("unsupported seek after consume message with the same kafka consumer")
	}

	log.Debug("kafka consumer seek start", zap.String("topic name", kc.topic),
		zap.Any("Msg offset", offset), zap.Bool("inclusive", inclusive))

	start := time.Now()
	err := kc.c.Assign([]kafka.TopicPartition{{Topic: &kc.topic, Partition: mqwrapper.DefaultPartitionIdx, Offset: offset}})
	if err != nil {
		log.Error("kafka consumer assign failed ", zap.String("topic name", kc.topic), zap.Any("Msg offset", offset), zap.Error(err))
		return err
	}

	cost := time.Since(start).Milliseconds()
	if cost > 100 {
		log.Debug("kafka consumer assign take too long!", zap.String("topic name", kc.topic),
			zap.Any("Msg offset", offset), zap.Bool("inclusive", inclusive), zap.Int64("time cost(ms)", cost))
	}

	// If seek timeout is not 0 the call twice will return error isStarted RD_KAFKA_RESP_ERR__STATE.
	// if the timeout is 0 it will initiate the seek  but return immediately without any error reporting
	kc.skipMsg = !inclusive
	if err := kc.c.Seek(kafka.TopicPartition{
		Topic:     &kc.topic,
		Partition: mqwrapper.DefaultPartitionIdx,
		Offset:    offset}, 1000); err != nil {
		return err
	}
	cost = time.Since(start).Milliseconds()
	log.Debug("kafka consumer seek finished", zap.String("topic name", kc.topic),
		zap.Any("Msg offset", offset), zap.Bool("inclusive", inclusive), zap.Int64("time cost(ms)", cost))

	kc.hasSeek = true
	return nil
}

func (kc *Consumer) Ack(message mqwrapper.Message) {
	kc.c.Commit()
}

func (kc *Consumer) GetLatestMsgID() (mqwrapper.MessageID, error) {
	low, high, err := kc.c.QueryWatermarkOffsets(kc.topic, mqwrapper.DefaultPartitionIdx, -1)
	if err != nil {
		return nil, err
	}

	// Current high value is next offset of the latest message ID, in order to keep
	// semantics consistency with the latest message ID, the high value need to move forward.
	if high > 0 {
		high = high - 1
	}

	log.Debug("get latest msg ID ", zap.Any("topic", kc.topic), zap.Int64("oldest offset", low), zap.Int64("latest offset", high))
	return &kafkaID{messageID: high}, nil
}

func (kc *Consumer) Close() {
	kc.closeOnce.Do(func() {
		start := time.Now()

		// FIXME we should not use goroutine to close consumer, it will be fix after pr https://github.com/confluentinc/confluent-kafka-go/pull/757
		go kc.c.Close()

		cost := time.Since(start).Milliseconds()
		if cost > 500 {
			log.Debug("kafka consumer is closed ", zap.Any("topic", kc.topic), zap.String("groupID", kc.groupID), zap.Int64("time cost(ms)", cost))
		}
	})
}
