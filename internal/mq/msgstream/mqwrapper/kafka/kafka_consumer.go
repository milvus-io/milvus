package kafka

import (
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"go.uber.org/zap"
)

type Consumer struct {
	c          *kafka.Consumer
	config     *kafka.ConfigMap
	msgChannel chan mqwrapper.Message
	hasSeek    bool
	isStarted  bool
	skipMsg    bool
	topic      string
	groupID    string
	closeCh    chan struct{}
	chanOnce   sync.Once
	closeOnce  sync.Once
}

func newKafkaConsumer(config *kafka.ConfigMap, topic string, groupID string) *Consumer {
	closeCh := make(chan struct{})
	msgChannel := make(chan mqwrapper.Message, 256)

	kafkaConsumer := &Consumer{
		config:     config,
		msgChannel: msgChannel,
		topic:      topic,
		groupID:    groupID,
		closeCh:    closeCh,
	}

	kafkaConsumer.createKafkaConsumer()
	return kafkaConsumer
}

func (kc *Consumer) createKafkaConsumer() error {
	var err error
	kc.c, err = kafka.NewConsumer(kc.config)
	if err != nil {
		log.Fatal("create kafka consumer failed", zap.String("topic", kc.topic), zap.Error(err))
		return err
	}
	return nil
}

func (kc *Consumer) startReceiveMsgTask() {
	if kc.isStarted {
		return
	}

	if !kc.hasSeek {
		tps := []kafka.TopicPartition{{Topic: &kc.topic, Partition: mqwrapper.DefaultPartitionIdx}}
		if err := kc.c.Assign(tps); err != nil {
			log.Error("kafka consumer assign failed ", zap.String("topic name", kc.topic), zap.Error(err))
			panic(err)
		}
	}

	go func() {
		for ev := range kc.c.Events() {
			switch e := ev.(type) {
			case *kafka.Message:
				if kc.skipMsg {
					kc.skipMsg = false
					continue
				}

				kc.msgChannel <- &kafkaMessage{msg: e}
			case kafka.Error:
				log.Error("read msg failed", zap.Any("topic", kc.topic), zap.String("groupID", kc.groupID), zap.Error(e))
			}
		}

		if kc.msgChannel != nil {
			close(kc.msgChannel)
		}
	}()

	kc.isStarted = true
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
		kc.startReceiveMsgTask()
	})
	return kc.msgChannel
}

func (kc *Consumer) Seek(id mqwrapper.MessageID, inclusive bool) error {
	offset := kafka.Offset(id.(*kafkaID).messageID)
	log.Debug("kafka consumer seek ", zap.String("topic name", kc.topic),
		zap.Any("Msg offset", offset), zap.Bool("inclusive", inclusive))

	err := kc.c.Assign([]kafka.TopicPartition{{Topic: &kc.topic, Partition: mqwrapper.DefaultPartitionIdx, Offset: offset}})
	if err != nil {
		log.Error("kafka consumer assign failed ", zap.String("topic name", kc.topic), zap.Any("Msg offset", offset), zap.Error(err))
		return err
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

	kc.hasSeek = true
	kc.startReceiveMsgTask()
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
