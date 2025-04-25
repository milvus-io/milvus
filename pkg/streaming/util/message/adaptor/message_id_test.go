package adaptor

import (
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
	wp "github.com/zilliztech/woodpecker/woodpecker/log"

	msgkafka "github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/kafka"
	msgpulsar "github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	msgwoodpecker "github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/wp"
)

func TestIDConvension(t *testing.T) {
	id := MustGetMessageIDFromMQWrapperID(MustGetMQWrapperIDFromMessage(rmq.NewRmqID(1)))
	assert.True(t, id.EQ(rmq.NewRmqID(1)))

	msgID := pulsar.EarliestMessageID()
	id = MustGetMessageIDFromMQWrapperID(MustGetMQWrapperIDFromMessage(msgpulsar.NewPulsarID(msgID)))
	assert.True(t, id.EQ(msgpulsar.NewPulsarID(msgID)))

	kafkaID := MustGetMessageIDFromMQWrapperID(MustGetMQWrapperIDFromMessage(msgkafka.NewKafkaID(1)))
	assert.True(t, kafkaID.EQ(msgkafka.NewKafkaID(1)))

	logMsgId := wp.EarliestLogMessageID()
	wpID := MustGetMessageIDFromMQWrapperID(MustGetMQWrapperIDFromMessage(msgwoodpecker.NewWpID(&logMsgId)))
	assert.True(t, wpID.EQ(msgwoodpecker.NewWpID(&logMsgId)))
}
