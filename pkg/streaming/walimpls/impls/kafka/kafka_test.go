package kafka

import (
	"os"
	"testing"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

func TestRegistry(t *testing.T) {
	registeredB := registry.MustGetBuilder(message.WALNameKafka)
	assert.NotNil(t, registeredB)
	assert.Equal(t, message.WALNameKafka, registeredB.Name())

	id, err := message.UnmarshalMessageID(&commonpb.MessageID{WALName: commonpb.WALName(message.WALNameKafka), Id: kafkaID(123).Marshal()})
	assert.NoError(t, err)
	assert.True(t, id.EQ(kafkaID(123)))

	id, err = message.UnmarshalMessageID(kafkaID(-2).IntoProto())
	assert.NoError(t, err)
	assert.True(t, id.EQ(kafkaID(-2)))
}

func TestKafka(t *testing.T) {
	if os.Getenv("MILVUS_UT_WITHOUT_KAFKA") != "" {
		t.Skip("there's no kafka broker available, skipping kafka test")
	}
	walimpls.NewWALImplsTestFramework(t, 100, &builderImpl{}).Run()
}

func TestGetBasicConfig(t *testing.T) {
	config := &paramtable.Get().KafkaCfg
	oldSecurityProtocol := config.SecurityProtocol.SwapTempValue("test")
	oldSaslUsername := config.SaslUsername.SwapTempValue("test")
	oldSaslPassword := config.SaslPassword.SwapTempValue("test")
	oldkafkaUseSSL := config.KafkaUseSSL.SwapTempValue("true")
	oldKafkaTLSKeyPassword := config.KafkaTLSKeyPassword.SwapTempValue("test")
	defer func() {
		config.SecurityProtocol.SwapTempValue(oldSecurityProtocol)
		config.SaslUsername.SwapTempValue(oldSaslUsername)
		config.SaslPassword.SwapTempValue(oldSaslPassword)
		config.KafkaUseSSL.SwapTempValue(oldkafkaUseSSL)
		config.KafkaTLSKeyPassword.SwapTempValue(oldKafkaTLSKeyPassword)
	}()
	basicConfig := getBasicConfig(config)

	assert.NotNil(t, basicConfig["ssl.key.password"])
	assert.NotNil(t, basicConfig["ssl.certificate.location"])
	assert.NotNil(t, basicConfig["sasl.username"])
	assert.NotNil(t, basicConfig["security.protocol"])
}

func TestGetProducerConfigUsesConfiguredMessageMaxBytes(t *testing.T) {
	config := &paramtable.Get().KafkaCfg
	oldValue := config.ProducerMessageMaxBytes.SwapTempValue("4096")
	defer config.ProducerMessageMaxBytes.SwapTempValue(oldValue)

	producerConfig := (&builderImpl{}).getProducerConfig()
	value, err := producerConfig.Get("message.max.bytes", nil)
	assert.NoError(t, err)
	assert.Equal(t, 4096, value)
}

func TestMapKafkaReadError(t *testing.T) {
	topicNotFound := ckafka.NewError(ckafka.ErrUnknownTopicOrPart, "historical topic does not exist", false)
	assert.ErrorIs(t, mapKafkaReadError("missing-topic", topicNotFound), merr.ErrMqTopicNotFound)
	offsetOutOfRange := ckafka.NewError(ckafka.ErrOffsetOutOfRange, "historical offset was removed", false)
	assert.ErrorIs(t, mapKafkaReadError("retained-topic", offsetOutOfRange), merr.ErrMqTopicNotFound)

	transient := ckafka.NewError(ckafka.ErrTimedOut, "transient metadata timeout", false)
	assert.Equal(t, transient, mapKafkaReadError("topic", transient))
}

func TestValidateKafkaReadOffset(t *testing.T) {
	assert.NoError(t, validateKafkaReadOffset("topic", int64(ckafka.OffsetBeginning), 0, 0))
	assert.NoError(t, validateKafkaReadOffset("topic", 10, 10, 20))
	assert.NoError(t, validateKafkaReadOffset("topic", 19, 10, 20))
	assert.ErrorIs(t, validateKafkaReadOffset("topic", 9, 10, 20), merr.ErrMqTopicNotFound)
	assert.ErrorIs(t, validateKafkaReadOffset("topic", 20, 10, 20), merr.ErrMqTopicNotFound)
	assert.ErrorIs(t, validateKafkaReadOffset("topic", 0, 0, 0), merr.ErrMqTopicNotFound)
}
