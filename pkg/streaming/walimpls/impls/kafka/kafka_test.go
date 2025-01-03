package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

func TestRegistry(t *testing.T) {
	registeredB := registry.MustGetBuilder(walName)
	assert.NotNil(t, registeredB)
	assert.Equal(t, walName, registeredB.Name())

	id, err := message.UnmarshalMessageID(walName,
		kafkaID(123).Marshal())
	assert.NoError(t, err)
	assert.True(t, id.EQ(kafkaID(123)))
}

func TestKafka(t *testing.T) {
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
