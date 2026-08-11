package kafka

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
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

func TestReadOnlyMissingTopicDoesNotAutoCreate(t *testing.T) {
	if os.Getenv("MILVUS_UT_WITHOUT_KAFKA") != "" {
		t.Skip("there's no kafka broker available, skipping kafka test")
	}

	opener, err := (&builderImpl{}).Build()
	require.NoError(t, err)
	defer opener.Close()

	topic := fmt.Sprintf("missing-historical-topic-%d", time.Now().UnixNano())
	wal, err := opener.Open(context.Background(), &walimpls.OpenOption{
		Channel: types.PChannelInfo{Name: topic, AccessMode: types.AccessModeRO},
	})
	require.NoError(t, err)
	defer wal.Close()

	scanner, err := wal.Read(context.Background(), walimpls.ReadOption{
		Name:          "missing-historical-reader",
		DeliverPolicy: options.DeliverPolicyAll(),
	})
	require.NoError(t, err)
	defer scanner.Close()

	select {
	case _, ok := <-scanner.Chan():
		require.False(t, ok)
		require.ErrorIs(t, scanner.Error(), merr.ErrMqTopicNotFound)
	case <-time.After(15 * time.Second):
		t.Fatal("read-only kafka scanner did not reject the missing topic")
	}
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
