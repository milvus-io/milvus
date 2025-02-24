package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	walName = "kafka"
)

func init() {
	// register the builder to the wal registry.
	registry.RegisterBuilder(&builderImpl{})
	// register the unmarshaler to the message registry.
	message.RegisterMessageIDUnmsarshaler(walName, UnmarshalMessageID)
}

// builderImpl is the builder for pulsar wal.
type builderImpl struct{}

// Name returns the name of the wal.
func (b *builderImpl) Name() string {
	return walName
}

// Build build a wal instance.
func (b *builderImpl) Build() (walimpls.OpenerImpls, error) {
	producerConfig, consumerConfig := b.getProducerConfig(), b.getConsumerConfig()

	p, err := kafka.NewProducer(&producerConfig)
	if err != nil {
		return nil, err
	}
	return newOpenerImpl(p, consumerConfig), nil
}

// getProducerAndConsumerConfig returns the producer and consumer config.
func (b *builderImpl) getProducerConfig() kafka.ConfigMap {
	config := &paramtable.Get().KafkaCfg
	producerConfig := getBasicConfig(config)

	producerConfig.SetKey("message.max.bytes", 10485760)
	producerConfig.SetKey("compression.codec", "zstd")
	// we want to ensure tt send out as soon as possible
	producerConfig.SetKey("linger.ms", 5)
	for k, v := range config.ProducerExtraConfig.GetValue() {
		producerConfig.SetKey(k, v)
	}
	return producerConfig
}

func (b *builderImpl) getConsumerConfig() kafka.ConfigMap {
	config := &paramtable.Get().KafkaCfg
	consumerConfig := getBasicConfig(config)
	consumerConfig.SetKey("allow.auto.create.topics", true)
	for k, v := range config.ConsumerExtraConfig.GetValue() {
		consumerConfig.SetKey(k, v)
	}
	return consumerConfig
}

// getBasicConfig returns the basic kafka config.
func getBasicConfig(config *paramtable.KafkaConfig) kafka.ConfigMap {
	basicConfig := kafka.ConfigMap{
		"bootstrap.servers":        config.Address.GetValue(),
		"api.version.request":      true,
		"reconnect.backoff.ms":     20,
		"reconnect.backoff.max.ms": 5000,
	}

	if (config.SaslUsername.GetValue() == "" && config.SaslPassword.GetValue() != "") ||
		(config.SaslUsername.GetValue() != "" && config.SaslPassword.GetValue() == "") {
		panic("enable security mode need config username and password at the same time!")
	}

	if config.SecurityProtocol.GetValue() != "" {
		basicConfig.SetKey("security.protocol", config.SecurityProtocol.GetValue())
	}

	if config.SaslUsername.GetValue() != "" && config.SaslPassword.GetValue() != "" {
		basicConfig.SetKey("sasl.mechanisms", config.SaslMechanisms.GetValue())
		basicConfig.SetKey("sasl.username", config.SaslUsername.GetValue())
		basicConfig.SetKey("sasl.password", config.SaslPassword.GetValue())
	}

	if config.KafkaUseSSL.GetAsBool() {
		basicConfig.SetKey("ssl.certificate.location", config.KafkaTLSCert.GetValue())
		basicConfig.SetKey("ssl.key.location", config.KafkaTLSKey.GetValue())
		basicConfig.SetKey("ssl.ca.location", config.KafkaTLSCACert.GetValue())
		if config.KafkaTLSKeyPassword.GetValue() != "" {
			basicConfig.SetKey("ssl.key.password", config.KafkaTLSKeyPassword.GetValue())
		}
	}
	return basicConfig
}

// cloneKafkaConfig clones a kafka config.
func cloneKafkaConfig(config kafka.ConfigMap) kafka.ConfigMap {
	newConfig := make(kafka.ConfigMap)
	for k, v := range config {
		newConfig[k] = v
	}
	return newConfig
}
