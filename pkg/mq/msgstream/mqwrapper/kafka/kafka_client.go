package kafka

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
)

var (
	producer atomic.Pointer[kafka.Producer]
	sf       conc.Singleflight[*kafka.Producer]
)

var once sync.Once

type kafkaClient struct {
	// more configs you can see https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	basicConfig    kafka.ConfigMap
	consumerConfig kafka.ConfigMap
	producerConfig kafka.ConfigMap
}

func getBasicConfig(address string) kafka.ConfigMap {
	return kafka.ConfigMap{
		"bootstrap.servers":        address,
		"api.version.request":      true,
		"reconnect.backoff.ms":     20,
		"reconnect.backoff.max.ms": 5000,
	}
}

func ConfigtoString(config kafka.ConfigMap) string {
	configString := "["
	for key := range config {
		if key == "sasl.password" || key == "sasl.username" {
			configString += key + ":" + "*** "
		} else {
			value, _ := config.Get(key, nil)
			configString += key + ":" + fmt.Sprintf("%v ", value)
		}
	}
	if len(configString) > 1 {
		configString = configString[:len(configString)-1]
	}
	configString += "]"
	return configString
}

func NewKafkaClientInstance(address string) *kafkaClient {
	config := getBasicConfig(address)
	return NewKafkaClientInstanceWithConfigMap(config, kafka.ConfigMap{}, kafka.ConfigMap{})
}

func NewKafkaClientInstanceWithConfigMap(config kafka.ConfigMap, extraConsumerConfig kafka.ConfigMap, extraProducerConfig kafka.ConfigMap) *kafkaClient {
	log.Info("init kafka Config ", zap.String("commonConfig", ConfigtoString(config)),
		zap.String("extraConsumerConfig", ConfigtoString(extraConsumerConfig)),
		zap.String("extraProducerConfig", ConfigtoString(extraProducerConfig)),
	)
	return &kafkaClient{basicConfig: config, consumerConfig: extraConsumerConfig, producerConfig: extraProducerConfig}
}

func GetBasicConfig(config *paramtable.KafkaConfig) kafka.ConfigMap {
	kafkaConfig := getBasicConfig(config.Address.GetValue())

	if (config.SaslUsername.GetValue() == "" && config.SaslPassword.GetValue() != "") ||
		(config.SaslUsername.GetValue() != "" && config.SaslPassword.GetValue() == "") {
		panic("enable security mode need config username and password at the same time!")
	}

	if config.SecurityProtocol.GetValue() != "" {
		kafkaConfig.SetKey("security.protocol", config.SecurityProtocol.GetValue())
	}

	if config.SaslUsername.GetValue() != "" && config.SaslPassword.GetValue() != "" {
		kafkaConfig.SetKey("sasl.mechanisms", config.SaslMechanisms.GetValue())
		kafkaConfig.SetKey("sasl.username", config.SaslUsername.GetValue())
		kafkaConfig.SetKey("sasl.password", config.SaslPassword.GetValue())
	}

	if config.KafkaUseSSL.GetAsBool() {
		kafkaConfig.SetKey("ssl.certificate.location", config.KafkaTLSCert.GetValue())
		kafkaConfig.SetKey("ssl.key.location", config.KafkaTLSKey.GetValue())
		kafkaConfig.SetKey("ssl.ca.location", config.KafkaTLSCACert.GetValue())
		if config.KafkaTLSKeyPassword.GetValue() != "" {
			kafkaConfig.SetKey("ssl.key.password", config.KafkaTLSKeyPassword.GetValue())
		}
	}

	return kafkaConfig
}

func NewKafkaClientInstanceWithConfig(ctx context.Context, config *paramtable.KafkaConfig) (*kafkaClient, error) {
	// connection setup timeout, default as 30000ms, available range is [1000, 2147483647]
	if deadline, ok := ctx.Deadline(); ok {
		if deadline.Before(time.Now()) {
			return nil, errors.New("context timeout when new kafka client")
		}
		// timeout := time.Until(deadline).Milliseconds()
		// kafkaConfig.SetKey("socket.connection.setup.timeout.ms", strconv.FormatInt(timeout, 10))
	}

	kafkaConfig := GetBasicConfig(config)
	specExtraConfig := func(config map[string]string) kafka.ConfigMap {
		kafkaConfigMap := make(kafka.ConfigMap, len(config))
		for k, v := range config {
			kafkaConfigMap.SetKey(k, v)
		}
		return kafkaConfigMap
	}

	return NewKafkaClientInstanceWithConfigMap(
		kafkaConfig,
		specExtraConfig(config.ConsumerExtraConfig.GetValue()),
		specExtraConfig(config.ProducerExtraConfig.GetValue())), nil
}

func cloneKafkaConfig(config kafka.ConfigMap) *kafka.ConfigMap {
	newConfig := make(kafka.ConfigMap)
	for k, v := range config {
		newConfig[k] = v
	}
	return &newConfig
}

func (kc *kafkaClient) getKafkaProducer() (*kafka.Producer, error) {
	if p := producer.Load(); p != nil {
		return p, nil
	}
	log := log.Ctx(context.TODO())
	p, err, _ := sf.Do("kafka_producer", func() (*kafka.Producer, error) {
		if p := producer.Load(); p != nil {
			return p, nil
		}
		config := kc.newProducerConfig()
		p, err := kafka.NewProducer(config)
		if err != nil {
			log.Error("create sync kafka producer failed", zap.Error(err))
			return nil, err
		}
		go func() {
			for e := range p.Events() {
				switch ev := e.(type) {
				case kafka.Error:
					// Generic client instance-level errors, such as broker connection failures,
					// authentication issues, etc.
					// After a fatal error has been raised, any subsequent Produce*() calls will fail with
					// the original error code.
					log.Error("kafka error", zap.String("error msg", ev.Error()))
					if ev.IsFatal() {
						panic(ev)
					}
				default:
					log.Debug("kafka producer event", zap.Any("event", ev))
				}
			}
		}()
		producer.Store(p)
		return p, nil
	})
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (kc *kafkaClient) newProducerConfig() *kafka.ConfigMap {
	newConf := cloneKafkaConfig(kc.basicConfig)
	// default max message size 5M
	newConf.SetKey("message.max.bytes", 10485760)
	newConf.SetKey("compression.codec", "zstd")
	// we want to ensure tt send out as soon as possible
	newConf.SetKey("linger.ms", 2)

	// special producer config
	kc.specialExtraConfig(newConf, kc.producerConfig)

	return newConf
}

func (kc *kafkaClient) newConsumerConfig(group string, offset common.SubscriptionInitialPosition) *kafka.ConfigMap {
	newConf := cloneKafkaConfig(kc.basicConfig)

	newConf.SetKey("group.id", group)
	newConf.SetKey("enable.auto.commit", false)
	// Kafka default will not create topics if consumer's the topics don't exist.
	// In order to compatible with other MQ, we need to enable the following configuration,
	// meanwhile, some implementation also try to consume a non-exist topic, such as dataCoordTimeTick.
	newConf.SetKey("allow.auto.create.topics", true)
	kc.specialExtraConfig(newConf, kc.consumerConfig)

	return newConf
}

func (kc *kafkaClient) CreateProducer(ctx context.Context, options common.ProducerOptions) (mqwrapper.Producer, error) {
	start := timerecord.NewTimeRecorder("create producer")
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateProducerLabel, metrics.TotalLabel).Inc()

	pp, err := kc.getKafkaProducer()
	if err != nil {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateProducerLabel, metrics.FailLabel).Inc()
		return nil, err
	}

	elapsed := start.ElapseSpan()
	metrics.MsgStreamRequestLatency.WithLabelValues(metrics.CreateProducerLabel).Observe(float64(elapsed.Milliseconds()))
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateProducerLabel, metrics.SuccessLabel).Inc()

	producer := &kafkaProducer{p: pp, stopCh: make(chan struct{}), topic: options.Topic}
	return producer, nil
}

func (kc *kafkaClient) Subscribe(ctx context.Context, options mqwrapper.ConsumerOptions) (mqwrapper.Consumer, error) {
	start := timerecord.NewTimeRecorder("create consumer")
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateConsumerLabel, metrics.TotalLabel).Inc()

	config := kc.newConsumerConfig(options.SubscriptionName, options.SubscriptionInitialPosition)
	consumer, err := newKafkaConsumer(config, options.BufSize, options.Topic, options.SubscriptionName, options.SubscriptionInitialPosition)
	if err != nil {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateConsumerLabel, metrics.FailLabel).Inc()
		return nil, err
	}
	elapsed := start.ElapseSpan()
	metrics.MsgStreamRequestLatency.WithLabelValues(metrics.CreateConsumerLabel).Observe(float64(elapsed.Milliseconds()))
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateConsumerLabel, metrics.SuccessLabel).Inc()
	return consumer, nil
}

func (kc *kafkaClient) EarliestMessageID() common.MessageID {
	return &kafkaID{messageID: int64(kafka.OffsetBeginning)}
}

func (kc *kafkaClient) StringToMsgID(id string) (common.MessageID, error) {
	offset, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return nil, err
	}

	return &kafkaID{messageID: offset}, nil
}

func (kc *kafkaClient) specialExtraConfig(current *kafka.ConfigMap, special kafka.ConfigMap) {
	for k, v := range special {
		if existingConf, _ := current.Get(k, nil); existingConf != nil {
			log.Warn(fmt.Sprintf("The existing config :  %v=%v  will be covered by the speciled kafka config :  %v.", k, v, existingConf))
		}

		current.SetKey(k, v)
	}
}

func (kc *kafkaClient) BytesToMsgID(id []byte) (common.MessageID, error) {
	offset := DeserializeKafkaID(id)
	return &kafkaID{messageID: offset}, nil
}

func (kc *kafkaClient) Close() {
}
