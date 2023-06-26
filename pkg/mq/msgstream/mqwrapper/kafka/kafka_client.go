package kafka

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

var Producer *kafka.Producer

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

func NewKafkaClientInstance(address string) *kafkaClient {
	config := getBasicConfig(address)
	return NewKafkaClientInstanceWithConfigMap(config, kafka.ConfigMap{}, kafka.ConfigMap{})

}

func NewKafkaClientInstanceWithConfigMap(config kafka.ConfigMap, extraConsumerConfig kafka.ConfigMap, extraProducerConfig kafka.ConfigMap) *kafkaClient {
	log.Info("init kafka Config ", zap.String("commonConfig", fmt.Sprintf("+%v", config)),
		zap.String("extraConsumerConfig", fmt.Sprintf("+%v", extraConsumerConfig)),
		zap.String("extraProducerConfig", fmt.Sprintf("+%v", extraProducerConfig)),
	)
	return &kafkaClient{basicConfig: config, consumerConfig: extraConsumerConfig, producerConfig: extraProducerConfig}
}

func NewKafkaClientInstanceWithConfig(config *paramtable.KafkaConfig) *kafkaClient {
	kafkaConfig := getBasicConfig(config.Address.GetValue())

	if (config.SaslUsername.GetValue() == "" && config.SaslPassword.GetValue() != "") ||
		(config.SaslUsername.GetValue() != "" && config.SaslPassword.GetValue() == "") {
		panic("enable security mode need config username and password at the same time!")
	}

	if config.SaslUsername.GetValue() != "" && config.SaslPassword.GetValue() != "" {
		kafkaConfig.SetKey("sasl.mechanisms", config.SaslMechanisms.GetValue())
		kafkaConfig.SetKey("security.protocol", config.SecurityProtocol.GetValue())
		kafkaConfig.SetKey("sasl.username", config.SaslUsername.GetValue())
		kafkaConfig.SetKey("sasl.password", config.SaslPassword.GetValue())
	}

	specExtraConfig := func(config map[string]string) kafka.ConfigMap {
		kafkaConfigMap := make(kafka.ConfigMap, len(config))
		for k, v := range config {
			kafkaConfigMap.SetKey(k, v)
		}
		return kafkaConfigMap
	}

	return NewKafkaClientInstanceWithConfigMap(kafkaConfig, specExtraConfig(config.ConsumerExtraConfig.GetValue()), specExtraConfig(config.ProducerExtraConfig.GetValue()))

}

func cloneKafkaConfig(config kafka.ConfigMap) *kafka.ConfigMap {
	newConfig := make(kafka.ConfigMap)
	for k, v := range config {
		newConfig[k] = v
	}
	return &newConfig
}

func (kc *kafkaClient) getKafkaProducer() (*kafka.Producer, error) {
	var err error
	once.Do(func() {
		config := kc.newProducerConfig()
		Producer, err = kafka.NewProducer(config)

		go func() {
			for e := range Producer.Events() {
				switch ev := e.(type) {
				case kafka.Error:
					// Generic client instance-level errors, such as broker connection failures,
					// authentication issues, etc.
					// After a fatal error has been raised, any subsequent Produce*() calls will fail with
					// the original error code.
					log.Error("kafka error", zap.Any("error msg", ev.Error()))
					if ev.IsFatal() {
						panic(ev)
					}
				default:
					log.Debug("kafka producer event", zap.Any("event", ev))
				}
			}
		}()
	})

	if err != nil {
		log.Error("create sync kafka producer failed", zap.Error(err))
		return nil, err
	}

	return Producer, nil
}

func (kc *kafkaClient) newProducerConfig() *kafka.ConfigMap {
	newConf := cloneKafkaConfig(kc.basicConfig)
	// default max message size 5M
	newConf.SetKey("message.max.bytes", 10485760)
	newConf.SetKey("compression.codec", "zstd")
	// we want to ensure tt send out as soon as possible
	newConf.SetKey("linger.ms", 2)

	//special producer config
	kc.specialExtraConfig(newConf, kc.producerConfig)

	return newConf
}

func (kc *kafkaClient) newConsumerConfig(group string, offset mqwrapper.SubscriptionInitialPosition) *kafka.ConfigMap {
	newConf := cloneKafkaConfig(kc.basicConfig)

	newConf.SetKey("group.id", group)
	newConf.SetKey("enable.auto.commit", false)
	//Kafka default will not create topics if consumer's the topics don't exist.
	//In order to compatible with other MQ, we need to enable the following configuration,
	//meanwhile, some implementation also try to consume a non-exist topic, such as dataCoordTimeTick.
	newConf.SetKey("allow.auto.create.topics", true)
	kc.specialExtraConfig(newConf, kc.consumerConfig)

	return newConf
}

func (kc *kafkaClient) CreateProducer(options mqwrapper.ProducerOptions) (mqwrapper.Producer, error) {
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

	deliveryChan := make(chan kafka.Event, 128)
	producer := &kafkaProducer{p: pp, deliveryChan: deliveryChan, topic: options.Topic}
	return producer, nil
}

func (kc *kafkaClient) Subscribe(options mqwrapper.ConsumerOptions) (mqwrapper.Consumer, error) {
	start := timerecord.NewTimeRecorder("create consumer")
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateConsumerLabel, metrics.TotalLabel).Inc()

	config := kc.newConsumerConfig(options.SubscriptionName, options.SubscriptionInitialPosition)
	consumer, err := newKafkaConsumer(config, options.Topic, options.SubscriptionName, options.SubscriptionInitialPosition)
	if err != nil {
		metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateConsumerLabel, metrics.FailLabel).Inc()
		return nil, err
	}
	elapsed := start.ElapseSpan()
	metrics.MsgStreamRequestLatency.WithLabelValues(metrics.CreateConsumerLabel).Observe(float64(elapsed.Milliseconds()))
	metrics.MsgStreamOpCounter.WithLabelValues(metrics.CreateConsumerLabel, metrics.SuccessLabel).Inc()
	return consumer, nil
}

func (kc *kafkaClient) EarliestMessageID() mqwrapper.MessageID {
	return &kafkaID{messageID: int64(kafka.OffsetBeginning)}
}

func (kc *kafkaClient) StringToMsgID(id string) (mqwrapper.MessageID, error) {
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

func (kc *kafkaClient) BytesToMsgID(id []byte) (mqwrapper.MessageID, error) {
	offset := DeserializeKafkaID(id)
	return &kafkaID{messageID: offset}, nil
}

func (kc *kafkaClient) Close() {
}
