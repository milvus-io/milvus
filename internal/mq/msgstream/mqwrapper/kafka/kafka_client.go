package kafka

import (
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/internal/util/paramtable"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"go.uber.org/zap"
)

var Producer *kafka.Producer
var once sync.Once

type kafkaClient struct {
	// more configs you can see https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	basicConfig kafka.ConfigMap
}

func getBasicConfig(address string) kafka.ConfigMap {
	return kafka.ConfigMap{
		"bootstrap.servers":   address,
		"socket.timeout.ms":   300000,
		"socket.max.fails":    3,
		"api.version.request": true,
	}
}

func NewKafkaClientInstance(address string) *kafkaClient {
	config := getBasicConfig(address)
	return &kafkaClient{basicConfig: config}
}

func NewKafkaClientInstanceWithConfigMap(config kafka.ConfigMap) *kafkaClient {
	return &kafkaClient{basicConfig: config}
}

func NewKafkaClientInstanceWithConfig(config *paramtable.KafkaConfig) *kafkaClient {
	kafkaConfig := getBasicConfig(config.Address)

	if (config.SaslUsername == "" && config.SaslPassword != "") ||
		(config.SaslUsername != "" && config.SaslPassword == "") {
		panic("enable security mode need config username and password at the same time!")
	}

	if config.SaslUsername != "" && config.SaslPassword != "" {
		kafkaConfig.SetKey("sasl.mechanisms", config.SaslMechanisms)
		kafkaConfig.SetKey("security.protocol", config.SecurityProtocol)
		kafkaConfig.SetKey("sasl.username", config.SaslUsername)
		kafkaConfig.SetKey("sasl.password", config.SaslPassword)
	}

	return &kafkaClient{basicConfig: kafkaConfig}
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
	newConf.SetKey("linger.ms", 20)
	return newConf
}

func (kc *kafkaClient) newConsumerConfig(group string, offset mqwrapper.SubscriptionInitialPosition) *kafka.ConfigMap {
	newConf := cloneKafkaConfig(kc.basicConfig)

	if offset == mqwrapper.SubscriptionPositionEarliest {
		newConf.SetKey("auto.offset.reset", "earliest")
	} else {
		newConf.SetKey("auto.offset.reset", "latest")
	}

	newConf.SetKey("session.timeout.ms", 180000)
	newConf.SetKey("group.id", group)
	newConf.SetKey("enable.auto.commit", false)

	//Kafka default will not create topics if consumer's the topics don't exist.
	//In order to compatible with other MQ, we need to enable the following configuration,
	//meanwhile, some implementation also try to consume a non-exist topic, such as dataCoordTimeTick.
	newConf.SetKey("allow.auto.create.topics", true)

	//newConf.SetKey("enable.partition.eof", true)
	newConf.SetKey("go.events.channel.enable", true)
	return newConf
}

func (kc *kafkaClient) CreateProducer(options mqwrapper.ProducerOptions) (mqwrapper.Producer, error) {
	pp, err := kc.getKafkaProducer()
	if err != nil {
		return nil, err
	}

	deliveryChan := make(chan kafka.Event, 128)
	producer := &kafkaProducer{p: pp, deliveryChan: deliveryChan, topic: options.Topic}
	return producer, nil
}

func (kc *kafkaClient) Subscribe(options mqwrapper.ConsumerOptions) (mqwrapper.Consumer, error) {
	config := kc.newConsumerConfig(options.SubscriptionName, options.SubscriptionInitialPosition)
	consumer, err := newKafkaConsumer(config, options.Topic, options.SubscriptionName)
	return consumer, err
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

func (kc *kafkaClient) BytesToMsgID(id []byte) (mqwrapper.MessageID, error) {
	offset := DeserializeKafkaID(id)
	return &kafkaID{messageID: offset}, nil
}

func (kc *kafkaClient) Close() {
}
