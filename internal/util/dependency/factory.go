package dependency

import (
	"context"

	"github.com/cockroachdb/errors"
	smsgstream "github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"go.uber.org/zap"
)

const (
	mqTypeDefault = "default"
	mqTypeNatsmq  = "natsmq"
	mqTypeRocksmq = "rocksmq"
	mqTypeKafka   = "kafka"
	mqTypePulsar  = "pulsar"
)

type mqEnable struct {
	Rocksmq bool
	Natsmq  bool
	Pulsar  bool
	Kafka   bool
}

// DefaultFactory is a factory that produces instances of storage.ChunkManager and message queue.
type DefaultFactory struct {
	standAlone          bool
	chunkManagerFactory storage.Factory
	msgStreamFactory    msgstream.Factory
}

// Only for test
func NewDefaultFactory(standAlone bool) *DefaultFactory {
	return &DefaultFactory{
		standAlone:       standAlone,
		msgStreamFactory: smsgstream.NewRocksmqFactory("/tmp/milvus/rocksmq/"),
		chunkManagerFactory: storage.NewChunkManagerFactory("local",
			storage.RootPath("/tmp/milvus")),
	}
}

// Only for test
func MockDefaultFactory(standAlone bool, params *paramtable.ComponentParam) *DefaultFactory {
	return &DefaultFactory{
		standAlone:          standAlone,
		msgStreamFactory:    smsgstream.NewRocksmqFactory("/tmp/milvus/rocksmq/"),
		chunkManagerFactory: storage.NewChunkManagerFactoryWithParam(params),
	}
}

// NewFactory creates a new instance of the DefaultFactory type.
// If standAlone is true, the factory will operate in standalone mode.
func NewFactory(standAlone bool) *DefaultFactory {
	return &DefaultFactory{standAlone: standAlone}
}

// Init create a msg factory(TODO only support one mq at the same time.)
// In order to guarantee backward compatibility of config file, we still support multiple mq configs.
// The initialization of MQ follows the following rules, if the mq.type is default.
// 1. standalone(local) mode: rocksmq(default) > natsmq > Pulsar > Kafka
// 2. cluster mode:  Pulsar(default) > Kafka (rocksmq and natsmq is unsupported in cluster mode)
func (f *DefaultFactory) Init(params *paramtable.ComponentParam) {
	// skip if using default factory
	if f.msgStreamFactory != nil {
		return
	}

	f.chunkManagerFactory = storage.NewChunkManagerFactoryWithParam(params)

	// initialize mq client or embedded mq.
	if err := f.initMQ(f.standAlone, params); err != nil {
		panic(err)
	}
}

func (f *DefaultFactory) initMQ(standalone bool, params *paramtable.ComponentParam) error {
	mqType := mustSelectMQType(standalone, params.MQCfg.Type.GetValue(), mqEnable{params.RocksmqEnable(), params.NatsmqEnable(), params.PulsarEnable(), params.KafkaEnable()})
	log.Info("try to init mq", zap.Bool("standalone", standalone), zap.String("mqType", mqType))

	switch mqType {
	case mqTypeNatsmq:
		f.msgStreamFactory = msgstream.NewNatsmqFactory()
	case mqTypeRocksmq:
		f.msgStreamFactory = smsgstream.NewRocksmqFactory(params.RocksmqCfg.Path.GetValue())
	case mqTypePulsar:
		f.msgStreamFactory = msgstream.NewPmsFactory(&params.PulsarCfg)
	case mqTypeKafka:
		f.msgStreamFactory = msgstream.NewKmsFactory(&params.KafkaCfg)
	}
	if f.msgStreamFactory == nil {
		return errors.New("failed to create MQ: check the milvus log for initialization failures")
	}
	return nil
}

// Select valid mq if mq type is default.
func mustSelectMQType(standalone bool, mqType string, enable mqEnable) string {
	if mqType != mqTypeDefault {
		if err := validateMQType(standalone, mqType); err != nil {
			panic(err)
		}
		return mqType
	}

	if standalone {
		if enable.Rocksmq {
			return mqTypeRocksmq
		}
		if enable.Natsmq {
			return mqTypeNatsmq
		}
	}
	if enable.Pulsar {
		return mqTypePulsar
	}
	if enable.Kafka {
		return mqTypeKafka
	}

	panic(errors.Errorf("no available mq config found, %s, enable: %+v", mqType, enable))
}

// Validate mq type.
func validateMQType(standalone bool, mqType string) error {
	if mqType != mqTypeNatsmq && mqType != mqTypeRocksmq && mqType != mqTypeKafka && mqType != mqTypePulsar {
		return errors.Newf("mq type %s is invalid", mqType)
	}
	if !standalone && (mqType == mqTypeRocksmq || mqType == mqTypeNatsmq) {
		return errors.Newf("mq %s is only valid in standalone mode")
	}
	return nil
}

func (f *DefaultFactory) NewMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return f.msgStreamFactory.NewMsgStream(ctx)
}

func (f *DefaultFactory) NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return f.msgStreamFactory.NewTtMsgStream(ctx)
}

func (f *DefaultFactory) NewQueryMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return f.msgStreamFactory.NewQueryMsgStream(ctx)
}

func (f *DefaultFactory) NewMsgStreamDisposer(ctx context.Context) func([]string, string) error {
	return f.msgStreamFactory.NewMsgStreamDisposer(ctx)
}

func (f *DefaultFactory) NewPersistentStorageChunkManager(ctx context.Context) (storage.ChunkManager, error) {
	return f.chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
}

type Factory interface {
	msgstream.Factory
	Init(p *paramtable.ComponentParam)
	NewPersistentStorageChunkManager(ctx context.Context) (storage.ChunkManager, error)
}
