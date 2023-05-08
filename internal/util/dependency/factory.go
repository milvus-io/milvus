package dependency

import (
	"context"

	"github.com/cockroachdb/errors"
	smsgstream "github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

const (
	mqTypeDefault = "default"
	mqTypeNatsmq  = "natsmq"
	mqTypeRocksmq = "rocksmq"
	mqTypeKafka   = "kafka"
	mqTypePulsar  = "pulsar"
)

type DefaultFactory struct {
	standAlone          bool
	chunkManagerFactory storage.Factory
	msgStreamFactory    msgstream.Factory
}

// Only for test
func NewDefaultFactory(standAlone bool) *DefaultFactory {
	return &DefaultFactory{
		standAlone:       standAlone,
		msgStreamFactory: smsgstream.NewFactory(smsgstream.MsgStreamTypeRmq, "/tmp/milvus/rocksmq/"),
		chunkManagerFactory: storage.NewChunkManagerFactory("local",
			storage.RootPath("/tmp/milvus")),
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
// 1. standalone(local) mode: rocksmq(ifdefault) > natsmq > Pulsar > Kafka
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
	err := errors.New("invalid mq config")
	mqType := f.selectMQType(standalone, params)
	if mqType == "" {
		return errors.Wrapf(err, "no available mq config found, %s", mqType)
	}
	if err := f.validateMQType(standalone, mqType); err != nil {
		panic(err)
	}

	generateErr := func(item *paramtable.ParamItem) error {
		return errors.Wrapf(err, "%s should not be empty when %s is %s", item.Key, params.MQCfg.Type.Key, mqType)
	}

	switch mqType {
	case mqTypeNatsmq:
		if !params.NatsmqEnable() {
			return generateErr(&params.NatsmqCfg.Server.StoreDir)
		}
		f.msgStreamFactory = smsgstream.NewFactory(smsgstream.MsgStreamTypeNmq, params.NatsmqCfg.Server.StoreDir.GetValue())
	case mqTypeRocksmq:
		if !params.RocksmqEnable() {
			return generateErr(&params.RocksmqCfg.Path)
		}
		f.msgStreamFactory = smsgstream.NewFactory(smsgstream.MsgStreamTypeRmq, params.RocksmqCfg.Path.GetValue())
	case mqTypePulsar:
		if !params.PulsarEnable() {
			return generateErr(&params.PulsarCfg.Address)
		}
		f.msgStreamFactory = msgstream.NewPmsFactory(&params.PulsarCfg)
	case mqTypeKafka:
		if !params.KafkaEnable() {
			return generateErr(&params.KafkaCfg.Address)
		}
		f.msgStreamFactory = msgstream.NewKmsFactory(&params.KafkaCfg)
	}

	if f.msgStreamFactory == nil {
		return errors.New("failed to create MQ: check the milvus log for initialization failures")
	}
	return nil
}

// Select valid mq if mq type is default.
func (f *DefaultFactory) selectMQType(standalone bool, params *paramtable.ComponentParam) string {
	mqType := params.MQCfg.Type.GetValue()
	if mqType != mqTypeDefault {
		return mqType
	}

	if standalone {
		if params.RocksmqEnable() {
			return mqTypeRocksmq
		}
		if params.NatsmqEnable() {
			return mqTypeNatsmq
		}
	}
	if params.PulsarEnable() {
		return mqTypePulsar
	}
	if params.KafkaEnable() {
		return mqTypeKafka
	}
	return ""
}

// Validate mq type.
func (f *DefaultFactory) validateMQType(standalone bool, mqType string) error {
	if mqType != mqTypeNatsmq && mqType != mqTypeRocksmq && mqType != mqTypeKafka && mqType != mqTypePulsar {
		return errors.Newf("mq.type %s is invalid", mqType)
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
