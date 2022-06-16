package dependency

import (
	"context"

	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/paramtable"
)

type DefaultFactory struct {
	standAlone          bool
	chunkManagerFactory storage.Factory
	msgStreamFactory    msgstream.Factory
}

func NewDefaultFactory(standAlone bool) *DefaultFactory {
	return &DefaultFactory{
		standAlone:       standAlone,
		msgStreamFactory: msgstream.NewRmsFactory("/tmp/milvus/rocksmq/"),
		chunkManagerFactory: storage.NewChunkManagerFactory("local", "local",
			storage.RootPath("/tmp/milvus")),
	}
}

func NewFactory(standAlone bool) *DefaultFactory {
	return &DefaultFactory{standAlone: standAlone}
}

// Init create a msg factory(TODO only support one mq at the same time.)
// In order to guarantee backward compatibility of config file, we still support multiple mq configs.
// 1. Rocksmq only run on local mode, and it has the highest priority
// 2. Pulsar has higher priority than Kafka within remote msg
func (f *DefaultFactory) Init(params *paramtable.ComponentParam) {
	// skip if using default factory
	if f.msgStreamFactory != nil {
		return
	}

	// init storage
	if params.CommonCfg.StorageType == "local" {
		f.chunkManagerFactory = storage.NewChunkManagerFactory("local", "local",
			storage.RootPath(params.LocalStorageCfg.Path))
	} else {
		f.chunkManagerFactory = storage.NewChunkManagerFactory("local", "minio",
			storage.RootPath(params.LocalStorageCfg.Path),
			storage.Address(params.MinioCfg.Address),
			storage.AccessKeyID(params.MinioCfg.AccessKeyID),
			storage.SecretAccessKeyID(params.MinioCfg.SecretAccessKey),
			storage.UseSSL(params.MinioCfg.UseSSL),
			storage.BucketName(params.MinioCfg.BucketName),
			storage.UseIAM(params.MinioCfg.UseIAM),
			storage.IAMEndpoint(params.MinioCfg.IAMEndpoint),
			storage.CreateBucket(true))
	}

	// init mq storage
	if f.standAlone {
		f.msgStreamFactory = f.initMQLocalService(params)
		if f.msgStreamFactory == nil {
			f.msgStreamFactory = f.initMQRemoteService(params)
			if f.msgStreamFactory == nil {
				panic("no available mq configuration, must config rocksmq, Pulsar or Kafka at least one of these!")
			}
		}
		return
	}

	f.msgStreamFactory = f.initMQRemoteService(params)
	if f.msgStreamFactory == nil {
		panic("no available remote mq configuration, must config Pulsar or Kafka at least one of these!")
	}
}

func (f *DefaultFactory) initMQLocalService(params *paramtable.ComponentParam) msgstream.Factory {
	if params.RocksmqEnable() {
		path, _ := params.Load("_RocksmqPath")
		return msgstream.NewRmsFactory(path)
	}
	return nil
}

// initRemoteService Pulsar has higher priority than Kafka.
func (f *DefaultFactory) initMQRemoteService(params *paramtable.ComponentParam) msgstream.Factory {
	if params.PulsarEnable() {
		return msgstream.NewPmsFactory(&params.PulsarCfg)
	}

	if params.KafkaEnable() {
		return msgstream.NewKmsFactory(&params.KafkaCfg)
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

func (f *DefaultFactory) NewCacheStorageChunkManager(ctx context.Context) (storage.ChunkManager, error) {
	return f.chunkManagerFactory.NewCacheStorageChunkManager(ctx)
}

func (f *DefaultFactory) NewVectorStorageChunkManager(ctx context.Context) (storage.ChunkManager, error) {
	return f.chunkManagerFactory.NewVectorStorageChunkManager(ctx)
}

type Factory interface {
	msgstream.Factory
	Init(p *paramtable.ComponentParam)
	NewCacheStorageChunkManager(ctx context.Context) (storage.ChunkManager, error)
	NewVectorStorageChunkManager(ctx context.Context) (storage.ChunkManager, error)
}
