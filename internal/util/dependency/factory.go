package dependency

import (
	"context"
	"sync"

	smsgstream "github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var (
	once           = &sync.Once{}
	defaultFactory *DefaultFactory
)

// DefaultFactory is a factory that produces instances of storage.ChunkManager and message queue.
type DefaultFactory struct {
	chunkManagerFactory storage.Factory
	msgStreamFactory    msgstream.Factory
}

// Only for test
func NewDefaultFactory(standAlone bool) *DefaultFactory {
	return &DefaultFactory{
		msgStreamFactory: smsgstream.NewRocksmqFactory("/tmp/milvus/rocksmq/", &paramtable.Get().ServiceParam),
		chunkManagerFactory: storage.NewChunkManagerFactory("local",
			storage.RootPath("/tmp/milvus")),
	}
}

// Only for test
func MockDefaultFactory(standAlone bool, params *paramtable.ComponentParam) *DefaultFactory {
	return &DefaultFactory{
		msgStreamFactory:    smsgstream.NewRocksmqFactory("/tmp/milvus/rocksmq/", &paramtable.Get().ServiceParam),
		chunkManagerFactory: storage.NewChunkManagerFactoryWithParam(params),
	}
}

// NewFactory creates a new instance of the DefaultFactory type.
// If standAlone is true, the factory will operate in standalone mode.
// Init create a msg factory(TODO only support one mq at the same time.)
// In order to guarantee backward compatibility of config file, we still support multiple mq configs.
// The initialization of MQ follows the following rules, if the mq.type is default.
// 1. standalone(local) mode: rocksmq(default) > natsmq > Pulsar > Kafka
// 2. cluster mode:  Pulsar(default) > Kafka (rocksmq and natsmq is unsupported in cluster mode)
// panic if dependency is not satisfied.
func NewFactory(standalone bool) *DefaultFactory {
	// A basic dependency should be global unique.
	once.Do(func() {
		params := paramtable.Get()
		util.EnableStandAlone(standalone)

		defaultFactory = &DefaultFactory{
			chunkManagerFactory: storage.NewChunkManagerFactoryWithParam(params),
			msgStreamFactory:    smsgstream.NewLogServiceFactory(),
		}
	})
	return defaultFactory
}

// GetFactory returns the global factory instance.
func GetFactory() *DefaultFactory {
	return defaultFactory
}

func (f *DefaultFactory) NewMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	// TODO: we should only use a single client for all msgstream.
	// But there's multiple msgstream client in milvus, remove in future.
	return f.msgStreamFactory.NewMsgStream(ctx)
}

func (f *DefaultFactory) NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return f.msgStreamFactory.NewTtMsgStream(ctx)
}

func (f *DefaultFactory) NewMsgStreamDisposer(ctx context.Context) func([]string, string) error {
	return f.msgStreamFactory.NewMsgStreamDisposer(ctx)
}

func (f *DefaultFactory) NewPersistentStorageChunkManager(ctx context.Context) (storage.ChunkManager, error) {
	return f.chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
}

type Factory interface {
	// TODO: message stream isn't the basic component of lognode should be removed in lognode.
	// TODO: removed in future, replaced by logservice.Client.
	msgstream.Factory

	NewPersistentStorageChunkManager(ctx context.Context) (storage.ChunkManager, error)
}
