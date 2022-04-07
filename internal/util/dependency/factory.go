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

func (f *DefaultFactory) Init(params *paramtable.ComponentParam) {
	if f.standAlone {
		path, _ := params.Load("_RocksmqPath")
		f.msgStreamFactory = msgstream.NewRmsFactory(path)
		f.chunkManagerFactory = storage.NewChunkManagerFactory("local", "local",
			storage.RootPath(params.LocalStorageCfg.Path))
	} else {
		f.msgStreamFactory = msgstream.NewPmsFactory(&params.PulsarCfg)
		f.chunkManagerFactory = storage.NewChunkManagerFactory("local", "minio",
			storage.RootPath(params.LocalStorageCfg.Path),
			storage.Address(params.MinioCfg.Address),
			storage.AccessKeyID(params.MinioCfg.AccessKeyID),
			storage.SecretAccessKeyID(params.MinioCfg.SecretAccessKey),
			storage.UseSSL(params.MinioCfg.UseSSL),
			storage.BucketName(params.MinioCfg.BucketName),
			storage.CreateBucket(true))
	}
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

func (f *DefaultFactory) NewCacheStorageChunkManager(ctx context.Context) (storage.ChunkManager, error) {
	return f.chunkManagerFactory.NewCacheStorageChunkManager(ctx)
}

func (f *DefaultFactory) NewVectorStorageChunkManager(ctx context.Context) (storage.ChunkManager, error) {
	return f.chunkManagerFactory.NewVectorStorageChunkManager(ctx)
}

type Factory interface {
	Init(p *paramtable.ComponentParam)
	NewMsgStream(ctx context.Context) (msgstream.MsgStream, error)
	NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error)
	NewQueryMsgStream(ctx context.Context) (msgstream.MsgStream, error)
	NewCacheStorageChunkManager(ctx context.Context) (storage.ChunkManager, error)
	NewVectorStorageChunkManager(ctx context.Context) (storage.ChunkManager, error)
}
