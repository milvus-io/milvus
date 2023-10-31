package storage

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/storage"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type ChunkManagerFactory struct {
	persistentStorage string
	config            *storage.Config
}

func NewChunkManagerFactoryWithParam(params *paramtable.ComponentParam) *ChunkManagerFactory {
	if params.CommonCfg.StorageType.GetValue() == "local" {
		return NewChunkManagerFactory("local", storage.RootPath(params.LocalStorageCfg.Path.GetValue()))
	}
	return NewChunkManagerFactory(params.CommonCfg.StorageType.GetValue(),
		storage.RootPath(params.MinioCfg.RootPath.GetValue()),
		storage.Address(params.MinioCfg.Address.GetValue()),
		storage.AccessKeyID(params.MinioCfg.AccessKeyID.GetValue()),
		storage.SecretAccessKeyID(params.MinioCfg.SecretAccessKey.GetValue()),
		storage.UseSSL(params.MinioCfg.UseSSL.GetAsBool()),
		storage.BucketName(params.MinioCfg.BucketName.GetValue()),
		storage.UseIAM(params.MinioCfg.UseIAM.GetAsBool()),
		storage.CloudProvider(params.MinioCfg.CloudProvider.GetValue()),
		storage.IAMEndpoint(params.MinioCfg.IAMEndpoint.GetValue()),
		storage.UseVirtualHost(params.MinioCfg.UseVirtualHost.GetAsBool()),
		storage.Region(params.MinioCfg.Region.GetValue()),
		storage.RequestTimeout(params.MinioCfg.RequestTimeoutMs.GetAsInt64()),
		storage.CreateBucket(true))
}

func NewChunkManagerFactory(persistentStorage string, opts ...storage.Option) *ChunkManagerFactory {
	c := storage.NewDefaultConfig()
	for _, opt := range opts {
		opt(c)
	}
	return &ChunkManagerFactory{
		persistentStorage: persistentStorage,
		config:            c,
	}
}

func (f *ChunkManagerFactory) newChunkManager(ctx context.Context, engine string) (storage.ChunkManager, error) {
	switch engine {
	case "local":
		return storage.NewLocalChunkManager(storage.RootPath(f.config.RootPath)), nil
	case "minio":
		return storage.NewMinioChunkManagerWithConfig(ctx, f.config)
	case "remote":
		return storage.NewRemoteChunkManager(ctx, f.config)
	default:
		return nil, errors.New("no chunk manager implemented with engine: " + engine)
	}
}

func (f *ChunkManagerFactory) NewPersistentStorageChunkManager(ctx context.Context) (storage.ChunkManager, error) {
	return f.newChunkManager(ctx, f.persistentStorage)
}

type Factory interface {
	NewPersistentStorageChunkManager(ctx context.Context) (storage.ChunkManager, error)
}
