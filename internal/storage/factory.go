package storage

import (
	"context"
	"errors"

	"github.com/milvus-io/milvus/internal/util/paramtable"
)

type ChunkManagerFactory struct {
	persistentStorage string
	config            *config
}

func NewChunkManagerFactoryWithParam(params *paramtable.ComponentParam) *ChunkManagerFactory {
	if params.CommonCfg.StorageType == "local" {
		return NewChunkManagerFactory("local", RootPath(params.LocalStorageCfg.Path))
	}
	return NewChunkManagerFactory("minio",
		RootPath(params.MinioCfg.RootPath),
		Address(params.MinioCfg.Address),
		AccessKeyID(params.MinioCfg.AccessKeyID),
		SecretAccessKeyID(params.MinioCfg.SecretAccessKey),
		UseSSL(params.MinioCfg.UseSSL),
		BucketName(params.MinioCfg.BucketName),
		UseIAM(params.MinioCfg.UseIAM),
		IAMEndpoint(params.MinioCfg.IAMEndpoint),
		CreateBucket(true))
}

func NewChunkManagerFactory(persistentStorage string, opts ...Option) *ChunkManagerFactory {
	c := newDefaultConfig()
	for _, opt := range opts {
		opt(c)
	}
	return &ChunkManagerFactory{
		persistentStorage: persistentStorage,
		config:            c,
	}
}

func (f *ChunkManagerFactory) newChunkManager(ctx context.Context, engine string) (ChunkManager, error) {
	switch engine {
	case "local":
		return NewLocalChunkManager(RootPath(f.config.rootPath)), nil
	case "minio":
		return newMinioChunkManagerWithConfig(ctx, f.config)
	default:
		return nil, errors.New("no chunk manager implemented with engine: " + engine)
	}
}

func (f *ChunkManagerFactory) NewPersistentStorageChunkManager(ctx context.Context) (ChunkManager, error) {
	return f.newChunkManager(ctx, f.persistentStorage)
}

type Factory interface {
	NewPersistentStorageChunkManager(ctx context.Context) (ChunkManager, error)
}
