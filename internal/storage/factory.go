package storage

import (
	"context"
	"errors"
)

type ChunkManagerFactory struct {
	cacheStorage  string
	vectorStorage string
	config        *config
}

func NewChunkManagerFactory(cacheStorage, vectorStorage string, opts ...Option) *ChunkManagerFactory {
	c := newDefaultConfig()
	for _, opt := range opts {
		opt(c)
	}
	return &ChunkManagerFactory{
		cacheStorage:  cacheStorage,
		vectorStorage: vectorStorage,
		config:        c,
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

func (f *ChunkManagerFactory) NewCacheStorageChunkManager(ctx context.Context) (ChunkManager, error) {
	return f.newChunkManager(ctx, f.cacheStorage)
}

func (f *ChunkManagerFactory) NewVectorStorageChunkManager(ctx context.Context) (ChunkManager, error) {
	return f.newChunkManager(ctx, f.vectorStorage)
}

type Factory interface {
	NewCacheStorageChunkManager(ctx context.Context) (ChunkManager, error)
	NewVectorStorageChunkManager(ctx context.Context) (ChunkManager, error)
}
