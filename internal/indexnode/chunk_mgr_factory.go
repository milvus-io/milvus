package indexnode

import (
	"context"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/storage"
)

type StorageFactory interface {
	NewChunkManager(ctx context.Context, config *indexpb.StorageConfig) (storage.ChunkManager, error)
}

type chunkMgr struct {
	cached sync.Map
}

func (m *chunkMgr) NewChunkManager(ctx context.Context, config *indexpb.StorageConfig) (storage.ChunkManager, error) {
	key := m.cacheKey(config.StorageType, config.BucketName, config.Address)
	if v, ok := m.cached.Load(key); ok {
		return v.(storage.ChunkManager), nil
	}

	opts := make([]storage.Option, 0)
	if config.StorageType == "local" {
		opts = append(opts, storage.RootPath(config.RootPath))
	} else {
		opts = append(opts, storage.Address(config.Address),
			storage.AccessKeyID(config.AccessKeyID),
			storage.SecretAccessKeyID(config.SecretAccessKey),
			storage.UseSSL(config.UseSSL),
			storage.BucketName(config.BucketName),
			storage.UseIAM(config.UseIAM),
			storage.IAMEndpoint(config.IAMEndpoint))
	}
	factory := storage.NewChunkManagerFactory("local", config.StorageType, opts...)
	mgr, err := factory.NewVectorStorageChunkManager(ctx)
	if err != nil {
		return nil, err
	}
	v, _ := m.cached.LoadOrStore(key, mgr)
	return v.(storage.ChunkManager), nil
}

func (m *chunkMgr) cacheKey(storageType, bucket, address string) string {
	return fmt.Sprintf("%s/%s/%s", storageType, bucket, address)
}
