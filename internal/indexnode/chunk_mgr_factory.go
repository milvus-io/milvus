package indexnode

import (
	"context"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
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

	chunkManagerFactory := storage.NewChunkManagerFactoryWithParam(&Params)
	mgr, err := chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
	if err != nil {
		return nil, err
	}
	v, _ := m.cached.LoadOrStore(key, mgr)
	log.Ctx(ctx).Info("index node successfully init chunk manager")
	return v.(storage.ChunkManager), nil
}

func (m *chunkMgr) cacheKey(storageType, bucket, address string) string {
	return fmt.Sprintf("%s/%s/%s", storageType, bucket, address)
}
