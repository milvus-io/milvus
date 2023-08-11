package indexnode

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type StorageFactory interface {
	NewChunkManager(ctx context.Context, config *indexpb.StorageConfig) (storage.ChunkManager, error)
}

type chunkMgrFactory struct {
	cached *typeutil.ConcurrentMap[string, storage.ChunkManager]
}

func NewChunkMgrFactory() *chunkMgrFactory {
	return &chunkMgrFactory{
		cached: typeutil.NewConcurrentMap[string, storage.ChunkManager](),
	}
}

func (m *chunkMgrFactory) NewChunkManager(ctx context.Context, config *indexpb.StorageConfig) (storage.ChunkManager, error) {
	key := m.cacheKey(config.GetStorageType(), config.GetBucketName(), config.GetAddress())
	if v, ok := m.cached.Get(key); ok {
		return v, nil
	}

	chunkManagerFactory := storage.NewChunkManagerFactoryWithParam(Params)
	mgr, err := chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
	if err != nil {
		return nil, err
	}
	v, _ := m.cached.GetOrInsert(key, mgr)
	log.Ctx(ctx).Info("index node successfully init chunk manager")
	return v, nil
}

func (m *chunkMgrFactory) cacheKey(storageType, bucket, address string) string {
	return fmt.Sprintf("%s/%s/%s", storageType, bucket, address)
}
