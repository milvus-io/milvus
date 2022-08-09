package indexnode

import (
	"context"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/storage"
)

type StorageFactory interface {
	NewChunkManager(ctx context.Context, bucket, storageAccessKey string) (storage.ChunkManager, error)
}

type chunkMgr struct {
	cached sync.Map
}

func (m *chunkMgr) NewChunkManager(ctx context.Context, bucket, storageAccessKey string) (storage.ChunkManager, error) {
	key := m.cacheKey(bucket, storageAccessKey)
	if v, ok := m.cached.Load(key); ok {
		return v.(storage.ChunkManager), nil
	}
	opts := []storage.Option{
		storage.AccessKeyID(storageAccessKey),
		storage.BucketName(bucket),
	}
	factory := storage.NewChunkManagerFactory("local", "minio", opts...)
	mgr, err := factory.NewVectorStorageChunkManager(ctx)
	if err != nil {
		return nil, err
	}
	v, _ := m.cached.LoadOrStore(key, mgr)
	return v.(storage.ChunkManager), nil
}

func (m *chunkMgr) cacheKey(bucket, storageAccessKey string) string {
	return fmt.Sprintf("%s/%s", bucket, storageAccessKey)
}
