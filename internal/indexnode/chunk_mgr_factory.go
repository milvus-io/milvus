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
	//key := m.cacheKey(bucket, storageAccessKey)
	//if v, ok := m.cached.Load(key); ok {
	//	return v.(storage.ChunkManager), nil
	//}
	opts := []storage.Option{
		storage.Address(config.Address),
		storage.AccessKeyID(config.AccessKeyID),
		storage.SecretAccessKeyID(config.SecretAccessKey),
		storage.UseSSL(config.UseSSL),
		storage.BucketName(config.BucketName),
		storage.UseIAM(config.UseIAM),
		storage.IAMEndpoint(config.IAMEndpoint),
	}
	factory := storage.NewChunkManagerFactory("local", "minio", opts...)
	mgr, err := factory.NewVectorStorageChunkManager(ctx)
	if err != nil {
		return nil, err
	}
	//v, _ := m.cached.LoadOrStore(key, mgr)
	return mgr, nil
}

func (m *chunkMgr) cacheKey(bucket, storageAccessKey string) string {
	return fmt.Sprintf("%s/%s", bucket, storageAccessKey)
}
