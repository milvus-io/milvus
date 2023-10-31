package indexnode

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/storage"
	pkgStorage "github.com/milvus-io/milvus/pkg/storage"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type StorageFactory interface {
	NewChunkManager(ctx context.Context, config *indexpb.StorageConfig) (pkgStorage.ChunkManager, error)
}

type chunkMgrFactory struct {
	cached *typeutil.ConcurrentMap[string, pkgStorage.ChunkManager]
}

func NewChunkMgrFactory() *chunkMgrFactory {
	return &chunkMgrFactory{
		cached: typeutil.NewConcurrentMap[string, pkgStorage.ChunkManager](),
	}
}

func (m *chunkMgrFactory) NewChunkManager(ctx context.Context, config *indexpb.StorageConfig) (pkgStorage.ChunkManager, error) {
	chunkManagerFactory := storage.NewChunkManagerFactory(config.GetStorageType(),
		pkgStorage.RootPath(config.GetRootPath()),
		pkgStorage.Address(config.GetAddress()),
		pkgStorage.AccessKeyID(config.GetAccessKeyID()),
		pkgStorage.SecretAccessKeyID(config.GetSecretAccessKey()),
		pkgStorage.UseSSL(config.GetUseSSL()),
		pkgStorage.BucketName(config.GetBucketName()),
		pkgStorage.UseIAM(config.GetUseIAM()),
		pkgStorage.CloudProvider(config.GetCloudProvider()),
		pkgStorage.IAMEndpoint(config.GetIAMEndpoint()),
		pkgStorage.UseVirtualHost(config.GetUseVirtualHost()),
		pkgStorage.RequestTimeout(config.GetRequestTimeoutMs()),
		pkgStorage.Region(config.GetRegion()),
		pkgStorage.CreateBucket(true),
	)
	return chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
}

func (m *chunkMgrFactory) cacheKey(storageType, bucket, address string) string {
	return fmt.Sprintf("%s/%s/%s", storageType, bucket, address)
}
