package indexnode

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
	chunkManagerFactory := storage.NewChunkManagerFactory(config.GetStorageType(),
		storage.RootPath(config.GetRootPath()),
		storage.Address(config.GetAddress()),
		storage.AccessKeyID(config.GetAccessKeyID()),
		storage.SecretAccessKeyID(config.GetSecretAccessKey()),
		storage.UseSSL(config.GetUseSSL()),
		storage.SslCACert(config.GetSslCACert()),
		storage.BucketName(config.GetBucketName()),
		storage.UseIAM(config.GetUseIAM()),
		storage.CloudProvider(config.GetCloudProvider()),
		storage.IAMEndpoint(config.GetIAMEndpoint()),
		storage.UseVirtualHost(config.GetUseVirtualHost()),
		storage.RequestTimeout(config.GetRequestTimeoutMs()),
		storage.Region(config.GetRegion()),
		storage.CreateBucket(true),
		storage.GcpCredentialJSON(config.GetGcpCredentialJSON()),
	)
	return chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
}

func (m *chunkMgrFactory) cacheKey(storageType, bucket, address string) string {
	return fmt.Sprintf("%s/%s/%s", storageType, bucket, address)
}
