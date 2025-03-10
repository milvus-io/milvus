package indexnode

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
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
		objectstorage.RootPath(config.GetRootPath()),
		objectstorage.Address(config.GetAddress()),
		objectstorage.AccessKeyID(config.GetAccessKeyID()),
		objectstorage.SecretAccessKeyID(config.GetSecretAccessKey()),
		objectstorage.UseSSL(config.GetUseSSL()),
		objectstorage.SslCACert(config.GetSslCACert()),
		objectstorage.BucketName(config.GetBucketName()),
		objectstorage.UseIAM(config.GetUseIAM()),
		objectstorage.CloudProvider(config.GetCloudProvider()),
		objectstorage.IAMEndpoint(config.GetIAMEndpoint()),
		objectstorage.UseVirtualHost(config.GetUseVirtualHost()),
		objectstorage.RequestTimeout(config.GetRequestTimeoutMs()),
		objectstorage.Region(config.GetRegion()),
		objectstorage.CreateBucket(true),
		objectstorage.GcpCredentialJSON(config.GetGcpCredentialJSON()),
	)
	return chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
}

func (m *chunkMgrFactory) cacheKey(storageType, bucket, address string) string {
	return fmt.Sprintf("%s/%s/%s", storageType, bucket, address)
}
