package storage

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type ChunkManagerFactory struct {
	persistentStorage string
	config            *objectstorage.Config
}

func NewChunkManagerFactoryWithParam(params *paramtable.ComponentParam) *ChunkManagerFactory {
	if params.CommonCfg.StorageType.GetValue() == "local" {
		return NewChunkManagerFactory("local", objectstorage.RootPath(params.LocalStorageCfg.Path.GetValue()))
	}
	return NewChunkManagerFactory(params.CommonCfg.StorageType.GetValue(),
		objectstorage.RootPath(params.MinioCfg.RootPath.GetValue()),
		objectstorage.Address(params.MinioCfg.Address.GetValue()),
		objectstorage.AccessKeyID(params.MinioCfg.AccessKeyID.GetValue()),
		objectstorage.SecretAccessKeyID(params.MinioCfg.SecretAccessKey.GetValue()),
		objectstorage.UseSSL(params.MinioCfg.UseSSL.GetAsBool()),
		objectstorage.SslCACert(params.MinioCfg.SslCACert.GetValue()),
		objectstorage.BucketName(params.MinioCfg.BucketName.GetValue()),
		objectstorage.UseIAM(params.MinioCfg.UseIAM.GetAsBool()),
		objectstorage.CloudProvider(params.MinioCfg.CloudProvider.GetValue()),
		objectstorage.IAMEndpoint(params.MinioCfg.IAMEndpoint.GetValue()),
		objectstorage.UseVirtualHost(params.MinioCfg.UseVirtualHost.GetAsBool()),
		objectstorage.Region(params.MinioCfg.Region.GetValue()),
		objectstorage.RequestTimeout(params.MinioCfg.RequestTimeoutMs.GetAsInt64()),
		objectstorage.CreateBucket(true),
		objectstorage.GcpCredentialJSON(params.MinioCfg.GcpCredentialJSON.GetValue()))
}

func NewChunkManagerFactory(persistentStorage string, opts ...objectstorage.Option) *ChunkManagerFactory {
	c := objectstorage.NewDefaultConfig()
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
		return NewLocalChunkManager(objectstorage.RootPath(f.config.RootPath)), nil
	case "remote", "minio", "opendal":
		return NewRemoteChunkManager(ctx, f.config)
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
