package storage

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type ChunkManagerFactory struct {
	persistentStorage string
	config            *config
}

func NewChunkManagerFactoryWithParam(params *paramtable.ComponentParam) *ChunkManagerFactory {
	if params.CommonCfg.StorageType.GetValue() == "local" {
		return NewChunkManagerFactory("local", RootPath(params.LocalStorageCfg.Path.GetValue()))
	}
	return NewChunkManagerFactory(params.CommonCfg.StorageType.GetValue(),
		RootPath(params.MinioCfg.RootPath.GetValue()),
		Address(params.MinioCfg.Address.GetValue()),
		AccessKeyID(params.MinioCfg.AccessKeyID.GetValue()),
		SecretAccessKeyID(params.MinioCfg.SecretAccessKey.GetValue()),
		UseSSL(params.MinioCfg.UseSSL.GetAsBool()),
		SslCACert(params.MinioCfg.SslCACert.GetValue()),
		BucketName(params.MinioCfg.BucketName.GetValue()),
		UseIAM(params.MinioCfg.UseIAM.GetAsBool()),
		CloudProvider(params.MinioCfg.CloudProvider.GetValue()),
		IAMEndpoint(params.MinioCfg.IAMEndpoint.GetValue()),
		UseVirtualHost(params.MinioCfg.UseVirtualHost.GetAsBool()),
		Region(params.MinioCfg.Region.GetValue()),
		RequestTimeout(params.MinioCfg.RequestTimeoutMs.GetAsInt64()),
		CreateBucket(true),
		GcpCredentialJSON(params.MinioCfg.GcpCredentialJSON.GetValue()))
}

func NewChunkManagerFactory(persistentStorage string, opts ...Option) *ChunkManagerFactory {
	c := newDefaultConfig()
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
		return NewLocalChunkManager(RootPath(f.config.rootPath)), nil
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
