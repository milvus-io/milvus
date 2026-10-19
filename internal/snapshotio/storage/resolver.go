// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"

	milvusstorage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	objectstore "github.com/milvus-io/milvus/pkg/v3/objectstorage/store"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	shared "github.com/milvus-io/milvus/pkg/v3/snapshotio"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func InstanceConfigFromParamtable(params *paramtable.ComponentParam) *objectstorage.Config {
	cfg := objectstorage.NewDefaultConfig()
	cfg.ReadRetryAttempts = params.CommonCfg.StorageReadRetryAttempts.GetAsUint()
	if params.CommonCfg.StorageType.GetValue() == "local" {
		cfg.RootPath = params.LocalStorageCfg.Path.GetValue()
		return cfg
	}

	cfg.Address = params.MinioCfg.Address.GetValue()
	cfg.BucketName = params.MinioCfg.BucketName.GetValue()
	cfg.AccessKeyID = params.MinioCfg.AccessKeyID.GetValue()
	cfg.SecretAccessKeyID = params.MinioCfg.SecretAccessKey.GetValue()
	cfg.UseSSL = params.MinioCfg.UseSSL.GetAsBool()
	cfg.SslCACert = params.MinioCfg.SslCACert.GetValue()
	cfg.SslTLSMinVersion = params.MinioCfg.SslTLSMinVersion.GetValue()
	cfg.CreateBucket = true
	cfg.RootPath = params.MinioCfg.RootPath.GetValue()
	cfg.UseIAM = params.MinioCfg.UseIAM.GetAsBool()
	cfg.CloudProvider = params.MinioCfg.CloudProvider.GetValue()
	cfg.IAMEndpoint = params.MinioCfg.IAMEndpoint.GetValue()
	cfg.UseVirtualHost = params.MinioCfg.UseVirtualHost.GetAsBool()
	cfg.Region = params.MinioCfg.Region.GetValue()
	cfg.RequestTimeoutMs = params.MinioCfg.RequestTimeoutMs.GetAsInt64()
	cfg.GcpCredentialJSON = params.MinioCfg.GcpCredentialJSON.GetValue()
	return cfg
}

func ResolveForeignStorage(ctx context.Context, cfg *objectstorage.Config, direction shared.Direction, uri, spec string) (*shared.ResolvedForeignStorage, error) {
	r, err := shared.ResolveForeignStorageWithFactory(ctx, cfg, direction, uri, spec, func(ctx context.Context, cfg *objectstorage.Config) (shared.ChunkManager, objectstore.ObjectStore, error) {
		cm, err := milvusstorage.NewRemoteChunkManager(ctx, cfg)
		if err != nil {
			return nil, nil, err
		}
		return cm, milvusstorage.NewObjectStore(cm), nil
	}, paramtable.Get().DataCoordCfg.SnapshotCrossBucketEndpointAllowlist.GetValue())
	if err != nil {
		return nil, err
	}
	r.ForeignStorageConfig.UseCrc32CChecksum = paramtable.Get().MinioCfg.UseCRC32C.GetAsBool()
	return r, nil
}
func StorageConfigFromInstance(params *paramtable.ComponentParam) *indexpb.StorageConfig {
	cfg := shared.StorageConfigFromObjectConfig(InstanceConfigFromParamtable(params), params.CommonCfg.StorageType.GetValue())
	cfg.UseCrc32CChecksum = params.MinioCfg.UseCRC32C.GetAsBool()
	return cfg
}
func ValidateForeignStorageRequest(cfg *objectstorage.Config, direction shared.Direction, uri, spec string) error {
	return shared.ValidateForeignStorageRequest(cfg, direction, uri, spec, paramtable.Get().DataCoordCfg.SnapshotCrossBucketEndpointAllowlist.GetValue())
}

// Keep server retry defaults at the adapter boundary rather than in the library.
func ObjectStorageConfigFromIndexConfig(config *indexpb.StorageConfig) *objectstorage.Config {
	cfg := shared.ObjectStorageConfigFromIndexConfig(config)
	cfg.ReadRetryAttempts = paramtable.Get().CommonCfg.StorageReadRetryAttempts.GetAsUint()
	return cfg
}

func ValidateInstanceSnapshotImportURI(cfg *objectstorage.Config, uri string) error {
	return shared.ValidateInstanceSnapshotImportURI(cfg, uri, paramtable.Get().DataCoordCfg.SnapshotCrossBucketEndpointAllowlist.GetValue())
}

func ResolveSnapshotReadStorage(ctx context.Context, cfg *objectstorage.Config, uri, spec string) (*shared.ResolvedForeignStorage, error) {
	resolved, err := shared.ResolveSnapshotReadStorageWithFactory(ctx, cfg, uri, spec, func(ctx context.Context, cfg *objectstorage.Config) (shared.ChunkManager, error) {
		return milvusstorage.NewRemoteChunkManager(ctx, cfg)
	}, paramtable.Get().DataCoordCfg.SnapshotCrossBucketEndpointAllowlist.GetValue())
	if err != nil {
		return nil, err
	}
	resolved.ForeignStorageConfig.UseCrc32CChecksum = paramtable.Get().MinioCfg.UseCRC32C.GetAsBool()
	return resolved, nil
}
