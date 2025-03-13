// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datanode

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
