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

package importutilv2

import (
	"context"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// ResolveSourceStorage selects storage for reading import files only. The caller
// must retain the original chunk manager and config for writing segment data.
func ResolveSourceStorage(ctx context.Context, options Options, cm storage.ChunkManager,
	config *indexpb.StorageConfig,
) (storage.ChunkManager, *indexpb.StorageConfig, error) {
	source, err := GetSourceStorageConfig(options)
	if err != nil {
		return nil, nil, err
	}
	if source == nil {
		return cm, config, nil
	}
	if err := ValidateImportSourceOptions(options); err != nil {
		return nil, nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	factory := storage.NewChunkManagerFactory(source.GetStorageType(),
		objectstorage.Address(source.GetAddress()),
		objectstorage.BucketName(source.GetBucketName()),
		objectstorage.AccessKeyID(source.GetAccessKeyID()),
		objectstorage.SecretAccessKeyID(source.GetSecretAccessKey()),
		objectstorage.UseSSL(source.GetUseSSL()),
		objectstorage.CloudProvider(source.GetCloudProvider()),
		objectstorage.Region(source.GetRegion()),
		objectstorage.UseVirtualHost(source.GetUseVirtualHost()),
		objectstorage.CreateBucket(false),
		// Object reads establish access. Do not require ListBucket permission
		// or probe/create a bucket while constructing a read-only source client.
		func(c *objectstorage.Config) { c.SkipBucketCheck = true },
	)
	readerCM, err := factory.NewPersistentStorageChunkManager(ctx)
	if err != nil {
		return nil, nil, storage.ToMilvusIoError("external import source", err)
	}
	return readerCM, source, nil
}
