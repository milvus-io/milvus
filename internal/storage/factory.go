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

package storage

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/paramtable"
)

type ChunkManagerFactory interface {
	Init(p *paramtable.ComponentParam)
	NewCacheStorageChunkManager(ctx context.Context) (ChunkManager, error)
	NewVectorStorageChunkManager(ctx context.Context) (ChunkManager, error)
}

type DefaultChunkManagerFactory struct {
	factory *chunkManagerFactory
}

func NewDefaultChunkManagerFactory() *DefaultChunkManagerFactory {
	return &DefaultChunkManagerFactory{factory: newChunkManagerFactory("local", "local", RootPath("/tmp/milvus"))}
}

func NewChunkManagerFactoryWithConfig(cacheStorage, vectorStorage string, opts ...Option) *DefaultChunkManagerFactory {
	c := newDefaultConfig()
	for _, opt := range opts {
		opt(c)
	}
	f := &chunkManagerFactory{
		cacheStorage:  cacheStorage,
		vectorStorage: vectorStorage,
		config:        c,
	}

	return &DefaultChunkManagerFactory{factory: f}
}

func NewChunkManagerFactory() *DefaultChunkManagerFactory {
	return &DefaultChunkManagerFactory{}
}

func (f *DefaultChunkManagerFactory) Init(params *paramtable.ComponentParam) {
	if params.CommonCfg.StorageType == "local" {
		f.factory = newChunkManagerFactory("local", "local",
			RootPath(params.LocalStorageCfg.Path))
	} else {
		f.factory = newChunkManagerFactory("local", "minio",
			RootPath(params.LocalStorageCfg.Path),
			Address(params.MinioCfg.Address),
			AccessKeyID(params.MinioCfg.AccessKeyID),
			SecretAccessKeyID(params.MinioCfg.SecretAccessKey),
			UseSSL(params.MinioCfg.UseSSL),
			BucketName(params.MinioCfg.BucketName),
			UseIAM(params.MinioCfg.UseIAM),
			IAMEndpoint(params.MinioCfg.IAMEndpoint),
			CreateBucket(true))
	}
}

func (f *DefaultChunkManagerFactory) NewCacheStorageChunkManager(ctx context.Context) (ChunkManager, error) {
	return f.factory.newCacheStorageChunkManager(ctx)
}

func (f *DefaultChunkManagerFactory) NewVectorStorageChunkManager(ctx context.Context) (ChunkManager, error) {
	if f.factory.vectorStorage == "local" {
		return f.factory.newCacheStorageChunkManager(ctx)
	}
	return f.factory.newVectorStorageChunkManager(ctx)
}

type chunkManagerFactory struct {
	cacheStorage  string
	vectorStorage string
	config        *config
}

func newChunkManagerFactory(cacheStorage, vectorStorage string, opts ...Option) *chunkManagerFactory {
	c := newDefaultConfig()
	for _, opt := range opts {
		opt(c)
	}
	return &chunkManagerFactory{
		cacheStorage:  cacheStorage,
		vectorStorage: vectorStorage,
		config:        c,
	}
}

func (f *chunkManagerFactory) newCacheStorageChunkManager(ctx context.Context) (ChunkManager, error) {
	return NewLocalChunkManager(RootPath(f.config.rootPath)), nil
}

func (f *chunkManagerFactory) newVectorStorageChunkManager(ctx context.Context) (ChunkManager, error) {
	return newMinioChunkManagerWithConfig(ctx, f.config)
}
