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

package dependency

import (
	"context"

	"github.com/milvus-io/milvus/internal/mq"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/paramtable"
)

// MixedFactory contains a msgstream factory and a chunkmanager factory.
//
// Only use a MixedFactory if you need both factories. For any packages using
//  MixedFactory would depend on `storage` package, which is a CGO package.
// Otherwise just import the factory you need from its own package.
type MixedFactory interface {
	mq.Factory
	storage.ChunkManagerFactory
}

type DefaultFactory struct {
	standAlone          bool
	chunkManagerFactory storage.ChunkManagerFactory
	msgStreamFactory    mq.Factory
}

func NewDefaultFactory(standAlone bool) MixedFactory {
	return &DefaultFactory{
		standAlone:          standAlone,
		msgStreamFactory:    mq.NewDefaultFactory(standAlone),
		chunkManagerFactory: storage.NewDefaultChunkManagerFactory(),
	}
}

func NewFactory(standAlone bool) MixedFactory {
	return &DefaultFactory{
		standAlone:          standAlone,
		msgStreamFactory:    mq.NewFactory(standAlone),
		chunkManagerFactory: storage.NewChunkManagerFactory(),
	}
}

// Init create a msg factory and a chunkmanager factory
func (f *DefaultFactory) Init(params *paramtable.ComponentParam) {
	f.msgStreamFactory.Init(params)
	f.chunkManagerFactory.Init(params)
}

func (f *DefaultFactory) NewMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return f.msgStreamFactory.NewMsgStream(ctx)
}

func (f *DefaultFactory) NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return f.msgStreamFactory.NewTtMsgStream(ctx)
}

func (f *DefaultFactory) NewQueryMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return f.msgStreamFactory.NewQueryMsgStream(ctx)
}

func (f *DefaultFactory) NewMsgStreamDisposer(ctx context.Context) func([]string, string) error {
	return f.msgStreamFactory.NewMsgStreamDisposer(ctx)
}

func (f *DefaultFactory) NewCacheStorageChunkManager(ctx context.Context) (storage.ChunkManager, error) {
	return f.chunkManagerFactory.NewCacheStorageChunkManager(ctx)
}

func (f *DefaultFactory) NewVectorStorageChunkManager(ctx context.Context) (storage.ChunkManager, error) {
	return f.chunkManagerFactory.NewVectorStorageChunkManager(ctx)
}
