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

package writer

import (
	"context"

	"github.com/milvus-io/milvus/cdc/core/util"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
)

//go:generate mockery --name=MilvusClientApi --filename=milvus_client_api_mock.go --output=../mocks
type MilvusClientApi interface {
	CreateCollection(ctx context.Context, schema *entity.Schema, shardsNum int32, opts ...client.CreateCollectionOption) error
	DropCollection(ctx context.Context, collName string) error
	Insert(ctx context.Context, collName string, partitionName string, columns ...entity.Column) (entity.Column, error)
	DeleteByPks(ctx context.Context, collName string, partitionName string, ids entity.Column) error
}

//go:generate mockery --name=MilvusClientFactory --filename=milvus_client_factory_mock.go --output=../mocks
type MilvusClientFactory interface {
	util.CDCMark
	NewGrpcClientWithTLSAuth(ctx context.Context, addr, username, password string) (MilvusClientApi, error)
	NewGrpcClientWithAuth(ctx context.Context, addr, username, password string) (MilvusClientApi, error)
	NewGrpcClient(ctx context.Context, addr string) (MilvusClientApi, error)
}

type DefaultMilvusClientFactory struct {
	util.CDCMark
}

func NewDefaultMilvusClientFactory() MilvusClientFactory {
	return &DefaultMilvusClientFactory{}
}

func (d *DefaultMilvusClientFactory) NewGrpcClientWithTLSAuth(ctx context.Context, addr, username, password string) (MilvusClientApi, error) {
	return client.NewDefaultGrpcClientWithTLSAuth(ctx, addr, username, password)
}

func (d *DefaultMilvusClientFactory) NewGrpcClientWithAuth(ctx context.Context, addr, username, password string) (MilvusClientApi, error) {
	return client.NewDefaultGrpcClientWithAuth(ctx, addr, username, password)
}

func (d *DefaultMilvusClientFactory) NewGrpcClient(ctx context.Context, addr string) (MilvusClientApi, error) {
	return client.NewDefaultGrpcClient(ctx, addr)
}
