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

package writer_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/milvus-io/milvus/cdc/core/mocks"
	"github.com/milvus-io/milvus/cdc/core/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var (
	address         = "localhost:19530"
	user            = "foo"
	password        = "123456"
	addressOption   = writer.AddressOption(address)
	userOption      = writer.UserOption(user, password)
	tlsOption       = writer.TlsOption(true)
	timeoutOption   = writer.ConnectTimeoutOption(10)
	ignorePartition = writer.IgnorePartitionOption(true)
)

func TestNewMilvusDataHandler(t *testing.T) {
	_, err := writer.NewMilvusDataHandler()
	assert.Error(t, err)

	mockMilvusFactory := mocks.NewMilvusClientFactory(t)
	mockMilvusClient := mocks.NewMilvusClientApi(t)
	factoryOption := writer.MilvusFactoryOption(mockMilvusFactory)
	t.Run("success tls", func(t *testing.T) {
		call := mockMilvusFactory.On("NewGrpcClientWithTLSAuth", mock.Anything, address, user, password).Return(mockMilvusClient, nil)
		defer call.Unset()

		_, err := writer.NewMilvusDataHandler(addressOption, userOption, tlsOption, factoryOption)
		assert.NoError(t, err)
	})

	t.Run("success no tls", func(t *testing.T) {
		call := mockMilvusFactory.On("NewGrpcClientWithAuth", mock.Anything, address, user, password).Return(mockMilvusClient, nil)
		defer call.Unset()

		_, err := writer.NewMilvusDataHandler(addressOption, userOption, factoryOption)
		assert.NoError(t, err)
	})

	t.Run("success no user", func(t *testing.T) {
		call := mockMilvusFactory.On("NewGrpcClient", mock.Anything, address).Return(mockMilvusClient, nil)
		defer call.Unset()

		_, err := writer.NewMilvusDataHandler(addressOption, factoryOption)
		assert.NoError(t, err)
	})

	t.Run("fail", func(t *testing.T) {
		call := mockMilvusFactory.On("NewGrpcClient", mock.Anything, address).Return(nil, errors.New("client error"))
		defer call.Unset()

		_, err := writer.NewMilvusDataHandler(addressOption, factoryOption)
		assert.Error(t, err)
	})
}

func TestMilvusOp(t *testing.T) {
	mockMilvusFactory := mocks.NewMilvusClientFactory(t)
	mockMilvusClient := mocks.NewMilvusClientApi(t)
	factoryOption := writer.MilvusFactoryOption(mockMilvusFactory)
	call := mockMilvusFactory.On("NewGrpcClient", mock.Anything, address).Return(mockMilvusClient, nil)
	defer call.Unset()

	handler, err := writer.NewMilvusDataHandler(addressOption, ignorePartition, factoryOption)
	assert.NoError(t, err)

	t.Run("create collection", func(t *testing.T) {
		schema := &entity.Schema{}
		var shardNum int32 = 5
		level := commonpb.ConsistencyLevel_Session
		kv := &commonpb.KeyValuePair{Key: "foo", Value: "111"}
		param := &writer.CreateCollectionParam{
			Schema:           schema,
			ShardsNum:        shardNum,
			ConsistencyLevel: level,
			Properties:       []*commonpb.KeyValuePair{kv},
		}
		options := []client.CreateCollectionOption{
			client.WithCollectionProperty(kv.GetKey(), kv.GetValue()),
			client.WithConsistencyLevel(entity.ConsistencyLevel(param.ConsistencyLevel)),
		}

		createCall := mockMilvusClient.On("CreateCollection", mock.Anything, schema, shardNum, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			assert.Len(t, args, 5)
			createRequest1 := &milvuspb.CreateCollectionRequest{}
			for _, option := range options {
				option(createRequest1)
			}
			createRequest2 := &milvuspb.CreateCollectionRequest{}
			for _, option := range args[3:] {
				option.(client.CreateCollectionOption)(createRequest2)
			}

			assert.EqualValues(t, createRequest1, createRequest2)
		}).Return(nil)
		err := handler.CreateCollection(context.Background(), param)
		assert.NoError(t, err)
		createCall.Unset()

		createCall = mockMilvusClient.On("CreateCollection", mock.Anything, schema, shardNum, mock.Anything, mock.Anything).Return(errors.New("crete error"))
		err = handler.CreateCollection(context.Background(), param)
		assert.Error(t, err)
		createCall.Unset()
	})

	t.Run("drop collection", func(t *testing.T) {
		name := "foo"
		param := &writer.DropCollectionParam{CollectionName: name}
		dropCall := mockMilvusClient.On("DropCollection", mock.Anything, name).Return(nil)
		err := handler.DropCollection(context.Background(), param)
		assert.NoError(t, err)
		dropCall.Unset()

		dropCall = mockMilvusClient.On("DropCollection", mock.Anything, name).Return(errors.New("drop error"))
		err = handler.DropCollection(context.Background(), param)
		assert.Error(t, err)
		dropCall.Unset()
	})

	t.Run("insert", func(t *testing.T) {
		collectionName := "col"
		partitionName := "par"
		column := entity.NewColumnBool("ok", []bool{true, false, true})
		param := &writer.InsertParam{
			CollectionName: collectionName,
			PartitionName:  partitionName,
			Columns:        []entity.Column{column},
		}
		insertCall := mockMilvusClient.On("Insert", mock.Anything, collectionName, "", column).Return(nil, nil)
		err := handler.Insert(context.Background(), param)
		assert.NoError(t, err)
		insertCall.Unset()

		insertCall = mockMilvusClient.On("Insert", mock.Anything, collectionName, "", column).Return(nil, errors.New("insert error"))
		err = handler.Insert(context.Background(), param)
		assert.Error(t, err)
		insertCall.Unset()

		handler2, err := writer.NewMilvusDataHandler(addressOption, factoryOption)
		assert.NoError(t, err)
		insertCall = mockMilvusClient.On("Insert", mock.Anything, collectionName, partitionName, column).Return(nil, nil)
		err = handler2.Insert(context.Background(), param)
		assert.NoError(t, err)
		insertCall.Unset()
	})

	t.Run("delete", func(t *testing.T) {
		collectionName := "col"
		partitionName := "par"
		column := entity.NewColumnString("ok", []string{"a", "b", "c"})
		param := &writer.DeleteParam{
			CollectionName: collectionName,
			PartitionName:  partitionName,
			Column:         column,
		}
		deleteCall := mockMilvusClient.On("DeleteByPks", mock.Anything, collectionName, "", column).Return(nil)
		err := handler.Delete(context.Background(), param)
		assert.NoError(t, err)
		deleteCall.Unset()

		deleteCall = mockMilvusClient.On("DeleteByPks", mock.Anything, collectionName, "", column).Return(errors.New("delete error"))
		err = handler.Delete(context.Background(), param)
		assert.Error(t, err)
		deleteCall.Unset()

		handler2, err := writer.NewMilvusDataHandler(addressOption, factoryOption)
		assert.NoError(t, err)
		deleteCall = mockMilvusClient.On("DeleteByPks", mock.Anything, collectionName, partitionName, column).Return(nil)
		err = handler2.Delete(context.Background(), param)
		assert.NoError(t, err)
		deleteCall.Unset()
	})
}
