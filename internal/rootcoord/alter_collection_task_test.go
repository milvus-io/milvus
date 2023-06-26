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

package rootcoord

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/common"
)

func Test_alterCollectionTask_Prepare(t *testing.T) {
	t.Run("invalid collectionID", func(t *testing.T) {
		task := &alterCollectionTask{Req: &milvuspb.AlterCollectionRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection}}}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		task := &alterCollectionTask{
			Req: &milvuspb.AlterCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection},
				CollectionName: "cn",
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_alterCollectionTask_Execute(t *testing.T) {
	properties := []*commonpb.KeyValuePair{
		{
			Key:   common.CollectionTTLConfigKey,
			Value: "3600",
		},
	}

	t.Run("properties is empty", func(t *testing.T) {
		task := &alterCollectionTask{Req: &milvuspb.AlterCollectionRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection}}}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to create alias", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &alterCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.AlterCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection},
				CollectionName: "cn",
				Properties:     properties,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("alter step failed", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(&model.Collection{CollectionID: int64(1)}, nil)
		meta.On("AlterCollection",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("err"))

		core := newTestCore(withMeta(meta))
		task := &alterCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.AlterCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection},
				CollectionName: "cn",
				Properties:     properties,
			},
		}

		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("broadcast step failed", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(&model.Collection{CollectionID: int64(1)}, nil)
		meta.On("AlterCollection",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)

		broker := newMockBroker()
		broker.BroadcastAlteredCollectionFunc = func(ctx context.Context, req *milvuspb.AlterCollectionRequest) error {
			return errors.New("err")
		}

		core := newTestCore(withValidProxyManager(), withMeta(meta), withBroker(broker))
		task := &alterCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.AlterCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection},
				CollectionName: "cn",
				Properties:     properties,
			},
		}

		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("alter successfully", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(&model.Collection{CollectionID: int64(1)}, nil)
		meta.On("AlterCollection",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)

		broker := newMockBroker()
		broker.BroadcastAlteredCollectionFunc = func(ctx context.Context, req *milvuspb.AlterCollectionRequest) error {
			return nil
		}

		core := newTestCore(withValidProxyManager(), withMeta(meta), withBroker(broker))
		task := &alterCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.AlterCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection},
				CollectionName: "cn",
				Properties:     properties,
			},
		}

		err := task.Execute(context.Background())
		assert.NoError(t, err)
	})

	t.Run("test update collection props", func(t *testing.T) {
		coll := &model.Collection{
			Properties: []*commonpb.KeyValuePair{
				{
					Key:   common.CollectionTTLConfigKey,
					Value: "1",
				},
			},
		}

		updateProps1 := []*commonpb.KeyValuePair{
			{
				Key:   common.CollectionAutoCompactionKey,
				Value: "true",
			},
		}
		updateCollectionProperties(coll, updateProps1)

		assert.Contains(t, coll.Properties, &commonpb.KeyValuePair{
			Key:   common.CollectionTTLConfigKey,
			Value: "1",
		})

		assert.Contains(t, coll.Properties, &commonpb.KeyValuePair{
			Key:   common.CollectionAutoCompactionKey,
			Value: "true",
		})

		updateProps2 := []*commonpb.KeyValuePair{
			{
				Key:   common.CollectionTTLConfigKey,
				Value: "2",
			},
		}
		updateCollectionProperties(coll, updateProps2)

		assert.Contains(t, coll.Properties, &commonpb.KeyValuePair{
			Key:   common.CollectionTTLConfigKey,
			Value: "2",
		})

		assert.Contains(t, coll.Properties, &commonpb.KeyValuePair{
			Key:   common.CollectionAutoCompactionKey,
			Value: "true",
		})

	})
}
