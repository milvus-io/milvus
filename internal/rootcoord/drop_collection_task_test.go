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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	pb "github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func Test_dropCollectionTask_Prepare(t *testing.T) {
	t.Run("drop via alias", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()

		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("IsAlias",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(true)

		core := newTestCore(withMeta(meta))
		task := &dropCollectionTask{
			Core: core,
			Req: &milvuspb.DropCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: collectionName,
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("collection not found", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().IsAlias(mock.Anything, mock.Anything, mock.Anything).Return(false)
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound)
		core := newTestCore(withMeta(meta))
		task := &dropCollectionTask{
			Core: core,
			Req: &milvuspb.DropCollectionRequest{
				CollectionName: collectionName,
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("collection has aliases", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().IsAlias(mock.Anything, mock.Anything, mock.Anything).Return(false)
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{
			CollectionID:        1,
			DBID:                1,
			State:               pb.CollectionState_CollectionCreated,
			VirtualChannelNames: []string{"vchannel1"},
		}, nil)
		meta.EXPECT().ListAliasesByID(mock.Anything, mock.Anything).Return([]string{"alias1"})
		core := newTestCore(withMeta(meta))
		task := &dropCollectionTask{
			Core: core,
			Req: &milvuspb.DropCollectionRequest{
				CollectionName: collectionName,
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()

		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("IsAlias",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(false)
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{
			CollectionID:        1,
			DBName:              "db1",
			DBID:                1,
			State:               pb.CollectionState_CollectionCreated,
			VirtualChannelNames: []string{"vchannel1"},
		}, nil)
		meta.EXPECT().ListAliasesByID(mock.Anything, mock.Anything).Return([]string{})

		core := newTestCore(withMeta(meta))
		task := &dropCollectionTask{
			Core: core,
			Req: &milvuspb.DropCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: collectionName,
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, int64(1), task.header.CollectionId)
		assert.Equal(t, int64(1), task.header.DbId)
		assert.Equal(t, collectionName, task.body.CollectionName)
		assert.Equal(t, "db1", task.body.DbName)
		assert.Equal(t, []string{"vchannel1"}, task.vchannels)
	})
}
