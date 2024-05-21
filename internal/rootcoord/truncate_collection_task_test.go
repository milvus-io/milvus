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
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func Test_truncateCollectionTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &truncateCollectionTask{
			Req: &rootcoordpb.TruncateCollectionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DescribeCollection},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("drop via alias", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().IsAlias(mock.Anything, mock.Anything).Return(true)
		core := newTestCore(withMeta(meta))
		task := &truncateCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &rootcoordpb.TruncateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: collectionName,
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().IsAlias(mock.Anything, mock.Anything).Return(false)
		core := newTestCore(withMeta(meta))
		task := &truncateCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &rootcoordpb.TruncateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: collectionName,
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_truncateCollectionTask_Execute(t *testing.T) {
	t.Run("truncate non-existent collection", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Once()
		core := newTestCore(withMeta(meta))
		task := &truncateCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &rootcoordpb.TruncateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: collectionName,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to expire cache", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName}

		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(coll, nil).Once()
		meta.EXPECT().ListAliasesByID(mock.Anything).Return([]string{}).Once()
		core := newTestCore(withInvalidProxyManager(), withMeta(meta))
		task := &truncateCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &rootcoordpb.TruncateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: collectionName,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to replace collection with temp", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName}

		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(coll, nil).Once()
		meta.EXPECT().ListAliasesByID(mock.Anything).Return([]string{}).Once()
		meta.EXPECT().ReplaceCollectionWithTemp(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(0, 0, errors.New("error mock ChangeCollectionState"))
		core := newTestCore(withValidProxyManager(), withMeta(meta))
		task := &truncateCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &rootcoordpb.TruncateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: collectionName,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName}

		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(coll, nil).Once()
		meta.EXPECT().ListAliasesByID(mock.Anything).Return([]string{}).Once()
		meta.EXPECT().ReplaceCollectionWithTemp(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(0, 0, nil)
		core := newTestCore(withValidProxyManager(), withMeta(meta))
		task := &truncateCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &rootcoordpb.TruncateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: collectionName,
			},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
	})
}
