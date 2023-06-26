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
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
)

func Test_dropCollectionTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &dropCollectionTask{
			Req: &milvuspb.DropCollectionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DescribeCollection},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("drop via alias", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()

		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("IsAlias",
			mock.Anything,
			mock.Anything,
		).Return(true)

		core := newTestCore(withMeta(meta))
		task := &dropCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.DropCollectionRequest{
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
		meta.On("IsAlias",
			mock.Anything,
			mock.Anything,
		).Return(false)

		core := newTestCore(withMeta(meta))
		task := &dropCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.DropCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: collectionName,
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_dropCollectionTask_Execute(t *testing.T) {
	t.Run("drop non-existent collection", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetCollectionByName",
			mock.Anything, // context.Context.
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil, func(ctx context.Context, dbName string, name string, ts Timestamp) error {
			if collectionName == name {
				return common.NewCollectionNotExistError("collection not exist")
			}
			return errors.New("error mock GetCollectionByName")
		})
		core := newTestCore(withMeta(meta))
		task := &dropCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.DropCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: collectionName,
			},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
		task.Req.CollectionName = collectionName + "_test"
		err = task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to expire cache", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName}

		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetCollectionByName",
			mock.Anything, // context.Context
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(coll.Clone(), nil)
		meta.On("ListAliasesByID",
			mock.AnythingOfType("int64"),
		).Return([]string{})

		core := newTestCore(withInvalidProxyManager(), withMeta(meta))
		task := &dropCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.DropCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: collectionName,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to change collection state", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName}

		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(coll.Clone(), nil)
		meta.On("ChangeCollectionState",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("error mock ChangeCollectionState"))
		meta.On("ListAliasesByID",
			mock.Anything,
		).Return([]string{})

		core := newTestCore(withValidProxyManager(), withMeta(meta))
		task := &dropCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.DropCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: collectionName,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case, redo", func(t *testing.T) {
		defer cleanTestEnv()

		confirmGCInterval = time.Millisecond
		defer restoreConfirmGCInterval()

		collectionName := funcutil.GenRandomStr()
		shardNum := 2

		ticker := newRocksMqTtSynchronizer()
		pchans := ticker.getDmlChannelNames(shardNum)
		ticker.addDmlChannels(pchans...)

		coll := &model.Collection{Name: collectionName, ShardsNum: int32(shardNum), PhysicalChannelNames: pchans}

		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(coll.Clone(), nil)
		meta.On("ChangeCollectionState",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		meta.On("ListAliasesByID",
			mock.Anything,
		).Return([]string{})
		removeCollectionMetaCalled := false
		removeCollectionMetaChan := make(chan struct{}, 1)
		meta.On("RemoveCollection",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(func(ctx context.Context, collID UniqueID, ts Timestamp) error {
			removeCollectionMetaCalled = true
			removeCollectionMetaChan <- struct{}{}
			return nil
		})

		broker := newMockBroker()
		releaseCollectionCalled := false
		releaseCollectionChan := make(chan struct{}, 1)
		broker.ReleaseCollectionFunc = func(ctx context.Context, collectionID UniqueID) error {
			releaseCollectionCalled = true
			releaseCollectionChan <- struct{}{}
			return nil
		}
		dropIndexCalled := false
		dropIndexChan := make(chan struct{}, 1)
		broker.DropCollectionIndexFunc = func(ctx context.Context, collID UniqueID, partIDs []UniqueID) error {
			dropIndexCalled = true
			dropIndexChan <- struct{}{}
			time.Sleep(confirmGCInterval)
			return nil
		}
		broker.GCConfirmFunc = func(ctx context.Context, collectionID, partitionID UniqueID) bool {
			return true
		}

		gc := newMockGarbageCollector()
		deleteCollectionCalled := false
		deleteCollectionChan := make(chan struct{}, 1)
		gc.GcCollectionDataFunc = func(ctx context.Context, coll *model.Collection) (Timestamp, error) {
			deleteCollectionCalled = true
			deleteCollectionChan <- struct{}{}
			return 0, nil
		}

		core := newTestCore(
			withValidProxyManager(),
			withMeta(meta),
			withBroker(broker),
			withGarbageCollector(gc),
			withTtSynchronizer(ticker))

		task := &dropCollectionTask{
			baseTask: newBaseTask(context.Background(), core),
			Req: &milvuspb.DropCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: collectionName,
			},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)

		// check if redo worked.

		<-releaseCollectionChan
		assert.True(t, releaseCollectionCalled)

		<-dropIndexChan
		assert.True(t, dropIndexCalled)

		<-deleteCollectionChan
		assert.True(t, deleteCollectionCalled)

		<-removeCollectionMetaChan
		assert.True(t, removeCollectionMetaCalled)
	})
}
