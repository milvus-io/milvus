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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
)

func Test_dropPartitionTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &dropPartitionTask{
			Req: &milvuspb.DropPartitionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("drop default partition", func(t *testing.T) {
		task := &dropPartitionTask{
			Req: &milvuspb.DropPartitionRequest{
				Base:          &commonpb.MsgBase{MsgType: commonpb.MsgType_DropPartition},
				PartitionName: Params.CommonCfg.DefaultPartitionName.GetValue(),
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to get collection meta", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &dropPartitionTask{
			baseTask: baseTask{core: core},
			Req: &milvuspb.DropPartitionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropPartition},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		Params.Init()

		collectionName := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName}

		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(coll.Clone(), nil)

		core := newTestCore(withMeta(meta))
		task := &dropPartitionTask{
			baseTask: baseTask{core: core},
			Req: &milvuspb.DropPartitionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropPartition},
				CollectionName: collectionName,
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
		assert.True(t, coll.Equal(*task.collMeta))
	})
}

func Test_dropPartitionTask_Execute(t *testing.T) {
	t.Run("drop non-existent partition", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		partitionName := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName, Partitions: []*model.Partition{}}
		task := &dropPartitionTask{
			Req: &milvuspb.DropPartitionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropPartition},
				CollectionName: collectionName,
				PartitionName:  partitionName,
			},
			collMeta: coll.Clone(),
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
	})

	t.Run("failed to expire cache", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		partitionName := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName, Partitions: []*model.Partition{{PartitionName: partitionName}}}
		core := newTestCore(withInvalidProxyManager())
		task := &dropPartitionTask{
			baseTask: baseTask{core: core},
			Req: &milvuspb.DropPartitionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropPartition},
				CollectionName: collectionName,
				PartitionName:  partitionName,
			},
			collMeta: coll.Clone(),
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to change partition state", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		partitionName := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName, Partitions: []*model.Partition{{PartitionName: partitionName}}}
		core := newTestCore(withValidProxyManager(), withInvalidMeta())
		task := &dropPartitionTask{
			baseTask: baseTask{core: core},
			Req: &milvuspb.DropPartitionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropPartition},
				CollectionName: collectionName,
				PartitionName:  partitionName,
			},
			collMeta: coll.Clone(),
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		confirmGCInterval = time.Millisecond
		defer restoreConfirmGCInterval()

		collectionName := funcutil.GenRandomStr()
		partitionName := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName, Partitions: []*model.Partition{{PartitionName: partitionName}}}
		removePartitionMetaCalled := false
		removePartitionMetaChan := make(chan struct{}, 1)

		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("ChangePartitionState",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		meta.On("RemovePartition",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(func(ctx context.Context, dbID int64, collectionID int64, partitionID int64, ts uint64) error {
			removePartitionMetaCalled = true
			removePartitionMetaChan <- struct{}{}
			return nil
		})

		gc := newMockGarbageCollector()
		deletePartitionCalled := false
		deletePartitionChan := make(chan struct{}, 1)
		gc.GcPartitionDataFunc = func(ctx context.Context, pChannels []string, coll *model.Partition) (Timestamp, error) {
			deletePartitionChan <- struct{}{}
			deletePartitionCalled = true
			time.Sleep(confirmGCInterval)
			return 0, nil
		}

		broker := newMockBroker()
		broker.GCConfirmFunc = func(ctx context.Context, collectionID, partitionID UniqueID) bool {
			return true
		}
		broker.DropCollectionIndexFunc = func(ctx context.Context, collID UniqueID, partIDs []UniqueID) error {
			return nil
		}
		broker.ReleasePartitionsFunc = func(ctx context.Context, collectionID UniqueID, partitionIDs ...UniqueID) error {
			return nil
		}

		core := newTestCore(
			withValidProxyManager(),
			withMeta(meta),
			withGarbageCollector(gc),
			withBroker(broker))

		task := &dropPartitionTask{
			baseTask: baseTask{core: core},
			Req: &milvuspb.DropPartitionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropPartition},
				CollectionName: collectionName,
				PartitionName:  partitionName,
			},
			collMeta: coll.Clone(),
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
		// check if redo worked.
		<-removePartitionMetaChan
		assert.True(t, removePartitionMetaCalled)
		<-deletePartitionChan
		assert.True(t, deletePartitionCalled)
	})
}
