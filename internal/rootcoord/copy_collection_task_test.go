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
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func Test_copyCollectionTask_validate(t *testing.T) {
	paramtable.Init()

	t.Run("invalid msg type", func(t *testing.T) {
		task := copyCollectionTask{
			Req: &rootcoordpb.TruncateCollectionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
			},
		}
		err := task.validate()
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		task := copyCollectionTask{
			Req: &rootcoordpb.TruncateCollectionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
			},
		}
		err := task.validate()
		assert.NoError(t, err)
	})
}

func Test_copyCollectionTask_Prepare(t *testing.T) {
	paramtable.Init()

	t.Run("fail to get collection by name", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("mock error")).Once()
		core := newTestCore(withMeta(meta))
		task := &copyCollectionTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &rootcoordpb.TruncateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: funcutil.GenRandomStr(),
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to assign id", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
		core := newTestCore(withInvalidIDAllocator(), withMeta(meta))
		task := &copyCollectionTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &rootcoordpb.TruncateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: funcutil.GenRandomStr(),
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to assign partitions", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{
			CollectionID: 1,
			ShardsNum:    2,
			Partitions: []*model.Partition{
				{
					PartitionID:   0,
					PartitionName: "",
					CollectionID:  1,
				},
			},
		}, nil).Once()

		idAllocator := newMockIDAllocator()
		idAllocator.AllocOneF = func() (UniqueID, error) {
			return 1, nil
		}
		idAllocator.AllocF = func(count uint32) (UniqueID, UniqueID, error) {
			return -1, -1, errors.New("error mock Alloc")
		}
		core := newTestCore(withIDAllocator(idAllocator), withMeta(meta))
		task := &copyCollectionTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &rootcoordpb.TruncateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: funcutil.GenRandomStr(),
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to get physical channel", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Once()
		ticker := newTickerWithMockFailStream()
		core := newTestCore(withTtSynchronizer(ticker), withMeta(meta))
		task := &copyCollectionTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &rootcoordpb.TruncateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: funcutil.GenRandomStr(),
			},
			collInfo: &model.Collection{
				CollectionID: 1,
				ShardsNum:    2,
				Partitions: []*model.Partition{
					{
						PartitionID:   0,
						PartitionName: "",
						CollectionID:  1,
					},
				},
				State: pb.CollectionState_CollectionCreated,
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{
			CollectionID: 1,
			ShardsNum:    2,
			Partitions: []*model.Partition{
				{
					PartitionID:   0,
					PartitionName: "",
					CollectionID:  1,
				},
			},
		}, nil).Once()
		core := newTestCore(withValidIDAllocator(), withTtSynchronizer(newTickerWithMockNormalStream()), withMeta(meta))
		task := &copyCollectionTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &rootcoordpb.TruncateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: funcutil.GenRandomStr(),
			},
			collInfo: &model.Collection{
				CollectionID: 1,
				ShardsNum:    2,
				Partitions: []*model.Partition{
					{
						PartitionID:   0,
						PartitionName: "",
						CollectionID:  1,
					},
				},
				State: pb.CollectionState_CollectionCreated,
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_copyCollectionTask_Execute(t *testing.T) {
	t.Run("temp collection already exists", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{
			CollectionID: 1,
			ShardsNum:    2,
			Partitions: []*model.Partition{
				{
					PartitionID:   0,
					PartitionName: "",
					CollectionID:  1,
				},
			},
		}, nil).Once()
		core := newTestCore(withMeta(meta))
		task := &copyCollectionTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &rootcoordpb.TruncateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: funcutil.GenRandomStr(),
			},
			collectionName: util.GenerateTempCollectionName("test"),
			collectionID:   99,
			collInfo: &model.Collection{
				CollectionID: 1,
				ShardsNum:    2,
				Partitions: []*model.Partition{
					{
						PartitionID:   0,
						PartitionName: "",
						CollectionID:  1,
					},
				},
				State: pb.CollectionState_CollectionCreated,
			},
			partitions: []*model.Partition{
				{
					PartitionID:   999,
					PartitionName: "",
					CollectionID:  99,
				},
			},
			partitionIDs: []UniqueID{999},
			channels: collectionChannels{
				physicalChannels: []string{funcutil.GenRandomStr(), funcutil.GenRandomStr()},
				virtualChannels:  []string{funcutil.GenRandomStr(), funcutil.GenRandomStr()},
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to get start positions", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Once()
		ticker := newTickerWithMockFailStream()
		shardNum := 2
		pchans := ticker.getDmlChannelNames(shardNum)
		core := newTestCore(withTtSynchronizer(ticker), withMeta(meta))
		task := &copyCollectionTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &rootcoordpb.TruncateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: funcutil.GenRandomStr(),
			},
			collectionName: util.GenerateTempCollectionName("test"),
			collectionID:   99,
			collInfo: &model.Collection{
				CollectionID: 1,
				ShardsNum:    2,
				Partitions: []*model.Partition{
					{
						PartitionID:   0,
						PartitionName: "",
						CollectionID:  1,
					},
				},
				State: pb.CollectionState_CollectionCreated,
			},
			partitions: []*model.Partition{
				{
					PartitionID:   999,
					PartitionName: "",
					CollectionID:  99,
				},
			},
			partitionIDs: []UniqueID{999},
			channels: collectionChannels{
				physicalChannels: pchans,
				virtualChannels:  []string{funcutil.GenRandomStr(), funcutil.GenRandomStr()},
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		shardNum := 2

		ticker := newRocksMqTtSynchronizer()
		pchans := ticker.getDmlChannelNames(shardNum)

		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Once()
		meta.EXPECT().AddCollection(mock.Anything, mock.Anything).Return(nil)
		meta.EXPECT().ChangeCollectionState(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		dc := newMockDataCoord()
		dc.GetComponentStatesFunc = func(ctx context.Context) (*milvuspb.ComponentStates, error) {
			return &milvuspb.ComponentStates{
				State: &milvuspb.ComponentInfo{
					NodeID:    TestRootCoordID,
					StateCode: commonpb.StateCode_Healthy,
				},
				SubcomponentStates: nil,
				Status:             merr.Success(),
			}, nil
		}
		dc.WatchChannelsFunc = func(ctx context.Context, req *datapb.WatchChannelsRequest) (*datapb.WatchChannelsResponse, error) {
			return &datapb.WatchChannelsResponse{Status: merr.Success()}, nil
		}

		core := newTestCore(withValidIDAllocator(),
			withMeta(meta),
			withTtSynchronizer(ticker),
			withValidProxyManager(),
			withDataCoord(dc))
		core.broker = newServerBroker(core)

		task := &copyCollectionTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &rootcoordpb.TruncateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: funcutil.GenRandomStr(),
			},
			collectionName: util.GenerateTempCollectionName("test"),
			collectionID:   99,
			collInfo: &model.Collection{
				CollectionID: 1,
				Fields: []*model.Field{
					{
						FieldID:      common.StartOfUserFieldID,
						Name:         "id",
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
				},
				ShardsNum: int32(shardNum),
				Partitions: []*model.Partition{
					{
						PartitionID:   0,
						PartitionName: "",
						CollectionID:  1,
					},
				},
				State: pb.CollectionState_CollectionCreated,
			},
			partitions: []*model.Partition{
				{
					PartitionID:   999,
					PartitionName: "",
					CollectionID:  99,
				},
			},
			partitionIDs: []UniqueID{999},
			channels: collectionChannels{
				physicalChannels: pchans,
				virtualChannels:  []string{funcutil.GenRandomStr(), funcutil.GenRandomStr()},
			},
		}

		err := task.Execute(context.Background())
		assert.NoError(t, err)
	})

	t.Run("partial error, check if undo worked", func(t *testing.T) {
		shardNum := 2
		ticker := newRocksMqTtSynchronizer()
		pchans := ticker.getDmlChannelNames(shardNum)

		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Once()
		meta.EXPECT().AddCollection(mock.Anything, mock.Anything).Return(nil)
		meta.EXPECT().ChangeCollectionState(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mock error"))

		removeCollectionCalled := false
		removeCollectionChan := make(chan struct{}, 1)
		meta.On("RemoveCollection",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(func(ctx context.Context, collID UniqueID, ts Timestamp) error {
			removeCollectionCalled = true
			removeCollectionChan <- struct{}{}
			return nil
		})

		broker := newMockBroker()
		broker.WatchChannelsFunc = func(ctx context.Context, info *watchInfo) error {
			return nil
		}

		unwatchChannelsCalled := false
		unwatchChannelsChan := make(chan struct{}, 1)
		gc := mockrootcoord.NewGarbageCollector(t)
		gc.On("GcCollectionData",
			mock.Anything, // context.Context
			mock.Anything, // *model.Collection
		).Return(func(ctx context.Context, collection *model.Collection) (ddlTs Timestamp) {
			for _, pchan := range pchans {
				ticker.syncedTtHistogram.update(pchan, 101)
			}
			unwatchChannelsCalled = true
			unwatchChannelsChan <- struct{}{}
			return 100
		}, nil)

		core := newTestCore(withValidIDAllocator(),
			withMeta(meta),
			withTtSynchronizer(ticker),
			withGarbageCollector(gc),
			withValidProxyManager(),
			withBroker(broker))

		task := &copyCollectionTask{
			baseTask: newBaseTask(context.TODO(), core),
			Req: &rootcoordpb.TruncateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: funcutil.GenRandomStr(),
			},
			collectionName: util.GenerateTempCollectionName("test"),
			collectionID:   99,
			collInfo: &model.Collection{
				CollectionID: 1,
				ShardsNum:    int32(shardNum),
				Partitions: []*model.Partition{
					{
						PartitionID:   0,
						PartitionName: "",
						CollectionID:  1,
					},
				},
				State: pb.CollectionState_CollectionCreated,
			},
			partitions: []*model.Partition{
				{
					PartitionID:   999,
					PartitionName: "",
					CollectionID:  99,
				},
			},
			partitionIDs: []UniqueID{999},
			channels: collectionChannels{
				physicalChannels: pchans,
				virtualChannels:  []string{funcutil.GenRandomStr(), funcutil.GenRandomStr()},
			},
		}

		err := task.Execute(context.Background())
		assert.Error(t, err)

		// check if undo worked.

		// undo watch.
		<-unwatchChannelsChan
		assert.True(t, unwatchChannelsCalled)

		// undo adding collection.
		<-removeCollectionChan
		assert.True(t, removeCollectionCalled)

		time.Sleep(time.Second * 2) // wait for asynchronous step done.
		// undo add channels.
		assert.Zero(t, len(ticker.listDmlChannels()))
	})
}
