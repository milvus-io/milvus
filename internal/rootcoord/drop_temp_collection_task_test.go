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
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func prepareColl() (*model.Collection, *timetickSync) {
	collectionName := funcutil.GenRandomStr()
	shardNum := 2
	ticker := newRocksMqTtSynchronizer()
	pchans := ticker.getDmlChannelNames(shardNum)
	ticker.addDmlChannels(pchans...)
	return &model.Collection{Name: collectionName, ShardsNum: int32(shardNum), PhysicalChannelNames: pchans}, ticker
}

func TestTask_tempCollection_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &dropTempCollectionTask{
			tempCollectionTask: tempCollectionTask{
				TruncateReq: &rootcoordpb.TruncateCollectionRequest{
					Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DescribeCollection},
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})
	t.Run("clear tmp collection fail while describe temp collection", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("mock error")).Once()
		core := newTestCore(withMeta(meta))
		task := &dropTempCollectionTask{
			tempCollectionTask: tempCollectionTask{
				baseTask: newBaseTask(context.Background(), core),
				TruncateReq: &rootcoordpb.TruncateCollectionRequest{
					Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
					CollectionName: collectionName,
					NeedLoad:       false,
					Clear:          true,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})
	t.Run("clear tmp collection success even if describe target collection fail", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Once()
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{CollectionID: int64(1), Name: collectionName}, nil).Once()
		core := newTestCore(withMeta(meta))
		task := &dropTempCollectionTask{
			tempCollectionTask: tempCollectionTask{
				baseTask: newBaseTask(context.Background(), core),
				TruncateReq: &rootcoordpb.TruncateCollectionRequest{
					Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
					CollectionName: collectionName,
					NeedLoad:       false,
					Clear:          true,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, task.CollectionIDs.CollectionID, task.CollectionIDs.TempCollectionID)
	})
	t.Run("clear tmp collection success", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{CollectionID: int64(1), Name: collectionName}, nil).Once()
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{CollectionID: int64(0), Name: collectionName}, nil).Once()
		core := newTestCore(withMeta(meta))
		task := &dropTempCollectionTask{
			tempCollectionTask: tempCollectionTask{
				baseTask: newBaseTask(context.Background(), core),
				TruncateReq: &rootcoordpb.TruncateCollectionRequest{
					Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
					CollectionName: collectionName,
					NeedLoad:       false,
					Clear:          true,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
		assert.NotEqual(t, task.CollectionIDs.CollectionID, task.CollectionIDs.TempCollectionID)
		assert.Equal(t, int64(1), task.CollectionIDs.CollectionID+task.CollectionIDs.TempCollectionID)
	})
	t.Run("prepare truncate collection with collection IDS", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		tempCollectionName := util.GenerateTempCollectionName(collectionName)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("mock error")).Once()
		meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Once()
		meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{CollectionID: int64(0), Name: collectionName}, nil).Once()
		meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{CollectionID: int64(0), Name: tempCollectionName, State: pb.CollectionState_CollectionDropping}, nil).Once()
		core := newTestCore(withMeta(meta))
		task := &dropTempCollectionTask{
			tempCollectionTask: tempCollectionTask{
				baseTask:      newBaseTask(context.Background(), core),
				CollectionIDs: &indexpb.CollectionWithTempRequest{TempCollectionID: 0, CollectionID: 1},
				TruncateReq: &rootcoordpb.TruncateCollectionRequest{
					Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
					CollectionName: collectionName,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
		err = task.Prepare(context.Background())
		assert.Error(t, err)
		err = task.Prepare(context.Background())
		assert.Error(t, err)
		err = task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func TestTask_markTempCollectionAsDeleted_Execute(t *testing.T) {
	t.Run("failed to change collection state", func(t *testing.T) {
		ctx := context.Background()
		collInfo, ticker := prepareColl()
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().ChangeCollectionState(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("error mock ChangeCollectionState")).Once()
		core := newTestCore(withValidProxyManager(), withMeta(meta), withTtSynchronizer(ticker))
		task := &markTempCollectionAsDeleteTask{
			tempCollectionTask: tempCollectionTask{
				baseTask:      newBaseTask(context.Background(), core),
				CollectionIDs: &indexpb.CollectionWithTempRequest{},
				collInfo:      collInfo,
			},
		}
		err := task.Execute(ctx)
		assert.Error(t, err)
	})
}

func TestTask_dropTempCollection_Execute(t *testing.T) {
	t.Run("normal case async", func(t *testing.T) {
		confirmGCInterval = time.Millisecond
		defer restoreConfirmGCInterval()
		ctx := context.Background()
		collInfo, ticker := prepareColl()
		collInfo.State = pb.CollectionState_CollectionDropping
		meta := mockrootcoord.NewIMetaTable(t)
		removeCollectionMetaCalled := false
		removeCollectionMetaChan := make(chan struct{}, 1)
		meta.EXPECT().RemoveCollection(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, collID UniqueID, ts Timestamp) error {
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
		times := 0
		broker.DropCollectionIndexFunc = func(ctx context.Context, collID UniqueID, partIDs []UniqueID) error {
			if times < 1 {
				times++
				return errors.New("mock error")
			}
			dropIndexCalled = true
			dropIndexChan <- struct{}{}
			time.Sleep(confirmGCInterval)
			return nil
		}
		broker.GCConfirmFunc = func(ctx context.Context, collectionID, partitionID UniqueID) bool { return true }
		gc := newMockGarbageCollector()
		deleteCollectionCalled := false
		deleteCollectionChan := make(chan struct{}, 1)
		gc.GcCollectionDataFunc = func(ctx context.Context, coll *model.Collection) (Timestamp, error) {
			deleteCollectionCalled = true
			deleteCollectionChan <- struct{}{}
			return 0, nil
		}
		executor := newMockStepExecutor()
		executor.AddStepsFunc = func(s *stepStack) {
			for todo := s; todo != nil; {
				todo = todo.Execute(ctx)
			}
		}
		core := newTestCore(withValidProxyManager(), withBroker(broker), withMeta(meta), withGarbageCollector(gc), withTtSynchronizer(ticker), withStepExecutor(executor))
		task := &dropTempCollectionTask{
			tempCollectionTask: tempCollectionTask{
				baseTask: newBaseTask(context.Background(), core),
				CollectionIDs: &indexpb.CollectionWithTempRequest{
					CollectionID:     int64(1),
					TempCollectionID: int64(0),
				},
				collInfo: collInfo,
			},
		}
		err := task.Execute(ctx)
		assert.NoError(t, err)

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

func TestStep_dropTempIndexes(t *testing.T) {
	broker := newMockBroker()
	broker.DropCollectionIndexFunc = func(ctx context.Context, collID UniqueID, partIDs []UniqueID) error {
		return nil
	}
	core := newTestCore(withBroker(broker))
	step := dropIndexTempStep{
		baseStep:         baseStep{core: core},
		collectionID:     int64(1),
		tempCollectionID: int64(0),
	}
	_, err := step.Execute(context.Background())
	assert.NoError(t, err)
	assert.NotEmpty(t, step.Desc())
	assert.Equal(t, stepPriorityNormal, int(step.Weight()))
}
