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

package querycoord

import (
	"container/list"
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var handoffHandlerTestDir = "/tmp/milvus_test/handoff_handler"

func TestHandoffHandlerReloadFromKV(t *testing.T) {
	refreshParams()
	baseCtx, cancel := context.WithCancel(context.Background())
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	defer etcdCli.Close()
	assert.Nil(t, err)
	kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	id := UniqueID(rand.Int31())
	idAllocator := func() (UniqueID, error) {
		newID := atomic.AddInt64(&id, 1)
		return newID, nil
	}
	meta, err := newMeta(baseCtx, kv, nil, idAllocator)
	assert.Nil(t, err)

	segmentInfo := &querypb.SegmentInfo{
		SegmentID:    defaultSegmentID,
		CollectionID: defaultCollectionID,
		PartitionID:  defaultPartitionID,
		SegmentState: commonpb.SegmentState_Sealed,
	}
	key := fmt.Sprintf("%s/%d/%d/%d", util.HandoffSegmentPrefix, defaultCollectionID, defaultPartitionID, defaultSegmentID)
	value, err := proto.Marshal(segmentInfo)
	assert.Nil(t, err)
	err = kv.Save(key, string(value))
	assert.Nil(t, err)

	scheduler, err := newTaskScheduler(baseCtx, meta, nil, kv, nil, func() (UniqueID, error) {
		return 1, nil
	})
	require.NoError(t, err)

	t.Run("Test_CollectionNotExist", func(t *testing.T) {
		handoffHandler, err := newHandoffHandler(baseCtx, kv, meta, nil, scheduler, nil)
		assert.Nil(t, err)
		assert.True(t, len(handoffHandler.tasks) > 0)
		for _, task := range handoffHandler.tasks {
			assert.Equal(t, handoffTaskCancel, task.state)
		}
	})

	err = kv.Save(key, string(value))
	assert.Nil(t, err)

	meta.addCollection(defaultCollectionID, querypb.LoadType_LoadPartition, genDefaultCollectionSchema(false))

	t.Run("Test_PartitionNotExist", func(t *testing.T) {
		handoffHandler, err := newHandoffHandler(baseCtx, kv, meta, nil, nil, nil)
		assert.Nil(t, err)
		assert.True(t, len(handoffHandler.tasks) > 0)
		for _, task := range handoffHandler.tasks {
			assert.Equal(t, handoffTaskCancel, task.state)
		}
	})

	err = kv.Save(key, string(value))
	assert.Nil(t, err)
	meta.setLoadType(defaultCollectionID, querypb.LoadType_LoadCollection)

	t.Run("Test_CollectionExist", func(t *testing.T) {
		handoffHandler, err := newHandoffHandler(baseCtx, kv, meta, nil, nil, nil)
		assert.Nil(t, err)
		assert.True(t, len(handoffHandler.tasks) > 0)
		for _, task := range handoffHandler.tasks {
			assert.Equal(t, handoffTaskInit, task.state)
		}
	})

	t.Run("Test_LoadInQueue", func(t *testing.T) {
		broker, _, rc, err := getMockGlobalMetaBroker(baseCtx)
		assert.NoError(t, err)

		list := list.New()
		list.PushBack(&querypb.LoadCollectionRequest{
			CollectionID: defaultCollectionID,
		})
		handler, err := newHandoffHandler(baseCtx, kv, meta, nil, &TaskScheduler{
			triggerTaskQueue: &taskQueue{
				tasks: list,
			},
		}, broker)

		schema := genDefaultCollectionSchema(false)

		rc.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			Schema: schema,
		}, nil)
		assert.Nil(t, err)
		assert.True(t, len(handler.tasks) > 0)
		for _, task := range handler.tasks {
			assert.Equal(t, handoffTaskInit, task.state)
		}
	})

	cancel()
}

func TestHandoffNotificationLoop(t *testing.T) {
	refreshParams()

	ctx := context.Background()
	coord, err := startQueryCoord(ctx)
	assert.NoError(t, err)
	defer coord.Stop()

	// Notify
	segmentInfo := &querypb.SegmentInfo{
		SegmentID:    defaultSegmentID,
		CollectionID: defaultCollectionID,
		PartitionID:  defaultPartitionID,
		SegmentState: commonpb.SegmentState_Sealed,
	}
	key := fmt.Sprintf("%s/%d/%d/%d", util.HandoffSegmentPrefix, defaultCollectionID, defaultPartitionID, defaultSegmentID)
	value, err := proto.Marshal(segmentInfo)
	assert.NoError(t, err)
	err = coord.kvClient.Save(key, string(value))
	assert.NoError(t, err)

	// Wait for the handoff tasks canceled
	for {
		_, err := coord.kvClient.Load(key)
		if err != nil {
			break
		}
	}
}

func TestHandoff(t *testing.T) {
	refreshParams()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	defer etcdCli.Close()
	assert.Nil(t, err)
	kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	id := UniqueID(rand.Int31())
	idAllocator := func() (UniqueID, error) {
		newID := atomic.AddInt64(&id, 1)
		return newID, nil
	}

	meta, err := newMeta(ctx, kv, nil, idAllocator)
	assert.Nil(t, err)

	rootCoord := newRootCoordMock(ctx)
	indexCoord, err := newIndexCoordMock(handoffHandlerTestDir)
	assert.Nil(t, err)
	dataCoord := newDataCoordMock(ctx)
	rootCoord.enableIndex = true
	cm := storage.NewLocalChunkManager(storage.RootPath(handoffHandlerTestDir))
	defer cm.RemoveWithPrefix("")
	broker, err := newGlobalMetaBroker(ctx, rootCoord, dataCoord, indexCoord, cm)
	assert.Nil(t, err)

	taskScheduler := &TaskScheduler{
		ctx:              ctx,
		cancel:           cancel,
		client:           kv,
		triggerTaskQueue: newTaskQueue(),
		taskIDAllocator:  idAllocator,
	}

	segmentInfo := &querypb.SegmentInfo{
		SegmentID:    defaultSegmentID,
		CollectionID: defaultCollectionID,
		PartitionID:  defaultPartitionID,
		SegmentState: commonpb.SegmentState_Sealed,
	}
	key := fmt.Sprintf("%s/%d/%d/%d", util.HandoffSegmentPrefix, defaultCollectionID, defaultPartitionID, defaultSegmentID)
	value, err := proto.Marshal(segmentInfo)
	assert.Nil(t, err)

	t.Run("Test_ReqInvalid", func(t *testing.T) {
		handoffHandler, err := newHandoffHandler(ctx, kv, meta, nil, taskScheduler, broker)
		assert.Nil(t, err)

		err = kv.Save(key, string(value))
		assert.Nil(t, err)
		handoffHandler.enqueue(segmentInfo)
		err = handoffHandler.process(segmentInfo.SegmentID)
		assert.ErrorIs(t, err, ErrHandoffRequestInvalid)

		// Process this task until it is cleaned
		for {
			_, ok := handoffHandler.tasks[segmentInfo.SegmentID]
			if !ok {
				break
			}

			handoffHandler.process(segmentInfo.SegmentID)
		}

		// Check whether the handoff event is removed
		for {
			_, err := kv.Load(key)
			if err != nil {
				break
			}
		}
	})

	t.Run("Test_CollectionReleased", func(t *testing.T) {
		meta.addCollection(defaultCollectionID, querypb.LoadType_LoadCollection, genDefaultCollectionSchema(false))

		handoffHandler, err := newHandoffHandler(ctx, kv, meta, nil, taskScheduler, broker)
		assert.Nil(t, err)

		err = kv.Save(key, string(value))
		assert.Nil(t, err)
		handoffHandler.enqueue(segmentInfo)

		// Init -> Ready
		err = handoffHandler.process(segmentInfo.SegmentID)
		assert.NoError(t, err)

		meta.releaseCollection(defaultCollectionID)

		// Ready -> Cancel, due to the collection has been released
		err = handoffHandler.process(segmentInfo.SegmentID)
		assert.ErrorIs(t, err, ErrHandoffRequestInvalid)

		task := handoffHandler.tasks[segmentInfo.SegmentID]
		assert.Equal(t, handoffTaskCancel, task.state)
		assert.True(t, task.locked)

		// Process this task until it is cleaned
		for {
			task, ok := handoffHandler.tasks[segmentInfo.SegmentID]
			if !ok {
				break
			}

			log.Debug("process task",
				zap.Int32("state", int32(task.state)),
				zap.Bool("locked", task.locked))
			handoffHandler.process(segmentInfo.SegmentID)
		}

		// Check whether the handoff event is removed
		for {
			_, err := kv.Load(key)
			if err != nil {
				break
			}
		}

		assert.Equal(t, 0, dataCoord.segmentRefCount[segmentInfo.SegmentID])
	})

	t.Run("Test_SegmentCompacted", func(t *testing.T) {
		meta.addCollection(defaultCollectionID, querypb.LoadType_LoadCollection, genDefaultCollectionSchema(false))
		defer meta.releaseCollection(defaultCollectionID)

		handoffHandler, err := newHandoffHandler(ctx, kv, meta, nil, taskScheduler, broker)
		assert.Nil(t, err)

		err = kv.Save(key, string(value))
		assert.Nil(t, err)
		handoffHandler.enqueue(segmentInfo)

		newSegment := &querypb.SegmentInfo{
			SegmentID:      defaultSegmentID + 1,
			CollectionID:   defaultCollectionID,
			PartitionID:    defaultPartitionID,
			SegmentState:   commonpb.SegmentState_Sealed,
			CompactionFrom: []UniqueID{defaultSegmentID},
		}
		newKey := fmt.Sprintf("%s/%d/%d/%d", util.HandoffSegmentPrefix, defaultCollectionID, defaultPartitionID, newSegment.SegmentID)
		value, err := proto.Marshal(newSegment)
		assert.NoError(t, err)
		err = kv.Save(newKey, string(value))
		assert.NoError(t, err)
		handoffHandler.enqueue(newSegment)

		// Init -> Ready
		err = handoffHandler.process(segmentInfo.SegmentID)
		assert.NoError(t, err)

		// Ready -> Triggered
		err = handoffHandler.process(segmentInfo.SegmentID)
		assert.NoError(t, err)

		// Process the new segment task until it is cleaned,
		// no any error in each step
		for {
			task, ok := handoffHandler.tasks[newSegment.SegmentID]
			if !ok {
				break
			}

			// Mock the task has succeeded
			if task.state == handoffTaskTriggered {
				task.state = handoffTaskDone
				continue
			}

			err := handoffHandler.process(newSegment.SegmentID)
			assert.NoError(t, err)
		}

		// The compacted segment should be removed
		for {
			_, err := kv.Load(key)
			if err != nil {
				break
			}
		}

		// The new segment should be also removed
		for {
			_, err := kv.Load(newKey)
			if err != nil {
				break
			}
		}

		assert.Equal(t, 0, dataCoord.segmentRefCount[segmentInfo.SegmentID])
		assert.Equal(t, 0, dataCoord.segmentRefCount[newSegment.SegmentID])
	})

	t.Run("Test_Handoff", func(t *testing.T) {
		meta.addCollection(defaultCollectionID, querypb.LoadType_LoadCollection, genDefaultCollectionSchema(false))

		handoffHandler, err := newHandoffHandler(ctx, kv, meta, nil, taskScheduler, broker)
		assert.Nil(t, err)

		err = kv.Save(key, string(value))
		assert.Nil(t, err)

		handoffHandler.enqueue(segmentInfo)

		// Process this task until it is cleaned,
		// no any error in each step
		for {
			task, ok := handoffHandler.tasks[segmentInfo.SegmentID]
			if !ok {
				break
			}

			// Mock the task has succeeded
			if task.state == handoffTaskTriggered {
				task.state = handoffTaskDone
				continue
			}

			err := handoffHandler.process(segmentInfo.SegmentID)
			assert.NoError(t, err)
		}

		for {
			_, err := kv.Load(key)
			if err != nil {
				break
			}
		}
	})
}
