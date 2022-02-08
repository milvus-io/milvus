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
	"context"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/allocator"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
)

func TestReloadFromKV(t *testing.T) {
	refreshParams()
	baseCtx, cancel := context.WithCancel(context.Background())
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	defer etcdCli.Close()
	assert.Nil(t, err)
	kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	meta, err := newMeta(baseCtx, kv, nil, nil)
	assert.Nil(t, err)

	segmentInfo := &querypb.SegmentInfo{
		SegmentID:    defaultSegmentID,
		CollectionID: defaultCollectionID,
		PartitionID:  defaultPartitionID,
		SegmentState: commonpb.SegmentState_Sealed,
	}
	key := fmt.Sprintf("%s/%d/%d/%d", handoffSegmentPrefix, defaultCollectionID, defaultPartitionID, defaultSegmentID)
	value, err := proto.Marshal(segmentInfo)
	assert.Nil(t, err)
	err = kv.Save(key, string(value))
	assert.Nil(t, err)

	t.Run("Test_CollectionNotExist", func(t *testing.T) {
		indexChecker, err := newIndexChecker(baseCtx, kv, meta, nil, nil, nil)
		assert.Nil(t, err)
		assert.Equal(t, 0, len(indexChecker.handoffReqChan))
	})

	err = kv.Save(key, string(value))
	assert.Nil(t, err)

	meta.addCollection(defaultCollectionID, querypb.LoadType_LoadPartition, genDefaultCollectionSchema(false))

	t.Run("Test_PartitionNotExist", func(t *testing.T) {
		indexChecker, err := newIndexChecker(baseCtx, kv, meta, nil, nil, nil)
		assert.Nil(t, err)
		assert.Equal(t, 0, len(indexChecker.handoffReqChan))
	})

	err = kv.Save(key, string(value))
	assert.Nil(t, err)
	meta.setLoadType(defaultCollectionID, querypb.LoadType_loadCollection)

	t.Run("Test_CollectionExist", func(t *testing.T) {
		indexChecker, err := newIndexChecker(baseCtx, kv, meta, nil, nil, nil)
		assert.Nil(t, err)
		for {
			if len(indexChecker.handoffReqChan) > 0 {
				break
			}
		}
	})

	cancel()
}

func TestCheckIndexLoop(t *testing.T) {
	refreshParams()
	ctx, cancel := context.WithCancel(context.Background())
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	defer etcdCli.Close()
	assert.Nil(t, err)
	kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	meta, err := newMeta(ctx, kv, nil, nil)
	assert.Nil(t, err)

	rootCoord := newRootCoordMock(ctx)
	indexCoord, err := newIndexCoordMock(ctx)
	assert.Nil(t, err)
	rootCoord.enableIndex = true
	broker, err := newGlobalMetaBroker(ctx, rootCoord, nil, indexCoord)
	assert.Nil(t, err)

	segmentInfo := &querypb.SegmentInfo{
		SegmentID:    defaultSegmentID,
		CollectionID: defaultCollectionID,
		PartitionID:  defaultPartitionID,
		SegmentState: commonpb.SegmentState_Sealed,
	}
	key := fmt.Sprintf("%s/%d/%d/%d", handoffSegmentPrefix, defaultCollectionID, defaultPartitionID, defaultSegmentID)
	value, err := proto.Marshal(segmentInfo)
	assert.Nil(t, err)

	t.Run("Test_ReqInValid", func(t *testing.T) {
		childCtx, childCancel := context.WithCancel(context.Background())
		indexChecker, err := newIndexChecker(childCtx, kv, meta, nil, nil, broker)
		assert.Nil(t, err)

		err = kv.Save(key, string(value))
		assert.Nil(t, err)
		indexChecker.enqueueHandoffReq(segmentInfo)
		indexChecker.wg.Add(1)
		go indexChecker.checkIndexLoop()
		for {
			_, err := kv.Load(key)
			if err != nil {
				break
			}
		}
		assert.Equal(t, 0, len(indexChecker.indexedSegmentsChan))
		childCancel()
		indexChecker.wg.Wait()
	})
	meta.addCollection(defaultCollectionID, querypb.LoadType_loadCollection, genDefaultCollectionSchema(false))
	t.Run("Test_GetIndexInfo", func(t *testing.T) {
		childCtx, childCancel := context.WithCancel(context.Background())
		indexChecker, err := newIndexChecker(childCtx, kv, meta, nil, nil, broker)
		assert.Nil(t, err)

		indexChecker.enqueueHandoffReq(segmentInfo)
		indexChecker.wg.Add(1)
		go indexChecker.checkIndexLoop()
		for {
			if len(indexChecker.indexedSegmentsChan) > 0 {
				break
			}
		}
		childCancel()
		indexChecker.wg.Wait()
	})

	cancel()
}

func TestHandoffNotExistSegment(t *testing.T) {
	refreshParams()
	ctx, cancel := context.WithCancel(context.Background())
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	defer etcdCli.Close()
	assert.Nil(t, err)
	kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	meta, err := newMeta(ctx, kv, nil, nil)
	assert.Nil(t, err)

	rootCoord := newRootCoordMock(ctx)
	rootCoord.enableIndex = true
	indexCoord, err := newIndexCoordMock(ctx)
	assert.Nil(t, err)
	indexCoord.returnError = true
	dataCoord := newDataCoordMock(ctx)
	dataCoord.segmentState = commonpb.SegmentState_NotExist
	broker, err := newGlobalMetaBroker(ctx, rootCoord, dataCoord, indexCoord)
	assert.Nil(t, err)

	meta.addCollection(defaultCollectionID, querypb.LoadType_loadCollection, genDefaultCollectionSchema(false))

	segmentInfo := &querypb.SegmentInfo{
		SegmentID:    defaultSegmentID,
		CollectionID: defaultCollectionID,
		PartitionID:  defaultPartitionID,
		SegmentState: commonpb.SegmentState_Sealed,
	}
	key := fmt.Sprintf("%s/%d/%d/%d", handoffSegmentPrefix, defaultCollectionID, defaultPartitionID, defaultSegmentID)
	value, err := proto.Marshal(segmentInfo)
	assert.Nil(t, err)

	indexChecker, err := newIndexChecker(ctx, kv, meta, nil, nil, broker)
	assert.Nil(t, err)

	err = kv.Save(key, string(value))
	assert.Nil(t, err)
	indexChecker.enqueueHandoffReq(segmentInfo)
	indexChecker.wg.Add(1)
	go indexChecker.checkIndexLoop()
	for {
		_, err := kv.Load(key)
		if err != nil {
			break
		}
	}
	assert.Equal(t, 0, len(indexChecker.indexedSegmentsChan))
	cancel()
	indexChecker.wg.Wait()
}

func TestProcessHandoffAfterIndexDone(t *testing.T) {
	refreshParams()
	ctx, cancel := context.WithCancel(context.Background())
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	defer etcdCli.Close()

	kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	meta, err := newMeta(ctx, kv, nil, nil)
	assert.Nil(t, err)
	taskScheduler := &TaskScheduler{
		ctx:              ctx,
		cancel:           cancel,
		client:           kv,
		triggerTaskQueue: newTaskQueue(),
	}
	idAllocatorKV := tsoutil.NewTSOKVBase(etcdCli, Params.EtcdCfg.KvRootPath, "queryCoordTaskID")
	idAllocator := allocator.NewGlobalIDAllocator("idTimestamp", idAllocatorKV)
	err = idAllocator.Initialize()
	assert.Nil(t, err)
	taskScheduler.taskIDAllocator = func() (UniqueID, error) {
		return idAllocator.AllocOne()
	}
	indexChecker, err := newIndexChecker(ctx, kv, meta, nil, taskScheduler, nil)
	assert.Nil(t, err)
	indexChecker.wg.Add(1)
	go indexChecker.processHandoffAfterIndexDone()

	segmentInfo := &querypb.SegmentInfo{
		SegmentID:    defaultSegmentID,
		CollectionID: defaultCollectionID,
		PartitionID:  defaultPartitionID,
		SegmentState: commonpb.SegmentState_Sealed,
	}
	key := fmt.Sprintf("%s/%d/%d/%d", handoffSegmentPrefix, defaultCollectionID, defaultPartitionID, defaultSegmentID)
	value, err := proto.Marshal(segmentInfo)
	assert.Nil(t, err)
	err = kv.Save(key, string(value))
	assert.Nil(t, err)
	indexChecker.enqueueIndexedSegment(segmentInfo)
	for {
		_, err := kv.Load(key)
		if err != nil {
			break
		}
	}
	assert.Equal(t, false, taskScheduler.triggerTaskQueue.taskEmpty())
	cancel()
	indexChecker.wg.Wait()
}
