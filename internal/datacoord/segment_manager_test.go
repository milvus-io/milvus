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

package datacoord

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	mockkv "github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestManagerOptions(t *testing.T) {
	//	ctx := context.Background()
	paramtable.Init()
	mockAllocator := newMockAllocator(t)
	meta, err := newMemoryMeta(t)
	assert.NoError(t, err)
	segmentManager, _ := newSegmentManager(meta, mockAllocator)
	t.Run("test with alloc helper", func(t *testing.T) {
		opt := withAllocHelper(allocHelper{})
		opt.apply(segmentManager)

		assert.True(t, segmentManager.helper.afterCreateSegment == nil)
	})

	t.Run("test withCalUpperLimitPolicy", func(t *testing.T) {
		opt := withCalUpperLimitPolicy(defaultCalUpperLimitPolicy())
		assert.NotNil(t, opt)

		// manual set nil``
		segmentManager.estimatePolicy = nil
		opt.apply(segmentManager)
		assert.True(t, segmentManager.estimatePolicy != nil)
	})

	t.Run("test withAllocPolicy", func(t *testing.T) {
		opt := withAllocPolicy(defaultAllocatePolicy())
		assert.NotNil(t, opt)
		// manual set nil
		segmentManager.allocPolicy = nil
		opt.apply(segmentManager)
		assert.True(t, segmentManager.allocPolicy != nil)
	})

	t.Run("test withSegmentSealPolicy", func(t *testing.T) {
		opt := withSegmentSealPolices(defaultSegmentSealPolicy()...)
		assert.NotNil(t, opt)
		// manual set nil
		segmentManager.segmentSealPolicies = []SegmentSealPolicy{}
		opt.apply(segmentManager)
		assert.True(t, len(segmentManager.segmentSealPolicies) > 0)
	})

	t.Run("test withChannelSealPolicies", func(t *testing.T) {
		opt := withChannelSealPolices(getChannelOpenSegCapacityPolicy(1000))
		assert.NotNil(t, opt)
		// manual set nil
		segmentManager.channelSealPolicies = []channelSealPolicy{}
		opt.apply(segmentManager)
		assert.True(t, len(segmentManager.channelSealPolicies) > 0)
	})
	t.Run("test withFlushPolicy", func(t *testing.T) {
		opt := withFlushPolicy(defaultFlushPolicy())
		assert.NotNil(t, opt)
		// manual set nil
		segmentManager.flushPolicy = nil
		opt.apply(segmentManager)
		assert.True(t, segmentManager.flushPolicy != nil)
	})
}

func TestAllocSegment(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()
	Params.Save(Params.DataCoordCfg.AllocLatestExpireAttempt.Key, "1")
	mockAllocator := newMockAllocator(t)
	meta, err := newMemoryMeta(t)
	assert.NoError(t, err)
	segmentManager, _ := newSegmentManager(meta, mockAllocator)

	schema := newTestSchema()
	collID, err := mockAllocator.AllocID(ctx)
	assert.NoError(t, err)
	meta.AddCollection(&collectionInfo{ID: collID, Schema: schema})

	t.Run("normal allocation", func(t *testing.T) {
		allocations, err := segmentManager.AllocSegment(ctx, collID, 100, "c1", 100)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(allocations))
		assert.EqualValues(t, 100, allocations[0].NumOfRows)
		assert.NotEqualValues(t, 0, allocations[0].SegmentID)
		assert.NotEqualValues(t, 0, allocations[0].ExpireTime)
	})

	t.Run("allocation fails 1", func(t *testing.T) {
		failsAllocator := allocator.NewMockAllocator(t)
		failsAllocator.EXPECT().AllocID(mock.Anything).Return(0, errors.New("mock")).Maybe()
		failsAllocator.EXPECT().AllocTimestamp(mock.Anything).Return(0, nil).Maybe()
		// failsAllocator := &FailsAllocator{
		// 	allocTsSucceed: true,
		// 	AllocIDSucceed: false,
		// }
		segmentManager, err := newSegmentManager(meta, failsAllocator)
		assert.NoError(t, err)
		_, err = segmentManager.AllocSegment(ctx, collID, 100, "c2", 100)
		assert.Error(t, err)
	})

	t.Run("allocation fails 2", func(t *testing.T) {
		failsAllocator := allocator.NewMockAllocator(t)
		failsAllocator.EXPECT().AllocID(mock.Anything).Return(0, nil).Maybe()
		failsAllocator.EXPECT().AllocTimestamp(mock.Anything).Return(0, errors.New("mock")).Maybe()
		segmentManager, err := newSegmentManager(meta, failsAllocator)
		assert.Error(t, err)
		assert.Nil(t, segmentManager)
	})

	t.Run("alloc clear unhealthy segment", func(t *testing.T) {
		vchannel := "c1"
		partitionID := int64(100)
		allocations1, err := segmentManager.AllocSegment(ctx, collID, partitionID, vchannel, 100)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(allocations1))
		segments, ok := segmentManager.channel2Growing.Get(vchannel)
		assert.True(t, ok)
		assert.EqualValues(t, 1, segments.Len())

		err = meta.SetState(context.TODO(), allocations1[0].SegmentID, commonpb.SegmentState_Dropped)
		assert.NoError(t, err)

		allocations2, err := segmentManager.AllocSegment(ctx, collID, partitionID, vchannel, 100)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(allocations2))
		// clear old healthy and alloc new
		segments, ok = segmentManager.channel2Growing.Get(vchannel)
		assert.True(t, ok)
		assert.EqualValues(t, 1, segments.Len())
		assert.NotEqual(t, allocations1[0].SegmentID, allocations2[0].SegmentID)
	})
}

func TestLastExpireReset(t *testing.T) {
	// set up meta on dc
	ctx := context.Background()
	paramtable.Init()
	Params.Save(Params.DataCoordCfg.AllocLatestExpireAttempt.Key, "1")
	Params.Save(Params.DataCoordCfg.SegmentMaxSize.Key, "1")
	defer func() {
		Params.Save(Params.DataCoordCfg.AllocLatestExpireAttempt.Key, "200")
		Params.Save(Params.DataCoordCfg.SegmentMaxSize.Key, "1024")
	}()
	mockAllocator := allocator.NewRootCoordAllocator(newMockRootCoordClient())
	etcdCli, _ := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	rootPath := "/test/segment/last/expire"
	metaKV := etcdkv.NewEtcdKV(etcdCli, rootPath)
	metaKV.RemoveWithPrefix(ctx, "")
	catalog := datacoord.NewCatalog(metaKV, "", "")
	collID, err := mockAllocator.AllocID(ctx)
	assert.Nil(t, err)
	broker := broker.NewMockBroker(t)
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(&rootcoordpb.ShowCollectionIDsResponse{
		Status: merr.Success(),
		DbCollections: []*rootcoordpb.DBCollections{
			{
				DbName:        "db_1",
				CollectionIDs: []int64{collID},
			},
		},
	}, nil)
	meta, err := newMeta(context.TODO(), catalog, nil, broker)
	assert.Nil(t, err)
	// add collection
	channelName := "c1"
	schema := newTestSchema()
	meta.AddCollection(&collectionInfo{ID: collID, Schema: schema})
	initSegment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1,
			CollectionID:  collID,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Growing,
		},
	}
	meta.AddSegment(context.TODO(), initSegment)

	// assign segments, set max segment to only 1MB, equalling to 10485 rows
	var bigRows, smallRows int64 = 10000, 1000
	segmentManager, _ := newSegmentManager(meta, mockAllocator)
	initSegment.SegmentInfo.State = commonpb.SegmentState_Dropped
	meta.segments.SetSegment(1, initSegment)
	allocs, _ := segmentManager.AllocSegment(context.Background(), collID, 0, channelName, bigRows)
	segmentID1, expire1 := allocs[0].SegmentID, allocs[0].ExpireTime
	time.Sleep(100 * time.Millisecond)
	allocs, _ = segmentManager.AllocSegment(context.Background(), collID, 0, channelName, bigRows)
	segmentID2, expire2 := allocs[0].SegmentID, allocs[0].ExpireTime
	time.Sleep(100 * time.Millisecond)
	allocs, _ = segmentManager.AllocSegment(context.Background(), collID, 0, channelName, smallRows)
	segmentID3, expire3 := allocs[0].SegmentID, allocs[0].ExpireTime

	// simulate handleTimeTick op on dataCoord
	meta.SetCurrentRows(segmentID1, bigRows)
	meta.SetCurrentRows(segmentID2, bigRows)
	meta.SetCurrentRows(segmentID3, smallRows)
	err = segmentManager.tryToSealSegment(context.TODO(), expire1, channelName)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.SegmentState_Sealed, meta.GetSegment(context.TODO(), segmentID1).GetState())
	assert.Equal(t, commonpb.SegmentState_Sealed, meta.GetSegment(context.TODO(), segmentID2).GetState())
	assert.Equal(t, commonpb.SegmentState_Growing, meta.GetSegment(context.TODO(), segmentID3).GetState())

	// pretend that dataCoord break down
	metaKV.Close()
	etcdCli.Close()

	// dataCoord restart
	newEtcdCli, _ := etcd.GetEtcdClient(Params.EtcdCfg.UseEmbedEtcd.GetAsBool(), Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(), Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(), Params.EtcdCfg.EtcdTLSCACert.GetValue(), Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	newMetaKV := etcdkv.NewEtcdKV(newEtcdCli, rootPath)
	defer newMetaKV.RemoveWithPrefix(ctx, "")
	newCatalog := datacoord.NewCatalog(newMetaKV, "", "")
	restartedMeta, err := newMeta(context.TODO(), newCatalog, nil, broker)
	restartedMeta.AddCollection(&collectionInfo{ID: collID, Schema: schema})
	assert.Nil(t, err)
	newSegmentManager, _ := newSegmentManager(restartedMeta, mockAllocator)
	// reset row number to avoid being cleaned by empty segment
	restartedMeta.SetCurrentRows(segmentID1, bigRows)
	restartedMeta.SetCurrentRows(segmentID2, bigRows)
	restartedMeta.SetCurrentRows(segmentID3, smallRows)

	// verify lastExpire of growing and sealed segments
	segment1, segment2, segment3 := restartedMeta.GetSegment(context.TODO(), segmentID1), restartedMeta.GetSegment(context.TODO(), segmentID2), restartedMeta.GetSegment(context.TODO(), segmentID3)
	// segmentState should not be altered but growing segment's lastExpire has been reset to the latest
	assert.Equal(t, commonpb.SegmentState_Sealed, segment1.GetState())
	assert.Equal(t, commonpb.SegmentState_Sealed, segment2.GetState())
	assert.Equal(t, commonpb.SegmentState_Growing, segment3.GetState())
	assert.Equal(t, expire1, segment1.GetLastExpireTime())
	assert.Equal(t, expire2, segment2.GetLastExpireTime())
	assert.True(t, segment3.GetLastExpireTime() > expire3)
	flushableSegIDs, _ := newSegmentManager.GetFlushableSegments(context.Background(), channelName, expire3)
	assert.ElementsMatch(t, []UniqueID{segmentID1, segmentID2}, flushableSegIDs) // segment1 and segment2 can be flushed
	newAlloc, err := newSegmentManager.AllocSegment(context.Background(), collID, 0, channelName, 2000)
	assert.Nil(t, err)
	assert.Equal(t, segmentID3, newAlloc[0].SegmentID) // segment3 still can be used to allocate
}

func TestLoadSegmentsFromMeta(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()
	mockAllocator := newMockAllocator(t)
	meta, err := newMemoryMeta(t)
	assert.NoError(t, err)

	schema := newTestSchema()
	collID, err := mockAllocator.AllocID(ctx)
	assert.NoError(t, err)
	meta.AddCollection(&collectionInfo{ID: collID, Schema: schema})

	vchannel := "ch0"
	partitionID := int64(100)

	sealedSegment := &datapb.SegmentInfo{
		ID:             1,
		CollectionID:   collID,
		PartitionID:    partitionID,
		InsertChannel:  vchannel,
		State:          commonpb.SegmentState_Sealed,
		MaxRowNum:      100,
		LastExpireTime: 1000,
	}
	growingSegment := &datapb.SegmentInfo{
		ID:             2,
		CollectionID:   collID,
		PartitionID:    partitionID,
		InsertChannel:  vchannel,
		State:          commonpb.SegmentState_Growing,
		MaxRowNum:      100,
		LastExpireTime: 1000,
	}
	flushedSegment := &datapb.SegmentInfo{
		ID:             3,
		CollectionID:   collID,
		PartitionID:    partitionID,
		InsertChannel:  vchannel,
		State:          commonpb.SegmentState_Flushed,
		MaxRowNum:      100,
		LastExpireTime: 1000,
	}
	err = meta.AddSegment(context.TODO(), NewSegmentInfo(sealedSegment))
	assert.NoError(t, err)
	err = meta.AddSegment(context.TODO(), NewSegmentInfo(growingSegment))
	assert.NoError(t, err)
	err = meta.AddSegment(context.TODO(), NewSegmentInfo(flushedSegment))
	assert.NoError(t, err)

	segmentManager, err := newSegmentManager(meta, mockAllocator)
	assert.NoError(t, err)
	growing, ok := segmentManager.channel2Growing.Get(vchannel)
	assert.True(t, ok)
	assert.EqualValues(t, 1, growing.Len())
	sealed, ok := segmentManager.channel2Sealed.Get(vchannel)
	assert.True(t, ok)
	assert.EqualValues(t, 1, sealed.Len())
}

func TestSaveSegmentsToMeta(t *testing.T) {
	paramtable.Init()
	mockAllocator := newMockAllocator(t)
	meta, err := newMemoryMeta(t)
	assert.NoError(t, err)

	schema := newTestSchema()
	collID, err := mockAllocator.AllocID(context.Background())
	assert.NoError(t, err)
	meta.AddCollection(&collectionInfo{ID: collID, Schema: schema})
	segmentManager, _ := newSegmentManager(meta, mockAllocator)
	allocations, err := segmentManager.AllocSegment(context.Background(), collID, 0, "c1", 1000)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(allocations))
	_, err = segmentManager.SealAllSegments(context.Background(), "c1", nil)
	assert.NoError(t, err)
	segment := meta.GetHealthySegment(context.TODO(), allocations[0].SegmentID)
	assert.NotNil(t, segment)
	assert.EqualValues(t, segment.LastExpireTime, allocations[0].ExpireTime)
	assert.EqualValues(t, commonpb.SegmentState_Sealed, segment.State)
}

func TestSaveSegmentsToMetaWithSpecificSegments(t *testing.T) {
	paramtable.Init()
	mockAllocator := newMockAllocator(t)
	meta, err := newMemoryMeta(t)
	assert.NoError(t, err)

	schema := newTestSchema()
	collID, err := mockAllocator.AllocID(context.Background())
	assert.NoError(t, err)
	meta.AddCollection(&collectionInfo{ID: collID, Schema: schema})
	segmentManager, _ := newSegmentManager(meta, mockAllocator)
	allocations, err := segmentManager.AllocSegment(context.Background(), collID, 0, "c1", 1000)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(allocations))
	_, err = segmentManager.SealAllSegments(context.Background(), "c1", []int64{allocations[0].SegmentID})
	assert.NoError(t, err)
	segment := meta.GetHealthySegment(context.TODO(), allocations[0].SegmentID)
	assert.NotNil(t, segment)
	assert.EqualValues(t, segment.LastExpireTime, allocations[0].ExpireTime)
	assert.EqualValues(t, commonpb.SegmentState_Sealed, segment.State)
}

func TestDropSegment(t *testing.T) {
	paramtable.Init()
	mockAllocator := newMockAllocator(t)
	meta, err := newMemoryMeta(t)
	assert.NoError(t, err)

	schema := newTestSchema()
	collID, err := mockAllocator.AllocID(context.Background())
	assert.NoError(t, err)
	meta.AddCollection(&collectionInfo{ID: collID, Schema: schema})
	segmentManager, _ := newSegmentManager(meta, mockAllocator)
	allocations, err := segmentManager.AllocSegment(context.Background(), collID, 100, "c1", 1000)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(allocations))
	segID := allocations[0].SegmentID
	segment := meta.GetHealthySegment(context.TODO(), segID)
	assert.NotNil(t, segment)

	segmentManager.DropSegment(context.Background(), "c1", segID)
	segment = meta.GetHealthySegment(context.TODO(), segID)
	assert.NotNil(t, segment)
}

func TestAllocRowsLargerThanOneSegment(t *testing.T) {
	paramtable.Init()
	mockAllocator := newMockAllocator(t)
	meta, err := newMemoryMeta(t)
	assert.NoError(t, err)

	schema := newTestSchema()
	collID, err := mockAllocator.AllocID(context.Background())
	assert.NoError(t, err)
	meta.AddCollection(&collectionInfo{ID: collID, Schema: schema})

	mockPolicy := func(schema *schemapb.CollectionSchema) (int, error) {
		return 1, nil
	}
	segmentManager, _ := newSegmentManager(meta, mockAllocator, withCalUpperLimitPolicy(mockPolicy))
	allocations, err := segmentManager.AllocSegment(context.TODO(), collID, 0, "c1", 2)
	assert.NoError(t, err)
	assert.EqualValues(t, 2, len(allocations))
	assert.EqualValues(t, 1, allocations[0].NumOfRows)
	assert.EqualValues(t, 1, allocations[1].NumOfRows)
}

func TestExpireAllocation(t *testing.T) {
	paramtable.Init()
	mockAllocator := newMockAllocator(t)
	meta, err := newMemoryMeta(t)
	assert.NoError(t, err)

	schema := newTestSchema()
	collID, err := mockAllocator.AllocID(context.Background())
	assert.NoError(t, err)
	meta.AddCollection(&collectionInfo{ID: collID, Schema: schema})

	mockPolicy := func(schema *schemapb.CollectionSchema) (int, error) {
		return 10000000, nil
	}
	segmentManager, _ := newSegmentManager(meta, mockAllocator, withCalUpperLimitPolicy(mockPolicy))
	// alloc 100 times and expire
	var maxts Timestamp
	var id int64 = -1
	for i := 0; i < 100; i++ {
		allocs, err := segmentManager.AllocSegment(context.TODO(), collID, 0, "ch1", 100)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(allocs))
		if id == -1 {
			id = allocs[0].SegmentID
		} else {
			assert.EqualValues(t, id, allocs[0].SegmentID)
		}
		if allocs[0].ExpireTime > maxts {
			maxts = allocs[0].ExpireTime
		}
	}

	segment := meta.GetHealthySegment(context.TODO(), id)
	assert.NotNil(t, segment)
	assert.EqualValues(t, 100, len(segment.allocations))
	segmentManager.ExpireAllocations(context.TODO(), "ch1", maxts)
	segment = meta.GetHealthySegment(context.TODO(), id)
	assert.NotNil(t, segment)
	assert.EqualValues(t, 0, len(segment.allocations))
}

func TestGetFlushableSegments(t *testing.T) {
	t.Run("get flushable segments between small interval", func(t *testing.T) {
		paramtable.Init()
		mockAllocator := newMockAllocator(t)
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		schema := newTestSchema()
		collID, err := mockAllocator.AllocID(context.Background())
		assert.NoError(t, err)
		meta.AddCollection(&collectionInfo{ID: collID, Schema: schema})
		segmentManager, _ := newSegmentManager(meta, mockAllocator)
		allocations, err := segmentManager.AllocSegment(context.TODO(), collID, 0, "c1", 2)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(allocations))

		ids, err := segmentManager.SealAllSegments(context.TODO(), "c1", nil)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(ids))
		assert.EqualValues(t, allocations[0].SegmentID, ids[0])

		meta.SetCurrentRows(allocations[0].SegmentID, 1)
		ids, err = segmentManager.GetFlushableSegments(context.TODO(), "c1", allocations[0].ExpireTime)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(ids))
		assert.EqualValues(t, allocations[0].SegmentID, ids[0])

		meta.SetLastFlushTime(allocations[0].SegmentID, time.Now())
		ids, err = segmentManager.GetFlushableSegments(context.TODO(), "c1", allocations[0].ExpireTime)
		assert.NoError(t, err)
		assert.Empty(t, ids)

		meta.SetLastFlushTime(allocations[0].SegmentID, time.Now().Local().Add(-1*paramtable.Get().DataCoordCfg.SegmentFlushInterval.GetAsDuration(time.Second)))
		ids, err = segmentManager.GetFlushableSegments(context.TODO(), "c1", allocations[0].ExpireTime)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(ids))
		assert.EqualValues(t, allocations[0].SegmentID, ids[0])

		meta.SetCurrentRows(allocations[0].SegmentID, 0)
		postions := make([]*msgpb.MsgPosition, 0)
		cpTs := allocations[0].ExpireTime + 1
		postions = append(postions, &msgpb.MsgPosition{
			ChannelName: "c1",
			MsgID:       []byte{1, 2, 3},
			Timestamp:   cpTs,
		})
		meta.UpdateChannelCheckpoints(context.TODO(), postions)
		segmentManager.CleanZeroSealedSegmentsOfChannel(context.TODO(), "c1", cpTs)
		ids, err = segmentManager.GetFlushableSegments(context.TODO(), "c1", allocations[0].ExpireTime)
		assert.NoError(t, err)
		assert.Empty(t, ids)
		assert.Nil(t, meta.GetHealthySegment(context.TODO(), allocations[0].SegmentID))
	})
}

func alwaysSealPolicy() segmentSealPolicyFunc {
	return func(segment *SegmentInfo, ts Timestamp) (bool, string) {
		return true, ""
	}
}

func TestTryToSealSegment(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	t.Run("normal_seal with_segment_policies", func(t *testing.T) {
		pt.Save(pt.DataCoordCfg.SegmentMaxLifetime.Key, fmt.Sprintf("%d", math.MinInt64))
		defer pt.Reset(pt.DataCoordCfg.SegmentMaxLifetime.Key)
		mockAllocator := newMockAllocator(t)
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		schema := newTestSchema()
		collID, err := mockAllocator.AllocID(context.Background())
		assert.NoError(t, err)
		meta.AddCollection(&collectionInfo{ID: collID, Schema: schema})
		segmentManager, _ := newSegmentManager(meta, mockAllocator, withSegmentSealPolices(sealL1SegmentByLifetime())) // always seal
		allocations, err := segmentManager.AllocSegment(context.TODO(), collID, 0, "c1", 2)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(allocations))

		ts, err := segmentManager.allocator.AllocTimestamp(context.Background())
		assert.NoError(t, err)
		err = segmentManager.tryToSealSegment(context.TODO(), ts, "c1")
		assert.NoError(t, err)

		for _, seg := range segmentManager.meta.segments.segments {
			if seg.GetStartPosition() != nil {
				assert.Equal(t, commonpb.SegmentState_Sealed, seg.GetState())
			}
		}
	})

	t.Run("normal_seal_with_channel_seal_policies", func(t *testing.T) {
		paramtable.Init()
		mockAllocator := newMockAllocator(t)
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		schema := newTestSchema()
		collID, err := mockAllocator.AllocID(context.Background())
		assert.NoError(t, err)
		meta.AddCollection(&collectionInfo{ID: collID, Schema: schema})
		segmentManager, _ := newSegmentManager(meta, mockAllocator, withChannelSealPolices(getChannelOpenSegCapacityPolicy(-1))) // always seal
		allocations, err := segmentManager.AllocSegment(context.TODO(), collID, 0, "c1", 2)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(allocations))

		ts, err := segmentManager.allocator.AllocTimestamp(context.Background())
		assert.NoError(t, err)
		err = segmentManager.tryToSealSegment(context.TODO(), ts, "c1")
		assert.NoError(t, err)

		for _, seg := range segmentManager.meta.segments.segments {
			assert.Equal(t, commonpb.SegmentState_Sealed, seg.GetState())
		}
	})

	t.Run("normal seal with both segment & channel seal policy", func(t *testing.T) {
		paramtable.Init()
		mockAllocator := newMockAllocator(t)
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		schema := newTestSchema()
		collID, err := mockAllocator.AllocID(context.Background())
		assert.NoError(t, err)
		meta.AddCollection(&collectionInfo{ID: collID, Schema: schema})
		segmentManager, _ := newSegmentManager(meta, mockAllocator,
			withSegmentSealPolices(sealL1SegmentByLifetime()),
			withChannelSealPolices(getChannelOpenSegCapacityPolicy(-1))) // always seal
		allocations, err := segmentManager.AllocSegment(context.TODO(), collID, 0, "c1", 2)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(allocations))

		ts, err := segmentManager.allocator.AllocTimestamp(context.Background())
		assert.NoError(t, err)
		err = segmentManager.tryToSealSegment(context.TODO(), ts, "c1")
		assert.NoError(t, err)

		for _, seg := range segmentManager.meta.segments.segments {
			assert.Equal(t, commonpb.SegmentState_Sealed, seg.GetState())
		}
	})

	t.Run("test sealByMaxBinlogFileNumberPolicy", func(t *testing.T) {
		paramtable.Init()
		mockAllocator := newMockAllocator(t)
		meta, err := newMemoryMeta(t)
		assert.NoError(t, err)

		schema := newTestSchema()
		collID, err := mockAllocator.AllocID(context.Background())
		assert.NoError(t, err)
		meta.AddCollection(&collectionInfo{ID: collID, Schema: schema})
		segmentManager, _ := newSegmentManager(meta, mockAllocator)
		allocations, err := segmentManager.AllocSegment(context.TODO(), collID, 0, "c1", 2)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(allocations))

		ts, err := segmentManager.allocator.AllocTimestamp(context.Background())
		assert.NoError(t, err)

		// No seal polices
		{
			err = segmentManager.tryToSealSegment(context.TODO(), ts, "c1")
			assert.NoError(t, err)
			segments := segmentManager.meta.segments.segments
			assert.Equal(t, 1, len(segments))
			for _, seg := range segments {
				assert.Equal(t, commonpb.SegmentState_Growing, seg.GetState())
			}
		}

		// Not trigger seal
		{
			segmentManager.segmentSealPolicies = []SegmentSealPolicy{sealL1SegmentByLifetime()}
			segments := segmentManager.meta.segments.segments
			assert.Equal(t, 1, len(segments))
			for _, seg := range segments {
				seg.Statslogs = []*datapb.FieldBinlog{
					{
						FieldID: 2,
						Binlogs: []*datapb.Binlog{
							{
								EntriesNum: 10,
								LogID:      3,
							},
						},
					},
				}
				err = segmentManager.tryToSealSegment(context.TODO(), ts, "c1")
				assert.NoError(t, err)
				seg = segmentManager.meta.segments.segments[seg.ID]
				assert.Equal(t, commonpb.SegmentState_Growing, seg.GetState())
			}
		}

		// Trigger seal
		{
			segmentManager.segmentSealPolicies = []SegmentSealPolicy{sealL1SegmentByBinlogFileNumber(2)}
			segments := segmentManager.meta.segments.segments
			assert.Equal(t, 1, len(segments))
			for _, seg := range segments {
				seg.Binlogs = []*datapb.FieldBinlog{
					{
						FieldID: 1,
						Binlogs: []*datapb.Binlog{
							{
								EntriesNum: 10,
								LogID:      1,
							},
							{
								EntriesNum: 20,
								LogID:      2,
							},
						},
					},
				}
				err = segmentManager.tryToSealSegment(context.TODO(), ts, "c1")
				assert.NoError(t, err)
				seg = segmentManager.meta.segments.segments[seg.ID]
				assert.Equal(t, commonpb.SegmentState_Sealed, seg.GetState())
			}
		}
	})

	t.Run("seal with segment policy with kv fails", func(t *testing.T) {
		mockAllocator := newMockAllocator(t)
		memoryKV := NewMetaMemoryKV()
		catalog := datacoord.NewCatalog(memoryKV, "", "")
		broker := broker.NewMockBroker(t)
		broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
		meta, err := newMeta(context.TODO(), catalog, nil, broker)
		assert.NoError(t, err)

		schema := newTestSchema()
		collID, err := mockAllocator.AllocID(context.Background())
		assert.NoError(t, err)
		meta.AddCollection(&collectionInfo{ID: collID, Schema: schema})
		segmentManager, _ := newSegmentManager(meta, mockAllocator, withSegmentSealPolices(alwaysSealPolicy())) // always seal
		allocations, err := segmentManager.AllocSegment(context.TODO(), collID, 0, "c1", 2)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(allocations))

		metakv := mockkv.NewMetaKv(t)
		metakv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
		metakv.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
		metakv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return(nil, nil, nil).Maybe()
		segmentManager.meta.catalog = &datacoord.Catalog{MetaKv: metakv}

		ts, err := segmentManager.allocator.AllocTimestamp(context.Background())
		assert.NoError(t, err)
		err = segmentManager.tryToSealSegment(context.TODO(), ts, "c1")
		assert.Error(t, err)
	})

	t.Run("seal with channel policy with kv fails", func(t *testing.T) {
		paramtable.Init()
		mockAllocator := newMockAllocator(t)
		memoryKV := NewMetaMemoryKV()
		catalog := datacoord.NewCatalog(memoryKV, "", "")
		broker := broker.NewMockBroker(t)
		broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
		meta, err := newMeta(context.TODO(), catalog, nil, broker)
		assert.NoError(t, err)

		schema := newTestSchema()
		collID, err := mockAllocator.AllocID(context.Background())
		assert.NoError(t, err)
		meta.AddCollection(&collectionInfo{ID: collID, Schema: schema})
		segmentManager, _ := newSegmentManager(meta, mockAllocator, withChannelSealPolices(getChannelOpenSegCapacityPolicy(-1))) // always seal
		allocations, err := segmentManager.AllocSegment(context.TODO(), collID, 0, "c1", 2)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(allocations))

		metakv := mockkv.NewMetaKv(t)
		metakv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
		metakv.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
		metakv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return(nil, nil, nil).Maybe()
		segmentManager.meta.catalog = &datacoord.Catalog{MetaKv: metakv}

		ts, err := segmentManager.allocator.AllocTimestamp(context.Background())
		assert.NoError(t, err)
		err = segmentManager.tryToSealSegment(context.TODO(), ts, "c1")
		assert.Error(t, err)
	})
}

func TestAllocationPool(t *testing.T) {
	t.Run("normal get&put", func(t *testing.T) {
		allocPool = sync.Pool{
			New: func() interface{} {
				return &Allocation{}
			},
		}

		allo := getAllocation(100)
		assert.EqualValues(t, 100, allo.NumOfRows)
		assert.EqualValues(t, 0, allo.ExpireTime)
		assert.EqualValues(t, 0, allo.SegmentID)

		putAllocation(allo)
	})

	t.Run("put nil", func(t *testing.T) {
		var allo *Allocation
		allocPool = sync.Pool{
			New: func() interface{} {
				return &Allocation{}
			},
		}
		putAllocation(allo)
		allo = getAllocation(100)
		assert.EqualValues(t, 100, allo.NumOfRows)
		assert.EqualValues(t, 0, allo.ExpireTime)
		assert.EqualValues(t, 0, allo.SegmentID)
	})

	t.Run("put something else", func(t *testing.T) {
		allocPool = sync.Pool{
			New: func() interface{} {
				return &Allocation{}
			},
		}
		allocPool.Put(&struct{}{})
		allo := getAllocation(100)
		assert.EqualValues(t, 100, allo.NumOfRows)
		assert.EqualValues(t, 0, allo.ExpireTime)
		assert.EqualValues(t, 0, allo.SegmentID)
	})
}

func TestSegmentManager_DropSegmentsOfChannel(t *testing.T) {
	partitionID := int64(100)
	type fields struct {
		meta     *meta
		segments []UniqueID
	}
	type args struct {
		channel string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []UniqueID
	}{
		{
			"test drop segments",
			fields{
				meta: &meta{
					segments: &SegmentsInfo{
						segments: map[int64]*SegmentInfo{
							1: {
								SegmentInfo: &datapb.SegmentInfo{
									ID:            1,
									PartitionID:   partitionID,
									InsertChannel: "ch1",
									State:         commonpb.SegmentState_Sealed,
								},
							},
							2: {
								SegmentInfo: &datapb.SegmentInfo{
									ID:            2,
									PartitionID:   partitionID,
									InsertChannel: "ch2",
									State:         commonpb.SegmentState_Growing,
								},
							},
						},
					},
				},
				segments: []UniqueID{1, 2},
			},
			args{
				"ch1",
			},
			[]UniqueID{2},
		},
		{
			"test drop segments with dropped segment",
			fields{
				meta: &meta{
					segments: &SegmentsInfo{
						segments: map[int64]*SegmentInfo{
							1: {
								SegmentInfo: &datapb.SegmentInfo{
									ID:            1,
									PartitionID:   partitionID,
									InsertChannel: "ch1",
									State:         commonpb.SegmentState_Sealed,
								},
							},
							2: {
								SegmentInfo: &datapb.SegmentInfo{
									ID:            2,
									PartitionID:   partitionID,
									InsertChannel: "ch2",
									State:         commonpb.SegmentState_Growing,
								},
							},
						},
					},
				},
				segments: []UniqueID{1, 2, 3},
			},
			args{
				"ch1",
			},
			[]UniqueID{2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SegmentManager{
				meta:            tt.fields.meta,
				channelLock:     lock.NewKeyLock[string](),
				channel2Growing: typeutil.NewConcurrentMap[string, typeutil.UniqueSet](),
				channel2Sealed:  typeutil.NewConcurrentMap[string, typeutil.UniqueSet](),
			}
			for _, segmentID := range tt.fields.segments {
				segmentInfo := tt.fields.meta.GetSegment(context.Background(), segmentID)
				channel := tt.args.channel
				if segmentInfo != nil {
					channel = segmentInfo.GetInsertChannel()
				}
				if segmentInfo == nil || segmentInfo.GetState() == commonpb.SegmentState_Growing {
					growing, _ := s.channel2Growing.GetOrInsert(channel, typeutil.NewUniqueSet())
					growing.Insert(segmentID)
				} else if segmentInfo.GetState() == commonpb.SegmentState_Sealed {
					sealed, _ := s.channel2Sealed.GetOrInsert(channel, typeutil.NewUniqueSet())
					sealed.Insert(segmentID)
				}
			}
			s.DropSegmentsOfChannel(context.TODO(), tt.args.channel)
			all := make([]int64, 0)
			s.channel2Sealed.Range(func(_ string, segments typeutil.UniqueSet) bool {
				all = append(all, segments.Collect()...)
				return true
			})
			s.channel2Growing.Range(func(_ string, segments typeutil.UniqueSet) bool {
				all = append(all, segments.Collect()...)
				return true
			})
			assert.ElementsMatch(t, tt.want, all)
		})
	}
}

func TestSegmentManager_CleanZeroSealedSegmentsOfChannel(t *testing.T) {
	partitionID := int64(100)
	type fields struct {
		meta     *meta
		segments []UniqueID
	}
	type args struct {
		channel string
		cpTs    Timestamp
	}

	mockCatalog := mocks.NewDataCoordCatalog(t)
	mockCatalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	seg1 := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             1,
			PartitionID:    partitionID,
			InsertChannel:  "ch1",
			State:          commonpb.SegmentState_Sealed,
			NumOfRows:      1,
			LastExpireTime: 100,
		},
		currRows: 1,
	}
	seg2 := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             2,
			PartitionID:    partitionID,
			InsertChannel:  "ch1",
			State:          commonpb.SegmentState_Sealed,
			NumOfRows:      0,
			LastExpireTime: 100,
		},
		currRows: 0,
	}
	seg3 := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             3,
			PartitionID:    partitionID,
			InsertChannel:  "ch1",
			State:          commonpb.SegmentState_Sealed,
			LastExpireTime: 90,
		},
	}
	seg4 := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             4,
			PartitionID:    partitionID,
			InsertChannel:  "ch2",
			State:          commonpb.SegmentState_Growing,
			NumOfRows:      1,
			LastExpireTime: 100,
		},
		currRows: 1,
	}

	newMetaFunc := func() *meta {
		return &meta{
			catalog: mockCatalog,
			segments: &SegmentsInfo{
				segments: map[int64]*SegmentInfo{
					1: seg1,
					2: seg2,
					3: seg3,
					4: seg4,
				},
				secondaryIndexes: segmentInfoIndexes{
					coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
						0: {1: seg1, 2: seg2, 3: seg3, 4: seg4},
					},
					channel2Segments: map[string]map[UniqueID]*SegmentInfo{
						"ch1": {1: seg1, 2: seg2, 3: seg3},
						"ch2": {4: seg4},
					},
				},
			},
		}
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		want   []UniqueID
	}{
		{
			"test clean empty sealed segments with normal channel cp <= lastExpireTs",
			fields{
				meta:     newMetaFunc(),
				segments: []UniqueID{1, 2, 3, 4},
			},
			args{
				"ch1", 100,
			},
			[]UniqueID{1, 2, 4},
		},
		{
			"test clean empty sealed segments with normal channel cp > lastExpireTs",
			fields{
				meta:     newMetaFunc(),
				segments: []UniqueID{1, 2, 3, 4},
			},
			args{
				"ch1", 101,
			},
			[]UniqueID{1, 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SegmentManager{
				meta:            tt.fields.meta,
				channelLock:     lock.NewKeyLock[string](),
				channel2Growing: typeutil.NewConcurrentMap[string, typeutil.UniqueSet](),
				channel2Sealed:  typeutil.NewConcurrentMap[string, typeutil.UniqueSet](),
			}
			for _, segmentID := range tt.fields.segments {
				segmentInfo := tt.fields.meta.GetSegment(context.TODO(), segmentID)
				channel := tt.args.channel
				if segmentInfo != nil {
					channel = segmentInfo.GetInsertChannel()
				}
				if segmentInfo == nil || segmentInfo.GetState() == commonpb.SegmentState_Growing {
					growing, _ := s.channel2Growing.GetOrInsert(channel, typeutil.NewUniqueSet())
					growing.Insert(segmentID)
				} else if segmentInfo.GetState() == commonpb.SegmentState_Sealed {
					sealed, _ := s.channel2Sealed.GetOrInsert(channel, typeutil.NewUniqueSet())
					sealed.Insert(segmentID)
				}
			}
			s.CleanZeroSealedSegmentsOfChannel(context.TODO(), tt.args.channel, tt.args.cpTs)
			all := make([]int64, 0)
			s.channel2Sealed.Range(func(_ string, segments typeutil.UniqueSet) bool {
				all = append(all, segments.Collect()...)
				return true
			})
			s.channel2Growing.Range(func(_ string, segments typeutil.UniqueSet) bool {
				all = append(all, segments.Collect()...)
				return true
			})
			assert.ElementsMatch(t, tt.want, all)
		})
	}
}
