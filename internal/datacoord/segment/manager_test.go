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

package segment

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestManagerOptions(t *testing.T) {
	//	ctx := context.Background()
	paramtable.Init()
	mockAllocator := meta.NewMockAllocator(t)
	mt, err := meta.NewMemoryMeta(t)
	assert.NoError(t, err)
	segmentManager, _ := NewSegmentManager(mt, mockAllocator)
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
		opt := WithSegmentSealPolices(defaultSegmentSealPolicy()...)
		assert.NotNil(t, opt)
		// manual set nil
		segmentManager.SegmentSealPolicies = []SegmentSealPolicy{}
		opt.apply(segmentManager)
		assert.True(t, len(segmentManager.SegmentSealPolicies) > 0)
	})

	t.Run("test withChannelSealPolicies", func(t *testing.T) {
		opt := WithChannelSealPolices(GetChannelOpenSegCapacityPolicy(1000))
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
	mockAllocator := meta.NewMockAllocator(t)
	mt, err := meta.NewMemoryMeta(t)
	assert.NoError(t, err)
	segmentManager, _ := NewSegmentManager(mt, mockAllocator)

	schema := meta.NewTestSchema()
	collID, err := mockAllocator.AllocID(ctx)
	assert.NoError(t, err)
	mt.AddCollection(&meta.CollectionInfo{ID: collID, Schema: schema})

	t.Run("normal allocation", func(t *testing.T) {
		allocations, err := segmentManager.AllocSegment(ctx, collID, 100, "c1", 100, storage.StorageV1)
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
		segmentManager, err := NewSegmentManager(mt, failsAllocator)
		assert.NoError(t, err)
		_, err = segmentManager.AllocSegment(ctx, collID, 100, "c2", 100, storage.StorageV1)
		assert.Error(t, err)
	})

	t.Run("allocation fails 2", func(t *testing.T) {
		failsAllocator := allocator.NewMockAllocator(t)
		failsAllocator.EXPECT().AllocID(mock.Anything).Return(0, nil).Maybe()
		failsAllocator.EXPECT().AllocTimestamp(mock.Anything).Return(0, errors.New("mock")).Maybe()
		segmentManager, err := NewSegmentManager(mt, failsAllocator)
		assert.Error(t, err)
		assert.Nil(t, segmentManager)
	})

	t.Run("alloc clear unhealthy segment", func(t *testing.T) {
		vchannel := "c1"
		partitionID := int64(100)
		allocations1, err := segmentManager.AllocSegment(ctx, collID, partitionID, vchannel, 100, storage.StorageV1)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(allocations1))
		segments, ok := segmentManager.channel2Growing.Get(vchannel)
		assert.True(t, ok)
		assert.EqualValues(t, 1, segments.Len())

		err = mt.SetState(context.TODO(), allocations1[0].SegmentID, commonpb.SegmentState_Dropped)
		assert.NoError(t, err)

		allocations2, err := segmentManager.AllocSegment(ctx, collID, partitionID, vchannel, 100, storage.StorageV1)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(allocations2))
		// clear old healthy and alloc new
		segments, ok = segmentManager.channel2Growing.Get(vchannel)
		assert.True(t, ok)
		assert.EqualValues(t, 1, segments.Len())
		assert.NotEqual(t, allocations1[0].SegmentID, allocations2[0].SegmentID)
	})
}

func TestLoadSegmentsFromMeta(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()
	mockAllocator := meta.NewMockAllocator(t)
	mt, err := meta.NewMemoryMeta(t)
	assert.NoError(t, err)

	schema := meta.NewTestSchema()
	collID, err := mockAllocator.AllocID(ctx)
	assert.NoError(t, err)
	mt.AddCollection(&meta.CollectionInfo{ID: collID, Schema: schema})

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
	err = mt.AddSegment(context.TODO(), meta.NewSegmentInfo(sealedSegment))
	assert.NoError(t, err)
	err = mt.AddSegment(context.TODO(), meta.NewSegmentInfo(growingSegment))
	assert.NoError(t, err)
	err = mt.AddSegment(context.TODO(), meta.NewSegmentInfo(flushedSegment))
	assert.NoError(t, err)

	segmentManager, err := NewSegmentManager(mt, mockAllocator)
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
	mockAllocator := meta.NewMockAllocator(t)
	mt, err := meta.NewMemoryMeta(t)
	assert.NoError(t, err)

	schema := meta.NewTestSchema()
	collID, err := mockAllocator.AllocID(context.Background())
	assert.NoError(t, err)
	mt.AddCollection(&meta.CollectionInfo{ID: collID, Schema: schema})
	segmentManager, _ := NewSegmentManager(mt, mockAllocator)
	allocations, err := segmentManager.AllocSegment(context.Background(), collID, 0, "c1", 1000, storage.StorageV1)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(allocations))
	_, err = segmentManager.SealAllSegments(context.Background(), "c1", nil)
	assert.NoError(t, err)
	segment := mt.GetHealthySegment(context.TODO(), allocations[0].SegmentID)
	assert.NotNil(t, segment)
	assert.EqualValues(t, segment.LastExpireTime, allocations[0].ExpireTime)
	assert.EqualValues(t, commonpb.SegmentState_Sealed, segment.State)
}

func TestSaveSegmentsToMetaWithSpecificSegments(t *testing.T) {
	paramtable.Init()
	mockAllocator := meta.NewMockAllocator(t)
	mt, err := meta.NewMemoryMeta(t)
	assert.NoError(t, err)

	schema := meta.NewTestSchema()
	collID, err := mockAllocator.AllocID(context.Background())
	assert.NoError(t, err)
	mt.AddCollection(&meta.CollectionInfo{ID: collID, Schema: schema})
	segmentManager, _ := NewSegmentManager(mt, mockAllocator)
	allocations, err := segmentManager.AllocSegment(context.Background(), collID, 0, "c1", 1000, storage.StorageV1)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(allocations))
	_, err = segmentManager.SealAllSegments(context.Background(), "c1", []int64{allocations[0].SegmentID})
	assert.NoError(t, err)
	segment := mt.GetHealthySegment(context.TODO(), allocations[0].SegmentID)
	assert.NotNil(t, segment)
	assert.EqualValues(t, segment.LastExpireTime, allocations[0].ExpireTime)
	assert.EqualValues(t, commonpb.SegmentState_Sealed, segment.State)
}

func TestDropSegment(t *testing.T) {
	paramtable.Init()
	mockAllocator := meta.NewMockAllocator(t)
	mt, err := meta.NewMemoryMeta(t)
	assert.NoError(t, err)

	schema := meta.NewTestSchema()
	collID, err := mockAllocator.AllocID(context.Background())
	assert.NoError(t, err)
	mt.AddCollection(&meta.CollectionInfo{ID: collID, Schema: schema})
	segmentManager, _ := NewSegmentManager(mt, mockAllocator)
	allocations, err := segmentManager.AllocSegment(context.Background(), collID, 100, "c1", 1000, storage.StorageV1)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(allocations))
	segID := allocations[0].SegmentID
	segment := mt.GetHealthySegment(context.TODO(), segID)
	assert.NotNil(t, segment)

	segmentManager.DropSegment(context.Background(), "c1", segID)
	segment = mt.GetHealthySegment(context.TODO(), segID)
	assert.NotNil(t, segment)
}

func TestAllocRowsLargerThanOneSegment(t *testing.T) {
	paramtable.Init()
	mockAllocator := meta.NewMockAllocator(t)
	mt, err := meta.NewMemoryMeta(t)
	assert.NoError(t, err)

	schema := meta.NewTestSchema()
	collID, err := mockAllocator.AllocID(context.Background())
	assert.NoError(t, err)
	mt.AddCollection(&meta.CollectionInfo{ID: collID, Schema: schema})

	mockPolicy := func(schema *schemapb.CollectionSchema) (int, error) {
		return 1, nil
	}
	segmentManager, _ := NewSegmentManager(mt, mockAllocator, withCalUpperLimitPolicy(mockPolicy))
	allocations, err := segmentManager.AllocSegment(context.TODO(), collID, 0, "c1", 2, storage.StorageV1)
	assert.NoError(t, err)
	assert.EqualValues(t, 2, len(allocations))
	assert.EqualValues(t, 1, allocations[0].NumOfRows)
	assert.EqualValues(t, 1, allocations[1].NumOfRows)
}

func TestExpireAllocation(t *testing.T) {
	paramtable.Init()
	mockAllocator := meta.NewMockAllocator(t)
	mt, err := meta.NewMemoryMeta(t)
	assert.NoError(t, err)

	schema := meta.NewTestSchema()
	collID, err := mockAllocator.AllocID(context.Background())
	assert.NoError(t, err)
	mt.AddCollection(&meta.CollectionInfo{ID: collID, Schema: schema})

	mockPolicy := func(schema *schemapb.CollectionSchema) (int, error) {
		return 10000000, nil
	}
	segmentManager, _ := NewSegmentManager(mt, mockAllocator, withCalUpperLimitPolicy(mockPolicy))
	// alloc 100 times and expire
	var maxts Timestamp
	var id int64 = -1
	for i := 0; i < 100; i++ {
		allocs, err := segmentManager.AllocSegment(context.TODO(), collID, 0, "ch1", 100, storage.StorageV1)
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

	segment := mt.GetHealthySegment(context.TODO(), id)
	assert.NotNil(t, segment)
	assert.EqualValues(t, 100, len(segment.Allocations))
	segmentManager.ExpireAllocations(context.TODO(), "ch1", maxts)
	segment = mt.GetHealthySegment(context.TODO(), id)
	assert.NotNil(t, segment)
	assert.EqualValues(t, 0, len(segment.Allocations))
}

func TestGetFlushableSegments(t *testing.T) {
	t.Run("get flushable segments between small interval", func(t *testing.T) {
		paramtable.Init()
		mockAllocator := meta.NewMockAllocator(t)
		mt, err := meta.NewMemoryMeta(t)
		assert.NoError(t, err)

		schema := meta.NewTestSchema()
		collID, err := mockAllocator.AllocID(context.Background())
		assert.NoError(t, err)
		mt.AddCollection(&meta.CollectionInfo{ID: collID, Schema: schema})
		segmentManager, _ := NewSegmentManager(mt, mockAllocator)
		allocations, err := segmentManager.AllocSegment(context.TODO(), collID, 0, "c1", 2, storage.StorageV1)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(allocations))

		ids, err := segmentManager.SealAllSegments(context.TODO(), "c1", nil)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(ids))
		assert.EqualValues(t, allocations[0].SegmentID, ids[0])

		mt.SetRowCount(allocations[0].SegmentID, 1)
		ids, err = segmentManager.GetFlushableSegments(context.TODO(), "c1", allocations[0].ExpireTime)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(ids))
		assert.EqualValues(t, allocations[0].SegmentID, ids[0])

		mt.SetLastFlushTime(allocations[0].SegmentID, time.Now())
		ids, err = segmentManager.GetFlushableSegments(context.TODO(), "c1", allocations[0].ExpireTime)
		assert.NoError(t, err)
		assert.Empty(t, ids)

		mt.SetLastFlushTime(allocations[0].SegmentID, time.Now().Local().Add(-1*paramtable.Get().DataCoordCfg.SegmentFlushInterval.GetAsDuration(time.Second)))
		ids, err = segmentManager.GetFlushableSegments(context.TODO(), "c1", allocations[0].ExpireTime)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(ids))
		assert.EqualValues(t, allocations[0].SegmentID, ids[0])

		mt.SetRowCount(allocations[0].SegmentID, 0)
		postions := make([]*msgpb.MsgPosition, 0)
		cpTs := allocations[0].ExpireTime + 1
		postions = append(postions, &msgpb.MsgPosition{
			ChannelName: "c1",
			MsgID:       []byte{1, 2, 3},
			Timestamp:   cpTs,
		})
		mt.UpdateChannelCheckpoints(context.TODO(), postions)
		segmentManager.CleanZeroSealedSegmentsOfChannel(context.TODO(), "c1", cpTs)
		ids, err = segmentManager.GetFlushableSegments(context.TODO(), "c1", allocations[0].ExpireTime)
		assert.NoError(t, err)
		assert.Empty(t, ids)
		assert.Nil(t, mt.GetHealthySegment(context.TODO(), allocations[0].SegmentID))
	})
}

func TestAllocationPool(t *testing.T) {
	t.Run("normal get&put", func(t *testing.T) {
		allocPool = sync.Pool{
			New: func() interface{} {
				return &meta.Allocation{}
			},
		}

		allo := getAllocation(100)
		assert.EqualValues(t, 100, allo.NumOfRows)
		assert.EqualValues(t, 0, allo.ExpireTime)
		assert.EqualValues(t, 0, allo.SegmentID)

		putAllocation(allo)
	})

	t.Run("put nil", func(t *testing.T) {
		var allo *meta.Allocation
		allocPool = sync.Pool{
			New: func() interface{} {
				return &meta.Allocation{}
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
				return &meta.Allocation{}
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
		meta     *meta.Meta
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
				meta: &meta.Meta{
					Segments: &meta.SegmentsInfo{
						Segments: map[int64]*meta.SegmentInfo{
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
				meta: &meta.Meta{
					Segments: &meta.SegmentsInfo{
						Segments: map[int64]*meta.SegmentInfo{
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
				Meta:            tt.fields.meta,
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
		meta     *meta.Meta
		segments []UniqueID
	}
	type args struct {
		channel string
		cpTs    Timestamp
	}

	mockCatalog := mocks.NewDataCoordCatalog(t)
	mockCatalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	mockCatalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	seg1 := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             1,
			PartitionID:    partitionID,
			InsertChannel:  "ch1",
			State:          commonpb.SegmentState_Sealed,
			NumOfRows:      1,
			LastExpireTime: 100,
		},
	}
	seg2 := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             2,
			PartitionID:    partitionID,
			InsertChannel:  "ch1",
			State:          commonpb.SegmentState_Sealed,
			NumOfRows:      0,
			LastExpireTime: 100,
		},
	}
	seg3 := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             3,
			PartitionID:    partitionID,
			InsertChannel:  "ch1",
			State:          commonpb.SegmentState_Sealed,
			LastExpireTime: 90,
		},
	}
	seg4 := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             4,
			PartitionID:    partitionID,
			InsertChannel:  "ch2",
			State:          commonpb.SegmentState_Growing,
			NumOfRows:      1,
			LastExpireTime: 100,
		},
	}
	newMetaFunc := func() *meta.Meta {
		return &meta.Meta{
			Catalog: mockCatalog,
			Segments: &meta.SegmentsInfo{
				Segments: map[int64]*meta.SegmentInfo{
					1: seg1,
					2: seg2,
					3: seg3,
					4: seg4,
				},
				SecondaryIndexes: meta.SegmentInfoIndexes{
					Coll2Segments: map[UniqueID]map[UniqueID]*meta.SegmentInfo{
						0: {1: seg1, 2: seg2, 3: seg3, 4: seg4},
					},
					Channel2Segments: map[string]map[UniqueID]*meta.SegmentInfo{
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
				Meta:            tt.fields.meta,
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

func TestDropSegmentOfPartition(t *testing.T) {
	paramtable.Init()
	mockAllocator := meta.NewMockAllocator(t)
	mt, err := meta.NewMemoryMeta(t)
	assert.NoError(t, err)

	schema := meta.NewTestSchema()
	collID, err := mockAllocator.AllocID(context.Background())
	assert.NoError(t, err)
	mt.AddCollection(&meta.CollectionInfo{ID: collID, Schema: schema})
	segmentManager, _ := NewSegmentManager(mt, mockAllocator)
	allocations, err := segmentManager.AllocSegment(context.Background(), collID, 100, "c1", 1000, storage.StorageV1)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(allocations))
	segID := allocations[0].SegmentID
	segment := mt.GetHealthySegment(context.TODO(), segID)
	assert.NotNil(t, segment)

	segmentManager.DropSegmentsOfPartition(context.Background(), "c1", []int64{100})
	segment = mt.GetHealthySegment(context.TODO(), segID)
	assert.NotNil(t, segment)
}

func TestAllocNewGrowingSegment_ManifestPath(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()
	mockAllocator := meta.NewMockAllocator(t)
	mt, err := meta.NewMemoryMeta(t)
	assert.NoError(t, err)
	segmentManager, _ := NewSegmentManager(mt, mockAllocator)

	schema := meta.NewTestSchema()
	collID, err := mockAllocator.AllocID(ctx)
	assert.NoError(t, err)
	mt.AddCollection(&meta.CollectionInfo{ID: collID, Schema: schema})

	t.Run("StorageV3 segment has manifest path", func(t *testing.T) {
		segID, err := mockAllocator.AllocID(ctx)
		assert.NoError(t, err)
		segment, err := segmentManager.AllocNewGrowingSegment(ctx, AllocNewGrowingSegmentRequest{
			CollectionID:         collID,
			PartitionID:          100,
			SegmentID:            segID,
			ChannelName:          "c1",
			StorageVersion:       storage.StorageV3,
			IsCreatedByStreaming: true,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, segment.ManifestPath)

		// Verify the manifest path can be unmarshalled
		basePath, ver, unmarshalErr := packed.UnmarshalManifestPath(segment.ManifestPath)
		assert.NoError(t, unmarshalErr)
		assert.NotEmpty(t, basePath)
		assert.Equal(t, packed.ManifestEarliest, ver)
	})

	t.Run("StorageV2 segment has no manifest path", func(t *testing.T) {
		segID, err := mockAllocator.AllocID(ctx)
		assert.NoError(t, err)
		segment, err := segmentManager.AllocNewGrowingSegment(ctx, AllocNewGrowingSegmentRequest{
			CollectionID:         collID,
			PartitionID:          100,
			SegmentID:            segID,
			ChannelName:          "c2",
			StorageVersion:       storage.StorageV2,
			IsCreatedByStreaming: true,
		})
		assert.NoError(t, err)
		assert.Empty(t, segment.ManifestPath)
	})
}
