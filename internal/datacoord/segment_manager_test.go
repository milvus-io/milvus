// Copyright (C) 2019-2020 Zilliz. All rights reserved.//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.
package datacoord

import (
	"context"
	"math"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"

	"github.com/stretchr/testify/assert"
)

func TestAllocSegment(t *testing.T) {
	ctx := context.Background()
	Params.Init()
	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)
	segmentManager := newSegmentManager(meta, mockAllocator)

	schema := newTestSchema()
	collID, err := mockAllocator.allocID()
	assert.Nil(t, err)
	meta.AddCollection(&datapb.CollectionInfo{ID: collID, Schema: schema})
	cases := []struct {
		collectionID UniqueID
		partitionID  UniqueID
		channelName  string
		requestRows  int64
		expectResult bool
	}{
		{collID, 100, "c1", 100, true},
		{collID, 100, "c1", math.MaxInt64, false},
	}
	for _, c := range cases {
		id, count, expireTime, err := segmentManager.AllocSegment(ctx, c.collectionID, c.partitionID, c.channelName, c.requestRows)
		if c.expectResult {
			assert.Nil(t, err)
			assert.EqualValues(t, c.requestRows, count)
			assert.NotEqualValues(t, 0, id)
			assert.NotEqualValues(t, 0, expireTime)
		} else {
			assert.NotNil(t, err)
		}
	}
}

func TestLoadSegmentsFromMeta(t *testing.T) {
	Params.Init()
	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)

	schema := newTestSchema()
	collID, err := mockAllocator.allocID()
	assert.Nil(t, err)
	meta.AddCollection(&datapb.CollectionInfo{ID: collID, Schema: schema})

	sealedSegment := &datapb.SegmentInfo{
		ID:             1,
		CollectionID:   collID,
		PartitionID:    0,
		InsertChannel:  "",
		State:          commonpb.SegmentState_Sealed,
		MaxRowNum:      100,
		LastExpireTime: 1000,
	}
	growingSegment := &datapb.SegmentInfo{
		ID:             2,
		CollectionID:   collID,
		PartitionID:    0,
		InsertChannel:  "",
		State:          commonpb.SegmentState_Growing,
		MaxRowNum:      100,
		LastExpireTime: 1000,
	}
	flushedSegment := &datapb.SegmentInfo{
		ID:             3,
		CollectionID:   collID,
		PartitionID:    0,
		InsertChannel:  "",
		State:          commonpb.SegmentState_Flushed,
		MaxRowNum:      100,
		LastExpireTime: 1000,
	}
	err = meta.AddSegment(NewSegmentInfo(sealedSegment))
	assert.Nil(t, err)
	err = meta.AddSegment(NewSegmentInfo(growingSegment))
	assert.Nil(t, err)
	err = meta.AddSegment(NewSegmentInfo(flushedSegment))
	assert.Nil(t, err)

	segmentManager := newSegmentManager(meta, mockAllocator)
	segments := segmentManager.segments
	assert.EqualValues(t, 2, len(segments))
}

func TestSaveSegmentsToMeta(t *testing.T) {
	Params.Init()
	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)

	schema := newTestSchema()
	collID, err := mockAllocator.allocID()
	assert.Nil(t, err)
	meta.AddCollection(&datapb.CollectionInfo{ID: collID, Schema: schema})
	segmentManager := newSegmentManager(meta, mockAllocator)
	segID, _, expireTs, err := segmentManager.AllocSegment(context.Background(), collID, 0, "c1", 1000)
	assert.Nil(t, err)
	_, err = segmentManager.SealAllSegments(context.Background(), collID)
	assert.Nil(t, err)
	segment := meta.GetSegment(segID)
	assert.NotNil(t, segment)
	assert.EqualValues(t, segment.LastExpireTime, expireTs)
	assert.EqualValues(t, commonpb.SegmentState_Sealed, segment.State)
}

func TestDropSegment(t *testing.T) {
	Params.Init()
	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)

	schema := newTestSchema()
	collID, err := mockAllocator.allocID()
	assert.Nil(t, err)
	meta.AddCollection(&datapb.CollectionInfo{ID: collID, Schema: schema})
	segmentManager := newSegmentManager(meta, mockAllocator)
	segID, _, _, err := segmentManager.AllocSegment(context.Background(), collID, 0, "c1", 1000)
	assert.Nil(t, err)
	segment := meta.GetSegment(segID)
	assert.NotNil(t, segment)

	segmentManager.DropSegment(context.Background(), segID)
	segment = meta.GetSegment(segID)
	assert.NotNil(t, segment)
}
