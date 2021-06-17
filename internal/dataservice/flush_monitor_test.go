// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package dataservice

import (
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/stretchr/testify/assert"
)

func TestFlushMonitor(t *testing.T) {
	const collID = UniqueID(0)
	const partID0 = UniqueID(100)
	const partID1 = UniqueID(101)
	const channelName = "c1"

	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)

	testSchema := newTestSchema()
	collInfo := &datapb.CollectionInfo{
		ID:         collID,
		Schema:     testSchema,
		Partitions: []UniqueID{partID0, partID1},
	}

	meta.AddCollection(collInfo)

	// create seg0 for partition0, seg0/seg1 for partition1
	segID0_0, err := mockAllocator.allocID()
	assert.Nil(t, err)
	segInfo0_0, err := BuildSegment(collID, partID0, segID0_0, channelName)
	assert.Nil(t, err)
	segID1_0, err := mockAllocator.allocID()
	assert.Nil(t, err)
	segInfo1_0, err := BuildSegment(collID, partID1, segID1_0, channelName)
	assert.Nil(t, err)
	segID1_1, err := mockAllocator.allocID()
	assert.Nil(t, err)
	segInfo1_1, err := BuildSegment(collID, partID1, segID1_1, channelName)
	assert.Nil(t, err)

	// check AddSegment
	err = meta.AddSegment(segInfo0_0)
	assert.Nil(t, err)
	err = meta.AddSegment(segInfo0_0)
	assert.NotNil(t, err)
	err = meta.AddSegment(segInfo1_0)
	assert.Nil(t, err)
	err = meta.AddSegment(segInfo1_1)
	assert.Nil(t, err)

	t.Run("Test empty flush monitor", func(t *testing.T) {
		fm := emptyFlushMonitor(meta)
		ids := fm.CheckSegments([]*datapb.SegmentInfo{})
		assert.Equal(t, 0, len(ids))

		ids = fm.CheckChannels([]string{channelName}, nil)
		assert.Equal(t, 0, len(ids))
	})

	t.Run("Test custom segment policy", func(t *testing.T) {
		fm := emptyFlushMonitor(meta)
		fm.segmentPolicy = estSegmentSizePolicy(1024*1024, 1024*1024*2) // row size 1Mib Limit 2 MB
		segID3Rows, err := mockAllocator.allocID()
		assert.Nil(t, err)
		segInfo3Rows, err := BuildSegment(collID, partID1, segID3Rows, channelName)
		segInfo3Rows.NumOfRows = 3
		assert.Nil(t, err)

		ids := fm.CheckSegments([]*datapb.SegmentInfo{segInfo3Rows})
		if assert.Equal(t, 1, len(ids)) {
			assert.Equal(t, segID3Rows, ids[0])
		}
	})

	t.Run("Test custom channel policy", func(t *testing.T) {
		const channelName2 = `ch2`
		fm := emptyFlushMonitor(meta)
		fm.channelPolicy = channelSizeEpochPolicy(100, uint64(time.Hour))

		for i := 0; i < 100; i++ {
			segID, err := mockAllocator.allocID()
			assert.Nil(t, err)
			seg, err := BuildSegment(collID, partID0, segID, channelName2)
			assert.Nil(t, err)
			seg.DmlPosition = &internalpb.MsgPosition{
				Timestamp: uint64(i + 1),
			}
			meta.AddSegment(seg)
		}

		ids := fm.CheckChannels([]string{channelName2}, nil)
		assert.Equal(t, 0, len(ids))

		exSegID, err := mockAllocator.allocID()
		assert.Nil(t, err)
		seg, err := BuildSegment(collID, partID0, exSegID, channelName2)
		assert.Nil(t, err)
		seg.DmlPosition = &internalpb.MsgPosition{
			Timestamp: uint64(0), // the oldest
		}
		meta.AddSegment(seg)

		ids = fm.CheckChannels([]string{channelName2}, nil)
		if assert.Equal(t, 1, len(ids)) {
			assert.Equal(t, exSegID, ids[0])
		}

		ids = fm.CheckChannels([]string{channelName2}, &internalpb.MsgPosition{Timestamp: uint64(time.Hour + 5)})
		assert.Equal(t, 5, len(ids))
	})
}
