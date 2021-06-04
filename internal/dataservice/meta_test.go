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

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/stretchr/testify/assert"
)

func TestMeta_Basic(t *testing.T) {
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
	collInfoWoPartition := &datapb.CollectionInfo{
		ID:         collID,
		Schema:     testSchema,
		Partitions: []UniqueID{},
	}

	t.Run("Test Collection", func(t *testing.T) {
		// check add collection
		err = meta.AddCollection(collInfo)
		assert.Nil(t, err)

		// check add existed collection
		err = meta.AddCollection(collInfo)
		assert.NotNil(t, err)

		// check has collection
		has := meta.HasCollection(collID)
		assert.True(t, has)

		// check partition info
		collInfo, err = meta.GetCollection(collID)
		assert.Nil(t, err)
		assert.EqualValues(t, collID, collInfo.ID)
		assert.EqualValues(t, testSchema, collInfo.Schema)
		assert.EqualValues(t, 2, len(collInfo.Partitions))
		assert.EqualValues(t, partID0, collInfo.Partitions[0])
		assert.EqualValues(t, partID1, collInfo.Partitions[1])

		// check drop collection
		err = meta.DropCollection(collID)
		assert.Nil(t, err)
		has = meta.HasCollection(collID)
		assert.False(t, has)
		_, err = meta.GetCollection(collID)
		assert.NotNil(t, err)
	})

	t.Run("Test Partition", func(t *testing.T) {
		err = meta.AddCollection(collInfoWoPartition)
		assert.Nil(t, err)

		// check add partition
		err = meta.AddPartition(collID, partID0)
		assert.Nil(t, err)
		err = meta.AddPartition(collID, partID1)
		assert.Nil(t, err)
		exist0 := meta.HasPartition(collID, partID0)
		assert.True(t, exist0)
		exist1 := meta.HasPartition(collID, partID1)
		assert.True(t, exist1)

		// check add existed partition
		err = meta.AddPartition(collID, partID0)
		assert.NotNil(t, err)

		// check GetCollection
		collInfo, err = meta.GetCollection(collID)
		assert.Nil(t, err)
		assert.EqualValues(t, 2, len(collInfo.Partitions))
		assert.Contains(t, collInfo.Partitions, partID0)
		assert.Contains(t, collInfo.Partitions, partID1)

		// check DropPartition
		err = meta.DropPartition(collID, partID0)
		assert.Nil(t, err)
		exist0 = meta.HasPartition(collID, partID0)
		assert.False(t, exist0)
		exist1 = meta.HasPartition(collID, partID1)
		assert.True(t, exist1)

		// check DropPartition twice
		err = meta.DropPartition(collID, partID0)
		assert.NotNil(t, err)

		err = meta.DropCollection(collID)
		assert.Nil(t, err)
	})

	t.Run("Test Segment", func(t *testing.T) {
		err = meta.AddCollection(collInfoWoPartition)
		assert.Nil(t, err)
		err = meta.AddPartition(collID, partID0)
		assert.Nil(t, err)

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

		// check GetSegment
		info0_0, err := meta.GetSegment(segID0_0)
		assert.Nil(t, err)
		assert.True(t, proto.Equal(info0_0, segInfo0_0))
		info1_0, err := meta.GetSegment(segID1_0)
		assert.Nil(t, err)
		assert.True(t, proto.Equal(info1_0, segInfo1_0))

		// check GetSegmentsOfCollection
		segIDs := meta.GetSegmentsOfCollection(collID)
		assert.EqualValues(t, 3, len(segIDs))
		assert.Contains(t, segIDs, segID0_0)
		assert.Contains(t, segIDs, segID1_0)
		assert.Contains(t, segIDs, segID1_1)

		// check GetSegmentsOfPartition
		segIDs = meta.GetSegmentsOfPartition(collID, partID0)
		assert.EqualValues(t, 1, len(segIDs))
		assert.Contains(t, segIDs, segID0_0)
		segIDs = meta.GetSegmentsOfPartition(collID, partID1)
		assert.EqualValues(t, 2, len(segIDs))
		assert.Contains(t, segIDs, segID1_0)
		assert.Contains(t, segIDs, segID1_1)

		// check DropSegment
		err = meta.DropSegment(segID1_0)
		assert.Nil(t, err)
		segIDs = meta.GetSegmentsOfPartition(collID, partID1)
		assert.EqualValues(t, 1, len(segIDs))
		assert.Contains(t, segIDs, segID1_1)

		err = meta.SealSegment(segID0_0)
		assert.Nil(t, err)
		err = meta.FlushSegment(segID0_0)
		assert.Nil(t, err)

		info0_0, err = meta.GetSegment(segID0_0)
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.SegmentState_Flushed, info0_0.State)

		err = meta.DropPartition(collID, partID0)
		assert.Nil(t, err)
		err = meta.DropCollection(collID)
		assert.Nil(t, err)
	})

	t.Run("Test GetCount", func(t *testing.T) {
		const rowCount0 = 100
		const rowCount1 = 300
		const dim = 1024

		// no segment
		nums, err := meta.GetNumRowsOfCollection(collID)
		assert.Nil(t, err)
		assert.EqualValues(t, 0, nums)

		// add seg1 with 100 rows
		segID0, err := mockAllocator.allocID()
		assert.Nil(t, err)
		segInfo0, err := BuildSegment(collID, partID0, segID0, channelName)
		assert.Nil(t, err)
		segInfo0.NumOfRows = rowCount0
		err = meta.AddSegment(segInfo0)
		assert.Nil(t, err)

		// add seg2 with 300 rows
		segID1, err := mockAllocator.allocID()
		assert.Nil(t, err)
		segInfo1, err := BuildSegment(collID, partID0, segID1, channelName)
		assert.Nil(t, err)
		segInfo1.NumOfRows = rowCount1
		err = meta.AddSegment(segInfo1)
		assert.Nil(t, err)

		// check partition/collection statistics
		nums, err = meta.GetNumRowsOfPartition(collID, partID0)
		assert.Nil(t, err)
		assert.EqualValues(t, (rowCount0 + rowCount1), nums)
		nums, err = meta.GetNumRowsOfCollection(collID)
		assert.Nil(t, err)
		assert.EqualValues(t, (rowCount0 + rowCount1), nums)
	})

	t.Run("Test Invalid", func(t *testing.T) {
		collIDInvalid := UniqueID(10000)
		partIDInvalid := UniqueID(10001)
		segIDInvalid := UniqueID(10002)

		// check drop non-exist collection
		err = meta.DropCollection(collID)
		assert.NotNil(t, err)

		// add partition wo collection
		err = meta.AddPartition(collID, partID0)
		assert.NotNil(t, err)

		// has partition wo collection
		exist := meta.HasPartition(collID, partID0)
		assert.False(t, exist)

		err = meta.AddCollection(collInfo)
		assert.Nil(t, err)

		// check drop non-exist partition
		err = meta.DropPartition(collIDInvalid, partID0)
		assert.NotNil(t, err)
		err = meta.DropPartition(collID, partIDInvalid)
		assert.NotNil(t, err)

		// check drop non-exist segment
		err = meta.DropSegment(segIDInvalid)
		assert.NotNil(t, err)

		// check seal non-exist segment
		err = meta.SealSegment(segIDInvalid)
		assert.NotNil(t, err)

		// check flush non-exist segment
		err = meta.FlushSegment(segIDInvalid)
		assert.NotNil(t, err)

		err = meta.DropCollection(collID)
		assert.Nil(t, err)
	})
}

func TestGetUnFlushedSegments(t *testing.T) {
	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)
	err = meta.AddSegment(&datapb.SegmentInfo{
		ID:           0,
		CollectionID: 0,
		PartitionID:  0,
		State:        commonpb.SegmentState_Growing,
	})
	assert.Nil(t, err)
	err = meta.AddSegment(&datapb.SegmentInfo{
		ID:           1,
		CollectionID: 0,
		PartitionID:  0,
		State:        commonpb.SegmentState_Flushed,
	})
	assert.Nil(t, err)

	segments := meta.GetUnFlushedSegments()
	assert.Nil(t, err)

	assert.EqualValues(t, 1, len(segments))
	assert.EqualValues(t, 0, segments[0].ID)
	assert.NotEqualValues(t, commonpb.SegmentState_Flushed, segments[0].State)
}
