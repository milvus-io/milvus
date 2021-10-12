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
package datacoord

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/stretchr/testify/assert"
)

func TestMeta_Basic(t *testing.T) {
	const collID = UniqueID(0)
	const partID0 = UniqueID(100)
	const partID1 = UniqueID(101)
	const channelName = "c1"
	ctx := context.Background()

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
		meta.AddCollection(collInfo)
		// check has collection
		collInfo := meta.GetCollection(collID)
		assert.NotNil(t, collInfo)

		// check partition info
		assert.EqualValues(t, collID, collInfo.ID)
		assert.EqualValues(t, testSchema, collInfo.Schema)
		assert.EqualValues(t, 2, len(collInfo.Partitions))
		assert.EqualValues(t, partID0, collInfo.Partitions[0])
		assert.EqualValues(t, partID1, collInfo.Partitions[1])
	})

	t.Run("Test Segment", func(t *testing.T) {
		meta.AddCollection(collInfoWoPartition)
		// create seg0 for partition0, seg0/seg1 for partition1
		segID0_0, err := mockAllocator.allocID(ctx)
		assert.Nil(t, err)
		segInfo0_0 := buildSegment(collID, partID0, segID0_0, channelName)
		segID1_0, err := mockAllocator.allocID(ctx)
		assert.Nil(t, err)
		segInfo1_0 := buildSegment(collID, partID1, segID1_0, channelName)
		segID1_1, err := mockAllocator.allocID(ctx)
		assert.Nil(t, err)
		segInfo1_1 := buildSegment(collID, partID1, segID1_1, channelName)

		// check AddSegment
		err = meta.AddSegment(segInfo0_0)
		assert.Nil(t, err)
		err = meta.AddSegment(segInfo1_0)
		assert.Nil(t, err)
		err = meta.AddSegment(segInfo1_1)
		assert.Nil(t, err)

		// check GetSegment
		info0_0 := meta.GetSegment(segID0_0)
		assert.NotNil(t, info0_0)
		assert.True(t, proto.Equal(info0_0, segInfo0_0))
		info1_0 := meta.GetSegment(segID1_0)
		assert.NotNil(t, info1_0)
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

		err = meta.SetState(segID0_0, commonpb.SegmentState_Sealed)
		assert.Nil(t, err)
		err = meta.SetState(segID0_0, commonpb.SegmentState_Flushed)
		assert.Nil(t, err)

		info0_0 = meta.GetSegment(segID0_0)
		assert.NotNil(t, info0_0)
		assert.EqualValues(t, commonpb.SegmentState_Flushed, info0_0.State)
	})

	t.Run("Test segment with kv fails", func(t *testing.T) {
		// inject error for `Save`
		memoryKV := memkv.NewMemoryKV()
		fkv := &saveFailKV{TxnKV: memoryKV}
		meta, err := newMeta(fkv)
		assert.Nil(t, err)

		err = meta.AddSegment(NewSegmentInfo(&datapb.SegmentInfo{}))
		assert.NotNil(t, err)

		fkv2 := &removeFailKV{TxnKV: memoryKV}
		meta, err = newMeta(fkv2)
		assert.Nil(t, err)
		// nil, since no segment yet
		err = meta.DropSegment(0)
		assert.Nil(t, err)
		// nil, since Save error not injected
		err = meta.AddSegment(NewSegmentInfo(&datapb.SegmentInfo{}))
		assert.Nil(t, err)
		// error injected
		err = meta.DropSegment(0)
		assert.NotNil(t, err)
	})

	t.Run("Test GetCount", func(t *testing.T) {
		const rowCount0 = 100
		const rowCount1 = 300

		// no segment
		nums := meta.GetNumRowsOfCollection(collID)
		assert.EqualValues(t, 0, nums)

		// add seg1 with 100 rows
		segID0, err := mockAllocator.allocID(ctx)
		assert.Nil(t, err)
		segInfo0 := buildSegment(collID, partID0, segID0, channelName)
		segInfo0.NumOfRows = rowCount0
		err = meta.AddSegment(segInfo0)
		assert.Nil(t, err)

		// add seg2 with 300 rows
		segID1, err := mockAllocator.allocID(ctx)
		assert.Nil(t, err)
		segInfo1 := buildSegment(collID, partID0, segID1, channelName)
		segInfo1.NumOfRows = rowCount1
		err = meta.AddSegment(segInfo1)
		assert.Nil(t, err)

		// check partition/collection statistics
		nums = meta.GetNumRowsOfPartition(collID, partID0)
		assert.EqualValues(t, (rowCount0 + rowCount1), nums)
		nums = meta.GetNumRowsOfCollection(collID)
		assert.EqualValues(t, (rowCount0 + rowCount1), nums)
	})
}

func TestGetUnFlushedSegments(t *testing.T) {
	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)
	s1 := &datapb.SegmentInfo{
		ID:           0,
		CollectionID: 0,
		PartitionID:  0,
		State:        commonpb.SegmentState_Growing,
	}
	err = meta.AddSegment(NewSegmentInfo(s1))
	assert.Nil(t, err)
	s2 := &datapb.SegmentInfo{
		ID:           1,
		CollectionID: 0,
		PartitionID:  0,
		State:        commonpb.SegmentState_Flushed,
	}
	err = meta.AddSegment(NewSegmentInfo(s2))
	assert.Nil(t, err)

	segments := meta.GetUnFlushedSegments()
	assert.Nil(t, err)

	assert.EqualValues(t, 1, len(segments))
	assert.EqualValues(t, 0, segments[0].ID)
	assert.NotEqualValues(t, commonpb.SegmentState_Flushed, segments[0].State)
}

func TestUpdateFlushSegmentsInfo(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		meta, err := newMeta(memkv.NewMemoryKV())
		assert.Nil(t, err)

		segment1 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Growing, Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"binlog0"}}}}}
		err = meta.AddSegment(segment1)
		assert.Nil(t, err)

		err = meta.UpdateFlushSegmentsInfo(1, true, []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"binlog1"}}},
			[]*datapb.CheckPoint{{SegmentID: 1, NumOfRows: 10}}, []*datapb.SegmentStartPosition{{SegmentID: 1, StartPosition: &internalpb.MsgPosition{MsgID: []byte{1, 2, 3}}}})
		assert.Nil(t, err)

		updated := meta.GetSegment(1)
		expected := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, State: commonpb.SegmentState_Flushing, NumOfRows: 10,
			StartPosition: &internalpb.MsgPosition{MsgID: []byte{1, 2, 3}},
			Binlogs:       []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"binlog0", "binlog1"}}},
		}}
		assert.EqualValues(t, expected, updated)
	})

	t.Run("update non-existed segment", func(t *testing.T) {
		meta, err := newMeta(memkv.NewMemoryKV())
		assert.Nil(t, err)

		err = meta.UpdateFlushSegmentsInfo(1, false, nil, nil, nil)
		assert.Nil(t, err)
	})

	t.Run("update checkpoints and start position of non existed segment", func(t *testing.T) {
		meta, err := newMeta(memkv.NewMemoryKV())
		assert.Nil(t, err)

		segment1 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Growing}}
		err = meta.AddSegment(segment1)
		assert.Nil(t, err)

		err = meta.UpdateFlushSegmentsInfo(1, false, nil, []*datapb.CheckPoint{{SegmentID: 2, NumOfRows: 10}},
			[]*datapb.SegmentStartPosition{{SegmentID: 2, StartPosition: &internalpb.MsgPosition{MsgID: []byte{1, 2, 3}}}})
		assert.Nil(t, err)
		assert.Nil(t, meta.GetSegment(2))
	})

	t.Run("test save etcd failed", func(t *testing.T) {
		kv := memkv.NewMemoryKV()
		failedKv := &saveFailKV{kv}
		meta, err := newMeta(failedKv)
		assert.Nil(t, err)

		segmentInfo := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:        1,
				NumOfRows: 0,
				State:     commonpb.SegmentState_Growing,
			},
		}
		meta.segments.SetSegment(1, segmentInfo)

		err = meta.UpdateFlushSegmentsInfo(1, true, []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"binlog"}}},
			[]*datapb.CheckPoint{{SegmentID: 1, NumOfRows: 10}}, []*datapb.SegmentStartPosition{{SegmentID: 1, StartPosition: &internalpb.MsgPosition{MsgID: []byte{1, 2, 3}}}})
		assert.NotNil(t, err)
		assert.Equal(t, "mocked fail", err.Error())
		segmentInfo = meta.GetSegment(1)
		assert.EqualValues(t, 0, segmentInfo.NumOfRows)
		assert.Equal(t, commonpb.SegmentState_Growing, segmentInfo.State)
		assert.Nil(t, segmentInfo.Binlogs)
		assert.Nil(t, segmentInfo.StartPosition)
	})
}
