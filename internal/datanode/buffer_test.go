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

package datanode

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func genTestCollectionSchema(dim int64, vectorType schemapb.DataType) *schemapb.CollectionSchema {
	floatVecFieldSchema := &schemapb.FieldSchema{
		FieldID:  100,
		Name:     "vec",
		DataType: vectorType,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: fmt.Sprintf("%d", dim),
			},
		},
	}
	schema := &schemapb.CollectionSchema{
		Name: "collection-0",
		Fields: []*schemapb.FieldSchema{
			floatVecFieldSchema,
		},
	}
	return schema
}

func TestBufferData(t *testing.T) {
	paramtable.Get().Save(Params.DataNodeCfg.FlushInsertBufferSize.Key, strconv.FormatInt(16*(1<<20), 10)) // 16 MB
	tests := []struct {
		isValid bool

		indim         int64
		expectedLimit int64
		vectorType    schemapb.DataType

		description string
	}{
		{true, 1, 4194304, schemapb.DataType_FloatVector, "Smallest of the DIM"},
		{true, 128, 32768, schemapb.DataType_FloatVector, "Normal DIM"},
		{true, 32768, 128, schemapb.DataType_FloatVector, "Largest DIM"},
		{true, 4096, 32768, schemapb.DataType_BinaryVector, "Normal binary"},
		{false, 0, 0, schemapb.DataType_FloatVector, "Illegal DIM"},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			idata, err := newBufferData(genTestCollectionSchema(test.indim, test.vectorType))

			if test.isValid {
				assert.NoError(t, err)
				assert.NotNil(t, idata)

				assert.Equal(t, test.expectedLimit, idata.limit)
				assert.Zero(t, idata.size)

				capacity := idata.effectiveCap()
				assert.Equal(t, test.expectedLimit, capacity)
			} else {
				assert.Error(t, err)
				assert.Nil(t, idata)
			}
		})
	}
}

func TestBufferData_updateTimeRange(t *testing.T) {
	paramtable.Get().Save(Params.DataNodeCfg.FlushInsertBufferSize.Key, strconv.FormatInt(16*(1<<20), 10)) // 16 MB

	type testCase struct {
		tag string

		trs        []TimeRange
		expectFrom Timestamp
		expectTo   Timestamp
	}

	cases := []testCase{
		{
			tag:        "no input range",
			expectTo:   0,
			expectFrom: math.MaxUint64,
		},
		{
			tag: "single range",
			trs: []TimeRange{
				{timestampMin: 100, timestampMax: 200},
			},
			expectFrom: 100,
			expectTo:   200,
		},
		{
			tag: "multiple range",
			trs: []TimeRange{
				{timestampMin: 150, timestampMax: 250},
				{timestampMin: 100, timestampMax: 200},
				{timestampMin: 50, timestampMax: 180},
			},
			expectFrom: 50,
			expectTo:   250,
		},
	}

	for _, tc := range cases {
		t.Run(tc.tag, func(t *testing.T) {
			bd, err := newBufferData(genTestCollectionSchema(16, schemapb.DataType_FloatVector))
			require.NoError(t, err)
			for _, tr := range tc.trs {
				bd.updateTimeRange(tr)
			}

			assert.Equal(t, tc.expectFrom, bd.tsFrom)
			assert.Equal(t, tc.expectTo, bd.tsTo)
		})
	}
}

func TestPriorityQueueString(t *testing.T) {
	item := &Item{
		segmentID:  0,
		memorySize: 1,
	}

	assert.Equal(t, "<segmentID=0, memorySize=1>", item.String())

	pq := &PriorityQueue{}
	heap.Push(pq, item)
	assert.Equal(t, "[<segmentID=0, memorySize=1>]", pq.String())
}

func Test_CompactSegBuff(t *testing.T) {
	channelSegments := make(map[UniqueID]*Segment)
	delBufferManager := &DeltaBufferManager{
		channel: &ChannelMeta{
			segments: channelSegments,
		},
		delBufHeap: &PriorityQueue{},
	}
	//1. set compactTo and compactFrom
	targetSeg := &Segment{segmentID: 3333}
	targetSeg.setType(datapb.SegmentType_Flushed)

	seg1 := &Segment{
		segmentID:   1111,
		compactedTo: targetSeg.segmentID,
	}
	seg1.setType(datapb.SegmentType_Compacted)

	seg2 := &Segment{
		segmentID:   2222,
		compactedTo: targetSeg.segmentID,
	}
	seg2.setType(datapb.SegmentType_Compacted)

	channelSegments[seg1.segmentID] = seg1
	channelSegments[seg2.segmentID] = seg2
	channelSegments[targetSeg.segmentID] = targetSeg

	//2. set up deleteDataBuf for seg1 and seg2
	delDataBuf1 := newDelDataBuf(seg1.segmentID)
	delDataBuf1.EntriesNum++
	delDataBuf1.updateStartAndEndPosition(nil, &msgpb.MsgPosition{Timestamp: 50})
	delBufferManager.updateMeta(seg1.segmentID, delDataBuf1)
	heap.Push(delBufferManager.delBufHeap, delDataBuf1.item)

	delDataBuf2 := newDelDataBuf(seg2.segmentID)
	delDataBuf2.EntriesNum++
	delDataBuf2.updateStartAndEndPosition(nil, &msgpb.MsgPosition{Timestamp: 50})
	delBufferManager.updateMeta(seg2.segmentID, delDataBuf2)
	heap.Push(delBufferManager.delBufHeap, delDataBuf2.item)

	//3. test compact
	delBufferManager.UpdateCompactedSegments()

	//4. expect results in two aspects:
	//4.1 compactedFrom segments are removed from delBufferManager
	//4.2 compactedTo seg is set properly with correct entriesNum
	_, seg1Exist := delBufferManager.Load(seg1.segmentID)
	_, seg2Exist := delBufferManager.Load(seg2.segmentID)
	assert.False(t, seg1Exist)
	assert.False(t, seg2Exist)
	assert.Equal(t, int64(2), delBufferManager.GetEntriesNum(targetSeg.segmentID))

	// test item of compactedToSegID is correct
	targetSegBuf, ok := delBufferManager.Load(targetSeg.segmentID)
	assert.True(t, ok)
	assert.NotNil(t, targetSegBuf.item)
	assert.Equal(t, targetSeg.segmentID, targetSegBuf.item.segmentID)

	//5. test roll and evict (https://github.com/milvus-io/milvus/issues/20501)
	delBufferManager.channel.rollDeleteBuffer(targetSeg.segmentID)
	_, segCompactedToExist := delBufferManager.Load(targetSeg.segmentID)
	assert.False(t, segCompactedToExist)
	delBufferManager.channel.evictHistoryDeleteBuffer(targetSeg.segmentID, &msgpb.MsgPosition{
		Timestamp: 100,
	})
	cp := delBufferManager.channel.getChannelCheckpoint(&msgpb.MsgPosition{
		Timestamp: 200,
	})
	assert.Equal(t, Timestamp(200), cp.Timestamp) // evict all buffer, use ttPos as cp
}

func TestUpdateCompactedSegments(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cm := storage.NewLocalChunkManager(storage.RootPath(deleteNodeTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	fm := NewRendezvousFlushManager(allocator.NewMockAllocator(t), cm, nil, func(*segmentFlushPack) {}, emptyFlushAndDropFunc)

	chanName := "datanode-test-FlowGraphDeletenode-showDelBuf"
	testPath := "/test/datanode/root/meta"
	assert.NoError(t, clearEtcd(testPath))
	Params.BaseTable.Save("etcd.rootPath", "/test/datanode/root")

	channel := ChannelMeta{
		segments: make(map[UniqueID]*Segment),
	}

	c := &nodeConfig{
		channel:      &channel,
		vChannelName: chanName,
	}
	delBufManager := &DeltaBufferManager{
		channel:    &channel,
		delBufHeap: &PriorityQueue{},
	}
	delNode, err := newDeleteNode(ctx, fm, delBufManager, make(chan string, 1), c)
	require.NoError(t, err)

	tests := []struct {
		description    string
		compactToExist bool

		compactedToIDs   []UniqueID
		compactedFromIDs []UniqueID

		expectedSegsRemain []UniqueID
	}{
		{"zero segments", false,
			[]UniqueID{}, []UniqueID{}, []UniqueID{}},
		{"segment no compaction", false,
			[]UniqueID{}, []UniqueID{}, []UniqueID{100, 101}},
		{"segment compacted", true,
			[]UniqueID{200}, []UniqueID{103}, []UniqueID{100, 101}},
		{"segment compacted 100>201", true,
			[]UniqueID{201}, []UniqueID{100}, []UniqueID{101, 201}},
		{"segment compacted 100+101>201", true,
			[]UniqueID{201, 201}, []UniqueID{100, 101}, []UniqueID{201}},
		{"segment compacted 100>201, 101>202", true,
			[]UniqueID{201, 202}, []UniqueID{100, 101}, []UniqueID{201, 202}},
		// false
		{"segment compacted 100>201", false,
			[]UniqueID{201}, []UniqueID{100}, []UniqueID{101}},
		{"segment compacted 100+101>201", false,
			[]UniqueID{201, 201}, []UniqueID{100, 101}, []UniqueID{}},
		{"segment compacted 100>201, 101>202", false,
			[]UniqueID{201, 202}, []UniqueID{100, 101}, []UniqueID{}},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			if test.compactToExist {
				for _, segID := range test.compactedToIDs {
					seg := Segment{
						segmentID: segID,
						numRows:   10,
					}
					seg.setType(datapb.SegmentType_Flushed)
					channel.segments[segID] = &seg
				}
			} else { // clear all segments in channel
				channel.segments = make(map[UniqueID]*Segment)
			}

			for i, segID := range test.compactedFromIDs {
				seg := Segment{
					segmentID:   segID,
					compactedTo: test.compactedToIDs[i],
				}
				seg.setType(datapb.SegmentType_Compacted)
				channel.segments[segID] = &seg
			}

			delNode.delBufferManager.UpdateCompactedSegments()

			for _, remain := range test.expectedSegsRemain {
				delNode.channel.hasSegment(remain, true)
			}
		})
	}
}
