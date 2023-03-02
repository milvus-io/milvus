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
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/util/paramtable"
)

func genTestCollectionSchema(dim int64, vectorType schemapb.DataType) *schemapb.CollectionSchema {
	floatVecFieldSchema := &schemapb.FieldSchema{
		FieldID:  100,
		Name:     "vec",
		DataType: vectorType,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
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

func Test_CompactSegBuff(t *testing.T) {
	channelSegments := make(map[UniqueID]*Segment)
	delBufferManager := &DelBufferManager{
		channel: &ChannelMeta{
			segments: channelSegments,
		},
		delMemorySize: 0,
		delBufHeap:    &PriorityQueue{},
	}
	//1. set compactTo and compactFrom
	compactedFromSegIDs := make([]UniqueID, 2)
	var segID1 UniqueID = 1111
	var segID2 UniqueID = 2222
	compactedFromSegIDs[0] = segID1
	compactedFromSegIDs[1] = segID2
	channelSegments[segID1] = &Segment{}
	channelSegments[segID2] = &Segment{}
	var compactedToSegID UniqueID = 3333
	channelSegments[compactedToSegID] = &Segment{}

	//2. set up deleteDataBuf for seg1 and seg2
	delDataBuf1 := newDelDataBuf()
	delDataBuf1.EntriesNum++
	delDataBuf1.updateStartAndEndPosition(nil, &msgpb.MsgPosition{Timestamp: 50})
	delBufferManager.Store(segID1, delDataBuf1)
	heap.Push(delBufferManager.delBufHeap, delDataBuf1.item)
	delDataBuf2 := newDelDataBuf()
	delDataBuf2.EntriesNum++
	delDataBuf2.updateStartAndEndPosition(nil, &msgpb.MsgPosition{Timestamp: 50})
	delBufferManager.Store(segID2, delDataBuf2)
	heap.Push(delBufferManager.delBufHeap, delDataBuf2.item)

	//3. test compact
	delBufferManager.CompactSegBuf(compactedToSegID, compactedFromSegIDs)

	//4. expect results in two aspects:
	//4.1 compactedFrom segments are removed from delBufferManager
	//4.2 compactedTo seg is set properly with correct entriesNum
	_, seg1Exist := delBufferManager.Load(segID1)
	_, seg2Exist := delBufferManager.Load(segID2)
	assert.False(t, seg1Exist)
	assert.False(t, seg2Exist)
	assert.Equal(t, int64(2), delBufferManager.GetEntriesNum(compactedToSegID))

	//5. test roll and evict (https://github.com/milvus-io/milvus/issues/20501)
	delBufferManager.channel.rollDeleteBuffer(compactedToSegID)
	_, segCompactedToExist := delBufferManager.Load(compactedToSegID)
	assert.False(t, segCompactedToExist)
	delBufferManager.channel.evictHistoryDeleteBuffer(compactedToSegID, &msgpb.MsgPosition{
		Timestamp: 100,
	})
	cp := delBufferManager.channel.getChannelCheckpoint(&msgpb.MsgPosition{
		Timestamp: 200,
	})
	assert.Equal(t, Timestamp(200), cp.Timestamp) // evict all buffer, use ttPos as cp
}
