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

package datanode

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

func TestFlowGraph_DDNode_newDDNode(te *testing.T) {
	tests := []struct {
		inCollID UniqueID

		inFlushedSegs        []UniqueID
		inUnFlushedSegID     UniqueID
		inUnFlushedChannelTs Timestamp

		description string
	}{
		{UniqueID(1), []UniqueID{100, 101, 102}, 200, 666666,
			"Input VchannelInfo with 3 flushed segs and 1 unflushed seg"},
		{UniqueID(2), []UniqueID{103}, 200, 666666,
			"Input VchannelInfo with 1 flushed seg and 1 unflushed seg"},
		{UniqueID(3), []UniqueID{}, 200, 666666,
			"Input VchannelInfo with 0 flushed segs and 1 unflushed seg"},
		{UniqueID(3), []UniqueID{104}, 0, 0,
			"Input VchannelInfo with 1 flushed seg and empty unflushed seg"},
	}

	for _, test := range tests {
		te.Run(test.description, func(t *testing.T) {
			di := &datapb.SegmentInfo{}

			if test.inUnFlushedSegID != 0 {
				di.ID = test.inUnFlushedSegID
				di.DmlPosition = &internalpb.MsgPosition{Timestamp: test.inUnFlushedChannelTs}
			}

			ddNode := newDDNode(
				make(chan UniqueID),
				test.inCollID,
				&datapb.VchannelInfo{
					FlushedSegments:   test.inFlushedSegs,
					UnflushedSegments: []*datapb.SegmentInfo{di},
				},
			)

			assert.Equal(t, "ddNode", ddNode.Name())
			assert.Equal(t, test.inCollID, ddNode.collectionID)
			assert.Equal(t, len(test.inFlushedSegs), len(ddNode.flushedSegments))
			assert.ElementsMatch(t, test.inFlushedSegs, ddNode.flushedSegments)

			si, ok := ddNode.segID2SegInfo.Load(test.inUnFlushedSegID)
			assert.True(t, ok)
			assert.Equal(t, test.inUnFlushedSegID, si.(*datapb.SegmentInfo).GetID())
			assert.Equal(t, test.inUnFlushedChannelTs, si.(*datapb.SegmentInfo).GetDmlPosition().GetTimestamp())
		})
	}
}

func TestFlowGraph_DDNode_Operate(to *testing.T) {
	to.Run("Test DDNode Operate DropCollection Msg", func(te *testing.T) {
		// invalid inputs
		invalidInTests := []struct {
			in          []Msg
			description string
		}{
			{[]Msg{},
				"Invalid input length == 0"},
			{[]Msg{&insertMsg{}, &insertMsg{}, &insertMsg{}},
				"Invalid input length == 3"},
			{[]Msg{&insertMsg{}},
				"Invalid input length == 1 but input message is not msgStreamMsg"},
		}

		for _, test := range invalidInTests {
			te.Run(test.description, func(t *testing.T) {
				ddn := ddNode{}
				rt := ddn.Operate(test.in)
				assert.Empty(t, rt)
			})
		}

		// valid inputs
		tests := []struct {
			ddnClearSignal chan UniqueID
			ddnCollID      UniqueID

			msgCollID     UniqueID
			expectedChlen int

			description string
		}{
			{make(chan UniqueID, 1), 1, 1, 1,
				"DropCollectionMsg collID == ddNode collID"},
			{make(chan UniqueID, 1), 1, 2, 0,
				"DropCollectionMsg collID != ddNode collID"},
		}

		for _, test := range tests {
			te.Run(test.description, func(t *testing.T) {
				ddn := ddNode{
					clearSignal:  test.ddnClearSignal,
					collectionID: test.ddnCollID,
				}

				var createCollMsg msgstream.TsMsg = &msgstream.DropCollectionMsg{
					DropCollectionRequest: internalpb.DropCollectionRequest{
						Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
						CollectionID: test.msgCollID,
					},
				}
				tsMessages := []msgstream.TsMsg{createCollMsg}
				var msgStreamMsg Msg = flowgraph.GenerateMsgStreamMsg(tsMessages, 0, 0, nil, nil)

				rt := ddn.Operate([]Msg{msgStreamMsg})
				assert.Equal(t, test.expectedChlen, len(test.ddnClearSignal))

				if test.ddnCollID == test.msgCollID {
					assert.Empty(t, rt)
				} else {
					assert.NotEmpty(t, rt)
				}
			})
		}
	})

	to.Run("Test DDNode Operate Insert Msg", func(te *testing.T) {
		tests := []struct {
			ddnCollID   UniqueID
			inMsgCollID UniqueID

			MsgEndTs  Timestamp
			threshold Timestamp

			ddnFlushedSegment UniqueID
			inMsgSegID        UniqueID

			expectedRtLen int
			description   string
		}{
			{1, 1, 2000, 3000, 100, 100, 0,
				"MsgEndTs(2000) < threshold(3000), inMsgSegID(100) IN ddnFlushedSeg {100}"},
			{1, 1, 2000, 3000, 100, 200, 1,
				"MsgEndTs(2000) < threshold(3000), inMsgSegID(200) NOT IN ddnFlushedSeg {100}"},
			{1, 1, 4000, 3000, 100, 101, 1,
				"Seg 101, MsgEndTs(4000) > FilterThreshold(3000)"},
			{1, 1, 4000, 3000, 100, 200, 1,
				"Seg 200, MsgEndTs(4000) > FilterThreshold(3000)"},
			{1, 2, 4000, 3000, 100, 100, 0,
				"inMsgCollID(2) != ddnCollID"},
		}

		for _, test := range tests {
			te.Run(test.description, func(t *testing.T) {
				// Prepare ddNode states
				ddn := ddNode{
					flushedSegments: []UniqueID{test.ddnFlushedSegment},
					collectionID:    test.ddnCollID,
				}
				FilterThreshold = test.threshold

				// Prepare insert messages
				var iMsg msgstream.TsMsg = &msgstream.InsertMsg{
					BaseMsg: msgstream.BaseMsg{EndTimestamp: test.MsgEndTs},
					InsertRequest: internalpb.InsertRequest{
						Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
						CollectionID: test.inMsgCollID,
						SegmentID:    test.inMsgSegID,
					},
				}
				tsMessages := []msgstream.TsMsg{iMsg}
				var msgStreamMsg Msg = flowgraph.GenerateMsgStreamMsg(tsMessages, 0, 0, nil, nil)

				// Test
				rt := ddn.Operate([]Msg{msgStreamMsg})
				assert.Equal(t, test.expectedRtLen, len(rt[0].(*insertMsg).insertMessages))
			})
		}
	})

}

func TestFlowGraph_DDNode_filterMessages(te *testing.T) {
	tests := []struct {
		ddnFlushedSegments []UniqueID
		ddnSegID2Ts        map[UniqueID]Timestamp

		inMsgSegID    UniqueID
		inMsgSegEntTs Timestamp
		expectedOut   bool

		description string
	}{
		{[]UniqueID{1, 2, 3}, map[UniqueID]Timestamp{4: 1000, 5: 2000}, 1, 1500, true,
			"Seg 1 in flushedSegs {1, 2, 3}"},
		{[]UniqueID{1, 2, 3}, map[UniqueID]Timestamp{4: 1000, 5: 2000}, 2, 1500, true,
			"Seg 2 in flushedSegs {1, 2, 3}"},
		{[]UniqueID{1, 2, 3}, map[UniqueID]Timestamp{4: 1000, 5: 2000}, 3, 1500, true,
			"Seg 3 in flushedSegs {1, 2, 3}"},
		{[]UniqueID{1, 2, 3}, map[UniqueID]Timestamp{4: 1000, 5: 2000}, 4, 1500, false,
			"Seg 4, inMsgSegEntTs(1500) > SegCheckPoint(1000)"},
		{[]UniqueID{1, 2, 3}, map[UniqueID]Timestamp{4: 1000, 5: 2000}, 4, 500, true,
			"Seg 4, inMsgSegEntTs(500) <= SegCheckPoint(1000)"},
		{[]UniqueID{1, 2, 3}, map[UniqueID]Timestamp{4: 1000, 5: 2000}, 4, 1000, true,
			"Seg 4 inMsgSegEntTs(1000) <= SegCheckPoint(1000)"},
		{[]UniqueID{1, 2, 3}, map[UniqueID]Timestamp{4: 1000, 5: 2000}, 5, 1500, true,
			"Seg 5 inMsgSegEntTs(1500) <= SegCheckPoint(2000)"},
	}

	for _, test := range tests {
		te.Run(test.description, func(t *testing.T) {
			// Prepare ddNode states
			ddn := ddNode{
				flushedSegments: test.ddnFlushedSegments,
			}

			for k, v := range test.ddnSegID2Ts {
				ddn.segID2SegInfo.Store(k, &datapb.SegmentInfo{DmlPosition: &internalpb.MsgPosition{Timestamp: v}})
			}

			// Prepare insert messages
			var iMsg = &msgstream.InsertMsg{
				BaseMsg: msgstream.BaseMsg{EndTimestamp: test.inMsgSegEntTs},
				InsertRequest: internalpb.InsertRequest{
					Base:      &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
					SegmentID: test.inMsgSegID,
				},
			}

			// Test
			rt := ddn.filterFlushedSegmentInsertMessages(iMsg)
			assert.Equal(t, test.expectedOut, rt)

			si, ok := ddn.segID2SegInfo.Load(iMsg.GetSegmentID())
			if !rt {
				assert.False(t, ok)
				assert.Nil(t, si)
			}

		})
	}
}

func TestFlowGraph_DDNode_isFlushed(te *testing.T) {
	tests := []struct {
		influshedSegment []UniqueID
		inSeg            UniqueID

		expectedOut bool

		description string
	}{
		{[]UniqueID{1, 2, 3}, 1, true,
			"Input seg 1 in flushedSegs{1, 2, 3}"},
		{[]UniqueID{1, 2, 3}, 2, true,
			"Input seg 2 in flushedSegs{1, 2, 3}"},
		{[]UniqueID{1, 2, 3}, 3, true,
			"Input seg 3 in flushedSegs{1, 2, 3}"},
		{[]UniqueID{1, 2, 3}, 4, false,
			"Input seg 4 not in flushedSegs{1, 2, 3}"},
		{[]UniqueID{}, 5, false,
			"Input seg 5, no flushedSegs {}"},
	}

	for _, test := range tests {
		te.Run(test.description, func(t *testing.T) {
			ddn := &ddNode{flushedSegments: test.influshedSegment}
			assert.Equal(t, test.expectedOut, ddn.isFlushed(test.inSeg))
		})
	}
}
