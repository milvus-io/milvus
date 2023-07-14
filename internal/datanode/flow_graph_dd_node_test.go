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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
)

const (
	ddNodeChannelName  = ""
	anotherChannelName = "another_channel_name"
)

func TestFlowGraph_DDNode_newDDNode(t *testing.T) {
	tests := []struct {
		description string

		inSealedSegs  []*datapb.SegmentInfo
		inGrowingSegs []*datapb.SegmentInfo
	}{
		{
			"3 sealed segments and 1 growing segment",
			[]*datapb.SegmentInfo{
				getSegmentInfo(100, 10000),
				getSegmentInfo(101, 10000),
				getSegmentInfo(102, 10000)},
			[]*datapb.SegmentInfo{
				getSegmentInfo(200, 10000)},
		},
		{
			"0 sealed segments and 0 growing segment",
			[]*datapb.SegmentInfo{},
			[]*datapb.SegmentInfo{},
		},
	}

	var (
		collectionID  = UniqueID(1)
		channelName   = fmt.Sprintf("by-dev-rootcoord-dml-%s", t.Name())
		droppedSegIDs = []UniqueID{}
	)

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			ddNode, err := newDDNode(
				context.Background(),
				collectionID,
				channelName,
				droppedSegIDs,
				test.inSealedSegs,
				test.inGrowingSegs,
				newCompactionExecutor(),
			)
			require.NoError(t, err)
			require.NotNil(t, ddNode)

			assert.Equal(t, fmt.Sprintf("ddNode-%d-%s", ddNode.collectionID, ddNode.vChannelName), ddNode.Name())

			assert.Equal(t, len(test.inSealedSegs), len(ddNode.sealedSegInfo))
			assert.Equal(t, len(test.inGrowingSegs), len(ddNode.growingSegInfo))
		})
	}
}

func TestFlowGraph_DDNode_Operate(t *testing.T) {
	t.Run("Test DDNode Operate DropCollection Msg", func(t *testing.T) {
		// invalid inputs
		invalidInTests := []struct {
			in          []Msg
			description string
		}{
			{[]Msg{},
				"Invalid input length == 0"},
			{[]Msg{&flowGraphMsg{}, &flowGraphMsg{}, &flowGraphMsg{}},
				"Invalid input length == 3"},
			{[]Msg{&flowGraphMsg{}},
				"Invalid input length == 1 but input message is not msgStreamMsg"},
		}

		for _, test := range invalidInTests {
			t.Run(test.description, func(t *testing.T) {
				ddn := ddNode{}
				assert.False(t, ddn.IsValidInMsg(test.in))
			})
		}
		// valid inputs
		tests := []struct {
			ddnCollID UniqueID

			msgCollID     UniqueID
			expectedChlen int

			description string
		}{
			{1, 1, 1,
				"DropCollectionMsg collID == ddNode collID"},
			{1, 2, 0,
				"DropCollectionMsg collID != ddNode collID"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				ddn := ddNode{
					ctx:                context.Background(),
					collectionID:       test.ddnCollID,
					vChannelName:       "ddn_drop_msg",
					compactionExecutor: newCompactionExecutor(),
				}

				var dropCollMsg msgstream.TsMsg = &msgstream.DropCollectionMsg{
					DropCollectionRequest: msgpb.DropCollectionRequest{
						Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
						CollectionID: test.msgCollID,
					},
				}
				tsMessages := []msgstream.TsMsg{dropCollMsg}
				var msgStreamMsg Msg = flowgraph.GenerateMsgStreamMsg(tsMessages, 0, 0, nil, nil)

				rt := ddn.Operate([]Msg{msgStreamMsg})

				if test.ddnCollID == test.msgCollID {
					assert.NotEmpty(t, rt)
					assert.True(t, rt[0].(*flowGraphMsg).dropCollection)
				} else {
					assert.NotEmpty(t, rt)
				}
			})
		}
	})

	t.Run("Test DDNode Operate DropPartition Msg", func(t *testing.T) {
		// valid inputs
		tests := []struct {
			ddnCollID UniqueID

			msgCollID    UniqueID
			msgPartID    UniqueID
			expectOutput []UniqueID

			description string
		}{
			{1, 1, 101, []UniqueID{101},
				"DropCollectionMsg collID == ddNode collID"},
			{1, 2, 101, []UniqueID{},
				"DropCollectionMsg collID != ddNode collID"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				ddn := ddNode{
					ctx:                context.Background(),
					collectionID:       test.ddnCollID,
					vChannelName:       "ddn_drop_msg",
					compactionExecutor: newCompactionExecutor(),
				}

				var dropPartMsg msgstream.TsMsg = &msgstream.DropPartitionMsg{
					DropPartitionRequest: msgpb.DropPartitionRequest{
						Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_DropPartition},
						CollectionID: test.msgCollID,
						PartitionID:  test.msgPartID,
					},
				}
				tsMessages := []msgstream.TsMsg{dropPartMsg}
				var msgStreamMsg Msg = flowgraph.GenerateMsgStreamMsg(tsMessages, 0, 0, nil, nil)

				rt := ddn.Operate([]Msg{msgStreamMsg})

				assert.NotEmpty(t, rt)
				fgMsg, ok := rt[0].(*flowGraphMsg)
				assert.True(t, ok)
				assert.ElementsMatch(t, test.expectOutput, fgMsg.dropPartitions)

			})
		}
	})

	t.Run("Test DDNode Operate and filter insert msg", func(t *testing.T) {
		var (
			collectionID UniqueID = 1
		)
		// Prepare ddNode states
		ddn := ddNode{
			ctx:               context.Background(),
			collectionID:      collectionID,
			droppedSegmentIDs: []UniqueID{100},
		}

		tsMessages := []msgstream.TsMsg{getInsertMsg(100, 10000), getInsertMsg(200, 20000)}
		var msgStreamMsg Msg = flowgraph.GenerateMsgStreamMsg(tsMessages, 0, 0, nil, nil)

		rt := ddn.Operate([]Msg{msgStreamMsg})
		assert.Equal(t, 1, len(rt[0].(*flowGraphMsg).insertMessages))
	})

	t.Run("Test DDNode Operate Delete Msg", func(t *testing.T) {
		tests := []struct {
			ddnCollID   UniqueID
			inMsgCollID UniqueID

			MsgEndTs Timestamp

			expectedRtLen int
			description   string
		}{
			{1, 1, 2000, 1, "normal"},
			{1, 2, 4000, 0, "inMsgCollID(2) != ddnCollID"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				ddn := ddNode{
					ctx:          context.Background(),
					collectionID: test.ddnCollID,
				}

				// Prepare delete messages
				var dMsg msgstream.TsMsg = &msgstream.DeleteMsg{
					BaseMsg: msgstream.BaseMsg{
						EndTimestamp: test.MsgEndTs,
						HashValues:   []uint32{0},
					},
					DeleteRequest: msgpb.DeleteRequest{
						Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
						ShardName:    "by-dev-rootcoord-dml-mock-0",
						CollectionID: test.inMsgCollID,
					},
				}
				tsMessages := []msgstream.TsMsg{dMsg}
				var msgStreamMsg Msg = flowgraph.GenerateMsgStreamMsg(tsMessages, 0, 0, nil, nil)

				// Test
				rt := ddn.Operate([]Msg{msgStreamMsg})
				assert.Equal(t, test.expectedRtLen, len(rt[0].(*flowGraphMsg).deleteMessages))
			})
		}
	})

}

func TestFlowGraph_DDNode_filterMessages(t *testing.T) {
	tests := []struct {
		description string

		droppedSegIDs  []UniqueID
		sealedSegInfo  map[UniqueID]*datapb.SegmentInfo
		growingSegInfo map[UniqueID]*datapb.SegmentInfo

		inMsg    *msgstream.InsertMsg
		expected bool
	}{
		{"test dropped segments true",
			[]UniqueID{100},
			nil,
			nil,
			getInsertMsg(100, 10000),
			true},
		{"test dropped segments true 2",
			[]UniqueID{100, 101, 102},
			nil,
			nil,
			getInsertMsg(102, 10000),
			true},
		{"test sealed segments msgTs <= segmentTs true",
			[]UniqueID{},
			map[UniqueID]*datapb.SegmentInfo{
				200: getSegmentInfo(200, 50000),
				300: getSegmentInfo(300, 50000),
			},
			nil,
			getInsertMsg(200, 10000),
			true},
		{"test sealed segments msgTs <= segmentTs true",
			[]UniqueID{},
			map[UniqueID]*datapb.SegmentInfo{
				200: getSegmentInfo(200, 50000),
				300: getSegmentInfo(300, 50000),
			},
			nil,
			getInsertMsg(200, 50000),
			true},
		{"test sealed segments msgTs > segmentTs false",
			[]UniqueID{},
			map[UniqueID]*datapb.SegmentInfo{
				200: getSegmentInfo(200, 50000),
				300: getSegmentInfo(300, 50000),
			},
			nil,
			getInsertMsg(222, 70000),
			false},
		{"test growing segments msgTs <= segmentTs true",
			[]UniqueID{},
			nil,
			map[UniqueID]*datapb.SegmentInfo{
				200: getSegmentInfo(200, 50000),
				300: getSegmentInfo(300, 50000),
			},
			getInsertMsg(200, 10000),
			true},
		{"test growing segments msgTs > segmentTs false",
			[]UniqueID{},
			nil,
			map[UniqueID]*datapb.SegmentInfo{
				200: getSegmentInfo(200, 50000),
				300: getSegmentInfo(300, 50000),
			},
			getInsertMsg(200, 70000),
			false},
		{"test not exist",
			[]UniqueID{},
			map[UniqueID]*datapb.SegmentInfo{
				400: getSegmentInfo(500, 50000),
				500: getSegmentInfo(400, 50000),
			},
			map[UniqueID]*datapb.SegmentInfo{
				200: getSegmentInfo(200, 50000),
				300: getSegmentInfo(300, 50000),
			},
			getInsertMsg(111, 70000),
			false},
		// for pChannel reuse on same collection
		{"test insert msg with different channelName",
			[]UniqueID{100},
			nil,
			nil,
			getInsertMsgWithChannel(100, 10000, anotherChannelName),
			true},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			// Prepare ddNode states
			ddn := ddNode{
				droppedSegmentIDs: test.droppedSegIDs,
				sealedSegInfo:     test.sealedSegInfo,
				growingSegInfo:    test.growingSegInfo,
			}

			// Test
			got := ddn.tryToFilterSegmentInsertMessages(test.inMsg)
			assert.Equal(t, test.expected, got)

		})
	}

	t.Run("Test delete segment from sealed segments", func(t *testing.T) {
		tests := []struct {
			description string
			segRemained bool

			segTs Timestamp
			msgTs Timestamp

			sealedSegInfo map[UniqueID]*datapb.SegmentInfo
			inMsg         *msgstream.InsertMsg
			msgFiltered   bool
		}{
			{"msgTs<segTs",
				true,
				50000,
				10000,
				map[UniqueID]*datapb.SegmentInfo{
					100: getSegmentInfo(100, 50000),
					101: getSegmentInfo(101, 50000)},
				getInsertMsg(100, 10000),
				true,
			},
			{"msgTs==segTs",
				true,
				50000,
				10000,
				map[UniqueID]*datapb.SegmentInfo{
					100: getSegmentInfo(100, 50000),
					101: getSegmentInfo(101, 50000)},
				getInsertMsg(100, 50000),
				true,
			},
			{"msgTs>segTs",
				false,
				50000,
				10000,
				map[UniqueID]*datapb.SegmentInfo{
					100: getSegmentInfo(100, 70000),
					101: getSegmentInfo(101, 50000)},
				getInsertMsg(300, 60000),
				false,
			},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				ddn := &ddNode{sealedSegInfo: test.sealedSegInfo}

				got := ddn.tryToFilterSegmentInsertMessages(test.inMsg)
				assert.Equal(t, test.msgFiltered, got)

				if test.segRemained {
					assert.Equal(t, 2, len(ddn.sealedSegInfo))
				} else {
					assert.Equal(t, 1, len(ddn.sealedSegInfo))
				}

				_, ok := ddn.sealedSegInfo[test.inMsg.GetSegmentID()]
				assert.Equal(t, test.segRemained, ok)
			})
		}
	})

	t.Run("Test delete segment from growing segments", func(t *testing.T) {
		tests := []struct {
			description string
			segRemained bool

			growingSegInfo map[UniqueID]*datapb.SegmentInfo
			inMsg          *msgstream.InsertMsg
			msgFiltered    bool
		}{
			{"msgTs<segTs",
				true,
				map[UniqueID]*datapb.SegmentInfo{
					100: getSegmentInfo(100, 50000),
					101: getSegmentInfo(101, 50000)},
				getInsertMsg(100, 10000),
				true,
			},
			{"msgTs==segTs",
				true,
				map[UniqueID]*datapb.SegmentInfo{
					100: getSegmentInfo(100, 50000),
					101: getSegmentInfo(101, 50000)},
				getInsertMsg(100, 50000),
				true,
			},
			{"msgTs>segTs",
				false,
				map[UniqueID]*datapb.SegmentInfo{
					100: getSegmentInfo(100, 50000),
					101: getSegmentInfo(101, 50000)},
				getInsertMsg(100, 60000),
				false,
			},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				ddn := &ddNode{
					growingSegInfo: test.growingSegInfo,
				}

				got := ddn.tryToFilterSegmentInsertMessages(test.inMsg)
				assert.Equal(t, test.msgFiltered, got)

				if test.segRemained {
					assert.Equal(t, 2, len(ddn.growingSegInfo))
				} else {
					assert.Equal(t, 1, len(ddn.growingSegInfo))
				}

				_, ok := ddn.growingSegInfo[test.inMsg.GetSegmentID()]
				assert.Equal(t, test.segRemained, ok)
			})
		}
	})
}

func TestFlowGraph_DDNode_isDropped(t *testing.T) {
	tests := []struct {
		indroppedSegment []*datapb.SegmentInfo
		inSeg            UniqueID

		expectedOut bool

		description string
	}{
		{[]*datapb.SegmentInfo{getSegmentInfo(1, 0), getSegmentInfo(2, 0), getSegmentInfo(3, 0)}, 1, true,
			"Input seg 1 in droppedSegs{1,2,3}"},
		{[]*datapb.SegmentInfo{getSegmentInfo(1, 0), getSegmentInfo(2, 0), getSegmentInfo(3, 0)}, 2, true,
			"Input seg 2 in droppedSegs{1,2,3}"},
		{[]*datapb.SegmentInfo{getSegmentInfo(1, 0), getSegmentInfo(2, 0), getSegmentInfo(3, 0)}, 3, true,
			"Input seg 3 in droppedSegs{1,2,3}"},
		{[]*datapb.SegmentInfo{getSegmentInfo(1, 0), getSegmentInfo(2, 0), getSegmentInfo(3, 0)}, 4, false,
			"Input seg 4 not in droppedSegs{1,2,3}"},
		{[]*datapb.SegmentInfo{}, 5, false,
			"Input seg 5, no droppedSegs {}"},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			dsIDs := []int64{}
			for _, seg := range test.indroppedSegment {
				dsIDs = append(dsIDs, seg.GetID())
			}

			ddn := &ddNode{droppedSegmentIDs: dsIDs, vChannelName: ddNodeChannelName}
			assert.Equal(t, test.expectedOut, ddn.isDropped(test.inSeg))
		})
	}
}

func getSegmentInfo(segmentID UniqueID, ts Timestamp) *datapb.SegmentInfo {
	return &datapb.SegmentInfo{
		ID:          segmentID,
		DmlPosition: &msgpb.MsgPosition{Timestamp: ts},
	}
}

func getInsertMsg(segmentID UniqueID, ts Timestamp) *msgstream.InsertMsg {
	return getInsertMsgWithChannel(segmentID, ts, ddNodeChannelName)
}

func getInsertMsgWithChannel(segmentID UniqueID, ts Timestamp, vChannelName string) *msgstream.InsertMsg {
	return &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{EndTimestamp: ts},
		InsertRequest: msgpb.InsertRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
			SegmentID:    segmentID,
			CollectionID: 1,
			ShardName:    vChannelName,
		},
	}
}

type mockFactory struct {
	msgstream.Factory
}
