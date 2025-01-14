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

package pipeline

import (
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// test of filter node
type FilterNodeSuite struct {
	suite.Suite
	// datas
	collectionID int64
	partitionIDs []int64
	channel      string

	validSegmentIDs    []int64
	excludedSegmentIDs []int64
	insertSegmentIDs   []int64
	deleteSegmentSum   int
	// segmentID of msg invalid because empty of not aligned
	errSegmentID int64

	// mocks
	manager *segments.Manager

	delegator *delegator.MockShardDelegator
}

func (suite *FilterNodeSuite) SetupSuite() {
	paramtable.Init()
	suite.collectionID = 111
	suite.partitionIDs = []int64{11, 22}
	suite.channel = "test-channel"

	// first one invalid because insert max timestamp before dmlPosition timestamp
	suite.excludedSegmentIDs = []int64{1, 2}
	suite.insertSegmentIDs = []int64{3, 4, 5, 6}
	suite.deleteSegmentSum = 4
	suite.errSegmentID = 7

	suite.delegator = delegator.NewMockShardDelegator(suite.T())
}

// test filter node with collection load collection
func (suite *FilterNodeSuite) TestWithLoadCollection() {
	// data
	suite.validSegmentIDs = []int64{2, 3, 4, 5, 6}

	// mock
	collection := segments.NewCollectionWithoutSchema(suite.collectionID, querypb.LoadType_LoadCollection)
	for _, partitionID := range suite.partitionIDs {
		collection.AddPartition(partitionID)
	}

	mockCollectionManager := segments.NewMockCollectionManager(suite.T())
	mockCollectionManager.EXPECT().Get(suite.collectionID).Return(collection)

	mockSegmentManager := segments.NewMockSegmentManager(suite.T())

	suite.manager = &segments.Manager{
		Collection: mockCollectionManager,
		Segment:    mockSegmentManager,
	}

	suite.delegator.EXPECT().VerifyExcludedSegments(mock.Anything, mock.Anything).RunAndReturn(func(segmentID int64, ts uint64) bool {
		return !(lo.Contains(suite.excludedSegmentIDs, segmentID) && ts <= 1)
	})
	suite.delegator.EXPECT().TryCleanExcludedSegments(mock.Anything)
	node := newFilterNode(suite.collectionID, suite.channel, suite.manager, suite.delegator, 8)
	in := suite.buildMsgPack()
	out := node.Operate(in)

	nodeMsg, ok := out.(*insertNodeMsg)
	suite.True(ok)

	suite.Equal(len(suite.validSegmentIDs), len(nodeMsg.insertMsgs))
	for _, msg := range nodeMsg.insertMsgs {
		suite.True(lo.Contains(suite.validSegmentIDs, msg.SegmentID))
	}
	suite.Equal(suite.deleteSegmentSum, len(nodeMsg.deleteMsgs))
}

// test filter node with collection load partition
func (suite *FilterNodeSuite) TestWithLoadPartation() {
	// data
	suite.validSegmentIDs = []int64{2, 3, 4, 5, 6}

	// mock
	collection := segments.NewCollectionWithoutSchema(suite.collectionID, querypb.LoadType_LoadPartition)
	collection.AddPartition(suite.partitionIDs[0])

	mockCollectionManager := segments.NewMockCollectionManager(suite.T())
	mockCollectionManager.EXPECT().Get(suite.collectionID).Return(collection)

	mockSegmentManager := segments.NewMockSegmentManager(suite.T())

	suite.manager = &segments.Manager{
		Collection: mockCollectionManager,
		Segment:    mockSegmentManager,
	}

	suite.delegator.EXPECT().VerifyExcludedSegments(mock.Anything, mock.Anything).RunAndReturn(func(segmentID int64, ts uint64) bool {
		return !(lo.Contains(suite.excludedSegmentIDs, segmentID) && ts <= 1)
	})
	suite.delegator.EXPECT().TryCleanExcludedSegments(mock.Anything)
	node := newFilterNode(suite.collectionID, suite.channel, suite.manager, suite.delegator, 8)
	in := suite.buildMsgPack()
	out := node.Operate(in)

	nodeMsg, ok := out.(*insertNodeMsg)
	suite.True(ok)

	suite.Equal(len(suite.validSegmentIDs), len(nodeMsg.insertMsgs))
	for _, msg := range nodeMsg.insertMsgs {
		suite.True(lo.Contains(suite.validSegmentIDs, msg.SegmentID))
	}
	suite.Equal(suite.deleteSegmentSum, len(nodeMsg.deleteMsgs))
}

func (suite *FilterNodeSuite) buildMsgPack() *msgstream.MsgPack {
	msgPack := &msgstream.MsgPack{
		BeginTs: 0,
		EndTs:   0,
		Msgs:    []msgstream.TsMsg{},
	}

	// add valid insert
	for _, id := range suite.insertSegmentIDs {
		insertMsg := buildInsertMsg(suite.collectionID, suite.partitionIDs[id%2], id, suite.channel, 1)
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}

	// add valid delete
	for i := 0; i < suite.deleteSegmentSum; i++ {
		deleteMsg := buildDeleteMsg(suite.collectionID, suite.partitionIDs[i%2], suite.channel, 1)
		msgPack.Msgs = append(msgPack.Msgs, deleteMsg)
	}

	// add invalid msg

	// segment in excludedSegments
	// some one end timestamp befroe dmlPosition timestamp will be invalid
	for _, id := range suite.excludedSegmentIDs {
		insertMsg := buildInsertMsg(suite.collectionID, suite.partitionIDs[id%2], id, suite.channel, 1)
		insertMsg.EndTimestamp = uint64(id)
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}

	// empty msg
	insertMsg := buildInsertMsg(suite.collectionID, suite.partitionIDs[0], suite.errSegmentID, suite.channel, 0)
	msgPack.Msgs = append(msgPack.Msgs, insertMsg)

	deleteMsg := buildDeleteMsg(suite.collectionID, suite.partitionIDs[0], suite.channel, 0)
	msgPack.Msgs = append(msgPack.Msgs, deleteMsg)

	// msg not target
	insertMsg = buildInsertMsg(suite.collectionID+1, 1, 0, "Unknown", 1)
	msgPack.Msgs = append(msgPack.Msgs, insertMsg)

	deleteMsg = buildDeleteMsg(suite.collectionID+1, 1, "Unknown", 1)
	msgPack.Msgs = append(msgPack.Msgs, deleteMsg)

	// msg not aligned
	insertMsg = buildInsertMsg(suite.collectionID, suite.partitionIDs[0], suite.errSegmentID, suite.channel, 1)
	insertMsg.Timestamps = []uint64{}
	msgPack.Msgs = append(msgPack.Msgs, insertMsg)

	deleteMsg = buildDeleteMsg(suite.collectionID, suite.partitionIDs[0], suite.channel, 1)
	deleteMsg.Timestamps = append(deleteMsg.Timestamps, 1)
	msgPack.Msgs = append(msgPack.Msgs, deleteMsg)
	return msgPack
}

func TestFilterNode(t *testing.T) {
	suite.Run(t, new(FilterNodeSuite))
}
