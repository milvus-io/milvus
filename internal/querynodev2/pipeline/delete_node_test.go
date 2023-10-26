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
	"context"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/querynodev2/tsafe"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type DeleteNodeSuite struct {
	suite.Suite
	// datas
	collectionID   int64
	collectionName string
	partitionIDs   []int64
	deletePKs      []int64
	channel        string
	timeRange      TimeRange

	// dependency
	tSafeManager TSafeManager
	// mocks
	manager   *segments.Manager
	delegator *delegator.MockShardDelegator
}

func (suite *DeleteNodeSuite) SetupSuite() {
	paramtable.Init()
	suite.collectionID = 111
	suite.collectionName = "test-collection"
	suite.partitionIDs = []int64{11, 22}
	suite.channel = "test-channel"
	// segment own data row which‘s pk same with segment‘s ID
	suite.deletePKs = []int64{1, 2, 3, 4}
	suite.timeRange = TimeRange{
		timestampMin: 0,
		timestampMax: 1,
	}
}

func (suite *DeleteNodeSuite) buildDeleteNodeMsg() *deleteNodeMsg {
	nodeMsg := &deleteNodeMsg{
		deleteMsgs: []*DeleteMsg{},
		timeRange:  suite.timeRange,
	}

	for i, pk := range suite.deletePKs {
		deleteMsg := buildDeleteMsg(suite.collectionID, suite.partitionIDs[i%len(suite.partitionIDs)], suite.channel, 1)
		deleteMsg.PrimaryKeys = genDeletePK(pk)
		nodeMsg.deleteMsgs = append(nodeMsg.deleteMsgs, deleteMsg)
	}
	return nodeMsg
}

func (suite *DeleteNodeSuite) TestBasic() {
	// mock
	mockCollectionManager := segments.NewMockCollectionManager(suite.T())
	mockSegmentManager := segments.NewMockSegmentManager(suite.T())
	suite.manager = &segments.Manager{
		Collection: mockCollectionManager,
		Segment:    mockSegmentManager,
	}
	suite.delegator = delegator.NewMockShardDelegator(suite.T())
	suite.delegator.EXPECT().ProcessDelete(mock.Anything, mock.Anything).Run(
		func(deleteData []*delegator.DeleteData, ts uint64) {
			for _, data := range deleteData {
				for _, pk := range data.PrimaryKeys {
					suite.True(lo.Contains(suite.deletePKs, pk.GetValue().(int64)))
				}
			}
		})
	// init dependency
	suite.tSafeManager = tsafe.NewTSafeReplica()
	suite.tSafeManager.Add(context.Background(), suite.channel, 0)
	// build delete node and data
	node := newDeleteNode(suite.collectionID, suite.channel, suite.manager, suite.tSafeManager, suite.delegator, 8)
	in := suite.buildDeleteNodeMsg()
	// run
	out := node.Operate(in)
	suite.Nil(out)
	// check tsafe
	tt, err := suite.tSafeManager.Get(suite.channel)
	suite.NoError(err)
	suite.Equal(suite.timeRange.timestampMax, tt)
}

func TestDeleteNode(t *testing.T) {
	suite.Run(t, new(DeleteNodeSuite))
}
