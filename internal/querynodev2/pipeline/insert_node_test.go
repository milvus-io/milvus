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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type InsertNodeSuite struct {
	suite.Suite
	// datas
	collectionName   string
	collectionID     int64
	partitionID      int64
	channel          string
	insertSegmentIDs []int64
	deleteSegmentSum int
	// mocks
	manager   *segments.Manager
	delegator *delegator.MockShardDelegator
}

func (suite *InsertNodeSuite) SetupSuite() {
	paramtable.Init()

	suite.collectionName = "test-collection"
	suite.collectionID = 111
	suite.partitionID = 11
	suite.channel = "test_channel"

	suite.insertSegmentIDs = []int64{4, 3}
	suite.deleteSegmentSum = 2
}

func (suite *InsertNodeSuite) TestBasic() {
	// data
	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, true)
	in := suite.buildInsertNodeMsg(schema)

	collection, err := segments.NewCollection(suite.collectionID, schema, mock_segcore.GenTestIndexMeta(suite.collectionID, schema), &querypb.LoadMetaInfo{
		LoadType: querypb.LoadType_LoadCollection,
	}, newTestLocalFileSystem(suite.T()))
	suite.NoError(err)
	collection.AddPartition(suite.partitionID)

	// init mock
	mockCollectionManager := segments.NewMockCollectionManager(suite.T())
	mockCollectionManager.EXPECT().Get(suite.collectionID).Return(collection)

	mockSegmentManager := segments.NewMockSegmentManager(suite.T())

	suite.manager = &segments.Manager{
		Collection: mockCollectionManager,
		Segment:    mockSegmentManager,
	}

	suite.delegator = delegator.NewMockShardDelegator(suite.T())
	suite.delegator.EXPECT().ProcessInsert(mock.Anything).Run(func(insertRecords map[int64]*delegator.InsertData) {
		for segID := range insertRecords {
			suite.True(lo.Contains(suite.insertSegmentIDs, segID))
		}
	})

	// TODO mock a delgator for test
	node, err := newInsertNode(suite.collectionID, suite.channel, suite.manager, suite.delegator, schema, 8)
	suite.NoError(err)
	out := node.Operate(in)

	nodeMsg, ok := out.(*deleteNodeMsg)
	suite.True(ok)
	suite.Equal(suite.deleteSegmentSum, len(nodeMsg.deleteMsgs))
}

func (suite *InsertNodeSuite) TestDataTypeNotSupported() {
	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, true)
	in := suite.buildInsertNodeMsg(schema)

	collection, err := segments.NewCollection(suite.collectionID, schema, mock_segcore.GenTestIndexMeta(suite.collectionID, schema), &querypb.LoadMetaInfo{
		LoadType: querypb.LoadType_LoadCollection,
	}, newTestLocalFileSystem(suite.T()))
	suite.NoError(err)
	collection.AddPartition(suite.partitionID)

	// init mock
	mockCollectionManager := segments.NewMockCollectionManager(suite.T())
	mockCollectionManager.EXPECT().Get(suite.collectionID).Return(collection)

	mockSegmentManager := segments.NewMockSegmentManager(suite.T())

	suite.manager = &segments.Manager{
		Collection: mockCollectionManager,
		Segment:    mockSegmentManager,
	}

	suite.delegator = delegator.NewMockShardDelegator(suite.T())

	for _, msg := range in.insertMsgs {
		for _, field := range msg.GetFieldsData() {
			field.Type = schemapb.DataType_None
		}
	}

	// TODO mock a delgator for test
	node, err := newInsertNode(suite.collectionID, suite.channel, suite.manager, suite.delegator, schema, 8)
	suite.NoError(err)
	suite.Panics(func() {
		node.Operate(in)
	})
}

func (suite *InsertNodeSuite) TestLegacyInsertMaterializesBM25Stats() {
	schema := &schemapb.CollectionSchema{
		Name: suite.collectionName,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "max_length", Value: "1024"},
				},
			},
			{
				FieldID:          102,
				Name:             "sparse",
				DataType:         schemapb.DataType_SparseFloatVector,
				IsFunctionOutput: true,
			},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Name:           "bm25",
				Type:           schemapb.FunctionType_BM25,
				InputFieldIds:  []int64{101},
				OutputFieldIds: []int64{102},
			},
			{
				Name:          "rerank",
				Type:          schemapb.FunctionType_Rerank,
				InputFieldIds: []int64{101},
			},
		},
	}
	in := suite.buildInsertNodeMsg(schema)
	for _, msg := range in.insertMsgs {
		msg.FieldsData = msg.FieldsData[:2]
	}

	collection := segments.NewCollectionWithoutSegcoreForTest(suite.collectionID, schema)
	collection.AddPartition(suite.partitionID)

	mockCollectionManager := segments.NewMockCollectionManager(suite.T())
	mockCollectionManager.EXPECT().Get(suite.collectionID).Return(collection)

	suite.manager = &segments.Manager{
		Collection: mockCollectionManager,
		Segment:    segments.NewMockSegmentManager(suite.T()),
	}

	suite.delegator = delegator.NewMockShardDelegator(suite.T())
	suite.delegator.EXPECT().ProcessInsert(mock.Anything).Run(func(insertRecords map[int64]*delegator.InsertData) {
		for _, insertData := range insertRecords {
			suite.Require().Contains(insertData.BM25Stats, int64(102))
			suite.Equal(int64(2), insertData.BM25Stats[102].NumRow())
		}
	})

	node, err := newInsertNode(suite.collectionID, suite.channel, suite.manager, suite.delegator, schema, 8)
	suite.NoError(err)
	node.Operate(in)
}

func (suite *InsertNodeSuite) buildInsertNodeMsg(schema *schemapb.CollectionSchema) *insertNodeMsg {
	nodeMsg := insertNodeMsg{
		insertMsgs: []*InsertMsg{},
		deleteMsgs: []*DeleteMsg{},
		timeRange: TimeRange{
			timestampMin: 0,
			timestampMax: 0,
		},
	}

	for _, segmentID := range suite.insertSegmentIDs {
		insertMsg := buildInsertMsg(suite.collectionID, suite.partitionID, segmentID, suite.channel, 1)
		insertMsg.FieldsData = genFiledDataWithSchema(schema, 1)
		nodeMsg.insertMsgs = append(nodeMsg.insertMsgs, insertMsg)

		insertMsg = buildInsertMsg(suite.collectionID, suite.partitionID, segmentID, suite.channel, 1)
		insertMsg.FieldsData = genFiledDataWithSchema(schema, 1)
		nodeMsg.insertMsgs = append(nodeMsg.insertMsgs, insertMsg)
	}

	for i := 0; i < suite.deleteSegmentSum; i++ {
		deleteMsg := buildDeleteMsg(suite.collectionID, suite.partitionID, suite.channel, 1)
		nodeMsg.deleteMsgs = append(nodeMsg.deleteMsgs, deleteMsg)
	}

	return &nodeMsg
}

func TestInsertNode(t *testing.T) {
	suite.Run(t, new(InsertNodeSuite))
}
