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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/querynodev2/tsafe"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type WriteNodeSuite struct {
	suite.Suite

	// datas
	collectionName   string
	collectionID     int64
	partitionIDs     []int64
	channel          string
	insertSegmentIDs []int64
	deletePKs        []int64

	// dependencies
	tSafeManager TSafeManager

	// mocks
	segmentManager    *segments.MockSegmentManager
	collectionManager *segments.MockCollectionManager
	delegator         *delegator.MockShardDelegator

	// node
	wNode *writeNode
}

func (s *WriteNodeSuite) SetupSuite() {
	paramtable.Init()

	s.collectionID = 111
	s.collectionName = "test-collection"
	s.channel = "test-channel"
	s.partitionIDs = []int64{11, 22}
	s.insertSegmentIDs = []int64{1, 2, 3}
	s.deletePKs = []int64{1, 2, 3}
}

func (s *WriteNodeSuite) SetupTest() {
	// init mock
	//	init manager
	s.collectionManager = segments.NewMockCollectionManager(s.T())
	s.segmentManager = segments.NewMockSegmentManager(s.T())
	//	init delegator
	s.delegator = delegator.NewMockShardDelegator(s.T())
	// init dependency
	// init tsafeManager
	s.tSafeManager = tsafe.NewTSafeReplica()
	s.tSafeManager.Add(context.Background(), s.channel, 0)

	schema := segments.GenTestCollectionSchema(s.collectionName, schemapb.DataType_Int64, true)
	s.collectionManager.EXPECT().Get(s.collectionID).Return(segments.NewCollection(s.collectionID, schema, nil, nil))

	s.wNode = newWriteNode(s.collectionID, s.channel, &DataManager{
		Segment: s.segmentManager, Collection: s.collectionManager,
	}, s.tSafeManager, s.delegator, 10)
}

func (s *WriteNodeSuite) buildMsgPack(schema *schemapb.CollectionSchema) *insertNodeMsg {
	input := &insertNodeMsg{
		timeRange: TimeRange{
			timestampMin: 1,
			timestampMax: 2,
		},
	}
	for id, segmentID := range s.insertSegmentIDs {
		insertMsg := buildInsertMsg(s.collectionID, s.partitionIDs[id%len(s.partitionIDs)], segmentID, s.channel, 1)
		insertMsg.FieldsData = genFieldDataWithSchema(schema, 1)
		input.insertMsgs = append(input.insertMsgs, insertMsg)
	}
	for id, pk := range s.deletePKs {
		deleteMsg := buildDeleteMsg(s.collectionID, s.partitionIDs[id%len(s.partitionIDs)], s.channel, 1)
		deleteMsg.PrimaryKeys = genDeletePK(pk)
		input.deleteMsgs = append(input.deleteMsgs, deleteMsg)
	}
	return input
}

func (s *WriteNodeSuite) TestOperate() {
	schema := segments.GenTestCollectionSchema(s.collectionName, schemapb.DataType_Int64, true)
	input := s.buildMsgPack(schema)

	s.Run("normal_run", func() {
		s.delegator.EXPECT().ProcessData(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		s.NotPanics(func() {
			s.wNode.Operate(input)
		})

		tsafe, err := s.tSafeManager.Get(s.channel)
		s.Require().NoError(err)
		s.EqualValues(2, tsafe)
	})

	s.Run("process_data_fail", func() {
		s.delegator.EXPECT().ProcessData(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mocked")).Once()
		s.Panics(func() {
			s.wNode.Operate(input)
		})
	})

	s.Run("set_tsafe_fail", func() {
		s.delegator.EXPECT().ProcessData(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		s.tSafeManager.Remove(context.Background(), s.channel)
		s.Panics(func() {
			s.wNode.Operate(input)
		})
	})
}

func TestWriteNode(t *testing.T) {
	suite.Run(t, new(WriteNodeSuite))
}
