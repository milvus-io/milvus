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

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/querynodev2/tsafe"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type PipelineTestSuite struct {
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
	msgDispatcher     *msgdispatcher.MockClient
	msgChan           chan *msgstream.MsgPack
}

func (suite *PipelineTestSuite) SetupSuite() {
	suite.collectionID = 111
	suite.collectionName = "test-collection"
	suite.channel = "test-channel"
	suite.partitionIDs = []int64{11, 22}
	suite.insertSegmentIDs = []int64{1, 2, 3}
	suite.deletePKs = []int64{1, 2, 3}
	suite.msgChan = make(chan *msgstream.MsgPack, 1)
}

func (suite *PipelineTestSuite) buildMsgPack(schema *schemapb.CollectionSchema) *msgstream.MsgPack {
	msgPack := &msgstream.MsgPack{
		BeginTs: 0,
		EndTs:   1,
		Msgs:    []msgstream.TsMsg{},
	}

	for id, segmentID := range suite.insertSegmentIDs {
		insertMsg := buildInsertMsg(suite.collectionID, suite.partitionIDs[id%len(suite.partitionIDs)], segmentID, suite.channel, 1)
		insertMsg.FieldsData = genFiledDataWithSchema(schema, 1)
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}

	for id, pk := range suite.deletePKs {
		deleteMsg := buildDeleteMsg(suite.collectionID, suite.partitionIDs[id%len(suite.partitionIDs)], suite.channel, 1)
		deleteMsg.PrimaryKeys = genDeletePK(pk)
		msgPack.Msgs = append(msgPack.Msgs, deleteMsg)
	}
	return msgPack
}

func (suite *PipelineTestSuite) SetupTest() {
	paramtable.Init()
	// init mock
	//	init manager
	suite.collectionManager = segments.NewMockCollectionManager(suite.T())
	suite.segmentManager = segments.NewMockSegmentManager(suite.T())
	//	init delegator
	suite.delegator = delegator.NewMockShardDelegator(suite.T())
	//	init mq dispatcher
	suite.msgDispatcher = msgdispatcher.NewMockClient(suite.T())

	// init dependency
	//	init tsafeManager
	suite.tSafeManager = tsafe.NewTSafeReplica()
	suite.tSafeManager.Add(context.Background(), suite.channel, 0)
}

func (suite *PipelineTestSuite) TestBasic() {
	// init mock
	//	mock collection manager
	schema := segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64)
	collection := segments.NewCollection(suite.collectionID, schema, segments.GenTestIndexMeta(suite.collectionID, schema), querypb.LoadType_LoadCollection)
	suite.collectionManager.EXPECT().Get(suite.collectionID).Return(collection)

	//  mock mq factory
	suite.msgDispatcher.EXPECT().Register(mock.Anything, suite.channel, mock.Anything, mqwrapper.SubscriptionPositionUnknown).Return(suite.msgChan, nil)
	suite.msgDispatcher.EXPECT().Deregister(suite.channel)

	//	mock delegator
	suite.delegator.EXPECT().ProcessInsert(mock.Anything).Run(
		func(insertRecords map[int64]*delegator.InsertData) {
			for segmentID := range insertRecords {
				suite.True(lo.Contains(suite.insertSegmentIDs, segmentID))
			}
		})

	suite.delegator.EXPECT().ProcessDelete(mock.Anything, mock.Anything).Run(
		func(deleteData []*delegator.DeleteData, ts uint64) {
			for _, data := range deleteData {
				for _, pk := range data.PrimaryKeys {
					suite.True(lo.Contains(suite.deletePKs, pk.GetValue().(int64)))
				}
			}
		})
	// build pipleine
	manager := &segments.Manager{
		Collection: suite.collectionManager,
		Segment:    suite.segmentManager,
	}
	pipeline, err := NewPipeLine(suite.collectionID, suite.channel, manager, suite.tSafeManager, suite.msgDispatcher, suite.delegator)
	suite.NoError(err)

	// Init Consumer
	err = pipeline.ConsumeMsgStream(&msgpb.MsgPosition{})
	suite.NoError(err)

	err = pipeline.Start()
	suite.NoError(err)
	defer pipeline.Close()

	// watch tsafe manager
	listener := suite.tSafeManager.WatchChannel(suite.channel)

	// build input msg
	in := suite.buildMsgPack(schema)
	suite.msgChan <- in

	// wait pipeline work
	<-listener.On()

	// check tsafe
	tsafe, err := suite.tSafeManager.Get(suite.channel)
	suite.NoError(err)
	suite.Equal(in.EndTs, tsafe)
}

func TestQueryNodePipeline(t *testing.T) {
	suite.Run(t, new(PipelineTestSuite))
}
