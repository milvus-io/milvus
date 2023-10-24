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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/querynodev2/tsafe"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type PipelineManagerTestSuite struct {
	suite.Suite
	// data
	collectionID int64
	channel      string
	// dependencies
	tSafeManager TSafeManager
	delegators   *typeutil.ConcurrentMap[string, delegator.ShardDelegator]

	// mocks
	segmentManager    *segments.MockSegmentManager
	collectionManager *segments.MockCollectionManager
	delegator         *delegator.MockShardDelegator
	msgDispatcher     *msgdispatcher.MockClient
	msgChan           chan *msgstream.MsgPack
}

func (suite *PipelineManagerTestSuite) SetupSuite() {
	suite.collectionID = 111
	suite.msgChan = make(chan *msgstream.MsgPack, 1)
}

func (suite *PipelineManagerTestSuite) SetupTest() {
	paramtable.Init()
	// init dependency
	//	init tsafeManager
	suite.tSafeManager = tsafe.NewTSafeReplica()
	suite.tSafeManager.Add(context.Background(), suite.channel, 0)
	suite.delegators = typeutil.NewConcurrentMap[string, delegator.ShardDelegator]()

	// init mock
	//	init manager
	suite.collectionManager = segments.NewMockCollectionManager(suite.T())
	suite.segmentManager = segments.NewMockSegmentManager(suite.T())
	//	init delegator
	suite.delegator = delegator.NewMockShardDelegator(suite.T())
	suite.delegators.Insert(suite.channel, suite.delegator)
	//	init mq dispatcher
	suite.msgDispatcher = msgdispatcher.NewMockClient(suite.T())
}

func (suite *PipelineManagerTestSuite) TestBasic() {
	// init mock
	//  mock collection manager
	suite.collectionManager.EXPECT().Get(suite.collectionID).Return(&segments.Collection{})
	//  mock mq factory
	suite.msgDispatcher.EXPECT().Register(mock.Anything, suite.channel, mock.Anything, mqwrapper.SubscriptionPositionUnknown).Return(suite.msgChan, nil)
	suite.msgDispatcher.EXPECT().Deregister(suite.channel)

	// build manager
	manager := &segments.Manager{
		Collection: suite.collectionManager,
		Segment:    suite.segmentManager,
	}
	pipelineManager := NewManager(manager, suite.tSafeManager, suite.msgDispatcher, suite.delegators)
	defer pipelineManager.Close()

	// Add pipeline
	_, err := pipelineManager.Add(suite.collectionID, suite.channel)
	suite.NoError(err)
	suite.Equal(1, pipelineManager.Num())

	// Get pipeline
	pipeline := pipelineManager.Get(suite.channel)
	suite.NotNil(pipeline)

	// Init Consumer
	err = pipeline.ConsumeMsgStream(&msgpb.MsgPosition{})
	suite.NoError(err)

	// Start pipeline
	err = pipelineManager.Start(suite.channel)
	suite.NoError(err)

	// Remove pipeline
	pipelineManager.Remove(suite.channel)
	suite.Equal(0, pipelineManager.Num())
}

func TestQueryNodePipelineManager(t *testing.T) {
	suite.Run(t, new(PipelineManagerTestSuite))
}
