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
	context2 "context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type StreamPipelineSuite struct {
	suite.Suite
	pipeline   StreamPipeline
	inChannel  chan *msgstream.MsgPack
	outChannel chan msgstream.Timestamp
	// data
	length  int
	channel string
	// mock
	msgDispatcher *msgdispatcher.MockClient
}

func (suite *StreamPipelineSuite) SetupTest() {
	paramtable.Init()
	suite.channel = "test-channel"
	suite.inChannel = make(chan *msgstream.MsgPack, 1)
	suite.outChannel = make(chan msgstream.Timestamp)
	suite.msgDispatcher = msgdispatcher.NewMockClient(suite.T())
	suite.msgDispatcher.EXPECT().Register(mock.Anything, suite.channel, mock.Anything, common.SubscriptionPositionUnknown).Return(suite.inChannel, nil)
	suite.msgDispatcher.EXPECT().Deregister(suite.channel)
	suite.pipeline = NewPipelineWithStream(suite.msgDispatcher, 0, false, suite.channel)
	suite.length = 4
}

func (suite *StreamPipelineSuite) TestBasic() {
	for i := 1; i <= suite.length; i++ {
		suite.pipeline.Add(&testNode{
			BaseNode: &BaseNode{
				name:           fmt.Sprintf("test-node-%d", i),
				maxQueueLength: 8,
			},
			outChannel: suite.outChannel,
		})
	}

	err := suite.pipeline.ConsumeMsgStream(context2.Background(), &msgpb.MsgPosition{})
	suite.NoError(err)

	suite.pipeline.Start()
	defer suite.pipeline.Close()
	suite.inChannel <- &msgstream.MsgPack{BeginTs: 1001}

	for i := 1; i <= suite.length; i++ {
		output := <-suite.outChannel
		suite.Equal(int64(1001), int64(output))
	}
}

func TestStreamPipeline(t *testing.T) {
	suite.Run(t, new(StreamPipelineSuite))
}
