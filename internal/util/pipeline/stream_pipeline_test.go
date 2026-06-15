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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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
	suite.inChannel = make(chan *msgstream.MsgPack, 4)
	suite.outChannel = make(chan msgstream.Timestamp)
	suite.msgDispatcher = msgdispatcher.NewMockClient(suite.T())
	suite.msgDispatcher.EXPECT().Register(mock.Anything, mock.Anything).Return(suite.inChannel, nil)
	suite.msgDispatcher.EXPECT().Deregister(suite.channel)
	suite.pipeline = NewPipelineWithStream(suite.msgDispatcher, 0, false, suite.channel, staticMVCCGetter{})
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

func (suite *StreamPipelineSuite) TestDMLMsgPackBatcherMergesBufferedDMLPacks() {
	suite.pipeline = NewPipelineWithStream(
		suite.msgDispatcher,
		0,
		false,
		suite.channel,
		staticMVCCGetter{},
		WithMsgPackBatcher(NewDMLMsgPackBatcher(8)),
	)

	received := make(chan *msgstream.MsgPack, 1)
	suite.pipeline.Add(&captureMsgPackNode{
		BaseNode:   NewBaseNode("capture-msg-pack", 8),
		outChannel: received,
	})

	err := suite.pipeline.ConsumeMsgStream(context2.Background(), &msgpb.MsgPosition{})
	suite.NoError(err)

	suite.inChannel <- &msgstream.MsgPack{
		BeginTs: 100,
		EndTs:   110,
		Msgs: []msgstream.TsMsg{
			newInsertTsMsg(101),
		},
		StartPositions: []*msgpb.MsgPosition{{Timestamp: 100}},
		EndPositions:   []*msgpb.MsgPosition{{Timestamp: 110}},
	}
	suite.inChannel <- &msgstream.MsgPack{
		BeginTs: 111,
		EndTs:   120,
		Msgs: []msgstream.TsMsg{
			newDeleteTsMsg(115),
		},
		StartPositions: []*msgpb.MsgPosition{{Timestamp: 111}},
		EndPositions:   []*msgpb.MsgPosition{{Timestamp: 120}},
	}

	suite.pipeline.Start()
	defer suite.pipeline.Close()

	merged := <-received
	suite.Equal(uint64(100), merged.BeginTs)
	suite.Equal(uint64(120), merged.EndTs)
	suite.Len(merged.Msgs, 2)
	suite.Equal(uint64(101), merged.Msgs[0].EndTs())
	suite.Equal(uint64(115), merged.Msgs[1].EndTs())
	suite.Equal(uint64(100), merged.StartPositions[0].GetTimestamp())
	suite.Equal(uint64(120), merged.EndPositions[0].GetTimestamp())
	suite.Empty(received)
}

func TestStreamPipeline(t *testing.T) {
	suite.Run(t, new(StreamPipelineSuite))
}

type staticMVCCGetter struct{}

func (staticMVCCGetter) GetLatestRequiredMVCCTimeTick() uint64 {
	return 0
}

type captureMsgPackNode struct {
	*BaseNode
	outChannel chan *msgstream.MsgPack
}

func (node *captureMsgPackNode) Operate(in Msg) Msg {
	node.outChannel <- in.(*msgstream.MsgPack)
	return nil
}

func newInsertTsMsg(ts uint64) *msgstream.InsertMsg {
	msg := &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{},
		InsertRequest: &msgpb.InsertRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_Insert),
				commonpbutil.WithTimeStamp(ts),
			),
		},
	}
	msg.SetTs(ts)
	return msg
}

func newDeleteTsMsg(ts uint64) *msgstream.DeleteMsg {
	msg := &msgstream.DeleteMsg{
		BaseMsg: msgstream.BaseMsg{},
		DeleteRequest: &msgpb.DeleteRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_Delete),
				commonpbutil.WithTimeStamp(ts),
			),
		},
	}
	msg.SetTs(ts)
	return msg
}
