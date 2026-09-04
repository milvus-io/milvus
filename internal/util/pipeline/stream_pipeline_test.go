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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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

func (suite *StreamPipelineSuite) TestDMLMsgPackBatcherMergesBufferedDeletePacks() {
	suite.pipeline = NewPipelineWithStream(
		suite.msgDispatcher,
		0,
		false,
		suite.channel,
		staticMVCCGetter{},
		WithMsgPackBatcher(NewDMLMsgPackBatcher(func() int { return 8 })),
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
			newDeleteTsMsg(101),
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

func (suite *StreamPipelineSuite) TestDMLMsgPackBatcherKeepsBufferedInsertPacksSeparate() {
	suite.pipeline = NewPipelineWithStream(
		suite.msgDispatcher,
		0,
		false,
		suite.channel,
		staticMVCCGetter{},
		WithMsgPackBatcher(NewDMLMsgPackBatcher(func() int { return 8 })),
	)

	received := make(chan *msgstream.MsgPack, 2)
	suite.pipeline.Add(&captureMsgPackNode{
		BaseNode:   NewBaseNode("capture-msg-pack", 8),
		outChannel: received,
	})

	err := suite.pipeline.ConsumeMsgStream(context2.Background(), &msgpb.MsgPosition{})
	suite.NoError(err)

	suite.inChannel <- newDMLMsgPack(100, 110, newInsertTsMsg(101))
	suite.inChannel <- newDMLMsgPack(111, 120, newInsertTsMsg(115))

	suite.pipeline.Start()
	defer suite.pipeline.Close()

	first := <-received
	suite.Require().Equal(uint64(100), first.BeginTs)
	suite.Require().Equal(uint64(110), first.EndTs)
	suite.Require().Len(first.Msgs, 1)

	second := <-received
	suite.Equal(uint64(111), second.BeginTs)
	suite.Equal(uint64(120), second.EndTs)
	suite.Len(second.Msgs, 1)
}

func (suite *StreamPipelineSuite) TestClosePreClosesNodesBeforeWaitingWorkDone() {
	node := &preCloseBlockingNode{
		BaseNode:  NewBaseNode("pre-close-blocking", 8),
		operating: make(chan struct{}),
		preClosed: make(chan struct{}),
	}
	suite.pipeline.Add(node)

	err := suite.pipeline.ConsumeMsgStream(context2.Background(), &msgpb.MsgPosition{})
	suite.NoError(err)
	suite.NoError(suite.pipeline.Start())
	suite.inChannel <- &msgstream.MsgPack{BeginTs: 1001}

	suite.Eventually(func() bool {
		select {
		case <-node.operating:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)

	done := make(chan struct{})
	go func() {
		defer close(done)
		suite.pipeline.Close()
	}()

	suite.Eventually(func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func TestDMLMsgPackBatcherLimitsByTotalMessageNum(t *testing.T) {
	maxMsgNum := 3
	batcher := NewDMLMsgPackBatcher(func() int { return maxMsgNum })
	input := make(chan *msgstream.MsgPack, 1)
	input <- newDMLMsgPack(110, 120, newDeleteTsMsg(111), newDeleteTsMsg(112))

	merged, pending := batcher.Batch(newDMLMsgPack(100, 109, newDeleteTsMsg(101)), input)

	require.NotNil(t, merged)
	assert.Len(t, merged.Msgs, 3)
	assert.Equal(t, uint64(100), merged.BeginTs)
	assert.Equal(t, uint64(120), merged.EndTs)
	assert.Nil(t, pending)

	maxMsgNum = 2
	input = make(chan *msgstream.MsgPack, 1)
	input <- newDMLMsgPack(211, 220, newDeleteTsMsg(211))
	merged, pending = batcher.Batch(newDMLMsgPack(200, 210, newDeleteTsMsg(201)), input)

	require.NotNil(t, merged)
	assert.Len(t, merged.Msgs, 2)
	assert.Equal(t, uint64(200), merged.BeginTs)
	assert.Equal(t, uint64(220), merged.EndTs)
	assert.Nil(t, pending)

	input = make(chan *msgstream.MsgPack, 1)
	input <- newDMLMsgPack(311, 320, newDeleteTsMsg(311), newDeleteTsMsg(312))
	merged, pending = batcher.Batch(newDMLMsgPack(300, 310, newDeleteTsMsg(301)), input)

	require.NotNil(t, merged)
	assert.Len(t, merged.Msgs, 1)
	assert.Equal(t, uint64(300), merged.BeginTs)
	assert.Equal(t, uint64(310), merged.EndTs)
	require.NotNil(t, pending)
	assert.Len(t, pending.Msgs, 2)
	assert.Equal(t, uint64(311), pending.BeginTs)
}

func TestDMLMsgPackBatcherTreatsInsertPacksAsBarriers(t *testing.T) {
	tests := []struct {
		name        string
		first       *msgstream.MsgPack
		next        *msgstream.MsgPack
		wantPending bool
	}{
		{
			name:  "insert first does not drain next pack",
			first: newDMLMsgPack(100, 110, newInsertTsMsg(101)),
			next:  newDMLMsgPack(111, 120, newDeleteTsMsg(115)),
		},
		{
			name:        "delete batch stops before insert",
			first:       newDMLMsgPack(200, 210, newDeleteTsMsg(201)),
			next:        newDMLMsgPack(211, 220, newInsertTsMsg(215)),
			wantPending: true,
		},
		{
			name:        "delete batch stops before mixed insert delete pack",
			first:       newDMLMsgPack(230, 240, newDeleteTsMsg(231)),
			next:        newDMLMsgPack(241, 250, newInsertTsMsg(245), newDeleteTsMsg(246)),
			wantPending: true,
		},
		{
			name:  "mixed first pack does not drain next pack",
			first: newDMLMsgPack(300, 310, newInsertTsMsg(301), newDeleteTsMsg(302)),
			next:  newDMLMsgPack(311, 320, newDeleteTsMsg(315)),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			batcher := NewDMLMsgPackBatcher(func() int { return 8 })
			input := make(chan *msgstream.MsgPack, 1)
			input <- test.next

			merged, pending := batcher.Batch(test.first, input)

			assert.Same(t, test.first, merged)
			if test.wantPending {
				assert.Same(t, test.next, pending)
				assert.Empty(t, input)
			} else {
				assert.Nil(t, pending)
				assert.Len(t, input, 1)
			}
		})
	}
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

type preCloseBlockingNode struct {
	*BaseNode
	operating chan struct{}
	preClosed chan struct{}
}

func (node *preCloseBlockingNode) Operate(in Msg) Msg {
	close(node.operating)
	<-node.preClosed
	return in
}

func (node *preCloseBlockingNode) PreClose() {
	close(node.preClosed)
}

func newDMLMsgPack(beginTs, endTs uint64, msgs ...msgstream.TsMsg) *msgstream.MsgPack {
	return &msgstream.MsgPack{
		BeginTs:        beginTs,
		EndTs:          endTs,
		Msgs:           msgs,
		StartPositions: []*msgpb.MsgPosition{{Timestamp: beginTs}},
		EndPositions:   []*msgpb.MsgPosition{{Timestamp: endTs}},
	}
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
