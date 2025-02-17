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

package flowgraph

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
)

func generateMsgPack() msgstream.MsgPack {
	msgPack := msgstream.MsgPack{}

	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: uint64(time.Now().Unix()),
			EndTimestamp:   uint64(time.Now().Unix() + 1),
			HashValues:     []uint32{0},
		},
		TimeTickMsg: &msgpb.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_TimeTick,
				MsgID:     0,
				Timestamp: math.MaxUint64,
				SourceID:  0,
			},
		},
	}
	msgPack.Msgs = append(msgPack.Msgs, timeTickMsg)

	return msgPack
}

func generateInsertMsgPack() msgstream.MsgPack {
	msgPack := msgstream.MsgPack{}
	insertMsg := &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: uint64(time.Now().Unix()),
			EndTimestamp:   uint64(time.Now().Unix() + 1),
			HashValues:     []uint32{0},
		},
		InsertRequest: &msgpb.InsertRequest{
			Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
		},
	}
	msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	return msgPack
}

func TestNodeManager_Start(t *testing.T) {
	t.Setenv("ROCKSMQ_PATH", "/tmp/MilvusTest/FlowGraph/TestNodeStart")
	factory := dependency.NewDefaultFactory(true)

	msgStream, _ := factory.NewMsgStream(context.TODO())
	channels := []string{"cc"}
	msgStream.AsConsumer(context.TODO(), channels, "sub", common.SubscriptionPositionEarliest)

	produceStream, _ := factory.NewMsgStream(context.TODO())
	produceStream.AsProducer(context.TODO(), channels)

	msgPack := generateMsgPack()
	produceStream.Produce(context.TODO(), &msgPack)
	time.Sleep(time.Millisecond * 2)
	msgPack = generateMsgPack()
	produceStream.Produce(context.TODO(), &msgPack)

	nodeName := "input_node"
	dispatcher := msgstream.NewSimpleMsgDispatcher(msgStream, func(pm msgstream.ConsumeMsg) bool { return true })
	inputNode := NewInputNode(dispatcher.Chan(), nodeName, 100, 100, "", 0, 0, "")

	ddNode := BaseNode{}

	node0 := &nodeCtx{
		node: inputNode,
	}

	node1 := &nodeCtx{
		node: &ddNode,
	}

	node0.downstream = node1

	node0.inputChannel = make(chan []Msg)

	nodeCtxManager := NewNodeCtxManager(node0, &sync.WaitGroup{})
	assert.NotPanics(t, func() {
		nodeCtxManager.Start()
	})

	nodeCtxManager.Close()
}

func TestBaseNode(t *testing.T) {
	node := &BaseNode{
		maxQueueLength: 10,
		maxParallelism: 50,
	}

	x := node.MaxQueueLength()
	assert.Equal(t, x, node.maxQueueLength)

	x = node.MaxParallelism()
	assert.Equal(t, x, node.maxParallelism)

	var val int32 = 3
	node.SetMaxQueueLength(val)
	assert.Equal(t, val, node.maxQueueLength)

	node.SetMaxParallelism(val)
	assert.Equal(t, val, node.maxParallelism)

	assert.Equal(t, false, node.IsInputNode())

	node.Close()
}
