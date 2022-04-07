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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/dependency"
)

func generateMsgPack() msgstream.MsgPack {
	msgPack := msgstream.MsgPack{}
	baseMsg := msgstream.BaseMsg{
		BeginTimestamp: uint64(time.Now().Unix()),
		EndTimestamp:   uint64(time.Now().Unix() + 1),
		HashValues:     []uint32{0},
	}
	timeTickResult := internalpb.TimeTickMsg{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_TimeTick,
			MsgID:     0,
			Timestamp: math.MaxUint64,
			SourceID:  0,
		},
	}
	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg:     baseMsg,
		TimeTickMsg: timeTickResult,
	}
	msgPack.Msgs = append(msgPack.Msgs, timeTickMsg)

	return msgPack
}

func TestNodeCtx_Start(t *testing.T) {
	os.Setenv("ROCKSMQ_PATH", "/tmp/MilvusTest/FlowGraph/TestNodeStart")
	factory := dependency.NewDefaultFactory(true)

	msgStream, _ := factory.NewMsgStream(context.TODO())
	channels := []string{"cc"}
	msgStream.AsConsumer(channels, "sub")

	produceStream, _ := factory.NewMsgStream(context.TODO())
	produceStream.AsProducer(channels)

	msgPack := generateMsgPack()
	produceStream.Produce(&msgPack)
	time.Sleep(time.Millisecond * 2)
	msgPack = generateMsgPack()
	produceStream.Produce(&msgPack)

	nodeName := "input_node"
	inputNode := &InputNode{
		inStream: msgStream,
		name:     nodeName,
	}

	node := &nodeCtx{
		node:                   inputNode,
		inputChannels:          make([]chan Msg, 2),
		downstreamInputChanIdx: make(map[string]int),
		closeCh:                make(chan struct{}),
	}

	for i := 0; i < len(node.inputChannels); i++ {
		node.inputChannels[i] = make(chan Msg)
	}

	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	node.Start(&waitGroup)

	node.Close()
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
