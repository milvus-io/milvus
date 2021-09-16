// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package flowgraph

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/stretchr/testify/assert"
)

func TestInputNode(t *testing.T) {
	os.Setenv("ROCKSMQ_PATH", "/tmp/MilvusTest/FlowGraph/TestInputNode")
	msFactory := msgstream.NewRmsFactory()
	m := map[string]interface{}{}
	err := msFactory.SetParams(m)
	assert.Nil(t, err)

	msgStream, _ := msFactory.NewMsgStream(context.TODO())
	channels := []string{"cc"}
	msgStream.AsConsumer(channels, "sub")
	msgStream.Start()

	msgPack := generateMsgPack()
	produceStream, _ := msFactory.NewMsgStream(context.TODO())
	produceStream.AsProducer(channels)
	produceStream.Produce(&msgPack)

	nodeName := "input_node"
	inputNode := &InputNode{
		inStream: &msgStream,
		name:     nodeName,
	}
	inputNode.Close()

	isInputNode := inputNode.IsInputNode()
	assert.True(t, isInputNode)

	name := inputNode.Name()
	assert.Equal(t, name, nodeName)

	stream := inputNode.InStream()
	assert.NotNil(t, stream)

	var waitGroup sync.WaitGroup
	OperateFunc := func() {
		msgs := make([]Msg, 0)
		output := inputNode.Operate(msgs)
		assert.Greater(t, len(output), 0)
		msgStream.Close()
		waitGroup.Done()
	}

	waitGroup.Add(1)
	go OperateFunc()
	waitGroup.Wait()
}

func Test_NewInputNode(t *testing.T) {
	nodeName := "input_node"
	var maxQueueLength int32 = 0
	var maxParallelism int32 = 100
	node := NewInputNode(nil, nodeName, maxQueueLength, maxParallelism)
	assert.NotNil(t, node)
	assert.Equal(t, node.name, nodeName)
	assert.Equal(t, node.maxQueueLength, maxQueueLength)
	assert.Equal(t, node.maxParallelism, maxParallelism)
}
