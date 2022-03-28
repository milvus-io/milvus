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
	"os"
	"testing"

	"github.com/milvus-io/milvus/internal/util/paramtable"

	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/stretchr/testify/assert"
)

func TestInputNode(t *testing.T) {
	os.Setenv("ROCKSMQ_PATH", "/tmp/MilvusTest/FlowGraph/TestInputNode")
	msFactory := msgstream.NewRmsFactory()
	var Params paramtable.ComponentParam
	err := msFactory.Init(&Params)
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
		inStream: msgStream,
		name:     nodeName,
	}
	defer inputNode.Close()

	isInputNode := inputNode.IsInputNode()
	assert.True(t, isInputNode)

	name := inputNode.Name()
	assert.Equal(t, name, nodeName)

	stream := inputNode.InStream()
	assert.NotNil(t, stream)

	output := inputNode.Operate([]Msg{})
	assert.Greater(t, len(output), 0)
}

func Test_NewInputNode(t *testing.T) {
	nodeName := "input_node"
	var maxQueueLength int32
	var maxParallelism int32 = 100
	node := NewInputNode(nil, nodeName, maxQueueLength, maxParallelism)
	assert.NotNil(t, node)
	assert.Equal(t, node.name, nodeName)
	assert.Equal(t, node.maxQueueLength, maxQueueLength)
	assert.Equal(t, node.maxParallelism, maxParallelism)
}
