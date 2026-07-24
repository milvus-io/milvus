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
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestInputNode(t *testing.T) {
	t.Setenv("ROCKSMQ_PATH", "/tmp/MilvusTest/FlowGraph/TestInputNode")
	factory := dependency.NewDefaultFactory(true)

	msgStream, _ := factory.NewMsgStream(context.TODO())
	channels := []string{"cc"}
	msgStream.AsConsumer(context.Background(), channels, "sub", common.SubscriptionPositionEarliest)

	msgPack := generateMsgPack()
	produceStream, _ := factory.NewMsgStream(context.TODO())
	produceStream.AsProducer(context.TODO(), channels)
	produceStream.Produce(context.TODO(), &msgPack)

	dispatcher := msgstream.NewSimpleMsgDispatcher(msgStream, func(pm msgstream.ConsumeMsg) bool { return true })
	nodeName := "input_node"
	inputNode := NewInputNode(dispatcher.Chan(), nodeName, 100, 100, "", 0, 0, "")
	defer inputNode.Close()

	isInputNode := inputNode.IsInputNode()
	assert.True(t, isInputNode)

	name := inputNode.Name()
	assert.Equal(t, name, nodeName)

	output := inputNode.Operate(nil)
	assert.NotNil(t, output)
	msg, ok := output[0].(*MsgStreamMsg)
	assert.True(t, ok)
	assert.False(t, msg.isCloseMsg)
}

func Test_NewInputNode(t *testing.T) {
	nodeName := "input_node"
	var maxQueueLength int32
	var maxParallelism int32 = 100
	node := NewInputNode(nil, nodeName, maxQueueLength, maxParallelism, "", 0, 0, "")
	assert.NotNil(t, node)
	assert.Equal(t, node.name, nodeName)
	assert.Equal(t, node.maxQueueLength, maxQueueLength)
	assert.Equal(t, node.maxParallelism, maxParallelism)
}

func TestInputNodeMetricsHandlesSurvivePartialCleanup(t *testing.T) {
	metrics.DataNodeConsumeMsgCount.Reset()
	metrics.DataNodeConsumeTimeTickLag.Reset()

	const (
		nodeID       = int64(10101)
		collectionID = int64(20202)
	)
	node1 := NewInputNode(nil, "input-node-1", 0, 1, typeutil.DataNodeRole, nodeID, collectionID, metrics.AllLabel)
	node2 := NewInputNode(nil, "input-node-2", 0, 1, typeutil.DataNodeRole, nodeID, collectionID, metrics.AllLabel)

	node1.consumeMsgCount.Inc()
	node1.consumeTimeTickLag.Set(100)
	node1.Free()
	node1.Free()

	// Closing one channel still runs collection cleanup. The surviving InputNode
	// must keep updating the exported AllLabel metrics with its cached handles.
	metrics.CleanupDataNodeCollectionMetrics(nodeID, collectionID, "unused-channel")
	node2.consumeMsgCount.Inc()
	node2.consumeTimeTickLag.Set(200)

	assert.Equal(t, float64(2), testutil.ToFloat64(node2.consumeMsgCount))
	assert.Equal(t, float64(200), testutil.ToFloat64(node2.consumeTimeTickLag))
	assert.Equal(t, 1, testutil.CollectAndCount(metrics.DataNodeConsumeMsgCount))
	assert.Equal(t, 1, testutil.CollectAndCount(metrics.DataNodeConsumeTimeTickLag))

	node2.Free()
	assert.Equal(t, 0, testutil.CollectAndCount(metrics.DataNodeConsumeMsgCount))
	assert.Equal(t, 0, testutil.CollectAndCount(metrics.DataNodeConsumeTimeTickLag))
}

func Test_InputNodeSkipMode(t *testing.T) {
	t.Setenv("ROCKSMQ_PATH", "/tmp/MilvusTest/FlowGraph/Test_InputNodeSkipMode")
	factory := dependency.NewDefaultFactory(true)
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.FlowGraphSkipModeColdTime.Key, "3")
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.FlowGraphSkipModeSkipNum.Key, "1")

	msgStream, _ := factory.NewMsgStream(context.TODO())
	channels := []string{"cc" + fmt.Sprint(rand.Int())}
	msgStream.AsConsumer(context.Background(), channels, "sub", common.SubscriptionPositionEarliest)

	produceStream, _ := factory.NewMsgStream(context.TODO())
	produceStream.AsProducer(context.TODO(), channels)
	closeCh := make(chan struct{})
	outputCh := make(chan bool)

	nodeName := "input_node"
	dispatcher := msgstream.NewSimpleMsgDispatcher(msgStream, func(pm msgstream.ConsumeMsg) bool { return true })
	inputNode := NewInputNode(dispatcher.Chan(), nodeName, 100, 100, typeutil.DataNodeRole, 0, 0, "")
	defer inputNode.Free()

	outputCount := 0
	go func() {
		for {
			select {
			case <-closeCh:
				return
			default:
				output := inputNode.Operate(nil)
				if len(output) > 0 {
					outputCount = outputCount + 1
				}
				outputCh <- true
			}
		}
	}()
	defer close(closeCh)

	msgPack := generateMsgPack()
	produceStream.Produce(context.TODO(), &msgPack)
	mlog.Info(context.TODO(), "produce empty ttmsg")
	<-outputCh
	assert.Equal(t, 1, outputCount)
	assert.Equal(t, false, inputNode.skipMode)

	time.Sleep(3 * time.Second)
	assert.Equal(t, false, inputNode.skipMode)
	produceStream.Produce(context.TODO(), &msgPack)
	mlog.Info(context.TODO(), "after 3 seconds with no active msg receive, input node will turn on skip mode")
	<-outputCh
	assert.Equal(t, 2, outputCount)
	assert.Equal(t, true, inputNode.skipMode)

	mlog.Info(context.TODO(), "some ttmsg will be skipped in skip mode")
	// this msg will be skipped
	produceStream.Produce(context.TODO(), &msgPack)
	<-outputCh
	assert.Equal(t, 2, outputCount)
	assert.Equal(t, true, inputNode.skipMode)

	// this msg will be consumed
	produceStream.Produce(context.TODO(), &msgPack)
	<-outputCh
	assert.Equal(t, 3, outputCount)
	assert.Equal(t, true, inputNode.skipMode)

	//log.Info("non empty msg will awake input node, turn off skip mode")
	//insertMsgPack := generateInsertMsgPack()
	//produceStream.Produce(&insertMsgPack)
	//<-outputCh
	//assert.Equal(t, 3, outputCount)
	//assert.Equal(t, false, inputNode.skipMode)
	//
	//log.Info("empty msg will be consumed in not-skip mode")
	//produceStream.Produce(&msgPack)
	//<-outputCh
	//assert.Equal(t, 4, outputCount)
	//assert.Equal(t, false, inputNode.skipMode)
	//close(closeCh)
}
