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

package querynode

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/mqclient"
)

// queryNodeFlowGraph is a TimeTickedFlowGraph in query node
type queryNodeFlowGraph struct {
	ctx          context.Context
	cancel       context.CancelFunc
	collectionID UniqueID
	channel      Channel
	flowGraph    *flowgraph.TimeTickedFlowGraph
	dmlStream    msgstream.MsgStream
}

// newQueryNodeFlowGraph returns a new queryNodeFlowGraph
func newQueryNodeFlowGraph(ctx context.Context,
	collectionID UniqueID,
	streamingReplica ReplicaInterface,
	tSafeReplica TSafeReplicaInterface,
	channel Channel,
	factory msgstream.Factory) *queryNodeFlowGraph {

	ctx1, cancel := context.WithCancel(ctx)

	q := &queryNodeFlowGraph{
		ctx:          ctx1,
		cancel:       cancel,
		collectionID: collectionID,
		channel:      channel,
		flowGraph:    flowgraph.NewTimeTickedFlowGraph(ctx1),
	}

	var dmStreamNode node = q.newDmInputNode(ctx1, factory)
	var filterDmNode node = newFilteredDmNode(streamingReplica, collectionID)
	var insertNode node = newInsertNode(streamingReplica)
	var serviceTimeNode node = newServiceTimeNode(ctx1, tSafeReplica, collectionID, channel)

	q.flowGraph.AddNode(dmStreamNode)
	q.flowGraph.AddNode(filterDmNode)
	q.flowGraph.AddNode(insertNode)
	q.flowGraph.AddNode(serviceTimeNode)

	// dmStreamNode
	var err = q.flowGraph.SetEdges(dmStreamNode.Name(),
		[]string{},
		[]string{filterDmNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node:", zap.String("node name", dmStreamNode.Name()))
	}

	// filterDmNode
	err = q.flowGraph.SetEdges(filterDmNode.Name(),
		[]string{dmStreamNode.Name()},
		[]string{insertNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node:", zap.String("node name", filterDmNode.Name()))
	}

	// insertNode
	err = q.flowGraph.SetEdges(insertNode.Name(),
		[]string{filterDmNode.Name()},
		[]string{serviceTimeNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node:", zap.String("node name", insertNode.Name()))
	}

	// serviceTimeNode
	err = q.flowGraph.SetEdges(serviceTimeNode.Name(),
		[]string{insertNode.Name()},
		[]string{},
	)
	if err != nil {
		log.Error("set edges failed in node:", zap.String("node name", serviceTimeNode.Name()))
	}

	return q
}

// newQueryNodeDeltaFlowGraph returns a new queryNodeFlowGraph
func newQueryNodeDeltaFlowGraph(ctx context.Context,
	collectionID UniqueID,
	historicalReplica ReplicaInterface,
	tSafeReplica TSafeReplicaInterface,
	channel Channel,
	factory msgstream.Factory) *queryNodeFlowGraph {

	ctx1, cancel := context.WithCancel(ctx)

	q := &queryNodeFlowGraph{
		ctx:          ctx1,
		cancel:       cancel,
		collectionID: collectionID,
		channel:      channel,
		flowGraph:    flowgraph.NewTimeTickedFlowGraph(ctx1),
	}

	var dmStreamNode node = q.newDmInputNode(ctx1, factory)
	var filterDeleteNode node = newFilteredDeleteNode(historicalReplica, collectionID)
	var deleteNode node = newDeleteNode(historicalReplica)
	var serviceTimeNode node = newServiceTimeNode(ctx1, tSafeReplica, collectionID, channel)

	q.flowGraph.AddNode(dmStreamNode)
	q.flowGraph.AddNode(filterDeleteNode)
	q.flowGraph.AddNode(deleteNode)
	q.flowGraph.AddNode(serviceTimeNode)

	// dmStreamNode
	var err = q.flowGraph.SetEdges(dmStreamNode.Name(),
		[]string{},
		[]string{filterDeleteNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node:", zap.String("node name", dmStreamNode.Name()))
	}

	// filterDmNode
	err = q.flowGraph.SetEdges(filterDeleteNode.Name(),
		[]string{dmStreamNode.Name()},
		[]string{deleteNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node:", zap.String("node name", filterDeleteNode.Name()))
	}

	// insertNode
	err = q.flowGraph.SetEdges(deleteNode.Name(),
		[]string{filterDeleteNode.Name()},
		[]string{serviceTimeNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node:", zap.String("node name", deleteNode.Name()))
	}

	// serviceTimeNode
	err = q.flowGraph.SetEdges(serviceTimeNode.Name(),
		[]string{deleteNode.Name()},
		[]string{},
	)
	if err != nil {
		log.Error("set edges failed in node:", zap.String("node name", serviceTimeNode.Name()))
	}

	return q
}

// newDmInputNode returns a new inputNode
func (q *queryNodeFlowGraph) newDmInputNode(ctx context.Context, factory msgstream.Factory) *flowgraph.InputNode {
	insertStream, err := factory.NewTtMsgStream(ctx)
	if err != nil {
		log.Warn(err.Error())
	} else {
		q.dmlStream = insertStream
	}

	maxQueueLength := Params.QueryNodeCfg.FlowGraphMaxQueueLength
	maxParallelism := Params.QueryNodeCfg.FlowGraphMaxParallelism

	node := flowgraph.NewInputNode(insertStream, "dmlInputNode", maxQueueLength, maxParallelism)
	return node
}

// consumerFlowGraph would consume by channel and subName
func (q *queryNodeFlowGraph) consumerFlowGraph(channel Channel, subName ConsumeSubName) error {
	if q.dmlStream == nil {
		return errors.New("null dml message stream in flow graph")
	}
	q.dmlStream.AsConsumer([]string{channel}, subName)
	log.Debug("query node flow graph consumes from pChannel",
		zap.Any("collectionID", q.collectionID),
		zap.Any("channel", channel),
		zap.Any("subName", subName),
	)

	metrics.QueryNodeNumConsumers.WithLabelValues(fmt.Sprint(q.collectionID), fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Inc()
	return nil
}

// consumerFlowGraphLatest would consume from latest by channel and subName
func (q *queryNodeFlowGraph) consumerFlowGraphLatest(channel Channel, subName ConsumeSubName) error {
	if q.dmlStream == nil {
		return errors.New("null dml message stream in flow graph")
	}
	q.dmlStream.AsConsumerWithPosition([]string{channel}, subName, mqclient.SubscriptionPositionLatest)
	log.Debug("query node flow graph consumes from pChannel",
		zap.Any("collectionID", q.collectionID),
		zap.Any("channel", channel),
		zap.Any("subName", subName),
	)

	metrics.QueryNodeNumConsumers.WithLabelValues(fmt.Sprint(q.collectionID), fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Inc()
	return nil
}

// seekQueryNodeFlowGraph would seek by position
func (q *queryNodeFlowGraph) seekQueryNodeFlowGraph(position *internalpb.MsgPosition) error {
	q.dmlStream.AsConsumer([]string{position.ChannelName}, position.MsgGroup)
	err := q.dmlStream.Seek([]*internalpb.MsgPosition{position})
	log.Debug("query node flow graph seeks from pChannel",
		zap.Any("collectionID", q.collectionID),
		zap.Any("channel", position.ChannelName),
	)

	metrics.QueryNodeNumConsumers.WithLabelValues(fmt.Sprint(q.collectionID), fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Inc()
	return err
}

// close would close queryNodeFlowGraph
func (q *queryNodeFlowGraph) close() {
	q.cancel()
	q.flowGraph.Close()
	log.Debug("stop query node flow graph",
		zap.Any("collectionID", q.collectionID),
		zap.Any("channel", q.channel),
	)
}
