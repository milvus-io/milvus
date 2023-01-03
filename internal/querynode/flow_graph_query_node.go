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
	"time"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
)

type (
	// baseNode is type flowgraph.BaseNode
	baseNode = flowgraph.BaseNode

	// node is type flowgraph.Node
	node = flowgraph.Node
)

// queryNodeFlowGraph is a TimeTickedFlowGraph in query node
type queryNodeFlowGraph struct {
	collectionID UniqueID
	vchannel     Channel
	flowGraph    *flowgraph.TimeTickedFlowGraph
	dmlStream    msgstream.MsgStream
	tSafeReplica TSafeReplicaInterface
	consumerCnt  int
}

// newQueryNodeFlowGraph returns a new queryNodeFlowGraph
func newQueryNodeFlowGraph(ctx context.Context,
	collectionID UniqueID,
	metaReplica ReplicaInterface,
	tSafeReplica TSafeReplicaInterface,
	vchannel Channel,
	factory msgstream.Factory) (*queryNodeFlowGraph, error) {

	q := &queryNodeFlowGraph{
		collectionID: collectionID,
		vchannel:     vchannel,
		tSafeReplica: tSafeReplica,
		flowGraph:    flowgraph.NewTimeTickedFlowGraph(ctx),
	}

	dmStreamNode, err := q.newDmInputNode(ctx, factory, collectionID, vchannel, metrics.InsertLabel)
	if err != nil {
		return nil, err
	}
	var filterDmNode node = newFilteredDmNode(metaReplica, collectionID, vchannel)
	var insertNode node = newInsertNode(metaReplica, collectionID, vchannel)
	var serviceTimeNode node = newServiceTimeNode(tSafeReplica, collectionID, vchannel)

	q.flowGraph.AddNode(dmStreamNode)
	q.flowGraph.AddNode(filterDmNode)
	q.flowGraph.AddNode(insertNode)
	q.flowGraph.AddNode(serviceTimeNode)

	// dmStreamNode
	err = q.flowGraph.SetEdges(dmStreamNode.Name(),
		[]string{filterDmNode.Name()},
	)
	if err != nil {
		return nil, fmt.Errorf("set edges failed in node: %s, err = %s", dmStreamNode.Name(), err.Error())
	}

	// filterDmNode
	err = q.flowGraph.SetEdges(filterDmNode.Name(),
		[]string{insertNode.Name()},
	)
	if err != nil {
		return nil, fmt.Errorf("set edges failed in node: %s, err = %s", filterDmNode.Name(), err.Error())
	}

	// insertNode
	err = q.flowGraph.SetEdges(insertNode.Name(),
		[]string{serviceTimeNode.Name()},
	)
	if err != nil {
		return nil, fmt.Errorf("set edges failed in node: %s, err = %s", insertNode.Name(), err.Error())
	}

	// serviceTimeNode
	err = q.flowGraph.SetEdges(serviceTimeNode.Name(),
		[]string{},
	)
	if err != nil {
		return nil, fmt.Errorf("set edges failed in node: %s, err = %s", serviceTimeNode.Name(), err.Error())
	}

	return q, nil
}

// newQueryNodeDeltaFlowGraph returns a new queryNodeFlowGraph
func newQueryNodeDeltaFlowGraph(ctx context.Context,
	collectionID UniqueID,
	metaReplica ReplicaInterface,
	tSafeReplica TSafeReplicaInterface,
	vchannel Channel,
	factory msgstream.Factory) (*queryNodeFlowGraph, error) {

	q := &queryNodeFlowGraph{
		collectionID: collectionID,
		vchannel:     vchannel,
		tSafeReplica: tSafeReplica,
		flowGraph:    flowgraph.NewTimeTickedFlowGraph(ctx),
	}

	dmStreamNode, err := q.newDmInputNode(ctx, factory, collectionID, vchannel, metrics.DeleteLabel)
	if err != nil {
		return nil, err
	}
	var filterDeleteNode node = newFilteredDeleteNode(metaReplica, collectionID, vchannel)
	deleteNode, err := newDeleteNode(metaReplica, collectionID, vchannel)
	if err != nil {
		return nil, err
	}
	var serviceTimeNode node = newServiceTimeNode(tSafeReplica, collectionID, vchannel)

	q.flowGraph.AddNode(dmStreamNode)
	q.flowGraph.AddNode(filterDeleteNode)
	q.flowGraph.AddNode(deleteNode)
	q.flowGraph.AddNode(serviceTimeNode)

	// dmStreamNode
	err = q.flowGraph.SetEdges(dmStreamNode.Name(),
		[]string{filterDeleteNode.Name()},
	)
	if err != nil {
		return nil, fmt.Errorf("set edges failed in node: %s, err = %s", dmStreamNode.Name(), err.Error())
	}

	// filterDmNode
	err = q.flowGraph.SetEdges(filterDeleteNode.Name(),
		[]string{deleteNode.Name()},
	)
	if err != nil {
		return nil, fmt.Errorf("set edges failed in node: %s, err = %s", filterDeleteNode.Name(), err.Error())
	}

	// insertNode
	err = q.flowGraph.SetEdges(deleteNode.Name(),
		[]string{serviceTimeNode.Name()},
	)
	if err != nil {
		return nil, fmt.Errorf("set edges failed in node: %s, err = %s", deleteNode.Name(), err.Error())
	}

	// serviceTimeNode
	err = q.flowGraph.SetEdges(serviceTimeNode.Name(),
		[]string{},
	)
	if err != nil {
		return nil, fmt.Errorf("set edges failed in node: %s, err = %s", serviceTimeNode.Name(), err.Error())
	}

	return q, nil
}

// newDmInputNode returns a new inputNode

func (q *queryNodeFlowGraph) newDmInputNode(ctx context.Context, factory msgstream.Factory, collectionID UniqueID, vchannel Channel, dataType string) (*flowgraph.InputNode, error) {
	insertStream, err := factory.NewTtMsgStream(ctx)
	if err != nil {
		return nil, err
	}

	q.dmlStream = insertStream

	maxQueueLength := Params.QueryNodeCfg.FlowGraphMaxQueueLength.GetAsInt32()
	maxParallelism := Params.QueryNodeCfg.FlowGraphMaxParallelism.GetAsInt32()
	name := fmt.Sprintf("dmInputNode-query-%d-%s", collectionID, vchannel)
	node := flowgraph.NewInputNode(insertStream, name, maxQueueLength, maxParallelism, typeutil.QueryNodeRole,
		paramtable.GetNodeID(), collectionID, dataType)
	return node, nil
}

// consumeFlowGraph would consume by channel and subName
func (q *queryNodeFlowGraph) consumeFlowGraph(channel Channel, subName ConsumeSubName) error {
	if q.dmlStream == nil {
		return errors.New("null dml message stream in flow graph")
	}
	q.dmlStream.AsConsumer([]string{channel}, subName, mqwrapper.SubscriptionPositionUnknown)
	log.Info("query node flow graph consumes from PositionUnknown",
		zap.Int64("collectionID", q.collectionID),
		zap.String("pchannel", channel),
		zap.String("vchannel", q.vchannel),
		zap.String("subName", subName),
	)
	q.consumerCnt++
	metrics.QueryNodeNumConsumers.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Inc()
	return nil
}

// consumeFlowGraphFromLatest would consume from latest by channel and subName
func (q *queryNodeFlowGraph) consumeFlowGraphFromLatest(channel Channel, subName ConsumeSubName) error {
	if q.dmlStream == nil {
		return errors.New("null dml message stream in flow graph")
	}
	q.dmlStream.AsConsumer([]string{channel}, subName, mqwrapper.SubscriptionPositionLatest)
	log.Info("query node flow graph consumes from latest",
		zap.Int64("collectionID", q.collectionID),
		zap.String("pchannel", channel),
		zap.String("vchannel", q.vchannel),
		zap.String("subName", subName),
	)
	q.consumerCnt++
	metrics.QueryNodeNumConsumers.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Inc()
	return nil
}

// seekQueryNodeFlowGraph would seek by position
func (q *queryNodeFlowGraph) consumeFlowGraphFromPosition(position *internalpb.MsgPosition) error {
	q.dmlStream.AsConsumer([]string{position.ChannelName}, position.MsgGroup, mqwrapper.SubscriptionPositionUnknown)

	start := time.Now()
	err := q.dmlStream.Seek([]*internalpb.MsgPosition{position})
	// setup first ts
	q.tSafeReplica.setTSafe(q.vchannel, position.GetTimestamp())

	ts, _ := tsoutil.ParseTS(position.GetTimestamp())
	log.Info("query node flow graph seeks from position",
		zap.Int64("collectionID", q.collectionID),
		zap.String("pchannel", position.ChannelName),
		zap.String("vchannel", q.vchannel),
		zap.Time("checkpointTs", ts),
		zap.Duration("tsLag", time.Since(ts)),
		zap.Duration("elapse", time.Since(start)),
	)
	q.consumerCnt++
	metrics.QueryNodeNumConsumers.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Inc()
	return err
}

// close would close queryNodeFlowGraph
func (q *queryNodeFlowGraph) close() {
	q.flowGraph.Close()
	if q.dmlStream != nil && q.consumerCnt > 0 {
		metrics.QueryNodeNumConsumers.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Sub(float64(q.consumerCnt))
	}
	log.Info("stop query node flow graph",
		zap.Int64("collectionID", q.collectionID),
		zap.String("vchannel", q.vchannel),
	)

	metrics.CleanupQueryNodeCollectionMetrics(paramtable.GetNodeID(), q.collectionID)
}
