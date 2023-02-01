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
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/mq/msgdispatcher"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
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
	tSafeReplica TSafeReplicaInterface
	consumerCnt  int
	dispClient   msgdispatcher.Client
}

// newQueryNodeFlowGraph returns a new queryNodeFlowGraph
func newQueryNodeFlowGraph(ctx context.Context,
	collectionID UniqueID,
	metaReplica ReplicaInterface,
	tSafeReplica TSafeReplicaInterface,
	vchannel Channel,
	pos *msgstream.MsgPosition,
	dispClient msgdispatcher.Client) (*queryNodeFlowGraph, error) {

	q := &queryNodeFlowGraph{
		collectionID: collectionID,
		vchannel:     vchannel,
		tSafeReplica: tSafeReplica,
		flowGraph:    flowgraph.NewTimeTickedFlowGraph(ctx),
		dispClient:   dispClient,
	}

	dmStreamNode, err := q.newDmInputNode(collectionID, vchannel, pos, metrics.InsertLabel)
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
	dispClient msgdispatcher.Client) (*queryNodeFlowGraph, error) {

	q := &queryNodeFlowGraph{
		collectionID: collectionID,
		vchannel:     vchannel,
		tSafeReplica: tSafeReplica,
		flowGraph:    flowgraph.NewTimeTickedFlowGraph(ctx),
		dispClient:   dispClient,
	}

	// use nil position, let deltaFlowGraph consume from latest.
	dmStreamNode, err := q.newDmInputNode(collectionID, vchannel, nil, metrics.DeleteLabel)
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
func (q *queryNodeFlowGraph) newDmInputNode(collectionID UniqueID, vchannel Channel, pos *msgstream.MsgPosition, dataType string) (*flowgraph.InputNode, error) {
	log := log.With(zap.Int64("nodeID", paramtable.GetNodeID()),
		zap.Int64("collection ID", collectionID),
		zap.String("vchannel", vchannel))
	var err error
	var input <-chan *msgstream.MsgPack
	tsBegin := time.Now()
	if pos != nil && len(pos.MsgID) != 0 {
		input, err = q.dispClient.Register(vchannel, pos, mqwrapper.SubscriptionPositionUnknown)
		if err != nil {
			return nil, err
		}
		log.Info("QueryNode seek successfully when register to msgDispatcher",
			zap.ByteString("msgID", pos.GetMsgID()),
			zap.Time("tsTime", tsoutil.PhysicalTime(pos.GetTimestamp())),
			zap.Duration("tsLag", time.Since(tsoutil.PhysicalTime(pos.GetTimestamp()))),
			zap.Duration("timeTaken", time.Since(tsBegin)))
	} else {
		input, err = q.dispClient.Register(vchannel, nil, mqwrapper.SubscriptionPositionLatest)
		if err != nil {
			return nil, err
		}
		log.Info("QueryNode consume successfully when register to msgDispatcher",
			zap.Duration("timeTaken", time.Since(tsBegin)))
	}

	maxQueueLength := Params.QueryNodeCfg.FlowGraphMaxQueueLength.GetAsInt32()
	maxParallelism := Params.QueryNodeCfg.FlowGraphMaxParallelism.GetAsInt32()
	name := fmt.Sprintf("dmInputNode-query-%d-%s", collectionID, vchannel)
	node := flowgraph.NewInputNode(input, name, maxQueueLength, maxParallelism, typeutil.QueryNodeRole,
		paramtable.GetNodeID(), collectionID, dataType)
	return node, nil
}

// close would close queryNodeFlowGraph
func (q *queryNodeFlowGraph) close() {
	q.dispClient.Deregister(q.vchannel)
	q.flowGraph.Close()
	if q.consumerCnt > 0 {
		metrics.QueryNodeNumConsumers.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Sub(float64(q.consumerCnt))
	}
	log.Info("stop query node flow graph",
		zap.Int64("collectionID", q.collectionID),
		zap.String("vchannel", q.vchannel),
	)

	metrics.CleanupQueryNodeCollectionMetrics(paramtable.GetNodeID(), q.collectionID)
}
