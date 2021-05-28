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

package querynode

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

type queryNodeFlowGraph struct {
	channel   VChannel
	flowGraph *flowgraph.TimeTickedFlowGraph
	dmlStream msgstream.MsgStream
}

func newQueryNodeFlowGraph(ctx context.Context,
	flowGraphType flowGraphType,
	collectionID UniqueID,
	partitionID UniqueID,
	streamingReplica ReplicaInterface,
	tSafeReplica TSafeReplicaInterface,
	channel VChannel,
	subName ConsumeSubName,
	factory msgstream.Factory) *queryNodeFlowGraph {

	q := &queryNodeFlowGraph{
		channel:   channel,
		flowGraph: flowgraph.NewTimeTickedFlowGraph(ctx),
	}

	var dmStreamNode node = q.newDmInputNode(ctx, channel, subName, factory)
	var filterDmNode node = newFilteredDmNode(streamingReplica, flowGraphType, collectionID, partitionID)
	var insertNode node = newInsertNode(streamingReplica)
	var serviceTimeNode node = newServiceTimeNode(ctx, tSafeReplica, channel, factory)

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

func (q *queryNodeFlowGraph) newDmInputNode(ctx context.Context,
	channel VChannel,
	subName ConsumeSubName,
	factory msgstream.Factory,
) *flowgraph.InputNode {

	insertStream, _ := factory.NewTtMsgStream(ctx)
	insertStream.AsConsumer([]string{channel}, subName)
	q.dmlStream = insertStream

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	node := flowgraph.NewInputNode(&insertStream, "dmlInputNode", maxQueueLength, maxParallelism)
	return node
}

func (q *queryNodeFlowGraph) seekQueryNodeFlowGraph(position *internalpb.MsgPosition) error {
	err := q.dmlStream.Seek(position)
	return err
}
