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
	"fmt"

	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/trace"
)

// filterDeleteNode is one of the nodes in delta flow graph
type filterDeleteNode struct {
	baseNode
	collectionID UniqueID
	replica      ReplicaInterface
}

// Name returns the name of filterDeleteNode
func (fddNode *filterDeleteNode) Name() string {
	return fmt.Sprintf("fdNode-%d", fddNode.collectionID)
}

// Operate handles input messages, to filter invalid delete messages
func (fddNode *filterDeleteNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {
	if len(in) != 1 {
		log.Warn("Invalid operate message input in filterDDNode", zap.Int("input length", len(in)))
		return []Msg{}
	}

	msgStreamMsg, ok := in[0].(*MsgStreamMsg)
	if !ok {
		log.Warn("type assertion failed for MsgStreamMsg")
		return []Msg{}
	}

	if msgStreamMsg == nil {
		return []Msg{}
	}

	var spans []opentracing.Span
	for _, msg := range msgStreamMsg.TsMessages() {
		sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}

	var dMsg = deleteMsg{
		deleteMessages: make([]*msgstream.DeleteMsg, 0),
		timeRange: TimeRange{
			timestampMin: msgStreamMsg.TimestampMin(),
			timestampMax: msgStreamMsg.TimestampMax(),
		},
	}

	for _, msg := range msgStreamMsg.TsMessages() {
		switch msg.Type() {
		case commonpb.MsgType_Delete:
			resMsg := fddNode.filterInvalidDeleteMessage(msg.(*msgstream.DeleteMsg))
			if resMsg != nil {
				dMsg.deleteMessages = append(dMsg.deleteMessages, resMsg)
			}
		default:
			log.Warn("Non supporting", zap.Int32("message type", int32(msg.Type())))
		}
	}
	var res Msg = &dMsg
	for _, sp := range spans {
		sp.Finish()
	}
	return []Msg{res}
}

// filterInvalidDeleteMessage would filter invalid delete messages
func (fddNode *filterDeleteNode) filterInvalidDeleteMessage(msg *msgstream.DeleteMsg) *msgstream.DeleteMsg {
	sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	msg.SetTraceCtx(ctx)
	defer sp.Finish()

	if msg.CollectionID != fddNode.collectionID {
		return nil
	}

	if len(msg.PrimaryKeys) != len(msg.Timestamps) {
		log.Warn("Error, misaligned messages detected")
		return nil
	}

	if len(msg.Timestamps) <= 0 {
		log.Debug("filter invalid delete message, no message",
			zap.Any("collectionID", msg.CollectionID),
			zap.Any("partitionID", msg.PartitionID))
		return nil
	}
	return msg
}

// newFilteredDeleteNode returns a new filterDeleteNode
func newFilteredDeleteNode(replica ReplicaInterface, collectionID UniqueID) *filterDeleteNode {

	maxQueueLength := Params.QueryNodeCfg.FlowGraphMaxQueueLength
	maxParallelism := Params.QueryNodeCfg.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &filterDeleteNode{
		baseNode:     baseNode,
		collectionID: collectionID,
		replica:      replica,
	}
}
