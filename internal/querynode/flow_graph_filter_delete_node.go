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
	"reflect"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// filterDeleteNode is one of the nodes in delta flow graph
type filterDeleteNode struct {
	baseNode
	collectionID UniqueID
	metaReplica  ReplicaInterface
	vchannel     Channel
}

// Name returns the name of filterDeleteNode
func (fddNode *filterDeleteNode) Name() string {
	return fmt.Sprintf("fdNode-%s", fddNode.vchannel)
}

func (fddNode *filterDeleteNode) IsValidInMsg(in []Msg) bool {
	if !fddNode.baseNode.IsValidInMsg(in) {
		return false
	}
	_, ok := in[0].(*MsgStreamMsg)
	if !ok {
		log.Warn("type assertion failed for MsgStreamMsg", zap.String("msgType", reflect.TypeOf(in[0]).Name()), zap.String("name", fddNode.Name()))
		return false
	}
	return true
}

// Operate handles input messages, to filter invalid delete messages
func (fddNode *filterDeleteNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {
	msgStreamMsg := in[0].(*MsgStreamMsg)

	var spans []trace.Span
	for _, msg := range msgStreamMsg.TsMessages() {
		ctx, sp := otel.Tracer(typeutil.QueryCoordRole).Start(msg.TraceCtx(), "FilterDelete_Node")
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}

	defer func() {
		for _, sp := range spans {
			sp.End()
		}
	}()

	if msgStreamMsg.IsCloseMsg() {
		return []Msg{
			&deleteMsg{BaseMsg: flowgraph.NewBaseMsg(true)},
		}
	}

	var dMsg = deleteMsg{
		deleteMessages: make([]*msgstream.DeleteMsg, 0),
		timeRange: TimeRange{
			timestampMin: msgStreamMsg.TimestampMin(),
			timestampMax: msgStreamMsg.TimestampMax(),
		},
	}

	collection, err := fddNode.metaReplica.getCollectionByID(fddNode.collectionID)
	if err != nil {
		// QueryNode should add collection before start flow graph
		panic(fmt.Errorf("%s getCollectionByID failed, collectionID = %d, channel = %s", fddNode.Name(), fddNode.collectionID, fddNode.vchannel))
	}

	for _, msg := range msgStreamMsg.TsMessages() {
		switch msg.Type() {
		case commonpb.MsgType_Delete:
			resMsg, err := fddNode.filterInvalidDeleteMessage(msg.(*msgstream.DeleteMsg), collection.getLoadType())
			if err != nil {
				// error occurs when missing meta info or data is misaligned, should not happen
				err = fmt.Errorf("filterInvalidDeleteMessage failed, err = %s, collection = %d, channel = %s", err, fddNode.collectionID, fddNode.vchannel)
				log.Error(err.Error())
				panic(err)
			}
			if resMsg != nil {
				dMsg.deleteMessages = append(dMsg.deleteMessages, resMsg)
			}
		default:
			log.Warn("invalid message type in filterDeleteNode",
				zap.String("message type", msg.Type().String()),
				zap.Int64("collection", fddNode.collectionID),
				zap.String("vchannel", fddNode.vchannel))
		}
	}

	return []Msg{&dMsg}
}

// filterInvalidDeleteMessage would filter invalid delete messages
func (fddNode *filterDeleteNode) filterInvalidDeleteMessage(msg *msgstream.DeleteMsg, loadType loadType) (*msgstream.DeleteMsg, error) {
	if err := msg.CheckAligned(); err != nil {
		return nil, fmt.Errorf("CheckAligned failed, err = %s", err)
	}

	ctx, sp := otel.Tracer(typeutil.QueryCoordRole).Start(msg.TraceCtx(), "FilterDelete_Node")
	msg.SetTraceCtx(ctx)
	defer sp.End()

	if msg.CollectionID != fddNode.collectionID {
		return nil, nil
	}

	if len(msg.Timestamps) <= 0 {
		log.Debug("filter invalid delete message, no message",
			zap.String("vchannel", fddNode.vchannel),
			zap.Int64("collectionID", msg.CollectionID),
			zap.Int64("partitionID", msg.PartitionID))
		return nil, nil
	}

	//if loadType == loadTypePartition {
	//	if !fddNode.metaReplica.hasPartition(msg.PartitionID) {
	//		// filter out msg which not belongs to the loaded partitions
	//		return nil, nil
	//	}
	//}
	return msg, nil
}

// newFilteredDeleteNode returns a new filterDeleteNode
func newFilteredDeleteNode(metaReplica ReplicaInterface, collectionID UniqueID, vchannel Channel) *filterDeleteNode {
	maxQueueLength := Params.QueryNodeCfg.FlowGraphMaxQueueLength.GetAsInt32()
	maxParallelism := Params.QueryNodeCfg.FlowGraphMaxParallelism.GetAsInt32()

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &filterDeleteNode{
		baseNode:     baseNode,
		collectionID: collectionID,
		metaReplica:  metaReplica,
		vchannel:     vchannel,
	}
}
