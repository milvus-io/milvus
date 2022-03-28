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

// filterDmNode is one of the nodes in query node flow graph
type filterDmNode struct {
	baseNode
	collectionID UniqueID
	replica      ReplicaInterface
}

// Name returns the name of filterDmNode
func (fdmNode *filterDmNode) Name() string {
	return fmt.Sprintf("fdmNode-%d", fdmNode.collectionID)
}

// Operate handles input messages, to filter invalid insert messages
func (fdmNode *filterDmNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {
	//log.Debug("Do filterDmNode operation")

	if len(in) != 1 {
		log.Warn("Invalid operate message input in filterDmNode", zap.Int("input length", len(in)))
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

	var iMsg = insertMsg{
		insertMessages: make([]*msgstream.InsertMsg, 0),
		deleteMessages: make([]*msgstream.DeleteMsg, 0),
		timeRange: TimeRange{
			timestampMin: msgStreamMsg.TimestampMin(),
			timestampMax: msgStreamMsg.TimestampMax(),
		},
	}

	for i, msg := range msgStreamMsg.TsMessages() {
		traceID, _, _ := trace.InfoFromSpan(spans[i])
		log.Info("Filter invalid message in QueryNode", zap.String("traceID", traceID))
		switch msg.Type() {
		case commonpb.MsgType_Insert:
			resMsg := fdmNode.filterInvalidInsertMessage(msg.(*msgstream.InsertMsg))
			if resMsg != nil {
				iMsg.insertMessages = append(iMsg.insertMessages, resMsg)
			}
		case commonpb.MsgType_Delete:
			resMsg := fdmNode.filterInvalidDeleteMessage(msg.(*msgstream.DeleteMsg))
			if resMsg != nil {
				iMsg.deleteMessages = append(iMsg.deleteMessages, resMsg)
			}
		default:
			log.Warn("Non supporting", zap.Int32("message type", int32(msg.Type())))
		}
	}

	var res Msg = &iMsg
	for _, sp := range spans {
		sp.Finish()
	}
	return []Msg{res}
}

// filterInvalidDeleteMessage would filter out invalid delete messages
func (fdmNode *filterDmNode) filterInvalidDeleteMessage(msg *msgstream.DeleteMsg) *msgstream.DeleteMsg {
	sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	msg.SetTraceCtx(ctx)
	defer sp.Finish()

	if msg.CollectionID != fdmNode.collectionID {
		return nil
	}

	// check if collection and partition exist
	col, err := fdmNode.replica.getCollectionByID(msg.CollectionID)
	if err != nil {
		log.Debug("filter invalid delete message, collection does not exist",
			zap.Any("collectionID", msg.CollectionID),
			zap.Any("partitionID", msg.PartitionID))
		return nil
	}
	if col.getLoadType() == loadTypePartition {
		if !fdmNode.replica.hasPartition(msg.PartitionID) {
			log.Debug("filter invalid delete message, partition does not exist",
				zap.Any("collectionID", msg.CollectionID),
				zap.Any("partitionID", msg.PartitionID))
			return nil
		}
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

// filterInvalidInsertMessage would filter out invalid insert messages
func (fdmNode *filterDmNode) filterInvalidInsertMessage(msg *msgstream.InsertMsg) *msgstream.InsertMsg {
	if err := msg.CheckAligned(); err != nil {
		// TODO: what if the messages are misaligned? Here, we ignore those messages and print error
		log.Warn("Error, misaligned messages detected")
		return nil
	}

	sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	msg.SetTraceCtx(ctx)
	defer sp.Finish()

	// check if the collection from message is target collection
	if msg.CollectionID != fdmNode.collectionID {
		//log.Debug("filter invalid insert message, collection is not the target collection",
		//	zap.Any("collectionID", msg.CollectionID),
		//	zap.Any("partitionID", msg.PartitionID))
		return nil
	}

	// check if collection and partition exist
	col, err := fdmNode.replica.getCollectionByID(msg.CollectionID)
	if err != nil {
		log.Debug("filter invalid insert message, collection does not exist",
			zap.Any("collectionID", msg.CollectionID),
			zap.Any("partitionID", msg.PartitionID))
		return nil
	}
	if col.getLoadType() == loadTypePartition {
		if !fdmNode.replica.hasPartition(msg.PartitionID) {
			log.Debug("filter invalid insert message, partition does not exist",
				zap.Any("collectionID", msg.CollectionID),
				zap.Any("partitionID", msg.PartitionID))
			return nil
		}
	}

	// Check if the segment is in excluded segments,
	// messages after seekPosition may contain the redundant data from flushed slice of segment,
	// so we need to compare the endTimestamp of received messages and position's timestamp.
	excludedSegments, err := fdmNode.replica.getExcludedSegments(fdmNode.collectionID)
	if err != nil {
		log.Warn(err.Error())
		return nil
	}
	for _, segmentInfo := range excludedSegments {
		// unFlushed segment may not have checkPoint, so `segmentInfo.DmlPosition` may be nil
		if segmentInfo.DmlPosition == nil {
			log.Debug("filter unFlushed segment without checkPoint",
				zap.Any("collectionID", msg.CollectionID),
				zap.Any("partitionID", msg.PartitionID))
			return nil
		}
		if msg.SegmentID == segmentInfo.ID && msg.EndTs() < segmentInfo.DmlPosition.Timestamp {
			log.Debug("filter invalid insert message, segments are excluded segments",
				zap.Any("collectionID", msg.CollectionID),
				zap.Any("partitionID", msg.PartitionID))
			return nil
		}
	}

	if len(msg.Timestamps) <= 0 {
		log.Debug("filter invalid insert message, no message",
			zap.Any("collectionID", msg.CollectionID),
			zap.Any("partitionID", msg.PartitionID))
		return nil
	}

	return msg
}

// newFilteredDmNode returns a new filterDmNode
func newFilteredDmNode(replica ReplicaInterface, collectionID UniqueID) *filterDmNode {

	maxQueueLength := Params.QueryNodeCfg.FlowGraphMaxQueueLength
	maxParallelism := Params.QueryNodeCfg.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &filterDmNode{
		baseNode:     baseNode,
		collectionID: collectionID,
		replica:      replica,
	}
}
