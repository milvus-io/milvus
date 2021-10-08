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
	"errors"

	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/trace"
)

type filterDmNode struct {
	baseNode
	loadType     loadType // load collection or load partition
	collectionID UniqueID
	partitionID  UniqueID
	replica      ReplicaInterface
}

func (fdmNode *filterDmNode) Name() string {
	return "fdmNode"
}

func (fdmNode *filterDmNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {
	//log.Debug("Do filterDmNode operation")

	if len(in) != 1 {
		log.Error("Invalid operate message input in filterDmNode", zap.Int("input length", len(in)))
		// TODO: add error handling
	}

	msgStreamMsg, ok := in[0].(*MsgStreamMsg)
	if !ok {
		log.Warn("type assertion failed for MsgStreamMsg")
		// TODO: add error handling
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

	for _, msg := range msgStreamMsg.TsMessages() {
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

func (fdmNode *filterDmNode) filterInvalidDeleteMessage(msg *msgstream.DeleteMsg) *msgstream.DeleteMsg {
	sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	msg.SetTraceCtx(ctx)
	defer sp.Finish()

	// check if collection and partition exist
	collection := fdmNode.replica.hasCollection(msg.CollectionID)
	partition := fdmNode.replica.hasPartition(msg.PartitionID)
	if fdmNode.loadType == loadTypeCollection && !collection {
		log.Debug("filter invalid delete message, collection dose not exist",
			zap.Any("collectionID", msg.CollectionID),
			zap.Any("partitionID", msg.PartitionID))
		return nil
	}
	if fdmNode.loadType == loadTypePartition && !partition {
		log.Debug("filter invalid delete message, partition dose not exist",
			zap.Any("collectionID", msg.CollectionID),
			zap.Any("partitionID", msg.PartitionID))
		return nil
	}

	if msg.CollectionID != fdmNode.collectionID {
		return nil
	}

	// if the flow graph type is partition, check if the partition is target partition
	if fdmNode.loadType == loadTypePartition && msg.PartitionID != fdmNode.partitionID {
		log.Debug("filter invalid delete message, partition is not the target partition",
			zap.Any("collectionID", msg.CollectionID),
			zap.Any("partitionID", msg.PartitionID))
		return nil
	}

	// check if partition has been released
	if fdmNode.loadType == loadTypeCollection {
		col, err := fdmNode.replica.getCollectionByID(msg.CollectionID)
		if err != nil {
			log.Warn(err.Error())
			return nil
		}
		if err = col.checkReleasedPartitions([]UniqueID{msg.PartitionID}); err != nil {
			log.Warn(err.Error())
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

func (fdmNode *filterDmNode) filterInvalidInsertMessage(msg *msgstream.InsertMsg) *msgstream.InsertMsg {
	sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	msg.SetTraceCtx(ctx)
	defer sp.Finish()
	// check if collection and partition exist
	collection := fdmNode.replica.hasCollection(msg.CollectionID)
	partition := fdmNode.replica.hasPartition(msg.PartitionID)
	if fdmNode.loadType == loadTypeCollection && !collection {
		log.Debug("filter invalid insert message, collection dose not exist",
			zap.Any("collectionID", msg.CollectionID),
			zap.Any("partitionID", msg.PartitionID))
		return nil
	}

	if fdmNode.loadType == loadTypePartition && !partition {
		log.Debug("filter invalid insert message, partition dose not exist",
			zap.Any("collectionID", msg.CollectionID),
			zap.Any("partitionID", msg.PartitionID))
		return nil
	}

	// check if the collection from message is target collection
	if msg.CollectionID != fdmNode.collectionID {
		//log.Debug("filter invalid insert message, collection is not the target collection",
		//	zap.Any("collectionID", msg.CollectionID),
		//	zap.Any("partitionID", msg.PartitionID))
		return nil
	}

	// if the flow graph type is partition, check if the partition is target partition
	if fdmNode.loadType == loadTypePartition && msg.PartitionID != fdmNode.partitionID {
		log.Debug("filter invalid insert message, partition is not the target partition",
			zap.Any("collectionID", msg.CollectionID),
			zap.Any("partitionID", msg.PartitionID))
		return nil
	}

	// check if partition has been released
	if fdmNode.loadType == loadTypeCollection {
		col, err := fdmNode.replica.getCollectionByID(msg.CollectionID)
		if err != nil {
			log.Warn(err.Error())
			return nil
		}
		if err = col.checkReleasedPartitions([]UniqueID{msg.PartitionID}); err != nil {
			log.Warn(err.Error())
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
		if msg.SegmentID == segmentInfo.ID && msg.EndTs() < segmentInfo.DmlPosition.Timestamp {
			log.Debug("filter invalid insert message, segments are excluded segments",
				zap.Any("collectionID", msg.CollectionID),
				zap.Any("partitionID", msg.PartitionID))
			return nil
		}
	}

	if len(msg.RowIDs) != len(msg.Timestamps) || len(msg.RowIDs) != len(msg.RowData) {
		// TODO: what if the messages are misaligned? Here, we ignore those messages and print error
		log.Warn("Error, misaligned messages detected")
		return nil
	}

	if len(msg.Timestamps) <= 0 {
		log.Debug("filter invalid insert message, no message",
			zap.Any("collectionID", msg.CollectionID),
			zap.Any("partitionID", msg.PartitionID))
		return nil
	}

	return msg
}

func newFilteredDmNode(replica ReplicaInterface,
	loadType loadType,
	collectionID UniqueID,
	partitionID UniqueID) *filterDmNode {

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	if loadType != loadTypeCollection && loadType != loadTypePartition {
		err := errors.New("invalid flow graph type")
		log.Warn(err.Error())
		return nil
	}

	return &filterDmNode{
		baseNode:     baseNode,
		loadType:     loadType,
		collectionID: collectionID,
		partitionID:  partitionID,
		replica:      replica,
	}
}
