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
	"github.com/golang/protobuf/proto"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/trace"
)

type ddNode struct {
	baseNode
	ddMsg   *ddMsg
	replica ReplicaInterface
}

func (ddNode *ddNode) Name() string {
	return "ddNode"
}

func (ddNode *ddNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {
	//log.Debug("Do filterDmNode operation")

	if len(in) != 1 {
		log.Error("Invalid operate message input in ddNode", zap.Int("input length", len(in)))
		// TODO: add error handling
	}

	msMsg, ok := in[0].(*MsgStreamMsg)
	if !ok {
		log.Warn("type assertion failed for MsgStreamMsg")
		// TODO: add error handling
	}

	var spans []opentracing.Span
	for _, msg := range msMsg.TsMessages() {
		sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}

	var ddMsg = ddMsg{
		collectionRecords: make(map[UniqueID][]metaOperateRecord),
		partitionRecords:  make(map[UniqueID][]metaOperateRecord),
		timeRange: TimeRange{
			timestampMin: msMsg.TimestampMin(),
			timestampMax: msMsg.TimestampMax(),
		},
	}
	ddNode.ddMsg = &ddMsg
	gcRecord := gcRecord{
		collections: make([]UniqueID, 0),
		partitions:  make([]partitionWithID, 0),
	}
	ddNode.ddMsg.gcRecord = &gcRecord

	// sort tsMessages
	//tsMessages := msMsg.TsMessages()
	//sort.Slice(tsMessages,
	//	func(i, j int) bool {
	//		return tsMessages[i].BeginTs() < tsMessages[j].BeginTs()
	//	})

	// do dd tasks
	//for _, msg := range tsMessages {
	//	switch msg.Type() {
	//	case commonpb.MsgType_kCreateCollection:
	//		ddNode.createCollection(msg.(*msgstream.CreateCollectionMsg))
	//	case commonpb.MsgType_kDropCollection:
	//		ddNode.dropCollection(msg.(*msgstream.DropCollectionMsg))
	//	case commonpb.MsgType_kCreatePartition:
	//		ddNode.createPartition(msg.(*msgstream.CreatePartitionMsg))
	//	case commonpb.MsgType_kDropPartition:
	//		ddNode.dropPartition(msg.(*msgstream.DropPartitionMsg))
	//	default:
	//		log.Println("Non supporting message type:", msg.Type())
	//	}
	//}

	var res Msg = ddNode.ddMsg
	for _, span := range spans {
		span.Finish()
	}
	return []Msg{res}
}

func (ddNode *ddNode) createCollection(msg *msgstream.CreateCollectionMsg) {
	collectionID := msg.CollectionID
	partitionID := msg.PartitionID

	hasCollection := ddNode.replica.hasCollection(collectionID)
	if hasCollection {
		log.Debug("collection already exists", zap.Int64("collectionID", collectionID))
		return
	}

	var schema schemapb.CollectionSchema
	err := proto.Unmarshal(msg.Schema, &schema)
	if err != nil {
		log.Error(err.Error())
		return
	}

	// add collection
	err = ddNode.replica.addCollection(collectionID, &schema)
	if err != nil {
		log.Error(err.Error())
		return
	}

	// add default partition
	// TODO: allocate default partition id in master
	err = ddNode.replica.addPartition(collectionID, partitionID)
	if err != nil {
		log.Error(err.Error())
		return
	}

	ddNode.ddMsg.collectionRecords[collectionID] = append(ddNode.ddMsg.collectionRecords[collectionID],
		metaOperateRecord{
			createOrDrop: true,
			timestamp:    msg.Base.Timestamp,
		})
}

func (ddNode *ddNode) dropCollection(msg *msgstream.DropCollectionMsg) {
	collectionID := msg.CollectionID

	ddNode.ddMsg.collectionRecords[collectionID] = append(ddNode.ddMsg.collectionRecords[collectionID],
		metaOperateRecord{
			createOrDrop: false,
			timestamp:    msg.Base.Timestamp,
		})

	ddNode.ddMsg.gcRecord.collections = append(ddNode.ddMsg.gcRecord.collections, collectionID)
}

func (ddNode *ddNode) createPartition(msg *msgstream.CreatePartitionMsg) {
	collectionID := msg.CollectionID
	partitionID := msg.PartitionID

	err := ddNode.replica.addPartition(collectionID, partitionID)
	if err != nil {
		log.Error(err.Error())
		return
	}

	ddNode.ddMsg.partitionRecords[partitionID] = append(ddNode.ddMsg.partitionRecords[partitionID],
		metaOperateRecord{
			createOrDrop: true,
			timestamp:    msg.Base.Timestamp,
		})
}

func (ddNode *ddNode) dropPartition(msg *msgstream.DropPartitionMsg) {
	collectionID := msg.CollectionID
	partitionID := msg.PartitionID

	ddNode.ddMsg.partitionRecords[partitionID] = append(ddNode.ddMsg.partitionRecords[partitionID],
		metaOperateRecord{
			createOrDrop: false,
			timestamp:    msg.Base.Timestamp,
		})

	ddNode.ddMsg.gcRecord.partitions = append(ddNode.ddMsg.gcRecord.partitions, partitionWithID{
		partitionID:  partitionID,
		collectionID: collectionID,
	})
}

func newDDNode(replica ReplicaInterface) *ddNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &ddNode{
		baseNode: baseNode,
		replica:  replica,
	}
}
