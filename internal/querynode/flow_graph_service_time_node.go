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
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

type serviceTimeNode struct {
	baseNode
	loadType          loadType
	collectionID      UniqueID
	partitionID       UniqueID
	vChannel          Channel
	tSafeReplica      TSafeReplicaInterface
	timeTickMsgStream msgstream.MsgStream
}

func (stNode *serviceTimeNode) Name() string {
	return "stNode"
}

func (stNode *serviceTimeNode) Close() {
	// `Close` needs to be invoked to close producers
	stNode.timeTickMsgStream.Close()
}

func (stNode *serviceTimeNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {
	//log.Debug("Do serviceTimeNode operation")

	if len(in) != 1 {
		log.Error("Invalid operate message input in serviceTimeNode, input length = ", zap.Int("input node", len(in)))
		// TODO: add error handling
	}

	serviceTimeMsg, ok := in[0].(*serviceTimeMsg)
	if !ok {
		log.Warn("type assertion failed for serviceTimeMsg")
		// TODO: add error handling
	}

	if serviceTimeMsg == nil {
		return []Msg{}
	}

	// update service time
	var id UniqueID
	if stNode.loadType == loadTypePartition {
		id = stNode.partitionID
	} else {
		id = stNode.collectionID
	}
	stNode.tSafeReplica.setTSafe(stNode.vChannel, id, serviceTimeMsg.timeRange.timestampMax)
	//log.Debug("update tSafe:",
	//	zap.Int64("tSafe", int64(serviceTimeMsg.timeRange.timestampMax)),
	//	zap.Any("collectionID", stNode.collectionID),
	//	zap.Any("id", id),
	//	zap.Any("channel", stNode.vChannel),
	//)

	//if err := stNode.sendTimeTick(serviceTimeMsg.timeRange.timestampMax); err != nil {
	//	log.Warn("Error: send time tick into pulsar channel failed", zap.Error(err))
	//}

	return []Msg{}
}

//func (stNode *serviceTimeNode) sendTimeTick(ts Timestamp) error {
//	msgPack := msgstream.MsgPack{}
//	timeTickMsg := msgstream.TimeTickMsg{
//		BaseMsg: msgstream.BaseMsg{
//			BeginTimestamp: ts,
//			EndTimestamp:   ts,
//			HashValues:     []uint32{0},
//		},
//		TimeTickMsg: internalpb.TimeTickMsg{
//			Base: &commonpb.MsgBase{
//				MsgType:   commonpb.MsgType_TimeTick,
//				MsgID:     0,
//				Timestamp: ts,
//				SourceID:  Params.QueryNodeID,
//			},
//		},
//	}
//	msgPack.Msgs = append(msgPack.Msgs, &timeTickMsg)
//	return stNode.timeTickMsgStream.Produce(&msgPack)
//}

func newServiceTimeNode(ctx context.Context,
	tSafeReplica TSafeReplicaInterface,
	loadType loadType,
	collectionID UniqueID,
	partitionID UniqueID,
	channel Channel,
	factory msgstream.Factory) *serviceTimeNode {

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	timeTimeMsgStream, err := factory.NewMsgStream(ctx)
	if err != nil {
		log.Warn(err.Error())
	} else {
		// TODO: use param table
		timeTickChannel := "query-node-time-tick-0"
		timeTimeMsgStream.AsProducer([]string{timeTickChannel})
		log.Debug("query node AsProducer: " + timeTickChannel)
	}

	return &serviceTimeNode{
		baseNode:          baseNode,
		loadType:          loadType,
		collectionID:      collectionID,
		partitionID:       partitionID,
		vChannel:          channel,
		tSafeReplica:      tSafeReplica,
		timeTickMsgStream: timeTimeMsgStream,
	}
}
