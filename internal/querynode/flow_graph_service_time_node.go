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
	"strconv"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

type serviceTimeNode struct {
	baseNode
	collectionID UniqueID
	vChannel     VChannel
	tSafeReplica TSafeReplicaInterface
	//timeTickMsgStream msgstream.MsgStream
}

func (stNode *serviceTimeNode) Name() string {
	return "stNode"
}

func (stNode *serviceTimeNode) Close() {
	//stNode.timeTickMsgStream.Close()
}

func (stNode *serviceTimeNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {
	//log.Debug("Do serviceTimeNode operation")

	if len(in) != 1 {
		log.Error("Invalid operate message input in serviceTimeNode, input length = ", zap.Int("input node", len(in)))
		// TODO: add error handling
	}

	serviceTimeMsg, ok := in[0].(*serviceTimeMsg)
	if !ok {
		log.Error("type assertion failed for serviceTimeMsg")
		// TODO: add error handling
	}

	if serviceTimeMsg == nil {
		return []Msg{}
	}

	// update service time
	channel := stNode.vChannel + strconv.FormatInt(stNode.collectionID, 10)
	stNode.tSafeReplica.setTSafe(channel, serviceTimeMsg.timeRange.timestampMax)
	//log.Debug("update tSafe:",
	//	zap.Int64("tSafe", int64(serviceTimeMsg.timeRange.timestampMax)),
	//	zap.Any("collectionID", stNode.collectionID),
	//)

	//if err := stNode.sendTimeTick(serviceTimeMsg.timeRange.timestampMax); err != nil {
	//	log.Error("Error: send time tick into pulsar channel failed", zap.Error(err))
	//}

	var res Msg = &gcMsg{
		gcRecord:  serviceTimeMsg.gcRecord,
		timeRange: serviceTimeMsg.timeRange,
	}
	return []Msg{res}
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
	collectionID UniqueID,
	channel VChannel,
	factory msgstream.Factory) *serviceTimeNode {

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	//timeTimeMsgStream, err := factory.NewMsgStream(ctx)
	//if err != nil {
	//	log.Error(err.Error())
	//} else {
	//	timeTimeMsgStream.AsProducer([]string{Params.QueryTimeTickChannelName})
	//	log.Debug("query node AsProducer: " + Params.QueryTimeTickChannelName)
	//}

	return &serviceTimeNode{
		baseNode:     baseNode,
		collectionID: collectionID,
		vChannel:     channel,
		tSafeReplica: tSafeReplica,
		//timeTickMsgStream: timeTimeMsgStream,
	}
}
