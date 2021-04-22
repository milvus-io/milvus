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

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"go.uber.org/zap"
)

type serviceTimeNode struct {
	baseNode
	collectionID      UniqueID
	replica           ReplicaInterface
	timeTickMsgStream msgstream.MsgStream
}

func (stNode *serviceTimeNode) Name() string {
	return "stNode"
}

func (stNode *serviceTimeNode) Close() {
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
		log.Error("type assertion failed for serviceTimeMsg")
		// TODO: add error handling
	}

	if serviceTimeMsg == nil {
		return []Msg{}
	}

	// update service time
	ts := stNode.replica.getTSafe(stNode.collectionID)
	if ts != nil {
		ts.set(serviceTimeMsg.timeRange.timestampMax)
		//log.Debug("update tSafe:",
		//	zap.Int64("tSafe", int64(serviceTimeMsg.timeRange.timestampMax)),
		//	zap.Int64("collectionID", stNode.collectionID))
	}

	if err := stNode.sendTimeTick(serviceTimeMsg.timeRange.timestampMax); err != nil {
		log.Error("Error: send time tick into pulsar channel failed", zap.Error(err))
	}

	var res Msg = &gcMsg{
		gcRecord:  serviceTimeMsg.gcRecord,
		timeRange: serviceTimeMsg.timeRange,
	}
	return []Msg{res}
}

func (stNode *serviceTimeNode) sendTimeTick(ts Timestamp) error {
	msgPack := msgstream.MsgPack{}
	timeTickMsg := msgstream.TimeTickMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: ts,
			EndTimestamp:   ts,
			HashValues:     []uint32{0},
		},
		TimeTickMsg: internalpb.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_TimeTick,
				MsgID:     0,
				Timestamp: ts,
				SourceID:  Params.QueryNodeID,
			},
		},
	}
	msgPack.Msgs = append(msgPack.Msgs, &timeTickMsg)
	return stNode.timeTickMsgStream.Produce(&msgPack)
}

func newServiceTimeNode(ctx context.Context, replica ReplicaInterface, factory msgstream.Factory, collectionID UniqueID) *serviceTimeNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	timeTimeMsgStream, _ := factory.NewMsgStream(ctx)
	timeTimeMsgStream.AsProducer([]string{Params.QueryTimeTickChannelName})
	log.Debug("querynode AsProducer: " + Params.QueryTimeTickChannelName)

	return &serviceTimeNode{
		baseNode:          baseNode,
		collectionID:      collectionID,
		replica:           replica,
		timeTickMsgStream: timeTimeMsgStream,
	}
}
