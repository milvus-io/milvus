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

package datanode

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

type ddNode struct {
	BaseNode
}

func (ddn *ddNode) Name() string {
	return "ddNode"
}

func (ddn *ddNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {
	if len(in) != 1 {
		log.Error("Invalid operate message input in ddNode", zap.Int("input length", len(in)))
		// TODO: add error handling
	}

	if len(in) == 0 {
		return []flowgraph.Msg{}
	}

	msMsg, ok := in[0].(*MsgStreamMsg)
	if !ok {
		log.Error("type assertion failed for MsgStreamMsg")
		// TODO: add error handling
	}

	if msMsg == nil {
		return []Msg{}
	}

	var iMsg = insertMsg{
		insertMessages: make([]*msgstream.InsertMsg, 0),
		timeRange: TimeRange{
			timestampMin: msMsg.TimestampMin(),
			timestampMax: msMsg.TimestampMax(),
		},
		startPositions: make([]*internalpb.MsgPosition, 0),
		endPositions:   make([]*internalpb.MsgPosition, 0),
	}

	for _, msg := range msMsg.TsMessages() {
		switch msg.Type() {
		case commonpb.MsgType_DropCollection:
			// TODO distroy dataSyncService and nodify datanode
			log.Error("Distorying current flowgraph")
		case commonpb.MsgType_Insert:
			resMsg := ddn.filterFlushedSegmentInsertMessages(msg.(*msgstream.InsertMsg))
			if resMsg != nil {
				iMsg.insertMessages = append(iMsg.insertMessages, resMsg)
			}
		}
	}

	iMsg.startPositions = append(iMsg.startPositions, msMsg.StartPositions()...)
	iMsg.endPositions = append(iMsg.endPositions, msMsg.EndPositions()...)

	var res Msg = &iMsg

	return []Msg{res}
}

func (ddn *ddNode) filterFlushedSegmentInsertMessages(msg *msgstream.InsertMsg) *msgstream.InsertMsg {
	// TODO fileter insert messages of flushed segments
	return msg
}

func newDDNode() *ddNode {
	baseNode := BaseNode{}
	baseNode.SetMaxParallelism(Params.FlowGraphMaxQueueLength)

	return &ddNode{
		BaseNode: baseNode,
	}
}
