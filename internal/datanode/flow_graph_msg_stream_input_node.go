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
	"context"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

func newDmInputNode(ctx context.Context, factory msgstream.Factory, vchannelName string, vchannelPos *datapb.PositionPair) *flowgraph.InputNode {
	// TODO use position pair in Seek

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism
	// consumeChannels := Params.InsertChannelNames
	consumeSubName := Params.MsgChannelSubName

	insertStream, _ := factory.NewTtMsgStream(ctx)
	insertStream.AsConsumer([]string{vchannelName}, consumeSubName)
	log.Debug("datanode AsConsumer: " + vchannelName + " : " + consumeSubName)

	var stream msgstream.MsgStream = insertStream
	node := flowgraph.NewInputNode(&stream, "dmInputNode", maxQueueLength, maxParallelism)
	return node
}

func newDDInputNode(ctx context.Context, factory msgstream.Factory, vchannelName string, vchannelPos *datapb.PositionPair) *flowgraph.InputNode {

	// TODO use position pair in Seek
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism
	consumeSubName := Params.MsgChannelSubName

	tmpStream, _ := factory.NewTtMsgStream(ctx)
	tmpStream.AsConsumer([]string{vchannelName}, consumeSubName)
	log.Debug("datanode AsConsumer: " + vchannelName + " : " + consumeSubName)

	var stream msgstream.MsgStream = tmpStream
	node := flowgraph.NewInputNode(&stream, "ddInputNode", maxQueueLength, maxParallelism)
	return node
}
