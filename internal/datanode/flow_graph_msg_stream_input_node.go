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
	"strings"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
)

func newDmInputNode(ctx context.Context, factory msgstream.Factory) *flowgraph.InputNode {

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism
	consumeChannels := Params.InsertChannelNames
	consumeSubName := Params.MsgChannelSubName

	insertStream, _ := factory.NewTtMsgStream(ctx)
	insertStream.AsConsumer(consumeChannels, consumeSubName)
	log.Debug("datanode AsConsumer: " + strings.Join(consumeChannels, ", ") + " : " + consumeSubName)

	var stream msgstream.MsgStream = insertStream
	node := flowgraph.NewInputNode(&stream, "dmInputNode", maxQueueLength, maxParallelism)
	return node
}

func newDDInputNode(ctx context.Context, factory msgstream.Factory) *flowgraph.InputNode {

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism
	consumeSubName := Params.MsgChannelSubName

	tmpStream, _ := factory.NewTtMsgStream(ctx)
	tmpStream.AsConsumer(Params.DDChannelNames, consumeSubName)
	log.Debug("datanode AsConsumer: " + strings.Join(Params.DDChannelNames, ", ") + " : " + consumeSubName)

	var stream msgstream.MsgStream = tmpStream
	node := flowgraph.NewInputNode(&stream, "ddInputNode", maxQueueLength, maxParallelism)
	return node
}
