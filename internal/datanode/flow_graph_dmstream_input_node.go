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
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

func newDmInputNode(ctx context.Context, factory msgstream.Factory, vchannelName string, seekPos *internalpb.MsgPosition) *flowgraph.InputNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism
	consumeSubName := Params.MsgChannelSubName
	insertStream, _ := factory.NewTtMsgStream(ctx)

	insertStream.AsConsumer([]string{vchannelName}, consumeSubName)
	log.Debug("datanode AsConsumer: " + vchannelName + " : " + consumeSubName)

	if seekPos != nil {
		insertStream.Seek([]*internalpb.MsgPosition{seekPos})
		log.Debug("datanode Seek: " + vchannelName)
	}

	var stream msgstream.MsgStream = insertStream
	node := flowgraph.NewInputNode(&stream, "dmInputNode", maxQueueLength, maxParallelism)
	return node
}
