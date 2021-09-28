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
	"strconv"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

func newDmInputNode(ctx context.Context, factory msgstream.Factory, collID UniqueID, chanName string, seekPos *internalpb.MsgPosition) (*flowgraph.InputNode, error) {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	// subName should be unique, since pchannelName is shared among several collections
	consumeSubName := Params.MsgChannelSubName + "-" + strconv.FormatInt(collID, 10)
	insertStream, err := factory.NewTtMsgStream(ctx)
	if err != nil {
		return nil, err
	}

	pchannelName := rootcoord.ToPhysicalChannel(chanName)
	insertStream.AsConsumer([]string{pchannelName}, consumeSubName)
	log.Debug("datanode AsConsumer physical channel: " + pchannelName + " : " + consumeSubName)

	if seekPos != nil {
		// ChannelName in seek position is virtual channel name.
		seekPos.ChannelName = pchannelName
		log.Debug("datanode Seek: " + seekPos.GetChannelName())
		insertStream.Seek([]*internalpb.MsgPosition{seekPos})
	}

	var stream msgstream.MsgStream = insertStream
	node := flowgraph.NewInputNode(stream, "dmInputNode", maxQueueLength, maxParallelism)
	return node, nil
}
