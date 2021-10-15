// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datanode

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"go.uber.org/zap"
)

// DmInputNode receives messages from message streams, packs messages between two timeticks, and passes all
//  messages between two timeticks to the following flowgraph node. In DataNode, the following flow graph node is
//  flowgraph ddNode.
func newDmInputNode(ctx context.Context, seekPos *internalpb.MsgPosition, dmNodeConfig *nodeConfig) (*flowgraph.InputNode, error) {
	// subName should be unique, since pchannelName is shared among several collections
	//	consumeSubName := Params.MsgChannelSubName + "-" + strconv.FormatInt(collID, 10)
	consumeSubName := fmt.Sprintf("%s-%d", Params.MsgChannelSubName, dmNodeConfig.collectionID)
	insertStream, err := dmNodeConfig.msFactory.NewTtMsgStream(ctx)
	if err != nil {
		return nil, err
	}

	// MsgStream needs a physical channel name, but the channel name in seek position from DataCoord
	//  is virtual channel name, so we need to convert vchannel name into pchannel neme here.
	pchannelName := rootcoord.ToPhysicalChannel(dmNodeConfig.vChannelName)
	insertStream.AsConsumer([]string{pchannelName}, consumeSubName)
	log.Debug("datanode AsConsumer", zap.String("physical channel", pchannelName), zap.String("subName", consumeSubName))

	if seekPos != nil {
		seekPos.ChannelName = pchannelName
		start := time.Now()
		log.Debug("datanode begin to seek: " + seekPos.GetChannelName())
		err = insertStream.Seek([]*internalpb.MsgPosition{seekPos})
		if err != nil {
			return nil, err
		}
		log.Debug("datanode Seek successfully: "+seekPos.GetChannelName(), zap.Int64("elapse ", time.Since(start).Milliseconds()))
	}

	node := flowgraph.NewInputNode(insertStream, "dmInputNode", dmNodeConfig.maxQueueLength, dmNodeConfig.maxParallelism)
	return node, nil
}
