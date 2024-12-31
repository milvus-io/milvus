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

package pipeline

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	pkgcommon "github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// DmInputNode receives messages from message streams, packs messages between two timeticks, and passes all
//
// messages between two timeticks to the following flowgraph node. In DataNode, the following flow graph node is
// flowgraph ddNode.
func newDmInputNode(dmNodeConfig *nodeConfig, input <-chan *msgstream.MsgPack) *flowgraph.InputNode {
	if input == nil {
		panic("unreachable: input channel is nil for input node")
	}
	name := fmt.Sprintf("dmInputNode-data-%s", dmNodeConfig.vChannelName)
	node := flowgraph.NewInputNode(
		input,
		name,
		paramtable.Get().DataNodeCfg.FlowGraphMaxQueueLength.GetAsInt32(),
		paramtable.Get().DataNodeCfg.FlowGraphMaxParallelism.GetAsInt32(),
		typeutil.DataNodeRole,
		dmNodeConfig.serverID,
		dmNodeConfig.collectionID,
		metrics.AllLabel,
	)
	return node
}

func createNewInputFromDispatcher(initCtx context.Context,
	dispatcherClient msgdispatcher.Client,
	vchannel string,
	seekPos *msgpb.MsgPosition,
	schema *schemapb.CollectionSchema,
	dbProperties []*commonpb.KeyValuePair,
) (<-chan *msgstream.MsgPack, error) {
	log := log.With(zap.Int64("nodeID", paramtable.GetNodeID()),
		zap.String("vchannel", vchannel))
	replicateID, _ := pkgcommon.GetReplicateID(schema.GetProperties())
	if replicateID == "" {
		log.Info("datanode consume without replicateID, try to get replicateID from dbProperties", zap.Any("dbProperties", dbProperties))
		replicateID, _ = pkgcommon.GetReplicateID(dbProperties)
	}
	replicateConfig := msgstream.GetReplicateConfig(replicateID, schema.GetDbName(), schema.GetName())

	if seekPos != nil && len(seekPos.MsgID) != 0 {
		input, err := dispatcherClient.Register(initCtx, &msgdispatcher.StreamConfig{
			VChannel:        vchannel,
			Pos:             seekPos,
			SubPos:          common.SubscriptionPositionUnknown,
			ReplicateConfig: replicateConfig,
		})
		if err != nil {
			return nil, err
		}
		log.Info("datanode seek successfully when register to msgDispatcher",
			zap.ByteString("msgID", seekPos.GetMsgID()),
			zap.Time("tsTime", tsoutil.PhysicalTime(seekPos.GetTimestamp())),
			zap.Duration("tsLag", time.Since(tsoutil.PhysicalTime(seekPos.GetTimestamp()))))
		return input, err
	}
	input, err := dispatcherClient.Register(initCtx, &msgdispatcher.StreamConfig{
		VChannel:        vchannel,
		Pos:             nil,
		SubPos:          common.SubscriptionPositionEarliest,
		ReplicateConfig: replicateConfig,
	})
	if err != nil {
		return nil, err
	}
	log.Info("datanode consume successfully when register to msgDispatcher")
	return input, err
}
