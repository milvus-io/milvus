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

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// DmInputNode receives messages from message streams, packs messages between two timeticks, and passes all
//
// messages between two timeticks to the following flowgraph node. In DataNode, the following flow graph node is
// flowgraph ddNode.
func newDmInputNode(initCtx context.Context, dispatcherClient msgdispatcher.Client, seekPos *msgpb.MsgPosition, dmNodeConfig *nodeConfig) (*flowgraph.InputNode, error) {
	log := log.With(zap.Int64("nodeID", paramtable.GetNodeID()),
		zap.String("vchannel", dmNodeConfig.vChannelName))

	var (
		input <-chan *msgstream.MsgPack
		err   error
		start = time.Now()
	)

	if seekPos != nil && len(seekPos.MsgID) != 0 {
		err := retry.Handle(initCtx, func() (bool, error) {
			input, err = dispatcherClient.Register(initCtx, dmNodeConfig.vChannelName, seekPos, common.SubscriptionPositionUnknown)
			if err != nil {
				log.Warn("datanode consume failed", zap.Error(err))
				return errors.Is(err, msgdispatcher.ErrTooManyConsumers), err
			}
			return false, nil
		}, retry.Sleep(paramtable.Get().MQCfg.RetrySleep.GetAsDuration(time.Second)), // 5 seconds
			retry.MaxSleepTime(paramtable.Get().MQCfg.RetryTimeout.GetAsDuration(time.Second))) // 5 minutes
		if err != nil {
			log.Warn("datanode consume failed after retried", zap.Error(err))
			return nil, err
		}

		log.Info("datanode seek successfully when register to msgDispatcher",
			zap.ByteString("msgID", seekPos.GetMsgID()),
			zap.Time("tsTime", tsoutil.PhysicalTime(seekPos.GetTimestamp())),
			zap.Duration("tsLag", time.Since(tsoutil.PhysicalTime(seekPos.GetTimestamp()))),
			zap.Duration("dur", time.Since(start)))
	} else {
		err = retry.Handle(initCtx, func() (bool, error) {
			input, err = dispatcherClient.Register(initCtx, dmNodeConfig.vChannelName, nil, common.SubscriptionPositionEarliest)
			if err != nil {
				log.Warn("datanode consume failed", zap.Error(err))
				return errors.Is(err, msgdispatcher.ErrTooManyConsumers), err
			}
			return false, nil
		}, retry.Sleep(paramtable.Get().MQCfg.RetrySleep.GetAsDuration(time.Second)), // 5 seconds
			retry.MaxSleepTime(paramtable.Get().MQCfg.RetryTimeout.GetAsDuration(time.Second))) // 5 minutes
		if err != nil {
			log.Warn("datanode consume failed after retried", zap.Error(err))
			return nil, err
		}
	
		log.Info("datanode consume successfully when register to msgDispatcher")
	}

	name := fmt.Sprintf("dmInputNode-data-%s", dmNodeConfig.vChannelName)
	node := flowgraph.NewInputNode(
		input,
		name,
		Params.DataNodeCfg.FlowGraphMaxQueueLength.GetAsInt32(),
		Params.DataNodeCfg.FlowGraphMaxParallelism.GetAsInt32(),
		typeutil.DataNodeRole,
		dmNodeConfig.serverID,
		dmNodeConfig.collectionID,
		metrics.AllLabel,
	)
	return node, nil
}
