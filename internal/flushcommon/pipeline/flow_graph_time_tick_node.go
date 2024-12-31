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
	"reflect"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

// make sure ttNode implements flowgraph.Node
var _ flowgraph.Node = (*ttNode)(nil)

type ttNode struct {
	BaseNode
	vChannelName       string
	metacache          metacache.MetaCache
	writeBufferManager writebuffer.BufferManager
	lastUpdateTime     *atomic.Time
	cpUpdater          *util.ChannelCheckpointUpdater
	dropMode           *atomic.Bool
	dropCallback       func()
}

// Name returns node name, implementing flowgraph.Node
func (ttn *ttNode) Name() string {
	return fmt.Sprintf("ttNode-%s", ttn.vChannelName)
}

func (ttn *ttNode) IsValidInMsg(in []Msg) bool {
	if !ttn.BaseNode.IsValidInMsg(in) {
		return false
	}
	_, ok := in[0].(*FlowGraphMsg)
	if !ok {
		log.Warn("type assertion failed for flowGraphMsg", zap.String("name", reflect.TypeOf(in[0]).Name()))
		return false
	}
	return true
}

func (ttn *ttNode) Close() {
}

// Operate handles input messages, implementing flowgraph.Node
func (ttn *ttNode) Operate(in []Msg) []Msg {
	fgMsg := in[0].(*FlowGraphMsg)
	if fgMsg.dropCollection {
		ttn.dropMode.Store(true)
		if ttn.dropCallback != nil {
			defer func() {
				// if drop collection setup, call drop callback
				// For streaming node to cleanup the resources.
				ttn.dropCallback()
			}()
		}
	}

	// skip updating checkpoint for drop collection
	// even if its the close msg
	if ttn.dropMode.Load() {
		log.RatedInfo(1.0, "ttnode in dropMode", zap.String("channel", ttn.vChannelName))
		return []Msg{}
	}

	curTs, _ := tsoutil.ParseTS(fgMsg.TimeRange.TimestampMax)
	if fgMsg.IsCloseMsg() {
		if ttn.dropMode.Load() {
			// if drop collection setup, skip update checkpoint to avoid update dirty checkpoint.
			return in
		}
		if len(fgMsg.EndPositions) > 0 {
			channelPos, _, err := ttn.writeBufferManager.GetCheckpoint(ttn.vChannelName)
			if err != nil {
				log.Warn("channel removed", zap.String("channel", ttn.vChannelName), zap.Error(err))
				return []Msg{}
			}
			log.Info("flowgraph is closing, force update channel CP",
				zap.Time("cpTs", tsoutil.PhysicalTime(channelPos.GetTimestamp())),
				zap.String("channel", channelPos.GetChannelName()))
			ttn.updateChannelCP(channelPos, curTs, false)
		}
		return in
	}

	// Do not block and async updateCheckPoint
	channelPos, needUpdate, err := ttn.writeBufferManager.GetCheckpoint(ttn.vChannelName)
	if err != nil {
		log.Warn("channel removed", zap.String("channel", ttn.vChannelName), zap.Error(err))
		return []Msg{}
	}

	if curTs.Sub(ttn.lastUpdateTime.Load()) >= paramtable.Get().DataNodeCfg.UpdateChannelCheckpointInterval.GetAsDuration(time.Second) {
		ttn.updateChannelCP(channelPos, curTs, false)
		return []Msg{}
	}
	if needUpdate {
		ttn.updateChannelCP(channelPos, curTs, true)
	}
	return []Msg{}
}

func (ttn *ttNode) updateChannelCP(channelPos *msgpb.MsgPosition, curTs time.Time, flush bool) {
	callBack := func() {
		channelCPTs, _ := tsoutil.ParseTS(channelPos.GetTimestamp())
		// reset flush ts to prevent frequent flush
		ttn.writeBufferManager.NotifyCheckpointUpdated(ttn.vChannelName, channelPos.GetTimestamp())
		log.Ctx(context.TODO()).Debug("UpdateChannelCheckpoint success",
			zap.String("channel", ttn.vChannelName),
			zap.Uint64("cpTs", channelPos.GetTimestamp()),
			zap.Time("cpTime", channelCPTs))
	}
	ttn.cpUpdater.AddTask(channelPos, flush, callBack)
	ttn.lastUpdateTime.Store(curTs)
}

func newTTNode(config *nodeConfig, wbManager writebuffer.BufferManager, cpUpdater *util.ChannelCheckpointUpdater) *ttNode {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(paramtable.Get().DataNodeCfg.FlowGraphMaxQueueLength.GetAsInt32())
	baseNode.SetMaxParallelism(paramtable.Get().DataNodeCfg.FlowGraphMaxParallelism.GetAsInt32())

	tt := &ttNode{
		BaseNode:           baseNode,
		vChannelName:       config.vChannelName,
		metacache:          config.metacache,
		writeBufferManager: wbManager,
		lastUpdateTime:     atomic.NewTime(time.Time{}), // set to Zero to update channel checkpoint immediately after fg started
		cpUpdater:          cpUpdater,
		dropMode:           atomic.NewBool(false),
		dropCallback:       config.dropCallback,
	}

	return tt
}
