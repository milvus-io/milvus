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
	"math"
	"reflect"
	"sync"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

const (
	updateChanCPInterval = 1 * time.Minute
	updateChanCPTimeout  = 10 * time.Second
)

// make sure ttNode implements flowgraph.Node
var _ flowgraph.Node = (*ttNode)(nil)

type ttNode struct {
	BaseNode
	vChannelName   string
	channel        Channel
	lastUpdateTime *atomic.Time
	dataCoord      types.DataCoordClient

	updateCPLock  sync.Mutex
	notifyChannel chan checkPoint
	closeChannel  chan struct{}
}

type checkPoint struct {
	curTs time.Time
	pos   *msgpb.MsgPosition
}

// Name returns node name, implementing flowgraph.Node
func (ttn *ttNode) Name() string {
	return fmt.Sprintf("ttNode-%s", ttn.vChannelName)
}

func (ttn *ttNode) IsValidInMsg(in []Msg) bool {
	if !ttn.BaseNode.IsValidInMsg(in) {
		return false
	}
	_, ok := in[0].(*flowGraphMsg)
	if !ok {
		log.Warn("type assertion failed for flowGraphMsg", zap.String("name", reflect.TypeOf(in[0]).Name()))
		return false
	}
	return true
}

// Operate handles input messages, implementing flowgraph.Node
func (ttn *ttNode) Operate(in []Msg) []Msg {
	fgMsg := in[0].(*flowGraphMsg)
	curTs, _ := tsoutil.ParseTS(fgMsg.timeRange.timestampMax)
	if fgMsg.IsCloseMsg() {
		if len(fgMsg.endPositions) > 0 {
			close(ttn.closeChannel)
			channelPos := ttn.channel.getChannelCheckpoint(fgMsg.endPositions[0])
			log.Info("flowgraph is closing, force update channel CP",
				zap.Time("cpTs", tsoutil.PhysicalTime(channelPos.GetTimestamp())),
				zap.String("channel", channelPos.GetChannelName()))
			ttn.updateChannelCP(channelPos, curTs)
		}
		return in
	}

	// Do not block and async updateCheckPoint
	channelPos := ttn.channel.getChannelCheckpoint(fgMsg.endPositions[0])
	nonBlockingNotify := func() {
		select {
		case ttn.notifyChannel <- checkPoint{curTs, channelPos}:
		default:
		}
	}

	if curTs.Sub(ttn.lastUpdateTime.Load()) >= updateChanCPInterval {
		nonBlockingNotify()
		return []Msg{}
	}

	if channelPos.GetTimestamp() >= ttn.channel.getFlushTs() {
		nonBlockingNotify()
	}
	return []Msg{}
}

func (ttn *ttNode) updateChannelCP(channelPos *msgpb.MsgPosition, curTs time.Time) error {
	ttn.updateCPLock.Lock()
	defer ttn.updateCPLock.Unlock()

	channelCPTs, _ := tsoutil.ParseTS(channelPos.GetTimestamp())
	// TODO, change to ETCD operation, avoid datacoord operation
	ctx, cancel := context.WithTimeout(context.Background(), updateChanCPTimeout)
	defer cancel()
	resp, err := ttn.dataCoord.UpdateChannelCheckpoint(ctx, &datapb.UpdateChannelCheckpointRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		VChannel: ttn.vChannelName,
		Position: channelPos,
	})
	if err = merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("UpdateChannelCheckpoint failed", zap.String("channel", ttn.vChannelName),
			zap.Time("channelCPTs", channelCPTs), zap.Error(err))
		return err
	}

	ttn.lastUpdateTime.Store(curTs)
	// channelPos ts > flushTs means we could stop flush.
	if channelPos.GetTimestamp() >= ttn.channel.getFlushTs() {
		ttn.channel.setFlushTs(math.MaxUint64)
	}
	log.Info("UpdateChannelCheckpoint success",
		zap.String("channel", ttn.vChannelName),
		zap.Uint64("cpTs", channelPos.GetTimestamp()),
		zap.Time("cpTime", channelCPTs))
	return nil
}

func newTTNode(config *nodeConfig, dc types.DataCoordClient) (*ttNode, error) {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(Params.DataNodeCfg.FlowGraphMaxQueueLength.GetAsInt32())
	baseNode.SetMaxParallelism(Params.DataNodeCfg.FlowGraphMaxParallelism.GetAsInt32())

	tt := &ttNode{
		BaseNode:       baseNode,
		vChannelName:   config.vChannelName,
		channel:        config.channel,
		lastUpdateTime: atomic.NewTime(time.Time{}), // set to Zero to update channel checkpoint immediately after fg started
		dataCoord:      dc,
		notifyChannel:  make(chan checkPoint, 1),
		closeChannel:   make(chan struct{}),
	}

	// check point updater
	go func() {
		for {
			select {
			case <-tt.closeChannel:
				return
			case cp := <-tt.notifyChannel:
				tt.updateChannelCP(cp.pos, cp.curTs)
			}
		}
	}()

	return tt, nil
}
