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

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
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

	if fgMsg.isAlterWal && !needUpdate {
		channelPos, needUpdate, err = ttn.waitForCheckpointUpdate(fgMsg, curTs)
	}

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

// waitForCheckpointUpdate waits for checkpoint to be ready using exponential backoff retry
func (ttn *ttNode) waitForCheckpointUpdate(fgMsg *FlowGraphMsg, curTs time.Time) (*msgpb.MsgPosition, bool, error) {
	ctx := context.Background()
	backoffConfig := backoff.NewExponentialBackOff()
	backoffConfig.InitialInterval = 100 * time.Millisecond
	backoffConfig.MaxInterval = 1 * time.Second
	backoffConfig.Multiplier = 1.5
	backoffConfig.MaxElapsedTime = 0 // No max elapsed time limit
	backoffConfig.Reset()

	var channelPos *msgpb.MsgPosition
	var needUpdate bool

	operation := func() error {
		var retryErr error
		channelPos, needUpdate, retryErr = ttn.writeBufferManager.GetCheckpoint(ttn.vChannelName)
		if retryErr != nil {
			return retryErr
		}
		if !needUpdate {
			// Return a temporary error to trigger retry with backoff
			return fmt.Errorf("checkpoint not ready yet")
		}
		// needUpdate is true, operation succeeded
		return nil
	}

	notify := func(err error, duration time.Duration) {
		var channelCheckpointTS uint64
		if channelPos != nil {
			channelCheckpointTS = channelPos.GetTimestamp()
		}
		log.Ctx(ctx).Info("waiting for checkpoint update, retrying with backoff",
			zap.String("channel", ttn.vChannelName),
			zap.Uint64("currentCheckpointTS", channelCheckpointTS),
			zap.Uint64("requiredTimeTick", fgMsg.TimeTick()),
			zap.Uint64("alterWalTimeTick", fgMsg.alterWalTimeTick),
			zap.Duration("retryInterval", duration),
		)
	}

	backoffCtx := backoff.WithContext(backoffConfig, ctx)
	if err := backoff.RetryNotify(operation, backoffCtx, notify); err != nil {
		log.Ctx(ctx).Warn("failed to wait for checkpoint update",
			zap.String("channel", ttn.vChannelName),
			zap.Error(err),
		)
		return channelPos, needUpdate, err
	}

	log.Ctx(ctx).Info("checkpoint update ready",
		zap.String("channel", ttn.vChannelName),
		zap.Uint64("checkpointTS", channelPos.GetTimestamp()),
	)
	return channelPos, needUpdate, nil
}

func (ttn *ttNode) updateChannelCP(channelPos *msgpb.MsgPosition, curTs time.Time, flush bool) {
	callBack := func() {
		channelCPTs, _ := tsoutil.ParseTS(channelPos.GetTimestamp())
		// reset flush ts to prevent frequent flush
		ttn.writeBufferManager.NotifyCheckpointUpdated(ttn.vChannelName, channelPos.GetTimestamp())
		log.Ctx(context.TODO()).Debug("UpdateChannelCheckpoint success",
			zap.String("channel", ttn.vChannelName),
			zap.Uint64("cpTs", channelPos.GetTimestamp()),
			zap.Stringer("walName", channelPos.WALName),
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
