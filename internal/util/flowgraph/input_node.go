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

package flowgraph

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	CloseGracefully  bool = true
	CloseImmediately bool = false
)

type inputNodeMetricsKey struct {
	nodeID       string
	dataType     string
	collectionID string
}

type inputNodeMetricsHandle struct {
	consumeMsgCount    prometheus.Counter
	consumeTimeTickLag prometheus.Gauge
	references         int
}

var inputNodeMetricsCache = struct {
	sync.Mutex
	handles map[inputNodeMetricsKey]*inputNodeMetricsHandle
}{
	handles: make(map[inputNodeMetricsKey]*inputNodeMetricsHandle),
}

func acquireInputNodeMetrics(key inputNodeMetricsKey) *inputNodeMetricsHandle {
	inputNodeMetricsCache.Lock()
	defer inputNodeMetricsCache.Unlock()

	handle, ok := inputNodeMetricsCache.handles[key]
	if !ok {
		handle = &inputNodeMetricsHandle{
			consumeMsgCount: metrics.DataNodeConsumeMsgCount.WithLabelValues(
				key.nodeID,
				key.dataType,
				key.collectionID,
			),
			consumeTimeTickLag: metrics.DataNodeConsumeTimeTickLag.WithLabelValues(
				key.nodeID,
				key.dataType,
				key.collectionID,
			),
		}
		inputNodeMetricsCache.handles[key] = handle
	}
	handle.references++
	return handle
}

func releaseInputNodeMetrics(key inputNodeMetricsKey) {
	inputNodeMetricsCache.Lock()
	defer inputNodeMetricsCache.Unlock()

	handle, ok := inputNodeMetricsCache.handles[key]
	if !ok {
		return
	}
	handle.references--
	if handle.references > 0 {
		return
	}

	metrics.DataNodeConsumeMsgCount.DeleteLabelValues(key.nodeID, key.dataType, key.collectionID)
	metrics.DataNodeConsumeTimeTickLag.DeleteLabelValues(key.nodeID, key.dataType, key.collectionID)
	delete(inputNodeMetricsCache.handles, key)
}

// InputNode is the entry point of flowgragh
type InputNode struct {
	BaseNode
	input              <-chan *msgstream.MsgPack
	lastMsg            *msgstream.MsgPack
	name               string
	role               string
	nodeID             int64
	nodeIDStr          string
	collectionID       int64
	collectionIDStr    string
	dataType           string
	consumeMsgCount    prometheus.Counter
	consumeTimeTickLag prometheus.Gauge
	metricsKey         inputNodeMetricsKey
	metricsReleaseOnce sync.Once

	closeGracefully *atomic.Bool

	skipMode            bool
	skipCount           int
	lastNotTimetickTime time.Time
}

// IsInputNode returns whether Node is InputNode
func (inNode *InputNode) IsInputNode() bool {
	return true
}

func (inNode *InputNode) IsValidInMsg(in []Msg) bool {
	return true
}

// Name returns node name
func (inNode *InputNode) Name() string {
	return inNode.name
}

func (inNode *InputNode) SetCloseMethod(gracefully bool) {
	inNode.closeGracefully.Store(gracefully)
	mlog.Info(context.TODO(), "input node close method set",
		mlog.String("node", inNode.Name()),
		mlog.Int64("collection", inNode.collectionID),
		mlog.Bool("gracefully", gracefully))
}

func (inNode *InputNode) Free() {
	if inNode.role != typeutil.DataNodeRole {
		return
	}
	inNode.metricsReleaseOnce.Do(func() {
		releaseInputNodeMetrics(inNode.metricsKey)
	})
}

// Operate consume a message pack from msgstream and return
func (inNode *InputNode) Operate(in []Msg) []Msg {
	msgPack, ok := <-inNode.input
	if !ok {
		log := mlog.With(
			mlog.String("node", inNode.Name()),
			mlog.Int64("collection", inNode.collectionID),
		)
		log.Info(context.TODO(), "input node message stream closed",
			mlog.Bool("closeGracefully", inNode.closeGracefully.Load()),
		)
		if inNode.lastMsg != nil && inNode.closeGracefully.Load() {
			log.Info(context.TODO(), "input node trigger force sync",
				mlog.Any("position", inNode.lastMsg.EndPositions))
			return []Msg{&MsgStreamMsg{
				BaseMsg:        NewBaseMsg(true),
				tsMessages:     []msgstream.TsMsg{},
				timestampMin:   inNode.lastMsg.BeginTs,
				timestampMax:   inNode.lastMsg.EndTs,
				startPositions: inNode.lastMsg.StartPositions,
				endPositions:   inNode.lastMsg.EndPositions,
			}}
		}
		return []Msg{&MsgStreamMsg{
			BaseMsg: NewBaseMsg(true),
		}}
	}

	// TODO: add status
	if msgPack == nil {
		return []Msg{}
	}

	inNode.lastMsg = msgPack
	sub := tsoutil.SubByNow(msgPack.EndTs)
	if inNode.role == typeutil.DataNodeRole {
		inNode.consumeMsgCount.Inc()
		inNode.consumeTimeTickLag.Set(float64(sub))
	}

	var spans []trace.Span
	defer func() {
		for _, span := range spans {
			span.End()
		}
	}()
	for _, msg := range msgPack.Msgs {
		ctx := msg.TraceCtx()
		if ctx == nil {
			ctx = context.Background()
		}
		ctx, sp := otel.Tracer(inNode.role).Start(ctx, "Operate")
		sp.AddEvent("input_node name" + inNode.Name())
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}

	// skip timetick message feature
	if inNode.role == typeutil.DataNodeRole &&
		len(msgPack.Msgs) > 0 &&
		paramtable.Get().DataNodeCfg.FlowGraphSkipModeEnable.GetAsBool() {
		if msgPack.Msgs[0].Type() == commonpb.MsgType_TimeTick {
			if inNode.skipMode {
				// if empty timetick message and in skipMode, will skip some of the timetick messages to reduce downstream work
				if inNode.skipCount == paramtable.Get().DataNodeCfg.FlowGraphSkipModeSkipNum.GetAsInt() {
					inNode.skipCount = 0
				} else {
					inNode.skipCount = inNode.skipCount + 1
					return []Msg{}
				}
			} else {
				cd := paramtable.Get().DataNodeCfg.FlowGraphSkipModeColdTime.GetAsInt()
				if time.Since(inNode.lastNotTimetickTime) > time.Second*time.Duration(cd) {
					inNode.skipMode = true
				}
			}
		} else {
			// if non empty message, refresh the lastNotTimetickTime and close skip mode
			inNode.skipMode = false
			inNode.skipCount = 0
			inNode.lastNotTimetickTime = time.Now()
		}
	}

	var msgStreamMsg Msg = &MsgStreamMsg{
		tsMessages:     msgPack.Msgs,
		timestampMin:   msgPack.BeginTs,
		timestampMax:   msgPack.EndTs,
		startPositions: msgPack.StartPositions,
		endPositions:   msgPack.EndPositions,
	}

	return []Msg{msgStreamMsg}
}

// NewInputNode composes an InputNode with provided input channel, name and parameters
func NewInputNode(input <-chan *msgstream.MsgPack, nodeName string, maxQueueLength int32, maxParallelism int32, role string, nodeID int64, collectionID int64, dataType string) *InputNode {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	nodeIDStr := fmt.Sprint(nodeID)
	collectionIDStr := fmt.Sprint(collectionID)
	node := &InputNode{
		BaseNode:            baseNode,
		input:               input,
		name:                nodeName,
		role:                role,
		nodeID:              nodeID,
		nodeIDStr:           nodeIDStr,
		collectionID:        collectionID,
		collectionIDStr:     collectionIDStr,
		dataType:            dataType,
		closeGracefully:     atomic.NewBool(CloseImmediately),
		skipCount:           0,
		lastNotTimetickTime: time.Now(),
	}
	if role == typeutil.DataNodeRole {
		node.metricsKey = inputNodeMetricsKey{
			nodeID:       nodeIDStr,
			dataType:     dataType,
			collectionID: collectionIDStr,
		}
		handle := acquireInputNodeMetrics(node.metricsKey)
		node.consumeMsgCount = handle.consumeMsgCount
		node.consumeTimeTickLag = handle.consumeTimeTickLag
	}
	return node
}
