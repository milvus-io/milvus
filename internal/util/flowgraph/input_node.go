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

	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"go.uber.org/zap"
)

// InputNode is the entry point of flowgragh
type InputNode struct {
	BaseNode
	input        <-chan *msgstream.MsgPack
	lastMsg      *msgstream.MsgPack
	name         string
	role         string
	nodeID       int64
	collectionID int64
	dataType     string
	closeOnce    sync.Once
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

// Operate consume a message pack from msgstream and return
func (inNode *InputNode) Operate(in []Msg) []Msg {
	msgPack, ok := <-inNode.input
	if !ok {
		log.Warn("input closed", zap.Any("input node", inNode.Name()))
		if inNode.lastMsg != nil {
			log.Info("trigger force sync", zap.Int64("collection", inNode.collectionID), zap.Any("position", inNode.lastMsg))
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
	if inNode.role == typeutil.QueryNodeRole {
		metrics.QueryNodeConsumerMsgCount.
			WithLabelValues(fmt.Sprint(inNode.nodeID), inNode.dataType, fmt.Sprint(inNode.collectionID)).
			Inc()

		metrics.QueryNodeConsumeTimeTickLag.
			WithLabelValues(fmt.Sprint(inNode.nodeID), inNode.dataType, fmt.Sprint(inNode.collectionID)).
			Set(float64(sub))
	}

	if inNode.role == typeutil.DataNodeRole {
		metrics.DataNodeConsumeMsgCount.
			WithLabelValues(fmt.Sprint(inNode.nodeID), inNode.dataType, fmt.Sprint(inNode.collectionID)).
			Inc()

		metrics.DataNodeConsumeTimeTickLag.
			WithLabelValues(fmt.Sprint(inNode.nodeID), inNode.dataType, fmt.Sprint(inNode.collectionID)).
			Set(float64(sub))
	}

	var spans []trace.Span
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

	var msgStreamMsg Msg = &MsgStreamMsg{
		tsMessages:     msgPack.Msgs,
		timestampMin:   msgPack.BeginTs,
		timestampMax:   msgPack.EndTs,
		startPositions: msgPack.StartPositions,
		endPositions:   msgPack.EndPositions,
	}

	for _, span := range spans {
		span.End()
	}

	return []Msg{msgStreamMsg}
}

// NewInputNode composes an InputNode with provided input channel, name and parameters
func NewInputNode(input <-chan *msgstream.MsgPack, nodeName string, maxQueueLength int32, maxParallelism int32, role string, nodeID int64, collectionID int64, dataType string) *InputNode {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &InputNode{
		BaseNode:     baseNode,
		input:        input,
		name:         nodeName,
		role:         role,
		nodeID:       nodeID,
		collectionID: collectionID,
		dataType:     dataType,
	}
}
