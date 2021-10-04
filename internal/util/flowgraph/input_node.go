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

package flowgraph

import (
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/opentracing/opentracing-go"
	oplog "github.com/opentracing/opentracing-go/log"
	"go.uber.org/zap"
)

// InputNode is the entry point of flowgragh
type InputNode struct {
	BaseNode
	inStream msgstream.MsgStream
	name     string
}

// IsInputNode returns whether Node is InputNode
func (inNode *InputNode) IsInputNode() bool {
	return true
}

// Start is used to start input msgstream
func (inNode *InputNode) Start() {
	inNode.inStream.Start()
}

// Close implements node
func (inNode *InputNode) Close() {
	inNode.inStream.Close()
	log.Debug("message stream closed",
		zap.String("node name", inNode.name),
	)
}

// Name returns node name
func (inNode *InputNode) Name() string {
	return inNode.name
}

// InStream returns the internal MsgStream
func (inNode *InputNode) InStream() msgstream.MsgStream {
	return inNode.inStream
}

// Operate consume a message pack from msgstream and return
func (inNode *InputNode) Operate(in []Msg) []Msg {
	msgPack := inNode.inStream.Consume()

	// TODO: add status
	if msgPack == nil {
		return nil
	}
	var spans []opentracing.Span
	for _, msg := range msgPack.Msgs {
		sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
		sp.LogFields(oplog.String("input_node name", inNode.Name()))
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
		span.Finish()
	}

	return []Msg{msgStreamMsg}
}

// NewInputNode composes an InputNode with provided MsgStream, name and parameters
func NewInputNode(inStream msgstream.MsgStream, nodeName string, maxQueueLength int32, maxParallelism int32) *InputNode {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &InputNode{
		BaseNode: baseNode,
		inStream: inStream,
		name:     nodeName,
	}
}
