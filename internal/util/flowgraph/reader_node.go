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
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const timeoutForEachRead = 10 * time.Second

// ReaderNode is the entry point of a flow graph
type ReaderNode struct {
	BaseNode
	ctx     context.Context
	stream  msgstream.MsgStream
	channel string
	name    string
}

// IsInputNode returns whether Node is InputNode
func (r *ReaderNode) IsInputNode() bool {
	return true
}

// Name returns node name
func (r *ReaderNode) Name() string {
	return r.name
}

// Operate consume a message pack from msgStream and return
func (r *ReaderNode) Operate(in []Msg) []Msg {
	msgPack := &msgstream.MsgPack{
		BeginTs:        typeutil.ZeroTimestamp,
		EndTs:          typeutil.ZeroTimestamp,
		Msgs:           make([]msgstream.TsMsg, 0),
		StartPositions: make([]*msgstream.MsgPosition, 0),
		EndPositions:   make([]*msgstream.MsgPosition, 0),
	}

	// begin to read
	for r.stream.HasNext(r.channel) {
		func() {
			ctx, cancel := context.WithTimeout(r.ctx, timeoutForEachRead)
			defer cancel()
			tsMsg, err := r.stream.Next(ctx, r.channel)
			if err != nil {
				log.Error(err.Error())
				return
			}
			if tsMsg == nil {
				return
			}
			msgPack.Msgs = append(msgPack.Msgs, tsMsg)
		}()
	}

	// read done
	var msgStreamMsg Msg = &MsgStreamMsg{
		tsMessages:     msgPack.Msgs,
		timestampMin:   msgPack.BeginTs,
		timestampMax:   msgPack.EndTs,
		startPositions: msgPack.StartPositions,
		endPositions:   msgPack.EndPositions,
	}
	return []Msg{msgStreamMsg}
}

// NewReaderNode returns a new ReaderNode with provided MsgStream, name and parameters
func NewReaderNode(ctx context.Context, stream msgstream.MsgStream, channel string, nodeName string, maxQueueLength int32, maxParallelism int32) *ReaderNode {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &ReaderNode{
		BaseNode: baseNode,
		ctx:      ctx,
		stream:   stream,
		channel:  channel,
		name:     nodeName,
	}
}
