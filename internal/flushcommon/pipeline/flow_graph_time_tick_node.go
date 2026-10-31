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

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var _ flowgraph.Node = (*ttNode)(nil)

// ttNode completes the pipeline's collection lifecycle after data processing.
type ttNode struct {
	BaseNode
	vChannelName string
	dropMode     *atomic.Bool
	dropCallback func()
}

func (ttn *ttNode) Name() string {
	return fmt.Sprintf("ttNode-%s", ttn.vChannelName)
}

func (ttn *ttNode) IsValidInMsg(in []Msg) bool {
	if !ttn.BaseNode.IsValidInMsg(in) {
		return false
	}
	_, ok := in[0].(*FlowGraphMsg)
	if !ok {
		mlog.Warn(context.TODO(), "type assertion failed for flowGraphMsg", mlog.String("name", reflect.TypeOf(in[0]).Name()))
		return false
	}
	return true
}

func (ttn *ttNode) Close() {}

func (ttn *ttNode) Operate(in []Msg) []Msg {
	fgMsg := in[0].(*FlowGraphMsg)
	if fgMsg.dropCollection {
		ttn.dropMode.Store(true)
		if ttn.dropCallback != nil {
			ttn.dropCallback()
		}
	}
	if ttn.dropMode.Load() {
		return []Msg{}
	}
	if fgMsg.IsCloseMsg() {
		return in
	}
	return []Msg{}
}

func newTTNode(config *nodeConfig) *ttNode {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(paramtable.Get().DataNodeCfg.FlowGraphMaxQueueLength.GetAsInt32())
	baseNode.SetMaxParallelism(paramtable.Get().DataNodeCfg.FlowGraphMaxParallelism.GetAsInt32())
	return &ttNode{
		BaseNode:     baseNode,
		vChannelName: config.vChannelName,
		dropMode:     atomic.NewBool(false),
		dropCallback: config.dropCallback,
	}
}
