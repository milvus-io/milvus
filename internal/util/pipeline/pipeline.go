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
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
)

type Pipeline interface {
	Add(node ...Node)
	Start() error
	Close()
}
type pipeline struct {
	nodes []*nodeCtx

	inputChannel    chan Msg
	nodeTtInterval  time.Duration
	enableTtChecker bool
}

func (p *pipeline) Add(nodes ...Node) {
	for _, node := range nodes {
		p.addNode(node)
	}
}

func (p *pipeline) addNode(node Node) {
	nodeCtx := NewNodeCtx(node)
	if p.enableTtChecker {
		manager := timerecord.GetCheckerManger("fgNode", p.nodeTtInterval, func(list []string) {
			log.Warn("some node(s) haven't received input", zap.Strings("list", list), zap.Duration("duration ", p.nodeTtInterval))
		})
		name := fmt.Sprintf("nodeCtxTtChecker-%s", node.Name())
		nodeCtx.Checker = timerecord.NewChecker(name, manager)
	}

	if len(p.nodes) != 0 {
		p.nodes[len(p.nodes)-1].Next = nodeCtx
	} else {
		p.inputChannel = nodeCtx.InputChannel
	}

	p.nodes = append(p.nodes, nodeCtx)
}

func (p *pipeline) Start() error {
	return nil
}

func (p *pipeline) Close() {
	for _, node := range p.nodes {
		node.node.Close()
		if node.Checker != nil {
			node.Checker.Close()
		}
	}
}

func (p *pipeline) process() {
	if len(p.nodes) == 0 {
		return
	}

	curNode := p.nodes[0]
	for curNode != nil {
		if len(curNode.InputChannel) == 0 {
			break
		}

		input := <-curNode.InputChannel
		output := curNode.node.Operate(input)
		if curNode.Checker != nil {
			curNode.Checker.Check()
		}
		if curNode.Next != nil && output != nil {
			curNode.Next.InputChannel <- output
		}
		curNode = curNode.Next
	}
}
