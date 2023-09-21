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
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

type Node interface {
	Name() string
	MaxQueueLength() int32
	Operate(in Msg) Msg
	Start()
	Close()
}

type nodeCtx struct {
	node Node

	inputChannel chan Msg

	next    *nodeCtx
	checker *timerecord.GroupChecker

	closeCh chan struct{} // notify work to exit
	closeWg sync.WaitGroup
}

func (c *nodeCtx) Start() {
	c.closeWg.Add(1)
	c.node.Start()
	go c.work()
}

func (c *nodeCtx) Close() {
	close(c.closeCh)
	c.closeWg.Wait()
}

func (c *nodeCtx) work() {
	defer c.closeWg.Done()
	name := fmt.Sprintf("nodeCtxTtChecker-%s", c.node.Name())
	if c.checker != nil {
		c.checker.Check(name)
		defer c.checker.Remove(name)
	}

	for {
		select {
		// close
		case <-c.closeCh:
			c.node.Close()
			close(c.inputChannel)
			log.Debug("pipeline node closed", zap.String("nodeName", c.node.Name()))
			return
		case input := <-c.inputChannel:
			var output Msg
			output = c.node.Operate(input)
			if c.checker != nil {
				c.checker.Check(name)
			}
			if c.next != nil && output != nil {
				c.next.inputChannel <- output
			}
		}
	}
}

func newNodeCtx(node Node) *nodeCtx {
	return &nodeCtx{
		node:         node,
		inputChannel: make(chan Msg, node.MaxQueueLength()),
		closeCh:      make(chan struct{}),
		closeWg:      sync.WaitGroup{},
	}
}

type BaseNode struct {
	name           string
	maxQueueLength int32
}

// Return name of Node
func (node *BaseNode) Name() string {
	return node.name
}

// length of pipeline input chnnel
func (node *BaseNode) MaxQueueLength() int32 {
	return node.maxQueueLength
}

// Start implementing Node, base node does nothing when starts
func (node *BaseNode) Start() {}

// Close implementing Node, base node does nothing when stops
func (node *BaseNode) Close() {}

func NewBaseNode(name string, maxQueryLength int32) *BaseNode {
	return &BaseNode{
		name:           name,
		maxQueueLength: maxQueryLength,
	}
}
