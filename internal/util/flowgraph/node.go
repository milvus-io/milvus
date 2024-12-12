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

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

const (
	// TODO: better to be configured
	nodeCtxTtInterval = 2 * time.Minute
	enableTtChecker   = true
	// blockAll should wait no more than 10 seconds
	blockAllWait = 10 * time.Second
)

// Node is the interface defines the behavior of flowgraph
type Node interface {
	Name() string
	MaxQueueLength() int32
	MaxParallelism() int32
	IsValidInMsg(in []Msg) bool
	Operate(in []Msg) []Msg
	IsInputNode() bool
	Start()
	Close()
}

// BaseNode defines some common node attributes and behavior
type BaseNode struct {
	maxQueueLength int32
	maxParallelism int32
}

// manage nodeCtx
type nodeCtxManager struct {
	inputNodeCtx *nodeCtx
	closeWg      *sync.WaitGroup
	closeOnce    sync.Once
	closeCh      chan struct{} // notify nodes to exit

	lastAccessTime *atomic.Time
}

// NewNodeCtxManager init with the inputNode and fg.closeWg
func NewNodeCtxManager(nodeCtx *nodeCtx, closeWg *sync.WaitGroup) *nodeCtxManager {
	return &nodeCtxManager{
		inputNodeCtx:   nodeCtx,
		closeWg:        closeWg,
		closeCh:        make(chan struct{}),
		lastAccessTime: atomic.NewTime(time.Now()),
	}
}

// Start invoke Node `Start` method and start a worker goroutine
func (nodeCtxManager *nodeCtxManager) Start() {
	// in dmInputNode, message from mq to channel, alloc goroutines
	// limit the goroutines in other node to prevent huge goroutines numbers
	nodeCtxManager.closeWg.Add(1)
	curNode := nodeCtxManager.inputNodeCtx
	// tt checker start
	if enableTtChecker {
		manager := timerecord.GetCheckerManger("data-fgNode", nodeCtxTtInterval, func(list []string) {
			log.Warn("some node(s) haven't received input", zap.Strings("list", list), zap.Duration("duration ", nodeCtxTtInterval))
		})
		for curNode != nil {
			name := fmt.Sprintf("nodeCtxTtChecker-%s", curNode.node.Name())
			curNode.checker = timerecord.NewChecker(name, manager)
			curNode = curNode.downstream
		}
	}
	go nodeCtxManager.workNodeStart()
}

func (nodeCtxManager *nodeCtxManager) workNodeStart() {
	defer nodeCtxManager.closeWg.Done()
	for {
		select {
		case <-nodeCtxManager.closeCh:
			return
		// handles node work spinning
		// 1. collectMessage from upstream or just produce Msg from InputNode
		// 2. invoke node.Operate
		// 3. deliver the Operate result to downstream nodes
		default:
			inputNode := nodeCtxManager.inputNodeCtx
			curNode := inputNode
			for curNode != nil {
				// inputs from inputsMessages for Operate
				var input, output []Msg
				if curNode != inputNode {
					// inputNode.input not from nodeCtx.inputChannel
					input = <-curNode.inputChannel
				}
				// the input message decides whether the operate method is executed
				n := curNode.node
				curNode.blockMutex.RLock()
				if !n.IsValidInMsg(input) {
					curNode.blockMutex.RUnlock()
					curNode = inputNode
					continue
				}

				if nodeCtxManager.lastAccessTime != nil {
					nodeCtxManager.lastAccessTime.Store(time.Now())
				}

				output = n.Operate(input)
				curNode.blockMutex.RUnlock()
				// the output decide whether the node should be closed.
				if isCloseMsg(output) {
					nodeCtxManager.closeOnce.Do(func() {
						close(nodeCtxManager.closeCh)
					})
					if curNode.inputChannel != nil {
						close(curNode.inputChannel)
					}
				}
				// deliver to all following flow graph node.
				if curNode.downstream != nil {
					curNode.downstream.inputChannel <- output
				}
				if enableTtChecker && curNode.checker != nil {
					curNode.checker.Check()
				}
				curNode = curNode.downstream
			}
		}
	}
}

// Close handles cleanup logic and notify worker to quit
func (nodeCtxManager *nodeCtxManager) Close() {
	nodeCtx := nodeCtxManager.inputNodeCtx
	nodeCtx.Close()
}

// nodeCtx maintains the running context for a Node in flowgragh
type nodeCtx struct {
	node         Node
	inputChannel chan []Msg
	downstream   *nodeCtx
	checker      *timerecord.Checker

	blockMutex sync.RWMutex
}

func (nodeCtx *nodeCtx) Block() {
	// input node operate function will be blocking
	if !nodeCtx.node.IsInputNode() {
		startTs := time.Now()
		nodeCtx.blockMutex.Lock()
		if time.Since(startTs) >= blockAllWait {
			log.Warn("flow graph wait for long time",
				zap.String("name", nodeCtx.node.Name()),
				zap.Duration("wait time", time.Since(startTs)))
		}
	}
}

func (nodeCtx *nodeCtx) Unblock() {
	if !nodeCtx.node.IsInputNode() {
		nodeCtx.blockMutex.Unlock()
	}
}

func isCloseMsg(msgs []Msg) bool {
	if len(msgs) == 1 {
		return msgs[0].IsClose()
	}
	return false
}

// Close handles cleanup logic and notify worker to quit
func (nodeCtx *nodeCtx) Close() {
	if nodeCtx.node.IsInputNode() {
		for nodeCtx != nil {
			nodeCtx.node.Close()
			if nodeCtx.checker != nil {
				nodeCtx.checker.Close()
			}
			log.Ctx(context.TODO()).Debug("flow graph node closed", zap.String("nodeName", nodeCtx.node.Name()))
			nodeCtx = nodeCtx.downstream
		}
	}
}

// MaxQueueLength returns the maximal queue length
func (node *BaseNode) MaxQueueLength() int32 {
	return node.maxQueueLength
}

// MaxParallelism returns the maximal parallelism
func (node *BaseNode) MaxParallelism() int32 {
	return node.maxParallelism
}

// SetMaxQueueLength is used to set the maximal queue length
func (node *BaseNode) SetMaxQueueLength(n int32) {
	node.maxQueueLength = n
}

// SetMaxParallelism is used to set the maximal parallelism
func (node *BaseNode) SetMaxParallelism(n int32) {
	node.maxParallelism = n
}

// IsInputNode returns whether Node is InputNode, BaseNode is not InputNode by default
func (node *BaseNode) IsInputNode() bool {
	return false
}

// Start implementing Node, base node does nothing when starts
func (node *BaseNode) Start() {}

// Close implementing Node, base node does nothing when stops
func (node *BaseNode) Close() {}

func (node *BaseNode) Name() string {
	return "BaseNode"
}

func (node *BaseNode) Operate(in []Msg) []Msg {
	return in
}

func (node *BaseNode) IsValidInMsg(in []Msg) bool {
	if in == nil {
		log.Info("type assertion failed because it's nil")
		return false
	}

	if len(in) == 0 {
		// avoid printing too many logs.
		return false
	}

	if len(in) != 1 {
		log.Warn("Invalid operate message input", zap.Int("input length", len(in)))
		return false
	}
	return true
}
