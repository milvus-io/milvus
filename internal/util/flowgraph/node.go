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
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/util/timerecord"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

const (
	// TODO: better to be configured
	nodeCtxTtInterval = 2 * time.Minute
	enableTtChecker   = true
)

// Node is the interface defines the behavior of flowgraph
type Node interface {
	Name() string
	MaxQueueLength() int32
	MaxParallelism() int32
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

// nodeCtx maintains the running context for a Node in flowgragh
type nodeCtx struct {
	node                   Node
	inputChannels          []chan Msg
	inputMessages          []Msg
	downstream             []*nodeCtx
	downstreamInputChanIdx map[string]int

	closeCh chan struct{} // notify work to exit
}

// Start invoke Node `Start` method and start a worker goroutine
func (nodeCtx *nodeCtx) Start() {
	nodeCtx.node.Start()

	go nodeCtx.work()
}

func isCloseMsg(msgs []Msg) bool {
	if len(msgs) == 1 {
		msg, ok := msgs[0].(*MsgStreamMsg)
		return ok && msg.isCloseMsg
	}
	return false
}

// work handles node work spinning
// 1. collectMessage from upstream or just produce Msg from InputNode
// 2. invoke node.Operate
// 3. deliver the Operate result to downstream nodes
func (nodeCtx *nodeCtx) work() {
	name := fmt.Sprintf("nodeCtxTtChecker-%s", nodeCtx.node.Name())
	var checker *timerecord.GroupChecker
	if enableTtChecker {
		checker = timerecord.GetGroupChecker("fgNode", nodeCtxTtInterval, func(list []string) {
			log.Warn("some node(s) haven't received input", zap.Strings("list", list), zap.Duration("duration ", nodeCtxTtInterval))
		})
		checker.Check(name)
		defer checker.Remove(name)
	}

	for {
		select {
		case <-nodeCtx.closeCh:
			log.Debug("flow graph node closed", zap.String("nodeName", nodeCtx.node.Name()))
			return
		default:
			// inputs from inputsMessages for Operate
			var inputs, res []Msg
			if !nodeCtx.node.IsInputNode() {
				nodeCtx.collectInputMessages()
				inputs = nodeCtx.inputMessages
			}
			// the input message decides whether the operate method is executed
			if isCloseMsg(inputs) {
				res = inputs
			}
			if len(res) == 0 {
				n := nodeCtx.node
				res = n.Operate(inputs)
			}
			// the res decide whether the node should be closed.
			if isCloseMsg(res) {
				close(nodeCtx.closeCh)
				nodeCtx.node.Close()
			}

			if enableTtChecker {
				checker.Check(name)
			}

			downstreamLength := len(nodeCtx.downstreamInputChanIdx)
			if len(nodeCtx.downstream) < downstreamLength {
				log.Warn("", zap.Any("nodeCtx.downstream length", len(nodeCtx.downstream)))
			}
			if len(res) < downstreamLength {
				// log.Println("node result length = ", len(res))
				break
			}

			w := sync.WaitGroup{}
			for i := 0; i < downstreamLength; i++ {
				w.Add(1)
				go nodeCtx.downstream[i].deliverMsg(&w, res[i], nodeCtx.downstreamInputChanIdx[nodeCtx.downstream[i].node.Name()])
			}
			w.Wait()
		}
	}
}

// Close handles cleanup logic and notify worker to quit
func (nodeCtx *nodeCtx) Close() {
	if nodeCtx.node.IsInputNode() {
		nodeCtx.node.Close()
	}
}

// deliverMsg tries to put the Msg to specified downstream channel
func (nodeCtx *nodeCtx) deliverMsg(wg *sync.WaitGroup, msg Msg, inputChanIdx int) {
	defer wg.Done()
	defer func() {
		err := recover()
		if err != nil {
			log.Warn(fmt.Sprintln(err))
		}
	}()
	nodeCtx.inputChannels[inputChanIdx] <- msg
}

func (nodeCtx *nodeCtx) collectInputMessages() {
	inputsNum := len(nodeCtx.inputChannels)
	nodeCtx.inputMessages = make([]Msg, inputsNum)

	// init inputMessages,
	// receive messages from inputChannels,
	// and move them to inputMessages.
	for i := 0; i < inputsNum; i++ {
		channel := nodeCtx.inputChannels[i]
		msg, ok := <-channel
		if !ok {
			// TODO: add status
			log.Warn("input channel closed")
			return
		}
		nodeCtx.inputMessages[i] = msg
	}

	// timeTick alignment check
	if len(nodeCtx.inputMessages) > 1 {
		t := nodeCtx.inputMessages[0].TimeTick()
		latestTime := t
		for i := 1; i < len(nodeCtx.inputMessages); i++ {
			if latestTime < nodeCtx.inputMessages[i].TimeTick() {
				latestTime = nodeCtx.inputMessages[i].TimeTick()
			}
		}

		// wait for time tick
		sign := make(chan struct{})
		go func() {
			for i := 0; i < len(nodeCtx.inputMessages); i++ {
				for nodeCtx.inputMessages[i].TimeTick() != latestTime {
					log.Debug("Try to align timestamp", zap.Uint64("t1", latestTime), zap.Uint64("t2", nodeCtx.inputMessages[i].TimeTick()))
					channel := nodeCtx.inputChannels[i]
					msg, ok := <-channel
					if !ok {
						log.Warn("input channel closed")
						return
					}
					nodeCtx.inputMessages[i] = msg
				}
			}
			sign <- struct{}{}
		}()

		select {
		case <-time.After(10 * time.Second):
			panic("Fatal, misaligned time tick, please restart pulsar")
		case <-sign:
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
