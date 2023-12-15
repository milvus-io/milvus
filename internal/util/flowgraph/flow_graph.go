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

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"
)

// Flow Graph is no longer a graph rather than a simple pipeline, this simplified our code and increase recovery speed - xiaofan.

// TimeTickedFlowGraph flowgraph with input from tt msg stream
type TimeTickedFlowGraph struct {
	nodeCtx         map[NodeName]*nodeCtx
	nodeSequence    []NodeName
	nodeCtxManager  *nodeCtxManager
	stopOnce        sync.Once
	startOnce       sync.Once
	closeWg         *sync.WaitGroup
	closeGracefully *atomic.Bool
}

// AddNode add Node into flowgraph and fill nodeCtxManager
func (fg *TimeTickedFlowGraph) AddNode(node Node) {
	nodeCtx := nodeCtx{
		node: node,
	}
	fg.nodeCtx[node.Name()] = &nodeCtx
	if node.IsInputNode() {
		fg.nodeCtxManager = NewNodeCtxManager(&nodeCtx, fg.closeWg)
	}
	fg.nodeSequence = append(fg.nodeSequence, node.Name())
}

// SetEdges set directed edges from in nodes to out nodes
func (fg *TimeTickedFlowGraph) SetEdges(nodeName string, out []string) error {
	currentNode, ok := fg.nodeCtx[nodeName]
	if !ok {
		errMsg := "Cannot find node:" + nodeName
		return errors.New(errMsg)
	}

	if len(out) > 1 {
		errMsg := "Flow graph now support only pipeline mode, with only one or zero output:" + nodeName
		return errors.New(errMsg)
	}

	// init current node's downstream
	// set out nodes
	for _, name := range out {
		outNode, ok := fg.nodeCtx[name]
		if !ok {
			errMsg := "Cannot find out node:" + name
			return errors.New(errMsg)
		}
		maxQueueLength := outNode.node.MaxQueueLength()
		outNode.inputChannel = make(chan []Msg, maxQueueLength)
		currentNode.downstream = outNode
	}

	return nil
}

// Start starts all nodes in timetick flowgragh
func (fg *TimeTickedFlowGraph) Start() {
	fg.startOnce.Do(func() {
		for _, v := range fg.nodeCtx {
			v.node.Start()
		}
		fg.nodeCtxManager.Start()
	})
}

func (fg *TimeTickedFlowGraph) Blockall() {
	// Lock with determined order to avoid deadlock.
	for _, nodeName := range fg.nodeSequence {
		fg.nodeCtx[nodeName].Block()
	}
}

func (fg *TimeTickedFlowGraph) Unblock() {
	// Unlock with reverse order.
	for i := len(fg.nodeSequence) - 1; i >= 0; i-- {
		fg.nodeCtx[fg.nodeSequence[i]].Unblock()
	}
}

func (fg *TimeTickedFlowGraph) SetCloseMethod(gracefully bool) {
	for _, v := range fg.nodeCtx {
		if v.node.IsInputNode() {
			v.node.(*InputNode).SetCloseMethod(gracefully)
		}
	}
}

// Close closes all nodes in flowgraph
func (fg *TimeTickedFlowGraph) Close() {
	fg.stopOnce.Do(func() {
		for _, v := range fg.nodeCtx {
			if v.node.IsInputNode() {
				v.Close()
			}
		}
		fg.closeWg.Wait()
	})
}

// NewTimeTickedFlowGraph create timetick flowgraph
func NewTimeTickedFlowGraph(ctx context.Context) *TimeTickedFlowGraph {
	flowGraph := TimeTickedFlowGraph{
		nodeCtx:         make(map[string]*nodeCtx),
		nodeCtxManager:  &nodeCtxManager{},
		closeWg:         &sync.WaitGroup{},
		closeGracefully: atomic.NewBool(CloseImmediately),
	}

	return &flowGraph
}

func (fg *TimeTickedFlowGraph) AssembleNodes(orderedNodes ...Node) error {
	for _, node := range orderedNodes {
		fg.AddNode(node)
	}

	for i, node := range orderedNodes {
		// Set edge to the next node
		if i < len(orderedNodes)-1 {
			err := fg.SetEdges(node.Name(), []string{orderedNodes[i+1].Name()})
			if err != nil {
				errMsg := fmt.Sprintf("set edges failed for flow graph, node=%s", node.Name())
				return errors.New(errMsg)
			}
		}
	}
	return nil
}
