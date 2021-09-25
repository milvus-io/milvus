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
	"sync"

	"errors"
)

// TimeTickedFlowGraph flowgraph with input from tt msg stream
type TimeTickedFlowGraph struct {
	ctx       context.Context
	cancel    context.CancelFunc
	nodeCtx   map[NodeName]*nodeCtx
	stopOnce  sync.Once
	startOnce sync.Once
}

// AddNode add Node into flowgraph
func (fg *TimeTickedFlowGraph) AddNode(node Node) {
	nodeName := node.Name()
	nodeCtx := nodeCtx{
		node:                   node,
		inputChannels:          make([]chan Msg, 0),
		downstreamInputChanIdx: make(map[string]int),
	}
	fg.nodeCtx[nodeName] = &nodeCtx
}

// SetEdges set directed edges from in nodes to out nodes
func (fg *TimeTickedFlowGraph) SetEdges(nodeName string, in []string, out []string) error {
	currentNode, ok := fg.nodeCtx[nodeName]
	if !ok {
		errMsg := "Cannot find node:" + nodeName
		return errors.New(errMsg)
	}

	// init current node's downstream
	currentNode.downstream = make([]*nodeCtx, len(out))

	// set in nodes
	for i, inNodeName := range in {
		inNode, ok := fg.nodeCtx[inNodeName]
		if !ok {
			errMsg := "Cannot find in node:" + inNodeName
			return errors.New(errMsg)
		}
		inNode.downstreamInputChanIdx[nodeName] = i
	}

	// set out nodes
	for i, n := range out {
		outNode, ok := fg.nodeCtx[n]
		if !ok {
			errMsg := "Cannot find out node:" + n
			return errors.New(errMsg)
		}
		maxQueueLength := outNode.node.MaxQueueLength()
		outNode.inputChannels = append(outNode.inputChannels, make(chan Msg, maxQueueLength))
		currentNode.downstream[i] = outNode
	}

	return nil
}

// Start starts all nodes in timetick flowgragh
func (fg *TimeTickedFlowGraph) Start() {
	fg.startOnce.Do(func() {
		wg := sync.WaitGroup{}
		for _, v := range fg.nodeCtx {
			wg.Add(1)
			go v.Start(fg.ctx, &wg)
		}
		wg.Wait()
	})
}

// Close closes all nodes in flowgraph
func (fg *TimeTickedFlowGraph) Close() {
	fg.stopOnce.Do(func() {
		for _, v := range fg.nodeCtx {
			// maybe need to stop in order
			v.Close()
		}
		fg.cancel()
	})
}

// NewTimeTickedFlowGraph create timetick flowgraph
func NewTimeTickedFlowGraph(ctx context.Context) *TimeTickedFlowGraph {
	ctx1, cancel := context.WithCancel(ctx)
	flowGraph := TimeTickedFlowGraph{
		ctx:     ctx1,
		cancel:  cancel,
		nodeCtx: make(map[string]*nodeCtx),
	}

	return &flowGraph
}
