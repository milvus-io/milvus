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

type TimeTickedFlowGraph struct {
	ctx     context.Context
	cancel  context.CancelFunc
	nodeCtx map[NodeName]*nodeCtx
}

func (fg *TimeTickedFlowGraph) AddNode(node Node) {
	nodeName := node.Name()
	nodeCtx := nodeCtx{
		node:                   node,
		inputChannels:          make([]chan Msg, 0),
		downstreamInputChanIdx: make(map[string]int),
	}
	fg.nodeCtx[nodeName] = &nodeCtx
}

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

func (fg *TimeTickedFlowGraph) Start() {
	wg := sync.WaitGroup{}
	for _, v := range fg.nodeCtx {
		wg.Add(1)
		go v.Start(fg.ctx, &wg)
	}
	wg.Wait()
}

func (fg *TimeTickedFlowGraph) Close() {
	for _, v := range fg.nodeCtx {
		// close message stream
		// if v.node.IsInputNode() {
		// 	inStream, ok := v.node.(*InputNode)
		// 	if !ok {
		// 		log.Fatal("Invalid inputNode")
		// 	}
		// 	(*inStream.inStream).Close()
		// }
		v.Close()
	}
	fg.cancel()
}

func NewTimeTickedFlowGraph(ctx context.Context) *TimeTickedFlowGraph {
	ctx1, cancel := context.WithCancel(ctx)
	flowGraph := TimeTickedFlowGraph{
		ctx:     ctx1,
		cancel:  cancel,
		nodeCtx: make(map[string]*nodeCtx),
	}

	return &flowGraph
}
