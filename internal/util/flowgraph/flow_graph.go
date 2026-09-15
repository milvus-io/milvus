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

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

	// closeDrainTimeout bounds the graceful drain of Close(): the single
	// pipeline goroutine may be blocked on an external call (e.g. object
	// storage) that neither completes nor returns an error promptly. After
	// the timeout the drain is abandoned so a hung call cannot stall the
	// caller (WAL close / shutdown) forever.
	closeDrainTimeout time.Duration
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
		return merr.WrapErrParameterInvalidMsg(errMsg)
	}

	if len(out) > 1 {
		errMsg := "Flow graph now support only pipeline mode, with only one or zero output:" + nodeName
		return merr.WrapErrParameterInvalidMsg(errMsg)
	}

	// init current node's downstream
	// set out nodes
	for _, name := range out {
		outNode, ok := fg.nodeCtx[name]
		if !ok {
			errMsg := "Cannot find out node:" + name
			return merr.WrapErrParameterInvalidMsg(errMsg)
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
		// Bound the graceful drain. The whole pipeline runs in one goroutine;
		// if a node is stuck in an external call (e.g. a hung object-storage
		// flush) the close message cannot propagate and the wait would block
		// forever. Abandoning the wait is safe: the stuck goroutine owns the
		// close propagation itself, so once the external call returns it
		// finishes the drain and exits without touching closed channels.
		waitDone := make(chan struct{})
		go func() {
			fg.closeWg.Wait()
			close(waitDone)
		}()
		select {
		case <-waitDone:
		case <-time.After(fg.closeDrainTimeout):
			mlog.Warn(context.TODO(), "flow graph close drain timeout, abandon wait for pipeline to finish",
				mlog.Duration("timeout", fg.closeDrainTimeout))
		}

		// free some source after all node close.
		// such as function.
		for _, v := range fg.nodeCtx {
			v.node.Free()
		}
	})
}

// Status returns the status of the pipeline, it will return "Healthy" if the input node
// has received any msg in the last nodeTtInterval
func (fg *TimeTickedFlowGraph) Status() string {
	diff := time.Since(fg.nodeCtxManager.lastAccessTime.Load())
	if diff > nodeCtxTtInterval {
		return fmt.Sprintf("input node hasn't received any msg in the last %s", diff.String())
	}
	return "Healthy"
}

// NewTimeTickedFlowGraph create timetick flowgraph
func NewTimeTickedFlowGraph(ctx context.Context) *TimeTickedFlowGraph {
	flowGraph := TimeTickedFlowGraph{
		nodeCtx:         make(map[string]*nodeCtx),
		nodeCtxManager:  &nodeCtxManager{lastAccessTime: atomic.NewTime(time.Now())},
		closeWg:         &sync.WaitGroup{},
		closeGracefully: atomic.NewBool(CloseImmediately),
		closeDrainTimeout: paramtable.Get().StreamingCfg.WALCloseGracefulTimeout.
			GetAsDurationByParse(),
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
				return merr.WrapErrParameterInvalidMsg(errMsg)
			}
		}
	}
	return nil
}
