package flowgraph

import (
	"context"
	"log"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/errors"
)

type TimeTickedFlowGraph struct {
	ctx     context.Context
	nodeCtx map[NodeName]*nodeCtx
}

func (fg *TimeTickedFlowGraph) AddNode(node *Node) {
	nodeName := (*node).Name()
	nodeCtx := nodeCtx{
		node:                   node,
		inputChannels:          make([]chan *Msg, 0),
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
		maxQueueLength := (*outNode.node).MaxQueueLength()
		outNode.inputChannels = append(outNode.inputChannels, make(chan *Msg, maxQueueLength))
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
		if (*v.node).IsInputNode() {
			inStream, ok := (*v.node).(*InputNode)
			if !ok {
				log.Fatal("Invalid inputNode")
			}
			(*inStream.inStream).Close()
		}
		// close input channels
		v.Close()
	}
}

func NewTimeTickedFlowGraph(ctx context.Context) *TimeTickedFlowGraph {
	flowGraph := TimeTickedFlowGraph{
		ctx:     ctx,
		nodeCtx: make(map[string]*nodeCtx),
	}

	return &flowGraph
}
