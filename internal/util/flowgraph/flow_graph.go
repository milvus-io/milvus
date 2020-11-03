package flowgraph

import (
	"context"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type Timestamp = typeutil.Timestamp

type flowGraphStates struct {
	startTick         Timestamp
	numActiveTasks    map[string]int64
	numCompletedTasks map[string]int64
}

type TimeTickedFlowGraph struct {
	ctx     context.Context
	states  *flowGraphStates
	nodeCtx map[string]*nodeCtx
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

func (fg *TimeTickedFlowGraph) Close() error {
	for _, v := range fg.nodeCtx {
		v.Close()
	}
	return nil
}

func NewTimeTickedFlowGraph(ctx context.Context) *TimeTickedFlowGraph {
	flowGraph := TimeTickedFlowGraph{
		ctx: ctx,
		states: &flowGraphStates{
			startTick:         0,
			numActiveTasks:    make(map[string]int64),
			numCompletedTasks: make(map[string]int64),
		},
		nodeCtx: make(map[string]*nodeCtx),
	}

	return &flowGraph
}
