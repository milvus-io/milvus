package flowgraph

import (
	"context"
	"log"
	"sync"
)

const maxQueueLength = 1024

type Node interface {
	Name() string
	MaxQueueLength() int32
	MaxParallelism() int32
	SetPipelineStates(states *flowGraphStates)
	Operate(in []*Msg) []*Msg
}

type BaseNode struct {
	maxQueueLength int32
	maxParallelism int32
	graphStates    *flowGraphStates
}

type nodeCtx struct {
	node                   *Node
	inputChannels          []chan *Msg
	inputMessages          [][]*Msg
	downstream             []*nodeCtx
	downstreamInputChanIdx map[string]int
}

func (nodeCtx *nodeCtx) Start(ctx context.Context, wg *sync.WaitGroup) {
	for {
		select {
		case <-ctx.Done():
			wg.Done()
			return
		default:
			if !nodeCtx.allUpstreamDone() {
				continue
			}
			nodeCtx.getMessagesFromChannel()
			// inputs from inputsMessages for Operate
			inputs := make([]*Msg, 0)
			for i := 0; i < len(nodeCtx.inputMessages); i++ {
				inputs = append(inputs, nodeCtx.inputMessages[i]...)
			}
			n := *nodeCtx.node
			res := n.Operate(inputs)
			wg := sync.WaitGroup{}
			downstreamLength := len(nodeCtx.downstreamInputChanIdx)
			if len(nodeCtx.downstream) < downstreamLength {
				log.Fatal("nodeCtx.downstream length = ", len(nodeCtx.downstream))
			}
			if len(res) < downstreamLength {
				log.Fatal("node result length = ", len(res))
			}
			for i := 0; i < downstreamLength; i++ {
				wg.Add(1)
				go nodeCtx.downstream[i].ReceiveMsg(&wg, res[i], nodeCtx.downstreamInputChanIdx[(*nodeCtx.downstream[i].node).Name()])
			}
			wg.Wait()
		}
	}
}

func (nodeCtx *nodeCtx) Close() {
	for _, channel := range nodeCtx.inputChannels {
		close(channel)
	}
}

func (nodeCtx *nodeCtx) ReceiveMsg(wg *sync.WaitGroup, msg *Msg, inputChanIdx int) {
	nodeCtx.inputChannels[inputChanIdx] <- msg
	// fmt.Println((*nodeCtx.node).Name(), "receive to input channel ", inputChanIdx)

	wg.Done()
}

func (nodeCtx *nodeCtx) allUpstreamDone() bool {
	inputsNum := len(nodeCtx.inputChannels)
	hasInputs := 0
	for i := 0; i < inputsNum; i++ {
		channel := nodeCtx.inputChannels[i]
		if len(channel) > 0 {
			hasInputs++
		}
	}
	return hasInputs == inputsNum
}

func (nodeCtx *nodeCtx) getMessagesFromChannel() {
	inputsNum := len(nodeCtx.inputChannels)
	nodeCtx.inputMessages = make([][]*Msg, inputsNum)

	// init inputMessages,
	// receive messages from inputChannels,
	// and move them to inputMessages.
	for i := 0; i < inputsNum; i++ {
		nodeCtx.inputMessages[i] = make([]*Msg, 0)
		channel := nodeCtx.inputChannels[i]
		msg := <-channel
		nodeCtx.inputMessages[i] = append(nodeCtx.inputMessages[i], msg)
	}
}

func (node *BaseNode) MaxQueueLength() int32 {
	return node.maxQueueLength
}

func (node *BaseNode) MaxParallelism() int32 {
	return node.maxParallelism
}

func (node *BaseNode) SetMaxQueueLength(n int32) {
	node.maxQueueLength = n
}

func (node *BaseNode) SetMaxParallelism(n int32) {
	node.maxParallelism = n
}

func (node *BaseNode) SetPipelineStates(states *flowGraphStates) {
	node.graphStates = states
}
