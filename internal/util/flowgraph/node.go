package flowgraph

import (
	"context"
	"log"
	"sync"
)

type Node interface {
	Name() string
	MaxQueueLength() int32
	MaxParallelism() int32
	Operate(in []*Msg) []*Msg
	IsInputNode() bool
}

type BaseNode struct {
	maxQueueLength int32
	maxParallelism int32
}

type nodeCtx struct {
	node                   *Node
	inputChannels          []chan *Msg
	inputMessages          []*Msg
	downstream             []*nodeCtx
	downstreamInputChanIdx map[string]int

	NumActiveTasks    int64
	NumCompletedTasks int64
}

func (nodeCtx *nodeCtx) Start(ctx context.Context, wg *sync.WaitGroup) {
	if (*nodeCtx.node).IsInputNode() {
		inStream, ok := (*nodeCtx.node).(*InputNode)
		if !ok {
			log.Fatal("Invalid inputNode")
		}
		go (*inStream.inStream).Start()
	}

	for {
		select {
		case <-ctx.Done():
			wg.Done()
			return
		default:
			// inputs from inputsMessages for Operate
			inputs := make([]*Msg, 0)

			if !(*nodeCtx.node).IsInputNode() {
				nodeCtx.collectInputMessages()
				inputs = nodeCtx.inputMessages
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

func (nodeCtx *nodeCtx) collectInputMessages() {
	inputsNum := len(nodeCtx.inputChannels)
	nodeCtx.inputMessages = make([]*Msg, inputsNum)

	// init inputMessages,
	// receive messages from inputChannels,
	// and move them to inputMessages.
	for i := 0; i < inputsNum; i++ {
		channel := nodeCtx.inputChannels[i]
		msg := <-channel
		nodeCtx.inputMessages = append(nodeCtx.inputMessages, msg)
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

func (node *BaseNode) IsInputNode() bool {
	return false
}
